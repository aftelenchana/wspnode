'use strict';

const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { randomInt } = require('crypto'); // ← para aleatoriedad robusta

// ==================== DEBUG / LOGGING ====================
const DEBUG = process.env.ENVIO_DEBUG !== '0'; // por defecto ON (set ENVIO_DEBUG=0 para silenciar)
const LOG_PREFIX = '[ENVIO]';

const dbg = (...a) => { if (DEBUG) console.log(LOG_PREFIX, ...a); };
const info = (...a) => console.log(LOG_PREFIX, ...a);
const warn = (...a) => console.warn(LOG_PREFIX, ...a);
const error = (...a) => console.error(LOG_PREFIX, ...a);

const pretty = (obj, max = 1000) => {
  try {
    const j = typeof obj === 'string' ? obj : JSON.stringify(obj);
    return j.length > max ? j.slice(0, max) + '…' : j;
  } catch (e) {
    return String(obj);
  }
};

// Logger JSON por "acción" (compat)
function logAction(action, data = {}) {
  try {
    const line = { action, at: new Date().toISOString(), ...data };
    console.log(JSON.stringify(line));
  } catch (e) {
    console.log(`[logAction:${action}]`, data);
  }
}

// ==================== CONFIG ====================
const DEFAULTS = {
  baseDir: path.join(process.cwd(), 'campanas'),
  guardIntervalMs: 15000
};

// Zona horaria fija para Ecuador (Guayaquil): UTC-5
const ECUADOR_TZ_OFFSET_HOURS = -5;

const SCHEDULES = new Map();
const IN_FLIGHT = new Set();

module.exports = function registerEnvioMasivo(app, opts = {}) {
  // 🟢 Desde server.js
  const urlSistemaBase = opts.urlSistema;

  const cfg = {
    ...DEFAULTS,
    ...opts,
    statusEndpoint: `${urlSistemaBase}/dev/api/estado`,
    varsApiEndpoint: `${urlSistemaBase}/dev/api/variables_globales`, // POST
    existenciaEndpoint: `${urlSistemaBase}/dev/api/existencia`,      // ✅ POST JSON (NO form)
    // endpoint del server WhatsApp para verificar existencia:
    // NO lo ponemos como cfg.checkWaEndpoint (como pediste), lo usamos directo abajo.
  };

  const CAMPANAS_DIR = cfg.baseDir;
  const RT_DIR  = path.join(CAMPANAS_DIR, 'en_tiempo_real');
  const DF_DIR  = path.join(CAMPANAS_DIR, 'diferido');

  // Dump de arranque
  info('INIT envio_masivo');
  dbg('cfg:', cfg);
  dbg('CAMPANAS_DIR:', CAMPANAS_DIR);
  dbg('RT_DIR:', RT_DIR);
  dbg('DF_DIR:', DF_DIR);

  ensureDir(CAMPANAS_DIR, 'CAMPANAS_DIR');
  ensureDir(RT_DIR, 'RT_DIR');
  ensureDir(DF_DIR, 'DF_DIR');

  // Programar diferidos presentes al boot
  loadAndScheduleDiferidosOnBoot();

  // Guardia única
  if (!global.__ENVIO_MASIVO_GUARD__) {
    global.__ENVIO_MASIVO_GUARD__ = setInterval(guardScanDiferido, cfg.guardIntervalMs);
    logAction('inicio', { nota: 'Guardia diferido activado', intervalo_ms: cfg.guardIntervalMs });
    info('Guardia diferido activado cada', cfg.guardIntervalMs, 'ms');
  } else {
    dbg('Guardia ya estaba activa; no se duplica.');
  }

  // ==================== HELPERS ====================
  function ensureDir(d, label) {
    try {
      if (!fs.existsSync(d)) {
        fs.mkdirSync(d, { recursive: true });
        info('Creada carpeta', label || '', d);
      } else {
        dbg('Carpeta OK', label || '', d);
      }
      fs.accessSync(d, fs.constants.W_OK | fs.constants.R_OK);
      dbg('Permisos OK en', d);
    } catch (e) {
      error('Error creando/verificando carpeta', d, e.message);
      throw e;
    }
  }

  const sleep = (ms) => new Promise(r => setTimeout(r, ms));

  const toMs = (txt) => {
    if (!txt || typeof txt !== 'string') return 0;
    const s = txt.trim().toLowerCase();
    const m = s.match(/(\d+(?:\.\d+)?)\s*(ms|milisegundos?|s|seg|segundos?|min|minutos?|h|horas?)/i);
    if (!m) return 0;
    const val = parseFloat(m[1]); const unit = m[2];
    if (/^ms|miliseg/i.test(unit)) return val;
    if (/^(s|seg|segundos?)/i.test(unit)) return val * 1000;
    if (/^min|minutos?/i.test(unit)) return val * 60 * 1000;
    if (/^h|horas?/i.test(unit)) return val * 60 * 60 * 1000;
    return 0;
  };

  /**
   * Interpreta una cadena de fecha-hora como HORA DE GUAYAQUIL (UTC-5)
   */
  const parseLocalDateTime = (str) => {
    if (!str) return new Date();

    const clean = String(str)
      .trim()
      .replace(' ', 'T')
      .replace(/\.\d+$/, '');

    const m = clean.match(
      /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/
    );

    if (!m) {
      const dFallback = new Date(clean);
      return isNaN(dFallback.getTime()) ? new Date() : dFallback;
    }

    const year  = parseInt(m[1], 10);
    const month = parseInt(m[2], 10);
    const day   = parseInt(m[3], 10);
    const hour  = parseInt(m[4], 10);
    const min   = parseInt(m[5], 10);
    const sec   = m[6] ? parseInt(m[6], 10) : 0;

    const utcMs = Date.UTC(
      year,
      month - 1,
      day,
      hour - ECUADOR_TZ_OFFSET_HOURS, // -(-5) => +5
      min,
      sec
    );

    return new Date(utcMs);
  };

  const campaignPathByMode = (modo_tiempo, id) => {
    const folder = (String(modo_tiempo || '').toLowerCase() === 'en_tiempo_real') ? RT_DIR : DF_DIR;
    return path.join(folder, `${id}.json`);
  };

  const loadJSON = (p) => {
    try {
      const raw = fs.readFileSync(p, 'utf8');
      return JSON.parse(raw);
    } catch (e) {
      dbg('loadJSON falló', p, e.message);
      return null;
    }
  };

  const saveJSON = (p, obj) => {
    try {
      fs.writeFileSync(p, JSON.stringify(obj, null, 2), 'utf8');
      dbg('saveJSON OK', p);
    } catch (e) {
      error('saveJSON ERROR', p, e.message);
      throw e;
    }
  };

  // ----------------- Parsers & Logs HTTP -----------------
  function logHttpFailure(ctx, err) {
    const st   = err?.response?.status;
    const data = err?.response?.data;
    warn(`${ctx} ERROR`, st ?? '', typeof data === 'string' ? (data + '').slice(0, 300) : data, err?.message);
  }

  // Acepta JSON, x-www-form-urlencoded con "payload", o “una sola key” que ya es el JSON
  function extractPayload(req) {
    const b = req.body;
    if (!b) {
      dbg('extractPayload: body vacío');
      return null;
    }

    if (typeof b === 'object') {
      // JSON directo
      if ('ok' in b || 'campana' in b || 'destinatarios' in b) {
        dbg('extractPayload: JSON directo', Object.keys(b));
        return b;
      }

      // urlencoded con campo "payload"
      if (typeof b.payload === 'string') {
        try {
          const parsed = JSON.parse(b.payload);
          dbg('extractPayload: via payload string (urlencoded)');
          return parsed;
        } catch (e) {
          dbg('extractPayload: payload inválido', e.message);
        }
      }

      // Caso clave única estilo PHP: {"{json...}":""}
      const keys = Object.keys(b);
      if (keys.length === 1 && keys[0].trim().startsWith('{')) {
        try {
          const parsed = JSON.parse(keys[0]);
          dbg('extractPayload: via clave única estilo PHP');
          return parsed;
        } catch (e) {
          dbg('extractPayload: clave única inválida', e.message);
        }
      }
    }

    if (typeof b === 'string') {
      try {
        const parsed = JSON.parse(b);
        dbg('extractPayload: string JSON');
        return parsed;
      } catch (e) {
        dbg('extractPayload: string no JSON', e.message);
      }
    }

    warn('extractPayload: no se pudo interpretar el body');
    return null;
  }

  const postStatus = async (body) => {
    try {
      const toSend = { ...body, timestamp: new Date().toISOString() };
      dbg('POST estado.php → body:', pretty(toSend));
      const resp = await axios.post(cfg.statusEndpoint, toSend, { timeout: 20000 });
      dbg('POST estado.php ← status:', resp.status, 'resp:', pretty(resp.data));
    } catch (e) {
      const st = e?.response?.status;
      const data = e?.response?.data;
      error('postStatus ERROR', st, typeof data === 'string' ? data.slice(0, 200) : data);
    }
  };

  // ==================== ✅ EXISTENCIA: POST JSON a /dev/existencia.php ====================
  async function reportExistencia(payload) {
    // payload: { sessionId, numero, exists (true/false/null), sent (true/false), motivo, ...extra }
    try {
      const body = {
        ...payload,
        ts: payload.ts || new Date().toISOString()
      };

      dbg('POST existencia.php (JSON) → url:', cfg.existenciaEndpoint, 'body:', pretty(body));
      const resp = await axios.post(
        cfg.existenciaEndpoint,
        body,
        {
          timeout: 15000,
          headers: { 'Content-Type': 'application/json' },
          validateStatus: () => true
        }
      );
      dbg('POST existencia.php (JSON) ← status:', resp.status, 'resp:', pretty(resp.data));
      return { ok: resp.status >= 200 && resp.status < 300, status: resp.status, data: resp.data };
    } catch (e) {
      logHttpFailure('reportExistencia(JSON)', e);
      return { ok: false, error: e?.message || String(e) };
    }
  }

  // ==================== ✅ CHEQUEO WHATSAPP (antes de enviar) ====================
  // Usa el server de WhatsApp (campana.url) + endpoint fijo /check-whatsapp
  // Debe responder algo tipo { ok:true, exists:true/false } o { exists:true/false }
  async function checkWhatsAppNumber(baseUrl, sessionId, numero) {
    const url = `${String(baseUrl).replace(/\/+$/, '')}/check-whatsapp`;
    const body = { sessionId, number: String(numero) };

    try {
      dbg('POST check-whatsapp → url:', url, 'body:', pretty(body));
      const r = await axios.post(url, body, {
        timeout: 15000,
        headers: { 'Content-Type': 'application/json' },
        validateStatus: () => true
      });

      dbg('POST check-whatsapp ← status:', r.status, 'resp:', pretty(r.data));

      if (r.status >= 200 && r.status < 300) {
        const d = r.data || {};
        // soporta varias formas:
        const exists =
          (typeof d.exists === 'boolean') ? d.exists :
          (typeof d.ok === 'boolean' && typeof d.data?.exists === 'boolean') ? d.data.exists :
          (Array.isArray(d) && typeof d[0]?.exists === 'boolean') ? d[0].exists :
          null;

        if (typeof exists === 'boolean') return { ok: true, exists, raw: d };
        return { ok: false, exists: null, raw: d, error: 'Respuesta sin boolean exists' };
      }

      return { ok: false, exists: null, raw: r.data, error: `HTTP_${r.status}` };
    } catch (e) {
      return { ok: false, exists: null, error: e?.message || String(e) };
    }
  }

  // ========= Tokens dinámicos #ID# (via variables_globales POR POST) =========
  const tokenRegex = /#(\d+)#/g;

  async function fetchVarAlternatives(id) {
    const url = cfg.varsApiEndpoint;

    // Intento 1: POST JSON
    try {
      const body = { id };
      dbg('POST variables_globales (JSON) → url:', url, 'body:', pretty(body));
      const resp = await axios.post(
        url,
        body,
        { timeout: 15000, validateStatus: () => true, headers: { 'Content-Type': 'application/json' } }
      );
      dbg('POST variables_globales (JSON) ← status:', resp.status, 'resp:', pretty(resp.data));

      if (resp.status >= 200 && resp.status < 300) {
        const data = resp?.data || {};
        const rawText =
          (typeof data.texto === 'string' ? data.texto :
           (typeof data?.data?.texto === 'string' ? data.data.texto : ''));
        const arr = String(rawText)
          .split(',')
          .map(s => s.trim())
          .filter(s => s.length > 0);
        dbg(`fetchVarAlternatives #${id}# (JSON): opciones=${arr.length}`, arr);
        return arr;
      }
      warn(`fetchVarAlternatives(JSON): HTTP ${resp.status}`);
    } catch (e) {
      logHttpFailure('fetchVarAlternatives(JSON)', e);
    }

    // Intento 2: POST x-www-form-urlencoded (PHP amigable)
    try {
      const params = new URLSearchParams();
      params.append('id', String(id));
      dbg('POST variables_globales (FORM) → url:', url, 'body:', pretty(params.toString()));
      const resp = await axios.post(
        url,
        params,
        { timeout: 15000, validateStatus: () => true, headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
      );
      dbg('POST variables_globales (FORM) ← status:', resp.status, 'resp:', pretty(resp.data));

      if (resp.status >= 200 && resp.status < 300) {
        const data = resp?.data || {};
        const rawText =
          (typeof data.texto === 'string' ? data.texto :
           (typeof data?.data?.texto === 'string' ? data.data.texto : ''));
        const arr = String(rawText)
          .split(',')
          .map(s => s.trim())
          .filter(s => s.length > 0);
        dbg(`fetchVarAlternatives #${id}# (FORM): opciones=${arr.length}`, arr);
        return arr;
      }
      warn(`fetchVarAlternatives(FORM): HTTP ${resp.status}`);
    } catch (e) {
      logHttpFailure('fetchVarAlternatives(FORM)', e);
    }

    warn(`No se pudo obtener texto para #${id}# via POST en ${url}`);
    return [];
  }

  function pickRandom(arr) {
    if (!Array.isArray(arr) || arr.length === 0) return '';
    if (arr.length === 1) return arr[0];
    const idx = randomInt(0, arr.length);
    dbg('pickRandom: len=', arr.length, 'idx=', idx);
    return arr[idx];
  }

  async function prepareTokenReplacer(template) {
    const ids = Array.from(new Set([...String(template || '').matchAll(tokenRegex)].map(m => m[1])));
    dbg('prepareTokenReplacer ids:', ids);

    const cache = new Map();
    for (const id of ids) {
      const opts = await fetchVarAlternatives(id);
      cache.set(id, opts);
    }

    const replaceWithRandom = (msg) => String(msg).replace(tokenRegex, (_full, id) => {
      const opts = cache.get(id) || [];
      if (!opts.length) {
        dbg(`replaceWithRandom: #${id}# sin opciones -> ''`);
        return '';
      }
      const choice = pickRandom(opts);
      dbg(`replaceWithRandom: #${id}# opciones=`, opts, '→ elegido=', choice);
      return choice;
    });

    return { replaceWithRandom, hasTokens: ids.length > 0 };
  }

  // ========= Params dinámicos ##paramN## =========
  const paramTokenRegex = /##param(\d+)##/gi;

  function makeParamReplacerFromParts(parts) {
    return function replaceParams(msg) {
      if (!msg) return msg;
      return String(msg).replace(paramTokenRegex, (_m, nStr) => {
        const n = parseInt(nStr, 10);
        if (!Number.isFinite(n) || n < 1) return '';
        const val = (parts[n - 1] ?? '').toString(); // sin trim
        return val;
      });
    };
  }

  // ========= Pre-chequeo de sesión para en_tiempo_real =========
  async function preflightSessionOrError(modo, baseUrl, sessionId) {
    const isRealtime = String(modo).toLowerCase() === 'en_tiempo_real';
    if (!baseUrl || !sessionId) {
      return { ok: !isRealtime, reason: 'Faltan baseUrl/sessionId' };
    }
    try {
      const url = `${baseUrl}/check-session`;
      const body = { sessionId };
      dbg('POST check-session → url:', url, 'body:', pretty(body));
      const r = await axios.post(url, body, { timeout: 8000 });
      dbg('POST check-session ← status:', r.status, 'resp:', pretty(r.data));
      return { ok: true, data: r.data };
    } catch (e) {
      const st = e?.response?.status;
      const data = e?.response?.data;
      warn('POST check-session ERROR status:', st, 'resp:', pretty(data), 'msg:', e?.message);
      if (isRealtime && st === 404) {
        return { ok: false, reason: 'SesionNoEncontradaEnMemoria', server: data };
      }
      return { ok: !isRealtime, reason: `ErrorPreflight:${st || e.message}`, server: data };
    }
  }

  // ==================== SCHEDULING ====================
  function scheduleDiferido(filePath) {
    const data = loadJSON(filePath);
    if (!data || !data.campana) {
      dbg('scheduleDiferido: JSON inválido', filePath);
      return;
    }

    const estado = data.estado_campana || 'pendiente';
    if (estado === 'en_proceso' || estado === 'finalizada' || estado === 'cancelado') {
      dbg('scheduleDiferido: omitido por estado', estado, filePath);
      return;
    }

    const when = parseLocalDateTime(data.campana.fecha_hora_envio);
    const delay = Math.max(0, when.getTime() - Date.now());

    const prev = SCHEDULES.get(filePath);
    if (prev) {
      clearTimeout(prev);
      dbg('scheduleDiferido: reprogramando existente', filePath);
    }

    const t = setTimeout(() => {
      SCHEDULES.delete(filePath);
      processCampaignFile(filePath, { modeLabel: 'diferido' }).catch(error);
    }, delay);

    SCHEDULES.set(filePath, t);

    logAction('inicio', {
      id_campana: data.campana.id,
      archivo: filePath,
      modo: 'diferido',
      programada_para: when.toISOString(),
      solo_programacion: true
    });

    info('Diferido programado:', path.basename(filePath), 'para', when.toISOString());
  }

  function loadAndScheduleDiferidosOnBoot() {
    try {
      const files = fs.readdirSync(DF_DIR).filter(f => f.endsWith('.json'));
      info('Diferidos detectados al boot:', files.length);
      for (const f of files) {
        const p = path.join(DF_DIR, f);
        scheduleDiferido(p);
      }
    } catch (e) {
      error('Error al programar diferidos en bootstrap:', e?.message || e);
    }
  }

  // ==================== ENDPOINT ====================
  app.post('/masivo/preview', async (req, res) => {
    try {
      let payload = extractPayload(req);
      if (!payload || typeof payload !== 'object') {
        return res.status(400).json({ ok: false, message: 'Body inválido: {ok, campana, destinatarios}.' });
      }

      const { ok, campana, destinatarios } = payload;
      if (ok !== true) return res.status(400).json({ ok: false, message: '`ok` debe ser true.' });
      if (!campana || typeof campana !== 'object') return res.status(400).json({ ok: false, message: 'Falta `campana`.' });
      if (!Array.isArray(destinatarios)) return res.status(400).json({ ok: false, message: '`destinatarios` debe ser array.' });

      const id = String(campana.id || '').trim();
      if (!id) return res.status(400).json({ ok: false, message: 'campana.id inválido.' });

      const modo = String(campana.modo_tiempo || '').toLowerCase();
      if (!['en_tiempo_real', 'diferido'].includes(modo)) {
        return res.status(400).json({ ok: false, message: 'modo_tiempo debe ser "en_tiempo_real" o "diferido".' });
      }

      const baseUrl = String(campana.url || '').replace(/\/+$/, '');
      const sessionId = String(campana.key_wsp || '').trim();

      // Preflight para en_tiempo_real
      const pre = await preflightSessionOrError(modo, baseUrl, sessionId);
      if (!pre.ok && modo === 'en_tiempo_real') {
        return res.status(400).json({
          ok: false,
          message: 'Sesión no encontrada en la memoria del servidor de WhatsApp.',
          detalle: pre.reason,
          server: pre.server || null,
          sugerencia: 'Inicia la sesión con /start-session y escanea el QR, o usa el sessionId correcto en campana.key_wsp.'
        });
      }

      // Guardar snapshot
      const filePath = campaignPathByMode(modo, id);
      const snapshot = {
        ...payload,
        estado_campana: 'pendiente', // pendiente | en_proceso | finalizada | error | cancelado
        progreso: {},                // { [destId]: { sent: true, at: ISO } }
        created_at: new Date().toISOString()
      };
      saveJSON(filePath, snapshot);

      logAction('inicio', {
        id_campana: campana.id,
        archivo: filePath,
        modo,
        total_destinatarios: destinatarios.length,
        solo_registro: true
      });

      info('Campaña registrada:', id, 'modo:', modo, 'destinatarios:', destinatarios.length);

      if (modo === 'en_tiempo_real') {
        processCampaignFile(filePath, { modeLabel: 'en_tiempo_real' }).catch(error);
      } else {
        if (!pre.ok) warn('Registro diferido con sesión no en memoria actualmente:', sessionId);
        scheduleDiferido(filePath);
      }

      return res.json({
        ok: true,
        message: 'Campaña registrada correctamente.',
        resumen: {
          id: campana.id,
          iduser: campana.iduser,
          empresa: campana.empresa,
          metodo_envio: campana.metodo_envio,
          intervalo_seg: Number(campana.intervalo_tiempo) || null,
          mensajes_para_pausa: Number(campana.mensajes_para_pausa) || 0,
          tiempo_para_cantidad: String(campana.tiempo_para_cantidad || ''),
          modo_tiempo: modo,
          total_destinatarios: destinatarios.length,
          archivo: `/campanas/${modo}/${id}.json`,
          programado_para: campana.fecha_hora_envio || null,
          accion: campana.accion || null
        }
      });
    } catch (err) {
      error('Error en /masivo/preview:', err?.message || err);
      return res.status(500).json({ ok: false, message: 'Error interno al registrar la campaña.' });
    }
  });

  // ==================== PROCESADOR ====================
  async function processCampaignFile(filePath, { modeLabel }) {
    // Evitar dobles ejecuciones
    if (IN_FLIGHT.has(filePath)) {
      logAction('envio_mensaje', { fase: 'skip_duplicado', archivo: filePath, motivo: 'IN_FLIGHT' });
      await postStatus({ action: 'skip_duplicado', id_campana: loadJSON(filePath)?.campana?.id || 0, fase: 'skip_duplicado' });
      dbg('IN_FLIGHT detectado, skip', filePath);
      return;
    }
    IN_FLIGHT.add(filePath);
    info('Procesando campaña:', path.basename(filePath), 'modo:', modeLabel);

    const startedAt = Date.now();
    let data = loadJSON(filePath);
    if (!data || !data.campana || !Array.isArray(data.destinatarios)) {
      warn('JSON de campaña inválido, aborta', filePath);
      IN_FLIGHT.delete(filePath);
      return;
    }

    // 🔒 Check 0: si está cancelada, no arrancar
    if (String(data.estado_campana || '').toLowerCase() === 'cancelado') {
      info('Campaña en estado "cancelado". No se procesa.', filePath);
      await postStatus({ action: 'cancelado', id_campana: data.campana.id, fase: 'inicio' });
      IN_FLIGHT.delete(filePath);
      return;
    }

    const currentState = data.estado_campana || 'pendiente';
    if (currentState === 'en_proceso' || currentState === 'finalizada') {
      dbg('Estado no ejecutable, aborta:', currentState);
      IN_FLIGHT.delete(filePath);
      return;
    }

    const { campana } = data;
    const baseUrl  = String(campana.url || '').replace(/\/+$/, '');
    const sendUrl  = `${baseUrl}/send-message`;
    const sessionId = String(campana.key_wsp || '').trim();

    // Tiempos
    const intervalMs = Number.isFinite(Number(campana.intervalo_tiempo))
      ? Number(campana.intervalo_tiempo) * 1000
      : toMs(String(campana.intervalo_tiempo || '').trim());
    const pausaCada = Math.max(0, parseInt(campana.mensajes_para_pausa, 10) || 0);
    const pausaMs   = toMs(String(campana.tiempo_para_cantidad || '').trim());

    // Diferido: respeta hora exacta si guardia disparó antes (usando hora de Guayaquil)
    if (String(modeLabel).toLowerCase() === 'diferido') {
      const when = parseLocalDateTime(campana.fecha_hora_envio);
      const delayToStart = Math.max(0, when.getTime() - Date.now());
      if (delayToStart > 0) {
        info('Esperando hasta hora programada (Guayaquil)', when.toISOString());
        await sleep(delayToStart);
      }
    }

    // Preparar reemplazos de tokens variables_globales
    const tokenReplacer = await prepareTokenReplacer(campana.mensaje);

    logAction('inicio', {
      id_campana: campana.id,
      archivo: filePath,
      modo: String(modeLabel),
      url_destino: sendUrl,
      sessionId,
      mensaje_resumen: (campana.mensaje || '').slice(0, 100),
      intervalo_ms: intervalMs,
      pausa_cada: pausaCada,
      pausa_ms: pausaMs,
      total_destinatarios: Array.isArray(data.destinatarios) ? data.destinatarios.length : 0,
      fecha_hora_envio: campana.fecha_hora_envio || null
    });

    await postStatus({ action: 'inicio', id_campana: campana.id });
    data.estado_campana = 'en_proceso';
    saveJSON(filePath, data);

    let errores = 0;

    const jobs = data.destinatarios.map((d) => {
      const parts = String(d.numero || '').split('-'); // ["5939...", "Alex", "mail", ...]
      const toNumber = (parts[0] || '').trim();       // 👈 número CRUDO como mandas (5939988...)
      return { row: d, to: toNumber, parts };
    }).filter(j => j.to);

    for (let i = 0; i < jobs.length; i++) {
      // 🔒 Check 1: justo antes de enviar a cada destinatario
      const latest = loadJSON(filePath) || data;
      if (String(latest.estado_campana || '').toLowerCase() === 'cancelado') {
        info('Cancelado detectado antes de enviar el siguiente mensaje. Se detiene el proceso.', filePath);
        await postStatus({ action: 'cancelado', id_campana: latest.campana.id, fase: 'antes_de_envio' });
        IN_FLIGHT.delete(filePath);
        return;
      }

      const j = jobs[i];
      const key = String(j.row.id ?? j.to);
      const prog = (latest.progreso && latest.progreso[key]) || data.progreso[key] || {};

      if (prog.sent === true) {
        dbg('skip ya enviado', key);
        continue;
      }

      // Mensaje por destinatario (respeta template exacto)
      const messageTemplate = String(campana.mensaje);
      const replaceParams   = makeParamReplacerFromParts(j.parts);
      const withParams      = replaceParams(messageTemplate);
      const msgForThisRecipient = tokenReplacer.replaceWithRandom(withParams);

      // ==================== ✅ 1) CHECK EXISTENCIA WHATSAPP ====================
      const check = await checkWhatsAppNumber(baseUrl, sessionId, j.to);

      // Reporte a existencia.php SIEMPRE (exista o no)
      await reportExistencia({
        tipo: 'check',
        sessionId,
        numero: j.to,
        exists: (typeof check.exists === 'boolean') ? check.exists : null,
        sent: false,
        motivo: check.ok
          ? (check.exists ? 'tiene_whatsapp' : 'no_tiene_whatsapp')
          : 'error_check',
        extra: {
          check_ok: check.ok,
          check_error: check.error || null,
          check_raw: check.raw || null
        }
      });

      // Si NO existe, NO enviamos
      if (!check.ok || check.exists !== true) {
        logAction('envio_mensaje', {
          fase: 'skip_no_whatsapp',
          id_campana: campana.id,
          archivo: filePath,
          idx: i,
          destinatario_id: j.row.id ?? key,
          numero: j.to,
          motivo: !check.ok ? 'error_check' : 'no_tiene_whatsapp'
        });

        // marcar como "sent" en progreso? (si quieres NO reintentar nunca)
        // aquí lo dejamos NO enviado para que puedas reintentar si luego cambias lógica.
        continue;
      }

      // ==================== ✅ 2) ENVIAR MENSAJE (solo si exists=true) ====================
      const body = { sessionId, to: j.to, message: msgForThisRecipient };
      dbg('POST send-message → url:', sendUrl, 'body:', pretty(body));

      logAction('envio_mensaje', {
        fase: 'antes',
        id_campana: campana.id,
        archivo: filePath,
        idx: i,
        destinatario: {
          id: j.row.id ?? key,
          numero: j.to,
          tipo: j.row.tipo,
          estado_envio: j.row.estado_envio
        },
        body
      });

      try {
        const resp = await axios.post(sendUrl, body, {
          timeout: 30000,
          headers: { 'Content-Type': 'application/json' }
        });

        dbg('POST send-message ← status:', resp.status, 'resp:', pretty(resp.data));

        // Reporte a existencia.php: enviado_ok
        await reportExistencia({
          tipo: 'send',
          sessionId,
          numero: j.to,
          exists: true,
          sent: true,
          motivo: 'enviado_ok',
          extra: { http_status: resp.status, resp: resp.data }
        });

        logAction('envio_mensaje', {
          fase: 'exito',
          id_campana: campana.id,
          archivo: filePath,
          idx: i,
          destinatario_id: j.row.id ?? key,
          numero: j.to,
          http_status: resp.status
        });

        await postStatus({
          action: 'envio_mensaje',
          id_campana: campana.id,
          destinatario_id: j.row.id,
          fase: 'exito'
        });

        // guardamos progreso
        const live = loadJSON(filePath) || data;
        live.progreso = live.progreso || {};
        live.progreso[key] = { sent: true, at: new Date().toISOString() };
        saveJSON(filePath, live);
        data = live;
      } catch (e) {
        errores++;
        const st = e?.response?.status;
        const dd = e?.response?.data;

        dbg('POST send-message ← ERROR status:', st, 'resp:', pretty(dd), 'msg:', e?.message);

        // Reporte a existencia.php: error_envio
        await reportExistencia({
          tipo: 'send',
          sessionId,
          numero: j.to,
          exists: true,
          sent: false,
          motivo: 'error_envio',
          extra: { http_status: st || null, error: e?.message || 'Error envío', resp: dd || null }
        });

        logAction('envio_mensaje', {
          fase: 'error',
          id_campana: campana.id,
          archivo: filePath,
          idx: i,
          destinatario_id: j.row.id ?? key,
          numero: j.to,
          http_status: st,
          error_message: e?.message || 'Error envío',
          error_body: typeof dd === 'string' ? dd.slice(0, 400) : dd
        });

        await postStatus({
          action: 'envio_mensaje',
          id_campana: campana.id,
          destinatario_id: j.row.id,
          fase: 'error'
        });
      }

      // Intervalo entre envíos
      if (i + 1 < jobs.length && intervalMs > 0) {
        await sleep(intervalMs);
        logAction('envio_mensaje', {
          fase: 'pausa_intervalo',
          id_campana: campana.id,
          archivo: filePath,
          ms: intervalMs,
          next_index: i + 1
        });
        await postStatus({ action: 'pausa_intervalo', id_campana: campana.id, fase: 'pausa_intervalo' });
      }

      // Pausa por lote
      if (i + 1 < jobs.length && pausaCada > 0 && pausaMs > 0 && (i + 1) % pausaCada === 0) {
        await sleep(pausaMs);
        logAction('envio_mensaje', {
          fase: 'pausa_lote',
          id_campana: campana.id,
          archivo: filePath,
          ms: pausaMs,
          despues_de: i + 1
        });
        await postStatus({ action: 'pausa_lote', id_campana: campana.id, fase: 'pausa_lote' });
      }
    }

    // Finalizar
    const enviados = Object.values(data.progreso || {}).filter(p => p && p.sent === true).length;

    await postStatus({
      action: 'Proceso Finalizado',
      id_campana: campana.id,
      enviados,
      errores
    });

    const durationMs = Date.now() - startedAt;

    data.estado_campana = 'finalizada';
    data.errores = errores;
    saveJSON(filePath, data);

    if (SCHEDULES.has(filePath)) {
      try { clearTimeout(SCHEDULES.get(filePath)); } catch {}
      SCHEDULES.delete(filePath);
    }

    // Borrar JSON al finalizar
    let deleted = false;
    let deleteError = null;
    try {
      if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
        deleted = true;
      } else {
        deleted = true;
      }
    } catch (e) {
      deleteError = e?.message || String(e);
    }

    logAction('Proceso Finalizado', {
      id_campana: campana.id,
      archivo: filePath,
      total_destinatarios: Array.isArray(data.destinatarios) ? data.destinatarios.length : 0,
      enviados,
      errores,
      duracion_ms: durationMs,
      json_borrado: deleted,
      delete_error: deleteError
    });

    info('Finalizada campaña:', campana.id, 'enviados:', enviados, 'errores:', errores, 'ms:', durationMs);

    IN_FLIGHT.delete(filePath);
  }

  // ==================== GUARDIA ====================
  async function guardScanDiferido() {
    try {
      const files = fs.readdirSync(DF_DIR).filter(f => f.endsWith('.json'));
      dbg('Guardia: archivos en diferido', files.length);

      for (const f of files) {
        const p = path.join(DF_DIR, f);
        const data = loadJSON(p);
        if (!data || !data.campana) {
          dbg('Guardia: JSON inválido', p);
          continue;
        }

        const estado = data.estado_campana || 'pendiente';
        if (estado === 'en_proceso' || estado === 'finalizada' || estado === 'cancelado') {
          dbg('Guardia: skip por estado', estado, p);
          continue;
        }

        const when = parseLocalDateTime(data.campana.fecha_hora_envio);
        const due = Date.now() >= when.getTime();

        if (SCHEDULES.has(p)) {
          if (due && !IN_FLIGHT.has(p)) {
            clearTimeout(SCHEDULES.get(p));
            SCHEDULES.delete(p);
            logAction('envio_mensaje', { fase: 'guard_trigger_immediate', archivo: p });
            await postStatus({ action: 'guard_trigger_immediate', id_campana: data.campana.id, fase: 'guard_trigger_immediate' });
            info('Guardia: disparo inmediato', path.basename(p));
            processCampaignFile(p, { modeLabel: 'diferido' }).catch(error);
          }
          continue;
        }

        if (due) {
          if (!IN_FLIGHT.has(p)) {
            logAction('envio_mensaje', { fase: 'guard_trigger_now', archivo: p });
            await postStatus({ action: 'guard_trigger_now', id_campana: data.campana.id, fase: 'guard_trigger_now' });
            info('Guardia: disparo NOW', path.basename(p));
            processCampaignFile(p, { modeLabel: 'diferido' }).catch(error);
          }
        } else {
          info('Guardia: programando pendiente', path.basename(p), 'para', when.toISOString());
          scheduleDiferido(p);
        }
      }
    } catch (e) {
      error('Guardia diferido error:', e?.message || e);
    }
  }

  // Export opcional
  return {
    dirs: { CAMPANAS_DIR, RT_DIR, DF_DIR },
    processCampaignFile,
    scheduleDiferido
  };
};
