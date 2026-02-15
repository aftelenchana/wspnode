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

// ==================== LOCK por archivo (anti-doble-proceso) ====================
// Nota: esto evita dobles envíos cuando una campaña diferida se dispara por timeout y por guardia al mismo tiempo.
// El lock es a nivel filesystem, así que incluso si hay re-entradas o dos disparadores, solo uno procesa.
function lockPathFor(filePath) { return `${filePath}.lock`; }

function acquireFileLock(filePath) {
  const lp = lockPathFor(filePath);
  try {
    // si el JSON no existe, no tiene sentido lockear
    if (!fs.existsSync(filePath)) return { ok: false, reason: 'no_json' };

    // openSync con 'wx' falla si ya existe
    const fd = fs.openSync(lp, 'wx');
    const meta = { pid: process.pid, at: new Date().toISOString(), file: filePath };
    try { fs.writeFileSync(fd, JSON.stringify(meta, null, 2)); } catch {}
    try { fs.closeSync(fd); } catch {}
    return { ok: true, lockPath: lp };
  } catch (e) {
    // lock ya existe
    if (e && (e.code === 'EEXIST' || e.code === 'EACCES')) return { ok: false, reason: 'locked', lockPath: lp };
    return { ok: false, reason: e?.code || e?.message || 'lock_error', lockPath: lp };
  }
}

function releaseFileLock(filePath) {
  const lp = lockPathFor(filePath);
  try { if (fs.existsSync(lp)) fs.unlinkSync(lp); } catch {}
}

// ==================== Normalización de número (dedupe) ====================
function normalizeNumber(n) {
  const s = String(n || '');
  const digits = s.replace(/\D+/g, '');
  return digits || s.trim();
}

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
          dbg('extractPayload: via key única (PHP)');
          return parsed;
        } catch (e) {
          dbg('extractPayload: key única inválida', e.message);
        }
      }
    }

    // Si llegó como string plano
    if (typeof b === 'string') {
      try {
        const parsed = JSON.parse(b);
        dbg('extractPayload: string JSON');
        return parsed;
      } catch {}
    }

    dbg('extractPayload: no se pudo interpretar');
    return null;
  }

  // ==================== STATUS REPORT ====================
  async function postStatus(payload) {
    try {
      await axios.post(cfg.statusEndpoint, payload, {
        timeout: 15000,
        headers: { 'Content-Type': 'application/json' }
      });
    } catch (e) {
      logHttpFailure('postStatus', e);
    }
  }

  // ==================== VARIABLES GLOBALES / TOKENS ====================
  async function fetchVariablesGlobales() {
    try {
      const r = await axios.post(cfg.varsApiEndpoint, {}, { timeout: 15000 });
      return r.data;
    } catch (e) {
      logHttpFailure('variables_globales', e);
      return null;
    }
  }

  const TOKEN_REGEX = /##([a-zA-Z0-9_]+)##/g;

  function makeTokenReplacer(vars) {
    const map = new Map();
    if (vars && typeof vars === 'object') {
      for (const [k, v] of Object.entries(vars)) {
        if (!k) continue;
        // Permitimos que cada token sea string o array
        map.set(k, v);
      }
    }

    function pickValue(val) {
      if (Array.isArray(val)) {
        if (!val.length) return '';
        return val[randomInt(0, val.length)] ?? '';
      }
      if (val === null || val === undefined) return '';
      return String(val);
    }

    return {
      replaceWithRandom(text) {
        if (!text) return text;
        return String(text).replace(TOKEN_REGEX, (_m, token) => {
          if (!map.has(token)) return '';
          return pickValue(map.get(token));
        });
      }
    };
  }

  async function prepareTokenReplacer(message) {
    try {
      const varsResp = await fetchVariablesGlobales();
      const vars = varsResp?.variables || varsResp?.data || varsResp || {};
      const replacer = makeTokenReplacer(vars);

      // Validación liviana
      const test = replacer.replaceWithRandom(message || '');
      dbg('Token test (preview):', (test || '').slice(0, 120));
      return replacer;
    } catch (e) {
      warn('prepareTokenReplacer fallback:', e?.message || e);
      return makeTokenReplacer({});
    }
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
        progreso: {},                // { [numero_normalizado]: { sent: true, at: ISO, ... } }
        created_at: new Date().toISOString()
      };
      saveJSON(filePath, snapshot);

      logAction('inicio', {
        id_campana: campana.id,
        archivo: filePath,
        modo: modo,
        total_destinatarios: destinatarios.length,
        preview_only: true
      });

      res.json({ ok: true, message: 'Preview guardado.', filePath });

    } catch (e) {
      error('Error /masivo/preview', e?.message || e);
      res.status(500).json({ ok: false, message: 'Error interno', error: e?.message || String(e) });
    }
  });

  app.post('/masivo/iniciar', async (req, res) => {
    try {
      const payload = extractPayload(req);
      if (!payload || payload.ok !== true || !payload.campana || !Array.isArray(payload.destinatarios)) {
        return res.status(400).json({ ok: false, message: 'Body inválido' });
      }

      const campana = payload.campana;
      const modo = String(campana.modo_tiempo || '').toLowerCase();
      const id = String(campana.id || '').trim();
      if (!id) return res.status(400).json({ ok: false, message: 'campana.id inválido' });
      if (!['en_tiempo_real', 'diferido'].includes(modo)) return res.status(400).json({ ok: false, message: 'modo_tiempo inválido' });

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
          sugerencia: 'Inicia la sesión con /start-session y escanea el QR, o usa el sessionId correcto.'
        });
      }

      const filePath = campaignPathByMode(modo, id);

      const snapshot = {
        ...payload,
        estado_campana: 'pendiente',
        progreso: payload.progreso && typeof payload.progreso === 'object' ? payload.progreso : {},
        created_at: new Date().toISOString()
      };
      saveJSON(filePath, snapshot);

      logAction('inicio', {
        id_campana: campana.id,
        archivo: filePath,
        modo: modo,
        total_destinatarios: payload.destinatarios.length,
        iniciar: true
      });

      if (modo === 'en_tiempo_real') {
        processCampaignFile(filePath, { modeLabel: 'en_tiempo_real' }).catch(error);
      } else {
        scheduleDiferido(filePath);
      }

      res.json({ ok: true, message: 'Campaña registrada', filePath });

    } catch (e) {
      error('Error /masivo/iniciar', e?.message || e);
      res.status(500).json({ ok: false, message: 'Error interno', error: e?.message || String(e) });
    }
  });

  app.post('/masivo/cancelar', (req, res) => {
    try {
      const body = req.body || {};
      const id = String(body.id_campana || body.id || '').trim();
      const modo = String(body.modo_tiempo || 'diferido').toLowerCase();

      if (!id) return res.status(400).json({ ok: false, message: 'Falta id_campana' });

      const filePath = campaignPathByMode(modo, id);
      const data = loadJSON(filePath);
      if (!data || !data.campana) return res.status(404).json({ ok: false, message: 'Campaña no encontrada' });

      data.estado_campana = 'cancelado';
      saveJSON(filePath, data);

      // limpiar timeout si estaba programada
      if (SCHEDULES.has(filePath)) {
        try { clearTimeout(SCHEDULES.get(filePath)); } catch {}
        SCHEDULES.delete(filePath);
      }

      logAction('cancelar', { id_campana: data.campana.id, archivo: filePath, modo });

      res.json({ ok: true, message: 'Campaña cancelada', filePath });
    } catch (e) {
      error('Error /masivo/cancelar', e?.message || e);
      res.status(500).json({ ok: false, message: 'Error interno', error: e?.message || String(e) });
    }
  });

  // ==================== WHATSAPP CHECK / REPORT EXISTENCIA ====================
  async function checkWhatsAppNumber(baseUrl, sessionId, numero) {
    const url = `${baseUrl}/check-number`;
    const body = { sessionId, number: numero };
    try {
      dbg('POST check-number → url:', url, 'body:', pretty(body));
      const r = await axios.post(url, body, { timeout: 15000, headers: { 'Content-Type': 'application/json' } });
      dbg('POST check-number ← status:', r.status, 'resp:', pretty(r.data));

      // se espera algo tipo { exists: true/false }
      const exists = (typeof r.data?.exists === 'boolean') ? r.data.exists : (typeof r.data === 'boolean' ? r.data : null);
      return { ok: true, exists, raw: r.data };
    } catch (e) {
      const st = e?.response?.status;
      const data = e?.response?.data;
      warn('POST check-number ERROR status:', st, 'resp:', pretty(data), 'msg:', e?.message);
      return { ok: false, exists: null, error: e?.message || 'error_check', raw: data };
    }
  }

  async function reportExistencia(payload) {
    try {
      await axios.post(cfg.existenciaEndpoint, payload, {
        timeout: 15000,
        headers: { 'Content-Type': 'application/json' }
      });
    } catch (e) {
      logHttpFailure('existencia', e);
    }
  }

  // ==================== PROCESADOR ====================
  async function processCampaignFile(filePath, { modeLabel }) {
    // 🔒 Lock filesystem (anti doble disparo)
    const lock = acquireFileLock(filePath);
    if (!lock.ok) {
      const cid = loadJSON(filePath)?.campana?.id || 0;
      logAction('envio_mensaje', { fase: 'skip_duplicado', archivo: filePath, motivo: `LOCK:${lock.reason}` });
      await postStatus({ action: 'skip_duplicado', id_campana: cid, fase: 'skip_duplicado' });
      dbg('LOCK detectado, skip', filePath, lock.reason);
      return;
    }

    // Evitar dobles ejecuciones en memoria (misma instancia)
    if (IN_FLIGHT.has(filePath)) {
      const cid = loadJSON(filePath)?.campana?.id || 0;
      logAction('envio_mensaje', { fase: 'skip_duplicado', archivo: filePath, motivo: 'IN_FLIGHT' });
      await postStatus({ action: 'skip_duplicado', id_campana: cid, fase: 'skip_duplicado' });
      dbg('IN_FLIGHT detectado, skip', filePath);
      releaseFileLock(filePath);
      return;
    }

    IN_FLIGHT.add(filePath);
    info('Procesando campaña:', path.basename(filePath), 'modo:', modeLabel);

    const startedAt = Date.now();

    try {
      let data = loadJSON(filePath);
      if (!data || !data.campana || !Array.isArray(data.destinatarios)) {
        warn('JSON de campaña inválido, aborta', filePath);
        return;
      }

      // 🔒 Check 0: si está cancelada, no arrancar
      if (String(data.estado_campana || '').toLowerCase() === 'cancelado') {
        info('Campaña en estado "cancelado". No se procesa.', filePath);
        await postStatus({ action: 'cancelado', id_campana: data.campana.id, fase: 'inicio' });
        return;
      }

      const currentState = data.estado_campana || 'pendiente';
      if (currentState === 'en_proceso' || currentState === 'finalizada') {
        dbg('Estado no ejecutable, aborta:', currentState);
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
          return;
        }

        const j = jobs[i];

        // ✅ Dedupe por número (no por id) para evitar dobles envíos si el mismo número aparece con IDs distintos
        const rowId = (j.row && (j.row.id !== undefined && j.row.id !== null)) ? j.row.id : null;
        const key = normalizeNumber(j.to);

        const prog = (latest.progreso && latest.progreso[key]) || data.progreso[key] || {};

        // si ya está enviado o alguien lo está enviando, saltar
        if (prog.sent === true || prog.sending === true) {
          dbg('skip ya procesado', key, { sent: !!prog.sent, sending: !!prog.sending });
          continue;
        }

        // 🔒 Marcar "sending" ANTES de check/envío para evitar carreras (especialmente en diferido)
        try {
          const live0 = loadJSON(filePath) || data;
          live0.progreso = live0.progreso || {};
          const prev0 = live0.progreso[key] || {};
          live0.progreso[key] = {
            ...prev0,
            sending: true,
            sent: false,
            started_at: new Date().toISOString(),
            row_id: rowId,
            numero: j.to
          };
          saveJSON(filePath, live0);
          data = live0;
        } catch (e) {
          warn('No se pudo marcar sending (continúo igual):', e?.message || e);
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

          // limpiar bandera sending (no se envió)
          try {
            const liveSkip = loadJSON(filePath) || data;
            liveSkip.progreso = liveSkip.progreso || {};
            const prevS = liveSkip.progreso[key] || {};
            liveSkip.progreso[key] = {
              ...prevS,
              sending: false,
              sent: false,
              skipped_at: new Date().toISOString(),
              skip_reason: (!check.ok ? 'error_check' : 'no_tiene_whatsapp')
            };
            saveJSON(filePath, liveSkip);
            data = liveSkip;
          } catch (e) {}

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
          const prev = live.progreso[key] || {};
          live.progreso[key] = { ...prev, sent: true, sending: false, at: new Date().toISOString() };
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

          // limpiar bandera sending (falló el envío)
          try {
            const liveErr = loadJSON(filePath) || data;
            liveErr.progreso = liveErr.progreso || {};
            const prevE = liveErr.progreso[key] || {};
            liveErr.progreso[key] = {
              ...prevE,
              sending: false,
              sent: false,
              error_at: new Date().toISOString(),
              error_message: e?.message || 'Error envío',
              http_status: st || null
            };
            saveJSON(filePath, liveErr);
            data = liveErr;
          } catch (e2) {}

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

      // Nota: no hacemos IN_FLIGHT.delete aquí; se hace en finally
    } finally {
      try { IN_FLIGHT.delete(filePath); } catch {}
      try { releaseFileLock(filePath); } catch {}
    }
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
