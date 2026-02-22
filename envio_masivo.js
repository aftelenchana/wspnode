'use strict';

const core = require('./envio_masivo.core');
const makeEnvioMasivoServices = require('./envio_masivo.services');

module.exports = function registerEnvioMasivo(app, opts = {}) {
  const {
    fs,
    path,
    DEFAULTS,
    SCHEDULES,
    IN_FLIGHT,
    dbg,
    info,
    warn,
    error,
    pretty,
    logAction,
    logServerClock,
    logScheduleDetail,
    appendRuntimeLog,
    ensureDir,
    sleep,
    toMs,
    parseLocalDateTime,
    loadJSON,
    saveJSON,
    extractPayload,
    acquireFileLock,
    releaseFileLock,
    normalizeNumber
  } = core;

  // 🟢 Desde server.js
  const urlSistemaBase = opts.urlSistema;

  const cfg = {
    ...DEFAULTS,
    ...opts,
    statusEndpoint: `${urlSistemaBase}/dev/api/estado`,
    varsApiEndpoint: `${urlSistemaBase}/dev/api/variables_globales`, // POST
    existenciaEndpoint: `${urlSistemaBase}/dev/api/existencia`,      // ✅ POST JSON (NO form)
  };

  const CAMPANAS_DIR = cfg.baseDir;
  const RT_DIR  = path.join(CAMPANAS_DIR, 'en_tiempo_real');
  const DF_DIR  = path.join(CAMPANAS_DIR, 'diferido');

  const services = makeEnvioMasivoServices({ cfg, core });
  const {
    postStatus,
    prepareTokenReplacer,
    makeParamReplacerFromParts,
    preflightSessionOrError,
    checkWhatsAppNumber,
    reportExistencia
  } = services;

  // Dump de arranque
  info('INIT envio_masivo');
  logServerClock('BOOT');
  dbg('cfg:', cfg);
  dbg('CAMPANAS_DIR:', CAMPANAS_DIR);
  dbg('RT_DIR:', RT_DIR);
  dbg('DF_DIR:', DF_DIR);

  ensureDir(CAMPANAS_DIR, 'CAMPANAS_DIR');
  ensureDir(RT_DIR, 'RT_DIR');
  ensureDir(DF_DIR, 'DF_DIR');

  // Programar diferidos presentes al boot
  loadAndScheduleDiferidosOnBoot();
  // Procesar en_tiempo_real pendientes (si el servidor se reinició y quedaron JSONs)
  loadAndTriggerRealtimeOnBoot();

  // Guardia única
  if (!global.__ENVIO_MASIVO_GUARD__) {
    global.__ENVIO_MASIVO_GUARD__ = setInterval(guardScanDiferido, cfg.guardIntervalMs);
    logAction('inicio', { nota: 'Guardia diferido activado', intervalo_ms: cfg.guardIntervalMs });
    info('Guardia diferido activado cada', cfg.guardIntervalMs, 'ms');
  } else {
    dbg('Guardia ya estaba activa; no se duplica.');
  }

  // Guardia para en_tiempo_real (por si el trigger inmediato no corre en tu proceso / cluster)
  if (!global.__ENVIO_MASIVO_RT_GUARD__) {
    global.__ENVIO_MASIVO_RT_GUARD__ = setInterval(guardScanRealtime, cfg.rtGuardIntervalMs);
    logAction('inicio', { nota: 'Guardia realtime activado', intervalo_ms: cfg.rtGuardIntervalMs });
    info('Guardia realtime activado cada', cfg.rtGuardIntervalMs, 'ms');
  } else {
    dbg('Guardia realtime ya estaba activa; no se duplica.');
  }

  // ==================== SCHEDULING ====================
  function campaignPathByMode(modo_tiempo, id) {
    const folder = (String(modo_tiempo || '').toLowerCase() === 'en_tiempo_real') ? RT_DIR : DF_DIR;
    return path.join(folder, `${id}.json`);
  }

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
    logScheduleDetail('scheduleDiferido', data.campana.fecha_hora_envio, when);
    appendRuntimeLog(CAMPANAS_DIR, { tag: 'SCHEDULE_DIFERIDO', file: filePath, when_iso: when.toISOString(), fecha_hora_envio: data.campana.fecha_hora_envio });
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
      solo_programacion: true,
      server_now: new Date().toISOString()
    });

    info('Diferido programado:', path.basename(filePath), 'para', when.toISOString(), '| faltan:', core.msToHuman(delay));
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

  function loadAndTriggerRealtimeOnBoot() {
    try {
      logServerClock('BOOT:loadAndTriggerRealtimeOnBoot');
      const files = fs.readdirSync(RT_DIR).filter(f => f.endsWith('.json'));
      info('Realtime pendientes al boot:', files.length);
      for (const f of files) {
        const p = path.join(RT_DIR, f);
        const data = loadJSON(p);
        if (!data || !data.campana) continue;

        const estado = String(data.estado_campana || 'pendiente').toLowerCase();
        if (estado === 'en_proceso' || estado === 'finalizada' || estado === 'cancelado') {
          dbg('Realtime boot: skip por estado', estado, p);
          continue;
        }

        // En realtime SIEMPRE disparamos inmediato (ignoramos fecha_hora_envio)
        info('Realtime boot: disparo inmediato', { file: path.basename(p), id: data.campana.id, server_now: new Date().toISOString() });
        processCampaignFile(p, { modeLabel: 'en_tiempo_real' }).catch(error);
      }
    } catch (e) {
      error('Error en loadAndTriggerRealtimeOnBoot:', e?.message || e);
    }
  }

  // ==================== ENDPOINTS ====================
  app.post('/masivo/preview', async (req, res) => {
    logServerClock('HTTP /masivo/preview:RECIBIDO');
    appendRuntimeLog(CAMPANAS_DIR, { tag: 'HTTP_PREVIEW_RECIBIDO', ip: req.ip, ua: req.headers?.['user-agent'] || null });
    dbg('HTTP headers:', pretty(req.headers, 1200));

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

      info('PREVIEW payload:', {
        id,
        modo,
        destinatarios: destinatarios.length,
        fecha_hora_envio: campana.fecha_hora_envio || null,
        intervalo_tiempo: campana.intervalo_tiempo || null
      });

      if (modo === 'diferido') {
        const when = parseLocalDateTime(campana.fecha_hora_envio);
        logScheduleDetail('HTTP /masivo/preview (diferido)', campana.fecha_hora_envio, when);
      } else {
        info('HTTP /masivo/preview (realtime) → arranca inmediatamente. server_now=', new Date().toISOString());
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

      const filePath = campaignPathByMode(modo, id);

      const snapshot = {
        ...payload,
        estado_campana: 'pendiente',
        progreso: payload.progreso && typeof payload.progreso === 'object' ? payload.progreso : {},
        created_at: new Date().toISOString(),
        debug_server_received_at: new Date().toISOString()
      };
      saveJSON(filePath, snapshot);

      logAction('inicio', {
        id_campana: campana.id,
        archivo: filePath,
        modo: modo,
        total_destinatarios: destinatarios.length,
        preview_only: true,
        server_now: new Date().toISOString()
      });

      // 🔥 Si es realtime: arranca ya
      if (modo === 'en_tiempo_real') {
        info('Realtime: processCampaignFile() disparado YA:', { filePath, server_now: new Date().toISOString() });
        appendRuntimeLog(CAMPANAS_DIR, { tag: 'REALTIME_TRIGGER_DIRECT', file: filePath, id: campana.id, server_now: new Date().toISOString() });
        setImmediate(() => processCampaignFile(filePath, { modeLabel: 'en_tiempo_real' }).catch(error));
      } else {
        scheduleDiferido(filePath);
      }

      res.json({ ok: true, message: 'Preview guardado (y ejecutado si es tiempo real).', filePath });

    } catch (e) {
      error('Error /masivo/preview', e?.message || e);
      res.status(500).json({ ok: false, message: 'Error interno', error: e?.message || String(e) });
    }
  });

  app.post('/masivo/iniciar', async (req, res) => {
    logServerClock('HTTP /masivo/iniciar:RECIBIDO');
    appendRuntimeLog(CAMPANAS_DIR, { tag: 'HTTP_INICIAR_RECIBIDO', ip: req.ip, ua: req.headers?.['user-agent'] || null });
    dbg('HTTP headers:', pretty(req.headers, 1200));

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

      info('INICIAR payload:', {
        id,
        modo,
        destinatarios: payload.destinatarios.length,
        fecha_hora_envio: campana.fecha_hora_envio || null,
        intervalo_tiempo: campana.intervalo_tiempo || null
      });

      if (modo === 'diferido') {
        const when = parseLocalDateTime(campana.fecha_hora_envio);
        logScheduleDetail('HTTP /masivo/iniciar (diferido)', campana.fecha_hora_envio, when);
      } else {
        info('HTTP /masivo/iniciar (realtime) → arranca inmediatamente. server_now=', new Date().toISOString());
      }

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
        created_at: new Date().toISOString(),
        debug_server_received_at: new Date().toISOString()
      };
      saveJSON(filePath, snapshot);

      logAction('inicio', {
        id_campana: campana.id,
        archivo: filePath,
        modo: modo,
        total_destinatarios: payload.destinatarios.length,
        iniciar: true,
        server_now: new Date().toISOString()
      });

      if (modo === 'en_tiempo_real') {
        info('Realtime: processCampaignFile() disparado YA:', { filePath, server_now: new Date().toISOString() });
        appendRuntimeLog(CAMPANAS_DIR, { tag: 'REALTIME_TRIGGER_DIRECT', file: filePath, id: campana.id, server_now: new Date().toISOString() });
        setImmediate(() => processCampaignFile(filePath, { modeLabel: 'en_tiempo_real' }).catch(error));
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
    logServerClock('HTTP /masivo/cancelar:RECIBIDO');

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

      if (SCHEDULES.has(filePath)) {
        try { clearTimeout(SCHEDULES.get(filePath)); } catch {}
        SCHEDULES.delete(filePath);
      }

      logAction('cancelar', { id_campana: data.campana.id, archivo: filePath, modo, server_now: new Date().toISOString() });

      res.json({ ok: true, message: 'Campaña cancelada', filePath });
    } catch (e) {
      error('Error /masivo/cancelar', e?.message || e);
      res.status(500).json({ ok: false, message: 'Error interno', error: e?.message || String(e) });
    }
  });

  // ==================== PROCESADOR ====================
  async function processCampaignFile(filePath, { modeLabel }) {
    logServerClock('processCampaignFile:ENTER', { file: filePath, modeLabel });
    appendRuntimeLog(CAMPANAS_DIR, { tag: 'PROCESS_ENTER', file: filePath, modeLabel });

    // 🔒 Lock filesystem
    const lock = acquireFileLock(filePath);
    if (!lock.ok) {
      const cid = loadJSON(filePath)?.campana?.id || 0;
      logAction('envio_mensaje', { fase: 'skip_duplicado', archivo: filePath, motivo: `LOCK:${lock.reason}`, server_now: new Date().toISOString() });
      await postStatus({ action: 'skip_duplicado', id_campana: cid, fase: 'skip_duplicado' });
      dbg('LOCK detectado, skip', filePath, lock.reason);
      appendRuntimeLog(CAMPANAS_DIR, { tag: 'SKIP_LOCK', file: filePath, reason: lock.reason });
      return;
    }

    // Evitar dobles ejecuciones en memoria
    if (IN_FLIGHT.has(filePath)) {
      const cid = loadJSON(filePath)?.campana?.id || 0;
      logAction('envio_mensaje', { fase: 'skip_duplicado', archivo: filePath, motivo: 'IN_FLIGHT', server_now: new Date().toISOString() });
      await postStatus({ action: 'skip_duplicado', id_campana: cid, fase: 'skip_duplicado' });
      dbg('IN_FLIGHT detectado, skip', filePath);
      appendRuntimeLog(CAMPANAS_DIR, { tag: 'SKIP_IN_FLIGHT', file: filePath });
      releaseFileLock(filePath);
      return;
    }

    IN_FLIGHT.add(filePath);
    info('Procesando campaña:', path.basename(filePath), 'modo:', modeLabel, '| server_now:', new Date().toISOString());

    const startedAt = Date.now();

    try {
      let data = loadJSON(filePath);
      if (!data || !data.campana || !Array.isArray(data.destinatarios)) {
        warn('JSON de campaña inválido, aborta', filePath);
        appendRuntimeLog(CAMPANAS_DIR, { tag: 'ABORT_INVALID_JSON', file: filePath });
        return;
      }

      // 🔒 Check 0: cancelado
      if (String(data.estado_campana || '').toLowerCase() === 'cancelado') {
        info('Campaña en estado "cancelado". No se procesa.', filePath);
        await postStatus({ action: 'cancelado', id_campana: data.campana.id, fase: 'inicio' });
        appendRuntimeLog(CAMPANAS_DIR, { tag: 'ABORT_CANCELLED', file: filePath, id: data.campana.id });
        return;
      }

      const currentState = data.estado_campana || 'pendiente';
      if (currentState === 'en_proceso' || currentState === 'finalizada') {
        dbg('Estado no ejecutable, aborta:', currentState);
        appendRuntimeLog(CAMPANAS_DIR, { tag: 'ABORT_STATE', file: filePath, state: currentState });
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

      info('PROCESS config:', {
        id_campana: campana.id,
        modeLabel,
        baseUrl,
        sendUrl,
        sessionId,
        fecha_hora_envio: campana.fecha_hora_envio || null,
        intervalMs,
        pausaCada,
        pausaMs,
        server_now: new Date().toISOString()
      });

      // Diferido: respeta hora exacta
      if (String(modeLabel).toLowerCase() === 'diferido') {
        const when = parseLocalDateTime(campana.fecha_hora_envio);
        logScheduleDetail('processCampaignFile:diferido_wait', campana.fecha_hora_envio, when);

        const delayToStart = Math.max(0, when.getTime() - Date.now());
        if (delayToStart > 0) {
          info('Esperando hasta hora programada (Guayaquil). Faltan:', core.msToHuman(delayToStart), '| server_now:', new Date().toISOString());
          await sleep(delayToStart);
          info('Desperté del sleep diferido. server_now:', new Date().toISOString(), '| debería ser >= when:', when.toISOString());
        } else {
          warn('Diferido vencido (fecha pasada). Se ejecuta inmediatamente.', {
            fecha_hora_envio: campana.fecha_hora_envio,
            when_iso: when.toISOString(),
            server_now: new Date().toISOString()
          });
        }
      } else {
        // ✅ Realtime: IGNORA fecha_hora_envio y arranca YA
        info('Realtime: no espera. Inicia envío de inmediato. server_now:', new Date().toISOString(), '| fecha_hora_envio_ignorada:', campana.fecha_hora_envio || null);
      }

      // ✅ Tokens variables_globales (MIX: ##NAME## y #ID#)
      const tokenReplacer = await prepareTokenReplacer(campana.mensaje, campana);

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
        fecha_hora_envio: campana.fecha_hora_envio || null,
        server_now: new Date().toISOString()
      });

      await postStatus({ action: 'inicio', id_campana: campana.id });
      data.estado_campana = 'en_proceso';
      saveJSON(filePath, data);

      let errores = 0;

      const jobs = data.destinatarios.map((d) => {
        const parts = String(d.numero || '').split('-'); // ["5939...", "Alex", "mail", ...]
        const toNumber = (parts[0] || '').trim();       // número crudo
        return { row: d, to: toNumber, parts };
      }).filter(j => j.to);

      info('JOBS construidos:', {
        total: jobs.length,
        ejemplo_primero: jobs[0] ? { to: jobs[0].to, parts: jobs[0].parts.slice(0, 4) } : null
      });

      for (let i = 0; i < jobs.length; i++) {
        // 🔒 Check 1: antes de enviar
        const latest = loadJSON(filePath) || data;
        if (String(latest.estado_campana || '').toLowerCase() === 'cancelado') {
          info('Cancelado detectado antes de enviar el siguiente mensaje. Se detiene.', filePath);
          await postStatus({ action: 'cancelado', id_campana: latest.campana.id, fase: 'antes_de_envio' });
          appendRuntimeLog(CAMPANAS_DIR, { tag: 'STOP_CANCELLED_BEFORE_SEND', file: filePath, idx: i });
          return;
        }

        const j = jobs[i];

        // dedupe por número
        const rowId = (j.row && (j.row.id !== undefined && j.row.id !== null)) ? j.row.id : null;
        const key = normalizeNumber(j.to);

        const prog = (latest.progreso && latest.progreso[key]) || data.progreso?.[key] || {};

        if (prog.sent === true || prog.sending === true) {
          dbg('skip ya procesado', key, { sent: !!prog.sent, sending: !!prog.sending });
          continue;
        }

        // marcar sending antes de check/envío
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

        // Mensaje por destinatario
        const messageTemplate = String(campana.mensaje);
        const replaceParams   = makeParamReplacerFromParts(j.parts);
        const withParams      = replaceParams(messageTemplate);

        // ✅ Aquí reemplazamos (1) ##NAME## y (2) #ID#
        const msgForThisRecipient = tokenReplacer.replaceWithRandom(withParams);

        // ==================== ✅ 1) CHECK EXISTENCIA WHATSAPP (usa /check-whatsapp) ====================
        const check = await checkWhatsAppNumber(baseUrl, sessionId, j.to);

        // Reporte SIEMPRE
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
            check_raw: check.raw || null,
            check_used: check.used || null
          }
        });

        // Si NO existe (o no se pudo verificar), NO enviamos (tu regla original)
        if (!check.ok || check.exists !== true) {
          logAction('envio_mensaje', {
            fase: 'skip_no_whatsapp',
            id_campana: campana.id,
            archivo: filePath,
            idx: i,
            destinatario_id: j.row.id ?? key,
            numero: j.to,
            motivo: !check.ok ? 'error_check' : 'no_tiene_whatsapp',
            server_now: new Date().toISOString()
          });

          // limpiar sending
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

          continue;
        }

        // ==================== ✅ 2) ENVIAR MENSAJE ====================
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
          body,
          server_now: new Date().toISOString()
        });

        try {
          const resp = await require('axios').post(sendUrl, body, {
            timeout: 30000,
            headers: { 'Content-Type': 'application/json' }
          });

          dbg('POST send-message ← status:', resp.status, 'resp:', pretty(resp.data));

          // Reporte: enviado_ok
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
            http_status: resp.status,
            server_now: new Date().toISOString()
          });

          await postStatus({
            action: 'envio_mensaje',
            id_campana: campana.id,
            destinatario_id: j.row.id,
            fase: 'exito'
          });

          // progreso
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

          // Reporte: error_envio
          await reportExistencia({
            tipo: 'send',
            sessionId,
            numero: j.to,
            exists: true,
            sent: false,
            motivo: 'error_envio',
            extra: { http_status: st || null, error: e?.message || 'Error envío', resp: dd || null }
          });

          // limpiar sending
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
            error_body: typeof dd === 'string' ? dd.slice(0, 400) : dd,
            server_now: new Date().toISOString()
          });

          await postStatus({
            action: 'envio_mensaje',
            id_campana: campana.id,
            destinatario_id: j.row.id,
            fase: 'error'
          });
        }

        // Intervalo
        if (i + 1 < jobs.length && intervalMs > 0) {
          info('Pausa intervalo antes de siguiente:', { ms: intervalMs, human: core.msToHuman(intervalMs) });
          await sleep(intervalMs);
          logAction('envio_mensaje', {
            fase: 'pausa_intervalo',
            id_campana: campana.id,
            archivo: filePath,
            ms: intervalMs,
            next_index: i + 1,
            server_now: new Date().toISOString()
          });
          await postStatus({ action: 'pausa_intervalo', id_campana: campana.id, fase: 'pausa_intervalo' });
        }

        // Pausa por lote
        if (i + 1 < jobs.length && pausaCada > 0 && pausaMs > 0 && (i + 1) % pausaCada === 0) {
          info('Pausa por lote:', { despues_de: i + 1, ms: pausaMs, human: core.msToHuman(pausaMs) });
          await sleep(pausaMs);
          logAction('envio_mensaje', {
            fase: 'pausa_lote',
            id_campana: campana.id,
            archivo: filePath,
            ms: pausaMs,
            despues_de: i + 1,
            server_now: new Date().toISOString()
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
        delete_error: deleteError,
        server_now: new Date().toISOString()
      });

      info('Finalizada campaña:', campana.id, 'enviados:', enviados, 'errores:', errores, 'ms:', durationMs, '| server_now:', new Date().toISOString());

    } finally {
      try { IN_FLIGHT.delete(filePath); } catch {}
      try { releaseFileLock(filePath); } catch {}
      logServerClock('processCampaignFile:EXIT', { file: filePath, modeLabel });
      appendRuntimeLog(CAMPANAS_DIR, { tag: 'PROCESS_EXIT', file: filePath, modeLabel });
    }
  }

  // ==================== GUARDIA REALTIME ====================
  async function guardScanRealtime() {
    try {
      const files = fs.readdirSync(RT_DIR).filter(f => f.endsWith('.json'));
      if (files.length) {
        logServerClock('RT_GUARD:tick', { count: files.length });
      } else {
        dbg('RT_GUARD:tick', { count: 0 });
      }

      for (const f of files) {
        const p = path.join(RT_DIR, f);
        const data = loadJSON(p);
        if (!data || !data.campana) {
          dbg('RT_GUARD: JSON inválido', p);
          continue;
        }

        const estado = String(data.estado_campana || 'pendiente').toLowerCase();
        if (estado === 'en_proceso' || estado === 'finalizada' || estado === 'cancelado') {
          dbg('RT_GUARD: skip por estado', estado, p);
          continue;
        }

        // En realtime ignoramos fecha_hora_envio
        const whenStr = data.campana.fecha_hora_envio || null;
        if (whenStr) {
          const when = parseLocalDateTime(whenStr);
          logScheduleDetail('RT_GUARD:realtime_ignora_fecha', whenStr, when);
        }

        if (IN_FLIGHT.has(p)) {
          dbg('RT_GUARD: ya en IN_FLIGHT', path.basename(p));
          continue;
        }

        info('RT_GUARD: disparo realtime', { file: path.basename(p), id: data.campana.id, server_now: new Date().toISOString() });
        processCampaignFile(p, { modeLabel: 'en_tiempo_real' }).catch(error);
      }
    } catch (e) {
      error('RT_GUARD error:', e?.message || e);
    }
  }

  async function guardScanDiferido() {
    logServerClock('GUARD:tick');

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
        const diff = when.getTime() - Date.now();

        info('GUARD:campaña', {
          file: path.basename(p),
          id: data.campana.id,
          fecha_hora_envio: data.campana.fecha_hora_envio,
          when_iso: when.toISOString(),
          due,
          faltan: core.msToHuman(diff)
        });

        if (SCHEDULES.has(p)) {
          if (due && !IN_FLIGHT.has(p)) {
            clearTimeout(SCHEDULES.get(p));
            SCHEDULES.delete(p);
            logAction('envio_mensaje', { fase: 'guard_trigger_immediate', archivo: p, server_now: new Date().toISOString() });
            await postStatus({ action: 'guard_trigger_immediate', id_campana: data.campana.id, fase: 'guard_trigger_immediate' });
            info('Guardia: disparo inmediato', path.basename(p));
            processCampaignFile(p, { modeLabel: 'diferido' }).catch(error);
          }
          continue;
        }

        if (due) {
          if (!IN_FLIGHT.has(p)) {
            logAction('envio_mensaje', { fase: 'guard_trigger_now', archivo: p, server_now: new Date().toISOString() });
            await postStatus({ action: 'guard_trigger_now', id_campana: data.campana.id, fase: 'guard_trigger_now' });
            info('Guardia: disparo NOW', path.basename(p));
            processCampaignFile(p, { modeLabel: 'diferido' }).catch(error);
          }
        } else {
          info('Guardia: programando pendiente', path.basename(p), 'para', when.toISOString(), '| faltan:', core.msToHuman(diff));
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