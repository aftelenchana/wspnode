'use strict';

const fs = require('fs');
const path = require('path');
const { randomInt } = require('crypto'); // aleatoriedad robusta

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
  guardIntervalMs: 15000,
  rtGuardIntervalMs: 4000
};

// Zona horaria fija para Ecuador (Guayaquil): UTC-5
const ECUADOR_TZ_OFFSET_HOURS = -5;

const SCHEDULES = new Map();
const IN_FLIGHT = new Set();

// ==================== LOG HELPERS (NUEVO) ====================
function serverNow() {
  const d = new Date();
  return {
    iso: d.toISOString(),
    local: d.toString(),
    ms: d.getTime()
  };
}

function msToHuman(ms) {
  const v = Math.max(0, Number(ms) || 0);
  const s = Math.floor(v / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}min ${s % 60}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}min`;
}

function logServerClock(tag, extra = {}) {
  const n = serverNow();
  info(`${tag} | server_time_iso=${n.iso} | server_time_local=${n.local} | server_ms=${n.ms}`, extra && Object.keys(extra).length ? `| extra=${pretty(extra, 800)}` : '');
}

// Para logs de programación (diferido)
function logScheduleDetail(tag, fechaStr, whenDate) {
  const n = serverNow();
  const whenMs = whenDate.getTime();
  const diff = whenMs - n.ms;

  if (diff > 0) {
    info(`${tag} | fecha_hora_envio=${fechaStr} | when_iso=${whenDate.toISOString()} | server_now=${n.iso} | faltan=${msToHuman(diff)} | diff_ms=${diff}`);
  } else {
    warn(`${tag} | fecha_hora_envio=${fechaStr} | when_iso=${whenDate.toISOString()} | server_now=${n.iso} | ⚠ vencido (diff=${diff}ms). Se ejecutará inmediato.`, {
      fecha_hora_envio: fechaStr,
      when_iso: whenDate.toISOString(),
      server_now: n.iso,
      diff_ms: diff
    });
  }
}

// Log a archivo (por si PM2/cluster no muestra stdout donde esperas)
function appendRuntimeLog(baseDir, lineObj) {
  try {
    const logFile = path.join(baseDir, 'envio_masivo_runtime.log');
    const line = (typeof lineObj === 'string') ? lineObj : JSON.stringify(lineObj);
    fs.appendFileSync(logFile, `${new Date().toISOString()} ${line}\n`, 'utf8');
  } catch {}
}

// ==================== LOCK por archivo (anti-doble-proceso) ====================
function lockPathFor(filePath) { return `${filePath}.lock`; }

function acquireFileLock(filePath) {
  const lp = lockPathFor(filePath);
  try {
    if (!fs.existsSync(filePath)) return { ok: false, reason: 'no_json' };

    // openSync con 'wx' falla si ya existe
    const fd = fs.openSync(lp, 'wx');
    const meta = { pid: process.pid, at: new Date().toISOString(), file: filePath };
    try { fs.writeFileSync(fd, JSON.stringify(meta, null, 2)); } catch {}
    try { fs.closeSync(fd); } catch {}
    return { ok: true, lockPath: lp };
  } catch (e) {
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

    // caso "una sola key" (a veces PHP manda todo como key)
    const keys = Object.keys(b);
    if (keys.length === 1) {
      const k = keys[0];

      // si value es string y parece json
      if (typeof b[k] === 'string' && (b[k].trim().startsWith('{') || b[k].trim().startsWith('['))) {
        try {
          const parsed = JSON.parse(b[k]);
          dbg('extractPayload: parsed desde único value-string');
          return parsed;
        } catch (e) {}
      }

      // si la key parece json
      if (typeof k === 'string' && (k.trim().startsWith('{') || k.trim().startsWith('['))) {
        try {
          const parsed = JSON.parse(k);
          dbg('extractPayload: parsed desde única key');
          return parsed;
        } catch (e) {}
      }
    }
  }

  // body string raw
  if (typeof b === 'string') {
    try {
      const parsed = JSON.parse(b);
      dbg('extractPayload: body string JSON');
      return parsed;
    } catch (e) {
      dbg('extractPayload: body string no parsea', e.message);
    }
  }

  return null;
}

module.exports = {
  // deps
  fs,
  path,
  randomInt,

  // globals
  DEBUG,
  LOG_PREFIX,
  DEFAULTS,
  ECUADOR_TZ_OFFSET_HOURS,
  SCHEDULES,
  IN_FLIGHT,

  // loggers
  dbg,
  info,
  warn,
  error,
  pretty,
  logAction,

  // helpers
  serverNow,
  msToHuman,
  logServerClock,
  logScheduleDetail,
  appendRuntimeLog,
  ensureDir,
  sleep,
  toMs,
  parseLocalDateTime,
  loadJSON,
  saveJSON,
  logHttpFailure,
  extractPayload,
  acquireFileLock,
  releaseFileLock,
  normalizeNumber
};