// whatsapp.js (SIN RESTRICCIONES: TODO entra/sale + LOGS CLARITOS)
// ✅ LOGS SOLO CONSOLA (NO ARCHIVOS)
// ✅ Multimedia ENTRANTE: NO renombra, NO descarga, NO guarda, NO manda a API

const fs = require('fs');
const path = require('path');
const axios = require('axios');
const mime = require('mime-types');
const { exec } = require('child_process');
const qrcode = require('qrcode');
const { Boom } = require('@hapi/boom');

const {
  default: makeWASocket,
  useMultiFileAuthState,
  // downloadMediaMessage, // ❌ ya no se usa (multimedia entrante se ignora)
  fetchLatestBaileysVersion,
  jidNormalizedUser,
} = require('@whiskeysockets/baileys');

const googleTTS = require('google-tts-api');
const ffmpeg = require('@ffmpeg-installer/ffmpeg').path;

const FORCE_QR_IN_TERMINAL = process.env.FORCE_QR_IN_TERMINAL === '1';

// ===================== LOGS SOLO CONSOLA =====================
const LOG_CONSOLE = process.env.LOG_CONSOLE !== '0';

// ✅ SOLO estos tags se imprimen
const LOG_ALLOW = new Set([
  'MSG_IN',

  'API_SEND_IN_TEXT',
  'API_RECV_IN_TEXT',

  'API_SEND_OUT',
  'API_RECV_OUT',

  'BOT_SEND_OK',
  'BOT_SEND_ERROR',

  'AXIOS_REQ',
  'AXIOS_RES',
  'AXIOS_ERR_RESPONSE',
  'AXIOS_ERR_NO_RESPONSE',
  'AXIOS_ERR_SETUP',
  'AXIOS_REQ_SETUP_ERROR',

  'INCOMING_HANDLER_ERROR',
  'API_OUT_ERROR',

  'UNHANDLED_REJECTION',
  'UNCAUGHT_EXCEPTION',
]);

function ts() { return new Date().toISOString(); }

function safeStringify(obj, maxLen = 14000) {
  try {
    const seen = new WeakSet();
    let s = JSON.stringify(obj, (k, v) => {
      if (typeof v === 'object' && v !== null) {
        if (seen.has(v)) return '[Circular]';
        seen.add(v);
      }
      if (Buffer.isBuffer(v)) return `[Buffer ${v.length} bytes]`;
      return v;
    }, 2);

    if (s.length > maxLen) {
      const extra = s.length - maxLen;
      s = s.slice(0, maxLen) + `\n... (truncado ${extra} chars)`;
    }
    return s;
  } catch (e) {
    return String(obj);
  }
}

function writeLog(level, tag, data = null) {
  if (!LOG_CONSOLE) return;
  if (!LOG_ALLOW.has(tag)) return;

  const payload = (data !== null)
    ? (typeof data === 'string' ? data : safeStringify(data))
    : '';

  const line = payload
    ? `[${ts()}] [${level}] [${tag}] ${payload}`
    : `[${ts()}] [${level}] [${tag}]`;

  if (level === 'ERROR') console.error(line);
  else console.log(line);
}

function log(tag, data = null) { writeLog('INFO', tag, data); }
function logError(tag, data = null) { writeLog('ERROR', tag, data); }

process.on('unhandledRejection', (reason) => {
  logError('UNHANDLED_REJECTION', { reason: reason?.stack || reason?.message || String(reason) });
});
process.on('uncaughtException', (err) => {
  logError('UNCAUGHT_EXCEPTION', { err: err?.stack || err?.message || String(err) });
});

function pickMsgType(msg) {
  const m = msg?.message || {};
  if (m.conversation) return 'conversation';
  if (m.text) return 'text';
  if (m.extendedTextMessage) return 'extendedTextMessage';
  if (m.imageMessage) return 'imageMessage';
  if (m.documentMessage) return 'documentMessage';
  if (m.audioMessage) return 'audioMessage';
  if (m.videoMessage) return 'videoMessage';
  if (m.stickerMessage) return 'stickerMessage';
  return 'unknown';
}

// ===================== Config inyectable desde server.js =====================
let CFG = {
  url_sistema: 'https://wsp.guibis.com',
  endpoint: '/dev/wspguibis/system_gtp',
  endpoint_salida: '/dev/wspguibis/system_gtp_salientes'
};

function initWhatsapp({ url_sistema, endpoint, endpoint_salida }) {
  if (url_sistema) CFG.url_sistema = url_sistema;
  if (endpoint) CFG.endpoint = endpoint;
  if (endpoint_salida) CFG.endpoint_salida = endpoint_salida;
}

// ===================== AXIOS =====================
const API_TIMEOUT_MS = Number(process.env.API_TIMEOUT_MS || 45000);

const api = axios.create({
  timeout: API_TIMEOUT_MS,
  headers: { 'Content-Type': 'application/json' },
});

api.interceptors.request.use((config) => {
  config.__start = Date.now();
  const url = `${config.baseURL || ''}${config.url || ''}` || '(no-url)';
  log('AXIOS_REQ', {
    method: (config.method || 'GET').toUpperCase(),
    url,
    timeout_ms: config.timeout,
    data: config.data
  });
  return config;
}, (error) => {
  logError('AXIOS_REQ_SETUP_ERROR', { message: error?.message || String(error) });
  return Promise.reject(error);
});

api.interceptors.response.use((response) => {
  const ms = response.config?.__start ? (Date.now() - response.config.__start) : null;
  log('AXIOS_RES', { status: response.status, url: response.config?.url, ms, data: response.data });
  return response;
}, (error) => {
  const cfg = error.config || {};
  const ms = cfg.__start ? (Date.now() - cfg.__start) : null;

  if (error.response) {
    logError('AXIOS_ERR_RESPONSE', {
      url: cfg.url,
      method: (cfg.method || 'GET').toUpperCase(),
      ms,
      status: error.response.status,
      data: error.response.data
    });
  } else if (error.request) {
    logError('AXIOS_ERR_NO_RESPONSE', {
      url: cfg.url,
      method: (cfg.method || 'GET').toUpperCase(),
      ms,
      message: error.message,
      code: error.code
    });
  } else {
    logError('AXIOS_ERR_SETUP', {
      url: cfg.url,
      method: (cfg.method || 'GET').toUpperCase(),
      ms,
      message: error.message
    });
  }

  return Promise.reject(error);
});

// ===================== Estado global =====================
const sessions = {};
const contactsStore = {};
const chatsStore = {};
const reconnectTimers = {};
const reconnectAttempts = {};

// ===================== Utils =====================
async function ensureDir(dir) {
  const abs = path.isAbsolute(dir) ? dir : path.join(__dirname, dir);
  if (!fs.existsSync(abs)) fs.mkdirSync(abs, { recursive: true });
  await fs.promises.access(abs, fs.constants.W_OK | fs.constants.R_OK);
  return abs;
}

function mergeObjects(oldObj, upd) {
  if (!oldObj) return { ...upd };
  return { ...oldObj, ...upd };
}

function clearReconnect(sessionId) {
  if (reconnectTimers[sessionId]) {
    clearTimeout(reconnectTimers[sessionId]);
    delete reconnectTimers[sessionId];
  }
  reconnectAttempts[sessionId] = 0;
}

function scheduleReconnect(sessionId, fn) {
  const attempt = (reconnectAttempts[sessionId] || 0) + 1;
  reconnectAttempts[sessionId] = attempt;
  const delay = Math.min(30000, 1000 * Math.pow(2, attempt));
  clearReconnect(sessionId);
  reconnectTimers[sessionId] = setTimeout(() => {
    delete reconnectTimers[sessionId];
    fn();
  }, delay);
}

// ===================== CORE: crear una sesión =====================
async function createSession(sessionId) {
  const authPath = path.join('./sessions', sessionId);

  await ensureDir('./sessions');
  await ensureDir(authPath);
  await ensureDir('./files'); // ✅ se usa para TTS y descargas por URL del bot
  // ❌ await ensureDir('./logs');  // quitado: ya no usamos logs a archivo

  const { state, saveCreds } = await useMultiFileAuthState(authPath);

  let waVersion;
  try {
    const { version } = await fetchLatestBaileysVersion();
    waVersion = version;
  } catch {
    waVersion = [2, 3000, 0];
  }

  const sock = makeWASocket({
    auth: state,
    version: waVersion,
    printQRInTerminal: FORCE_QR_IN_TERMINAL,
    browser: ['Desktop', 'Chrome', '121'],
    markOnlineOnConnect: false,
    syncFullHistory: true,
  });

  sessions[sessionId] = sock;
  sock.connectionStatus = 'inactiva';
  clearReconnect(sessionId);

  contactsStore[sessionId] = new Map();
  chatsStore[sessionId] = new Map();

  // store
  sock.ev.on('messaging-history.set', ({ chats, contacts }) => {
    try {
      const cMap = contactsStore[sessionId];
      const chMap = chatsStore[sessionId];
      if (contacts && cMap) {
        for (const c of contacts) {
          if (!c?.id) continue;
          const jid = jidNormalizedUser(c.id);
          const prev = cMap.get(jid);
          cMap.set(jid, mergeObjects(prev, c));
        }
      }
      if (chats && chMap) {
        for (const ch of chats) {
          if (!ch?.id) continue;
          const jid = jidNormalizedUser(ch.id);
          const prev = chMap.get(jid);
          chMap.set(jid, mergeObjects(prev, ch));
        }
      }
    } catch {}
  });

  sock.ev.on('contacts.set', (payload) => {
    try {
      const map = contactsStore[sessionId];
      if (!map) return;
      for (const c of payload?.contacts || []) {
        if (!c?.id) continue;
        const jid = jidNormalizedUser(c.id);
        const prev = map.get(jid);
        map.set(jid, mergeObjects(prev, c));
      }
    } catch {}
  });

  sock.ev.on('contacts.upsert', (arr) => {
    try {
      const map = contactsStore[sessionId];
      if (!map) return;
      for (const c of arr || []) {
        if (!c?.id) continue;
        const jid = jidNormalizedUser(c.id);
        const prev = map.get(jid);
        map.set(jid, mergeObjects(prev, c));
      }
    } catch {}
  });

  sock.ev.on('contacts.update', (arr) => {
    try {
      const map = contactsStore[sessionId];
      if (!map) return;
      for (const u of arr || []) {
        if (!u?.id) continue;
        const jid = jidNormalizedUser(u.id);
        const prev = map.get(jid);
        map.set(jid, mergeObjects(prev, u));
      }
    } catch {}
  });

  sock.ev.on('chats.set', ({ chats }) => {
    try {
      const map = chatsStore[sessionId];
      if (!map) return;
      for (const ch of chats || []) {
        if (!ch?.id) continue;
        const jid = jidNormalizedUser(ch.id);
        const prev = map.get(jid);
        map.set(jid, mergeObjects(prev, ch));
      }
    } catch {}
  });

  sock.ev.on('chats.upsert', (chats) => {
    try {
      const map = chatsStore[sessionId];
      if (!map) return;
      for (const ch of chats || []) {
        if (!ch?.id) continue;
        const jid = jidNormalizedUser(ch.id);
        const prev = map.get(jid);
        map.set(jid, mergeObjects(prev, ch));
      }
    } catch {}
  });

  sock.ev.on('chats.update', (updates) => {
    try {
      const map = chatsStore[sessionId];
      if (!map) return;
      for (const u of updates || []) {
        if (!u?.id) continue;
        const jid = jidNormalizedUser(u.id);
        const prev = map.get(jid);
        map.set(jid, mergeObjects(prev, u));
      }
    } catch {}
  });

  // conexión/QR/reconexión
  sock.ev.on('connection.update', async (update) => {
    try {
      const { connection, qr, lastDisconnect } = update || {};

      if (qr) {
        const qrDir = await ensureDir(path.join(__dirname, 'qrcodes'));
        const qrPngPath = path.join(qrDir, `${sessionId}.png`);
        const qrTxtPath = path.join(qrDir, `${sessionId}.txt`);
        try { fs.writeFileSync(qrTxtPath, qr, 'utf8'); } catch {}
        try { await qrcode.toFile(qrPngPath, qr, { errorCorrectionLevel: 'M', margin: 2 }); } catch {}
        if (FORCE_QR_IN_TERMINAL) {
          try { console.log(await qrcode.toString(qr, { type: 'terminal' })); } catch {}
        }
      }

      if (connection === 'open') {
        sock.connectionStatus = 'activa';
        clearReconnect(sessionId);
      } else if (connection === 'close') {
        sock.connectionStatus = 'inactiva';

        const err = lastDisconnect?.error;
        const isBoom = err instanceof Boom;
        const sc = isBoom ? err.output?.statusCode : undefined;

        if (sc === 401) {
          try { await sock.logout(); } catch {}
          return;
        }

        if (sessions[sessionId] !== sock) return;

        scheduleReconnect(sessionId, async () => {
          if (sessions[sessionId] === sock) {
            try { await createSession(sessionId); } catch {}
          }
        });
      }
    } catch {}
  });

  // ===================== MENSAJES =====================
  sock.ev.on('messages.upsert', async (m) => {
    const msg = m?.messages?.[0];
    if (!msg || !msg.message) return;

    const remoteJid = msg?.key?.remoteJid || '';
    const fromMe = !!msg?.key?.fromMe;
    const msgType = pickMsgType(msg);
    const msgId = msg?.key?.id || null;

    // ============ ENTRANTE ============
    if (!fromMe) {
      let messageContent = '';

      // TEXTO
      if (msg.message.conversation) messageContent = msg.message.conversation;
      else if (msg.message.text) messageContent = msg.message.text;
      else if (msg.message.extendedTextMessage) messageContent = msg.message.extendedTextMessage.text;

      // MULTIMEDIA (IGNORAR COMPLETAMENTE: no renombra, no guarda, no API)
      const isIncomingMedia =
        !!msg.message.imageMessage ||
        !!msg.message.documentMessage ||
        !!msg.message.audioMessage ||
        !!msg.message.videoMessage ||
        !!msg.message.stickerMessage;

      if (isIncomingMedia) {
        // Solo para log de ingreso (como tu querías “sin perder estructura”)
        const msg_preview = `[${msgType}] ignorado (multimedia entrante)`;
        log('MSG_IN', { sessionId, msgId, remoteJid, msgType, msg_preview });
        return;
      }

      const msg_in = String(messageContent || '');
      const msg_preview = msg_in.length > 300 ? (msg_in.slice(0, 300) + '...') : msg_in;

      // LOG ingreso SIEMPRE
      log('MSG_IN', { sessionId, msgId, remoteJid, msgType, msg_preview });

      // Responder al mismo JID exacto
      const replyJid = remoteJid;

      try {
        const url = `${CFG.url_sistema}${CFG.endpoint}`;

        // TEXT (único que va a API)
        const payload = {
          sessionId,
          from: remoteJid,
          messageContent,
          user: 'usuario',
          msgType,
          msgId,
        };

        log('API_SEND_IN_TEXT', { sessionId, url, payload });

        const t0 = Date.now();
        const response = await api.post(url, payload);
        const ms = Date.now() - t0;

        log('API_RECV_IN_TEXT', { sessionId, ms, status: response.status, data: response.data });

        await handleBotReplyAndMedia(sock, replyJid, response.data);

      } catch (e) {
        logError('INCOMING_HANDLER_ERROR', {
          sessionId,
          remoteJid,
          msgType,
          msgId,
          err: e?.stack || e?.message || String(e)
        });
      }
    }

    // ============ SALIENTE (TU MENSAJE) ============
    if (fromMe) {
      let messageContent = '';
      if (msg.message.conversation) messageContent = msg.message.conversation;
      else if (msg.message.text) messageContent = msg.message.text;
      else if (msg.message.extendedTextMessage) messageContent = msg.message.extendedTextMessage.text;
      else if (msg.message.imageMessage) messageContent = '[Imagen enviada]';
      else if (msg.message.documentMessage) messageContent = '[Documento enviado]';

      const payload = { sessionId, from: remoteJid, messageContent, user: 'usuario', msgType, msgId };

      try {
        const url = `${CFG.url_sistema}${CFG.endpoint_salida}`;
        log('API_SEND_OUT', { sessionId, url, payload });

        const t0 = Date.now();
        const res = await api.post(url, payload);
        const ms = Date.now() - t0;

        log('API_RECV_OUT', { sessionId, ms, status: res.status, data: res.data });
      } catch (e) {
        logError('API_OUT_ERROR', { sessionId, remoteJid, err: e?.stack || e?.message || String(e) });
      }
    }
  });

  sock.ev.on('creds.update', async () => {
    try { await saveCreds(); } catch {}
  });

  return sock;
}

// ===================== Respuesta del bot: URLs + texto o voz =====================
async function handleBotReplyAndMedia(sock, toJid, botData) {
  // ✅ SIN FILTROS: intentamos enviar siempre

  if (!botData || botData.fuente !== 'bot_interno') return;

  const mensaje = botData.mensaje || '';
  if (!mensaje.trim()) return;

  const urlRegex = /(https?:\/\/[^\s]+\.(jpg|jpeg|png|gif|pdf|mp4|docx|xlsx|zip|xml))/ig;
  const urlMatches = mensaje.match(urlRegex);
  const textWithoutUrls = mensaje.replace(urlRegex, '').trim();

  let textUsedAsCaption = false;

  // Archivos detectados en texto
  if (urlMatches && urlMatches.length) {
    const filesDir = await ensureDir(path.join(__dirname, 'files'));

    for (const fileUrl of urlMatches) {
      try {
        const fileName = path.basename(fileUrl.split('?')[0]);
        const filePath = path.join(filesDir, fileName);

        if (!fs.existsSync(filePath)) {
          const response = await api({ url: fileUrl, method: 'GET', responseType: 'stream' });
          const writer = fs.createWriteStream(filePath);
          response.data.pipe(writer);

          await new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
          });
        }

        const fileBuffer = fs.readFileSync(filePath);
        const mimeType = mime.lookup(filePath) || 'application/octet-stream';

        if (mimeType.startsWith('image/')) {
          await sock.sendMessage(toJid, { image: fileBuffer, caption: !textUsedAsCaption && textWithoutUrls ? textWithoutUrls : null });
          textUsedAsCaption = true;
          log('BOT_SEND_OK', { sessionId: botData?.sessionId, toJid, kind: 'image' });
        } else if (mimeType.startsWith('video/')) {
          await sock.sendMessage(toJid, { video: fileBuffer, caption: !textUsedAsCaption && textWithoutUrls ? textWithoutUrls : null });
          textUsedAsCaption = true;
          log('BOT_SEND_OK', { sessionId: botData?.sessionId, toJid, kind: 'video' });
        } else {
          await sock.sendMessage(toJid, { document: fileBuffer, mimetype: mimeType, fileName });
          log('BOT_SEND_OK', { sessionId: botData?.sessionId, toJid, kind: 'document', fileName });
        }
      } catch (e) {
        logError('BOT_SEND_ERROR', { sessionId: botData?.sessionId, toJid, err: e?.stack || e?.message || String(e) });
      }
    }
  }

  // Respuesta texto
  if (botData.tipo_salida_voz_texto === 'Texto') {
    if (!textUsedAsCaption && textWithoutUrls) {
      try {
        await sock.sendMessage(toJid, { text: textWithoutUrls });
        log('BOT_SEND_OK', { sessionId: botData?.sessionId, toJid, kind: 'text' });
      } catch (e) {
        logError('BOT_SEND_ERROR', { sessionId: botData?.sessionId, toJid, err: e?.stack || e?.message || String(e) });
      }
    }
  }
  // Respuesta voz
  else if (botData.tipo_salida_voz_texto === 'Voz') {
    try {
      const voiceLang = 'es-ES';
      const ttsText = textWithoutUrls || ' ';
      const audioUrl = googleTTS.getAudioUrl(ttsText, {
        lang: voiceLang, slow: false, host: 'https://translate.google.com'
      });

      const mp3Path = path.join(__dirname, 'files', `tts_${Date.now()}.mp3`);
      const opusPath = path.join(__dirname, 'files', `tts_${Date.now()}.opus`);

      const responseAudio = await api.get(audioUrl, { responseType: 'arraybuffer' });
      fs.writeFileSync(mp3Path, responseAudio.data);

      await new Promise((resolve, reject) => {
        exec(`${ffmpeg} -i "${mp3Path}" -c:a libopus -b:a 32k "${opusPath}"`, (error) => {
          if (error) reject(error);
          else resolve();
        });
      });

      await sock.sendMessage(toJid, {
        audio: fs.readFileSync(opusPath),
        mimetype: 'audio/ogg; codecs=opus',
        ptt: true
      });

      log('BOT_SEND_OK', { sessionId: botData?.sessionId, toJid, kind: 'voice' });

      try { fs.unlinkSync(mp3Path); } catch {}
      try { fs.unlinkSync(opusPath); } catch {}
    } catch (e) {
      logError('BOT_SEND_ERROR', { sessionId: botData?.sessionId, toJid, err: e?.stack || e?.message || String(e) });
    }
  }
}

// ===================== Helpers para cierre =====================
async function closeSessionFull(sessionId) {
  const sock = sessions[sessionId];
  if (!sock) return false;

  clearReconnect(sessionId);
  await sock.logout();
  if (sessions[sessionId] === sock) delete sessions[sessionId];

  try { delete contactsStore[sessionId]; } catch {}
  try { delete chatsStore[sessionId]; } catch {}

  const sessionPath = path.join(__dirname, 'sessions', sessionId);
  fs.rm(sessionPath, { recursive: true, force: true }, () => {});
  return true;
}

function closeSessionPrev(sessionId) {
  const sock = sessions[sessionId];
  if (!sock) return false;

  clearReconnect(sessionId);
  try { sock.end?.(true); } catch {}
  if (sessions[sessionId] === sock) delete sessions[sessionId];

  try { delete contactsStore[sessionId]; } catch {}
  try { delete chatsStore[sessionId]; } catch {}

  const qrCodePath = path.join(__dirname, 'qrcodes', `${sessionId}.png`);
  if (fs.existsSync(qrCodePath)) fs.unlinkSync(qrCodePath);

  const sessionDir = path.join(__dirname, 'sessions', sessionId);
  if (fs.existsSync(sessionDir)) fs.rmSync(sessionDir, { recursive: true, force: true });

  return true;
}

function closeAllSessions() {
  for (const id of Object.keys(sessions)) {
    try { sessions[id]?.end?.(true); } catch {}
    delete sessions[id];
    try { delete contactsStore[id]; } catch {}
    try { delete chatsStore[id]; } catch {}
    const qr = path.join(__dirname, 'qrcodes', `${id}.png`);
    if (fs.existsSync(qr)) fs.unlinkSync(qr);
  }
  return true;
}

// ===================== Cargar sesiones existentes al arrancar =====================
async function loadExistingSessions() {
  const sessionsRoot = await ensureDir('./sessions');
  await ensureDir('./files');
  // ❌ await ensureDir('./logs'); // quitado

  const sessionDirs = fs.readdirSync(sessionsRoot)
    .filter(f => fs.statSync(path.join(sessionsRoot, f)).isDirectory());

  for (const sessionId of sessionDirs) {
    const sessionDirPath = path.join(sessionsRoot, sessionId);
    if (!fs.existsSync(sessionDirPath)) continue;

    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
      try {
        await createSession(sessionId);
        sessions[sessionId].connectionStatus = 'activa';
        break;
      } catch (e) {
        attempts++;
        if (attempts === maxAttempts && sessions[sessionId]) sessions[sessionId].connectionStatus = 'inactiva';
      }
    }
  }
}

module.exports = {
  initWhatsapp,
  createSession,
  sessions,
  loadExistingSessions,
  closeSessionFull,
  closeSessionPrev,
  closeAllSessions,
};
