// server.js
// === Solo APIs y servidor Express ===

const cors = require('cors');
const express = require('express');
const app = express();
const PORT = process.env.PORT || 50002;

const fs = require('fs');
const path = require('path');
const axios = require('axios');
const mime = require('mime-types');
const { promisify } = require('util');
const stream = require('stream');

// === Tus constantes (SIN CAMBIOS) ===
const url_sistema = 'https://whatsflash.app';
const endpoint = '/dev/wspguibis/system_gtp';
const endpoint_salida = '/dev/wspguibis/system_gtp_salientes';

const url_sistema_node = 'https://wsp.whatsflash.app';
const CHECK_NUMBER = '593998855160'; // FIJO, NO SE PIDE AL CLIENTE

// 1) importar módulos propios
const registerEnvioMasivo = require('./envio_masivo');
const {
  initWhatsapp,
  createSession,
  sessions,
  loadExistingSessions,
  closeSessionFull,
  closeSessionPrev,
  closeAllSessions,
  // === Opción B: store personalizado ===
  getContacts,
  getAllChats,
  getChatByNumber
} = require('./whatsapp');

// Iniciar configuración para whatsapp.js
initWhatsapp({ url_sistema, endpoint, endpoint_salida });

// Middlewares
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));
app.use(cors());

// 2) registrar tu worker de campañas
registerEnvioMasivo(app, {
  urlSistema: url_sistema,
  scanIntervalMs: 10000,
  baseDir: path.join(__dirname, 'campanas')
});

// ===== Helpers locales =====
function timeoutPromise(promise, ms) {
  const timeout = new Promise((_, reject) =>
    setTimeout(() => reject(new Error('Tiempo agotado')), ms)
  );
  return Promise.race([promise, timeout]);
}

// ===== Rutas de sesión =====
app.post('/reset-session-prev', async (req, res) => {
  const { sessionId } = req.body;
  if (!sessionId) return res.status(400).send('El sessionId es requerido.');

  if (sessions[sessionId]) {
    return res.status(400).json({
      success: false,
      message: 'Esta sesión ya está activa o se ha vuelto a activar.',
      sessionId
    });
  }

  await createSession(sessionId);
  res.json({ message: 'Sesión iniciada. Escanea el QR en GET /get-qr/:sessionId' });
});

app.post('/close-session-full', async (req, res) => {
  const { sessionId } = req.body;
  if (!sessionId) return res.status(400).json({ success: false, message: 'El sessionId es requerido.' });

  if (!sessions[sessionId]) {
    return res.status(404).json({ success: false, message: 'La sesión no existe.', sessionId });
  }

  try {
    await closeSessionFull(sessionId);
    return res.json({ success: true, message: `Sesión ${sessionId} cerrada correctamente.` });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ success: false, message: `Error al cerrar la sesión ${sessionId}`, error: e.message });
  }
});

app.get('/get-qr/:sessionId', (req, res) => {
  const { sessionId } = req.params;
  const qrCodePath = path.join(__dirname, 'qrcodes', `${sessionId}.png`);
  if (fs.existsSync(qrCodePath)) return res.sendFile(qrCodePath);
  res.status(404).send('QR no encontrado. Inicia la sesión primero.');
});

app.post('/start-session', async (req, res) => {
  const { sessionId } = req.body;
  if (!sessionId) return res.status(400).json({ success: false, message: 'Falta el sessionId.' });

  const sessionDirPath = path.join(__dirname, 'sessions', sessionId);

  if (sessions[sessionId]) {
    return res.status(400).json({ success: false, message: `La sesión ${sessionId} ya está activa.` });
  }
  if (fs.existsSync(sessionDirPath) && fs.readdirSync(sessionDirPath).length > 0) {
    return res.status(400).json({
      success: false,
      message: `La sesión ${sessionId} ya fue iniciada previamente. Ciérrala antes de reiniciar.`
    });
  }

  try {
    await createSession(sessionId);
    res.json({ success: true, message: `Sesión ${sessionId} iniciada correctamente.` });
  } catch (e) {
    console.error('Error al crear la sesión:', e);
    res.status(500).json({ success: false, message: 'Error al iniciar la sesión.' });
  }
});

app.post('/close-session-prev', (req, res) => {
  const { sessionId } = req.body;
  if (!sessionId) return res.status(400).send('El sessionId es requerido.');
  if (!sessions[sessionId]) return res.status(404).json({ error: 'Sesión no encontrada.' });

  closeSessionPrev(sessionId);
  res.json({ message: `Sesión ${sessionId} cerrada y eliminada correctamente.` });
});

app.post('/close-all-sessions', (req, res) => {
  closeAllSessions();
  res.json({ message: 'Todas las sesiones cerradas y archivos eliminados correctamente.' });
});




app.post('/check-session', async (req, res) => {
  try {
    const { sessionId } = req.body;
    if (!sessionId) {
      return res.status(400).json({ error: 'El sessionId es requerido.' });
    }

    const session = sessions[sessionId];
    if (!session) {
      return res.status(404).json({ error: 'Sesión no encontrada en la memoria.' });
    }

    const sessionDirPath = path.join(__dirname, 'sessions', sessionId);
    if (!fs.existsSync(sessionDirPath)) {
      return res.status(404).json({ error: 'Sesión no encontrada en la carpeta de sesiones.' });
    }

    const filesInSessionDir = fs.readdirSync(sessionDirPath);
    if (!filesInSessionDir.length) {
      return res.status(200).json({
        sessionId,
        status: 'inactiva',
        message: 'La sesión no ha sido completada (no se ha escaneado el QR).'
      });
    }

    // ✅ Si está activa, hacemos la validación extra (check-whatsapp)
    const numberEstatico = '593998855160'; // <-- dato estático (como pediste)

    let validacionExtra = null;
    let validacionExtraError = null;

    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 8000); // 8s timeout

      const resp = await fetch('https://wsp.whatsflash.app/check-whatsapp', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal: controller.signal,
        body: JSON.stringify({
          sessionId,
          number: numberEstatico
        })
      });

      clearTimeout(timeout);

      // Si la API responde algo raro, lo controlamos
      const data = await resp.json().catch(() => null);

      if (resp.ok && data && typeof data.exists === 'boolean') {
        validacionExtra = data.exists; // true/false
      } else {
        validacionExtra = null;
        validacionExtraError = data?.error || `Respuesta inválida de check-whatsapp (status ${resp.status})`;
      }
    } catch (e) {
      validacionExtra = null;
      validacionExtraError = e?.name === 'AbortError'
        ? 'Timeout consultando check-whatsapp'
        : (e?.message || 'Error consultando check-whatsapp');
    }

    // ✅ Respuesta final (mantiene tu formato y agrega validacionExtra)
    const respuesta = {
      sessionId,
      status: 'activa',
      message: 'La sesión está completa.',
      validacionExtra
    };

    // opcional: si quieres ver el porqué falló cuando sea null
    if (validacionExtra === null && validacionExtraError) {
      respuesta.validacionExtraError = validacionExtraError;
    }

    return res.json(respuesta);

  } catch (err) {
    return res.status(500).json({
      error: 'Error interno en /check-session',
      detail: err?.message || String(err)
    });
  }
});


// ===== Envíos y consultas =====
app.post('/send-message', async (req, res) => {
  const { sessionId, to, message } = req.body;
  if (!sessionId || !to || !message) {
    return res.status(400).json({ error: true, message: 'sessionId, to y message son requeridos.' });
  }

  const session = sessions[sessionId];
  if (!session) return res.status(404).json({ error: 'Sesión no encontrada en la memoria.' });

  try {
    const sessionDirPath = path.join(__dirname, 'sessions', sessionId);
    if (!fs.existsSync(sessionDirPath)) return res.status(404).json({ error: 'Sesión no encontrada en la carpeta de sesiones.' });
    const filesInSessionDir = fs.readdirSync(sessionDirPath);
    if (!filesInSessionDir.length) return res.status(400).json({ error: 'La sesión no ha sido completada (no se ha escaneado el QR).' });

    const urlRegex = /(https?:\/\/[^\s]+\.(jpg|jpeg|png|gif|pdf|mp4|docx|xlsx|zip|xml))/ig;
    const urlMatches = message.match(urlRegex);
    const textWithoutUrls = message.replace(urlRegex, '').trim();

    let textUsedAsCaption = false;

    if (urlMatches && urlMatches.length) {
      for (const fileUrl of urlMatches) {
        const fileName = path.basename(fileUrl);
        const filePath = path.join(__dirname, 'files', fileName);

        if (!fs.existsSync(filePath)) {
          const response = await axios({ url: fileUrl, method: 'GET', responseType: 'stream' });
          const writer = fs.createWriteStream(filePath);
          response.data.pipe(writer);
          await new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
          });
          console.log(`Archivo descargado: ${filePath}`);
        } else {
          console.log(`Archivo ya existe: ${filePath}`);
        }

        const fileBuffer = fs.readFileSync(filePath);
        const mimeType = mime.lookup(filePath) || 'application/octet-stream';

        if (mimeType.startsWith('image/')) {
          await session.sendMessage(`${to}@s.whatsapp.net`, {
            image: fileBuffer,
            caption: !textUsedAsCaption && textWithoutUrls ? textWithoutUrls : null
          });
          textUsedAsCaption = true;
        } else if (mimeType.startsWith('video/')) {
          await session.sendMessage(`${to}@s.whatsapp.net`, {
            video: fileBuffer,
            caption: !textUsedAsCaption && textWithoutUrls ? textWithoutUrls : null
          });
          textUsedAsCaption = true;
        } else {
          await session.sendMessage(`${to}@s.whatsapp.net`, {
            document: fileBuffer,
            mimetype: mimeType,
            fileName
          });
        }
      }
    }

    if (!textUsedAsCaption && textWithoutUrls) {
      await session.sendMessage(`${to}@s.whatsapp.net`, { text: textWithoutUrls });
    }

    res.json({ message: 'Mensaje enviado correctamente.' });
  } catch (err) {
    res.status(500).json({ error: true, message: 'Error al enviar el mensaje: ' + err.message });
  }
});

// ====== Store personalizado: contactos y chats (Opción B) ======
app.post('/get-contacts', (req, res) => {
  const { sessionId } = req.body;
  if (!sessionId) return res.status(400).json({ error: 'sessionId requerido' });

  const session = sessions[sessionId];
  if (!session) return res.status(404).json({ error: 'Sesión no encontrada en memoria' });

  const list = getContacts(sessionId) || [];
  return res.json({ ok: true, count: list.length, contacts: list });
});

app.post('/get-all-chats', (req, res) => {
  const { sessionId } = req.body;
  if (!sessionId) return res.status(400).json({ error: 'sessionId requerido' });

  const session = sessions[sessionId];
  if (!session) return res.status(404).json({ error: 'Sesión no encontrada en memoria' });

  const list = getAllChats(sessionId) || [];
  return res.json({ ok: true, count: list.length, chats: list });
});

app.post('/get-chat-by-number', (req, res) => {
  const { sessionId, number } = req.body; // number puede ser 09xxxxxxxx, +593..., o un JID
  if (!sessionId || !number) {
    return res.status(400).json({ error: 'sessionId y number son requeridos' });
  }

  const session = sessions[sessionId];
  if (!session) return res.status(404).json({ error: 'Sesión no encontrada en memoria' });

  const item = getChatByNumber(sessionId, number);
  if (!item) return res.status(404).json({ ok: false, message: 'No se encontró chat/contacto para ese número' });

  return res.json({ ok: true, chat: item });
});

// ===== Archivos utilitarios =====
app.post('/download-file', async (req, res) => {
  const { fileUrl } = req.body;
  if (!fileUrl) return res.status(400).json({ success: false, message: 'Falta la URL del archivo.' });

  try {
    const fileName = path.basename(fileUrl);
    const filePath = path.join(__dirname, 'files', fileName);
    const writer = fs.createWriteStream(filePath);
    const response = await axios({ method: 'get', url: fileUrl, responseType: 'stream' });
    response.data.pipe(writer);
    const finished = promisify(stream.finished);
    await finished(writer);
    return res.json({ success: true, message: 'Archivo descargado con éxito.', fileName });
  } catch (e) {
    console.error('Error al descargar:', e.message);
    return res.status(500).json({ success: false, message: 'Error al descargar el archivo.' });
  }
});

app.post('/check-file', (req, res) => {
  const { fileName } = req.body;
  if (!fileName) return res.status(400).json({ success: false, message: 'Falta el nombre del archivo.' });

  const filePath = path.join(__dirname, 'files', fileName);
  fs.stat(filePath, (err, stats) => {
    if (err) {
      if (err.code === 'ENOENT') return res.status(404).json({ success: false, message: 'El archivo no existe.' });
      console.error(err);
      return res.status(500).json({ success: false, message: 'Error al verificar el archivo.' });
    }
    return res.json({
      success: true,
      message: 'El archivo existe.',
      fileName,
      size: stats.size,
      createdAt: stats.birthtime,
      modifiedAt: stats.mtime
    });
  });
});

app.post('/clear-files', (req, res) => {
  const dirPath = path.join(__dirname, 'files');
  if (!fs.existsSync(dirPath)) return res.status(404).json({ success: false, message: 'La carpeta "files" no existe.' });

  const deleteFolderContents = (folderPath) => {
    const files = fs.readdirSync(folderPath);
    for (const file of files) {
      const fp = path.join(folderPath, file);
      const stat = fs.statSync(fp);
      if (stat.isDirectory()) {
        deleteFolderContents(fp);
        fs.rmdirSync(fp);
      } else {
        fs.unlinkSync(fp);
      }
    }
  };

  try {
    deleteFolderContents(dirPath);
    res.json({ success: true, message: 'Contenido de "files" eliminado.' });
  } catch (e) {
    console.error(e);
    res.status(500).json({ success: false, message: 'Error al limpiar "files".' });
  }
});

app.post('/delete-file', (req, res) => {
  const { fileName } = req.body;
  if (!fileName) return res.status(400).json({ success: false, message: 'Falta el nombre del archivo.' });

  const filePath = path.join(__dirname, 'files', fileName);
  fs.unlink(filePath, (err) => {
    if (err) {
      if (err.code === 'ENOENT') return res.status(404).json({ success: false, message: 'El archivo no existe.' });
      console.error(err);
      return res.status(500).json({ success: false, message: 'Error al eliminar el archivo.' });
    }
    res.json({ success: true, message: 'Archivo eliminado con éxito.' });
  });
}); 

// Cancela una campaña por modo_tiempo + id (cambia estado_campana → "cancelado")
app.post('/campana/estado', (req, res) => {
  try {
    const { modo_tiempo, id } = req.body || {};
    if (!modo_tiempo || !id) {
      return res.status(400).json({ ok: false, message: 'Faltan modo_tiempo e id.' });
    }

    const modo = String(modo_tiempo).toLowerCase();
    if (!['en_tiempo_real', 'diferido'].includes(modo)) {
      return res.status(400).json({ ok: false, message: 'modo_tiempo inválido (en_tiempo_real | diferido).' });
    }

    const baseDir = path.join(process.cwd(), 'campanas');
    const folder  = (modo === 'en_tiempo_real') ? 'en_tiempo_real' : 'diferido';
    const filePath = path.join(baseDir, folder, `${id}.json`);

    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ ok: false, message: 'Campaña no encontrada.' });
    }

    let data;
    try {
      data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    } catch {
      return res.status(422).json({ ok: false, message: 'JSON de campaña inválido.' });
    }

    const actual = String(data.estado_campana || '').toLowerCase();
    if (actual === 'finalizada') {
      return res.status(409).json({ ok: false, message: 'La campaña ya está finalizada.' });
    }

    // Cambiamos a "cancelado"
    data.estado_campana = 'cancelado';
    data.updated_at = new Date().toISOString();
    data.cancelado_at = data.cancelado_at || data.updated_at;

    // Guardado atómico: escribe a .tmp y renombra
    const tmp = filePath + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(data, null, 2), 'utf8');
    fs.renameSync(tmp, filePath);

    return res.json({
      ok: true,
      message: 'Campaña cancelada.',
      resumen: {
        id: data?.campana?.id ?? id,
        modo_tiempo: modo,
        archivo: `/campanas/${folder}/${id}.json`,
        estado_campana: data.estado_campana
      }
    });
  } catch (e) {
    console.error('Error en /campana/estado:', e?.message || e);
    return res.status(500).json({ ok: false, message: 'Error al cancelar la campaña.' });
  }
});


app.post('/check-whatsapp', async (req, res) => {
  try {
    const { sessionId, number } = req.body || {};

    if (!sessionId || !number) {
      return res.status(400).json({
        ok: false,
        message: 'sessionId y number son requeridos'
      });
    }

    // número esperado: 593xxxxxxxxx (solo dígitos)
    if (!/^\d{8,15}$/.test(number)) {
      return res.status(422).json({
        ok: false,
        message: 'Formato inválido. Use solo dígitos, ej: 593998855160'
      });
    }

    const sock = sessions[sessionId];
    if (!sock) {
      return res.status(404).json({
        ok: false,
        message: 'Sesión no encontrada'
      });
    }

    // Verifica que la sesión esté activa (QR escaneado)
    const sessionDirPath = path.join(__dirname, 'sessions', sessionId);
    if (!fs.existsSync(sessionDirPath) || !fs.readdirSync(sessionDirPath).length) {
      return res.status(400).json({
        ok: false,
        message: 'La sesión no está activa (QR no escaneado)'
      });
    }

    const jid = `${number}@s.whatsapp.net`;

    // 🔥 CONSULTA REAL A WHATSAPP
    const result = await sock.onWhatsApp(jid);

    const exists = Array.isArray(result) && result[0]?.exists === true;

    return res.json({
      ok: true,
      number,
      jid,
      exists
    });

  } catch (err) {
    return res.status(500).json({
      ok: false,
      message: 'Error al verificar número',
      error: err?.message || String(err)
    });
  }
});


// ===== Estáticos =====
app.use('/qrcodes', express.static(path.join(__dirname, 'qrcodes')));
app.use('/files', express.static(path.join(__dirname, 'files')));

// ===== Arranque =====
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Servidor ejecutándose en el puerto ${PORT}`);
  loadExistingSessions(); // no-await
  console.log('Iniciando carga de sesiones existentes...');
});
