const cors = require('cors'); // Importa el paquete cors
const express = require('express');
const PORT = process.env.PORT || 3000; // Cambiar 3000 a la variable de entorno
const { default: makeWASocket, useMultiFileAuthState, makeInMemoryStore, DisconnectReason } = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');
const qrcode = require('qrcode');
const fs = require('fs');
const path = require('path');
const axios = require('axios'); // Asegúrate de tener axios instalado
const mime = require('mime-types'); // Para obtener el mime type de forma automática
const { promisify } = require('util'); // Importar promisify
const stream = require('stream');
const { downloadMediaMessage } = require('@whiskeysockets/baileys');
const googleTTS = require('google-tts-api'); // Importa google-tts-api
const ffmpeg = require('@ffmpeg-installer/ffmpeg').path;
const { exec } = require('child_process');


// Crear el store para almacenar contactos (declararlo globalmente)
const store = makeInMemoryStore({});

// Vincular el almacenamiento del store a un archivo (opcional, si quieres persistir datos entre reinicios)
store.readFromFile('./baileys_store.json');

// Guardar el store periódicamente en el archivo
setInterval(() => {
    store.writeToFile('./baileys_store.json');
}, 10_000);

// Definir la carpeta donde se guardarán los archivos
const filesDir = path.join(__dirname, 'files');

// Crear la carpeta si no existe
if (!fs.existsSync(filesDir)) {
    fs.mkdirSync(filesDir, { recursive: true });
}

const app = express();
app.use(cors()); // Habilita CORS para todas las rutas
app.use(express.json());

// Almacenar las sesiones en un objeto
const sessions = {};

// Función para crear una nueva sesión de WhatsApp
async function createSession(sessionId) {
    const { state, saveCreds } = await useMultiFileAuthState(`./sessions/${sessionId}`);

    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        // Vincular el almacenamiento (store) a la sesión
        store,
    });

    // Enlazar los eventos de la sesión con el store
    store.bind(sock.ev);

    sock.connectionStatus = "inactiva"; // Estado por defecto

    sock.ev.on('connection.update', async (update) => {
        const { connection, qr, lastDisconnect } = update;

        if (qr) {
            const qrCodeDir = path.join(__dirname, 'qrcodes');
            const qrCodePath = path.join(qrCodeDir, `${sessionId}.png`);
        
            // Verifica si la carpeta existe
            if (!fs.existsSync(qrCodeDir)) {
                // Si no existe, crea la carpeta
                fs.mkdirSync(qrCodeDir, { recursive: true });
                console.log(`La carpeta '${qrCodeDir}' no existía y ha sido creada.`);
            }
        
            // Genera el código QR y guarda el archivo
            await qrcode.toFile(qrCodePath, qr);
        }

        if (connection === 'open') {
            //console.log(`Conexión abierta para la sesión ${sessionId}`);
            sock.connectionStatus = "activa"; // Actualizar estado a activa
        } else if (connection === 'close') {
             sock.connectionStatus = "inactiva"; // Actualizar estado a inactiva
            const shouldReconnect = (lastDisconnect.error = Boom)?.output?.statusCode !== 401;
            //console.log(`Conexión cerrada para la sesión ${sessionId}. Reintentando...`);
            if (shouldReconnect) {
                await createSession(sessionId);
            }
        }
    });

       // Escuchar mensajes entrantes

       sock.ev.on('messages.upsert', async (m) => {
        const msg = m.messages[0];
        if (!msg.key.fromMe && msg.message) {
            const from = msg.key.remoteJid;
            let messageContent = '';
            let fileName = null; // Inicializar fileName para evitar errores
    
            // Manejar diferentes tipos de mensajes
            if (msg.message.conversation) {
                messageContent = msg.message.conversation;
            } else if (msg.message.text) {
                messageContent = msg.message.text;
            } else if (msg.message.extendedTextMessage) {
                messageContent = msg.message.extendedTextMessage.text;
            } else if (msg.message.imageMessage) {
                fileName = `imagen_${Date.now()}.jpg`;
                messageContent = '[Imagen recibida]';
            } else if (msg.message.documentMessage) {
                fileName = msg.message.documentMessage.fileName || `documento_${Date.now()}.pdf`;
                messageContent = '[Documento recibido]';
            } else if (msg.message.audioMessage) {
                fileName = `audio_${Date.now()}.mp3`;
                messageContent = '[Audio recibido]';
            } else if (msg.message.videoMessage) {
                fileName = `video_${Date.now()}.mp4`;
                messageContent = '[Video recibido]';
            } else if (msg.message.stickerMessage) {
                fileName = `sticker_${Date.now()}.webp`;
                messageContent = '[Sticker recibido]';
            }


          
            
              // Imprimir la información deseada
              console.log(`Mensaje recibido de: ${from}`);
              console.log(`Contenido del mensaje: ${messageContent}`);
              console.log(`Session ID: ${sessionId}`);

            if (!from.includes('@newsletter') && !from.includes('status@broadcast')) {
              try {

                   if (fileName) {
                   
                    const filePath = path.join(filesDir, fileName); // Se define filePath solo si fileName existe

                    try {
                        const buffer = await downloadMediaMessage(msg, 'buffer');
                        fs.writeFileSync(filePath, buffer);
                        console.log(`Archivo guardado: ${filePath}`);
                    } catch (error) {
                        console.error('Error al descargar el archivo:', error);
                    }
                
                        console.log('Enviando nombre del archivo a la API...');
                        const response = await axios.post('https://whatsflash.app/dev/wspguibis/system_gtp', {
                            sessionId: sessionId, // Sustituir con la sesión real
                            from: from,
                            messageContent: fileName,
                            user: "usuario"
                        });
                        console.log('Respuesta de la API:', response.data);

                        if (msg.message.audioMessage) {
                            if (response.data.fuente === "bot_interno") {
                                const mensaje = response.data.mensaje; 
                        
                                // Verificar que el mensaje no esté vacío y que 'from' no contenga '@newsletter'
                                if (mensaje) {
                                        const urlRegex = /(https?:\/\/[^\s]+\.(jpg|jpeg|png|gif|pdf|mp4|docx|xlsx|zip|xml))/ig;
                                        const urlMatches = mensaje.match(urlRegex);
                                        const textWithoutUrls = mensaje.replace(urlRegex, '').trim();
            
                                        let textUsedAsCaption = false; // Indicador para saber si el texto ya se usó como leyenda
            
                                        // Procesar y enviar archivos multimedia si existen URLs en el mensaje
                                        if (urlMatches && urlMatches.length > 0) {
                                            for (let i = 0; i < urlMatches.length; i++) {
                                                const fileUrl = urlMatches[i];
                                                const fileName = path.basename(fileUrl);
                                                const filePath = path.join(__dirname, 'files', fileName);
            
                                                // Verificar si el archivo ya existe
                                                if (!fs.existsSync(filePath)) {
                                                    // Descargar el archivo
                                                    const response = await axios({
                                                        url: fileUrl,
                                                        method: 'GET',
                                                        responseType: 'stream'
                                                    });
            
                                                    // Guardar el archivo en la carpeta 'files'
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
            
                                                // Leer el archivo descargado y convertirlo en un buffer
                                                const fileBuffer = fs.readFileSync(filePath);
            
                                                // Detectar el tipo MIME automáticamente según la extensión del archivo
                                                const mimeType = mime.lookup(filePath) || 'application/octet-stream';
            
                                                if (mimeType.startsWith('image/')) {
                                                    // Enviar imagen
                                                    await sock.sendMessage(from, {
                                                        image: fileBuffer,
                                                        caption: !textUsedAsCaption && textWithoutUrls ? textWithoutUrls : null // Agregar leyenda solo en la primera imagen
                                                    });
                                                    textUsedAsCaption = true; // Marcar que el texto ya se usó
                                                    console.log(`Imagen enviada: ${filePath}`);
                                                } else if (mimeType.startsWith('video/')) {
                                                    // Enviar video
                                                    await sock.sendMessage(from, {
                                                        video: fileBuffer,
                                                        caption: !textUsedAsCaption && textWithoutUrls ? textWithoutUrls : null // Agregar leyenda solo en el primer video
                                                    });
                                                    textUsedAsCaption = true; // Marcar que el texto ya se usó
                                                    console.log(`Video enviado: ${filePath}`);
                                                } else {
                                                    // Enviar como documento para otros tipos de archivos
                                                    await sock.sendMessage(from, {
                                                        document: fileBuffer,
                                                        mimetype: mimeType,
                                                        fileName: fileName,
                                                    });
                                                    console.log(`Archivo multimedia enviado: ${filePath}`);
                                                }
                                            }
                                        }
            
                                        // Enviar el mensaje de texto si no se usó como leyenda y si hay texto sin URLs
                                            // Enviar el mensaje de texto si no se usó como leyenda y si hay texto sin URLs 
                                        if (response.data.tipo_salida_voz_texto === "Texto") {
                                            if (!textUsedAsCaption && textWithoutUrls) {
                                                await sock.sendMessage(from, { text: textWithoutUrls });
                                                console.log(`Mensaje de texto enviado a ${from}: ${textWithoutUrls}`);
                                            }
                                        } else if (response.data.tipo_salida_voz_texto === "Voz") {
                                            let voiceGender = response.data.voz_salida_informacion === "Masculino" ? "es-ES" : "es-ES"; // Puedes cambiar el acento si es necesario
                                            let audioUrl = googleTTS.getAudioUrl(textWithoutUrls, {
                                                lang: voiceGender,
                                                slow: false,
                                                host: 'https://translate.google.com',
                                            });
            
                                            let mp3Path = 'temp.mp3';
                                            let opusPath = 'temp.opus';
            
                                            // Descargar el MP3 de Google TTS
                                            const responseAudio = await axios.get(audioUrl, { responseType: 'arraybuffer' });
                                            fs.writeFileSync(mp3Path, responseAudio.data);
            
                                            // Convertir MP3 a OPUS con FFmpeg
                                            await new Promise((resolve, reject) => {
                                                exec(`${ffmpeg} -i ${mp3Path} -c:a libopus -b:a 32k ${opusPath}`, (error) => {
                                                    if (error) reject(error);
                                                    else resolve();
                                                });
                                            });
            
                                            let audioMessage = {
                                                audio: fs.readFileSync(opusPath),
                                                mimetype: 'audio/ogg; codecs=opus',
                                                ptt: true, // Esto lo convierte en nota de voz
                                            };
            
                                            await sock.sendMessage(from, audioMessage);
                                            console.log(`Mensaje de voz enviado a ${from}`);
            
                                            // Eliminar archivos temporales
                                            fs.unlinkSync(mp3Path);
                                            fs.unlinkSync(opusPath);
                                        }
            
                                        res.json({ message: 'Mensaje enviado correctamente.' });
            
                                } else {
                                    if (!mensaje) {
                                        console.log("Mensaje vacío. No se enviará el mensaje.");
                                    }
                                    if (from.includes('@newsletter')) {
                                        console.log("El destinatario es un newsletter. No se enviará el mensaje.");
                                    }
                                }
                            } else {
                                console.log("Condiciones no cumplidas para enviar el mensaje.");
                                console.log(`Fuente: ${response.data.fuente}, Nivel: ${response.data.nivel}`);
                            }

                        }
                    } else {
                        // Si no es un archivo, enviar el mensaje normal
              
                // Llamar a la API
                console.log('Enviando datos a la API...');
                const response = await axios.post('https://wsp.whatsflash.app/dev/wspguibis/system_gtp', {
                    sessionId: sessionId,
                    from: from,
                    messageContent: messageContent,
                    user: "usuario" // Define el usuario correspondiente
                });
            
                // Imprimir la respuesta de la API
                console.log('Respuesta de la API de conectividad:', response.data);
            
                // Validar si la respuesta cumple con los requisitos
                if (response.data.fuente === "bot_interno") {
                    const mensaje = response.data.mensaje; 
            
                    // Verificar que el mensaje no esté vacío y que 'from' no contenga '@newsletter'
                    if (mensaje) {
                            const urlRegex = /(https?:\/\/[^\s]+\.(jpg|jpeg|png|gif|pdf|mp4|docx|xlsx|zip|xml))/ig;
                            const urlMatches = mensaje.match(urlRegex);
                            const textWithoutUrls = mensaje.replace(urlRegex, '').trim();

                            let textUsedAsCaption = false; // Indicador para saber si el texto ya se usó como leyenda

                            // Procesar y enviar archivos multimedia si existen URLs en el mensaje
                            if (urlMatches && urlMatches.length > 0) {
                                for (let i = 0; i < urlMatches.length; i++) {
                                    const fileUrl = urlMatches[i];
                                    const fileName = path.basename(fileUrl);
                                    const filePath = path.join(__dirname, 'files', fileName);

                                    // Verificar si el archivo ya existe
                                    if (!fs.existsSync(filePath)) {
                                        // Descargar el archivo
                                        const response = await axios({
                                            url: fileUrl,
                                            method: 'GET',
                                            responseType: 'stream'
                                        });

                                        // Guardar el archivo en la carpeta 'files'
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

                                    // Leer el archivo descargado y convertirlo en un buffer
                                    const fileBuffer = fs.readFileSync(filePath);

                                    // Detectar el tipo MIME automáticamente según la extensión del archivo
                                    const mimeType = mime.lookup(filePath) || 'application/octet-stream';

                                    if (mimeType.startsWith('image/')) {
                                        // Enviar imagen
                                        await sock.sendMessage(from, {
                                            image: fileBuffer,
                                            caption: !textUsedAsCaption && textWithoutUrls ? textWithoutUrls : null // Agregar leyenda solo en la primera imagen
                                        });
                                        textUsedAsCaption = true; // Marcar que el texto ya se usó
                                        console.log(`Imagen enviada: ${filePath}`);
                                    } else if (mimeType.startsWith('video/')) {
                                        // Enviar video
                                        await sock.sendMessage(from, {
                                            video: fileBuffer,
                                            caption: !textUsedAsCaption && textWithoutUrls ? textWithoutUrls : null // Agregar leyenda solo en el primer video
                                        });
                                        textUsedAsCaption = true; // Marcar que el texto ya se usó
                                        console.log(`Video enviado: ${filePath}`);
                                    } else {
                                        // Enviar como documento para otros tipos de archivos
                                        await sock.sendMessage(from, {
                                            document: fileBuffer,
                                            mimetype: mimeType,
                                            fileName: fileName,
                                        });
                                        console.log(`Archivo multimedia enviado: ${filePath}`);
                                    }
                                }
                            }

                            // Enviar el mensaje de texto si no se usó como leyenda y si hay texto sin URLs
                                // Enviar el mensaje de texto si no se usó como leyenda y si hay texto sin URLs 
                            if (response.data.tipo_salida_voz_texto === "Texto") {
                                if (!textUsedAsCaption && textWithoutUrls) {
                                    await sock.sendMessage(from, { text: textWithoutUrls });
                                    console.log(`Mensaje de texto enviado a ${from}: ${textWithoutUrls}`);
                                }
                            } else if (response.data.tipo_salida_voz_texto === "Voz") {
                                let voiceGender = response.data.voz_salida_informacion === "Masculino" ? "es-ES" : "es-ES"; // Puedes cambiar el acento si es necesario
                                let audioUrl = googleTTS.getAudioUrl(textWithoutUrls, {
                                    lang: voiceGender,
                                    slow: false,
                                    host: 'https://translate.google.com',
                                });

                                let mp3Path = 'temp.mp3';
                                let opusPath = 'temp.opus';

                                // Descargar el MP3 de Google TTS
                                const responseAudio = await axios.get(audioUrl, { responseType: 'arraybuffer' });
                                fs.writeFileSync(mp3Path, responseAudio.data);

                                // Convertir MP3 a OPUS con FFmpeg
                                await new Promise((resolve, reject) => {
                                    exec(`${ffmpeg} -i ${mp3Path} -c:a libopus -b:a 32k ${opusPath}`, (error) => {
                                        if (error) reject(error);
                                        else resolve();
                                    });
                                });

                                let audioMessage = {
                                    audio: fs.readFileSync(opusPath),
                                    mimetype: 'audio/ogg; codecs=opus',
                                    ptt: true, // Esto lo convierte en nota de voz
                                };

                                await sock.sendMessage(from, audioMessage);
                                console.log(`Mensaje de voz enviado a ${from}`);

                                // Eliminar archivos temporales
                                fs.unlinkSync(mp3Path);
                                fs.unlinkSync(opusPath);
                            }

                            res.json({ message: 'Mensaje enviado correctamente.' });

                    } else {
                        if (!mensaje) {
                            console.log("Mensaje vacío. No se enviará el mensaje.");
                        }
                        if (from.includes('@newsletter')) {
                            console.log("El destinatario es un newsletter. No se enviará el mensaje.");
                        }
                    }
                } else {
                    console.log("Condiciones no cumplidas para enviar el mensaje.");
                    console.log(`Fuente: ${response.data.fuente}, Nivel: ${response.data.nivel}`);
                }
                    }






            } catch (error) {
                console.error('Error al consumir la API o al enviar el mensaje:', error);
                console.error('Detalles del error:', error.response ? error.response.data : error.message);
            }
            
        }else{
            console.log("Números con especificaciones no permitidas");
        }
         
        }

//COLOCAR LOS MENSAJES SALIENTES EN CAMBIO 

if (msg.key.fromMe && msg.message) {
    const to = msg.key.remoteJid; // JID del destinatario
    let messageContent = '';

    // Manejar diferentes tipos de mensajes de salida
    if (msg.message.conversation) {
        messageContent = msg.message.conversation;
    } else if (msg.message.text) {
        messageContent = msg.message.text;
    } else if (msg.message.extendedTextMessage) {
        messageContent = msg.message.extendedTextMessage.text;
    } else if (msg.message.imageMessage) {
        messageContent = '[Imagen enviada]';
    } else if (msg.message.documentMessage) {
        messageContent = '[Documento enviado]';
    }

    // Imprimir la información deseada
    console.log(`Session ID SALIENTE: ${sessionId}`);
    console.log(`Mensaje enviado a: ${to}`);
    console.log(`Contenido del mensaje: ${messageContent}`);

    // Enviar datos a la API para cambiar el estado de activo a inactivo
    console.log('Enviando datos a la API...');

    try {
        const response = await axios.post('https://wsp.whatsflash.app/dev/wspguibis/system_gtp_salientes', {
            sessionId: sessionId,
            from: to,
            messageContent: messageContent,
            user: "usuario" // Define el usuario correspondiente
        });
        
        // Mostrar la respuesta de la API en la consola
        console.log('Respuesta de la API:', response.data);
    } catch (error) {
        console.error('Error al enviar los datos a la API:', error);
    }
}




    });

    sessions[sessionId] = sock;
    sock.ev.on('creds.update', saveCreds);
}

async function checkSession(key) {
    // Comprobar si la clave de sesión existe en el objeto sessions
    const session = Object.values(sessions).find(sock => sock.authState.keys[key]);

    if (session) {
        return { valid: true }; // Si la sesión existe, devuelve que es válida
    } else {
        return { valid: false }; // Si no existe, devuelve que no es válida
    }
}

// Endpoint para iniciar una nueva sesión (POST)
app.post('/reset-session-prev', async (req, res) => {
    const { sessionId } = req.body;

    if (!sessionId) {
        return res.status(400).send('El sessionId es requerido.');
    }

    if (sessions[sessionId]) {
        return res.status(400).json({
            success: false,
            message: 'Esta sesión ya está activao se ha vuelto a activar.',
            sessionId: sessionId
        });
    }

    await createSession(sessionId);
    res.send({ message: 'Sesión iniciada. Escanea el código QR usando el endpoint GET /get-qr/:sessionId.' });
});



// Ruta para cerrar sesión
app.post('/close-session-full', async (req, res) => {
    const { sessionId } = req.body;

    if (!sessionId) {
        return res.status(400).json({
            success: false,
            message: 'El sessionId es requerido.'
        });
    }

    // Verificar si la sesión existe
    if (!sessions[sessionId]) {
        return res.status(404).json({
            success: false,
            message: 'La sesión no existe.',
            sessionId: sessionId
        });
    }

    try {
        const sock = sessions[sessionId];

        // Cerrar la sesión y eliminar del almacenamiento
        await sock.logout();
        delete sessions[sessionId]; 

         // Eliminar la carpeta de sesión
         const sessionPath = path.join(__dirname, 'sessions', sessionId);
         fs.rmdir(sessionPath, { recursive: true }, (err) => {
             if (err) {
                 console.error(`Error al eliminar la carpeta de la sesión ${sessionId}:`, err);
             } else {
                 console.log(`Carpeta de la sesión ${sessionId} eliminada correctamente.`);
             }
         });
 



        console.log(`Sesión ${sessionId} cerrada correctamente.`);
        return res.json({
            success: true,
            message: `Sesión ${sessionId} cerrada correctamente.`
        });

    } catch (error) {
        console.error(`Error cerrando la sesión ${sessionId}:`, error);
        return res.status(500).json({
            success: false,
            message: `Error al cerrar la sesión ${sessionId}`,
            error: error.message
        });
    }
});



// Endpoint para obtener el QR de una sesión (GET)
app.get('/get-qr/:sessionId', (req, res) => {
    const { sessionId } = req.params;
    const qrCodePath = path.join(__dirname, 'qrcodes', `${sessionId}.png`);

    if (fs.existsSync(qrCodePath)) {
        res.sendFile(qrCodePath);
    } else {
        res.status(404).send('QR no encontrado. Asegúrate de iniciar la sesión primero.');
    }
});



// Endpoint para enviar mensajes
// Endpoint para enviar mensajes
app.post('/send-message', async (req, res) => {
    const { sessionId, to, message } = req.body;

    if (!sessionId || !to || !message) {
        return res.status(400).json({
            error: true,
            message: 'sessionId, to y message son requeridos.'
        });
    }

    // Buscar la sesión en el objeto 'sessions'
    const session = sessions[sessionId];

    if (!session) {
        return res.status(404).json({ error: 'Sesión no encontrada en la memoria.' });
    }

    try {
        // Ruta de la carpeta de la sesión
        const sessionDirPath = path.join(__dirname, 'sessions', sessionId);

        // Verificar si la carpeta de la sesión existe
        if (!fs.existsSync(sessionDirPath)) {
            return res.status(404).json({ error: 'Sesión no encontrada en la carpeta de sesiones.' });
        }

        // Leer el contenido de la carpeta
        const filesInSessionDir = fs.readdirSync(sessionDirPath);

        // Verificar si la carpeta está vacía
        if (filesInSessionDir.length === 0) {
            return res.status(400).json({ error: 'La sesión no ha sido completada (no se ha escaneado el QR).' });
        }

        // Expresión regular para detectar URLs de archivos multimedia
        const urlRegex = /(https?:\/\/[^\s]+\.(jpg|jpeg|png|gif|pdf|mp4|docx|xlsx|zip|xml))/ig;
        const urlMatches = message.match(urlRegex);
        const textWithoutUrls = message.replace(urlRegex, '').trim();

        let textUsedAsCaption = false;  // Indicador para saber si el texto ya se usó como leyenda

        // Procesar archivos multimedia
        if (urlMatches && urlMatches.length > 0) {
            for (let i = 0; i < urlMatches.length; i++) {
                const fileUrl = urlMatches[i];
                const fileName = path.basename(fileUrl);
                const filePath = path.join(__dirname, 'files', fileName);

                // Verificar si el archivo ya existe
                if (!fs.existsSync(filePath)) {
                    // Descargar el archivo
                    const response = await axios({
                        url: fileUrl,
                        method: 'GET',
                        responseType: 'stream'
                    });

                    // Guardar el archivo en la carpeta 'files'
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

                // Leer el archivo descargado y convertirlo en un buffer
                const fileBuffer = fs.readFileSync(filePath);

                // Detectar el tipo MIME automáticamente según la extensión del archivo
                const mimeType = mime.lookup(filePath) || 'application/octet-stream';

                if (mimeType.startsWith('image/')) {
                    // Enviar imagen
                    await session.sendMessage(`${to}@s.whatsapp.net`, {
                        image: fileBuffer,
                        caption: !textUsedAsCaption && textWithoutUrls ? textWithoutUrls : null // Agregar leyenda solo en la primera imagen
                    });
                    textUsedAsCaption = true; // Marcar que el texto ya se usó
                    console.log(`Imagen enviada: ${filePath}`);
                } else if (mimeType.startsWith('video/')) {
                    // Enviar video
                    await session.sendMessage(`${to}@s.whatsapp.net`, {
                        video: fileBuffer,
                        caption: !textUsedAsCaption && textWithoutUrls ? textWithoutUrls : null // Agregar leyenda solo en el primer video
                    });
                    textUsedAsCaption = true; // Marcar que el texto ya se usó
                    console.log(`Video enviado: ${filePath}`);
                } else {
                    // Enviar como documento para otros tipos de archivos
                    await session.sendMessage(`${to}@s.whatsapp.net`, {
                        document: fileBuffer,
                        mimetype: mimeType,
                        fileName: fileName,
                    });
                    console.log(`Archivo multimedia enviado: ${filePath}`);
                }
            }
        }

        // Enviar texto si no se usó como leyenda y hay mensaje sin URLs
        if (!textUsedAsCaption && textWithoutUrls) {
            await session.sendMessage(`${to}@s.whatsapp.net`, { text: textWithoutUrls });
            console.log('Mensaje de texto enviado correctamente.');
        }

        res.json({ message: 'Mensaje enviado correctamente.' });
    } catch (err) {
        res.status(500).json({
            error: true,
            message: 'Error al enviar el mensaje: ' + err.message
        });
    }
});


// Endpoint para obtener contactos (POST)
app.post('/get-contacts', async (req, res) => {
    const { sessionId } = req.body;

    if (!sessionId) {
        return res.status(400).send('El sessionId es requerido.');
    }

    const session = sessions[sessionId];

    // Verificar si la sesión existe en la memoria
    if (!session) {
        return res.status(404).json({ error: 'Sesión no encontrada en la memoria.' });
    }

    try {
        // Ruta de la carpeta de la sesión
        const sessionDirPath = path.join(__dirname, 'sessions', sessionId);

        // Verificar si la carpeta de la sesión existe
        if (!fs.existsSync(sessionDirPath)) {
            return res.status(404).json({ error: 'Sesión no encontrada en la carpeta de sesiones.' });
        }

        // Leer el contenido de la carpeta
        const filesInSessionDir = fs.readdirSync(sessionDirPath);

        // Verificar si la carpeta está vacía
        if (filesInSessionDir.length === 0) {
            return res.status(400).json({ error: 'La sesión no ha sido completada (no se ha escaneado el QR).' });
        }

        // Si la carpeta no está vacía, se asume que la sesión está completa
        // Acceder a los contactos desde store.contacts
        const contacts = store.contacts;
        res.json(contacts);

    } catch (err) {
        res.status(500).send('Error al obtener los contactos: ' + err.message);
    }
});


function timeoutPromise(promise, ms) {
    const timeout = new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Tiempo agotado')), ms)
    );
    return Promise.race([promise, timeout]);
}



// Endpoint para obtener grupos
app.post('/get-groups', async (req, res) => {
    const { sessionId } = req.body;

    if (!sessionId) {
        return res.status(400).json({ error: 'El sessionId es requerido.' });
    }

    // Buscar la sesión en el objeto 'sessions'
    const session = sessions[sessionId];

    if (!session) {
        return res.status(404).json({ error: 'Sesión no encontrada en la memoria.' });
    }

    try {
        // Ruta de la carpeta de la sesión
        const sessionDirPath = path.join(__dirname, 'sessions', sessionId);

        // Verificar si la carpeta de la sesión existe
        if (!fs.existsSync(sessionDirPath)) {
            return res.status(404).json({ error: 'Sesión no encontrada en la carpeta de sesiones.' });
        }

        // Leer el contenido de la carpeta
        const filesInSessionDir = fs.readdirSync(sessionDirPath);

        // Verificar si la carpeta está vacía
        if (filesInSessionDir.length === 0) {
            return res.status(400).json({ error: 'La sesión no ha sido completada (no se ha escaneado el QR).' });
        }

        // Limitar la búsqueda de grupos a 8 segundos
        const groups = await timeoutPromise(session.groupFetchAllParticipating(), 8000);

        if (groups && Object.keys(groups).length > 0) {
            res.json(groups); // Si se encuentran grupos, los devuelve
        } else {
            res.status(404).json({ error: 'No se encontraron grupos.' });
        }
    } catch (err) {
        // Manejar el error de tiempo de espera sin mostrarlo en consola
        if (err.message === 'Tiempo agotado') {
            return res.status(408).json({
                status: 'error',
                message: 'Tiempo de espera agotado. No se encontraron grupos en el tiempo permitido.'
            });
        }

        // Para cualquier otro error, envía un mensaje JSON sin imprimir en consola
        res.status(500).json({
            status: 'error',
            message: 'Error al obtener la lista de grupos.'
        });
    }
});




// Endpoint para obtener todas las conversaciones (chats)
app.post('/get-all-chats', async (req, res) => {
    const { sessionId } = req.body;

    if (!sessionId) {
        return res.status(400).send('El sessionId es requerido.');
    }

    // Buscar la sesión en el objeto 'sessions'
    const session = sessions[sessionId];

    if (!session) {
        return res.status(404).json({ error: 'Sesión no encontrada en la memoria.' });
    }

    try {
        // Ruta de la carpeta de la sesión
        const sessionDirPath = path.join(__dirname, 'sessions', sessionId);

        // Verificar si la carpeta de la sesión existe
        if (!fs.existsSync(sessionDirPath)) {
            return res.status(404).json({ error: 'Sesión no encontrada en la carpeta de sesiones.' });
        }

        // Leer el contenido de la carpeta
        const filesInSessionDir = fs.readdirSync(sessionDirPath);

        // Verificar si la carpeta está vacía
        if (filesInSessionDir.length === 0) {
            return res.status(400).json({ error: 'La sesión no ha sido completada (no se ha escaneado el QR).' });
        }

        // Verificar si 'store.chats' está definido y contiene conversaciones
        if (!store.chats || store.chats.all().length === 0) {
            return res.status(404).send('No se encontraron conversaciones.');
        }

        const chats = store.chats.all(); // Obtener todos los chats
        res.json(chats);
    } catch (err) {
        res.status(500).send('Error al obtener las conversaciones: ' + err.message);
    }
});




// Endpoint para obtener la conversación de un número específico
app.post('/get-chat-by-number', async (req, res) => {
    const { sessionId, phoneNumber } = req.body; // El phoneNumber debe incluir el código de país

    // Validar la entrada
    if (!sessionId || !phoneNumber) {
        return res.status(400).send('El sessionId y el phoneNumber son requeridos.');
    }

    const session = sessions[sessionId]; // Obtener la sesión correspondiente

    // Verificar si la sesión existe en memoria
    if (!session) {
        return res.status(404).json({ error: 'Sesión no encontrada en la memoria.' });
    }

    try {
        // Ruta de la carpeta de la sesión
        const sessionDirPath = path.join(__dirname, 'sessions', sessionId);

        // Verificar si la carpeta de la sesión existe
        if (!fs.existsSync(sessionDirPath)) {
            return res.status(404).json({ error: 'Sesión no encontrada en la carpeta de sesiones.' });
        }

        // Leer el contenido de la carpeta
        const filesInSessionDir = fs.readdirSync(sessionDirPath);

        // Verificar si la carpeta está vacía (sesión incompleta)
        if (filesInSessionDir.length === 0) {
            return res.status(400).json({ error: 'La sesión no ha sido completada (no se ha escaneado el QR).' });
        }

        // Verificar si los chats existen en el store
        if (!store.chats) {
            return res.status(404).send('No se encontraron conversaciones.');
        }

        // Crear el JID del número de WhatsApp
        const jid = `${phoneNumber}@s.whatsapp.net`;

        // Obtener el chat de ese número específico
        const chat = store.chats.get(jid);

        // Verificar si hay un chat para ese número
        if (!chat) {
            return res.status(404).send(`No se encontró una conversación con el número ${phoneNumber}.`);
        }

        // Si se encuentra el chat, devolver la información
        res.json(chat);
    } catch (err) {
        // Manejar errores en caso de que ocurra algo
        return res.status(500).json({ error: 'Error al obtener la conversación: ' + err.message });

    }
});

// Endpoint para verificar el estado de la sesión (POST)
app.post('/check-session', (req, res) => {
    const { sessionId } = req.body;

    // Validar que se proporcione un sessionId
    if (!sessionId) {
        return res.status(400).json({ error: 'El sessionId es requerido.' });

    }

    // Buscar la sesión en el objeto 'sessions'
    const session = sessions[sessionId];

    // Comprobar si la sesión existe en memoria
    if (!session) {
        return res.status(404).json({ error: 'Sesión no encontrada en la memoria.' });
    }

    try {
        // Ruta de la carpeta de la sesión
        const sessionDirPath = path.join(__dirname, 'sessions', sessionId);

        // Verificar si la carpeta de la sesión existe
        if (!fs.existsSync(sessionDirPath)) {
            return res.status(404).json({ error: 'Sesión no encontrada en la carpeta de sesiones.' });
        }

        // Leer el contenido de la carpeta
        const filesInSessionDir = fs.readdirSync(sessionDirPath);

        // Verificar si la carpeta está vacía
        if (filesInSessionDir.length === 0) {
            return res.status(200).json({
                sessionId: sessionId,
                status: 'inactiva', // La sesión no ha sido completada (QR no escaneado)
                message: 'La sesión no ha sido completada (no se ha escaneado el QR).'
            });
        }

        // Si la carpeta no está vacía, la sesión se considera completada
        res.status(200).json({
            sessionId: sessionId,
            status: 'activa', // Sesión completada
            message: 'La sesión está completa.'
        });

    } catch (err) {
        res.status(500).send('Error al verificar la sesión: ' + err.message);
    }
});


app.post('/close-session-prev', (req, res) => {
    const { sessionId } = req.body;

    if (!sessionId) {
        return res.status(400).send('El sessionId es requerido.');
    }

    const session = sessions[sessionId];

    if (!session) {
        return res.status(404).json({ error: 'Sesión no encontrada.' });
    }

    // Cerrar la conexión y eliminar la sesión
    session.end(true); // Terminar la sesión
    delete sessions[sessionId]; // Eliminar la sesión del objeto de sesiones

    // Eliminar el archivo QR si existe
    const qrCodePath = path.join(__dirname, 'qrcodes', `${sessionId}.png`);
    if (fs.existsSync(qrCodePath)) {
        fs.unlinkSync(qrCodePath);
        console.log(`Archivo QR ${qrCodePath} eliminado.`);
    }

    // Eliminar los archivos de credenciales si existen
    const sessionDir = path.join(__dirname, 'sessions', sessionId);
    if (fs.existsSync(sessionDir)) {
        fs.rmSync(sessionDir, { recursive: true, force: true });
        console.log(`Archivos de sesión para ${sessionId} eliminados.`);
    }

    res.send({ message: `Sesión ${sessionId} cerrada y eliminada correctamente.` });
});




app.post('/close-all-sessions', (req, res) => {
    // Recorrer cada sesión activa en el objeto 'sessions'
    for (const sessionId in sessions) {
        const session = sessions[sessionId];

        // Finalizar la sesión si existe
        if (session) {
            session.end(true);
            console.log(`Sesión ${sessionId} cerrada.`);
        }

        // Eliminar la sesión del objeto 'sessions'
        delete sessions[sessionId];

        // Eliminar el archivo QR correspondiente
        const qrCodePath = path.join(__dirname, 'qrcodes', `${sessionId}.png`);
        if (fs.existsSync(qrCodePath)) {
            fs.unlinkSync(qrCodePath);
            console.log(`Archivo QR ${qrCodePath} eliminado.`);
        }
    }

    // Eliminar todas las carpetas en la carpeta 'sessions'
    const sessionsDir = path.join(__dirname, 'sessions');
    if (fs.existsSync(sessionsDir)) {
        fs.readdirSync(sessionsDir).forEach((file) => {
            const filePath = path.join(sessionsDir, file);
            if (fs.lstatSync(filePath).isDirectory()) {
                fs.rmSync(filePath, { recursive: true, force: true });
                console.log(`Directorio de sesión ${filePath} eliminado.`);
            }
        });
    }

    res.send({ message: 'Todas las sesiones cerradas y archivos eliminados correctamente.' });
});

// Endpoint para iniciar una sesión
app.post('/start-session', async (req, res) => {
    const { sessionId } = req.body;

    if (!sessionId) {
        return res.status(400).json({ success: false, message: 'Falta el sessionId.' });
    }

    // Ruta de la carpeta de la sesión
    const sessionDirPath = path.join(__dirname, 'sessions', sessionId);

    // Verificar si la sesión ya está activa en memoria
    if (sessions[sessionId]) {
        return res.status(400).json({
            success: false,
            message: `La sesión con ID ${sessionId} ya está activa.`,
        });
    }

    // Verificar si la carpeta de la sesión ya existe y no está vacía
    if (fs.existsSync(sessionDirPath) && fs.readdirSync(sessionDirPath).length > 0) {
        return res.status(400).json({
            success: false,
            message: `La sesión con ID ${sessionId} ya ha sido iniciada previamente. Por favor, cierra la sesión actual antes de reiniciarla.`,
        });
    }

    // Iniciar una nueva sesión si no está activa y la carpeta no contiene datos
    try {
        await createSession(sessionId);
        res.status(200).json({ success: true, message: `Sesión ${sessionId} iniciada correctamente.` });
    } catch (error) {
        console.error('Error al crear la sesión:', error);
        res.status(500).json({ success: false, message: 'Error al iniciar la sesión.' });
    }
});


app.post('/download-file', async (req, res) => {
    const { fileUrl } = req.body;

    if (!fileUrl) {
        console.log('Falta la URL del archivo en la solicitud.');
        return res.status(400).json({ success: false, message: 'Falta la URL del archivo.' });
    }

    try {
        console.log(`Intentando descargar el archivo desde: ${fileUrl}`);
        const fileName = path.basename(fileUrl); // Obtener nombre del archivo
        const filePath = path.join(__dirname, 'files', fileName); // Ruta de destino

        console.log(`Ruta de destino: ${filePath}`);
        const writer = fs.createWriteStream(filePath); // Crear stream de escritura

        const response = await axios({
            method: 'get',
            url: fileUrl,
            responseType: 'stream'
        });

        console.log('Respuesta recibida, empezando a guardar el archivo.');
        response.data.pipe(writer); // Descargar el archivo

        const finished = promisify(stream.finished); // Promesa para esperar hasta que el stream termine
        await finished(writer);

        console.log('Archivo descargado y guardado con éxito.');
        return res.status(200).json({ success: true, message: 'Archivo descargado con éxito.', fileName });
    } catch (error) {
        console.error('Error al descargar el archivo:', error.message);
        console.log('Detalles del error:', error); // Muestra detalles del error
        return res.status(500).json({ success: false, message: 'Error al descargar el archivo.' });
    }
});


// Endpoint para verificar la existencia de un archivo y obtener su tamaño
app.post('/check-file', (req, res) => {
    const { fileName } = req.body;

    if (!fileName) {
        return res.status(400).json({ success: false, message: 'Falta el nombre del archivo.' });
    }

    const filePath = path.join(__dirname, 'files', fileName);

    // Verificar si el archivo existe y obtener su información
    fs.stat(filePath, (err, stats) => {
        if (err) {
            if (err.code === 'ENOENT') {
                // El archivo no existe
                return res.status(404).json({ success: false, message: 'El archivo no existe.' });
            } else {
                // Otro error
                console.error('Error al obtener información del archivo:', err);
                return res.status(500).json({ success: false, message: 'Error al verificar el archivo.' });
            }
        }

        // Archivo existe, enviar información
        return res.status(200).json({
            success: true,
            message: 'El archivo existe.',
            fileName,
            size: stats.size, // Tamaño del archivo en bytes
            createdAt: stats.birthtime, // Fecha de creación
            modifiedAt: stats.mtime // Fecha de última modificación
        });
    });
});


// Endpoint para eliminar todas las carpetas y archivos en 'files'
app.post('/clear-files', (req, res) => {
    const dirPath = path.join(__dirname, 'files');

    // Verificar si la carpeta 'files' existe
    if (!fs.existsSync(dirPath)) {
        return res.status(404).json({ success: false, message: 'La carpeta "files" no existe.' });
    }

    // Función para eliminar recursivamente el contenido de la carpeta
    const deleteFolderContents = (folderPath) => {
        // Obtener todos los elementos dentro de la carpeta
        const files = fs.readdirSync(folderPath);
        for (const file of files) {
            const filePath = path.join(folderPath, file);
            const stat = fs.statSync(filePath);

            // Si es un directorio, llamar recursivamente
            if (stat.isDirectory()) {
                deleteFolderContents(filePath);
                fs.rmdirSync(filePath); // Eliminar el directorio una vez vaciado
            } else {
                fs.unlinkSync(filePath); // Eliminar el archivo
            }
        }
    };

    try {
        deleteFolderContents(dirPath); // Eliminar el contenido de 'files'
        return res.status(200).json({ success: true, message: 'Todos los archivos y carpetas han sido eliminados de "files".' });
    } catch (error) {
        console.error('Error al eliminar el contenido de la carpeta "files":', error);
        return res.status(500).json({ success: false, message: 'Error al limpiar la carpeta "files".' });
    }
});


// Endpoint para eliminar un archivo
app.post('/delete-file', (req, res) => {
    const { fileName } = req.body;

    if (!fileName) {
        return res.status(400).json({ success: false, message: 'Falta el nombre del archivo.' });
    }

    const filePath = path.join(__dirname, 'files', fileName);

    fs.unlink(filePath, (err) => {
        if (err) {
            if (err.code === 'ENOENT') {
                // El archivo no existe
                return res.status(404).json({ success: false, message: 'El archivo no existe.' });
            } else {
                // Error al intentar eliminar el archivo
                console.error('Error al eliminar el archivo:', err);
                return res.status(500).json({ success: false, message: 'Error al eliminar el archivo.' });
            }
        }

        // Archivo eliminado con éxito
        return res.status(200).json({ success: true, message: 'Archivo eliminado con éxito.' });
    });
});


//


// Cargar sesiones existentes al iniciar el servidor
async function loadExistingSessions() {
    const sessionsDir = './sessions';
    const filesDir = './files'; // Definir la ruta para la carpeta 'files'

    // Verificar si la carpeta 'sessions' existe, si no, crearla
    if (!fs.existsSync(sessionsDir)) {
        fs.mkdirSync(sessionsDir, { recursive: true });
        console.log(`La carpeta "${sessionsDir}" ha sido creada.`);
        return; // Salir si se creó la carpeta, ya que no hay sesiones que cargar
    }

    // Verificar si la carpeta 'files' existe, si no, crearla
    if (!fs.existsSync(filesDir)) {
        fs.mkdirSync(filesDir, { recursive: true });
        console.log(`La carpeta "${filesDir}" ha sido creada.`);
    }

    // Leer directorios dentro de 'sessions'
    const sessionDirs = fs.readdirSync(sessionsDir).filter(file => fs.statSync(path.join(sessionsDir, file)).isDirectory());

    for (const sessionId of sessionDirs) {
        const sessionDirPath = path.join(sessionsDir, sessionId);
        let attempts = 0;
        const maxAttempts = 3;

        // Verificar si la subcarpeta de la sesión existe
        if (!fs.existsSync(sessionDirPath)) {
            console.warn(`La carpeta de sesión "${sessionDirPath}" no existe. Omitiendo la carga de esta sesión.`);
            continue; // Omitir esta sesión si no existe
        }

        while (attempts < maxAttempts) {
            try {
                // Intentar crear la sesión y almacenar el socket en el objeto sessions
                await createSession(sessionId);
                // console.log(`Sesión ${sessionId} cargada correctamente.`);

                // Actualizar estado a activa en el objeto sessions
                sessions[sessionId].connectionStatus = "activa"; 
                break; // Salir del bucle si la sesión se carga correctamente
            } catch (error) {
                attempts++;
                console.error(`Error al cargar la sesión ${sessionId}. Intento ${attempts} de ${maxAttempts}.`, error);

                // Si se alcanzó el número máximo de intentos
                if (attempts === maxAttempts) {
                    console.error(`No se pudo cargar la sesión ${sessionId} después de ${maxAttempts} intentos. Pasando a la siguiente.`);

                    // Actualizar estado a inactiva en el objeto sessions
                    if (sessions[sessionId]) {
                        sessions[sessionId].connectionStatus = "inactiva"; 
                    }
                }
            }
        }
    }
}



// Endpoint para cargar una sesión específica
app.post('/load-session', async (req, res) => {
    const { sessionId } = req.body;

    console.log(`Solicitud recibida para cargar la sesión: ${sessionId}`);

    // Validación inicial: verificar si se proporcionó sessionId
    if (!sessionId) {
        console.log('Error: Falta el sessionId en la solicitud.');
        return res.status(400).json({ success: false, message: 'Falta el sessionId.' });
    }

    const sessionsDir = './sessions';
    const sessionDirPath = path.join(sessionsDir, sessionId);

    // Verificar si la carpeta 'sessions' existe
    if (!fs.existsSync(sessionsDir)) {
        console.log(`Error: La carpeta "${sessionsDir}" no existe.`);
        return res.status(404).json({ success: false, message: 'No hay sesiones existentes para cargar.' });
    }

    // Verificar si la carpeta de la sesión específica existe
    if (!fs.existsSync(sessionDirPath)) {
        console.log(`Error: La sesión con ID ${sessionId} no existe.`);
        return res.status(404).json({ success: false, message: `La sesión con ID ${sessionId} no existe.` });
    }

    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
        try {
            // Aquí, simplemente marcamos la sesión como activa
            if (!sessions[sessionId]) {
                sessions[sessionId] = { connectionStatus: 'activa' };
                console.log(`Sesión ${sessionId} cargada y marcada como activa.`);
            } else {
                sessions[sessionId].connectionStatus = 'activa';
                console.log(`Sesión ${sessionId} ya estaba en memoria. Estado actualizado a activa.`);
            }
            return res.status(200).json({ success: true, message: `Sesión ${sessionId} cargada correctamente.` });
        } catch (error) {
            attempts++;
            console.error(`Error al cargar la sesión ${sessionId}. Intento ${attempts} de ${maxAttempts}.`, error);

            // Si se alcanzó el número máximo de intentos
            if (attempts === maxAttempts) {
                console.error(`No se pudo cargar la sesión ${sessionId} después de ${maxAttempts} intentos.`);
                return res.status(500).json({ success: false, message: `No se pudo cargar la sesión ${sessionId} después de ${maxAttempts} intentos.` });
            }
        }
    }
});

app.use(express.json({ limit: '10mb' })); // <-- debe ir antes de app.post('/masivo/preview', ...)

// Endpoint para recibir y guardar campañas masivas
app.post('/masivo/preview', async (req, res) => {
  try {
    let payload = req.body;

    // Si viene como string (por ejemplo desde PHP con json_encode), intenta parsear
    if (typeof payload === 'string') {
      try { payload = JSON.parse(payload); } catch (_) {}
    }

    if (!payload || typeof payload !== 'object') {
      return res.status(400).json({ ok: false, message: 'Body inválido: se esperaba JSON con {ok, campana, destinatarios}.' });
    }

    const { ok, campana, destinatarios } = payload;

    if (ok !== true) {
      return res.status(400).json({ ok: false, message: '`ok` debe ser true.' });
    }
    if (!campana || typeof campana !== 'object') {
      return res.status(400).json({ ok: false, message: 'Falta `campana`.' });
    }
    if (!Array.isArray(destinatarios)) {
      return res.status(400).json({ ok: false, message: '`destinatarios` debe ser un array.' });
    }

    // === Logs a consola ===
    console.log('📢 Nueva campaña masiva recibida');
    console.log('---------------------------------');
    console.log('ID campaña:', campana.id);
    console.log('Usuario:', campana.iduser);
    console.log('Empresa:', campana.empresa);
    console.log('Método envío:', campana.metodo_envio);
    console.log('Intervalo tiempo:', campana.intervalo_tiempo);
    console.log('Modo tiempo:', campana.modo_tiempo);
    console.log('Mensaje base:', campana.mensaje);
    console.log('Total destinatarios:', destinatarios.length);
    console.log('---------------------------------');

    // === Guardar archivo JSON ===
    const fs = require('fs');
    const path = require('path');
    const campanasDir = path.join(__dirname, 'campanas');
    if (!fs.existsSync(campanasDir)) fs.mkdirSync(campanasDir, { recursive: true });

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const fileName = `campana_${campana.id || 'sinid'}_${timestamp}.json`;
    const filePath = path.join(campanasDir, fileName);
    fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), 'utf8');

    console.log(`✅ Campaña guardada en: ${filePath}`);

    // === Respuesta ===
    return res.json({
      ok: true,
      message: 'Campaña registrada y guardada correctamente.',
      resumen: {
        id: campana.id,
        iduser: campana.iduser,
        empresa: campana.empresa,
        metodo_envio: campana.metodo_envio,
        intervalo_seg: Number(campana.intervalo_tiempo) || null,
        modo_tiempo: campana.modo_tiempo,
        total_destinatarios: destinatarios.length,
        archivo: `/campanas/${fileName}`
      }
    });

  } catch (err) {
    console.error('❌ Error al guardar campaña:', err);
    return res.status(500).json({ ok: false, message: 'Error interno al registrar la campaña.' });
  }
});

app.use('/campanas', express.static(require('path').join(__dirname, 'campanas')));


// Servir los QR codes generados como imágenes estáticas
app.use('/qrcodes', express.static(path.join(__dirname, 'qrcodes')));

app.listen(PORT, '0.0.0.0', () => {
    console.log(`Servidor ejecutándose en el puerto ${PORT}`);
    loadExistingSessions(); // Cargar sesiones existentes (sin await)
    console.log('Iniciando carga de sesiones existentes...');
});
