const { default: makeWASocket, useMultiFileAuthState, makeInMemoryStore } = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');
const express = require('express');
const qrcode = require('qrcode');
const fs = require('fs');
const path = require('path');
const axios = require('axios'); // Asegúrate de tener axios instalado
const mime = require('mime-types'); // Para obtener el mime type de forma automática

// Crear el store para almacenar contactos (declararlo globalmente)
const store = makeInMemoryStore({});

// Vincular el almacenamiento del store a un archivo (opcional, si quieres persistir datos entre reinicios)
store.readFromFile('./baileys_store.json');

// Guardar el store periódicamente en el archivo
setInterval(() => {
    store.writeToFile('./baileys_store.json');
}, 10_000);

const app = express();
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
            const qrCodePath = path.join(__dirname, 'qrcodes', `${sessionId}.png`);
            await qrcode.toFile(qrCodePath, qr);
        }

        if (connection === 'open') {
            sock.connectionStatus = "activa"; // Actualizar estado a activa
            console.log(`Conexión abierta para la sesión ${sessionId}`);
        } else if (connection === 'close') {
            sock.connectionStatus = "inactiva"; // Actualizar estado a inactiva
            const shouldReconnect = (lastDisconnect.error = Boom)?.output?.statusCode !== 401;
            console.log(`Conexión cerrada para la sesión ${sessionId}. Reintentando...`);
            if (shouldReconnect) {
                await createSession(sessionId);
            }
        }
    });

       // Escuchar mensajes entrantes



    sessions[sessionId] = sock;
    sock.ev.on('creds.update', saveCreds);
}




app.listen(3000, '0.0.0.0', () => {
    console.log('Servidor ejecutándose en el puerto 3000');
});