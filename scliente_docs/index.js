const express = require('express');
const axios = require('axios');
const amqp = require('amqplib');

// Crear una aplicación Express
const app = express();
app.use(express.json());

// Configuración de URLs y nombre de la cola
const SCLIENTE_URL = 'http://localhost:3000'; 
const RABBITMQ_URL = 'amqp://scliente_docs:scliente_docs@localhost';
const QUEUE_NAME = 'document_queue';

// Array para almacenar documentos enviados
let documents = [];

// Función para conectar a RabbitMQ y crear un canal
async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();
    await channel.assertQueue(QUEUE_NAME, { durable: true });
    return channel;
  } catch (error) {
    console.error('Error conectando a RabbitMQ:', error);
    throw error;
  }
}

// Función para enviar documentos a la cola de RabbitMQ
async function sendDocuments(docs) {
  try {
    const channel = await connectRabbitMQ();
    docs.forEach(doc => {
      const message = Buffer.from(JSON.stringify(doc));
      channel.sendToQueue(QUEUE_NAME, message, { persistent: true });
    });
    console.log('Documentos enviados a la cola');
  } catch (error) {
    console.error('Error enviando documentos:', error);
  }
}

// Función para consultar el estado de los documentos 
async function checkStatus() {
  try {
    for (const doc of documents) {
      const response = await axios.get(`${SCLIENTE_URL}/check_status/${doc.id}`); 
      console.log(`Documento ${doc.id}: ${response.data.status}`);
    }
  } catch (error) {
    console.error('Error consultando estado:', error);
  }
}

// Función para generar nuevos documentos
function generateNewDocuments() {
  const newDocs = Array.from({ length: 50 }, (_, i) => ({ id: `doc_${Date.now()}_${i}` }));
  documents.push(...newDocs);
  return newDocs;
}

// Configurar intervalos para enviar documentos y consultar estado cada 3 minutos
setInterval(() => {
  const newDocs = generateNewDocuments();
  sendDocuments(newDocs).catch(error => {
    console.error('Error al enviar documentos:', error);
  });
}, 180000); // Intervalo de 3 minutos

setInterval(() => {
  checkStatus().catch(error => {
    console.error('Error al consultar estado:', error);
  });
}, 180000); // Intervalo de 3 minutos

// Iniciar el servidor en el puerto especificado
const PORT = 4000;
app.listen(PORT, () => {
  console.log(`scliente_docs corriendo en el puerto ${PORT}`);
});
