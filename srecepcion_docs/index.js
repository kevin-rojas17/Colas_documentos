const express = require('express');
const amqp = require('amqplib/callback_api');

// Crear una aplicación Express
const app = express();
app.use(express.json());

// Configuración de RabbitMQ y nombre de la cola
const RABBITMQ_URL = 'amqp://srecepcion_docs:srecepcion_docs@localhost'; // Cambio realizado aquí
const QUEUE_NAME = 'document_queue';

// Objeto para almacenar estados de los documentos
let documents = {};

// Conectar a RabbitMQ y configurar el canal
amqp.connect(RABBITMQ_URL, (error0, connection) => {
  if (error0) {
    throw error0;
  }
  connection.createChannel((error1, channel) => {
    if (error1) {
      throw error1;
    }
    channel.assertQueue(QUEUE_NAME, { durable: true });

    // Procesar documentos de la cola
    channel.consume(QUEUE_NAME, (msg) => {
      if (msg !== null) {
        const doc = JSON.parse(msg.content.toString());
        const docId = doc.id;
        documents[docId] = 'En Proceso';
        
        // Cambiar el estado después de 1 minuto
        setTimeout(() => {
          const newState = Math.random() > 0.5 ? 'Aceptado' : 'Rechazado';
          documents[docId] = newState;
          console.log(`Documento ${docId} actualizado a ${newState}`);
          
          // Eliminar el mensaje de la cola
          channel.ack(msg);
        }, 60000);
      }
    }, { noAck: false });

    // Endpoint para recibir documentos
    app.post('/receive_documents', (req, res) => {
      const docs = req.body.documents;
      docs.forEach(doc => {
        channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(doc)), { persistent: true });
      });
      res.json({ message: 'En Proceso' });
    });

    // Endpoint para consultar el estado de los documentos
    app.get('/check_status/:id', (req, res) => {
      const docId = req.params.id;
      const status = documents[docId];
      if (status) {
        res.json({ id: docId, status: status });
      } else {
        res.status(404).json({ message: 'Documento no encontrado' });
      }
    });

  });
});

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`srecepcion_docs corriendo en el puerto ${PORT}`); // Cambio realizado aquí
});
