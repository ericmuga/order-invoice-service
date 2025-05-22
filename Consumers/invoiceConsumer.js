// consumers/invoiceConsumer.js
import { getRabbitMQConnection } from '../config/default.js';
import logger from '../utils/logger.js';

export const fetchInvoicesFromQueue = async (queueType = 'fcl', limit = 100) => {
  // Map queue types to their actual queue names
  const queueMap = {
    fcl: 'invoices_fcl.bc',
    cm: 'invoices_cm.bc',
    rmk: 'invoices_rmk.bc'
  };

  const queueName = queueMap[queueType.toLowerCase()] || queueMap.fcl;
  const exchange = 'fcl.exchange.direct';

  try {
    const connection = await getRabbitMQConnection();
    const channel = await connection.createChannel();

    await channel.assertExchange(exchange, 'direct', { durable: true });
    await channel.assertQueue(queueName, { durable: true });
    await channel.bindQueue(queueName, exchange, queueName);

    channel.prefetch(limit);
    const messages = [];

    await new Promise((resolve) => {
      channel.consume(
        queueName,
        (msg) => {
          if (msg) {
            try {
              const data = JSON.parse(msg.content.toString());
              messages.push(data);
              channel.ack(msg);
              if (messages.length >= limit) resolve();
            } catch (err) {
              logger.error(`Error parsing message: ${err.message}`);
              channel.nack(msg, false, false);
            }
          }
        },
        { noAck: false }
      );

      // Timeout if we don't reach the limit
      setTimeout(() => {
        logger.info(`Timeout reached for queue: ${queueName}, fetched ${messages.length} messages.`);
        resolve();
      }, 5000); // 5 second timeout
    });

    await channel.close();
    // await connection.close();

    if (messages.length === 0) {
      logger.info(`No messages found in queue: ${queueName}`);
    }

    return messages;
  } catch (error) {
    logger.error(`Error fetching invoices: ${error.message}`);
    throw error;
  }
};