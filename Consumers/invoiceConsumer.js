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
  const deadLetterExchange = 'fcl.exchange.dlx'; // Match your producer configuration

  try {
    const connection = await getRabbitMQConnection();
    const channel = await connection.createChannel();

    // Declare exchanges
    await channel.assertExchange(exchange, 'direct', { durable: true });
    await channel.assertExchange(deadLetterExchange, 'direct', { durable: true });

    // Declare queue with same parameters as producer
    await channel.assertQueue(queueName, {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': deadLetterExchange,
        'x-dead-letter-routing-key': queueName
      }
    });

    // Bind queue to exchange
    await channel.bindQueue(queueName, exchange, queueName);

    // Set prefetch limit
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
              channel.ack(msg); // Uncommented the ack to properly acknowledge messages
              if (messages.length >= limit) {
                logger.info(`Reached message limit (${limit}) for queue: ${queueName}`);
                resolve();
              }
            } catch (err) {
              logger.error(`Error parsing message: ${err.message}`);
              channel.nack(msg, false, false); // Reject and don't requeue
            }
          }
        },
        { noAck: false }
      );

      // Timeout if we don't reach the limit
      setTimeout(() => {
        if (messages.length > 0) {
          logger.info(`Timeout reached for queue: ${queueName}, fetched ${messages.length} messages.`);
        }
        resolve();
      }, 5000); // 5 second timeout
    });

    await channel.close();

    if (messages.length === 0) {
      logger.info(`No messages found in queue: ${queueName}`);
    }

    return messages;
  } catch (error) {
    logger.error(`Error fetching invoices from queue ${queueName}: ${error.message}`);
    throw error;
  }
};