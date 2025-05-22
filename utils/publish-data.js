import { getPool } from '../config/default.js';
import { getRabbitMQConnection } from '../config/default.js';
import logger from './logger.js';
import { setupRabbitMQQueue } from './queueManager.js'; // Import your queue manager

export async function fetchAndPublishInvoiceLines() {
    const pool = await getPool('orders');

    const result = await pool.request().query(`
        SELECT [ExtDocNo]
            ,[LineNo]
            ,[CustNo]
            ,[Date]
            ,[SPCode]
            ,[ShiptoCode]
            ,[ItemNo]
            ,[Qty]
            ,[Location]
            ,[SUOM]
            ,[UnitPrice]
            ,[TotalHeaderAmount]
            ,[LineAmount]
            ,[TotalHeaderQty]
            ,[Type]
            ,[CUInvoiceNo]
            ,[CUNo]
            ,[SigningTime]
            ,[Published]
            FROM [orders].[dbo].[invoice_data]
        WHERE Published = 0 
        AND [Date] >= DATEADD(d, -2, DATEDIFF(d, 0, GETDATE()))
    `);

    const rows = result.recordset;

    if (!rows || rows.length === 0) {
        logger.info('No new invoices found to publish.');
        return 0;
    }

    logger.info(`Found ${rows.length} invoices to publish`);
    logger.info(`First invoice sample: ${JSON.stringify(rows[0])}`);

    try {
        const connection = await getRabbitMQConnection();
        const channel = await connection.createChannel();
        
        // Use your queue manager to ensure proper queue setup
        await setupRabbitMQQueue('invoices_fcl');
        await setupRabbitMQQueue('invoices_cm');
        await setupRabbitMQQueue('invoices_rmk');

        // Define queue names consistently
        const fcl_queueName = 'invoices_fcl.bc';
        const cm_queueName = 'invoices_cm.bc';
        const rmk_queueName = 'invoices_rmk.bc';

        // Publish messages
        const publishedInvoices = [];
        
        for (const invoice of rows) {
            try {
                const payload = JSON.stringify(invoice);
                const queueName = invoice.CustNo.charAt(0) === 'B' ? cm_queueName : 
                                invoice.CustNo.charAt(0) === 'C' ? rmk_queueName : fcl_queueName;
                
                logger.debug(`Routing to queue: ${queueName} for customer ${invoice.CustNo}`);
                
                const sent = await channel.publish(
                    'fcl.exchange.direct',
                    queueName,
                    Buffer.from(payload),
                    {
                        persistent: true,
                        contentType: 'application/json',
                    }
                );
                
                if (sent) {
                    publishedInvoices.push(invoice.ExtDocNo);
                    logger.debug(`Published invoice ${invoice.ExtDocNo} to ${queueName}`);
                } else {
                    logger.error(`Failed to publish invoice ${invoice.ExtDocNo}`);
                }
            } catch (publishError) {
                logger.error(`Error publishing invoice ${invoice.ExtDocNo}: ${publishError.message}`);
            }
        }

        logger.info(`Published ${publishedInvoices.length} invoice lines to RabbitMQ`);
        
        // Only update published status for successfully published invoices
        if (publishedInvoices.length > 0) {
            const invoiceIds = publishedInvoices.map(id => `'${id}'`).join(',');
            await pool.request().query(`
                UPDATE [orders].[dbo].[invoice_data] 
                SET Published = 1 
                WHERE ExtDocNo IN (${invoiceIds})
            `);
            logger.info(`Updated published status for ${publishedInvoices.length} invoices`);
        }

        await channel.close();
        return publishedInvoices.length;
    } catch (error) {
        logger.error(`Error in publish process: ${error.message}`);
        throw error;
    }
}

// Run once on launch
(async () => {
    try {
        const invoiceLineCount = await fetchAndPublishInvoiceLines();
        console.log(`Published ${invoiceLineCount} invoice lines to RabbitMQ`);
    } catch (error) {
        console.error(`Error publishing data: ${error.message}`);
    }
})();

// Schedule every 2 minutes
setInterval(async () => {
    try {
        const invoiceLineCount = await fetchAndPublishInvoiceLines();
        console.log(`Published ${invoiceLineCount} invoice lines to RabbitMQ`);
    } catch (error) {
        console.error(`Error publishing data: ${error.message}`);
    }
}, 120000); // 2 minutes in ms