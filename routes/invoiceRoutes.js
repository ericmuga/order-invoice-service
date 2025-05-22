// routes/invoiceRoutes.js
import express from 'express';
import { fetchInvoicesFromQueue } from '../Consumers/invoiceConsumer.js';

const router = express.Router();

router.get('/fetch-invoices', async (req, res) => {
  try {
    const queueType = req.query.queue || 'fcl'; // Default to FCL
    const limit = parseInt(req.query.limit || '100'); // Default to 100
    
    if (!['fcl', 'cm', 'rmk'].includes(queueType.toLowerCase())) {
      return res.status(400).json({ 
        success: false, 
        message: 'Invalid queue type. Must be fcl, cm, or rmk' 
      });
    }

    if (isNaN(limit) || limit < 1 || limit > 1000) {
      return res.status(400).json({ 
        success: false, 
        message: 'Limit must be a number between 1 and 1000' 
      });
    }

    const invoices = await fetchInvoicesFromQueue(queueType, limit);
    
    res.json({
      success: true,
      queue: queueType,
      count: invoices.length,
      invoices
    });
    
  } catch (err) {
    logger.error(`API Error: ${err.message}`);
    res.status(500).json({ 
      success: false, 
      message: 'Failed to fetch invoices',
      error: err.message 
    });
  }
});

export default router;