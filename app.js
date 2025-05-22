// app.js
import express from 'express';
import invoiceRoutes from './routes/invoiceRoutes.js';
import dotenv from 'dotenv';

// Load environment variables from .env file
dotenv.config();


const app = express();
app.use(express.json());


// Register routes
app.use('/api', invoiceRoutes);


// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});