/**
 * PetZone data warehouse service.
 *
 * Standalone Node application that owns:
 *   - the OLAP database (`petzone_dw`)
 *   - the incremental ETL + scheduler
 *   - the `/api/reports/*` report read API
 *   - the `/api/warehouse-reports/*` status/admin API
 *
 * The POS backend proxies `/api/reports/{sales,clinic,inventory,ledger,financial}`
 * to this service when `WAREHOUSE_SERVICE_URL` is configured, so the frontend
 * needs no changes.
 */

require('dotenv').config();

const express = require('express');
const cors = require('cors');
const morgan = require('morgan');

const { connectDB, closeDB } = require('./config/database');
const { closeWarehouseDB, warehouseEnabled } = require('./config/warehouseDatabase');
const auth = require('./middleware/auth');
const reportRoutes = require('./routes/reports');
const adminRoutes = require('./routes/admin');
const {
  startWarehouseScheduler,
  stopWarehouseScheduler,
} = require('./services/warehouse/warehouseScheduler');

if (!process.env.JWT_SECRET || process.env.JWT_SECRET.length < 16) {
  console.error('[warehouse] JWT_SECRET is required (same value as the POS backend).');
  process.exit(1);
}

const app = express();

const corsOrigins = String(process.env.CORS_ORIGIN || '*')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);

app.use(cors({
  origin: corsOrigins.includes('*') ? '*' : corsOrigins,
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
}));

if (process.env.NODE_ENV !== 'test' && process.env.HTTP_LOG !== 'false') {
  app.use(morgan('combined'));
}

app.use(express.json({ limit: '1mb' }));

app.get('/health', (req, res) => {
  res.json({
    success: true,
    service: 'petzone-data-warehouse',
    warehouseEnabled,
    time: new Date().toISOString(),
  });
});

app.use('/api', auth);

// Same URLs as the POS backend. The proxy can point either path at this app.
app.use('/api/reports', reportRoutes);
app.use('/api/warehouse-reports', reportRoutes);
app.use('/api/warehouse-reports', adminRoutes);

app.use((req, res) => {
  res.status(404).json({ success: false, message: `Route ${req.originalUrl} not found` });
});

app.use((error, req, res, next) => {
  console.error('[warehouse] unhandled request error:', error);
  if (res.headersSent) return next(error);
  return res.status(500).json({ success: false, message: 'Internal server error' });
});

const parsedPort = Number.parseInt(process.env.PORT, 10);
const PORT = Number.isInteger(parsedPort) && parsedPort >= 0 && parsedPort < 65536
  ? parsedPort
  : 4100;

if (process.env.NODE_ENV !== 'test') {
  connectDB();

  if (warehouseEnabled && String(process.env.DW_SCHEDULER_ENABLED || 'true').toLowerCase() !== 'false') {
    startWarehouseScheduler();
  }

  const server = app.listen(PORT, '0.0.0.0', () => {
    const address = server.address();
    console.log(`[warehouse] service listening on http://${address.address}:${address.port}`);
  });

  server.on('error', (error) => {
    if (error.code === 'EADDRINUSE') {
      console.error(`[warehouse] port ${PORT} is already in use`);
    } else {
      console.error(`[warehouse] server error: ${error.message}`);
    }
    process.exit(1);
  });

  let shuttingDown = false;
  const shutdown = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[warehouse] ${signal} received, shutting down`);
    stopWarehouseScheduler();
    await Promise.allSettled([closeWarehouseDB(), closeDB()]);
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 5000).unref();
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

module.exports = app;
