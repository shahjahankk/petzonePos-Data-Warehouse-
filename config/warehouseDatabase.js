/**
 * Dedicated connection pool for the reporting warehouse (OLAP).
 *
 * The warehouse is derived data: it is never used for POS writes and can be
 * dropped/rebuilt at any time. Defaults to a separate database on the same
 * MySQL server as OLTP, but can be pointed at another host with DW_DB_*.
 */

const mysql = require('mysql2/promise');

const asBool = (value, fallback) => {
  if (value === undefined || value === null || value === '') return fallback;
  return !['false', '0', 'no', 'off'].includes(String(value).trim().toLowerCase());
};

const asInt = (value, fallback) => {
  const n = parseInt(value, 10);
  return Number.isFinite(n) ? n : fallback;
};

const warehouseEnabled = asBool(process.env.DW_ENABLED, true);

const dwConfig = {
  host: process.env.DW_DB_HOST || process.env.DB_HOST || 'localhost',
  port: asInt(process.env.DW_DB_PORT || process.env.DB_PORT, 3306),
  user: process.env.DW_DB_USER || process.env.DB_USER || 'root',
  password: process.env.DW_DB_PASSWORD !== undefined
    ? process.env.DW_DB_PASSWORD
    : (process.env.DB_PASSWORD || ''),
  database: process.env.DW_DB_NAME || 'petzone_dw',
  waitForConnections: true,
  connectionLimit: asInt(process.env.DW_DB_POOL_LIMIT, 5),
  queueLimit: asInt(process.env.DW_DB_QUEUE_LIMIT, 50),
  idleTimeout: 300000,
  enableKeepAlive: true,
  keepAliveInitialDelay: 0,
  charset: 'utf8mb4',
  timezone: '+05:00',
  supportBigNumbers: true,
  bigNumberStrings: true,
  multipleStatements: false,
};

/** Pool used by ETL + report queries. `null` when the warehouse is disabled. */
const dwPool = warehouseEnabled ? mysql.createPool(dwConfig) : null;

/** Connection config without a database, used to CREATE DATABASE on init. */
const dwAdminConfig = {
  host: dwConfig.host,
  port: dwConfig.port,
  user: dwConfig.user,
  password: dwConfig.password,
  connectTimeout: 15000,
  multipleStatements: true,
};

const closeWarehouseDB = async () => {
  if (!dwPool) return;
  try {
    await dwPool.end();
  } catch {
    /* ignore */
  }
};

module.exports = {
  warehouseEnabled,
  dwPool,
  dwConfig,
  dwAdminConfig,
  closeWarehouseDB,
};
