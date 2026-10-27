/**
 * OLTP connection pool (read-only in this service).
 *
 * The warehouse ETL reads source tables from the POS database and never writes
 * to it. Keep this pool small: the POS backend already has its own pool.
 */

const mysql = require('mysql2/promise');

const port = parseInt(process.env.DB_PORT || '3306', 10);

const dbConfig = {
  host: process.env.DB_HOST || 'localhost',
  port: Number.isFinite(port) ? port : 3306,
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'multipos_db',
  waitForConnections: true,
  connectionLimit: parseInt(process.env.DB_POOL_LIMIT || '10', 10),
  queueLimit: parseInt(process.env.DB_QUEUE_LIMIT || '50', 10),
  idleTimeout: 300000,
  keepAliveInitialDelay: 0,
  enableKeepAlive: true,
  charset: 'utf8mb4',
  timezone: '+05:00',
  supportBigNumbers: true,
  bigNumberStrings: true,
};

const pool = mysql.createPool(dbConfig);

async function connectDB() {
  try {
    const connection = await pool.getConnection();
    connection.release();
    console.log(`[warehouse] OLTP connected: ${dbConfig.host}:${dbConfig.port}/${dbConfig.database}`);
  } catch (error) {
    console.error(`[warehouse] OLTP connection failed (${dbConfig.host}:${dbConfig.port}/${dbConfig.database}): ${error.message}`);
  }
}

async function closeDB() {
  try {
    await pool.end();
  } catch {
    /* already closed */
  }
}

module.exports = { pool, dbConfig, connectDB, closeDB };
