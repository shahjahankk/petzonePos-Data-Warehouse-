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
  timezone: 'Z',
  supportBigNumbers: true,
  bigNumberStrings: true,
};

// TIMESTAMP columns are returned in the session timezone. Keep the session
// aligned with SOURCE_TIMEZONE so both TIMESTAMP and DATETIME columns are read
// in the same (source) timezone before CONVERT_TZ shifts them to business time.
const sourceTimeZone = process.env.SOURCE_TIMEZONE || 'SYSTEM';

const pool = mysql.createPool(dbConfig);
pool.on('connection', (connection) => {
  connection.query(`SET time_zone = '${sourceTimeZone.replace(/'/g, "''")}'`);
});

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
