#!/usr/bin/env node
/**
 * Create the warehouse database (if missing) and apply warehouse/schema.sql.
 *
 *   node scripts/warehouse-init.js
 */

require('dotenv').config();

const mysql = require('mysql2/promise');
const { dwConfig, dwAdminConfig, closeWarehouseDB } = require('../config/warehouseDatabase');
const { ensureSchema } = require('../services/warehouse/schema');

async function main() {
  const database = dwConfig.database;
  if (!/^[A-Za-z0-9_$-]+$/.test(database)) {
    throw new Error(`Unsafe DW_DB_NAME: ${database}`);
  }

  try {
    const admin = await mysql.createConnection(dwAdminConfig);
    try {
      await admin.query(
        `CREATE DATABASE IF NOT EXISTS \`${database}\`
         CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`
      );
    } finally {
      await admin.end();
    }
  } catch (err) {
    console.log('[warehouse] skipping CREATE DATABASE (no privilege or already exists):', err.message);
  }

  await ensureSchema();
  console.log(`[warehouse] schema ready in database "${database}"`);
  await closeWarehouseDB();
}

main().catch(async (error) => {
  console.error('[warehouse] init failed:', error && (error.stack || error.message || error));
  try { await closeWarehouseDB(); } catch { /* ignore */ }
  process.exit(1);
});
