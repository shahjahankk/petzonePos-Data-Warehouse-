#!/usr/bin/env node
/**
 * Print warehouse health: watermarks, recent runs, fact counts, cache stats.
 *
 *   node scripts/warehouse-status.js
 */

require('dotenv').config();

const { getWarehouseStatus } = require('../services/warehouse/reportQueryService');
const { closeWarehouseDB } = require('../config/warehouseDatabase');
const { closeDB } = require('../config/database');

async function main() {
  const status = await getWarehouseStatus();
  console.log(JSON.stringify(status, null, 2));
}

main()
  .catch((error) => {
    console.error('[warehouse] status failed:', error.message);
    process.exitCode = 1;
  })
  .finally(async () => {
    await Promise.allSettled([closeWarehouseDB(), closeDB()]);
  });
