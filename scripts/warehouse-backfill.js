#!/usr/bin/env node
/**
 * Backfill warehouse facts.
 *
 *   node scripts/warehouse-backfill.js                         # full history
 *   node scripts/warehouse-backfill.js --from=2026-01-01 --to=2026-03-31
 *
 * A window backfill deletes and re-extracts only that business-date range, which
 * is the supported way to repair old hard-deletes/edits outside the daily
 * reconcile window.
 */

require('dotenv').config();

const { runEtl } = require('../services/warehouse/etlOrchestrator');
const { closeWarehouseDB } = require('../config/warehouseDatabase');
const { closeDB } = require('../config/database');

function arg(name) {
  const prefix = `--${name}=`;
  const found = process.argv.find((a) => a.startsWith(prefix));
  return found ? found.slice(prefix.length) : null;
}

async function main() {
  const summary = await runEtl({ mode: 'backfill', from: arg('from'), to: arg('to') });
  console.log(JSON.stringify(summary, null, 2));
}

main()
  .catch((error) => {
    console.error('[warehouse] backfill failed:', error.message);
    process.exitCode = 1;
  })
  .finally(async () => {
    await Promise.allSettled([closeWarehouseDB(), closeDB()]);
  });
