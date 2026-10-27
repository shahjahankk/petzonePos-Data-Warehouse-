#!/usr/bin/env node
/**
 * Run a warehouse ETL load.
 *
 *   node scripts/warehouse-sync-once.js                 # incremental
 *   node scripts/warehouse-sync-once.js --mode=daily    # incremental + reconcile + snapshot
 *   node scripts/warehouse-sync-once.js --mode=backfill # full load
 *   node scripts/warehouse-sync-once.js --mode=backfill --from=2026-01-01 --to=2026-03-31
 *   node scripts/warehouse-sync-once.js --mode=rebuild  # truncate + full load
 */

require('dotenv').config();

const { runEtl } = require('../services/warehouse/etlOrchestrator');
const { closeWarehouseDB } = require('../config/warehouseDatabase');
const { closeDB } = require('../config/database');

function arg(name, fallback = null) {
  const prefix = `--${name}=`;
  const found = process.argv.find((a) => a.startsWith(prefix));
  return found ? found.slice(prefix.length) : fallback;
}

async function main() {
  const mode = arg('mode', 'incremental');
  const from = arg('from');
  const to = arg('to');

  const summary = await runEtl({ mode, from, to });
  console.log(JSON.stringify(summary, null, 2));
}

main()
  .catch((error) => {
    console.error('[warehouse] sync failed:', error.message);
    process.exitCode = 1;
  })
  .finally(async () => {
    await Promise.allSettled([closeWarehouseDB(), closeDB()]);
  });
