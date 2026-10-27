/**
 * ETL orchestrator — the only entry point for warehouse loads.
 *
 * Modes:
 *   incremental : pull changed rows since the watermark, rebuild touched days
 *   daily       : incremental + reconcile the last N days + snapshot
 *   backfill    : full load, or a specific business-date window
 *   rebuild     : truncate facts + aggregates, then full load
 */

const { warehouseEnabled, dwPool } = require('../../config/warehouseDatabase');
const { ensureSchema } = require('./schema');
const dims = require('./dimensionService');
const facts = require('./factSyncService');
const aggs = require('./aggregateService');
const { snapshotInventory } = require('./snapshotService');
const stateStore = require('./etlState');
const reportCache = require('./reportCache');
const {
  todayDateKey,
  shiftDateKey,
  dateKeyRange,
  parseDateKey,
} = require('./dateKeys');

const RECONCILE_DAYS = Math.max(1, parseInt(process.env.DW_RECONCILE_DAYS, 10) || 7);
const AUTO_INIT = String(process.env.DW_AUTO_INIT || 'true').toLowerCase() !== 'false';

let runningPromise = null;

function assertEnabled() {
  if (!warehouseEnabled || !dwPool) {
    throw new Error('Warehouse is disabled (DW_ENABLED=false)');
  }
}

/**
 * Process-safe mutex using MySQL's advisory lock. The in-process promise is
 * enough for a single Node process; this also protects cron + server overlap.
 */
async function withEtlLock(fn) {
  const connection = await dwPool.getConnection();
  let acquired = false;
  try {
    const [rows] = await connection.query("SELECT GET_LOCK('petzone_dw_etl', 0) AS got");
    acquired = Number(rows?.[0]?.got) === 1;
    if (!acquired) {
      const error = new Error('Warehouse ETL is already running');
      error.code = 'ETL_ALREADY_RUNNING';
      throw error;
    }
    return await fn();
  } finally {
    if (acquired) {
      try {
        await connection.query("SELECT RELEASE_LOCK('petzone_dw_etl')");
      } catch {
        /* connection may already be gone */
      }
    }
    connection.release();
  }
}

function unionKeys(...lists) {
  const set = new Set();
  for (const list of lists) {
    for (const key of list || []) {
      if (key != null) set.add(Number(key));
    }
  }
  return [...set].sort((a, b) => a - b);
}

async function advanceStates(perSource, syncAt, full = false) {
  for (const source of perSource || []) {
    await stateStore.setState(source.table, {
      highWaterId: source.maxId || 0,
      syncAt,
      rows: source.rows || 0,
      full,
    });
  }
}

async function runIncremental({ states }) {
  return facts.syncAll({ mode: 'incremental', states });
}

async function runWindow({ fromKey, toKey }) {
  await facts.deleteFactsInRange(fromKey, toKey);
  return facts.syncAll({ mode: 'window', fromKey, toKey });
}

async function runFull() {
  await facts.truncateFacts();
  await aggs.truncateAggregates();
  return facts.syncAll({ mode: 'full' });
}

/**
 * @param {object} opts
 * @param {'incremental'|'daily'|'backfill'|'rebuild'} [opts.mode]
 * @param {string|number} [opts.from]  YYYY-MM-DD or date key (backfill only)
 * @param {string|number} [opts.to]
 */
async function runEtl({ mode = 'incremental', from = null, to = null } = {}) {
  assertEnabled();

  if (runningPromise) {
    const error = new Error('Warehouse ETL is already running');
    error.code = 'ETL_ALREADY_RUNNING';
    throw error;
  }

  const operation = withEtlLock(async () => {
    if (AUTO_INIT) await ensureSchema();

    const startedAtMs = Date.now();
    const today = todayDateKey();
    const runId = await stateStore.startRun(mode);

    try {
      await dims.refreshAll();

      let result = { rows: 0, dateKeys: [], perSource: [] };
      let affected = [];
      let effectiveMode = mode;
      let stateSyncAt = null;
      let advanceFull = false;

      if (mode === 'rebuild') {
        stateSyncAt = new Date();
        advanceFull = true;
        result = await runFull();
        affected = unionKeys(result.dateKeys);
      } else if (mode === 'backfill') {
        const hasWindow = from != null || to != null;
        if (hasWindow && (from == null || to == null)) {
          throw new Error('A window backfill requires both `from` and `to`');
        }
        const fromKey = from != null ? parseDateKey(from) : null;
        const toKey = to != null ? parseDateKey(to) : null;
        if (hasWindow && (!fromKey || !toKey)) {
          throw new Error('`from`/`to` must be valid YYYY-MM-DD dates');
        }
        if (fromKey && toKey) {
          if (fromKey > toKey) throw new Error('`from` must be before `to`');
          result = await runWindow({ fromKey, toKey });
          affected = unionKeys(result.dateKeys, dateKeyRange(fromKey, toKey));
          effectiveMode = 'backfill:window';
        } else {
          stateSyncAt = new Date();
          advanceFull = true;
          result = await runFull();
          affected = unionKeys(result.dateKeys);
          effectiveMode = 'backfill:full';
        }
      } else if (mode === 'daily') {
        stateSyncAt = new Date();
        const incremental = await runIncremental({
          states: Object.fromEntries((await stateStore.listState()).map((r) => [r.source_table, r])),
        });
        const reconcileFrom = shiftDateKey(today, -RECONCILE_DAYS);
        const window = await runWindow({ fromKey: reconcileFrom, toKey: today });
        result = {
          rows: incremental.rows + window.rows,
          dateKeys: unionKeys(incremental.dateKeys, window.dateKeys),
          perSource: [...incremental.perSource, ...window.perSource],
        };
        affected = unionKeys(
          incremental.dateKeys,
          window.dateKeys,
          dateKeyRange(reconcileFrom, today)
        );
        effectiveMode = 'daily';
      } else {
        stateSyncAt = new Date();
        const states = Object.fromEntries((await stateStore.listState()).map((r) => [r.source_table, r]));
        result = await runIncremental({ states });
        affected = unionKeys(result.dateKeys);
        effectiveMode = 'incremental';
      }

      // Always refresh today so dashboards show intraday data even if the
      // watermark misses a row committed during the run.
      affected = unionKeys(affected, [today]);

      const snapshot = await snapshotInventory(today);
      const aggregates = await aggs.rebuildForDateKeys(affected);

      // Only move the watermark once facts AND aggregates are durable. If the
      // aggregate step fails, the next run re-reads the same rows (idempotent).
      if (stateSyncAt) {
        await advanceStates(result.perSource, stateSyncAt, advanceFull);
      }

      const summary = {
        mode: effectiveMode,
        rows: result.rows,
        dateKeys: aggregates.dateKeys,
        monthKeys: aggregates.monthKeys,
        snapshot,
        perSource: result.perSource,
        durationMs: Date.now() - startedAtMs,
      };

      await stateStore.finishRun(runId, {
        rowsStaged: result.rows,
        dateKeys: affected.length,
      });

      // Fresh partitions → drop any cached report payloads immediately.
      reportCache.clear();

      return summary;
    } catch (error) {
      await stateStore.failRun(runId, error).catch(() => {});
      throw error;
    }
  });

  runningPromise = operation;
  try {
    return await operation;
  } finally {
    if (runningPromise === operation) runningPromise = null;
  }
}

function isRunning() {
  return Boolean(runningPromise);
}

module.exports = {
  runEtl,
  isRunning,
  RECONCILE_DAYS,
};
