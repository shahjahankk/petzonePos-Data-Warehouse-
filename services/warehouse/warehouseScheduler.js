/**
 * Background warehouse scheduler.
 *
 * No cron dependency: a self-rescheduling timeout fires the daily load at the
 * configured business-time wall clock, and an interval keeps the warehouse
 * fresh during the day. cPanel deployments can disable this entirely and call
 * `scripts/warehouse-sync-once.js --mode=daily` from cron instead.
 */

const { warehouseEnabled } = require('../../config/warehouseDatabase');
const { REPORT_TIMEZONE_NAME } = require('./dateKeys');
const { runEtl, isRunning } = require('./etlOrchestrator');

const ENABLED = String(process.env.DW_SCHEDULER_ENABLED || 'true').toLowerCase() !== 'false';
const DAILY_TIME = process.env.DW_DAILY_SYNC_TIME || '12:00';
const INCREMENTAL_MINUTES = Math.max(1, parseInt(process.env.DW_INCREMENTAL_MINUTES, 10) || 15);
const STARTUP_DELAY_MS = Math.max(0, parseInt(process.env.DW_STARTUP_DELAY_SECONDS, 10) || 15) * 1000;

let timers = [];
let dailyTimer = null;
let started = false;

const partsFormatter = new Intl.DateTimeFormat('en-US', {
  timeZone: REPORT_TIMEZONE_NAME,
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
});

function businessParts(date) {
  const parts = partsFormatter.formatToParts(date);
  const get = (type) => Number(parts.find((p) => p.type === type)?.value);
  return {
    year: get('year'),
    month: get('month'),
    day: get('day'),
    hour: get('hour') % 24,
    minute: get('minute'),
    second: get('second'),
  };
}

/** Offset (ms) of the business timezone at a given instant. */
function zoneOffsetMs(date) {
  const p = businessParts(date);
  return Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second) - date.getTime();
}

/** Convert a wall-clock time in the business timezone to a real UTC Date. */
function zonedWallTimeToUtc(year, month, day, hours, minutes) {
  const guess = Date.UTC(year, month - 1, day, hours, minutes, 0, 0);
  // Two passes handle DST transitions correctly.
  let timestamp = guess - zoneOffsetMs(new Date(guess));
  timestamp = guess - zoneOffsetMs(new Date(timestamp));
  return new Date(timestamp);
}

function parseDailyTime(value) {
  const m = String(value || '').trim().match(/^(\d{1,2}):(\d{2})$/);
  if (!m) return { hours: 12, minutes: 0 };
  const hours = Math.min(23, Math.max(0, Number(m[1])));
  const minutes = Math.min(59, Math.max(0, Number(m[2])));
  return { hours, minutes };
}

/** Next occurrence of HH:mm in the business timezone, as a real UTC Date. */
function nextDailyRunAt(now = new Date()) {
  const { hours, minutes } = parseDailyTime(DAILY_TIME);
  const p = businessParts(now);
  let target = zonedWallTimeToUtc(p.year, p.month, p.day, hours, minutes);
  if (target.getTime() <= now.getTime()) {
    const nextDay = new Date(Date.UTC(p.year, p.month - 1, p.day + 1));
    target = zonedWallTimeToUtc(
      nextDay.getUTCFullYear(),
      nextDay.getUTCMonth() + 1,
      nextDay.getUTCDate(),
      hours,
      minutes
    );
  }
  return target;
}

function log(message, extra) {
  const line = `[warehouse] ${message}`;
  if (extra !== undefined) console.log(line, extra);
  else console.log(line);
}

async function trigger(mode) {
  if (isRunning()) {
    log(`skipping ${mode} run: another ETL is in progress`);
    return null;
  }
  try {
    const summary = await runEtl({ mode });
    log(`${mode} run finished`, {
      rows: summary.rows,
      dates: summary.dateKeys,
      months: summary.monthKeys,
      snapshotRows: summary.snapshot?.rows,
      ms: summary.durationMs,
    });
    return summary;
  } catch (error) {
    console.error(`[warehouse] ${mode} run failed:`, error.message);
    return null;
  }
}

function startWarehouseScheduler() {
  if (!warehouseEnabled) {
    log('disabled (DW_ENABLED=false)');
    return { stop() {}, nextDailyRunAt: () => null };
  }
  if (!ENABLED) {
    log('scheduler disabled (DW_SCHEDULER_ENABLED=false)');
    return { stop() {}, nextDailyRunAt: () => null };
  }
  if (started) {
    return { stop: stopWarehouseScheduler, nextDailyRunAt: () => nextDailyRunAt() };
  }
  started = true;

  const startup = setTimeout(() => {
    trigger('incremental');
  }, STARTUP_DELAY_MS);
  if (startup.unref) startup.unref();
  timers.push(startup);

  const interval = setInterval(() => {
    trigger('incremental');
  }, INCREMENTAL_MINUTES * 60000);
  if (interval.unref) interval.unref();
  timers.push(interval);

  const scheduleDaily = () => {
    const next = nextDailyRunAt();
    const delay = Math.max(1000, next.getTime() - Date.now());
    log(`next daily sync scheduled`, { at: next.toISOString(), delayMs: delay });
    dailyTimer = setTimeout(async () => {
      await trigger('daily');
      scheduleDaily();
    }, delay);
    if (dailyTimer.unref) dailyTimer.unref();
  };
  scheduleDaily();

  return {
    stop: stopWarehouseScheduler,
    nextDailyRunAt: () => nextDailyRunAt(),
  };
}

function stopWarehouseScheduler() {
  for (const timer of timers) {
    clearTimeout(timer);
    clearInterval(timer);
  }
  timers = [];
  if (dailyTimer) {
    clearTimeout(dailyTimer);
    dailyTimer = null;
  }
  started = false;
}

module.exports = {
  startWarehouseScheduler,
  stopWarehouseScheduler,
  nextDailyRunAt,
  DAILY_TIME,
  INCREMENTAL_MINUTES,
};
