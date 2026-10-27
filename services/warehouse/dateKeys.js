/**
 * Single source of truth for business dates in the warehouse.
 *
 * OLTP stores timestamps in UTC. Every report is viewed in the configured
 * business timezone (default Asia/Karachi). Dates are normalised ONCE here,
 * during ETL, and stored as integer keys:
 *
 *   date_key  = YYYYMMDD  (e.g. 20261023)
 *   month_key = YYYYMM    (e.g. 202610)
 */

const REPORT_TIMEZONE_NAME = process.env.REPORT_TIMEZONE_NAME || 'Asia/Karachi';

const partsFormatter = new Intl.DateTimeFormat('en-US', {
  timeZone: REPORT_TIMEZONE_NAME,
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
});

const isValidDate = (d) => d instanceof Date && !Number.isNaN(d.getTime());

function businessParts(value) {
  if (value === null || value === undefined || value === '') return null;
  const d = value instanceof Date ? value : new Date(value);
  if (!isValidDate(d)) return null;
  const parts = partsFormatter.formatToParts(d);
  const get = (type) => parts.find((p) => p.type === type)?.value;
  const year = Number(get('year'));
  const month = Number(get('month'));
  const day = Number(get('day'));
  if (!year || !month || !day) return null;
  return { year, month, day };
}

/** UTC timestamp (Date | ISO string) -> business date key YYYYMMDD. */
function toDateKey(value) {
  const p = businessParts(value);
  if (!p) return null;
  return p.year * 10000 + p.month * 100 + p.day;
}

/** UTC timestamp (Date | ISO string) -> business month key YYYYMM. */
function toMonthKey(value) {
  const p = businessParts(value);
  if (!p) return null;
  return p.year * 100 + p.month;
}

/** UTC timestamp -> ISO business date "YYYY-MM-DD". */
function toDateOnly(value) {
  const key = toDateKey(value);
  return key == null ? null : dateKeyToIso(key);
}

/** "YYYY-MM-DD" (already a business date) -> 20261023. No TZ conversion. */
function parseDateKey(value) {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'number' && Number.isInteger(value)) return value;
  const m = String(value).trim().match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return null;
  const y = Number(m[1]);
  const mo = Number(m[2]);
  const d = Number(m[3]);
  if (mo < 1 || mo > 12 || d < 1 || d > 31) return null;
  return y * 10000 + mo * 100 + d;
}

function dateKeyToIso(key) {
  if (key == null) return null;
  const s = String(key).padStart(8, '0');
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
}

function dateKeyToMonthKey(key) {
  return key == null ? null : Math.floor(key / 100);
}

function monthKeyToYearMonth(monthKey) {
  if (monthKey == null) return null;
  const s = String(monthKey).padStart(6, '0');
  return `${s.slice(0, 4)}-${s.slice(4, 6)}`;
}

function todayDateKey(now = new Date()) {
  return toDateKey(now);
}

/** Shift a date key by N calendar days (UTC math on a synthetic midnight). */
function shiftDateKey(key, days) {
  if (key == null) return null;
  const s = String(key).padStart(8, '0');
  const d = new Date(Date.UTC(
    Number(s.slice(0, 4)),
    Number(s.slice(4, 6)) - 1,
    Number(s.slice(6, 8))
  ));
  if (!isValidDate(d)) return null;
  d.setUTCDate(d.getUTCDate() + Number(days || 0));
  return d.getUTCFullYear() * 10000 + (d.getUTCMonth() + 1) * 100 + d.getUTCDate();
}

/** Inclusive list of date keys between two keys. */
function dateKeyRange(startKey, endKey) {
  if (startKey == null || endKey == null || startKey > endKey) return [];
  const out = [];
  let cur = startKey;
  // 10 years guard
  for (let i = 0; i < 3660 && cur <= endKey; i += 1) {
    out.push(cur);
    cur = shiftDateKey(cur, 1);
  }
  return out;
}

/**
 * Business date for a sale: backdated sales store the intended date in
 * sales.sale_date; otherwise use created_at converted to the business TZ.
 */
function saleBusinessDate({ sale_date, created_at }) {
  if (sale_date) {
    const key = parseDateKey(sale_date instanceof Date ? toDateOnly(sale_date) : sale_date);
    if (key != null) return key;
  }
  return toDateKey(created_at);
}

/** Rows of SQL date values -> distinct non-null keys. */
function collectDateKeys(rows, getter = (r) => r.date_key) {
  const set = new Set();
  for (const row of rows || []) {
    const key = getter(row);
    if (key != null) set.add(key);
  }
  return [...set].sort((a, b) => a - b);
}

function collectMonthKeys(dateKeys) {
  return [...new Set((dateKeys || []).map(dateKeyToMonthKey).filter((k) => k != null))].sort((a, b) => a - b);
}

module.exports = {
  REPORT_TIMEZONE_NAME,
  toDateKey,
  toMonthKey,
  toDateOnly,
  parseDateKey,
  dateKeyToIso,
  dateKeyToMonthKey,
  monthKeyToYearMonth,
  todayDateKey,
  shiftDateKey,
  dateKeyRange,
  saleBusinessDate,
  collectDateKeys,
  collectMonthKeys,
};
