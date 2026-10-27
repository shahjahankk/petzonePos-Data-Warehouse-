/**
 * Warehouse schema bootstrap + lightweight introspection helpers.
 */

const fs = require('fs');
const path = require('path');
const { dwPool } = require('../../config/warehouseDatabase');
const {
  dateKeyToIso,
  dateKeyToMonthKey,
  shiftDateKey,
  todayDateKey,
} = require('./dateKeys');

const SCHEMA_PATH = path.join(__dirname, '..', '..', 'warehouse', 'schema.sql');

let schemaPromise = null;
const columnCache = new Map();

function splitStatements(sql) {
  return sql
    .replace(/\r\n/g, '\n')
    .split(/;\s*(?:\n|$)/)
    .map((s) => s.trim())
    .filter((s) => s && !s.split('\n').every((line) => line.trim().startsWith('--')));
}

async function ensureSchema({ force = false } = {}) {
  if (!dwPool) throw new Error('Warehouse is disabled (DW_ENABLED=false)');
  if (!schemaPromise || force) {
    schemaPromise = (async () => {
      const sql = fs.readFileSync(SCHEMA_PATH, 'utf8');
      const statements = splitStatements(sql);
      for (const statement of statements) {
        await dwPool.query(statement);
      }
      await seedDateDimension();
      return true;
    })().catch((error) => {
      schemaPromise = null;
      throw error;
    });
  }
  return schemaPromise;
}

/** Columns of a warehouse table, cached for the process lifetime. */
async function getTableColumns(table) {
  if (columnCache.has(table)) return columnCache.get(table);
  const [rows] = await dwPool.execute(
    `SELECT COLUMN_NAME AS name
       FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`,
    [table]
  );
  const set = new Set(rows.map((r) => r.name));
  columnCache.set(table, set);
  return set;
}

async function tableExists(table) {
  const [rows] = await dwPool.execute(
    `SELECT 1 AS ok FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? LIMIT 1`,
    [table]
  );
  return rows.length > 0;
}

const MONTH_NAMES = ['January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December'];
const DAY_NAMES = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

function buildDateRows(fromKey, toKey) {
  const rows = [];
  let key = fromKey;
  for (let i = 0; i < 366 * 30 && key <= toKey; i += 1) {
    const iso = dateKeyToIso(key);
    const d = new Date(`${iso}T00:00:00Z`);
    const year = d.getUTCFullYear();
    const month = d.getUTCMonth() + 1;
    const day = d.getUTCDate();
    const dow = d.getUTCDay(); // 0=Sunday
    const startOfYear = Date.UTC(year, 0, 1);
    const week = Math.floor((Date.UTC(year, month - 1, day) - startOfYear) / 86400000 / 7) + 1;
    rows.push([
      key,
      iso,
      year,
      Math.floor((month - 1) / 3) + 1,
      month,
      MONTH_NAMES[month - 1],
      dateKeyToMonthKey(key),
      `${year}-${String(month).padStart(2, '0')}`,
      day,
      dow === 0 ? 7 : dow, // ISO 1=Monday .. 7=Sunday
      DAY_NAMES[dow],
      week,
      dow === 0 || dow === 6 ? 1 : 0,
    ]);
    key = shiftDateKey(key, 1);
  }
  return rows;
}

async function seedDateDimension({ fromKey = 20150101, toKey } = {}) {
  if (!dwPool) return;
  const end = toKey || shiftDateKey(todayDateKey(), 400);
  const rows = buildDateRows(fromKey, end);
  const chunkSize = 1000;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    await dwPool.query(
      `INSERT INTO dim_date
        (date_key, full_date, year, quarter, month, month_name, month_key, \`year_month\`,
         day, day_of_week, day_name, week_of_year, is_weekend)
       VALUES ?
       ON DUPLICATE KEY UPDATE
         full_date=VALUES(full_date), year=VALUES(year), quarter=VALUES(quarter),
         month=VALUES(month), month_name=VALUES(month_name), month_key=VALUES(month_key),
         \`year_month\`=VALUES(\`year_month\`), day=VALUES(day), day_of_week=VALUES(day_of_week),
         day_name=VALUES(day_name), week_of_year=VALUES(week_of_year), is_weekend=VALUES(is_weekend)`,
      [chunk]
    );
  }
  return rows.length;
}

function clearColumnCache() {
  columnCache.clear();
}

module.exports = {
  SCHEMA_PATH,
  ensureSchema,
  getTableColumns,
  tableExists,
  seedDateDimension,
  clearColumnCache,
  buildDateRows,
};
