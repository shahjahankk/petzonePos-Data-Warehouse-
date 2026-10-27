/**
 * Daily inventory snapshot.
 *
 * The OLTP inventory report derives stock by summing `inventory_ledger_entries`
 * scoped to each item. That is correct but expensive, so we materialise one row
 * per product per day and let the report read a single small partition.
 *
 * Quantity is rebuilt from `fact_inventory_movements` (already in the warehouse),
 * which keeps the read path fully OLAP-only.
 */

const { dwPool } = require('../../config/warehouseDatabase');
const { todayDateKey } = require('./dateKeys');

const SNAPSHOT_SQL = `
INSERT INTO fact_inventory_snapshot (
  snapshot_date_key, product_key, scope_key, quantity,
  unit_cost, unit_price, value, min_stock_level, category
)
SELECT
  ?,
  p.product_key,
  p.scope_key,
  COALESCE(st.qty, 0)                       AS quantity,
  p.cost_price,
  p.selling_price,
  COALESCE(st.qty, 0) * p.cost_price        AS value,
  p.min_stock_level,
  p.category
FROM dim_product p
LEFT JOIN (
  SELECT m.product_key,
         m.scope_key,
         SUM(m.quantity_in - m.quantity_out) AS qty
    FROM fact_inventory_movements m
   WHERE m.date_key <= ?
     AND m.product_key IS NOT NULL
   GROUP BY m.product_key, m.scope_key
) st ON st.product_key = p.product_key
    AND st.scope_key <=> p.scope_key
WHERE p.is_deleted = 0
  AND p.scope_key IS NOT NULL`;

/**
 * Rebuild the snapshot for a business date (defaults to today).
 * @param {number} [dateKey]
 */
async function snapshotInventory(dateKey = todayDateKey()) {
  if (dateKey == null) return { dateKey: null, rows: 0 };
  await dwPool.execute(
    'DELETE FROM fact_inventory_snapshot WHERE snapshot_date_key = ?',
    [dateKey]
  );
  const [result] = await dwPool.execute(SNAPSHOT_SQL, [dateKey, dateKey]);
  return { dateKey, rows: result.affectedRows || 0 };
}

module.exports = { snapshotInventory };
