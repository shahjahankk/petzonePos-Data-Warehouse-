/**
 * Aggregate (mart) rebuilds.
 *
 * Reports only ever read these tables. Aggregates are partitioned by date/month;
 * the ETL deletes and rebuilds only the partitions affected by changed facts.
 */

const { dwPool } = require('../../config/warehouseDatabase');
const { collectMonthKeys, monthKeyToYearMonth } = require('./dateKeys');
const { bulkUpsert } = require('./dimensionService');

const CHUNK = 400;

function chunks(list, size = CHUNK) {
  const out = [];
  for (let i = 0; i < list.length; i += size) out.push(list.slice(i, i + size));
  return out;
}

const placeholders = (n) => Array(n).fill('?').join(', ');

async function replaceDaily(table, dateKeys, insertSql) {
  for (const chunk of chunks(dateKeys)) {
    const ph = placeholders(chunk.length);
    await dwPool.query(`DELETE FROM ${table} WHERE date_key IN (${ph})`, chunk);
    await dwPool.query(insertSql(chunk.length), chunk);
  }
}

const SALES_DAILY_SQL = (n) => `
INSERT INTO agg_sales_daily (
  date_key, scope_key, user_key, payment_method, payment_group,
  transaction_count, sale_count, gross_sales, product_sales, clinic_sales,
  paid_amount, collected_amount, credit_amount, discount, tax, cogs, gross_profit,
  cash_count, card_count, credit_count, partial_count,
  refund_amount, refund_count, settlement_amount, settlement_count
)
SELECT
  t.date_key, t.scope_key, t.user_key, t.payment_method, t.payment_group,
  SUM(CASE WHEN t.is_real_sale = 1 AND t.product_total > 0 THEN 1 ELSE 0 END),
  SUM(CASE WHEN t.is_real_sale = 1 THEN 1 ELSE 0 END),
  SUM(CASE WHEN t.is_real_sale = 1 THEN t.total ELSE 0 END),
  SUM(CASE WHEN t.is_real_sale = 1 AND t.product_total > 0
           THEN t.product_total - (CASE WHEN t.subtotal > 0
                                        THEN t.discount * t.product_total / t.subtotal
                                        ELSE 0 END)
           ELSE 0 END),
  SUM(CASE WHEN t.is_real_sale = 1 THEN t.clinic_total ELSE 0 END),
  SUM(CASE WHEN t.is_real_sale = 1 THEN t.product_paid ELSE 0 END),
  SUM(CASE WHEN t.is_real_sale = 1 THEN t.product_collected ELSE 0 END),
  SUM(CASE WHEN t.is_real_sale = 1 THEN t.product_credit ELSE 0 END),
  SUM(CASE WHEN t.is_real_sale = 1 AND t.product_total > 0
           THEN (CASE WHEN t.subtotal > 0
                      THEN t.discount * t.product_total / t.subtotal
                      ELSE 0 END)
           ELSE 0 END),
  SUM(CASE WHEN t.is_real_sale = 1 THEN t.product_tax ELSE 0 END),
  SUM(CASE WHEN t.is_real_sale = 1 THEN t.cogs ELSE 0 END),
  SUM(CASE WHEN t.is_real_sale = 1 THEN t.gross_profit ELSE 0 END),
  SUM(CASE WHEN t.is_real_sale = 1 AND t.product_total > 0 AND t.payment_group = 'CASH' THEN 1 ELSE 0 END),
  SUM(CASE WHEN t.is_real_sale = 1 AND t.product_total > 0 AND t.payment_group = 'CARD' THEN 1 ELSE 0 END),
  SUM(CASE WHEN t.is_real_sale = 1 AND t.product_total > 0 AND t.payment_group = 'FULLY_CREDIT' THEN 1 ELSE 0 END),
  SUM(CASE WHEN t.is_real_sale = 1 AND t.product_total > 0 AND t.payment_group = 'PARTIAL' THEN 1 ELSE 0 END),
  SUM(CASE WHEN t.is_refund = 1\n           THEN ABS(CASE WHEN COALESCE(t.payment_amount, 0) = 0 THEN t.total ELSE t.payment_amount END)\n           ELSE 0 END),
  SUM(CASE WHEN t.is_refund = 1 THEN 1 ELSE 0 END),
  SUM(CASE WHEN t.is_settlement = 1 THEN t.payment_amount ELSE 0 END),
  SUM(CASE WHEN t.is_settlement = 1 THEN 1 ELSE 0 END)
FROM (
  SELECT
    f.date_key,
    COALESCE(f.scope_key, 0)  AS scope_key,
    COALESCE(f.user_key, 0)   AS user_key,
    COALESCE(f.payment_method, '') AS payment_method,
    f.is_real_sale, f.is_refund, f.is_settlement,
    f.total, f.subtotal, f.discount, f.product_total, f.clinic_total, f.payment_amount,
    f.product_paid, f.product_collected, f.product_credit,
    f.product_discount, f.product_tax, f.cogs, f.gross_profit,
    CASE
      WHEN UPPER(COALESCE(f.payment_method, '')) IN ('CASH_REFUND', 'REFUND') THEN 'REFUND'
      WHEN UPPER(COALESCE(f.payment_type, '')) = 'OUTSTANDING_SETTLEMENT' THEN 'SETTLEMENT'
      WHEN UPPER(COALESCE(f.payment_method, '')) = 'FULLY_CREDIT' THEN 'FULLY_CREDIT'
      WHEN UPPER(COALESCE(f.payment_method, '')) = 'PARTIAL'
        OR UPPER(COALESCE(f.payment_type, '')) = 'PARTIAL'
        OR UPPER(COALESCE(f.payment_status, '')) = 'PARTIAL' THEN 'PARTIAL'
      WHEN UPPER(COALESCE(f.payment_method, '')) IN ('CARD', 'CREDIT_CARD') THEN 'CARD'
      WHEN UPPER(COALESCE(f.payment_method, '')) = 'MOBILE_PAYMENT' THEN 'MOBILE'
      ELSE 'CASH'
    END AS payment_group
  FROM fact_sales f
  WHERE f.date_key IN (${placeholders(n)})
    AND f.deleted_at IS NULL
    AND f.status = 'COMPLETED'
) t
GROUP BY t.date_key, t.scope_key, t.user_key, t.payment_method, t.payment_group`;

const PRODUCT_DAILY_SQL = (n) => `
INSERT INTO agg_product_daily (
  date_key, scope_key, user_key, product_key, quantity, revenue, cogs, gross_profit, \`lines\`, zero_cost_lines
)
SELECT
  i.date_key,
  COALESCE(i.scope_key, 0),
  COALESCE(i.user_key, 0),
  i.product_key,
  SUM(i.quantity),
  SUM(i.total),
  SUM(i.cogs),
  SUM(i.total - i.cogs),
  COUNT(*),
  SUM(CASE WHEN i.cost_price = 0 THEN 1 ELSE 0 END)
FROM fact_sale_items i
INNER JOIN fact_sales s ON s.id = i.sale_id
WHERE i.date_key IN (${placeholders(n)})
  AND i.is_clinic_line = 0
  AND i.product_key IS NOT NULL
  AND s.status = 'COMPLETED'
  AND s.deleted_at IS NULL
  AND s.is_real_sale = 1
GROUP BY i.date_key, COALESCE(i.scope_key, 0), COALESCE(i.user_key, 0), i.product_key`;

const CLINIC_DAILY_SQL = (n) => `
INSERT INTO agg_clinic_daily (
  date_key, scope_key, user_key, service_key, quantity, revenue, \`lines\`
)
SELECT
  i.date_key,
  COALESCE(i.scope_key, 0),
  COALESCE(i.user_key, 0),
  i.service_key,
  SUM(i.quantity),
  SUM(i.total),
  COUNT(*)
FROM fact_sale_items i
INNER JOIN fact_sales s ON s.id = i.sale_id
WHERE i.date_key IN (${placeholders(n)})
  AND i.is_clinic_line = 1
  AND i.service_key IS NOT NULL
  AND s.status = 'COMPLETED'
  AND s.deleted_at IS NULL
  AND s.clinic_eligible = 1
GROUP BY i.date_key, COALESCE(i.scope_key, 0), COALESCE(i.user_key, 0), i.service_key`;

const INVENTORY_DAILY_SQL = (n) => `
INSERT INTO agg_inventory_daily (
  date_key, scope_key, product_key,
  received_qty, purchase_qty, sold_qty, returned_qty,
  adjustment_qty, transfer_in_qty, transfer_out_qty
)
SELECT
  m.date_key,
  COALESCE(m.scope_key, 0),
  m.product_key,
  SUM(CASE WHEN m.event_type IN ('PURCHASE', 'TRANSFER_IN', 'RESTOCK', 'OPENING') THEN m.quantity_in ELSE 0 END),
  SUM(CASE WHEN m.event_type = 'PURCHASE' THEN m.quantity_in ELSE 0 END),
  SUM(CASE WHEN m.event_type = 'SALE' THEN m.quantity_out ELSE 0 END),
  SUM(CASE WHEN m.event_type = 'RETURN' THEN m.quantity_in ELSE 0 END),
  SUM(CASE WHEN m.event_type = 'ADJUSTMENT' THEN m.quantity_in - m.quantity_out ELSE 0 END),
  SUM(CASE WHEN m.event_type = 'TRANSFER_IN' THEN m.quantity_in ELSE 0 END),
  SUM(CASE WHEN m.event_type = 'TRANSFER_OUT' THEN m.quantity_out ELSE 0 END)
FROM fact_inventory_movements m
WHERE m.date_key IN (${placeholders(n)})
  AND m.product_key IS NOT NULL
GROUP BY m.date_key, COALESCE(m.scope_key, 0), m.product_key`;

const LEDGER_DAILY_SQL = (n) => `
INSERT INTO agg_ledger_daily (
  date_key, scope_key, account_type, entry_type, amount, entries
)
SELECT
  l.date_key,
  COALESCE(l.scope_key, 0),
  COALESCE(NULLIF(l.account_type, ''), 'General'),
  COALESCE(NULLIF(l.entry_type, ''), 'OTHER'),
  SUM(CASE WHEN UPPER(l.entry_type) = 'DEBIT' THEN l.debit
           WHEN UPPER(l.entry_type) = 'CREDIT' THEN l.credit
           ELSE l.debit + l.credit END),
  COUNT(*)
FROM fact_ledger_entries l
WHERE l.date_key IN (${placeholders(n)})
GROUP BY l.date_key, COALESCE(l.scope_key, 0),
         COALESCE(NULLIF(l.account_type, ''), 'General'),
         COALESCE(NULLIF(l.entry_type, ''), 'OTHER')`;

async function replaceMonthly(table, monthKeys, insertSql) {
  for (const chunk of chunks(monthKeys)) {
    const ph = placeholders(chunk.length);
    await dwPool.query(`DELETE FROM ${table} WHERE month_key IN (${ph})`, chunk);
    await dwPool.query(insertSql(chunk.length), chunk);
  }
}

const EXPENSE_MONTHLY_SQL = (n) => `
INSERT INTO agg_expense_monthly (
  month_key, scope_key, expense_category, amount, entries
)
SELECT
  d.month_key,
  COALESCE(e.scope_key, 0),
  COALESCE(NULLIF(e.expense_category, ''), NULLIF(e.category, ''), 'Other'),
  SUM(e.amount),
  COUNT(*)
FROM fact_expenses e
INNER JOIN dim_date d ON d.date_key = e.date_key
WHERE e.date_key IN (${placeholders(n)})
  AND e.type = 'EXPENSE'
  AND e.status IN ('APPROVED', 'PENDING')
GROUP BY d.month_key, COALESCE(e.scope_key, 0),
         COALESCE(NULLIF(e.expense_category, ''), NULLIF(e.category, ''), 'Other')`;

async function rebuildFinancialMonthly(monthKeys) {
  if (!monthKeys.length) return;
  const rows = new Map();

  for (const chunk of chunks(monthKeys)) {
    const ph = placeholders(chunk.length);

    const [sales] = await dwPool.query(
      `SELECT d.month_key, COALESCE(a.scope_key, 0) AS scope_key,
              SUM(a.gross_sales) AS revenue,
              SUM(a.sale_count)  AS sales_count,
              SUM(a.cogs)        AS cogs
         FROM agg_sales_daily a
         INNER JOIN dim_date d ON d.date_key = a.date_key
        WHERE d.month_key IN (${ph})
        GROUP BY d.month_key, COALESCE(a.scope_key, 0)`,
      chunk
    );

    const [vouchers] = await dwPool.query(
      `SELECT d.month_key, COALESCE(e.scope_key, 0) AS scope_key,
              SUM(CASE WHEN e.type = 'EXPENSE' THEN e.amount ELSE 0 END) AS expenses,
              SUM(CASE WHEN e.type = 'INCOME'  THEN e.amount ELSE 0 END) AS income
         FROM fact_expenses e
         INNER JOIN dim_date d ON d.date_key = e.date_key
        WHERE d.month_key IN (${ph})
          AND e.status IN ('APPROVED', 'PENDING')
        GROUP BY d.month_key, COALESCE(e.scope_key, 0)`,
      chunk
    );

    for (const r of sales) {
      const key = `${r.month_key}|${r.scope_key}`;
      rows.set(key, {
        month_key: Number(r.month_key),
        scope_key: Number(r.scope_key),
        revenue: Number(r.revenue) || 0,
        sales_count: Number(r.sales_count) || 0,
        cogs: Number(r.cogs) || 0,
        expenses: 0,
        income: 0,
      });
    }
    for (const r of vouchers) {
      const key = `${r.month_key}|${r.scope_key}`;
      const entry = rows.get(key) || {
        month_key: Number(r.month_key),
        scope_key: Number(r.scope_key),
        revenue: 0,
        sales_count: 0,
        cogs: 0,
        expenses: 0,
        income: 0,
      };
      entry.expenses = Number(r.expenses) || 0;
      entry.income = Number(r.income) || 0;
      rows.set(key, entry);
    }
  }

  const data = [...rows.values()].map((r) => {
    const grossProfit = r.revenue - r.cogs;
    return [
      r.month_key,
      r.scope_key,
      r.revenue,
      r.sales_count,
      r.cogs,
      grossProfit,
      r.expenses,
      r.income,
      grossProfit - r.expenses,
    ];
  });

  for (const chunk of chunks(monthKeys)) {
    const ph = placeholders(chunk.length);
    await dwPool.query(`DELETE FROM agg_financial_monthly WHERE month_key IN (${ph})`, chunk);
  }

  await bulkUpsert(
    'agg_financial_monthly',
    ['month_key', 'scope_key', 'revenue', 'sales_count', 'cogs', 'gross_profit',
      'expenses', 'income', 'net_profit'],
    data,
    ['revenue', 'sales_count', 'cogs', 'gross_profit', 'expenses', 'income', 'net_profit'],
    250
  );
}

/**
 * Rebuild every aggregate partition touched by the given business dates.
 * @param {number[]} dateKeys
 */
async function rebuildForDateKeys(dateKeys) {
  const keys = [...new Set((dateKeys || []).filter((k) => k != null))].sort((a, b) => a - b);
  if (!keys.length) return { dateKeys: 0, monthKeys: 0 };

  await replaceDaily('agg_sales_daily', keys, SALES_DAILY_SQL);
  await replaceDaily('agg_product_daily', keys, PRODUCT_DAILY_SQL);
  await replaceDaily('agg_clinic_daily', keys, CLINIC_DAILY_SQL);
  await replaceDaily('agg_inventory_daily', keys, INVENTORY_DAILY_SQL);
  await replaceDaily('agg_ledger_daily', keys, LEDGER_DAILY_SQL);

  const monthKeys = collectMonthKeys(keys);
  await replaceMonthly('agg_expense_monthly', monthKeys, EXPENSE_MONTHLY_SQL);
  await rebuildFinancialMonthly(monthKeys);

  return { dateKeys: keys.length, monthKeys: monthKeys.length };
}

/** Wipe all aggregates (used by full rebuilds). */
async function truncateAggregates() {
  const tables = [
    'agg_sales_daily',
    'agg_product_daily',
    'agg_clinic_daily',
    'agg_inventory_daily',
    'agg_ledger_daily',
    'agg_expense_monthly',
    'agg_financial_monthly',
  ];
  for (const table of tables) {
    await dwPool.query(`TRUNCATE TABLE ${table}`);
  }
}

module.exports = {
  rebuildForDateKeys,
  truncateAggregates,
  monthKeyToYearMonth,
};
