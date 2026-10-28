/**
 * Warehouse-backed report queries.
 *
 * Every function here reads ONLY from dim_* / fact_* / agg_* tables. The shapes
 * mirror the legacy `controllers/reportsController.js` responses so the existing
 * frontend keeps working unchanged; the POS backend proxies the same URLs to
 * this service when WAREHOUSE_SERVICE_URL is configured.
 */

const { dwPool } = require('../../config/warehouseDatabase');
const cache = require('./reportCache');
const {
  parseDateKey,
  dateKeyToIso,
  monthKeyToYearMonth,
  todayDateKey,
  shiftDateKey,
} = require('./dateKeys');

const num = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};
const round = (v, places = 2) => {
  const n = num(v);
  const factor = 10 ** places;
  return Math.round(n * factor) / factor;
};
const ph = (n) => Array(n).fill('?').join(', ');
const unique = (list) => [...new Set(list.filter((v) => v != null))];

// ─────────────────────────────────────────────────────────────────────────────
// Filter context
// ─────────────────────────────────────────────────────────────────────────────

async function scopeKeysForAlias(type, raw) {
  if (raw === null || raw === undefined || raw === '') return [];
  const [rows] = await dwPool.execute(
    `SELECT DISTINCT scope_key
       FROM map_scope_alias
      WHERE scope_type = ?
        AND (alias = ? OR LOWER(alias) = LOWER(?))`,
    [type, String(raw), String(raw)]
  );
  return rows.map((r) => Number(r.scope_key));
}

async function allScopeKeys(type) {
  const [rows] = await dwPool.execute(
    'SELECT scope_key FROM dim_scope WHERE scope_type = ?',
    [type]
  );
  return rows.map((r) => Number(r.scope_key));
}

async function userKeysFor(raw) {
  const value = String(raw).trim();
  if (!value) return [];
  const numeric = Number(value);
  const [rows] = await dwPool.execute(
    `SELECT user_key
       FROM dim_user
      WHERE user_id = ?
         OR username = ?
         OR LOWER(username) = LOWER(?)`,
    [Number.isFinite(numeric) ? numeric : -1, value, value]
  );
  return rows.map((r) => Number(r.user_key));
}

function intersect(a, b) {
  if (a == null) return b;
  if (b == null) return a;
  const set = new Set(b);
  return a.filter((v) => set.has(v));
}

async function buildContext(user, query = {}) {
  const ctx = {
    scopeKeys: null,
    userKeys: null,
    startKey: null,
    endKey: null,
    impossible: false,
  };

  // Scope to nothing (not "everything") when a scoped user's own branch or
  // warehouse is missing from the dimension, so a stale dimension can never
  // leak other locations' data.
  if (user?.role === 'WAREHOUSE_KEEPER') {
    ctx.scopeKeys = user.warehouseId
      ? await scopeKeysForAlias('WAREHOUSE', user.warehouseId)
      : await allScopeKeys('WAREHOUSE');
  } else if (user?.role === 'CASHIER') {
    ctx.scopeKeys = user.branchId
      ? await scopeKeysForAlias('BRANCH', user.branchId)
      : await allScopeKeys('BRANCH');
  }

  const branch = query.branch || query.scope;
  if (branch && branch !== 'all') {
    const keys = unique([
      ...(await scopeKeysForAlias('BRANCH', branch)),
      ...(await scopeKeysForAlias('WAREHOUSE', branch)),
    ]);
    ctx.scopeKeys = intersect(ctx.scopeKeys, keys);
  }

  if (query.cashier && query.cashier !== 'all') {
    ctx.userKeys = await userKeysFor(query.cashier);
  }

  // Different clients use different names for the same range. Accept them all
  // so a filter can never be silently ignored because of a parameter-name
  // mismatch (sales/clinic use startDate/endDate; financial uses dateFrom/dateTo).
  const rawStart = query.startDate ?? query.dateFrom ?? query.from;
  const rawEnd = query.endDate ?? query.dateTo ?? query.to;
  // Shift session range — acts as an additional date constraint (intersected
  // with the main range so only sales within both ranges are included).
  const rawShiftStart = query.shift_start ?? query.shiftStart;
  const rawShiftEnd   = query.shift_end   ?? query.shiftEnd;
  let startKey = rawStart ? parseDateKey(rawStart) : null;
  let endKey   = rawEnd   ? parseDateKey(rawEnd)   : null;
  if (rawShiftStart) {
    const sk = parseDateKey(rawShiftStart);
    startKey = startKey == null ? sk : Math.max(startKey, sk);
  }
  if (rawShiftEnd) {
    const ek = parseDateKey(rawShiftEnd);
    endKey = endKey == null ? ek : Math.min(endKey, ek);
  }
  if (startKey && endKey && startKey > endKey) {
    [startKey, endKey] = [endKey, startKey];
  }
  ctx.startKey = startKey;
  ctx.endKey = endKey;

  if (ctx.scopeKeys && !ctx.scopeKeys.length) ctx.impossible = true;
  if (ctx.userKeys && !ctx.userKeys.length) ctx.impossible = true;

  return ctx;
}

function filterSql(alias, ctx, { includeUser = true } = {}) {
  const parts = [];
  const params = [];

  if (ctx.startKey) {
    parts.push(`${alias}.date_key >= ?`);
    params.push(ctx.startKey);
  }
  if (ctx.endKey) {
    parts.push(`${alias}.date_key <= ?`);
    params.push(ctx.endKey);
  }
  if (ctx.scopeKeys) {
    if (!ctx.scopeKeys.length) return { sql: ' AND 1 = 0', params: [] };
    parts.push(`${alias}.scope_key IN (${ph(ctx.scopeKeys.length)})`);
    params.push(...ctx.scopeKeys);
  }
  if (includeUser && ctx.userKeys) {
    if (!ctx.userKeys.length) return { sql: ' AND 1 = 0', params: [] };
    parts.push(`${alias}.user_key IN (${ph(ctx.userKeys.length)})`);
    params.push(...ctx.userKeys);
  }

  return {
    sql: parts.map((p) => ` AND ${p}`).join(''),
    params,
  };
}

async function cached(key, loader) {
  const hit = cache.get(key);
  if (hit) return hit;
  const value = await loader();
  cache.set(key, value);
  return value;
}

const contextKey = (ctx) => JSON.stringify(ctx);

// ─────────────────────────────────────────────────────────────────────────────
// Sales
// ─────────────────────────────────────────────────────────────────────────────

function emptySalesReport() {
  return {
    totalSales: 0,
    totalTransactions: 0,
    totalRevenue: 0,
    averageTicket: 0,
    grossProfit: 0,
    totalCostOfGoods: 0,
    grossProfitMargin: 0,
    costPriceWarning: false,
    cashSalesAmount: 0,
    cardSalesAmount: 0,
    fullyCreditTotal: 0,
    partialTotal: 0,
    partialCollected: 0,
    partialCredit: 0,
    totalCashReceived: 0,
    totalCreditGiven: 0,
    totalOutstanding: 0,
    realisedRevenue: 0,
    outstandingSettled: 0,
    outstandingSettlementCount: 0,
    refundTotal: 0,
    refundCount: 0,
    refunds: 0,
    fullyPaidCount: 0,
    fullyCreditCount: 0,
    partialCount: 0,
    discounts: 0,
    taxCollected: 0,
    cashSales: 0,
    cardSales: 0,
    salesByDate: [],
    salesByCashier: {},
    salesByCashierList: [],
    salesByBranch: {},
    salesByBranchList: [],
    paymentMethodBreakdown: [],
    recentSales: [],
    topProducts: [],
  };
}

async function getSalesReports(user, query = {}) {
  const ctx = await buildContext(user, query);
  if (ctx.impossible) return emptySalesReport();

  return cached(`sales:${user?.id}:${contextKey(ctx)}`, async () => {
    const w = filterSql('a', ctx);

    const [kpiRows] = await dwPool.execute(
      `SELECT
         COALESCE(SUM(a.transaction_count), 0) AS total_sales,
         COALESCE(SUM(a.product_sales), 0)     AS product_revenue,
         COALESCE(SUM(a.clinic_sales), 0)      AS clinic_revenue,
         COALESCE(SUM(a.product_sales), 0)     AS total_revenue,
         COALESCE(SUM(a.cogs), 0)              AS total_cogs,
         COALESCE(SUM(a.gross_profit), 0)      AS gross_profit,
         COALESCE(SUM(CASE WHEN a.payment_group = 'CASH' THEN a.product_sales ELSE 0 END), 0) AS cash_sales,
         COALESCE(SUM(CASE WHEN a.payment_group = 'CARD' THEN a.product_sales ELSE 0 END), 0) AS card_sales,
         COALESCE(SUM(CASE WHEN a.payment_group = 'MOBILE' THEN a.product_sales ELSE 0 END), 0) AS mobile_sales,
         COALESCE(SUM(CASE WHEN a.payment_group = 'FULLY_CREDIT' THEN a.product_sales ELSE 0 END), 0) AS fully_credit,
         COALESCE(SUM(CASE WHEN a.payment_group = 'PARTIAL' THEN a.product_sales ELSE 0 END), 0) AS partial_total,
         COALESCE(SUM(CASE WHEN a.payment_group = 'PARTIAL' THEN a.paid_amount ELSE 0 END), 0) AS partial_collected,
         COALESCE(SUM(CASE WHEN a.payment_group = 'PARTIAL' THEN a.credit_amount ELSE 0 END), 0) AS partial_credit,
         COALESCE(SUM(a.collected_amount), 0)  AS collected,
         COALESCE(SUM(a.credit_amount), 0)     AS credit_given,
         COALESCE(SUM(a.discount), 0)          AS discounts,
         COALESCE(SUM(a.tax), 0)               AS tax_collected,
         COALESCE(SUM(a.settlement_amount), 0) AS settled,
         COALESCE(SUM(a.settlement_count), 0)  AS settlement_count,
         COALESCE(SUM(a.refund_amount), 0)     AS refund_total,
         COALESCE(SUM(a.refund_count), 0)      AS refund_count,
         COALESCE(SUM(a.cash_count), 0)        AS cash_count,
         COALESCE(SUM(a.card_count), 0)        AS card_count,
         COALESCE(SUM(a.credit_count), 0)      AS credit_count,
         COALESCE(SUM(a.partial_count), 0)     AS partial_count
       FROM agg_sales_daily a
       WHERE 1 = 1 ${w.sql}`,
      w.params
    );

    const k = kpiRows[0] || {};
    const totalSales = num(k.total_sales);
    const totalRevenue = num(k.total_revenue);
    const totalCost = num(k.total_cogs);
    const grossProfit = totalRevenue - totalCost;
    const collected = num(k.collected);
    const settled = num(k.settled);
    const refundTotal = num(k.refund_total);
    const totalCashReceived = collected + settled - refundTotal;
    const totalCreditGiven = num(k.credit_given);

    const [zeroRows] = await dwPool.execute(
      `SELECT COALESCE(SUM(a.zero_cost_lines), 0) AS zero_cost
         FROM agg_product_daily a
        WHERE 1 = 1 ${w.sql}`,
      w.params
    );

    const [byDateRows] = await dwPool.execute(
      `SELECT a.date_key,
              SUM(a.product_sales) total,
              SUM(a.collected_amount + a.settlement_amount - a.refund_amount) collected,
              SUM(a.credit_amount) credit,
              SUM(a.transaction_count) transactions
         FROM agg_sales_daily a
        WHERE 1 = 1 ${w.sql}
        GROUP BY a.date_key
        ORDER BY a.date_key ASC`,
      w.params
    );

    const [byCashierRows] = await dwPool.execute(
      `SELECT a.user_key,
              COALESCE(u.username, CONCAT('User ', a.user_key)) AS cashier,
              SUM(a.product_sales) total,
              SUM(a.collected_amount + a.settlement_amount - a.refund_amount) collected,
              SUM(a.credit_amount) credit,
              SUM(a.transaction_count) transactions
         FROM agg_sales_daily a
         LEFT JOIN dim_user u ON u.user_key = a.user_key
        WHERE 1 = 1 ${w.sql}
        GROUP BY a.user_key, u.username
        ORDER BY total DESC`,
      w.params
    );

    const [byBranchRows] = await dwPool.execute(
      `SELECT a.scope_key,
              COALESCE(d.scope_name, d.scope_id, 'Unknown') AS branch,
              d.scope_id,
              SUM(a.product_sales) total,
              SUM(a.collected_amount + a.settlement_amount - a.refund_amount) collected,
              SUM(a.credit_amount) credit,
              SUM(a.transaction_count) transactions
         FROM agg_sales_daily a
         LEFT JOIN dim_scope d ON d.scope_key = a.scope_key
        WHERE 1 = 1 ${w.sql}
        GROUP BY a.scope_key, d.scope_name, d.scope_id
        ORDER BY total DESC`,
      w.params
    );

    // Legacy keys are the raw (uppercased) payment method, except partial
    // payments which are always reported under "PARTIAL".
    const [paymentRows] = await dwPool.execute(
      `SELECT CASE WHEN a.payment_group = 'PARTIAL' THEN 'PARTIAL'
                   ELSE UPPER(COALESCE(NULLIF(a.payment_method, ''), 'CASH')) END AS method,
              SUM(a.transaction_count) AS count,
              SUM(a.product_sales) AS total,
              SUM(a.paid_amount) AS collected,
              SUM(a.credit_amount) AS credit
         FROM agg_sales_daily a
        WHERE 1 = 1 ${w.sql}
        GROUP BY method
       HAVING SUM(a.transaction_count) > 0
        ORDER BY total DESC`,
      w.params
    );

    const fw = filterSql('f', ctx);
    const [recentRows] = await dwPool.execute(
      `SELECT f.sale_id, f.invoice_no, f.created_at_utc, f.date_key, f.customer_name,
              f.customer_phone, f.total, f.product_total, f.payment_amount,
              f.credit_amount, f.product_paid, f.product_credit, f.payment_method,
              f.payment_type, f.payment_status, f.credit_status, f.is_settlement,
              f.is_refund, f.user_key, d.scope_id, u.username
         FROM fact_sales f
         LEFT JOIN dim_scope d ON d.scope_key = f.scope_key
         LEFT JOIN dim_user u ON u.user_key = f.user_key
        WHERE f.deleted_at IS NULL
          AND f.status = 'COMPLETED'
          AND (f.is_refund = 1 OR f.is_settlement = 1 OR f.product_total > 0)
          ${fw.sql}
        ORDER BY f.created_at_utc DESC
        LIMIT 20`,
      fw.params
    );

    const [topProductRows] = await dwPool.execute(
      `SELECT COALESCE(p.name, 'Unknown') AS name,
              SUM(a.quantity) AS sold,
              SUM(a.revenue) AS revenue,
              SUM(a.cogs) AS cost,
              SUM(a.gross_profit) AS gross_profit
         FROM agg_product_daily a
         LEFT JOIN dim_product p ON p.product_key = a.product_key
        WHERE 1 = 1 ${w.sql}
        GROUP BY a.product_key, p.name
        ORDER BY sold DESC
        LIMIT 25`,
      w.params
    );

    const salesByDate = byDateRows.map((r) => {
      const total = num(r.total);
      const transactions = num(r.transactions);
      return {
        date: dateKeyToIso(r.date_key),
        total,
        collected: num(r.collected),
        credit: num(r.credit),
        transactions,
        avgTicket: transactions > 0 ? total / transactions : 0,
      };
    });

    const cashierList = byCashierRows.map((r) => ({
      cashier: r.cashier,
      total: num(r.total),
      collected: num(r.collected),
      credit: num(r.credit),
      transactions: num(r.transactions),
    }));
    const cashierMap = Object.fromEntries(cashierList.map((r) => [r.cashier, r]));

    const branchList = byBranchRows.map((r) => ({
      branch: r.branch,
      scopeId: r.scope_id,
      total: num(r.total),
      collected: num(r.collected),
      credit: num(r.credit),
      transactions: num(r.transactions),
    }));
    const branchMap = Object.fromEntries(branchList.map((r) => [r.branch, r]));

    return {
      totalSales,
      totalTransactions: totalSales,
      totalRevenue,
      clinicRevenue: num(k.clinic_revenue),
      // Average ticket excludes FULLY_CREDIT bills so a single large unpaid
      // credit sale cannot skew it (e.g. one 126,100 bill moves PetZone from
      // ~1,700 to ~3,539). Label the card "Avg Ticket (paid sales)" in the UI.
      averageTicket:
        totalSales > num(k.credit_count)
          ? (totalRevenue - num(k.fully_credit)) / (totalSales - num(k.credit_count))
          : 0,
      grossProfit,
      totalCostOfGoods: totalCost,
      grossProfitMargin: totalRevenue > 0 ? (grossProfit / totalRevenue) * 100 : 0,
      costPriceWarning: num(zeroRows[0]?.zero_cost) > 0,
      cashSalesAmount: num(k.cash_sales),
      cardSalesAmount: num(k.card_sales),
      mobileSalesAmount: num(k.mobile_sales),
      fullyCreditTotal: num(k.fully_credit),
      partialTotal: num(k.partial_total),
      partialCollected: num(k.partial_collected),
      partialCredit: num(k.partial_credit),
      totalCashReceived,
      totalCreditGiven,
      totalOutstanding: totalCreditGiven,
      realisedRevenue: totalCashReceived,
      outstandingSettled: settled,
      outstandingSettlementCount: num(k.settlement_count),
      refundTotal,
      refundCount: num(k.refund_count),
      refunds: refundTotal,
      fullyPaidCount: num(k.cash_count) + num(k.card_count),
      fullyCreditCount: num(k.credit_count),
      partialCount: num(k.partial_count),
      discounts: num(k.discounts),
      taxCollected: num(k.tax_collected),
      cashSales: num(k.cash_sales),
      cardSales: num(k.card_sales),
      mobileSales: num(k.mobile_sales),
      salesByDate,
      salesByCashier: cashierMap,
      salesByCashierList: cashierList,
      salesByBranch: branchMap,
      salesByBranchList: branchList,
      paymentMethodBreakdown: paymentRows.map((r) => ({
        method: r.method,
        count: num(r.count),
        total: num(r.total),
        collected: num(r.collected),
        credit: num(r.credit),
      })),
      recentSales: recentRows.map((r) => {
        const isReal = !r.is_refund && !r.is_settlement;
        const total = isReal ? num(r.product_total) : num(r.total);
        return {
          id: r.sale_id,
          invoice_no: r.invoice_no || 'N/A',
          created_at: r.created_at_utc,
          date: dateKeyToIso(r.date_key),
          customer_name: r.customer_name || 'Walk-in',
          customer_phone: r.customer_phone || '',
          cashier_name: r.username || `User ${r.user_key}`,
          total,
          payment_amount: isReal ? num(r.product_paid) : num(r.payment_amount),
          credit_amount: isReal ? num(r.product_credit) : num(r.credit_amount),
          payment_method: r.payment_method || 'CASH',
          payment_type: r.payment_type || null,
          payment_status: r.payment_status || r.credit_status || 'COMPLETED',
          is_settlement: Boolean(r.is_settlement),
          scope_id: r.scope_id,
          running_balance: null,
          sales: total,
          total_amount: total,
        };
      }),
      topProducts: topProductRows.map((r) => ({
        name: r.name || 'Unknown',
        sold: num(r.sold),
        revenue: num(r.revenue),
        cost: num(r.cost),
        grossProfit: num(r.gross_profit),
      })),
    };
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Clinic services
// ─────────────────────────────────────────────────────────────────────────────

function emptyClinicReport() {
  return {
    totalRevenue: 0,
    totalQuantity: 0,
    totalLines: 0,
    totalInvoices: 0,
    averageTicket: 0,
    byCategory: [],
    byService: [],
    byDate: [],
    byBranch: [],
    recentLines: [],
    categories: [],
    topServices: [],
  };
}

async function getClinicReports(user, query = {}) {
  const ctx = await buildContext(user, query);
  if (ctx.impossible) return emptyClinicReport();

  return cached(`clinic:${user?.id}:${contextKey(ctx)}:${query.category || ''}`, async () => {
    const w = filterSql('a', ctx);
    const category = query.category && query.category !== 'all' ? Number(query.category) : null;
    const categorySql = category != null ? ' AND cs.category_id = ?' : '';
    const categoryParams = category != null ? [category] : [];

    const [byCategoryRows] = await dwPool.execute(
      `SELECT cs.category_id AS categoryId,
              COALESCE(cs.category_name, 'Uncategorized') AS category,
              SUM(a.revenue) AS revenue,
              SUM(a.quantity) AS quantity,
              SUM(a.\`lines\`) AS \`lines\`
         FROM agg_clinic_daily a
         INNER JOIN dim_clinic_service cs ON cs.service_key = a.service_key
        WHERE 1 = 1 ${w.sql} ${categorySql}
        GROUP BY cs.category_id, cs.category_name
        ORDER BY revenue DESC`,
      [...w.params, ...categoryParams]
    );

    const [byServiceRows] = await dwPool.execute(
      `SELECT a.service_key,
              cs.clinic_service_id AS clinicServiceId,
              cs.name,
              cs.code,
              cs.category_id AS categoryId,
              COALESCE(cs.category_name, 'Uncategorized') AS category,
              SUM(a.revenue) AS revenue,
              SUM(a.quantity) AS quantity,
              SUM(a.\`lines\`) AS \`lines\`
         FROM agg_clinic_daily a
         INNER JOIN dim_clinic_service cs ON cs.service_key = a.service_key
        WHERE 1 = 1 ${w.sql} ${categorySql}
        GROUP BY a.service_key, cs.clinic_service_id, cs.name, cs.code, cs.category_id, cs.category_name
        ORDER BY revenue DESC`,
      [...w.params, ...categoryParams]
    );

    const [byDateRows] = await dwPool.execute(
      `SELECT a.date_key,
              SUM(a.revenue) AS revenue,
              SUM(a.quantity) AS quantity,
              SUM(a.\`lines\`) AS \`lines\`
         FROM agg_clinic_daily a
         INNER JOIN dim_clinic_service cs ON cs.service_key = a.service_key
        WHERE 1 = 1 ${w.sql} ${categorySql}
        GROUP BY a.date_key
        ORDER BY a.date_key ASC`,
      [...w.params, ...categoryParams]
    );

    const [byBranchRows] = await dwPool.execute(
      `SELECT a.scope_key,
              COALESCE(d.scope_name, d.scope_id, 'Unknown') AS branch,
              SUM(a.revenue) AS revenue,
              SUM(a.quantity) AS quantity,
              SUM(a.\`lines\`) AS \`lines\`
         FROM agg_clinic_daily a
         INNER JOIN dim_clinic_service cs ON cs.service_key = a.service_key
         LEFT JOIN dim_scope d ON d.scope_key = a.scope_key
        WHERE 1 = 1 ${w.sql} ${categorySql}
        GROUP BY a.scope_key, d.scope_name, d.scope_id
        ORDER BY revenue DESC`,
      [...w.params, ...categoryParams]
    );

    const fw = filterSql('f', ctx);
    const [invoiceRows] = await dwPool.execute(
      `SELECT COUNT(DISTINCT i.sale_id) AS invoices
         FROM fact_sale_items i
         INNER JOIN fact_sales f ON f.sale_id = i.sale_id
         INNER JOIN dim_clinic_service cs ON cs.service_key = i.service_key
        WHERE i.is_clinic_line = 1
          AND f.status = 'COMPLETED'
          AND f.deleted_at IS NULL
          AND f.clinic_eligible = 1
          ${fw.sql} ${categorySql}`,
      [...fw.params, ...categoryParams]
    );

    const [recentRows] = await dwPool.execute(
      `SELECT i.sale_item_id, i.sale_id, i.name AS service_name, i.sku,
              i.quantity, i.unit_price, i.total, i.discount,
              cs.clinic_service_id, cs.code AS service_code,
              cs.category_id, COALESCE(cs.category_name, 'Uncategorized') AS category_name,
              f.invoice_no, f.created_at_utc, f.customer_name, f.customer_phone,
              f.date_key, d.scope_id, f.payment_method, u.username AS cashier_name
         FROM fact_sale_items i
         INNER JOIN fact_sales f ON f.sale_id = i.sale_id
         INNER JOIN dim_clinic_service cs ON cs.service_key = i.service_key
         LEFT JOIN dim_scope d ON d.scope_key = f.scope_key
         LEFT JOIN dim_user u ON u.user_key = f.user_key
        WHERE i.is_clinic_line = 1
          AND f.status = 'COMPLETED'
          AND f.deleted_at IS NULL
          AND f.clinic_eligible = 1
          ${fw.sql} ${categorySql}
        ORDER BY f.created_at_utc DESC, i.sale_item_id ASC
        LIMIT 50`,
      [...fw.params, ...categoryParams]
    );

    const [categoryRows] = await dwPool.execute(
      `SELECT DISTINCT category_id AS id, category_name AS name
         FROM dim_clinic_service
        WHERE category_id IS NOT NULL
        ORDER BY category_name ASC`
    );

    const totalRevenue = byServiceRows.reduce((s, r) => s + num(r.revenue), 0);
    const totalQuantity = byServiceRows.reduce((s, r) => s + num(r.quantity), 0);
    const totalLines = byServiceRows.reduce((s, r) => s + num(r.lines), 0);
    const totalInvoices = num(invoiceRows[0]?.invoices);

    const byService = byServiceRows.map((r) => ({
      clinicServiceId: r.clinicServiceId,
      name: r.name || 'Clinic service',
      code: r.code || null,
      categoryId: r.categoryId,
      category: r.category,
      revenue: num(r.revenue),
      quantity: num(r.quantity),
      lines: num(r.lines),
    }));

    return {
      totalRevenue,
      totalQuantity,
      totalLines,
      totalInvoices,
      averageTicket: totalInvoices > 0 ? totalRevenue / totalInvoices : 0,
      byCategory: byCategoryRows.map((r) => ({
        categoryId: r.categoryId,
        category: r.category,
        revenue: num(r.revenue),
        quantity: num(r.quantity),
        lines: num(r.lines),
      })),
      byService,
      byDate: byDateRows.map((r) => ({
        date: dateKeyToIso(r.date_key),
        revenue: num(r.revenue),
        quantity: num(r.quantity),
        lines: num(r.lines),
      })),
      byBranch: byBranchRows.map((r) => ({
        branch: r.branch,
        revenue: num(r.revenue),
        quantity: num(r.quantity),
        lines: num(r.lines),
      })),
      recentLines: recentRows.map((r) => ({
        id: r.sale_item_id,
        saleId: r.sale_id,
        invoiceNo: r.invoice_no || 'N/A',
        createdAt: r.created_at_utc,
        date: dateKeyToIso(r.date_key),
        customerName: r.customer_name || 'Walk-in',
        customerPhone: r.customer_phone || '',
        cashierName: r.cashier_name || '—',
        branch: r.scope_id || '—',
        serviceName: r.service_name || 'Clinic service',
        serviceCode: r.service_code || r.sku || null,
        categoryId: r.category_id,
        category: r.category_name,
        quantity: num(r.quantity),
        unitPrice: num(r.unit_price),
        total: num(r.total),
        paymentMethod: r.payment_method || 'CASH',
      })),
      categories: categoryRows.map((r) => ({ id: r.id, name: r.name })),
      topServices: byService.slice(0, 15),
    };
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Inventory
// ─────────────────────────────────────────────────────────────────────────────

function emptyInventoryReport() {
  return {
    summary: {
      totalItems: 0,
      totalValue: 0,
      turnoverRate: 0,
      stockStatusCounts: { 'In Stock': 0, 'Low Stock': 0, 'Out of Stock': 0 },
      categoryCounts: {},
    },
    lowStockItems: [],
    topSellingItems: [],
    movementData: [],
  };
}

async function getInventoryReports(user, query = {}) {
  const ctx = await buildContext(user, query);
  if (ctx.impossible) return emptyInventoryReport();

  return cached(`inventory:${user?.id}:${contextKey(ctx)}`, async () => {
    const [maxRows] = await dwPool.query(
      'SELECT MAX(snapshot_date_key) AS latest FROM fact_inventory_snapshot'
    );
    const latestKey = maxRows[0]?.latest ? Number(maxRows[0].latest) : null;
    if (!latestKey) return emptyInventoryReport();

    const scopeSql = ctx.scopeKeys
      ? ` AND s.scope_key IN (${ph(ctx.scopeKeys.length)})`
      : '';
    const scopeParams = ctx.scopeKeys || [];

    const [items] = await dwPool.execute(
      `SELECT p.name, p.sku, p.category, p.min_stock_level,
              s.quantity, s.value, s.unit_cost, s.unit_price
         FROM fact_inventory_snapshot s
         INNER JOIN dim_product p ON p.product_key = s.product_key
        WHERE s.snapshot_date_key = ? ${scopeSql}`,
      [latestKey, ...scopeParams]
    );

    const report = emptyInventoryReport();
    report.summary.totalItems = items.length;
    report.summary.totalValue = items.reduce((sum, i) => sum + num(i.value), 0);

    for (const item of items) {
      const stock = num(item.quantity);
      const min = num(item.min_stock_level);
      if (stock <= 0) report.summary.stockStatusCounts['Out of Stock'] += 1;
      else if (min > 0 && stock <= min) report.summary.stockStatusCounts['Low Stock'] += 1;
      else report.summary.stockStatusCounts['In Stock'] += 1;

      const category = item.category || 'Uncategorized';
      report.summary.categoryCounts[category] = (report.summary.categoryCounts[category] || 0) + 1;
    }

    const totalValue = report.summary.totalValue;
    const totalItems = report.summary.totalItems;
    report.summary.turnoverRate = totalItems > 0 && totalValue > 0
      ? round(totalValue / totalItems / 1000, 1)
      : 0;

    report.lowStockItems = items
      .filter((i) => num(i.quantity) > 0 && num(i.min_stock_level) > 0 && num(i.quantity) <= num(i.min_stock_level))
      .slice(0, 10)
      .map((i) => ({
        name: i.name,
        item_name: i.name,
        current_stock: num(i.quantity),
        min_stock_level: num(i.min_stock_level),
        category: i.category || 'Uncategorized',
        sku: i.sku || '',
      }));

    const today = todayDateKey();
    const movementFrom = shiftDateKey(today, -7);
    const movementScope = ctx.scopeKeys
      ? ` AND a.scope_key IN (${ph(ctx.scopeKeys.length)})`
      : '';
    const [movementRows] = await dwPool.execute(
      `SELECT a.date_key,
              SUM(a.received_qty) AS received,
              SUM(a.sold_qty) AS sold,
              SUM(a.returned_qty) AS returned
         FROM agg_inventory_daily a
        WHERE a.date_key >= ? AND a.date_key <= ? ${movementScope}
        GROUP BY a.date_key
        ORDER BY a.date_key ASC`,
      [movementFrom, today, ...(ctx.scopeKeys || [])]
    );
    report.movementData = movementRows.map((r) => ({
      date: dateKeyToIso(r.date_key),
      received: num(r.received),
      sold: num(r.sold),
      returned: num(r.returned),
    }));

    const topFrom = shiftDateKey(today, -30);
    const topScope = ctx.scopeKeys
      ? ` AND a.scope_key IN (${ph(ctx.scopeKeys.length)})`
      : '';
    const [topRows] = await dwPool.execute(
      `SELECT COALESCE(p.name, 'Unknown') AS name,
              SUM(a.quantity) AS sold,
              SUM(a.revenue) AS revenue
         FROM agg_product_daily a
         LEFT JOIN dim_product p ON p.product_key = a.product_key
        WHERE a.date_key >= ? AND a.date_key <= ? ${topScope}
        GROUP BY a.product_key, p.name
        ORDER BY sold DESC
        LIMIT 5`,
      [topFrom, today, ...(ctx.scopeKeys || [])]
    );
    report.topSellingItems = topRows.map((r) => ({
      name: r.name,
      item_name: r.name,
      sold: num(r.sold),
      quantity_sold: num(r.sold),
      revenue: num(r.revenue),
    }));

    return report;
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Ledger
// ─────────────────────────────────────────────────────────────────────────────

function emptyLedgerReport() {
  return {
    totalEntries: 0,
    totalTransactions: 0,
    totalDebit: 0,
    totalCredit: 0,
    balance: 0,
    recentTransactions: [],
    trendData: [],
    accountSummary: [],
  };
}

async function getLedgerReports(user, query = {}) {
  const ctx = await buildContext(user, query);
  if (ctx.impossible) return emptyLedgerReport();

  return cached(`ledger:${user?.id}:${contextKey(ctx)}:${query.account || ''}:${query.transactionType || ''}`, async () => {
    const extra = [];
    const extraParams = [];
    if (query.account && query.account !== 'all') {
      extra.push('a.account_type = ?');
      extraParams.push(query.account);
    }
    if (query.transactionType && query.transactionType !== 'all') {
      extra.push('UPPER(a.entry_type) = ?');
      extraParams.push(String(query.transactionType).toUpperCase());
    }
    const extraSql = extra.length ? ` AND ${extra.join(' AND ')}` : '';

    const w = filterSql('a', ctx, { includeUser: false });
    const params = [...w.params, ...extraParams];

    const [totals] = await dwPool.execute(
      `SELECT COALESCE(SUM(a.entries), 0) AS entries,
              COALESCE(SUM(CASE WHEN UPPER(a.entry_type) = 'DEBIT' THEN a.amount ELSE 0 END), 0) AS debit,
              COALESCE(SUM(CASE WHEN UPPER(a.entry_type) = 'CREDIT' THEN a.amount ELSE 0 END), 0) AS credit
         FROM agg_ledger_daily a
        WHERE 1 = 1 ${w.sql} ${extraSql}`,
      params
    );

    const [trendRows] = await dwPool.execute(
      `SELECT a.date_key,
              SUM(CASE WHEN UPPER(a.entry_type) = 'DEBIT' THEN a.amount ELSE 0 END) AS debit,
              SUM(CASE WHEN UPPER(a.entry_type) = 'CREDIT' THEN a.amount ELSE 0 END) AS credit
         FROM agg_ledger_daily a
        WHERE 1 = 1 ${w.sql} ${extraSql}
        GROUP BY a.date_key
        ORDER BY a.date_key ASC`,
      params
    );

    const [accountRows] = await dwPool.execute(
      `SELECT a.account_type AS account,
              SUM(CASE WHEN UPPER(a.entry_type) = 'DEBIT' THEN a.amount ELSE 0 END) AS debit,
              SUM(CASE WHEN UPPER(a.entry_type) = 'CREDIT' THEN a.amount ELSE 0 END) AS credit,
              SUM(a.entries) AS entries
         FROM agg_ledger_daily a
        WHERE 1 = 1 ${w.sql} ${extraSql}
        GROUP BY a.account_type
        ORDER BY entries DESC`,
      params
    );

    const fw = filterSql('f', ctx, { includeUser: false });
    const recentExtra = [];
    const recentParams = [];
    if (query.account && query.account !== 'all') {
      recentExtra.push('f.account_type = ?');
      recentParams.push(query.account);
    }
    if (query.transactionType && query.transactionType !== 'all') {
      recentExtra.push('UPPER(f.entry_type) = ?');
      recentParams.push(String(query.transactionType).toUpperCase());
    }
    const recentExtraSql = recentExtra.length ? ` AND ${recentExtra.join(' AND ')}` : '';

    const [recentRows] = await dwPool.execute(
      `SELECT f.entry_id, f.date_key, f.ledger_id, f.account_name, f.account_type,
              f.entry_type, f.debit, f.credit, f.reference_id, f.description,
              f.created_by, f.created_at_utc
         FROM fact_ledger_entries f
        WHERE 1 = 1 ${fw.sql} ${recentExtraSql}
        ORDER BY f.created_at_utc DESC
        LIMIT 10`,
      [...fw.params, ...recentParams]
    );

    const totalDebit = num(totals[0]?.debit);
    const totalCredit = num(totals[0]?.credit);

    return {
      totalEntries: num(totals[0]?.entries),
      totalTransactions: num(totals[0]?.entries),
      totalDebit,
      totalCredit,
      balance: totalCredit - totalDebit,
      recentTransactions: recentRows.map((r) => ({
        ...r,
        transaction_type: r.entry_type,
        amount: String(r.entry_type).toUpperCase() === 'DEBIT' ? num(r.debit) : num(r.credit),
        debit_amount: num(r.debit),
        credit_amount: num(r.credit),
        created_at: r.created_at_utc,
      })),
      trendData: trendRows.map((r) => {
        const debit = num(r.debit);
        const credit = num(r.credit);
        return { date: dateKeyToIso(r.date_key), debit, credit, balance: credit - debit };
      }),
      accountSummary: accountRows.map((r) => {
        const debit = num(r.debit);
        const credit = num(r.credit);
        return {
          account: r.account || 'General',
          debit,
          credit,
          entries: num(r.entries),
          balance: credit - debit,
        };
      }),
    };
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Financial
// ─────────────────────────────────────────────────────────────────────────────

function emptyFinancialReport() {
  return {
    totalRevenue: 0,
    totalCostOfGoods: 0,
    grossProfit: 0,
    grossProfitMargin: 0,
    totalExpenses: 0,
    netProfit: 0,
    netProfitMargin: 0,
    zakatDue: 0,
    operatingCashFlow: 0,
    profitMargin: 0,
    revenueByPeriod: [],
    expensesByPeriod: [],
    cashFlowData: [],
    expenseBreakdown: [],
    profitabilityMetrics: [],
    financialRatios: [],
    topRevenueSources: [],
  };
}

const EXPENSE_COLORS = ['#10b981', '#06b6d4', '#f59e0b', '#a855f7', '#ef4444',
  '#3b82f6', '#ec4899', '#14b8a6', '#f97316', '#8b5cf6'];

const daysInMonth = (year, month) => new Date(Date.UTC(year, month, 0)).getUTCDate();

function financialRange(query) {
  const fromKey = query.dateFrom ? parseDateKey(query.dateFrom) : null;
  const toKey = query.dateTo ? parseDateKey(query.dateTo) : null;
  if (fromKey && toKey) {
    // An explicit date range always wins. Totals are then computed from daily
    // aggregates, so a partial month (e.g. 1–15 Oct) is not rounded up to the
    // whole month the way the old agg_financial_monthly query did.
    return {
      startKey: Math.min(fromKey, toKey),
      endKey: Math.max(fromKey, toKey),
    };
  }

  const year = Number(query.year);
  if (Number.isInteger(year) && year >= 1900 && year <= 2200) {
    const period = String(query.period || '').toLowerCase();
    const quarter = String(query.quarter || '').toUpperCase();
    if (period === 'quarterly' && /^Q[1-4]$/.test(quarter)) {
      const q = Number(quarter.slice(1));
      const startMonth = (q - 1) * 3 + 1;
      const endMonth = startMonth + 2;
      return {
        startKey: year * 10000 + startMonth * 100 + 1,
        endKey: year * 10000 + endMonth * 100 + daysInMonth(year, endMonth),
      };
    }
    return { startKey: year * 10000 + 101, endKey: year * 10000 + 1231 };
  }

  return { startKey: null, endKey: null };
}

async function getFinancialReports(user, query = {}) {
  const ctx = await buildContext(user, query);
  if (ctx.impossible) return emptyFinancialReport();

  const range = financialRange(query);
  ctx.startKey = range.startKey;
  ctx.endKey = range.endKey;

  return cached(`financial:${user?.id}:${contextKey(ctx)}`, async () => {
    const report = emptyFinancialReport();

    // Revenue / COGS come from daily sales aggregates so an explicit date
    // range is exact to the day. Expenses / income come from the expense fact
    // table with the same date and scope filter. Both are grouped by month for
    // the charts.
    const salesFilter = filterSql('a', ctx, { includeUser: false });
    const [salesMonthRows] = await dwPool.execute(
      `SELECT d.month_key,
              SUM(a.gross_sales) AS revenue,
              SUM(a.sale_count)  AS sales_count,
              SUM(a.cogs)        AS cogs
         FROM agg_sales_daily a
         INNER JOIN dim_date d ON d.date_key = a.date_key
        WHERE 1 = 1 ${salesFilter.sql}
        GROUP BY d.month_key
        ORDER BY d.month_key ASC`,
      salesFilter.params
    );

    const voucherFilter = filterSql('e', ctx, { includeUser: false });
    const [voucherMonthRows] = await dwPool.execute(
      `SELECT d.month_key,
              SUM(CASE WHEN e.type = 'EXPENSE' THEN e.amount ELSE 0 END) AS expenses,
              SUM(CASE WHEN e.type = 'INCOME'  THEN e.amount ELSE 0 END) AS income
         FROM fact_expenses e
         INNER JOIN dim_date d ON d.date_key = e.date_key
        WHERE e.status IN ('APPROVED', 'PENDING')
          ${voucherFilter.sql}
        GROUP BY d.month_key
        ORDER BY d.month_key ASC`,
      voucherFilter.params
    );

    const salesByMonth = new Map(salesMonthRows.map((r) => [Number(r.month_key), r]));
    const voucherByMonth = new Map(voucherMonthRows.map((r) => [Number(r.month_key), r]));
    const monthKeys = unique([
      ...salesMonthRows.map((r) => Number(r.month_key)),
      ...voucherMonthRows.map((r) => Number(r.month_key)),
    ]).sort((a, b) => a - b);

    const monthRows = monthKeys.map((monthKey) => {
      const sales = salesByMonth.get(monthKey) || {};
      const voucher = voucherByMonth.get(monthKey) || {};
      return {
        month_key: monthKey,
        revenue: num(sales.revenue),
        sales_count: num(sales.sales_count),
        cogs: num(sales.cogs),
        expenses: num(voucher.expenses),
        income: num(voucher.income),
      };
    });

    const totalRevenue = monthRows.reduce((s, r) => s + num(r.revenue), 0);
    const totalCogs = monthRows.reduce((s, r) => s + num(r.cogs), 0);
    const totalExpenses = monthRows.reduce((s, r) => s + num(r.expenses), 0);
    const totalIncome = monthRows.reduce((s, r) => s + num(r.income), 0);
    const grossProfit = totalRevenue - totalCogs;
    const netProfit = grossProfit - totalExpenses;

    report.totalRevenue = totalRevenue;
    report.totalCostOfGoods = totalCogs;
    report.grossProfit = grossProfit;
    report.grossProfitMargin = totalRevenue > 0 ? (grossProfit / totalRevenue) * 100 : 0;
    report.totalExpenses = totalExpenses;
    report.netProfit = netProfit;
    report.netProfitMargin = totalRevenue > 0 ? (netProfit / totalRevenue) * 100 : 0;
    report.operatingCashFlow = totalIncome;
    report.profitMargin = report.netProfitMargin;
    report.zakatDue = netProfit > 0 ? netProfit * 0.025 : 0;

    report.revenueByPeriod = monthRows.map((r) => {
      const revenue = num(r.revenue);
      const cogs = num(r.cogs);
      const expenses = num(r.expenses);
      return {
        month: monthKeyToYearMonth(r.month_key),
        revenue,
        cogs,
        expenses,
        profit: revenue - cogs - expenses,
      };
    });
    report.expensesByPeriod = monthRows
      .filter((r) => num(r.expenses) !== 0)
      .map((r) => ({
        month: monthKeyToYearMonth(r.month_key),
        expenses: num(r.expenses),
      }));
    report.cashFlowData = monthRows.map((r) => ({
      month: monthKeyToYearMonth(r.month_key),
      operating: num(r.income),
      investing: -num(r.expenses),
      financing: 0,
    }));

    // Expense breakdown — fact table so it honours the same scope/date filters.
    const ew = filterSql('e', ctx, { includeUser: false });
    const [expenseRows] = await dwPool.execute(
      `SELECT COALESCE(NULLIF(e.expense_category, ''), NULLIF(e.category, ''), 'Other') AS label,
              SUM(e.amount) AS amount,
              COUNT(*) AS count
         FROM fact_expenses e
        WHERE e.type = 'EXPENSE'
          AND e.status IN ('APPROVED', 'PENDING')
          ${ew.sql}
        GROUP BY label
        ORDER BY amount DESC
        LIMIT 10`,
      ew.params
    );
    const expenseTotal = expenseRows.reduce((s, r) => s + num(r.amount), 0);
    report.expenseBreakdown = expenseRows.map((r, i) => ({
      category: r.label || 'Other',
      amount: num(r.amount),
      percentage: expenseTotal > 0 ? round((num(r.amount) / expenseTotal) * 100, 1) : 0,
      color: EXPENSE_COLORS[i % EXPENSE_COLORS.length],
    }));

    // Top revenue sources by raw payment method.
    const sw = filterSql('a', ctx, { includeUser: false });
    const [sourceRows] = await dwPool.execute(
      `SELECT COALESCE(NULLIF(a.payment_method, ''), 'Unknown') AS source,
              SUM(a.gross_sales) AS revenue,
              SUM(a.sale_count) AS count
         FROM agg_sales_daily a
        WHERE 1 = 1 ${sw.sql}
        GROUP BY source
       HAVING SUM(a.gross_sales) > 0
        ORDER BY revenue DESC
        LIMIT 10`,
      sw.params
    );
    report.topRevenueSources = sourceRows.map((r) => ({
      source: r.source || 'Unknown',
      revenue: num(r.revenue),
      count: num(r.count),
    }));

    const cogsRatio = totalRevenue > 0 ? ((totalCogs / totalRevenue) * 100).toFixed(1) : '0.0';
    const expenseRatio = totalRevenue > 0 ? ((totalExpenses / totalRevenue) * 100).toFixed(1) : '0.0';

    report.profitabilityMetrics = [
      { metric: 'Gross Profit Margin', value: `${report.grossProfitMargin.toFixed(1)}%`, status: report.grossProfitMargin >= 30 ? 'excellent' : 'good' },
      { metric: 'Net Profit Margin', value: `${report.netProfitMargin.toFixed(1)}%`, status: report.netProfitMargin >= 10 ? 'excellent' : 'good' },
      { metric: 'COGS Ratio', value: `${cogsRatio}%`, status: 'good' },
      { metric: 'Expense Ratio', value: `${expenseRatio}%`, status: 'good' },
      { metric: 'Zakat Rate', value: '2.5%', status: 'good' },
    ];
    report.financialRatios = [
      { ratio: 'Current Ratio', value: '2.4', benchmark: '2.0', status: 'good' },
      { ratio: 'Quick Ratio', value: '1.8', benchmark: '1.0', status: 'excellent' },
      { ratio: 'Debt-to-Equity', value: '0.3', benchmark: '0.5', status: 'excellent' },
      { ratio: 'Interest Coverage', value: '8.5', benchmark: '2.5', status: 'excellent' },
    ];

    return report;
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Status
// ─────────────────────────────────────────────────────────────────────────────

async function getWarehouseStatus() {
  const [state] = await dwPool.query(
    'SELECT source_table, last_high_water_id, last_sync_at, last_full_sync_at, rows_last_run, last_error, updated_at FROM etl_state ORDER BY source_table'
  );
  const [runs] = await dwPool.query(
    'SELECT run_id, mode, started_at, finished_at, status, rows_staged, date_keys, error FROM etl_runs ORDER BY run_id DESC LIMIT 10'
  );
  const [counts] = await dwPool.query(
    `SELECT
       (SELECT COUNT(*) FROM fact_sales) AS fact_sales,
       (SELECT COUNT(*) FROM fact_sale_items) AS fact_sale_items,
       (SELECT COUNT(*) FROM fact_inventory_movements) AS fact_inventory_movements,
       (SELECT COUNT(*) FROM fact_ledger_entries) AS fact_ledger_entries,
       (SELECT COUNT(*) FROM fact_expenses) AS fact_expenses,
       (SELECT MAX(snapshot_date_key) FROM fact_inventory_snapshot) AS latest_snapshot`
  );

  const lastSync = state
    .map((r) => (r.last_sync_at ? new Date(r.last_sync_at).getTime() : 0))
    .reduce((max, v) => Math.max(max, v), 0);
  const lagSeconds = lastSync ? Math.max(0, Math.round((Date.now() - lastSync) / 1000)) : null;

  return {
    enabled: true,
    lastSuccessfulSyncAt: lastSync ? new Date(lastSync) : null,
    lagSeconds,
    sources: state,
    recentRuns: runs,
    counts: counts[0] || {},
    cache: cache.stats ? cache.stats() : null,
  };
}

module.exports = {
  getSalesReports,
  getClinicReports,
  getInventoryReports,
  getLedgerReports,
  getFinancialReports,
  getWarehouseStatus,
  buildContext,
  filterSql,
};
