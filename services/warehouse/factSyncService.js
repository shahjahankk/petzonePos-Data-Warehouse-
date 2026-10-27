/**
 * OLTP → warehouse fact extraction and upsert.
 *
 * Each extractor supports three modes:
 *   incremental — rows newer than the stored watermark / high-water id
 *   window      — rows whose business date falls in [fromKey, toKey]
 *   full        — everything (used by backfill/rebuild)
 *
 * Every write is an idempotent upsert, so overlapping windows and re-runs are safe.
 */

const { pool: oltpPool } = require('../../config/database');
const { dwPool } = require('../../config/warehouseDatabase');
const { CLINIC_LINE_SQL } = require('../../utils/clinicLines');
const { REPORT_TIMEZONE } = require('../../utils/reportDate');
const dims = require('./dimensionService');
const {
  toDateKey,
  toDateOnly,
  saleBusinessDate,
  dateKeyToIso,
  dateKeyToMonthKey,
} = require('./dateKeys');

const BATCH_SIZE = Math.max(100, parseInt(process.env.DW_BATCH_SIZE, 10) || 1000);
const OVERLAP_MS = Math.max(0, parseInt(process.env.DW_SYNC_OVERLAP_MINUTES, 10) || 10) * 60000;
const CLINIC_ID_FROM_SKU = /^CLINIC-(\d+)$/i;

const num = (v) => (Number.isFinite(Number(v)) ? Number(v) : 0);

async function upsertFacts(table, columns, rows, updateColumns, chunkSize = 250) {
  if (!rows.length) return 0;
  const colSql = columns.join(', ');
  const updateSql = updateColumns.map((c) => `${c} = VALUES(${c})`).join(', ');
  const placeholders = `(${columns.map(() => '?').join(', ')})`;
  let written = 0;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const sql = `INSERT INTO ${table} (${colSql}) VALUES ${chunk
      .map(() => placeholders)
      .join(', ')} ON DUPLICATE KEY UPDATE ${updateSql}`;
    await dwPool.query(sql, chunk.flat());
    written += chunk.length;
  }
  return written;
}

const tzSqlDate = (expr) => `DATE(CONVERT_TZ(${expr}, '+00:00', '${REPORT_TIMEZONE}'))`;

function normalizedScopeType(value) {
  const t = String(value || 'BRANCH').trim().toUpperCase();
  return t === 'WAREHOUSE' ? 'WAREHOUSE' : 'BRANCH';
}

function buildPredicate({
  alias, idColumn, tsColumns, mode, highWaterId, watermark, cursor,
  dateExpr, fromKey, toKey,
}) {
  const conditions = [];
  const params = [];

  if (mode === 'incremental') {
    if (watermark || Number(highWaterId) > 0) {
      const tsExpr = tsColumns.length
        ? `COALESCE(${tsColumns.map((c) => `${alias}.${c}`).join(', ')})`
        : null;
      if (tsExpr) {
        conditions.push(`(${alias}.${idColumn} > ? OR ${tsExpr} >= ?)`);
        const since = watermark
          ? new Date(new Date(watermark).getTime() - OVERLAP_MS)
          : new Date(0);
        params.push(Number(highWaterId) || 0, since);
      } else {
        conditions.push(`${alias}.${idColumn} > ?`);
        params.push(Number(highWaterId) || 0);
      }
    }
  } else if (mode === 'window') {
    conditions.push(`${dateExpr} BETWEEN ? AND ?`);
    params.push(dateKeyToIso(fromKey), dateKeyToIso(toKey));
  }

  if (cursor) {
    conditions.push(`${alias}.${idColumn} > ?`);
    params.push(cursor);
  }

  return {
    where: conditions.length ? `WHERE ${conditions.join(' AND ')}` : '',
    params,
  };
}

async function resolveScopesForRows(rows, typeField = 'scope_type', idField = 'scope_id') {
  const seen = new Set();
  for (const row of rows) {
    const type = normalizedScopeType(row[typeField]);
    const raw = row[idField];
    if (raw === null || raw === undefined || raw === '') continue;
    const key = `${type}|${String(raw).trim().toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    await dims.resolveScopeKey(type, raw);
  }
}

function scopeKeyOf(row, typeField = 'scope_type', idField = 'scope_id') {
  return dims.maps.scopeSync(normalizedScopeType(row[typeField]), row[idField]);
}

// ─────────────────────────────────────────────────────────────────────────────
// Sales
// ─────────────────────────────────────────────────────────────────────────────

const SALES_COLUMNS = [
  'sale_id', 'invoice_no', 'date_key', 'created_at_utc', 'updated_at_utc',
  'scope_key', 'user_key', 'customer_key', 'shift_id',
  'subtotal', 'tax', 'discount', 'total', 'payment_amount', 'credit_amount',
  'payment_method', 'payment_type', 'payment_status', 'credit_status', 'status',
  'customer_name', 'customer_phone',
  'is_real_sale', 'is_refund', 'is_settlement', 'clinic_eligible',
  'product_total', 'clinic_total', 'product_paid', 'product_credit',
  'product_discount', 'product_tax', 'product_collected',
  'cogs', 'gross_profit', 'deleted_at',
];
const SALES_UPDATE = SALES_COLUMNS.filter((c) => c !== 'sale_id');

function classifySale(row) {
  const pm = String(row.payment_method || '').toUpperCase();
  const pt = String(row.payment_type || '').toUpperCase();
  const isRefund = pm === 'CASH_REFUND' || pm === 'REFUND' || pt === 'CASH_REFUND' || pt === 'REFUND';
  const isSettlement = pt === 'OUTSTANDING_SETTLEMENT';
  return { isRefund, isSettlement, isReal: !isRefund && !isSettlement };
}

async function lineTotalsForSales(saleIds) {
  const map = new Map();
  if (!saleIds.length) return map;
  const placeholders = saleIds.map(() => '?').join(',');
  const [rows] = await oltpPool.execute(
    `SELECT si.sale_id,
            SUM(CASE WHEN ${CLINIC_LINE_SQL} THEN COALESCE(si.total, 0) ELSE 0 END) AS clinic_total,
            SUM(CASE WHEN NOT (${CLINIC_LINE_SQL}) THEN COALESCE(si.total, 0) ELSE 0 END) AS product_total,
            SUM(CASE WHEN NOT (${CLINIC_LINE_SQL})
                     THEN COALESCE(si.quantity, 0) * COALESCE(ii.cost_price, 0) ELSE 0 END) AS cogs,
            COUNT(*) AS line_count
       FROM sale_items si
       LEFT JOIN inventory_items ii ON ii.id = si.inventory_item_id
      WHERE si.sale_id IN (${placeholders})
      GROUP BY si.sale_id`,
    saleIds
  );
  for (const r of rows) {
    map.set(Number(r.sale_id), {
      product: num(r.product_total),
      clinic: num(r.clinic_total),
      cogs: num(r.cogs),
      lines: num(r.line_count),
    });
  }
  return map;
}

async function syncSales({
  mode, fromKey, toKey, state, batchSize = BATCH_SIZE, onDateKeys,
}) {
  const cols = await dims.oltpColumns('sales');
  const tsColumns = [];
  if (cols.has('updated_at')) tsColumns.push('updated_at');
  if (cols.has('created_at')) tsColumns.push('created_at');

  const select = [
    's.id', 's.invoice_no', 's.scope_type', 's.scope_id', 's.user_id',
    's.subtotal', 's.tax', 's.discount', 's.total',
    's.payment_method', 's.payment_type', 's.payment_status',
    's.payment_amount', 's.credit_amount', 's.status',
    's.customer_name', 's.customer_phone',
    's.created_at',
    cols.has('sale_date') ? 's.sale_date' : 'NULL AS sale_date',
    cols.has('updated_at') ? 's.updated_at' : 'NULL AS updated_at',
    cols.has('deleted_at') ? 's.deleted_at' : 'NULL AS deleted_at',
    cols.has('shift_id') ? 's.shift_id' : 'NULL AS shift_id',
    cols.has('credit_status') ? 's.credit_status' : 'NULL AS credit_status',
    cols.has('customer_id') ? 's.customer_id' : 'NULL AS customer_id',
  ].join(', ');

  const dateExpr = `COALESCE(s.sale_date, ${tzSqlDate('s.created_at')})`;
  let cursor = 0;
  let written = 0;
  let maxId = 0;
  const dateKeySet = new Set();

  for (;;) {
    const p = buildPredicate({
      alias: 's',
      idColumn: 'id',
      tsColumns,
      mode,
      highWaterId: state?.last_high_water_id,
      watermark: state?.last_sync_at,
      cursor,
      dateExpr,
      fromKey,
      toKey,
    });
    const [rows] = await oltpPool.execute(
      `SELECT ${select} FROM sales s ${p.where} ORDER BY s.id ASC LIMIT ?`,
      [...p.params, batchSize]
    );
    if (!rows.length) break;

    await resolveScopesForRows(rows);

    const saleIds = rows.map((r) => Number(r.id));
    const totals = await lineTotalsForSales(saleIds);

    const factRows = [];
    for (const row of rows) {
      const id = Number(row.id);
      maxId = Math.max(maxId, id);
      cursor = Math.max(cursor, id);

      const dateKey = saleBusinessDate(row);
      if (dateKey == null) continue;
      dateKeySet.add(dateKey);
      const { isRefund, isSettlement, isReal } = classifySale(row);
      const split = totals.get(id);
      const total = num(row.total);
      let productTotal = split ? split.product : 0;
      let clinicTotal = split ? split.clinic : 0;
      let cogs = split ? split.cogs : 0;
      if (!split || split.lines === 0) {
        productTotal = total;
        clinicTotal = 0;
        cogs = 0;
      }

      // Legacy sales report strips clinic lines and pro-rates payment fields
      // by the product share of the bill. Precompute it so report reads are pure sums.
      const denom = total > 0 ? total : (productTotal + clinicTotal);
      const share = denom > 0 ? productTotal / denom : (productTotal > 0 ? 1 : 0);
      const method = String(row.payment_method || 'CASH').toUpperCase();
      const payType = String(row.payment_type || '').toUpperCase();
      const payStatus = String(row.payment_status || '').toUpperCase();
      const paid = num(row.payment_amount) * share;
      const credit = num(row.credit_amount) * share;
      const discount = num(row.discount) * share;
      const tax = num(row.tax) * share;

      let collected = 0;
      if (isReal) {
        if (method === 'FULLY_CREDIT') collected = 0;
        else if (method === 'PARTIAL' || payType === 'PARTIAL' || payStatus === 'PARTIAL') collected = paid;
        else collected = paid > 0 ? paid : productTotal;
      }
      const clinicEligible = isReal
        && !['BILTY_CHARGE', 'CREDIT_REFUND_SETTLEMENT'].includes(payType);

      factRows.push([
        id,
        row.invoice_no || null,
        dateKey,
        row.created_at || null,
        row.updated_at || null,
        scopeKeyOf(row),
        dims.maps.user(row.user_id),
        dims.maps.customer(row.customer_id),
        row.shift_id || null,
        num(row.subtotal),
        num(row.tax),
        num(row.discount),
        total,
        num(row.payment_amount),
        num(row.credit_amount),
        row.payment_method || null,
        row.payment_type || null,
        row.payment_status || null,
        row.credit_status || null,
        row.status || null,
        row.customer_name || null,
        row.customer_phone || null,
        isReal ? 1 : 0,
        isRefund ? 1 : 0,
        isSettlement ? 1 : 0,
        clinicEligible ? 1 : 0,
        productTotal,
        clinicTotal,
        paid,
        credit,
        discount,
        tax,
        collected,
        cogs,
        isReal ? productTotal - cogs : 0,
        row.deleted_at || null,
      ]);
    }

    written += await upsertFacts('fact_sales', SALES_COLUMNS, factRows, SALES_UPDATE);
    if (rows.length < batchSize) break;
  }

  if (onDateKeys) onDateKeys([...dateKeySet]);
  return { rows: written, maxId, dateKeys: [...dateKeySet] };
}

// ─────────────────────────────────────────────────────────────────────────────
// Sale items
// ─────────────────────────────────────────────────────────────────────────────

const SALE_ITEM_COLUMNS = [
  'sale_item_id', 'sale_id', 'date_key', 'scope_key', 'user_key',
  'product_key', 'service_key', 'is_clinic_line', 'is_refund', 'is_settlement',
  'sku', 'name', 'quantity', 'unit_price', 'discount', 'total',
  'cost_price', 'cogs',
];
const SALE_ITEM_UPDATE = SALE_ITEM_COLUMNS.filter((c) => c !== 'sale_item_id');

function resolveClinicServiceKey(row) {
  const direct = dims.maps.service(row.clinic_service_id);
  if (direct != null) return direct;
  const m = String(row.sku || '').trim().match(CLINIC_ID_FROM_SKU);
  if (m) {
    const bySku = dims.maps.service(Number(m[1]));
    if (bySku != null) return bySku;
  }
  const byName = dims.maps.serviceByName(row.name);
  if (byName != null) return byName;
  return dims.maps.service(0);
}

async function syncSaleItems({
  mode, fromKey, toKey, state, batchSize = BATCH_SIZE,
}) {
  const cols = await dims.oltpColumns('sale_items');
  const tsColumns = cols.has('created_at') ? ['created_at'] : [];
  const saleCols = await dims.oltpColumns('sales');

  let cursor = 0;
  let written = 0;
  let maxId = 0;
  const dateKeySet = new Set();

  const saleDateExpr = saleCols.has('sale_date')
    ? `COALESCE(s.sale_date, ${tzSqlDate('s.created_at')})`
    : tzSqlDate('s.created_at');
  const dateExpr = saleDateExpr;

  const select = [
    'si.id', 'si.sale_id', 'si.inventory_item_id', 'si.clinic_service_id',
    'si.sku', 'si.name', 'si.quantity', 'si.unit_price', 'si.discount', 'si.total',
    'ii.cost_price',
    's.scope_type', 's.scope_id', 's.user_id',
    saleCols.has('sale_date') ? 's.sale_date' : 'NULL AS sale_date',
    's.created_at AS sale_created_at',
    saleCols.has('credit_status') ? 's.credit_status' : 'NULL AS credit_status',
    's.payment_method', 's.payment_type',
  ].join(', ');

  for (;;) {
    const p = buildPredicate({
      alias: 'si',
      idColumn: 'id',
      tsColumns,
      mode,
      highWaterId: state?.last_high_water_id,
      watermark: state?.last_sync_at,
      cursor,
      dateExpr,
      fromKey,
      toKey,
    });
    const [rows] = await oltpPool.execute(
      `SELECT ${select}
         FROM sale_items si
         INNER JOIN sales s ON s.id = si.sale_id
         LEFT JOIN inventory_items ii ON ii.id = si.inventory_item_id
         ${p.where}
         ORDER BY si.id ASC LIMIT ?`,
      [...p.params, batchSize]
    );
    if (!rows.length) break;

    await resolveScopesForRows(rows);

    const factRows = [];
    for (const row of rows) {
      const id = Number(row.id);
      maxId = Math.max(maxId, id);
      cursor = Math.max(cursor, id);

      const dateKey = saleBusinessDate({ sale_date: row.sale_date, created_at: row.sale_created_at });
      if (dateKey == null) continue;
      dateKeySet.add(dateKey);

      const name = String(row.name || '').trim();
      const skuIsClinic = /^CLINIC-/i.test(String(row.sku || ''));
      const serviceKey = resolveClinicServiceKey(row);
      const isClinic = row.clinic_service_id != null || skuIsClinic || dims.maps.serviceByName(name) != null;
      const { isRefund, isSettlement } = classifySale(row);
      const quantity = num(row.quantity);
      const costPrice = num(row.cost_price);
      const productKey = isClinic ? null : dims.maps.product(row.inventory_item_id);

      factRows.push([
        id,
        Number(row.sale_id),
        dateKey,
        scopeKeyOf(row),
        dims.maps.user(row.user_id),
        productKey,
        isClinic ? serviceKey : null,
        isClinic ? 1 : 0,
        isRefund ? 1 : 0,
        isSettlement ? 1 : 0,
        row.sku || null,
        row.name || null,
        quantity,
        num(row.unit_price),
        num(row.discount),
        num(row.total),
        costPrice,
        isClinic ? 0 : quantity * costPrice,
      ]);
    }

    written += await upsertFacts('fact_sale_items', SALE_ITEM_COLUMNS, factRows, SALE_ITEM_UPDATE);
    if (rows.length < batchSize) break;
  }

  return { rows: written, maxId, dateKeys: [...dateKeySet] };
}

// ─────────────────────────────────────────────────────────────────────────────
// Inventory movements
// ─────────────────────────────────────────────────────────────────────────────

const MOVEMENT_COLUMNS = [
  'entry_id', 'date_key', 'scope_key', 'product_key', 'user_key',
  'event_type', 'quantity_in', 'quantity_out', 'unit_cost', 'value',
  'reference_type', 'reference_id', 'entry_date', 'created_at_utc',
];
const MOVEMENT_UPDATE = MOVEMENT_COLUMNS.filter((c) => c !== 'entry_id');

async function syncInventoryMovements({
  mode, fromKey, toKey, state, batchSize = BATCH_SIZE,
}) {
  const cols = await dims.oltpColumns('inventory_ledger_entries');
  const tsColumns = [];
  if (cols.has('created_at')) tsColumns.push('created_at');
  if (cols.has('updated_at')) tsColumns.push('updated_at');

  const dateExpr = `COALESCE(${cols.has('entry_date') ? 'le.entry_date' : 'NULL'},
                            ${tzSqlDate('le.created_at')})`;

  const select = [
    'le.id', 'le.inventory_item_id', 'le.scope_type', 'le.scope_id',
    'le.event_type', 'le.quantity_in', 'le.quantity_out',
    cols.has('created_by') ? 'le.created_by' : 'NULL AS created_by',
    cols.has('reference_type') ? 'le.reference_type' : 'NULL AS reference_type',
    cols.has('reference_id') ? 'le.reference_id' : 'NULL AS reference_id',
    cols.has('entry_date') ? 'le.entry_date' : 'NULL AS entry_date',
    'le.created_at',
    'ii.cost_price',
  ].join(', ');

  let cursor = 0;
  let written = 0;
  let maxId = 0;
  const dateKeySet = new Set();

  for (;;) {
    const p = buildPredicate({
      alias: 'le',
      idColumn: 'id',
      tsColumns,
      mode,
      highWaterId: state?.last_high_water_id,
      watermark: state?.last_sync_at,
      cursor,
      dateExpr,
      fromKey,
      toKey,
    });
    const [rows] = await oltpPool.execute(
      `SELECT ${select}
         FROM inventory_ledger_entries le
         LEFT JOIN inventory_items ii ON ii.id = le.inventory_item_id
         ${p.where}
         ORDER BY le.id ASC LIMIT ?`,
      [...p.params, batchSize]
    );
    if (!rows.length) break;

    await resolveScopesForRows(rows);

    const factRows = [];
    for (const row of rows) {
      const id = Number(row.id);
      maxId = Math.max(maxId, id);
      cursor = Math.max(cursor, id);

      const dateKey = row.entry_date ? toDateKey(row.entry_date) : toDateKey(row.created_at);
      if (dateKey == null) continue;
      dateKeySet.add(dateKey);
      const unitCost = num(row.cost_price);
      const qty = num(row.quantity_in) + num(row.quantity_out);

      factRows.push([
        id,
        dateKey,
        scopeKeyOf(row),
        dims.maps.product(row.inventory_item_id),
        dims.maps.user(row.created_by),
        row.event_type || null,
        num(row.quantity_in),
        num(row.quantity_out),
        unitCost,
        qty * unitCost,
        row.reference_type || null,
        row.reference_id || null,
        row.entry_date || null,
        row.created_at || null,
      ]);
    }

    written += await upsertFacts('fact_inventory_movements', MOVEMENT_COLUMNS, factRows, MOVEMENT_UPDATE);
    if (rows.length < batchSize) break;
  }

  return { rows: written, maxId, dateKeys: [...dateKeySet] };
}

// ─────────────────────────────────────────────────────────────────────────────
// Ledger entries
// ─────────────────────────────────────────────────────────────────────────────

const LEDGER_COLUMNS = [
  'entry_id', 'date_key', 'ledger_id', 'account_name', 'account_type',
  'entry_type', 'debit', 'credit', 'reference_id', 'description',
  'scope_key', 'created_by', 'created_at_utc',
];
const LEDGER_UPDATE = LEDGER_COLUMNS.filter((c) => c !== 'entry_id');

async function syncLedgerEntries({
  mode, fromKey, toKey, state, batchSize = BATCH_SIZE,
}) {
  const cols = await dims.oltpColumns('ledger_entries');
  const tsColumns = cols.has('created_at') ? ['created_at'] : [];

  let cursor = 0;
  let written = 0;
  let maxId = 0;
  const dateKeySet = new Set();

  const select = [
    'le.id', 'le.branch_id', 'le.entry_type', 'le.debit_amount', 'le.credit_amount',
    cols.has('reference_id') ? 'le.reference_id' : 'NULL AS reference_id',
    cols.has('description') ? 'le.description' : 'NULL AS description',
    cols.has('created_by') ? 'le.created_by' : 'NULL AS created_by',
    'le.created_at',
    'l.account_name', 'l.account_type', 'l.scope_type', 'l.scope_id',
  ].join(', ');

  for (;;) {
    const p = buildPredicate({
      alias: 'le',
      idColumn: 'id',
      tsColumns,
      mode,
      highWaterId: state?.last_high_water_id,
      watermark: state?.last_sync_at,
      cursor,
      dateExpr: tzSqlDate('le.created_at'),
      fromKey,
      toKey,
    });
    const [rows] = await oltpPool.execute(
      `SELECT ${select}
         FROM ledger_entries le
         LEFT JOIN ledgers l ON l.id = le.branch_id
         ${p.where}
         ORDER BY le.id ASC LIMIT ?`,
      [...p.params, batchSize]
    );
    if (!rows.length) break;

    await resolveScopesForRows(rows);

    const factRows = [];
    for (const row of rows) {
      const id = Number(row.id);
      maxId = Math.max(maxId, id);
      cursor = Math.max(cursor, id);

      const dateKey = toDateKey(row.created_at);
      if (dateKey == null) continue;
      dateKeySet.add(dateKey);

      factRows.push([
        id,
        dateKey,
        row.branch_id || null,
        row.account_name || null,
        row.account_type || null,
        row.entry_type || null,
        num(row.debit_amount),
        num(row.credit_amount),
        row.reference_id || null,
        row.description || null,
        scopeKeyOf(row),
        row.created_by || null,
        row.created_at || null,
      ]);
    }

    written += await upsertFacts('fact_ledger_entries', LEDGER_COLUMNS, factRows, LEDGER_UPDATE);
    if (rows.length < batchSize) break;
  }

  return { rows: written, maxId, dateKeys: [...dateKeySet] };
}

// ─────────────────────────────────────────────────────────────────────────────
// Expenses / financial vouchers
// ─────────────────────────────────────────────────────────────────────────────

const EXPENSE_COLUMNS = [
  'voucher_id', 'date_key', 'scope_key', 'user_key', 'voucher_no', 'type',
  'category', 'expense_category', 'payment_method', 'amount', 'status', 'created_at_utc',
];
const EXPENSE_UPDATE = EXPENSE_COLUMNS.filter((c) => c !== 'voucher_id');

async function syncExpenses({
  mode, fromKey, toKey, state, batchSize = BATCH_SIZE,
}) {
  const cols = await dims.oltpColumns('financial_vouchers');
  const tsColumns = [];
  if (cols.has('updated_at')) tsColumns.push('updated_at');
  if (cols.has('created_at')) tsColumns.push('created_at');

  let cursor = 0;
  let written = 0;
  let maxId = 0;
  const dateKeySet = new Set();

  const select = [
    'v.id', 'v.scope_type', 'v.scope_id', 'v.user_id',
    cols.has('voucher_no') ? 'v.voucher_no' : 'NULL AS voucher_no',
    'v.type', 'v.category',
    cols.has('expense_category') ? 'v.expense_category' : 'NULL AS expense_category',
    cols.has('payment_method') ? 'v.payment_method' : 'NULL AS payment_method',
    'v.amount', 'v.status', 'v.created_at',
    cols.has('updated_at') ? 'v.updated_at' : 'NULL AS updated_at',
  ].join(', ');

  for (;;) {
    const p = buildPredicate({
      alias: 'v',
      idColumn: 'id',
      tsColumns,
      mode,
      highWaterId: state?.last_high_water_id,
      watermark: state?.last_sync_at,
      cursor,
      dateExpr: tzSqlDate('v.created_at'),
      fromKey,
      toKey,
    });
    const [rows] = await oltpPool.execute(
      `SELECT ${select} FROM financial_vouchers v ${p.where} ORDER BY v.id ASC LIMIT ?`,
      [...p.params, batchSize]
    );
    if (!rows.length) break;

    await resolveScopesForRows(rows);

    const factRows = [];
    for (const row of rows) {
      const id = Number(row.id);
      maxId = Math.max(maxId, id);
      cursor = Math.max(cursor, id);

      const dateKey = toDateKey(row.created_at);
      if (dateKey == null) continue;
      dateKeySet.add(dateKey);

      factRows.push([
        id,
        dateKey,
        scopeKeyOf(row),
        dims.maps.user(row.user_id),
        row.voucher_no || null,
        row.type || null,
        row.category || null,
        row.expense_category || null,
        row.payment_method || null,
        num(row.amount),
        row.status || null,
        row.created_at || null,
      ]);
    }

    written += await upsertFacts('fact_expenses', EXPENSE_COLUMNS, factRows, EXPENSE_UPDATE);
    if (rows.length < batchSize) break;
  }

  return { rows: written, maxId, dateKeys: [...dateKeySet] };
}

// ─────────────────────────────────────────────────────────────────────────────
// Sales returns
// ─────────────────────────────────────────────────────────────────────────────

const RETURN_COLUMNS = [
  'return_id', 'date_key', 'scope_key', 'user_key', 'original_sale_id',
  'return_no', 'total_refund', 'status', 'created_at_utc',
];
const RETURN_UPDATE = RETURN_COLUMNS.filter((c) => c !== 'return_id');

async function syncSalesReturns({
  mode, fromKey, toKey, state, batchSize = BATCH_SIZE,
}) {
  try {
    const [chk] = await oltpPool.execute("SHOW TABLES LIKE 'sales_returns'");
    if (!chk.length) return 0;
  } catch { /* ignore */ }

  const cols = await dims.oltpColumns('sales_returns');
  const tsColumns = cols.has('created_at') ? ['created_at'] : [];
  const saleCols = await dims.oltpColumns('sales');

  let cursor = 0;
  let written = 0;
  let maxId = 0;
  const dateKeySet = new Set();

  const select = [
    'r.id', cols.has('original_sale_id') ? 'r.original_sale_id' : (cols.has('sale_id') ? 'r.sale_id AS original_sale_id' : 'NULL AS original_sale_id'),
    cols.has('return_no') ? 'r.return_no' : 'NULL AS return_no',
    cols.has('total_refund') ? 'r.total_refund' : (cols.has('refund_amount') ? 'r.refund_amount AS total_refund' : '0 AS total_refund'),
    cols.has('status') ? 'r.status' : "'COMPLETED' AS status",
    'r.created_at',
    cols.has('user_id') ? 'r.user_id' : 'NULL AS user_id',
    saleCols.has('scope_type') ? 's.scope_type' : 'NULL AS scope_type',
    saleCols.has('scope_id') ? 's.scope_id' : 'NULL AS scope_id',
    saleCols.has('user_id') ? 's.user_id AS sale_user_id' : 'NULL AS sale_user_id',
  ].join(', ');

  for (;;) {
    const p = buildPredicate({
      alias: 'r',
      idColumn: 'id',
      tsColumns,
      mode,
      highWaterId: state?.last_high_water_id,
      watermark: state?.last_sync_at,
      cursor,
      dateExpr: tzSqlDate('r.created_at'),
      fromKey,
      toKey,
    });
    const joinSql = (cols.has('original_sale_id') || cols.has('sale_id')) && saleCols.has('id')
      ? `LEFT JOIN sales s ON s.id = ${cols.has('original_sale_id') ? 'r.original_sale_id' : 'r.sale_id'}`
      : '';
    const [rows] = await oltpPool.execute(
      `SELECT ${select}
         FROM sales_returns r
         ${joinSql}
         ${p.where}
         ORDER BY r.id ASC LIMIT ?`,
      [...p.params, batchSize]
    );
    if (!rows.length) break;

    await resolveScopesForRows(rows);

    const factRows = [];
    for (const row of rows) {
      const id = Number(row.id);
      maxId = Math.max(maxId, id);
      cursor = Math.max(cursor, id);

      const dateKey = toDateKey(row.created_at);
      if (dateKey == null) continue;
      dateKeySet.add(dateKey);

      factRows.push([
        id,
        dateKey,
        scopeKeyOf(row),
        dims.maps.user(row.user_id ?? row.sale_user_id),
        row.original_sale_id || null,
        row.return_no || null,
        num(row.total_refund),
        row.status || null,
        row.created_at || null,
      ]);
    }

    written += await upsertFacts('fact_sales_returns', RETURN_COLUMNS, factRows, RETURN_UPDATE);
    if (rows.length < batchSize) break;
  }

  return { rows: written, maxId, dateKeys: [...dateKeySet] };
}

const SOURCES = [
  { table: 'sales', run: syncSales },
  { table: 'sale_items', run: syncSaleItems },
  { table: 'inventory_ledger_entries', run: syncInventoryMovements },
  { table: 'ledger_entries', run: syncLedgerEntries },
  { table: 'financial_vouchers', run: syncExpenses },
  { table: 'sales_returns', run: syncSalesReturns },
];

/**
 * Run all fact extractors.
 * @returns {{rows:number, dateKeys:number[], perSource:object[]}}
 */
async function syncAll({ mode, fromKey, toKey, states = {}, onProgress }) {
  let rows = 0;
  const dateKeys = new Set();
  const perSource = [];

  for (const source of SOURCES) {
    const state = states[source.table] || null;
    const result = await source.run({
      mode,
      fromKey,
      toKey,
      state,
    });
    rows += result.rows;
    result.dateKeys.forEach((k) => dateKeys.add(k));
    perSource.push({
      table: source.table,
      mode,
      rows: result.rows,
      maxId: result.maxId,
      dateKeys: result.dateKeys.length,
    });
    if (onProgress) onProgress(source.table, result);
  }

  return { rows, dateKeys: [...dateKeys].sort((a, b) => a - b), perSource };
}

/** Delete fact rows for a business-date window (hard-delete reconciliation). */
async function deleteFactsInRange(fromKey, toKey) {
  if (fromKey == null || toKey == null) return;
  const tables = [
    'fact_sales',
    'fact_sale_items',
    'fact_inventory_movements',
    'fact_ledger_entries',
    'fact_expenses',
    'fact_sales_returns',
  ];
  for (const table of tables) {
    await dwPool.execute(
      `DELETE FROM ${table} WHERE date_key BETWEEN ? AND ?`,
      [fromKey, toKey]
    );
  }
}

/** Truncate all facts. Aggregates are rebuilt afterwards. */
async function truncateFacts() {
  const tables = [
    'fact_sales',
    'fact_sale_items',
    'fact_inventory_movements',
    'fact_ledger_entries',
    'fact_expenses',
    'fact_sales_returns',
    'fact_inventory_snapshot',
  ];
  for (const table of tables) {
    await dwPool.query(`TRUNCATE TABLE ${table}`);
  }
}

module.exports = {
  syncAll,
  syncSales,
  syncSaleItems,
  syncInventoryMovements,
  syncLedgerEntries,
  syncExpenses,
  syncSalesReturns,
  deleteFactsInRange,
  truncateFacts,
  upsertFacts,
  classifySale,
  BATCH_SIZE,
};
