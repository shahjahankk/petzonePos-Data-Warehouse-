/**
 * Dimension refresh.
 *
 * Dimensions are Type-1 (current values). They are re-read from OLTP at the
 * start of every ETL run, which is cheap relative to the facts and keeps
 * names/categories/prices up to date.
 */

const { pool: oltpPool } = require('../../config/database');
const { dwPool } = require('../../config/warehouseDatabase');

const oltpColumnCache = new Map();

async function oltpColumns(table) {
  if (oltpColumnCache.has(table)) return oltpColumnCache.get(table);
  const [rows] = await oltpPool.execute(
    `SELECT COLUMN_NAME AS name
       FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`,
    [table]
  );
  const set = new Set(rows.map((r) => r.name));
  oltpColumnCache.set(table, set);
  return set;
}

async function bulkUpsert(table, columns, rows, updateColumns, chunkSize = 500) {
  if (!rows.length) return 0;
  const colSql = columns.join(', ');
  const updateSql = updateColumns
    .map((c) => `${c} = VALUES(${c})`)
    .join(', ');
  const placeholders = `(${columns.map(() => '?').join(', ')})`;
  let written = 0;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const sql = `INSERT INTO ${table} (${colSql}) VALUES ${chunk.map(() => placeholders).join(', ')}
      ON DUPLICATE KEY UPDATE ${updateSql}`;
    await dwPool.query(sql, chunk.flat());
    written += chunk.length;
  }
  return written;
}

const state = {
  scopeByAlias: new Map(),   // `${scopeType}|${aliasLower}` -> scope_key
  userById: new Map(),       // user_id -> user_key
  customerById: new Map(),   // customer_id -> customer_key
  productById: new Map(),    // inventory_item_id -> product_key
  serviceById: new Map(),    // clinic_service_id -> service_key
  serviceByName: new Map(),  // lower(name) -> service_key
  loadedAt: null,
};

function resetMaps() {
  state.scopeByAlias.clear();
  state.userById.clear();
  state.customerById.clear();
  state.productById.clear();
  state.serviceById.clear();
  state.serviceByName.clear();
}

async function refreshScopes() {
  const [branches] = await oltpPool.execute('SELECT id, name, code FROM branches');
  const [warehouses] = await oltpPool.execute('SELECT id, name, code FROM warehouses');

  const scopeRows = [];
  const aliases = [];

  const addScope = (type, row) => {
    const id = String(row.id);
    const name = row.name || null;
    scopeRows.push([type, id, name, 1]);
    const seen = new Set();
    for (const raw of [id, name, row.code]) {
      if (raw === null || raw === undefined || raw === '') continue;
      const alias = String(raw).trim();
      const key = `${type}|${alias.toLowerCase()}`;
      if (seen.has(key)) continue;
      seen.add(key);
      aliases.push([type, alias, `${type}|${id}`]);
    }
  };

  branches.forEach((row) => addScope('BRANCH', row));
  warehouses.forEach((row) => addScope('WAREHOUSE', row));

  await bulkUpsert(
    'dim_scope',
    ['scope_type', 'scope_id', 'scope_name', 'is_active'],
    scopeRows,
    ['scope_name', 'is_active'],
    500
  );

  const [scopeMap] = await dwPool.execute('SELECT scope_key, scope_type, scope_id FROM dim_scope');
  const keyByTypeId = new Map();
  for (const row of scopeMap) {
    keyByTypeId.set(`${row.scope_type}|${String(row.scope_id)}`, row.scope_key);
  }

  resetMaps();

  // Rebuild aliases from scratch so removed branches/warehouses disappear.
  await dwPool.query('DELETE FROM map_scope_alias');

  const aliasRows = [];
  const seenAlias = new Set();
  for (const [type, alias, typeId] of aliases) {
    const scopeKey = keyByTypeId.get(typeId);
    if (scopeKey == null) continue;
    const lower = alias.toLowerCase();
    const dedupe = `${type}|${lower}`;
    if (seenAlias.has(dedupe)) continue;
    seenAlias.add(dedupe);
    aliasRows.push([type, alias, scopeKey]);
    state.scopeByAlias.set(dedupe, scopeKey);
  }

  if (aliasRows.length) {
    const chunkSize = 500;
    for (let i = 0; i < aliasRows.length; i += chunkSize) {
      const chunk = aliasRows.slice(i, i + chunkSize);
      await dwPool.query(
        'INSERT INTO map_scope_alias (scope_type, alias, scope_key) VALUES ?',
        [chunk]
      );
    }
  }

  return aliasRows.length;
}

async function refreshUsers() {
  const cols = await oltpColumns('users');
  const where = cols.has('deleted_at') ? 'WHERE deleted_at IS NULL' : '';
  const [rows] = await oltpPool.execute(
    `SELECT id, username, role, status, branch_id, warehouse_id FROM users ${where}`
  );

  const data = rows.map((r) => [
    r.id,
    r.username || null,
    r.role || null,
    r.status || null,
    r.branch_id || null,
    r.warehouse_id || null,
  ]);

  await bulkUpsert(
    'dim_user',
    ['user_id', 'username', 'role', 'status', 'branch_id', 'warehouse_id'],
    data,
    ['username', 'role', 'status', 'branch_id', 'warehouse_id'],
    500
  );

  const [map] = await dwPool.execute('SELECT user_key, user_id FROM dim_user');
  for (const row of map) state.userById.set(Number(row.user_id), row.user_key);
}

async function refreshCustomers() {
  const cols = await oltpColumns('customers');
  const where = cols.has('deleted_at') ? 'WHERE deleted_at IS NULL' : '';
  const [rows] = await oltpPool.execute(
    `SELECT id, name, phone FROM customers ${where}`
  );

  const data = rows.map((r) => [r.id, r.name || null, r.phone || null]);

  await bulkUpsert(
    'dim_customer',
    ['customer_id', 'name', 'phone'],
    data,
    ['name', 'phone'],
    500
  );

  const [map] = await dwPool.execute('SELECT customer_key, customer_id FROM dim_customer');
  for (const row of map) {
    if (row.customer_id != null) state.customerById.set(Number(row.customer_id), row.customer_key);
  }
}

async function refreshProducts() {
  const cols = await oltpColumns('inventory_items');
  const softDelete = cols.has('deleted_at') ? 'i.deleted_at' : 'NULL';
  const [rows] = await oltpPool.execute(
    `SELECT i.id, i.sku, i.barcode, i.name, i.category, i.unit,
            i.cost_price, i.selling_price, i.min_stock_level,
            i.supplier_id, i.supplier_name, i.scope_type, i.scope_id,
            ${softDelete} AS deleted_at
       FROM inventory_items i`
  );

  // Register scopes referenced only by inventory items (no sales/movements yet),
  // so the daily snapshot never drops a product for an unknown location.
  for (const r of rows) {
    const scopeType = normalizeScopeType(r.scope_type);
    if (r.scope_id === null || r.scope_id === undefined || r.scope_id === '') continue;
    if (resolveScopeKeySync(scopeType, r.scope_id) == null) {
      await resolveScopeKey(scopeType, r.scope_id);
    }
  }

  const data = rows.map((r) => {
    const scopeType = normalizeScopeType(r.scope_type);
    const scopeKey = resolveScopeKeySync(scopeType, r.scope_id);
    return [
      r.id,
      r.sku || null,
      r.barcode || null,
      r.name || null,
      r.category || null,
      r.unit || null,
      r.supplier_id || null,
      r.supplier_name || null,
      Number(r.cost_price) || 0,
      Number(r.selling_price) || 0,
      Number(r.min_stock_level) || 0,
      scopeKey,
      r.deleted_at ? 1 : 0,
    ];
  });

  await bulkUpsert(
    'dim_product',
    ['inventory_item_id', 'sku', 'barcode', 'name', 'category', 'unit',
      'supplier_id', 'supplier_name', 'cost_price', 'selling_price',
      'min_stock_level', 'scope_key', 'is_deleted'],
    data,
    ['sku', 'barcode', 'name', 'category', 'unit', 'supplier_id', 'supplier_name',
      'cost_price', 'selling_price', 'min_stock_level', 'scope_key', 'is_deleted'],
    500
  );

  const [map] = await dwPool.execute('SELECT product_key, inventory_item_id FROM dim_product');
  for (const row of map) {
    state.productById.set(Number(row.inventory_item_id), row.product_key);
  }
}

async function refreshClinicServices() {
  const [rows] = await oltpPool.execute(
    `SELECT s.id, s.code, s.name, s.category_id, s.default_price, s.status,
            c.name AS category_name
       FROM clinic_services s
       LEFT JOIN clinic_service_categories c ON c.id = s.category_id`
  );

  const data = rows.map((r) => [
    r.id,
    r.code || null,
    r.name || null,
    r.category_id || null,
    r.category_name || null,
    Number(r.default_price) || 0,
    r.status || null,
  ]);

  await bulkUpsert(
    'dim_clinic_service',
    ['clinic_service_id', 'code', 'name', 'category_id', 'category_name', 'default_price', 'status'],
    data,
    ['code', 'name', 'category_id', 'category_name', 'default_price', 'status'],
    500
  );

  // Placeholder used for legacy clinic lines that cannot be matched to a service.
  await dwPool.execute(
    `INSERT INTO dim_clinic_service
       (clinic_service_id, code, name, category_id, category_name, default_price, status)
     VALUES (0, 'UNKNOWN', 'Uncategorized service', NULL, 'Uncategorized', 0, 'ACTIVE')
     ON DUPLICATE KEY UPDATE name = VALUES(name), category_name = VALUES(category_name)`
  );

  const [map] = await dwPool.execute(
    'SELECT service_key, clinic_service_id, name FROM dim_clinic_service'
  );
  for (const row of map) {
    state.serviceById.set(Number(row.clinic_service_id), row.service_key);
    if (row.name) state.serviceByName.set(String(row.name).trim().toLowerCase(), row.service_key);
  }
}

function normalizeScopeType(scopeType) {
  const t = String(scopeType || 'BRANCH').trim().toUpperCase();
  if (t === 'WAREHOUSE') return 'WAREHOUSE';
  if (t === 'BRANCH') return 'BRANCH';
  return t || 'BRANCH';
}

function resolveScopeKeySync(scopeType, rawScopeId) {
  if (rawScopeId === null || rawScopeId === undefined || rawScopeId === '') return null;
  const type = normalizeScopeType(scopeType);
  const alias = String(rawScopeId).trim().toLowerCase();
  return state.scopeByAlias.get(`${type}|${alias}`) ?? null;
}

/**
 * Resolve a raw scope value (id or name) to a dim_scope key. Unknown values are
 * registered on the fly so no fact row is ever dropped.
 */
async function resolveScopeKey(scopeType, rawScopeId) {
  if (rawScopeId === null || rawScopeId === undefined || rawScopeId === '') return null;
  const type = normalizeScopeType(scopeType);
  const alias = String(rawScopeId).trim();
  const cacheKey = `${type}|${alias.toLowerCase()}`;
  const hit = state.scopeByAlias.get(cacheKey);
  if (hit != null) return hit;

  await dwPool.execute(
    `INSERT INTO dim_scope (scope_type, scope_id, scope_name, is_active)
     VALUES (?, ?, ?, 1)
     ON DUPLICATE KEY UPDATE scope_name = VALUES(scope_name)`,
    [type, alias, alias]
  );
  const [rows] = await dwPool.execute(
    'SELECT scope_key FROM dim_scope WHERE scope_type = ? AND scope_id = ? LIMIT 1',
    [type, alias]
  );
  const scopeKey = rows[0]?.scope_key ?? null;
  if (scopeKey != null) {
    state.scopeByAlias.set(cacheKey, scopeKey);
    await dwPool.execute(
      `INSERT INTO map_scope_alias (scope_type, alias, scope_key) VALUES (?, ?, ?)
       ON DUPLICATE KEY UPDATE scope_key = VALUES(scope_key)`,
      [type, alias, scopeKey]
    );
  }
  return scopeKey;
}

async function refreshAll() {
  await refreshScopes();
  await refreshUsers();
  await refreshCustomers();
  await refreshProducts();
  await refreshClinicServices();
  state.loadedAt = new Date();
  return {
    loadedAt: state.loadedAt,
    scopes: state.scopeByAlias.size,
    users: state.userById.size,
    customers: state.customerById.size,
    products: state.productById.size,
    services: state.serviceById.size,
  };
}

const maps = {
  scope: (scopeType, rawScopeId) => resolveScopeKey(scopeType, rawScopeId),
  scopeSync: resolveScopeKeySync,
  user: (userId) => (userId == null ? null : state.userById.get(Number(userId)) ?? null),
  customer: (customerId) => (customerId == null ? null : state.customerById.get(Number(customerId)) ?? null),
  product: (itemId) => (itemId == null ? null : state.productById.get(Number(itemId)) ?? null),
  service: (serviceId) => (serviceId == null ? null : state.serviceById.get(Number(serviceId)) ?? null),
  serviceByName: (name) => {
    if (!name) return null;
    return state.serviceByName.get(String(name).trim().toLowerCase()) ?? null;
  },
  isLoaded: () => Boolean(state.loadedAt),
  info: () => ({
    loadedAt: state.loadedAt,
    scopes: state.scopeByAlias.size,
    users: state.userById.size,
    customers: state.customerById.size,
    products: state.productById.size,
    services: state.serviceById.size,
  }),
};

module.exports = {
  refreshAll,
  refreshScopes,
  refreshUsers,
  refreshCustomers,
  refreshProducts,
  refreshClinicServices,
  resolveScopeKey,
  resolveScopeKeySync,
  maps,
  oltpColumns,
  bulkUpsert,
};
