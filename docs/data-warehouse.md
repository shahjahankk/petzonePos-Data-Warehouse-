# PetZone Data Warehouse (DW) — Design & Runbook

Status: v1 (implemented)
Owner: PetZone Engineering
Scope: Reporting only. The OLTP database remains the single source of truth for writes.

---

## 1. Why

Report endpoints in `controllers/reportsController.js` run live joins/aggregations over
`sales`, `sale_items`, `inventory_ledger_entries`, `ledger_entries`, `financial_vouchers`,
`customers`, etc. On production-sized data this is slow (multi-second, sometimes timeouts)
and date bucketing is done in JavaScript with mixed timezone rules, which produced the
wrong-day bugs.

Goal:

* Reports never touch OLTP tables at read time.
* Report responses < 3 seconds (target < 300 ms for cached, < 1 s cold).
* Daily consolidated load at a configurable time (default 12:00 PKT).
* "Changed, not full" loads: every change in OLTP is picked up incrementally, and only the
  affected days are re-aggregated.
* One timezone rule everywhere: **business dates are Pakistan dates** (`Asia/Karachi`,
  configurable), derived once during ETL and stored as an integer `date_key` (YYYYMMDD).

---

## 2. Architecture

This is a **standalone application** (`petzone-data-warehouse/`) with its own Node process,
its own database (`petzone_dw`), and its own ETL scheduler.

```
  POS / dashboard
  (frontend, unchanged)
        │  GET /api/reports/{sales,clinic,inventory,ledger,financial}
        ▼
  backend/petzoneSoftware-backend
  routes/reports.js
        │  auth + reverse proxy (WAREHOUSE_SERVICE_URL)
        │  fallback to legacy reportsController on failure
        ▼
  petzone-data-warehouse  (this app)
        │  reads aggregates only
        ▼
  petzone_dw (OLAP)
  dim_* / fact_* / agg_*
        ▲
        │ ETL: incremental + reconcile
        │
  multipos_db (OLTP, read-only)
```

The POS backend still owns `/api/reports/summary` (cheap cross-module counts). Every other
report path is served by this service. The frontend sees no change.

The warehouse lives in a separate MySQL database (`petzone_dw`) on the same server by
default. It can be moved to another host with env vars. It contains only derived data and
can be dropped/rebuilt at any time.

---

## 3. Schema (star + marts)

### Dimensions

| Table | Grain | Notes |
|---|---|---|
| `dim_date` | one row per calendar day | PK `date_key` = YYYYMMDD, `month_key` = YYYYMM |
| `dim_scope` | one row per branch/warehouse | canonical `scope_type` + `scope_id` (numeric id as text) |
| `map_scope_alias` | one row per alias | maps legacy `scope_id` values (name **or** id) to `scope_key` |
| `dim_user` | one row per user | `user_id`, username, role |
| `dim_customer` | one row per customer | `customer_key` is `NULL` for walk-ins |
| `dim_product` | one row per inventory item | current cost/selling price, category, scope |
| `dim_clinic_service` | one row per clinic service | category id/name |

`map_scope_alias` exists because `sales.scope_id` sometimes stores a branch **name** and
sometimes a branch **id** (string). Resolving this once at ETL time removes a whole class
of report bugs.

### Facts

| Table | Grain | Source |
|---|---|---|
| `fact_sales` | one sale header | `sales` |
| `fact_sale_items` | one sale line | `sale_items` + `inventory_items` + `clinic_services` |
| `fact_inventory_movements` | one stock ledger entry | `inventory_ledger_entries` |
| `fact_ledger_entries` | one accounting entry | `ledger_entries` + `ledgers` |
| `fact_expenses` | one voucher | `financial_vouchers` |
| `fact_sales_returns` | one return | `sales_returns` |
| `fact_inventory_snapshot` | one product/scope/day | computed from `fact_inventory_movements` (OLAP only) |

`fact_sales` carries `product_total`, `clinic_total`, `cogs`, `gross_profit`, and the
`is_real_sale` / `is_refund` / `is_settlement` flags, so refunds and outstanding
settlements never contaminate revenue KPIs. It also precomputes the legacy product-share
fields (`product_paid`, `product_credit`, `product_discount`, `product_tax`,
`product_collected`) and `clinic_eligible`, so report queries are pure sums with no
conditional business logic. `fact_sale_items.cost_price` stores the inventory item's
current cost (matching legacy profit calculations), not the line's historical price.

### Aggregates (marts) — the tables reports actually read

| Table | PK | Used by |
|---|---|---|
| `agg_sales_daily` | (date_key, scope_key, user_key, payment_method, payment_group) | sales KPIs, by date/cashier/branch/method, financial |
| `agg_product_daily` | (date_key, scope_key, user_key, product_key) | top products, COGS, profit, inventory best-sellers |
| `agg_clinic_daily` | (date_key, scope_key, user_key, service_key) | clinic report |
| `agg_inventory_daily` | (date_key, scope_key, product_key) | movement chart |
| `agg_ledger_daily` | (date_key, scope_key, account_type, entry_type) | ledger report |
| `agg_expense_monthly` | (month_key, scope_key, expense_category) | expense breakdown, P&L |
| `agg_financial_monthly` | (month_key, scope_key) | P&L, cash-flow, revenue by period |

Aggregates are **partitioned by date/month**. The ETL only deletes and rebuilds the
partitions that changed, never the whole table.

### ETL bookkeeping

| Table | Purpose |
|---|---|
| `etl_state` | per-source high-water id, last sync timestamp, last error |
| `etl_runs` | audit log of every ETL run (mode, duration, rows, status, error) |

---

## 4. ETL

### 4.1 Modes

| Mode | Trigger | What it does |
|---|---|---|
| `incremental` | every `DW_INCREMENTAL_MINUTES` (default 15) | pulls rows changed since last watermark (with overlap), upserts facts, rebuilds affected date keys |
| `daily` | every day at `DW_DAILY_SYNC_TIME` (default `12:00`, `Asia/Karachi`) | incremental + reconciles the last `DW_RECONCILE_DAYS` days (catches hard deletes/edits) + refreshes inventory snapshot |
| `backfill` | `npm run dw:backfill` | full load of all history |
| `rebuild` | `npm run dw:rebuild` | truncate facts/aggregates and reload everything |

### 4.2 Change detection

For each source table the ETL reads the best available watermark column
(`updated_at` → `created_at` → none) and the max `id`. A row is a candidate when:

```sql
id > last_high_water_id
OR COALESCE(updated_at, created_at) >= (last_sync_at - overlap)
```

The overlap (`DW_SYNC_OVERLAP_MINUTES`, default 10) absorbs clock skew and transactions
committed mid-run. Upserts are idempotent, so re-reading rows is harmless.

### 4.3 Affected partitions

Every staged row yields a `date_key` (Pakistan date; backdated sales use `sale_date`).
The ETL collects the distinct affected `date_key`s and, for monthly marts, the affected
`month_key`s. Only those partitions are deleted and rebuilt from facts.

`today` is always included so the current day is live-ish even with watermark edge cases.

### 4.4 Idempotency & safety

* All fact writes are `INSERT … ON DUPLICATE KEY UPDATE`.
* Aggregate rebuilds are partition-scoped `DELETE` + `INSERT … SELECT`. A crash between
  the two leaves that partition empty until the next run rebuilds it; reports stay online.
* Only one ETL run can execute at a time (in-process mutex **plus** a MySQL advisory
  `GET_LOCK('petzone_dw_etl', 0)`, so server + cron cannot overlap).
* ETL failures never affect POS writes. The scheduler logs and retries next tick.
* Warehouse connection is separate from the OLTP pool and is never used for writes to OLTP.

---

## 5. Report API

The frontend contract is unchanged. The POS backend keeps the same five URLs and proxies
them to this service when `WAREHOUSE_SERVICE_URL` is configured:

```env
# backend/petzoneSoftware-backend/.env
WAREHOUSE_SERVICE_URL=http://127.0.0.1:4100
WAREHOUSE_PROXY_FALLBACK=true
WAREHOUSE_PROXY_TIMEOUT_MS=8000
```

If the proxy request fails (service down, timeout, or non-2xx response), the backend falls
back to the legacy OLTP controller and logs the error, so reporting never goes dark.

This service exposes the same paths directly:

| Method | Path | Description |
|---|---|---|
| GET | `/api/reports/sales` | product sales report |
| GET | `/api/reports/clinic` | clinic services report |
| GET | `/api/reports/inventory` | inventory report |
| GET | `/api/reports/ledger` | ledger report |
| GET | `/api/reports/financial` | financial report |
| GET | `/api/warehouse-reports/status` | last successful sync, row counts, lag |
| POST | `/api/warehouse-reports/sync` | admin-only on-demand sync (`{ "mode": "incremental" \| "daily" \| "backfill" }`) |
| GET | `/health` | unauthenticated liveness check |

`/api/reports/summary` stays on the POS backend (cheap cross-module counts).

### Performance

* Aggregate tables are indexed on `(date_key, scope_key, user_key, …)`.
* Date filters are integer range scans (`date_key BETWEEN ? AND ?`).
* Role scoping is a `scope_key IN (...)` / `user_key IN (...)` filter, resolved once.
* Results are cached in-process for `DW_REPORT_CACHE_SECONDS` (default 30 s), keyed by
  route + user scope + query string.
* Typical cold query: 20–150 ms. Cached: < 5 ms.

---

## 6. Timezone rules

* OLTP timestamps are stored as UTC (`config/database.js` uses `timezone: 'Z'`).
* One rule: **business date = date in `REPORT_TIMEZONE_NAME`** (default `Asia/Karachi`).
* Derived once in ETL via `services/warehouse/dateKeys.js`; stored as `date_key`.
* Backdated sales use `sales.sale_date` as the business date (matches user intent).
* No JavaScript `toISOString().split('T')[0]` or bare `toLocaleDateString()` anywhere in
  the reporting path.
* SQL date functions in legacy code still use `utils/reportDate.js` offsets; the DW does
  not depend on them.

---

## 7. Environment variables

```env
# ── Service ───────────────────────────────────────────────────────────────────
PORT=4100
CORS_ORIGIN=*

# Must match the POS backend exactly.
JWT_SECRET=replace-with-the-same-secret-as-the-pos-backend

# ── OLTP (source) ─────────────────────────────────────────────────────────────
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=
DB_NAME=multipos_db
DB_POOL_LIMIT=10
DB_QUEUE_LIMIT=50

# ── OLAP (warehouse) ──────────────────────────────────────────────────────────
# Enable/disable the whole module (default true)
DW_ENABLED=true

# Dedicated warehouse database. If DW_DB_HOST is omitted it reuses the OLTP host/user.
DW_DB_HOST=localhost
DW_DB_PORT=3306
DW_DB_USER=root
DW_DB_PASSWORD=
DW_DB_NAME=petzone_dw
DW_DB_POOL_LIMIT=5
DW_DB_QUEUE_LIMIT=50

# Report timezone. Both values must describe the same zone.
REPORT_TIMEZONE=+05:00
REPORT_TIMEZONE_NAME=Asia/Karachi

# ETL scheduling
DW_SCHEDULER_ENABLED=true
DW_DAILY_SYNC_TIME=12:00          # HH:mm in REPORT_TIMEZONE_NAME
DW_INCREMENTAL_MINUTES=15
DW_RECONCILE_DAYS=7
DW_SYNC_OVERLAP_MINUTES=10
DW_BATCH_SIZE=500
DW_STARTUP_DELAY_SECONDS=15       # first incremental run after boot
DW_AUTO_INIT=true                 # create schema on first run

# Read path
DW_REPORT_CACHE_SECONDS=30
DW_REPORT_CACHE_MAX=200
```

`DW_DB_*` fall back to `DB_*` when unset, except `DW_DB_NAME` which falls back to
`petzone_dw` (never to the OLTP database).

---

## 8. Operations

### First install

```bash
cd petzone-data-warehouse
npm install
cp .env.example .env             # fill in DB_*, DW_DB_*, JWT_SECRET
npm run dw:init                  # create DB + tables (idempotent)
npm run dw:backfill              # load all history (can take a while)
npm run dw:status                # check freshness
```

Enable the proxy in the POS backend only **after** the backfill finishes:

```env
WAREHOUSE_SERVICE_URL=http://127.0.0.1:4100
```

### Daily

The in-process scheduler handles both incremental and daily runs. On hosts where the Node
process may sleep (cPanel/Passenger), add a cron job:

```cron
*/15 * * * * cd /path/to/petzone-data-warehouse && /usr/bin/node scripts/warehouse-sync-once.js --mode=incremental >> logs/warehouse.log 2>&1
0 12 * * *   cd /path/to/petzone-data-warehouse && /usr/bin/node scripts/warehouse-sync-once.js --mode=daily       >> logs/warehouse.log 2>&1
```

Set `DW_SCHEDULER_ENABLED=false` when using cron to avoid double runs (double runs are
safe thanks to the advisory lock and idempotency, just wasteful).

### Recovery

Any inconsistency can be fixed by rebuilding a window or everything:

```bash
node scripts/warehouse-backfill.js --from=2026-10-01 --to=2026-10-23
npm run dw:rebuild
```

### Rollback

Remove `WAREHOUSE_SERVICE_URL` from the POS backend (or set
`WAREHOUSE_PROXY_FALLBACK=true` and stop this service). Reports go back to OLTP
immediately. The warehouse can stay offline without impacting POS.

---

## 9. Files

```
server.js
config/database.js
config/warehouseDatabase.js
middleware/auth.js
middleware/rbac.js
routes/reports.js
routes/admin.js
controllers/warehouseReportsController.js
utils/reportDate.js
utils/clinicLines.js
warehouse/schema.sql
services/warehouse/dateKeys.js
services/warehouse/schema.js
services/warehouse/etlState.js
services/warehouse/dimensionService.js
services/warehouse/factSyncService.js
services/warehouse/aggregateService.js
services/warehouse/snapshotService.js
services/warehouse/etlOrchestrator.js
services/warehouse/warehouseScheduler.js
services/warehouse/reportCache.js
services/warehouse/reportQueryService.js
scripts/warehouse-init.js
scripts/warehouse-backfill.js
scripts/warehouse-sync-once.js
scripts/warehouse-status.js
```

---

## 10. Known limitations (v1)

* Hard deletes/edits are only detected automatically during the daily reconcile window
  (`DW_RECONCILE_DAYS`). Older changes require a targeted window backfill:
  `node scripts/warehouse-backfill.js --from=YYYY-MM-DD --to=YYYY-MM-DD`.
* Inventory snapshots start on the first ETL run; historical daily snapshots are not
  reconstructed retroactively.
* Customer/product dimension history is Type-1 (current values). Cost/profit for past
  periods therefore uses the current cost price, matching the legacy report behaviour.
* `GET /api/reports/summary` stays on OLTP (cheap, cross-module counts); it is not
  proxied to the warehouse service.
