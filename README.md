# PetZone Data Warehouse

Standalone OLAP reporting service for the PetZone POS. It has its **own Node process**, its
**own database** (`petzone_dw`), its own ETL and scheduler, and its own report API.

The storefront/frontend is unchanged: the POS backend transparently proxies the existing
`/api/reports/*` endpoints to this service.

```
petzone-pos-frontend
        │  GET /api/reports/sales|clinic|inventory|ledger|financial
        ▼
backend/petzoneSoftware-backend   (auth, POS writes, /api/reports/summary)
        │  reverse proxy when WAREHOUSE_SERVICE_URL is set
        ▼
petzone-data-warehouse            (this app)
        │  reads aggregates only
        ▼
MySQL: petzone_dw                 (dim_* / fact_* / agg_*)
        ▲
        │ incremental ETL
        └── reads the POS database (multipos_db) every N minutes
```

## 1. Why a separate app

- Report reads never touch the OLTP database, so POS checkout latency is unaffected.
- The warehouse can be restarted, rebuilt, or moved to another host without touching POS.
- ETL is isolated from the backend process; a warehouse failure cannot take down sales.
- The frontend keeps the same URLs and payloads.

## 2. Requirements

- Node.js 18+
- MySQL 8 (or MariaDB 10.6+) with `CONVERT_TZ` usable for the configured timezone
- Read access to the POS database
- Create/read/write access to the warehouse database
- The same `JWT_SECRET` as `backend/petzoneSoftware-backend`

## 3. Install

```bash
cd petzone-data-warehouse
npm install
cp .env.example .env
# edit .env — DB_* (POS), DW_DB_* (warehouse), JWT_SECRET
```

Create the warehouse database and tables:

```bash
npm run dw:init
```

Load all history (first run only):

```bash
npm run dw:backfill
npm run dw:status
```

Start the service:

```bash
npm start
```

Default port: `4100` (`PORT`).

## 4. Connect the POS backend (frontend unchanged)

In `backend/petzoneSoftware-backend/.env` add:

```env
WAREHOUSE_SERVICE_URL=http://127.0.0.1:4100
WAREHOUSE_PROXY_FALLBACK=true
WAREHOUSE_PROXY_TIMEOUT_MS=8000
```

Restart the POS backend. It proxies these five routes to the warehouse service:

- `GET /api/reports/sales`
- `GET /api/reports/clinic`
- `GET /api/reports/inventory`
- `GET /api/reports/ledger`
- `GET /api/reports/financial`

`GET /api/reports/summary` stays on OLTP because it is a cheap cross-module count.

If the warehouse service is down or returns an error, the backend falls back to the legacy
OLTP report controller (controlled by `WAREHOUSE_PROXY_FALLBACK`), so reports stay online.

> Do not run the integrated warehouse scheduler/ETL in the POS backend. It was moved here.
> Only the HTTP proxy remains in the backend.

## 5. API

All routes require `Authorization: Bearer <token>`.

| Method | Path | Description |
|---|---|---|
| GET | `/api/reports/sales` | product sales report |
| GET | `/api/reports/clinic` | clinic services report |
| GET | `/api/reports/inventory` | inventory report |
| GET | `/api/reports/ledger` | ledger report |
| GET | `/api/reports/financial` | financial report |
| GET | `/api/warehouse-reports/status` | ETL watermarks, lag, counts |
| POST | `/api/warehouse-reports/sync` | admin only: `{ "mode": "incremental" }` |
| GET | `/health` | unauthenticated liveness check |

The report payloads are byte-compatible in shape with the legacy
`controllers/reportsController.js` responses (`{ success: true, data: ... }`).

## 6. Scheduling

The app schedules ETL itself:

- `incremental` every `DW_INCREMENTAL_MINUTES` (default 15)
- `daily` at `DW_DAILY_SYNC_TIME` (default `12:00` in `REPORT_TIMEZONE_NAME`),
  which also reconciles the last `DW_RECONCILE_DAYS` and refreshes the inventory snapshot
- `DW_STARTUP_DELAY_SECONDS` after boot, one incremental run

On hosts where the process sleeps (cPanel/Passenger), disable the internal scheduler and use
cron:

```env
DW_SCHEDULER_ENABLED=false
```

```cron
*/15 * * * * cd /path/to/petzone-data-warehouse && /usr/bin/node scripts/warehouse-sync-once.js --mode=incremental >> logs/warehouse.log 2>&1
0 12 * * *   cd /path/to/petzone-data-warehouse && /usr/bin/node scripts/warehouse-sync-once.js --mode=daily       >> logs/warehouse.log 2>&1
```

Both paths are safe together: a MySQL advisory lock (`GET_LOCK('petzone_dw_etl', 0)`) plus an
in-process mutex prevent overlapping runs.

## 7. Commands

| Command | Purpose |
|---|---|
| `npm run dw:init` | create the database + schema |
| `npm run dw:backfill` | full history load |
| `npm run dw:rebuild` | truncate facts/aggregates and reload everything |
| `npm run dw:sync` | one incremental load |
| `npm run dw:daily` | incremental + reconcile + snapshot |
| `npm run dw:status` | print watermarks/runs/counts |

Window repair (hard deletes/edits outside the reconcile window):

```bash
node scripts/warehouse-backfill.js --from=2026-10-01 --to=2026-10-23
```

## 8. Environment

See `.env.example` for the complete list. The important ones:

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `4100` | service port |
| `JWT_SECRET` | — | **must match the POS backend** |
| `DB_*` | POS defaults | source OLTP database |
| `DW_DB_*` | falls back to `DB_*` | warehouse database, default name `petzone_dw` |
| `REPORT_TIMEZONE` | `+05:00` | SQL `CONVERT_TZ` offset used during extraction |
| `REPORT_TIMEZONE_NAME` | `Asia/Karachi` | IANA zone for business dates + scheduler |
| `DW_REPORT_CACHE_SECONDS` | `30` | in-process response cache TTL |

> `REPORT_TIMEZONE` and `REPORT_TIMEZONE_NAME` must describe the same zone.

## 9. Data model

- `dim_*`: scope/branch/warehouse, user, customer, product, clinic service, date
- `fact_*`: sales, sale items, inventory movements, ledger entries, expenses, returns,
  inventory snapshots
- `agg_*`: daily/monthly marts read by the report API

Reports read **only** `agg_*` / `fact_*` / `dim_*` tables. Business dates are Pakistan dates
stored as an integer `date_key` (YYYYMMDD).

See [`docs/data-warehouse.md`](docs/data-warehouse.md) for the design, incremental rules,
partition rebuilds, and known limitations.

## 10. Backup / recovery

The warehouse is derived data. Safe recovery path:

```bash
npm run dw:init
npm run dw:rebuild
npm run dw:status
```

The OLTP database is never modified by this service.

## 11. Security

- Reads OLTP with the provided DB user; use a read-only user if possible.
- Verifies the POS JWT locally; never trusts a user id from the request body.
- Role scope (`ADMIN`, `CASHIER`, `WAREHOUSE_KEEPER`) is applied to every report query.
- Admin-only manual sync: `POST /api/warehouse-reports/sync`.
