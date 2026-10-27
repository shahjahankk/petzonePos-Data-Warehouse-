/**
 * SQL fragments that classify `sale_items` lines.
 *
 * Must stay in sync with the POS backend's `utils/clinicSaleItem.js` so the
 * warehouse reports match the legacy report semantics exactly.
 */

const CLINIC_LINE_SQL = `(
  si.clinic_service_id IS NOT NULL
  OR UPPER(COALESCE(si.sku, '')) LIKE 'CLINIC-%'
  OR EXISTS (
    SELECT 1 FROM clinic_services _cs
    WHERE CONVERT(LOWER(TRIM(_cs.name)) USING utf8mb4) COLLATE utf8mb4_unicode_ci
        = CONVERT(LOWER(TRIM(si.name)) USING utf8mb4) COLLATE utf8mb4_unicode_ci
  )
)`;

const PRODUCT_LINE_SQL = `(
  si.inventory_item_id IS NOT NULL
  AND si.clinic_service_id IS NULL
  AND UPPER(COALESCE(si.sku, '')) NOT LIKE 'CLINIC-%'
  AND NOT EXISTS (
    SELECT 1 FROM clinic_services _cs
    WHERE CONVERT(LOWER(TRIM(_cs.name)) USING utf8mb4) COLLATE utf8mb4_unicode_ci
        = CONVERT(LOWER(TRIM(si.name)) USING utf8mb4) COLLATE utf8mb4_unicode_ci
  )
)`;

module.exports = { CLINIC_LINE_SQL, PRODUCT_LINE_SQL };
