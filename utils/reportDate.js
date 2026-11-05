/**
 * Report date helpers.
 *
 * OLTP timestamps are stored in the source DB server's timezone (SYSTEM by
 * default), not UTC. Reports must first convert them to the business timezone
 * (Asia/Karachi, UTC+05:00) before extracting a date/month/year.
 */

const SOURCE_TIMEZONE = process.env.SOURCE_TIMEZONE || 'SYSTEM';
const REPORT_TIMEZONE = process.env.REPORT_TIMEZONE || '+05:00';

/**
 * Returns a SQL expression that converts a source timestamp column to the
 * report timezone and extracts the date.
 */
const dateColumnInTz = (column) =>
  `DATE(CONVERT_TZ(${column}, '${SOURCE_TIMEZONE}', '${REPORT_TIMEZONE}'))`;

/**
 * Returns a SQL expression that converts a source timestamp column to the
 * report timezone and extracts a YYYYMMDD integer date key.
 */
const dateKeyColumnInTz = (column) =>
  `DATE_FORMAT(CONVERT_TZ(${column}, '${SOURCE_TIMEZONE}', '${REPORT_TIMEZONE}'), '%Y%m%d')`;

/**
 * Returns a SQL expression that converts a source timestamp column to UTC.
 */
const utcColumnInTz = (column) =>
  `CONVERT_TZ(${column}, '${SOURCE_TIMEZONE}', '+00:00')`;

/**
 * Returns a SQL expression that extracts the year in the report timezone.
 */
const yearColumnInTz = (column) =>
  `YEAR(CONVERT_TZ(${column}, '${SOURCE_TIMEZONE}', '${REPORT_TIMEZONE}'))`;

/**
 * Returns a SQL expression that extracts the month in the report timezone.
 */
const monthColumnInTz = (column) =>
  `MONTH(CONVERT_TZ(${column}, '${SOURCE_TIMEZONE}', '${REPORT_TIMEZONE}'))`;

/**
 * Returns a SQL expression for the current date in the report timezone.
 * UTC_TIMESTAMP() is session-timezone independent, so the source is always UTC.
 */
const currentDateInTz = () =>
  `DATE(CONVERT_TZ(UTC_TIMESTAMP(), '+00:00', '${REPORT_TIMEZONE}'))`;

/**
 * Returns a SQL expression for the current year in the report timezone.
 */
const currentYearInTz = () =>
  `YEAR(CONVERT_TZ(UTC_TIMESTAMP(), '+00:00', '${REPORT_TIMEZONE}'))`;

/**
 * Returns a SQL expression for the current month in the report timezone.
 */
const currentMonthInTz = () =>
  `MONTH(CONVERT_TZ(UTC_TIMESTAMP(), '+00:00', '${REPORT_TIMEZONE}'))`;

module.exports = {
  SOURCE_TIMEZONE,
  REPORT_TIMEZONE,
  dateColumnInTz,
  dateKeyColumnInTz,
  utcColumnInTz,
  yearColumnInTz,
  monthColumnInTz,
  currentDateInTz,
  currentYearInTz,
  currentMonthInTz,
};
