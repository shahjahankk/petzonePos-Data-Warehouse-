/**
 * Report date helpers.
 *
 * The application stores timestamps in UTC. Business reports are viewed in
 * Pakistan time (+05:00), so DATE() filters must convert UTC to the report
 * timezone first.
 */

const REPORT_TIMEZONE = process.env.REPORT_TIMEZONE || '+05:00';

/**
 * Returns a SQL expression that converts a UTC timestamp column to the report
 * timezone and extracts the date: DATE(CONVERT_TZ(col, '+00:00', tz)).
 */
const dateColumnInTz = (column) =>
  `DATE(CONVERT_TZ(${column}, '+00:00', '${REPORT_TIMEZONE}'))`;

/**
 * Returns a SQL expression that extracts the year in the report timezone.
 */
const yearColumnInTz = (column) =>
  `YEAR(CONVERT_TZ(${column}, '+00:00', '${REPORT_TIMEZONE}'))`;

/**
 * Returns a SQL expression that extracts the month in the report timezone.
 */
const monthColumnInTz = (column) =>
  `MONTH(CONVERT_TZ(${column}, '+00:00', '${REPORT_TIMEZONE}'))`;

/**
 * Returns a SQL expression for the current date in the report timezone.
 */
const currentDateInTz = () =>
  `DATE(CONVERT_TZ(NOW(), '+00:00', '${REPORT_TIMEZONE}'))`;

/**
 * Returns a SQL expression for the current year in the report timezone.
 */
const currentYearInTz = () =>
  `YEAR(CONVERT_TZ(NOW(), '+00:00', '${REPORT_TIMEZONE}'))`;

/**
 * Returns a SQL expression for the current month in the report timezone.
 */
const currentMonthInTz = () =>
  `MONTH(CONVERT_TZ(NOW(), '+00:00', '${REPORT_TIMEZONE}'))`;

module.exports = {
  REPORT_TIMEZONE,
  dateColumnInTz,
  yearColumnInTz,
  monthColumnInTz,
  currentDateInTz,
  currentYearInTz,
  currentMonthInTz,
};
