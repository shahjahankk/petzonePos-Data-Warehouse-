/**
 * Warehouse-backed report endpoints.
 *
 * Same response envelopes as `controllers/reportsController.js`:
 *   { success: true, data: ... }
 * so the frontend can switch between OLTP and OLAP without changes.
 */

const warehouseReports = require('../services/warehouse/reportQueryService');
const { warehouseEnabled } = require('../config/warehouseDatabase');
const { runEtl, isRunning } = require('../services/warehouse/etlOrchestrator');

function send(res, promise, errorMessage) {
  promise
    .then((data) => res.json({ success: true, data }))
    .catch((error) => {
      console.error(`${errorMessage}:`, error);
      res.status(500).json({ success: false, message: errorMessage, error: error.message });
    });
}

const getSalesReports = (req, res) => {
  send(res, warehouseReports.getSalesReports(req.user, req.query), 'Error retrieving sales reports');
};

const getClinicSalesReports = (req, res) => {
  send(res, warehouseReports.getClinicReports(req.user, req.query), 'Error retrieving clinic reports');
};

const getInventoryReports = (req, res) => {
  send(res, warehouseReports.getInventoryReports(req.user, req.query), 'Error retrieving inventory reports');
};

const getLedgerReports = (req, res) => {
  send(res, warehouseReports.getLedgerReports(req.user, req.query), 'Error retrieving ledger reports');
};

const getFinancialReports = (req, res) => {
  send(res, warehouseReports.getFinancialReports(req.user, req.query), 'Error retrieving financial reports');
};

const getStatus = async (req, res) => {
  if (!warehouseEnabled) {
    return res.json({ success: true, data: { enabled: false, running: false } });
  }
  try {
    const data = await warehouseReports.getWarehouseStatus();
    res.json({ success: true, data: { ...data, enabled: true, running: isRunning() } });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Error reading warehouse status', error: error.message });
  }
};

const postSync = async (req, res) => {
  if (!warehouseEnabled) {
    return res.status(409).json({ success: false, message: 'Warehouse is disabled (DW_ENABLED=false)' });
  }
  if (isRunning()) {
    return res.status(409).json({ success: false, message: 'Warehouse ETL is already running' });
  }
  const allowed = ['incremental', 'daily', 'backfill', 'rebuild'];
  const mode = allowed.includes(req.body?.mode) ? req.body.mode : 'incremental';

  try {
    const summary = await runEtl({
      mode,
      from: req.body?.from || null,
      to: req.body?.to || null,
    });
    res.json({ success: true, data: summary });
  } catch (error) {
    console.error('Warehouse manual sync failed:', error);
    res.status(500).json({ success: false, message: 'Warehouse sync failed', error: error.message });
  }
};

module.exports = {
  getSalesReports,
  getClinicSalesReports,
  getInventoryReports,
  getLedgerReports,
  getFinancialReports,
  getStatus,
  postSync,
};
