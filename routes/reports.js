const express = require('express');
const controller = require('../controllers/warehouseReportsController');

const router = express.Router();

// Auth is applied globally in server.js.
router.get('/sales', controller.getSalesReports);
router.get('/clinic', controller.getClinicSalesReports);
router.get('/inventory', controller.getInventoryReports);
router.get('/ledger', controller.getLedgerReports);
router.get('/financial', controller.getFinancialReports);

module.exports = router;
