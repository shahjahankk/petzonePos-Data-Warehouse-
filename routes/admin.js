const express = require('express');
const controller = require('../controllers/warehouseReportsController');
const { requireAdmin } = require('../middleware/rbac');

const router = express.Router();

// Auth is applied globally in server.js.
router.get('/status', controller.getStatus);
router.post('/sync', requireAdmin, controller.postSync);

module.exports = router;
