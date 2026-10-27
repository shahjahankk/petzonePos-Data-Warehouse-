/**
 * JWT authentication compatible with the POS backend.
 *
 * The frontend keeps sending the same Bearer token. Set `JWT_SECRET` to the
 * exact same value used by `backend/petzoneSoftware-backend`.
 */

const jwt = require('jsonwebtoken');
const { pool } = require('../config/database');

function accessSecret() {
  if (!process.env.JWT_SECRET || process.env.JWT_SECRET.length < 16) {
    throw new Error('JWT_SECRET is not configured (must be at least 16 characters)');
  }
  return process.env.JWT_SECRET;
}

async function auth(req, res, next) {
  try {
    const header = req.headers.authorization;
    if (!header || !header.startsWith('Bearer ')) {
      return res.status(401).json({ success: false, message: 'Access token required' });
    }

    let decoded;
    try {
      decoded = jwt.verify(header.substring(7), accessSecret());
    } catch (error) {
      if (error.name === 'TokenExpiredError') {
        return res.status(401).json({ success: false, message: 'Token expired' });
      }
      return res.status(401).json({ success: false, message: 'Invalid token' });
    }

    let rows;
    try {
      [rows] = await pool.execute(
        `SELECT id, username, role, status, branch_id, warehouse_id
           FROM users
          WHERE id = ? AND deleted_at IS NULL
          LIMIT 1`,
        [decoded.userId]
      );
    } catch {
      return res.status(503).json({
        success: false,
        message: 'Unavailable: could not verify user. Please try again.',
      });
    }

    const row = rows[0];
    if (!row) {
      return res.status(401).json({ success: false, message: 'User not found' });
    }
    if (row.status !== 'ACTIVE') {
      return res.status(401).json({ success: false, message: 'User account is not active' });
    }

    req.user = {
      id: row.id,
      username: row.username,
      role: row.role,
      status: row.status,
      branchId: row.branch_id,
      warehouseId: row.warehouse_id,
    };
    return next();
  } catch (error) {
    console.error('[warehouse] auth error:', error);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
}

module.exports = auth;
