/**
 * Tiny in-process TTL cache for report responses.
 *
 * Reports are read-heavy and the warehouse refreshes every few minutes, so a
 * short TTL removes repeated query cost without making data feel stale.
 */

const ttlSeconds = parseInt(process.env.DW_REPORT_CACHE_SECONDS, 10);
const maxEntries = parseInt(process.env.DW_REPORT_CACHE_MAX, 10);
const TTL_MS = Math.max(0, Number.isFinite(ttlSeconds) ? ttlSeconds : 30) * 1000;
const MAX_ENTRIES = Math.max(10, Number.isFinite(maxEntries) ? maxEntries : 200);

const store = new Map();

function get(key) {
  if (!TTL_MS) return null;
  const hit = store.get(key);
  if (!hit) return null;
  if (Date.now() > hit.expiresAt) {
    store.delete(key);
    return null;
  }
  return hit.value;
}

function set(key, value) {
  if (!TTL_MS) return;
  if (store.size >= MAX_ENTRIES) {
    // Drop the oldest entry (Map preserves insertion order).
    const oldest = store.keys().next().value;
    if (oldest !== undefined) store.delete(oldest);
  }
  store.set(key, { value, expiresAt: Date.now() + TTL_MS });
}

function clear() {
  store.clear();
}

function stats() {
  return { entries: store.size, ttlSeconds: TTL_MS / 1000 };
}

module.exports = { get, set, clear, stats };
