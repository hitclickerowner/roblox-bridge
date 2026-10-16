// =============================================================================
// Roblox Realtime Editor — Bridge Server (Open Cloud v2)
// -----------------------------------------------------------------------------
// Receives changes from the in-experience Luau scripts, persists them to the
// Open Cloud DataStore v2 API, and broadcasts them to every running server of
// your experience via the Open Cloud MessagingService v2 API. Also exposes a
// REST surface for the (optional) Studio plugin and the Luau bootstrap.
//
// Current endpoints (as of 2025 Open Cloud v2):
//   MessagingService:  POST https://apis.roblox.com/cloud/v2/universes/{universe}:publishMessage
//   DataStore list:     GET .../cloud/v2/universes/{universe}/data-stores/{ds}/entries?maxPageSize=...
//   DataStore get:      GET .../cloud/v2/universes/{universe}/data-stores/{ds}/entries/{entry}
//   DataStore upsert:  POST .../cloud/v2/universes/{universe}/data-stores/{ds}/entries (body has key+value)
//
// All v2 calls use the `x-api-key` header for auth.
// =============================================================================

import express from "express";
import crypto from "node:crypto";

// -- env -----------------------------------------------------------------------
const PORT             = process.env.PORT             ? Number(process.env.PORT)             : 8080;
const HOST             = process.env.HOST             || "0.0.0.0";
const EDITOR_KEY       = process.env.EDITOR_KEY       || "";
const ROBLOX_API_KEY   = process.env.ROBLOX_API_KEY   || "";
const ROBLOX_UNIVERSE  = process.env.ROBLOX_UNIVERSE_ID || "";
const DS_NAME          = process.env.ROBLOX_DATASTORE_NAME || "RealtimeEditorLog";
const LIVE_TOPIC       = process.env.LIVE_TOPIC       || "RealtimeEditor_Live_v1";
const DS_ENTRY_KEY     = "change_log_v1";

// -- guards --------------------------------------------------------------------
function assertEnv() {
  const missing = [];
  if (!EDITOR_KEY)      missing.push("EDITOR_KEY");
  if (!ROBLOX_API_KEY)  missing.push("ROBLOX_API_KEY");
  if (!ROBLOX_UNIVERSE) missing.push("ROBLOX_UNIVERSE_ID");
  if (missing.length) {
    console.error(`[bridge] FATAL: missing env: ${missing.join(", ")}`);
    console.error("[bridge] Copy .env.example to .env and fill it in.");
    process.exit(1);
  }
  if (ROBLOX_UNIVERSE.length < 5 || !/^\d+$/.test(ROBLOX_UNIVERSE)) {
    console.warn(`[bridge] WARNING: ROBLOX_UNIVERSE_ID looks unusual ("${ROBLOX_UNIVERSE}"). Should be a long number.`);
  }
}
assertEnv();

const API_BASE = "https://apis.roblox.com/cloud/v2";

// -- in-memory cache (DataStore is the source of truth) -----------------------
let changeCache = null; // array of change records, or null = not yet loaded

// =============================================================================
// Open Cloud v2 — DataStore helpers
// =============================================================================

// GET a single entry's value. Returns {value, etag} or null.
async function dsGetEntry(dataStoreName, entryKey) {
  const url = `${API_BASE}/universes/${ROBLOX_UNIVERSE}/data-stores/${encodeURIComponent(dataStoreName)}/entries/${encodeURIComponent(entryKey)}`;
  const res = await fetch(url, {
    method: "GET",
    headers: { "x-api-key": ROBLOX_API_KEY, "Content-Type": "application/json" },
  });
  if (res.status === 200) {
    const body = await res.json();
    const etag = res.headers.get("etag") || res.headers.get("ETag");
    return { value: body.value ?? body, etag };
  }
  if (res.status === 404) return null;
  const text = await res.text();
  console.warn(`[bridge] dsGetEntry failed ${res.status}: ${text}`);
  return null;
}

// UPSERT an entry. v2 supports POST .../entries to create a new entry; we use
// the create-or-replace approach: if entry exists, PATCH it; else POST it.
async function dsUpsertEntry(dataStoreName, entryKey, value, ifMatch) {
  const patchUrl = `${API_BASE}/universes/${ROBLOX_UNIVERSE}/data-stores/${encodeURIComponent(dataStoreName)}/entries/${encodeURIComponent(entryKey)}?allow_missing=true`;
  const headers = {
    "x-api-key": ROBLOX_API_KEY,
    "Content-Type": "application/json",
  };
  if (ifMatch) headers["If-Match"] = ifMatch;

  const body = JSON.stringify({ value });

  const res = await fetch(patchUrl, {
    method: "PATCH",
    headers,
    body,
  });
  if (res.status === 200) {
    const etag = res.headers.get("etag") || res.headers.get("ETag");
    return { ok: true, etag };
  }
  if (res.status === 404 || res.status === 405) {
    const postUrl = `${API_BASE}/universes/${ROBLOX_UNIVERSE}/data-stores/${encodeURIComponent(dataStoreName)}/entries`;
    const postRes = await fetch(postUrl, {
      method: "POST",
      headers: {
        "x-api-key": ROBLOX_API_KEY,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ key: entryKey, value }),
    });
    if (postRes.status === 200) {
      const etag = postRes.headers.get("etag") || postRes.headers.get("ETag");
      return { ok: true, etag };
    }
    const text = await postRes.text();
    console.warn(`[bridge] dsUpsert POST fallback failed ${postRes.status}: ${text}`);
    return { ok: false, status: postRes.status, body: text };
  }
  if (res.status === 412 || res.status === 409) {
    return { ok: false, status: res.status, conflict: true };
  }
  const text = await res.text();
  console.warn(`[bridge] dsUpsert PATCH failed ${res.status}: ${text}`);
  return { ok: false, status: res.status, body: text };
}

async function loadCacheFromDataStore() {
  const got = await dsGetEntry(DS_NAME, DS_ENTRY_KEY);
  if (!got) {
    changeCache = [];
    console.log("[bridge] DataStore entry doesn't exist yet; starting fresh.");
    return changeCache;
  }
  changeCache = Array.isArray(got.value) ? got.value : [];
  changeCacheETag = got.etag || null;
  console.log(`[bridge] loaded ${changeCache.length} change(s) from DataStore (etag=${changeCacheETag})`);
  return changeCache;
}

let changeCacheETag = null;

async function persistWithRetry(maxAttempts = 5) {
  for (let i = 1; i <= maxAttempts; i++) {
    const r = await dsUpsertEntry(DS_NAME, DS_ENTRY_KEY, changeCache, changeCacheETag);
    if (r.ok) {
      changeCacheETag = r.etag;
      return true;
    }
    if (r.conflict) {
      console.log(`[bridge] DataStore write collision on attempt ${i}; reloading...`);
      await loadCacheFromDataStore();
      continue;
    }
    return false;
  }
  return false;
}

// =============================================================================
// Open Cloud v2 — MessagingService publish
// =============================================================================

async function broadcastLive(change) {
  const payload = JSON.stringify(change);
  if (payload.length > 1000) {
    console.warn(`[bridge] change ${change.id} too large (${payload.length} bytes) for MessagingService; skipping live broadcast`);
    return false;
  }
  const url = `${API_BASE}/universes/${ROBLOX_UNIVERSE}:publishMessage`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "x-api-key": ROBLOX_API_KEY, "Content-Type": "application/json" },
    body: JSON.stringify({ topic: LIVE_TOPIC, message: payload }),
  });
  if (res.status === 200) {
    console.log(`[bridge] broadcast change ${change.id} to all servers`);
    return true;
  }
  const text = await res.text();
  console.warn(`[bridge] MessagingService POST failed ${res.status}: ${text}`);
  return false;
}

// =============================================================================
// Auth middleware
// =============================================================================
function authMiddleware(req, res, next) {
  const key = req.header("X-Editor-Key") || "";
  if (!key || key !== EDITOR_KEY) {
    return res.status(401).json({ error: "unauthorized", reason: "missing or wrong X-Editor-Key" });
  }
  next();
}

// =============================================================================
// Express app
// =============================================================================
const app = express();
app.use(express.json({ limit: "1mb" }));

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    changesCached: changeCache ? changeCache.length : -1,
    universeId: ROBLOX_UNIVERSE,
    cacheEtag: changeCacheETag,
    apiBase: API_BASE,
    liveTopic: LIVE_TOPIC,
  });
});

app.post("/api/change", authMiddleware, async (req, res) => {
  const change = req.body;
  if (!change || typeof change !== "object" || !change.target || !change.target.path) {
    return res.status(400).json({ error: "bad change record" });
  }
  change.id = change.id || `${Date.now()}-${crypto.randomUUID().slice(0, 8)}`;
  change.timestamp = change.timestamp || Date.now();
  change.receivedAt = Date.now();

  if (!Array.isArray(changeCache)) await loadCacheFromDataStore();
  const existing = changeCache.findIndex((c) => c.id === change.id);
  if (existing >= 0) {
    changeCache[existing] = change;
  } else {
    changeCache.push(change);
  }

  const persisted = await persistWithRetry();
  broadcastLive(change).catch((e) => console.warn("[bridge] broadcast error:", e));

  res.json({ ok: true, persisted, id: change.id, cached: changeCache.length });
});

app.get("/api/changes", authMiddleware, async (req, res) => {
  if (!Array.isArray(changeCache)) await loadCacheFromDataStore();
  const sorted = [...changeCache].sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
  res.json(sorted);
});

app.get("/api/changes/:id", authMiddleware, async (req, res) => {
  if (!Array.isArray(changeCache)) await loadCacheFromDataStore();
  const found = changeCache.find((c) => c.id === req.params.id);
  if (!found) return res.status(404).json({ error: "not found" });
  res.json(found);
});

app.delete("/api/changes/:id", authMiddleware, async (req, res) => {
  if (!Array.isArray(changeCache)) await loadCacheFromDataStore();
  const before = changeCache.length;
  changeCache = changeCache.filter((c) => c.id !== req.params.id);
  if (changeCache.length === before) {
    return res.status(404).json({ error: "not found" });
  }
  await persistWithRetry();
  res.json({ ok: true, remaining: changeCache.length });
});

app.post("/api/clear", authMiddleware, async (req, res) => {
  changeCache = [];
  changeCacheETag = null;
  await persistWithRetry();
  res.json({ ok: true });
});

app.post("/api/rebroadcast", authMiddleware, async (req, res) => {
  if (!Array.isArray(changeCache)) await loadCacheFromDataStore();
  let ok = 0, fail = 0;
  for (const change of changeCache) {
    const r = await broadcastLive(change);
    if (r) ok++; else fail++;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  res.json({ ok, fail, total: changeCache.length });
});

// =============================================================================
// Start
// =============================================================================
async function start() {
  console.log("[bridge] booting...");
  console.log(`[bridge] universeId      = ${ROBLOX_UNIVERSE}`);
  console.log(`[bridge] datastore        = ${DS_NAME} (entry key "${DS_ENTRY_KEY}")`);
  console.log(`[bridge] live topic       = ${LIVE_TOPIC}`);
  console.log(`[bridge] API base (v2)     = ${API_BASE}`);
  console.log(`[bridge] EDITOR_KEY prefix = ${EDITOR_KEY.slice(0, 6)}...`);
  console.log(`[bridge] API_KEY prefix    = ${ROBLOX_API_KEY.slice(0, 6)}...`);

  await loadCacheFromDataStore();

  app.listen(PORT, HOST, () => {
    console.log(`[bridge] listening on http://${HOST}:${PORT}`);
    console.log("[bridge] endpoints:");
    console.log("  GET    /health");
    console.log("  POST   /api/change         (X-Editor-Key)");
    console.log("  GET    /api/changes        (X-Editor-Key)");
    console.log("  GET    /api/changes/:id    (X-Editor-Key)");
    console.log("  DELETE /api/changes/:id    (X-Editor-Key)");
    console.log("  POST   /api/clear          (X-Editor-Key)");
    console.log("  POST   /api/rebroadcast    (X-Editor-Key)");
  });
}

start().catch((err) => {
  console.error("[bridge] boot failure:", err);
  process.exit(1);
});