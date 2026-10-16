// =============================================================================
// Roblox Realtime Editor — Bridge Server (Open Cloud v2)
// =============================================================================

import express from "express";
import crypto from "node:crypto";

const PORT             = process.env.PORT             ? Number(process.env.PORT)             : 8080;
const HOST             = process.env.HOST             || "0.0.0.0";
const EDITOR_KEY       = process.env.EDITOR_KEY       || "";
const ROBLOX_API_KEY   = process.env.ROBLOX_API_KEY   || "";
const ROBLOX_UNIVERSE  = process.env.ROBLOX_UNIVERSE_ID || "";
const DS_NAME          = process.env.ROBLOX_DATASTORE_NAME || "RealtimeEditorLog";
const LIVE_TOPIC       = process.env.LIVE_TOPIC       || "RealtimeEditor_Live_v1";
const DS_ENTRY_KEY     = "change_log_v1";

function assertEnv() {
  const missing = [];
  if (!EDITOR_KEY)      missing.push("EDITOR_KEY");
  if (!ROBLOX_API_KEY)  missing.push("ROBLOX_API_KEY");
  if (!ROBLOX_UNIVERSE) missing.push("ROBLOX_UNIVERSE_ID");
  if (missing.length) {
    console.error(`[bridge] FATAL: missing env: ${missing.join(", ")}`);
    process.exit(1);
  }
}
assertEnv();

const API_BASE = "https://apis.roblox.com/cloud/v2";
let changeCache = null;
let changeCacheETag = null;

// LIVE SYNC STATE — latest snapshot only, kept in memory
let latestState = null;
let latestStateAt = 0;

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
  return null;
}

async function dsUpsertEntry(dataStoreName, entryKey, value, ifMatch) {
  const patchUrl = `${API_BASE}/universes/${ROBLOX_UNIVERSE}/data-stores/${encodeURIComponent(dataStoreName)}/entries/${encodeURIComponent(entryKey)}?allow_missing=true`;
  const headers = { "x-api-key": ROBLOX_API_KEY, "Content-Type": "application/json" };
  if (ifMatch) headers["If-Match"] = ifMatch;
  const body = JSON.stringify({ value });
  const res = await fetch(patchUrl, { method: "PATCH", headers, body });
  if (res.status === 200) {
    return { ok: true, etag: res.headers.get("etag") || res.headers.get("ETag") };
  }
  if (res.status === 412 || res.status === 409) return { ok: false, conflict: true };
  return { ok: false };
}

async function loadCacheFromDataStore() {
  const got = await dsGetEntry(DS_NAME, DS_ENTRY_KEY);
  if (!got) { changeCache = []; return changeCache; }
  changeCache = Array.isArray(got.value) ? got.value : [];
  changeCacheETag = got.etag || null;
  return changeCache;
}

async function persistWithRetry(maxAttempts = 5) {
  for (let i = 1; i <= maxAttempts; i++) {
    const r = await dsUpsertEntry(DS_NAME, DS_ENTRY_KEY, changeCache, changeCacheETag);
    if (r.ok) { changeCacheETag = r.etag; return true; }
    if (r.conflict) { await loadCacheFromDataStore(); continue; }
    return false;
  }
  return false;
}

async function broadcastLive(change) {
  const payload = JSON.stringify(change);
  if (payload.length > 1000) return false;
  const url = `${API_BASE}/universes/${ROBLOX_UNIVERSE}:publishMessage`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "x-api-key": ROBLOX_API_KEY, "Content-Type": "application/json" },
    body: JSON.stringify({ topic: LIVE_TOPIC, message: payload }),
  });
  return res.status === 200;
}

function authMiddleware(req, res, next) {
  const key = req.header("X-Editor-Key") || "";
  if (!key || key !== EDITOR_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }
  next();
}

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
    hasState: latestState !== null,
    stateAgeMs: latestState ? Date.now() - latestStateAt : -1,
  });
});

app.post("/api/change", authMiddleware, async (req, res) => {
  const change = req.body;
  if (!change || typeof change !== "object" || !change.target || !change.target.path) {
    return res.status(400).json({ error: "bad change record" });
  }
  change.id = change.id || `${Date.now()}-${crypto.randomUUID().slice(0, 8)}`;
  change.timestamp = change.timestamp || Date.now();
  if (!Array.isArray(changeCache)) await loadCacheFromDataStore();
  const existing = changeCache.findIndex((c) => c.id === change.id);
  if (existing >= 0) changeCache[existing] = change;
  else changeCache.push(change);
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
  if (changeCache.length === before) return res.status(404).json({ error: "not found" });
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

// LIVE SYNC STATE ENDPOINTS
app.post("/api/state", authMiddleware, (req, res) => {
  const snap = req.body;
  if (!snap || typeof snap !== "object") return res.status(400).json({ error: "bad snapshot" });
  latestState = snap;
  latestStateAt = Date.now();
  res.json({ ok: true, parts: (snap.parts || []).length, players: (snap.players || []).length });
});

app.get("/api/state", authMiddleware, (req, res) => {
  if (!latestState) return res.json({ ok: false, reason: "no snapshots received yet" });
  res.json({ ok: true, ageMs: Date.now() - latestStateAt, snapshot: latestState });
});

async function start() {
  console.log("[bridge] booting...");
  console.log(`[bridge] universeId = ${ROBLOX_UNIVERSE}`);
  console.log(`[bridge] datastore = ${DS_NAME}`);
  console.log(`[bridge] live topic = ${LIVE_TOPIC}`);
  console.log(`[bridge] API base (v2) = ${API_BASE}`);
  await loadCacheFromDataStore();
  app.listen(PORT, HOST, () => {
    console.log(`[bridge] listening on http://${HOST}:${PORT}`);
    console.log("[bridge] endpoints:");
    console.log("  GET    /health");
    console.log("  POST   /api/change         (X-Editor-Key)  editor change");
    console.log("  GET    /api/changes        (X-Editor-Key)  list changes");
    console.log("  POST   /api/state          (X-Editor-Key)  live sync snapshot (in)");
    console.log("  GET    /api/state          (X-Editor-Key)  live sync snapshot (out)");
  });
}
start().catch((err) => { console.error("[bridge] boot failure:", err); process.exit(1); });
