// =============================
// Part 1/6
// =============================

// worker.js — hosei-content-api + LINE ingest
// (GitHub images + R2 images/video/poster + OpenAI(text-only + Vision optional JSON Schema) + Gemini fallback -> D1)
// + KV scoped debug (openai/gemini/line/db/general) + OpenAI request-id/usage logs
//
// - GET  /health
// - GET  /import        : (legacy CSV -> D1) optional  ✅ token required
// - GET  /posts?type=   : posts JSON   ✅ type whitelist
// - GET  /api/{type}    : alias to /posts?type=
// - GET  /media/<key>   : R2 object proxy (image/video/poster)  (Range supported)
// - POST /line-webhook  : LINE ingress (admin only)
// - GET  /debug-last    : last debug log (admin only, header token auth, scope normalize)
//
// Bindings:
//   env.DB (D1)
//   env.KV (KV)
//   env.R2 (R2)
//
// Secrets:
//   LINE_CHANNEL_ACCESS_TOKEN
//   LINE_CHANNEL_SECRET
//   ADMIN_USER_ID
//   OPENAI_API_KEY
//   OPENAI_MODEL              (text用)
//   OPENAI_VISION_MODEL       (画像入力用 / Vision用) 例: gpt-4.1-mini
//   GEMINI_API_KEY
//   GEMINI_MODEL
//   GITHUB_TOKEN
//   GITHUB_OWNER
//   GITHUB_REPO
//   GITHUB_BRANCH
//   DEBUG_TOKEN               (/debug-last 認証トークン)
//   IMPORT_TOKEN              (✅ /import 認証トークン)
//
// Vars (non-secret):
//   R2_PUBLIC_BASE            (例: https://media.harhisa-hosei.com)  // Custom Domain を貼る
//   IMAGE_GITHUB_MAX_BYTES    (例: 2500000)  // これ超えたらR2へ
//
// Optional Secrets (debug):
//   DEBUG_OPENAI    = "1"  // OpenAI raw_ok log
//   DEBUG_LOG_BODY  = "1"  // longer error body in KV
//
// Optional Secrets (behavior flags):
//   USE_OPENAI_JSON_SCHEMA = "1"
//   VISION_AUTOPOST_MIN_CONF = "0.85"
//   VISION_AUTOPOST_VOICE_MIN_CONF = "0.90"

const VERSION =
  "hosei-content-api-2026-02-11-r2image_split+news_prefix_fix+import_donothing+image_src_normalize+voice_date_posted_priority";

const CSV = {
  news: "https://docs.google.com/spreadsheets/d/e/2PACX-1vQSkBOovAHzdZWtA0Z-KRe27h5ZzGFi5Bq2G7Bp0Mv4sQ-2C9urIYy8oR9IaMf7xdSR9M_iww2zMbG-/pub?gid=0&single=true&output=csv",
  voice:
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vQSkBOovAHzdZWtA0Z-KRe27h5ZzGFi5Bq2G7Bp0Mv4sQ-2C9urIYy8oR9IaMf7xdSR9M_iww2zMbG-/pub?gid=793239367&single=true&output=csv",
  archive:
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vQSkBOovAHzdZWtA0Z-KRe27h5ZzGFi5Bq2G7Bp0Mv4sQ-2C9urIYy8oR9IaMf7xdSR9M_iww2zMbG-/pub?gid=260654898&single=true&output=csv",
};

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization,Range,X-Debug-Token,X-Import-Token",
  "Access-Control-Expose-Headers": "Content-Length,Content-Range,Accept-Ranges",
};

function withCors(headers = {}) {
  return { ...CORS, ...headers };
}
function json(data, status = 200, headers = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: withCors({ "Content-Type": "application/json; charset=utf-8", ...headers }),
  });
}
function textOut(s, status = 200, headers = {}) {
  return new Response(s, {
    status,
    headers: withCors({ "Content-Type": "text/plain; charset=utf-8", ...headers }),
  });
}

function nz(v) {
  return (v ?? "").toString();
}
function clampInt(v, def, min, max) {
  const n = parseInt(v ?? "", 10);
  if (Number.isNaN(n)) return def;
  return Math.min(Math.max(n, min), max);
}
function clampFloat(v, def, min, max) {
  const n = parseFloat(v ?? "");
  if (Number.isNaN(n)) return def;
  return Math.min(Math.max(n, min), max);
}
function errorText(e) {
  try {
    return (e && (e.stack || e.message)) || String(e);
  } catch {
    return "unknown error";
  }
}

// ---- TRUE/FALSE 正規化（import用途：空欄は TRUE とみなす）----
function normalizeBoolTextDefaultTrue(v) {
  const s = nz(v).trim().toLowerCase();
  if (!s) return "TRUE";
  if (s === "true" || s === "1" || s === "yes") return "TRUE";
  if (s === "false" || s === "0" || s === "no") return "FALSE";
  return s.toUpperCase() === "FALSE" ? "FALSE" : "TRUE";
}

// -----------------------------
// ✅ Debug KV (scoped last pointer)
// -----------------------------
const TTL_DEBUG = 24 * 60 * 60; // 24h
// TEXT先行ルート用
const TTL_PENDING_TEXT = 30 * 60; // 30分
const DEBUG_SCOPES = new Set(["general", "openai", "gemini", "line", "db"]);

function normalizeDebugScope(raw) {
  const s = nz(raw).trim().toLowerCase();
  if (!s) return "general";
  return DEBUG_SCOPES.has(s) ? s : "general";
}
function keyDebug(scope = "general") {
  return `debug:${scope}:${Date.now()}:${crypto.randomUUID()}`;
}
function keyDebugLast(scope = "general") {
  return `debug:last:${scope}`;
}
async function kvLogDebug(env, payload, ttl = TTL_DEBUG, scopeRaw = "general") {
  const scope = normalizeDebugScope(scopeRaw);
  try {
    const k = keyDebug(scope);
    await env.KV.put(k, JSON.stringify(payload), { expirationTtl: ttl });
    await env.KV.put(keyDebugLast(scope), k, { expirationTtl: ttl });
    if (scope === "general") await env.KV.put("debug:last", k, { expirationTtl: ttl });
    return k;
  } catch {
    return null;
  }
}

function short(s, n = 400) {
  s = nz(s);
  return s.length > n ? s.slice(0, n) + "..." : s;
}
function pickResponsesUsage(data) {
  const u = data?.usage || data?.response?.usage || null;
  if (!u) return null;
  return {
    input_tokens: u.input_tokens ?? u.prompt_tokens ?? null,
    output_tokens: u.output_tokens ?? u.completion_tokens ?? null,
    total_tokens: u.total_tokens ?? null,
  };
}
function shouldDebugOpenAI(env) {
  return (env.DEBUG_OPENAI || "") === "1";
}
function shouldDebugBody(env) {
  return (env.DEBUG_LOG_BODY || "") === "1";
}
function getOpenAITextModel(env) {
  return env.OPENAI_MODEL || "gpt-5-mini-2025-08-07";
}
function getOpenAIVisionModel(env) {
  return env.OPENAI_VISION_MODEL || env.OPENAI_MODEL || "gpt-4.1-mini";
}

// -----------------------------
// LINE signature verify (HMAC-SHA256, base64)
// -----------------------------
async function verifyLineSignature(env, rawBody, signatureB64) {
  if (!env.LINE_CHANNEL_SECRET) return false;
  if (!signatureB64) return false;
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(env.LINE_CHANNEL_SECRET),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const mac = await crypto.subtle.sign("HMAC", key, rawBody);
  const got = arrayBufferToBase64(mac);
  return timingSafeEqual(got, signatureB64);
}
function timingSafeEqual(a, b) {
  if (a.length !== b.length) return false;
  let out = 0;
  for (let i = 0; i < a.length; i++) out |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return out === 0;
}
function arrayBufferToBase64(buf) {
  const bytes = new Uint8Array(buf);
  let binary = "";
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
  }
  return btoa(binary);
}

// -----------------------------
// Prefix -> type
// -----------------------------
function detectTypeAndContent(text) {
  const t = nz(text).trim();
  let type = "voice";
  let explicit = false;

  if (/^(ニュース|ニュース：|N：|N:|に：|に:)/i.test(t)) {
    type = "news";
    explicit = true;
  } else if (/^(アーカイブ|アーカイブ：|A：|A:|あ：|あ:)/i.test(t)) {
    type = "archive";
    explicit = true;
  } else if (/^(V：|V:|v：|v:|ボイス|voice|VOICE)[:：\s]/.test(t)) {
    type = "voice";
    explicit = true;
  }

  const content = t
    .replace(
      /^(ニュース|アーカイブ|ボイス|VOICE|voice|ニュース：|アーカイブ：|N：|A：|V：|N:|A:|V:|に：|あ：|に:|あ:|v：|v:)\s*[:：]?\s*/i,
      ""
    )
    .trim();

  return { type, content, explicit };
}


// ✅ manual type-only command (for pending image)
function parseTypeOnlyCommand(text) {
  const s = nz(text).trim();
  const m = s.match(/^(?:T|TYPE|種別)\s*[:：]\s*(news|voice|archive)\s*$/i);
  if (m) return m[1].toLowerCase();
  const m2 = s.match(/^(?:T|TYPE|種別)\s*[:：]\s*(ニュース|ボイス|アーカイブ)\s*$/i);
  if (m2) {
    const t = m2[1];
    if (t.includes("ニュー")) return "news";
    if (t.includes("アーカ")) return "archive";
    return "voice";
  }
  if (/^(OK|ok|投稿|確定)$/i.test(s)) return "ok";
  return null;
}

// Workers is UTC; compute JST by adding 9h
function todayJstDatePadded() {
  const now = new Date(Date.now() + 9 * 60 * 60 * 1000);
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  return `${y}.${m}.${d}`;
}
function extractDatePadded(content) {
  const c = nz(content);
  const m = c.match(/(?:(\d{4})[.\/年])?(\d{1,2})[.\/月](\d{1,2})日?/);
  if (!m) return null;
  const y = m[1]
    ? String(m[1])
    : String(new Date(Date.now() + 9 * 60 * 60 * 1000).getUTCFullYear());
  const mo = String(parseInt(m[2], 10)).padStart(2, "0");
  const da = String(parseInt(m[3], 10)).padStart(2, "0");
  return `${y}.${mo}.${da}`;
}
// ✅ view_date: "YYYY.MM.DD" -> "YYYY.M.D"
function viewDateFromPadded(padded) {
  const m = nz(padded).match(/^(\d{4})\.(\d{2})\.(\d{2})$/);
  if (!m) return padded || null;
  const y = m[1];
  const mo = String(parseInt(m[2], 10));
  const da = String(parseInt(m[3], 10));
  return `${y}.${mo}.${da}`;
}
function extractUrl(content) {
  const m = nz(content).match(/https?:\/\/[\w!?\/+\-_~=;.,*&@#$%()'[\]]+/);
  return m ? m[0] : "";
}

