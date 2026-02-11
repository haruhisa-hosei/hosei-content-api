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
  "hosei-content-api-2026-02-09-r2image_split+news_prefix_fix+import_donothing+image_src_normalize";

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

// =============================
// Part 2/6
// =============================

// -----------------------------
// ✅ HTML helpers
// -----------------------------
function escapeHtml(s) {
  return nz(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function normalizeVoiceTextToHtml(s) {
  const t = nz(s)
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .trim();

  if (/^<span>[\s\S]*<\/span>$/.test(t)) return t;

  const parts = t
    .split(/<br\s*\/?>|\n/i)
    .map((x) => x.trim())
    .filter((x) => x.length > 0);

  const safe = parts.map((x) => escapeHtml(x)).join("<br>");
  return `<span>${safe}</span>`;
}

function wrapIfVoiceSpan(type, htmlOrText) {
  const t = nz(htmlOrText).trim();
  if (type === "voice") return normalizeVoiceTextToHtml(t);
  return t;
}

function nl2br(s) {
  return nz(s)
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .split("\n")
    .map((x) => x.trim())
    .filter(Boolean)
    .join("<br>");
}

// -----------------------------
// ✅ NEWS: 公演名（先頭1行）に「足すだけ」
//  - 詳細（2行目以降）は一切触らない
// -----------------------------
function addNewsFixedSuffixToFirstLine(html, suffix = "に出演します。") {
  const raw = nz(html).trim();
  if (!raw) return raw;

  // <br> 区切りを優先
  const parts = raw.split(/<br\s*\/?>/i);

  const first = (parts[0] || "").trim();
  const rest = parts.slice(1);

  // 既に出演文っぽいものが入ってたら二重付与しない
  const already = /出演(し|い)ます|出演予定|出演致|出演いた|出演します/.test(first);
  const first2 = already ? first : `${first}${suffix}`;

  return [first2, ...rest].filter((x) => nz(x).trim() !== "").join("<br>");
}

// -----------------------------
// ✅ URL helpers
// -----------------------------
function isUrl(s) {
  if (!s) return false;
  try {
    new URL(s);
    return true;
  } catch {
    return false;
  }
}
function joinUrl(base, key) {
  return `${String(base || "").replace(/\/+$/, "")}/${String(key || "").replace(/^\/+/, "")}`;
}

// image_src を URL / filename / R2key どれでも許容し、
// APIレスポンス上は「表示に使える形（URL or /media/..）」へ寄せる
function normalizeImageSrcForOutput(env, image_src) {
  const s = nz(image_src).trim();
  if (!s) return null;
  if (isUrl(s)) return s;

  // R2 key らしい（media/〜）→ public domain があれば URL化、なければ /media/ へ
  if (s.startsWith("media/")) {
    const base = nz(env.R2_PUBLIC_BASE).trim();
    if (base) return joinUrl(base, s);
    return `/media/${encodeURIComponent(s)}`;
  }

  // GitHub filename 等はそのまま（フロント側が既存の組み立てで表示）
  return s;
}

// -----------------------------
// OpenAI generation (Responses API)
// -----------------------------
function pickOutputTextFromResponses(data) {
  const ot = nz(data?.output_text).trim();
  if (ot) return ot;

  const out = data?.output;
  if (Array.isArray(out)) {
    for (const item of out) {
      const contents = item?.content;
      if (Array.isArray(contents)) {
        for (const c of contents) {
          const t = nz(c?.text).trim();
          if (t) return t;
        }
      }
      const t2 = nz(item?.text).trim();
      if (t2) return t2;
    }
  }

  const maybe = nz(data?.text).trim();
  if (maybe) return maybe;

  return "";
}

function safeJsonParse(s) {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

async function openaiResponsesText(env, { system, user, maxTokens = 350 }) {
  const model = getOpenAITextModel(env);
  if (!env.OPENAI_API_KEY) throw new Error("Missing OPENAI_API_KEY");

  const body = {
    model,
    input: [
      { role: "system", content: system },
      { role: "user", content: user },
    ],
    max_output_tokens: maxTokens,
  };

  const t0 = Date.now();
  const res = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${env.OPENAI_API_KEY}` },
    body: JSON.stringify(body),
  });

  const xrid = res.headers.get("x-request-id") || res.headers.get("x-request_id") || "";
  const durMs = Date.now() - t0;

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    await kvLogDebug(
      env,
      {
        where: "openaiResponsesText:http_error",
        status: res.status,
        x_request_id: xrid,
        model,
        durMs,
        body: shouldDebugBody(env) ? t.slice(0, 2000) : t.slice(0, 800),
        ts: Date.now(),
      },
      TTL_DEBUG,
      "openai"
    );
    throw new Error(`OpenAI error: ${res.status} ${t.slice(0, 300)}`);
  }

  const data = await res.json();
  const outText = pickOutputTextFromResponses(data);
  const usage = pickResponsesUsage(data);
  const respId = nz(data?.id);

  await kvLogDebug(
    env,
    {
      where: "openaiResponsesText:ok_light",
      model,
      response_id: respId || null,
      x_request_id: xrid || null,
      usage,
      durMs,
      outTextPreview: short(outText, 300),
      ts: Date.now(),
    },
    TTL_DEBUG,
    "openai"
  );

  if (shouldDebugOpenAI(env)) {
    await kvLogDebug(
      env,
      {
        where: "openaiResponsesText:raw_ok",
        model,
        response_id: respId || null,
        x_request_id: xrid || null,
        usage,
        durMs,
        outTextPreview: short(outText, 800),
        ts: Date.now(),
      },
      TTL_DEBUG,
      "openai"
    );
  }

  const text = nz(outText).trim();
  if (!text) throw new Error("OpenAI returned empty text");
  return text;
}

function looksJapanese(s) {
  const t = nz(s);
  if (!t) return true;
  return /[ぁ-んァ-ヶ一-龠]/.test(t);
}

async function openaiResponsesJsonSchema(env, { system, user, schemaName = "hosei_copy_schema", maxTokens = 400 }) {
  const model = getOpenAITextModel(env);
  if (!env.OPENAI_API_KEY) throw new Error("Missing OPENAI_API_KEY");

  const jsonSchema = {
    name: schemaName,
    strict: true,
    schema: {
      type: "object",
      additionalProperties: false,
      properties: {
        ja: { type: "string" },
        en: { type: "string" },
        btnJa: { type: "string" },
        btnEn: { type: "string" },
      },
      required: ["ja", "en", "btnJa", "btnEn"],
    },
  };

  const body = {
    model,
    input: [
      { role: "system", content: system },
      { role: "user", content: user },
    ],
    text: {
      format: {
        type: "json_schema",
        name: jsonSchema.name,
        strict: jsonSchema.strict,
        schema: jsonSchema.schema,
      },
    },
    max_output_tokens: maxTokens,
  };

  const t0 = Date.now();
  const res = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${env.OPENAI_API_KEY}` },
    body: JSON.stringify(body),
  });

  const xrid = res.headers.get("x-request-id") || res.headers.get("x-request_id") || "";
  const durMs = Date.now() - t0;

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    await kvLogDebug(
      env,
      {
        where: "openaiResponsesJsonSchema:http_error",
        status: res.status,
        x_request_id: xrid,
        model,
        schemaName,
        durMs,
        body: shouldDebugBody(env) ? t.slice(0, 2000) : t.slice(0, 800),
        ts: Date.now(),
      },
      TTL_DEBUG,
      "openai"
    );
    throw new Error(`OpenAI error: ${res.status} ${t.slice(0, 300)}`);
  }

  const data = await res.json();
  const outText = pickOutputTextFromResponses(data);
  const usage = pickResponsesUsage(data);
  const respId = nz(data?.id);

  await kvLogDebug(
    env,
    {
      where: "openaiResponsesJsonSchema:ok_light",
      model,
      schemaName,
      response_id: respId || null,
      x_request_id: xrid || null,
      usage,
      durMs,
      outTextPreview: short(outText, 260),
      ts: Date.now(),
    },
    TTL_DEBUG,
    "openai"
  );

  if (shouldDebugOpenAI(env)) {
    await kvLogDebug(
      env,
      {
        where: "openaiResponsesJsonSchema:raw_ok",
        model,
        schemaName,
        response_id: respId || null,
        x_request_id: xrid || null,
        usage,
        durMs,
        outTextPreview: short(outText, 800),
        ts: Date.now(),
      },
      TTL_DEBUG,
      "openai"
    );
  }

  if (!outText) throw new Error("OpenAI response has no output text");

  const obj = safeJsonParse(outText);
  if (!obj) throw new Error("OpenAI JSON parse failed");

  const ja = nz(obj.ja).trim();
  const en = nz(obj.en).trim();
  const btnJa = nz(obj.btnJa).trim();
  const btnEn = nz(obj.btnEn).trim();

  if (!ja || !btnJa || !btnEn) throw new Error("OpenAI JSON missing required fields");
  return { ja, en, btnJa, btnEn };
}

// =============================
// Part 3/6
// =============================

// -----------------------------
// OpenAI Vision -> classify + extract + generate (JSON Schema)
// -----------------------------
function toDataUrl(contentType, bytesArrayBuffer) {
  const b64 = arrayBufferToBase64(bytesArrayBuffer);
  const ct = contentType || "image/jpeg";
  return `data:${ct};base64,${b64}`;
}

function normalizePaddedDateOrToday(padded) {
  const m = nz(padded).match(/^(\d{4})\.(\d{2})\.(\d{2})$/);
  if (m) return padded;
  return todayJstDatePadded();
}

async function openaiResponsesVisionJson(env, { promptText, imageDataUrl, schemaName = "hosei_vision_post", maxTokens = 650 }) {
  const model = getOpenAIVisionModel(env);
  if (!env.OPENAI_API_KEY) throw new Error("Missing OPENAI_API_KEY");

  const jsonSchema = {
    name: schemaName,
    strict: true,
    schema: {
      type: "object",
      additionalProperties: false,
      properties: {
        type: { type: "string", enum: ["news", "voice", "archive"] },
        date: { type: "string", description: "YYYY.MM.DD padded" },
        ja_html: { type: "string" },
        en_html: { type: "string" },
        confidence: { type: "number" },
        has_event_info: { type: "boolean" },
      },
      required: ["type", "date", "ja_html", "en_html", "confidence", "has_event_info"],
    },
  };

  const body = {
    model,
    input: [
      {
        role: "user",
        content: [
          { type: "input_text", text: promptText },
          { type: "input_image", image_url: imageDataUrl },
        ],
      },
    ],
    text: {
      format: {
        type: "json_schema",
        name: jsonSchema.name,
        strict: jsonSchema.strict,
        schema: jsonSchema.schema,
      },
    },
    max_output_tokens: maxTokens,
  };

  const t0 = Date.now();
  const res = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${env.OPENAI_API_KEY}` },
    body: JSON.stringify(body),
  });

  const xrid = res.headers.get("x-request-id") || res.headers.get("x-request_id") || "";
  const durMs = Date.now() - t0;

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    await kvLogDebug(
      env,
      {
        where: "openaiResponsesVisionJson:http_error",
        status: res.status,
        x_request_id: xrid,
        model,
        durMs,
        body: shouldDebugBody(env) ? t.slice(0, 2000) : t.slice(0, 800),
        ts: Date.now(),
      },
      TTL_DEBUG,
      "openai"
    );
    throw new Error(`OpenAI vision error: ${res.status} ${t.slice(0, 300)}`);
  }

  const data = await res.json();
  const outText = pickOutputTextFromResponses(data);
  const usage = pickResponsesUsage(data);
  const respId = nz(data?.id);

  await kvLogDebug(
    env,
    {
      where: "openaiResponsesVisionJson:ok_light",
      model,
      response_id: respId || null,
      x_request_id: xrid || null,
      usage,
      durMs,
      outTextPreview: short(outText, 260),
      ts: Date.now(),
    },
    TTL_DEBUG,
    "openai"
  );

  if (shouldDebugOpenAI(env)) {
    await kvLogDebug(
      env,
      {
        where: "openaiResponsesVisionJson:raw_ok",
        model,
        response_id: respId || null,
        x_request_id: xrid || null,
        usage,
        durMs,
        outTextPreview: short(outText, 800),
        ts: Date.now(),
      },
      TTL_DEBUG,
      "openai"
    );
  }

  const obj = safeJsonParse(outText);
  if (!obj) throw new Error("OpenAI vision JSON parse failed");
  return obj;
}

async function generateFromImage(env, { imageDataUrl }) {
  const prompt = `
You are generating content for the website of a Japanese traditional performing arts artist.

The site definitions are strict:
- news  = upcoming performance information (future events / announcements)
- archive = past performance record (past events)
- voice = a personal murmur / the world as seen by the artist (landscapes, snapshots, backstage vibes)

Classification rules:
1) Only choose news/archive if the image clearly contains event-related text (flyer/poster/program) such as date/time/venue/price/program or event title. Otherwise choose voice.
2) If event text exists, determine whether it is future (news) or past (archive) based on the date compared to TODAY in JST.
3) If the date is not visible, use today's JST date, BUT do not force news/archive unless event text is clearly present.

Output fields:
- type: news|voice|archive
- has_event_info: true only if there is clear flyer/poster/program info
- date: YYYY.MM.DD padded
- ja_html: Japanese website-ready copy. Use <br> if appropriate. No URLs.
- en_html: natural English translation of ja_html. Use <br>. No URLs.
- confidence: 0.0-1.0 overall confidence

Do NOT invent names/numbers not visible. Return STRICT JSON only.
`.trim();

  const out = await openaiResponsesVisionJson(env, { promptText: prompt, imageDataUrl });

  const type = (out.type || "voice").toLowerCase();
  const date = normalizePaddedDateOrToday(out.date);
  const ja_html = nl2br(out.ja_html);
  const en_html = nl2br(out.en_html) || ja_html;

  const confidence = Number(out.confidence ?? 0);
  const hasEvent = !!out.has_event_info;

  return { type, date, ja_html, en_html, confidence, hasEvent };
}

// -----------------------------
// Gemini fallback (translate only)
// -----------------------------
function pickGeminiText(json) {
  const t =
    json?.candidates?.[0]?.content?.parts?.map((p) => nz(p?.text)).join("") ||
    json?.candidates?.[0]?.content?.parts?.[0]?.text ||
    "";
  return nz(t).trim();
}

async function geminiGenerateText(env, prompt) {
  if (!env.GEMINI_API_KEY) throw new Error("Missing GEMINI_API_KEY");

  const model = env.GEMINI_MODEL || "gemini-1.5-flash";
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent`;
  const body = { contents: [{ parts: [{ text: prompt }] }] };

  const t0 = Date.now();
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-goog-api-key": env.GEMINI_API_KEY },
    body: JSON.stringify(body),
  });
  const durMs = Date.now() - t0;

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    await kvLogDebug(
      env,
      { where: "geminiGenerateText:http_error", status: res.status, body: t.slice(0, 800), model, durMs, ts: Date.now() },
      TTL_DEBUG,
      "gemini"
    );
    throw new Error(`Gemini error: ${res.status} ${t.slice(0, 300)}`);
  }

  const data = await res.json();
  const text = pickGeminiText(data);
  if (!text) throw new Error("Gemini returned empty text");

  await kvLogDebug(
    env,
    { where: "geminiGenerateText:ok_light", model, durMs, outTextPreview: short(text, 260), ts: Date.now() },
    TTL_DEBUG,
    "gemini"
  );

  return text;
}

async function geminiTranslateEn(env, jaText) {
  const prompt = `Translate the following Japanese into natural English for a website (concise, no extra commentary). Output ONLY the English text.\n\nJapanese:\n${jaText}`;
  const en = await geminiGenerateText(env, prompt);
  return en.trim();
}

// -----------------------------
// Unified generation (text message path)
// -----------------------------
async function generateJaEn(env, content, forNews) {
  const raw = nz(content).trim();

  let ja = raw;
  let en = "";
  let btnJa = "詳細を見る";
  let btnEn = "View Details";

  const forceJsonSchema = (env.USE_OPENAI_JSON_SCHEMA || "") === "1";

  if (!forceJsonSchema) {
    try {
      const sysJa = `You are a Japanese copy editor for a Japanese artist website.
Rules:
- Return ONLY Japanese text (no quotes, no markdown, no commentary)
- Make it concise and website-ready
- If the input contains a URL, do NOT include the URL
- Do a minor improvement (punctuation/wording) unless already perfect
- Avoid line breaks unless necessary`;

      const userJa = forNews
        ? `Input:\n${raw}\n\nTask: Rewrite as a short neutral news line (Japanese only).`
        : `Input:\n${raw}\n\nTask: Rewrite as a concise voice post (Japanese only).`;

      const jaOut = await openaiResponsesText(env, { system: sysJa, user: userJa, maxTokens: 220 });
      if (jaOut) ja = jaOut.trim();
    } catch (e) {
      await kvLogDebug(env, { where: "generateJaEn:step1_ja_failed", err: errorText(e), rawPreview: short(raw, 240), ts: Date.now() }, TTL_DEBUG, "openai");
    }

    try {
      const sysEn = `You are a professional translator.
Rules:
- Output ONLY natural English text (no quotes, no markdown, no commentary)
- Keep it concise and website-ready
- Do NOT include any URL
- Preserve meaning; do not add new info`;

      const userEn = `Japanese:\n${ja}\n\nTask: Translate into natural English. Output ONLY English.`;

      const enOut = await openaiResponsesText(env, { system: sysEn, user: userEn, maxTokens: 260 });
      en = nz(enOut).trim();
    } catch (e) {
      await kvLogDebug(env, { where: "generateJaEn:step2_en_failed", err: errorText(e), jaPreview: short(ja, 240), ts: Date.now() }, TTL_DEBUG, "openai");
    }

    if (!en || en.length < 4 || looksJapanese(en)) {
      try {
        const en2 = await geminiTranslateEn(env, ja);
        if (en2) en = en2.trim();
        await kvLogDebug(
          env,
          { where: "generateJaEn:gemini_en_fallback", reason: "empty/short/japanese", jaPreview: short(ja, 160), enPreview: short(en, 160), ts: Date.now() },
          TTL_DEBUG,
          "gemini"
        );
      } catch (e) {
        await kvLogDebug(env, { where: "generateJaEn:gemini_en_fallback_failed", err: errorText(e), ts: Date.now() }, TTL_DEBUG, "gemini");
      }
    }

    if (!en) en = ja;
    return { ja, en, btnJa, btnEn };
  }

  const sys = `You are a bilingual copy editor for a Japanese artist website.
You MUST output STRICT JSON that conforms to the provided JSON Schema (no markdown, no extra text).

Rules:
- "ja": natural Japanese, concise (website-ready)
- "en": natural English translation of "ja" (must not be empty)
- "btnJa"/"btnEn": button labels (use sensible defaults if no URL context)
- Prefer single-line text (no line breaks unless necessary for <br>)
- Always perform at least one minor edit to improve readability (punctuation/wording), unless the input is already perfect.
- If the input contains a URL, do NOT include the URL in "ja"/"en".`;

  const prompt2 = forNews
    ? `Input:\n${raw}\n\n(News item: keep it short, neutral, informative.)`
    : `Input:\n${raw}`;

  try {
    const obj = await openaiResponsesJsonSchema(env, { system: sys, user: prompt2, schemaName: forNews ? "hosei_news_copy" : "hosei_copy", maxTokens: 450 });

    const ja2 = nz(obj.ja).trim() || ja;
    let en2 = nz(obj.en).trim();
    const btnJa2 = nz(obj.btnJa).trim() || btnJa;
    const btnEn2 = nz(obj.btnEn).trim() || btnEn;

    if (!en2 || en2.length < 4 || looksJapanese(en2)) {
      try {
        const en3 = await geminiTranslateEn(env, ja2);
        if (en3) en2 = en3.trim();
      } catch {}
    }
    if (!en2) en2 = ja2;

    return { ja: ja2, en: en2, btnJa: btnJa2, btnEn: btnEn2 };
  } catch (e) {
    await kvLogDebug(env, { where: "generateJaEn:openai_jsonschema_failed", err: errorText(e), contentPreview: short(raw, 200), ts: Date.now() }, TTL_DEBUG, "openai");
  }

  try {
    const en2 = await geminiTranslateEn(env, ja);
    en = (en2 || "").trim();
  } catch {}

  if (!en) en = ja;
  return { ja, en, btnJa, btnEn };
}

// =============================
// Part 4/6
// =============================

// -----------------------------
// LINE reply/push
// -----------------------------
async function linePush(env, to, text) {
  const url = "https://api.line.me/v2/bot/message/push";
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${env.LINE_CHANNEL_ACCESS_TOKEN}` },
    body: JSON.stringify({ to, messages: [{ type: "text", text }] }),
  });
  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`LINE push failed: ${res.status} ${t.slice(0, 200)}`);
  }
}

async function lineReply(env, replyToken, text, fallbackToUserId) {
  const url = "https://api.line.me/v2/bot/message/reply";
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${env.LINE_CHANNEL_ACCESS_TOKEN}` },
    body: JSON.stringify({ replyToken, messages: [{ type: "text", text }] }),
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    await kvLogDebug(env, { where: "lineReply:failed", status: res.status, body: t.slice(0, 400), textPreview: (text || "").slice(0, 160), ts: Date.now() }, TTL_DEBUG, "line");

    if (fallbackToUserId) {
      try {
        await linePush(env, fallbackToUserId, text);
        await kvLogDebug(env, { where: "lineReply:failed_but_pushed", status: res.status, ts: Date.now() }, TTL_DEBUG, "line");
        return;
      } catch (e) {
        await kvLogDebug(env, { where: "lineReply:push_fallback_failed", err: errorText(e), ts: Date.now() }, TTL_DEBUG, "line");
      }
    }

    throw new Error(`LINE reply failed: ${res.status} ${t.slice(0, 200)}`);
  }
}

// -----------------------------
// SHA-1 helper (legacy_key hashing)
// -----------------------------
async function sha1hex(s) {
  if (!s) return "";
  const buf = await crypto.subtle.digest("SHA-1", new TextEncoder().encode(s));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

// -----------------------------
// legacy_key
// -----------------------------
async function pickLegacyKey(type, date, contentOrUrl = "") {
  if (type === "archive" && date) return `archive:date:${date}`;

  if (type === "news") {
    const h = await sha1hex(contentOrUrl);
    return `news:${date}:${h.slice(0, 10)}`;
  }
  if (type === "voice") {
    const h = await sha1hex(contentOrUrl);
    return `voice:${date}:${h.slice(0, 10)}`;
  }

  return `${type}:${date}:${crypto.randomUUID()}`;
}

// -----------------------------
// DB insert (UPSERT)
// -----------------------------
async function insertPost(env, row) {
  const q = `
    INSERT INTO posts
      (type,date,ja_html,en_html,ja_link_text,ja_link_href,en_link_text,en_link_href,
       image_src,image_kind,enabled,view_date,
       media_type,media_src,poster_src,legacy_key,created_at)
    VALUES
      (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?, datetime('now'))
    ON CONFLICT(legacy_key) DO UPDATE SET
      type         = excluded.type,
      date         = excluded.date,
      ja_html      = excluded.ja_html,
      en_html      = excluded.en_html,
      ja_link_text = excluded.ja_link_text,
      ja_link_href = excluded.ja_link_href,
      en_link_text = excluded.en_link_text,
      en_link_href = excluded.en_link_href,
      image_src    = excluded.image_src,
      image_kind   = excluded.image_kind,
      enabled      = excluded.enabled,
      view_date    = excluded.view_date,
      media_type   = excluded.media_type,
      media_src    = excluded.media_src,
      poster_src   = excluded.poster_src
  `;
  await env.DB.prepare(q)
    .bind(
      row.type,
      row.date,
      row.ja_html,
      row.en_html,
      row.ja_link_text,
      row.ja_link_href,
      row.en_link_text,
      row.en_link_href,
      row.image_src,
      row.image_kind,
      row.enabled,
      row.view_date,
      row.media_type,
      row.media_src,
      row.poster_src,
      row.legacy_key
    )
    .run();

  const got = await env.DB.prepare(`SELECT id FROM posts WHERE legacy_key=? LIMIT 1`).bind(row.legacy_key).first();
  const id = got?.id ?? null;

  await kvLogDebug(env, { where: "insertPost:ok", id, type: row.type, date: row.date, ts: Date.now() }, TTL_DEBUG, "db");
  return id;
}

async function softDeleteMany(env, ids) {
  let ok = 0;
  for (const id of ids) {
    const out = await env.DB.prepare(`UPDATE posts SET enabled='FALSE' WHERE id=?`).bind(id).run();
    if ((out?.meta?.changes ?? 0) > 0) ok++;
  }
  return ok;
}

// -----------------------------
// get/update helpers for editing
// -----------------------------
async function getPostById(env, id) {
  const row = await env.DB.prepare(`
    SELECT
      id, type, date, view_date,
      ja_html, en_html,
      ja_link_text, ja_link_href,
      en_link_text, en_link_href,
      image_src, image_kind,
      media_type, media_src, poster_src,
      enabled
    FROM posts
    WHERE id=?
    LIMIT 1
  `).bind(id).first();
  return row || null;
}

async function updatePostFields(env, id, fieldsObj) {
  const keys = Object.keys(fieldsObj || {});
  if (!keys.length) return false;

  const setSql = keys.map((k) => `${k}=?`).join(", ");
  const values = keys.map((k) => fieldsObj[k]);

  const q = `UPDATE posts SET ${setSql} WHERE id=?`;
  const out = await env.DB.prepare(q).bind(...values, id).run();
  const changes = out?.meta?.changes ?? 0;
  return changes > 0;
}

// -----------------------------
// Pending KV
// -----------------------------
const TTL_PENDING = 20 * 60;
function keyPendingImage(userId) {
  return `pending_image:${userId}`;
}
function keyPendingVideo(userId) {
  return `pending_video:${userId}`;
}

// -----------------------------
// ✅ Next-type KV (set destination before sending image)
//  - User can send: NEXT:voice / NEXT:news / NEXT:archive (also 日本語)
//  - Applied to the next incoming image, then auto-cleared
// -----------------------------
const TTL_NEXTTYPE = 30 * 60; // 30min
function keyNextType(userId) {
  return `next_type:${userId}`;
}
function normalizeTypeWord(raw) {
  const t = nz(raw).trim().toLowerCase();
  if (t === "news" || t === "voice" || t === "archive") return t;
  if (t.includes("ニュー")) return "news";
  if (t.includes("アーカ")) return "archive";
  if (t.includes("ボイ") || t.includes("voice")) return "voice";
  return null;
}
function parseNextTypeCommand(text) {
  const s = nz(text).trim();
  const m = s.match(/^NEXT\s*[:：]\s*(.+)$/i);
  if (!m) return null;
  return normalizeTypeWord(m[1]);
}
async function setNextType(env, userId, type) {
  if (!type) return;
  await env.KV.put(keyNextType(userId), type, { expirationTtl: TTL_NEXTTYPE });
}
async function consumeNextType(env, userId) {
  const t = await env.KV.get(keyNextType(userId));
  if (t) await env.KV.delete(keyNextType(userId));
  return t ? normalizeTypeWord(t) : null;
}

async function kvGetJson(env, key) {
  const raw = await env.KV.get(key);
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}
async function kvPutJson(env, key, obj, ttl = TTL_PENDING) {
  await env.KV.put(key, JSON.stringify(obj), { expirationTtl: ttl });
}

// -----------------------------
// Editing KV
// -----------------------------
const TTL_EDITING = 30 * 60; // 30min
function keyEditing(userId) {
  return `editing:${userId}`;
}
async function setEditing(env, userId, obj) {
  await env.KV.put(keyEditing(userId), JSON.stringify(obj), { expirationTtl: TTL_EDITING });
}
async function getEditing(env, userId) {
  return await kvGetJson(env, keyEditing(userId));
}
async function clearEditing(env, userId) {
  await env.KV.delete(keyEditing(userId));
}

// -----------------------------
// Delete command
// -----------------------------
function parseDeleteIds(text) {
  const s = nz(text)
    .replace(/[「」『』"]/g, "")
    .trim()
    .replace(/\s+/g, " ");

  const m = s.match(/^(削除|消去|さ)\s*[:：]\s*(?:id\s*[:：]\s*)?(.+)$/i);
  if (!m) return null;

  const rest = (m[2] || "").trim();

  const r = rest.match(/^(\d+)\s*-\s*(\d+)$/);
  if (r) {
    const a = parseInt(r[1], 10);
    const b = parseInt(r[2], 10);
    if (Number.isFinite(a) && Number.isFinite(b)) {
      const lo = Math.min(a, b);
      const hi = Math.max(a, b);
      const ids = [];
      for (let i = lo; i <= hi; i++) ids.push(i);
      return ids;
    }
  }

  const parts = rest.split(/[,\s]+/).filter(Boolean);
  const ids = parts
    .map((x) => parseInt(x.replace(/[^0-9]/g, ""), 10))
    .filter((n) => Number.isFinite(n) && n > 0);

  return ids.length ? Array.from(new Set(ids)) : null;
}

// -----------------------------
// ✅ Edit commands (+ TYPE + DATE support)
// -----------------------------
function parseEditStart(text) {
  const s = nz(text).trim().replace(/\s+/g, "");
  const m = s.match(/^編集[:：](\d+)$/);
  return m ? m[1] : null;
}
function parseEditEnd(text) {
  const s = nz(text).trim();
  return /^(完了|終了|end)$/i.test(s);
}
function parseEditCancel(text) {
  const s = nz(text).trim();
  return /^(取消|キャンセル|中止|cancel)$/i.test(s);
}
function parseEditFieldUpdate(text) {
  const s = nz(text).trim();
  const m = s.match(/^(JA|EN|BTNJA|BTNEN|TYPE|DATE)\s*[:：]\s*([\s\S]+)$/i);
  if (!m) return null;
  return { field: m[1].toUpperCase(), value: (m[2] || "").trim() };
}

// -----------------------------
// LINE content fetch
// -----------------------------
async function fetchLineMessageContent(env, messageId) {
  const url = `https://api-data.line.me/v2/bot/message/${messageId}/content`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${env.LINE_CHANNEL_ACCESS_TOKEN}` } });
  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`LINE content fetch failed: ${res.status} ${t.slice(0, 200)}`);
  }
  const contentType = res.headers.get("content-type") || "application/octet-stream";
  const bytes = await res.arrayBuffer();
  return { bytes, contentType };
}

// -----------------------------
// GitHub upload (images/)
// -----------------------------
function extFromContentType(ct) {
  const s = (ct || "").toLowerCase();
  if (s.includes("png")) return "png";
  if (s.includes("webp")) return "webp";
  if (s.includes("gif")) return "gif";
  return "jpg";
}

async function uploadImageToGitHub(env, { bytes, contentType, messageId }) {
  const owner = env.GITHUB_OWNER || "haruhisa-hosei";
  const repo = env.GITHUB_REPO || "haruhisa-hosei-site";
  const branch = env.GITHUB_BRANCH || "main";
  if (!env.GITHUB_TOKEN) throw new Error("Missing GITHUB_TOKEN");

  const ext = extFromContentType(contentType);
  const date = todayJstDatePadded().replace(/\./g, "");
  const filename = `voice_${date}_${messageId}_${Math.floor(Math.random() * 1000)}.${ext}`;
  const path = `images/${filename}`;
  const b64 = arrayBufferToBase64(bytes);

  const url = `https://api.github.com/repos/${owner}/${repo}/contents/${path}`;
  const res = await fetch(url, {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${env.GITHUB_TOKEN}`,
      Accept: "application/vnd.github+json",
      "User-Agent": "hosei-worker",
    },
    body: JSON.stringify({ message: `Upload ${filename} from LINE`, content: b64, branch }),
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`GitHub upload failed: ${res.status} ${t.slice(0, 400)}`);
  }

  return filename; // GitHub filename
}

// -----------------------------
// R2 keys (image/video/poster)
// -----------------------------
function r2KeyForVideo(userId, messageId) {
  const ym = todayJstDatePadded().slice(0, 7).replace(".", "");
  return `media/video/${ym}/${userId}/${messageId}.mp4`;
}
function r2KeyForPoster(userId, messageId, ext = "jpg") {
  const ym = todayJstDatePadded().slice(0, 7).replace(".", "");
  return `media/poster/${ym}/${userId}/${messageId}.${ext}`;
}
function r2KeyForImage(userId, messageId, ext = "jpg") {
  const ym = todayJstDatePadded().slice(0, 7).replace(".", "");
  return `media/image/${ym}/${userId}/${messageId}.${ext}`;
}

// =============================
// Part 5/6
// =============================

// -----------------------------
// ✅ /posts handler (type whitelist)
// -----------------------------
const POST_TYPES = new Set(["news", "voice", "archive"]);

async function handlePosts(url, env) {
  const rawType = url.searchParams.get("type") || "news";
  const type = nz(rawType).trim().toLowerCase();

  if (!POST_TYPES.has(type)) {
    return json({ ok: false, error: "invalid type", allowed: Array.from(POST_TYPES), got: rawType }, 400);
  }

  const onlyEnabled = (url.searchParams.get("onlyEnabled") ?? "1") !== "0";
  const limit = clampInt(url.searchParams.get("limit"), 20, 1, 500);
  const offset = clampInt(url.searchParams.get("offset"), 0, 0, 1_000_000);

  const baseSql = `
    SELECT
      id, type, date, view_date,
      ja_html, en_html,
      ja_link_text, ja_link_href,
      en_link_text, en_link_href,
      image_src, image_kind,
      media_type, media_src, poster_src,
      enabled,
      legacy_key, created_at, updated_at
    FROM posts
    WHERE type=?
  `;

  const flagSql = onlyEnabled ? ` AND enabled='TRUE' ` : ``;

  const dateKeyExpr = `
    REPLACE(REPLACE(REPLACE(REPLACE(date,'.',''),'/',''),' ',''),':','')
  `;

  const orderSql =
    type === "voice"
      ? `
    ORDER BY created_at DESC, id DESC
    LIMIT ? OFFSET ?
  `
      : `
    ORDER BY
      LENGTH(${dateKeyExpr}) DESC,
      ${dateKeyExpr} DESC,
      id DESC
    LIMIT ? OFFSET ?
  `;

  const stmt = env.DB.prepare(baseSql + flagSql + orderSql).bind(type, limit, offset);
  const { results } = await stmt.all();

  // ✅ 出力時に image_src / media_src / poster_src を「使える形」へ正規化
  const out = (results || []).map((r) => ({
    ...r,
    image_src: normalizeImageSrcForOutput(env, r.image_src),
    media_src: normalizeImageSrcForOutput(env, r.media_src),
    poster_src: normalizeImageSrcForOutput(env, r.poster_src),
  }));

  return json(out);
}

// -----------------------------
// /media/<key> Range対応
// -----------------------------
function parseRangeHeader(rangeHeader) {
  if (!rangeHeader) return null;
  const m = rangeHeader.match(/^bytes=(\d*)-(\d*)$/);
  if (!m) return null;
  const a = m[1] ? parseInt(m[1], 10) : NaN;
  const b = m[2] ? parseInt(m[2], 10) : NaN;
  if (!Number.isNaN(a) && !Number.isNaN(b) && b >= a) return { offset: a, endInclusive: b };
  if (!Number.isNaN(a) && Number.isNaN(b)) return { offset: a, endInclusive: null };
  if (Number.isNaN(a) && !Number.isNaN(b)) return { suffix: b };
  return null;
}

async function handleMedia(url, req, env) {
  const key = decodeURIComponent(url.pathname.replace(/^\/media\//, ""));
  if (!key) return textOut("missing key", 400);

  const rangeHeader = req.headers.get("range");
  const range = parseRangeHeader(rangeHeader);

  let obj;
  try {
    if (range?.suffix != null) obj = await env.R2.get(key, { range: { suffix: range.suffix } });
    else if (range?.offset != null && range.endInclusive != null)
      obj = await env.R2.get(key, { range: { offset: range.offset, length: range.endInclusive - range.offset + 1 } });
    else obj = await env.R2.get(key);
  } catch (e) {
    await kvLogDebug(env, { where: "handleMedia:r2_error", err: errorText(e), ts: Date.now() }, TTL_DEBUG, "general");
    return textOut("R2 error", 500);
  }

  if (!obj) return textOut("not found", 404);

  const headers = new Headers(withCors());
  const ct = obj.httpMetadata?.contentType || "application/octet-stream";
  headers.set("Content-Type", ct);
  headers.set("Cache-Control", "public, max-age=31536000, immutable");
  headers.set("Accept-Ranges", "bytes");

  if (rangeHeader && obj.range && obj.size != null) {
    headers.set("Content-Range", `bytes ${obj.range.offset}-${obj.range.offset + obj.range.length - 1}/${obj.size}`);
    headers.set("Content-Length", String(obj.range.length));
    return new Response(obj.body, { status: 206, headers });
  }

  headers.set("Content-Length", String(obj.size ?? 0));
  return new Response(obj.body, { status: 200, headers });
}

// -----------------------------
// LINE processing
// -----------------------------
async function processLineWebhook(env, payload) {
  const events = payload?.events || [];
  for (const event of events) {
    try {
      const userId = event?.source?.userId;
      if (!userId || userId !== env.ADMIN_USER_ID) continue;

      const replyToken = event?.replyToken;
      const msg = event?.message;
      if (!msg?.type) continue;

      // -------------------------
      // image
      // -------------------------
      if (msg.type === "image") {
        const pendingVideo = await kvGetJson(env, keyPendingVideo(userId));
        if (pendingVideo?.stage === "await_poster" && pendingVideo?.video_message_id) {
          const { bytes, contentType } = await fetchLineMessageContent(env, msg.id);
          const ext = extFromContentType(contentType);
          const posterKey = r2KeyForPoster(userId, pendingVideo.video_message_id, ext);

          await env.R2.put(posterKey, bytes, { httpMetadata: { contentType } });

          pendingVideo.stage = "await_text";
          pendingVideo.poster_key = posterKey;
          await kvPutJson(env, keyPendingVideo(userId), pendingVideo);

          if (replyToken) {
            await lineReply(env, replyToken, "🖼 サムネを受け取りました。続けて本文（N:/に: / V: / A:/あ:）を送ってください。", userId);
          }
          continue;
        }

        const { bytes, contentType } = await fetchLineMessageContent(env, msg.id);
        const sizeBytes = bytes?.byteLength ?? 0;

        // ✅ 画像サイズで GitHub / R2 分岐
        const maxGitHub = clampInt(env.IMAGE_GITHUB_MAX_BYTES, 2_500_000, 100_000, 20_000_000);

        let stored; // { kind: 'github'|'r2', value: filename|key }
        try {
          if (sizeBytes > maxGitHub) {
            const ext = extFromContentType(contentType);
            const key = r2KeyForImage(userId, msg.id, ext);
            await env.R2.put(key, bytes, { httpMetadata: { contentType } });
            stored = { kind: "r2", value: key };
          } else {
            const fileName = await uploadImageToGitHub(env, { bytes, contentType, messageId: msg.id });
            stored = { kind: "github", value: fileName };
          }
        } catch (e) {
          await kvLogDebug(env, { where: "image:store_failed", err: errorText(e), sizeBytes, maxGitHub, ts: Date.now() }, TTL_DEBUG, "general");
          if (replyToken) await lineReply(env, replyToken, "⚠️ 画像保存に失敗しました。", userId);
          continue;
        }

                // ✅ NEXT:type が設定されていれば、この画像の行き先を先に確定（自動で1回消費）
        const forcedNextType = await consumeNextType(env, userId);

// pending には「そのまま」格納（URL化は posts出力時にやる）
        await kvPutJson(
          env,
          keyPendingImage(userId),
          { image_src: stored.value, stage: "await_confirm_or_text", forcedType: forcedNextType || null, gen: null },
          TTL_PENDING
        );

        // Visionは「小さい画像のみ」or 必要なら常に、のどちらでも良いが
        // ✅ 今回は「GitHub行き＝小さい」だけ自動読取（R2行きは原則スキップ）
        if (stored.kind === "r2") {
          if (replyToken) {
            await lineReply(
              env,
              replyToken,
              ((forcedNextType ? `📷 画像を保存しました（R2）。\n画像が大きいため自動読取はスキップしました。\n行き先は ${forcedNextType.toUpperCase()} に確定済みです。\n続けて本文（に:/N:/A:/あ:/V:）を送ってください。` : `📷 画像を保存しました（R2）。\n画像が大きいため自動読取はスキップしました。\n続けて本文（に:/N:/A:/あ:/V: または T:news 等）を送ってください。`)),
              userId
            );
          }
          continue;
        }

        // GitHub行き（小さい）→ Vision
        let gen;
        try {
          const imageDataUrl = toDataUrl(contentType, bytes);
          gen = await generateFromImage(env, { imageDataUrl });
        } catch (e) {
          await kvLogDebug(env, { where: "image:vision_failed", err: errorText(e), ts: Date.now() }, TTL_DEBUG, "general");
          if (replyToken) {
            await lineReply(env, replyToken, `📷 画像は保存しました。自動読取に失敗したため、本文（に:/N:/A:/あ:/V:）を送ってください。`, userId);
          }
          continue;
        }

        let type = ((forcedNextType || gen.type) || "voice").toLowerCase();
        // voice は「投稿日（実行日）」を最優先：画像から日付が取れても、本文に日付が無い限り today を使う
        const date = (type === "voice") ? todayJstDatePadded() : (gen.date || todayJstDatePadded());

        const minConf = clampFloat(env.VISION_AUTOPOST_MIN_CONF, 0.85, 0.0, 1.0);
        const minVoiceConf = clampFloat(env.VISION_AUTOPOST_VOICE_MIN_CONF, 0.9, 0.0, 1.0);
        const conf = Number(gen.confidence ?? 0);
        let canAutoPostNewsArchive = gen.hasEvent && (type === "news" || type === "archive") && conf >= minConf;
        let canAutoPostVoice = !gen.hasEvent && type === "voice" && conf >= minVoiceConf;

        // ✅ 行き先を手動確定している場合は、自動投稿は行わない（本文 or OK を待つ）
        if (forcedNextType) {
          canAutoPostNewsArchive = false;
          canAutoPostVoice = false;
        }

        // pending に gen を載せる
        await kvPutJson(
          env,
          keyPendingImage(userId),
          { image_src: stored.value, stage: "await_confirm_or_text", forcedType: forcedNextType || null, gen: { ...gen, type, date } },
          TTL_PENDING
        );

        if (canAutoPostNewsArchive || canAutoPostVoice) {
          const finalType = type;
          let ja_html = gen.ja_html;
          let en_html = gen.en_html;

          // ✅ NEWS: 公演名（先頭1行）に定型文を「足すだけ」
          if (finalType === "news") {
            ja_html = addNewsFixedSuffixToFirstLine(ja_html, "に出演します。");
            // en_html は Vision 由来の詳細を壊さないため触らない（そのまま）
          }

          const view_date = viewDateFromPadded(date);

          let image_kind = null;
          if (finalType === "voice") {
            ja_html = wrapIfVoiceSpan("voice", ja_html);
            en_html = wrapIfVoiceSpan("voice", en_html || ja_html);
            image_kind = "voice";
          }

          const legacy_key = await pickLegacyKey(finalType, date, `${stored.value}:${ja_html}`);

          const row = {
            type: finalType,
            date,
            ja_html,
            en_html,
            ja_link_text: "",
            ja_link_href: "",
            en_link_text: "",
            en_link_href: "",
            image_src: stored.value, // filename or R2 key
            image_kind,
            enabled: "TRUE",
            view_date,
            media_type: "image",
            media_src: null,
            poster_src: null,
            legacy_key,
          };

          const newId = await insertPost(env, row);
          await env.KV.delete(keyPendingImage(userId));

          if (replyToken) {
            await lineReply(
              env,
              replyToken,
              `✅ 画像から自動投稿しました (ID:${newId ?? "?"})\n` +
                `[${finalType.toUpperCase()}] date=${date} (conf=${String(conf)})\n` +
                `必要なら「編集:${newId}」で修正できます。`,
              userId
            );
          }
          continue;
        }

        if (replyToken) {
          await lineReply(
            env,
            replyToken,
            `📷 画像を保存しました。\n` +
              `推定: [${type.toUpperCase()}] date=${date} (conf=${String(conf)})\n` +
              `このままなら「OK」で投稿。\n` +
              `種別変更は「T:voice / T:news / T:archive」。\n` +
              `本文で上書きするなら（に:/N:/A:/あ:/V:）を送ってください。`,
            userId
          );
        }
        continue;
      }

      // -------------------------
      // video
      // -------------------------
      if (msg.type === "video") {
        const { bytes, contentType } = await fetchLineMessageContent(env, msg.id);
        const videoKey = r2KeyForVideo(userId, msg.id);

        await env.R2.put(videoKey, bytes, { httpMetadata: { contentType: contentType || "video/mp4" } });

        await kvPutJson(env, keyPendingVideo(userId), { stage: "await_poster", video_key: videoKey, video_message_id: msg.id });

        if (replyToken) {
          await lineReply(env, replyToken, "🎥 動画を受け取りました。続けてサムネ画像を送ってください。", userId);
        }
        continue;
      }

      // -------------------------
      // text
      // -------------------------
      if (msg.type === "text") {
        const text = nz(msg.text).trim();

        if (parseEditEnd(text)) {
          await clearEditing(env, userId);
          if (replyToken) await lineReply(env, replyToken, "✅ 編集モードを終了しました。", userId);
          continue;
        }
        if (parseEditCancel(text)) {
          await clearEditing(env, userId);
          if (replyToken) await lineReply(env, replyToken, "🟡 編集をキャンセルしました。", userId);
          continue;
        }

        const editId = parseEditStart(text);
        if (editId) {
          const row = await getPostById(env, editId);
          if (!row) {
            if (replyToken) await lineReply(env, replyToken, `⚠️ ID:${editId} が見つかりませんでした。`, userId);
            continue;
          }

          await setEditing(env, userId, { id: row.id, type: row.type });

          const msgOut =
            `✏️ 編集モード (ID:${row.id} / ${String(row.type).toUpperCase()})\n\n` +
            `DATE:\n${nz(row.date)}\n\n` +
            `JA:\n${nz(row.ja_html)}\n\n` +
            `EN:\n${nz(row.en_html)}\n\n` +
            `修正はこう送ってください：\n` +
            `DATE: YYYY.MM.DD / JA: ... / EN: ... / BTNJA: ... / BTNEN: ... / TYPE: news|voice|archive\n` +
            `終わるとき：完了　やめる：取消`;

          if (replyToken) await lineReply(env, replyToken, msgOut, userId);
          continue;
        }

        const editing = await getEditing(env, userId);
        const upd = parseEditFieldUpdate(text);
        if (editing && upd) {
          const row = await getPostById(env, editing.id);
          if (!row) {
            await clearEditing(env, userId);
            if (replyToken) await lineReply(env, replyToken, "⚠️ 対象が消えました。編集モードを解除しました。", userId);
            continue;
          }

          let ok = false;

          if (upd.field === "TYPE") {
            const t = nz(upd.value).trim().toLowerCase();
            const newType = t === "news" || t === "archive" || t === "voice" ? t : null;
            if (!newType) {
              if (replyToken) await lineReply(env, replyToken, `⚠️ TYPE は news|voice|archive のみです。`, userId);
              continue;
            }

            const newViewDate = viewDateFromPadded(nz(row.date).trim());
            let newJa = nz(row.ja_html);
            let newEn = nz(row.en_html);
            let newImageKind = nz(row.image_kind) || null;

            if (newType === "voice") {
              newJa = wrapIfVoiceSpan("voice", newJa);
              newEn = wrapIfVoiceSpan("voice", newEn || newJa);
              newImageKind = row.image_src ? "voice" : null;
            } else {
              newImageKind = null;
            }

            ok = await updatePostFields(env, row.id, {
              type: newType,
              view_date: newViewDate,
              ja_html: newJa,
              en_html: newEn,
              image_kind: newImageKind,
            });

            if (ok) await setEditing(env, userId, { id: row.id, type: newType });
          } else if (upd.field === "DATE") {
            const newDatePadded = extractDatePadded(upd.value) || null;
            if (!newDatePadded) {
              if (replyToken) await lineReply(env, replyToken, `⚠️ DATE は YYYY.MM.DD（または 2/8 形式）で送ってください。`, userId);
              continue;
            }
            const newViewDate = viewDateFromPadded(newDatePadded);
            ok = await updatePostFields(env, row.id, { date: newDatePadded, view_date: newViewDate });
          } else if (upd.field === "JA") {
            const newJa = row.type === "voice" ? wrapIfVoiceSpan("voice", upd.value) : upd.value;
            ok = await updatePostFields(env, row.id, { ja_html: newJa });
          } else if (upd.field === "EN") {
            const newEn = row.type === "voice" ? wrapIfVoiceSpan("voice", upd.value) : upd.value;
            ok = await updatePostFields(env, row.id, { en_html: newEn });
          } else if (upd.field === "BTNJA") {
            ok = await updatePostFields(env, row.id, { ja_link_text: upd.value });
          } else if (upd.field === "BTNEN") {
            ok = await updatePostFields(env, row.id, { en_link_text: upd.value });
          }

          if (replyToken) {
            await lineReply(env, replyToken, ok ? `✅ ${upd.field} を更新しました (ID:${row.id})` : `⚠️ 更新できませんでした (ID:${row.id})`, userId);
          }
          continue;
        }

        const delIds = parseDeleteIds(text);
        if (delIds) {
          const n = await softDeleteMany(env, delIds);
          if (replyToken) await lineReply(env, replyToken, `🗑️ 非表示にしました：${n}/${delIds.length} 件\n(${delIds.join(", ")})`, userId);
          continue;
        }

        const nextTypeCmd = parseNextTypeCommand(text);
        if (nextTypeCmd) {
          await setNextType(env, userId, nextTypeCmd);
          if (replyToken) await lineReply(env, replyToken, `✅ 次の画像の行き先を ${nextTypeCmd.toUpperCase()} に確定しました。続けて画像を送ってください。`, userId);
          continue;
        }

const cmd = parseTypeOnlyCommand(text);
        const pendingImg = await kvGetJson(env, keyPendingImage(userId));

        if (pendingImg && cmd) {
          if (cmd === "news" || cmd === "voice" || cmd === "archive") {
            pendingImg.forcedType = cmd;
            await kvPutJson(env, keyPendingImage(userId), pendingImg, TTL_PENDING);
            if (replyToken) await lineReply(env, replyToken, `✅ 種別を ${cmd.toUpperCase()} に設定しました。続けて「OK」で投稿、または本文で上書きしてください。`, userId);
            continue;
          }

          if (cmd === "ok") {
            const g = pendingImg.gen;
            if (!g) {
              if (replyToken) await lineReply(env, replyToken, `⚠️ 自動投稿用の下書きがありません。本文（に:/N:/A:/あ:/V:）を送ってください。`, userId);
              continue;
            }

            const finalType = (pendingImg.forcedType || g.type || "voice").toLowerCase();
            const date = g.date || todayJstDatePadded();

            let ja_html = g.ja_html;
            let en_html = g.en_html;

            // ✅ NEWS: 先頭1行だけ「足す」
            if (finalType === "news") {
              ja_html = addNewsFixedSuffixToFirstLine(ja_html, "に出演します。");
            }

            const view_date = viewDateFromPadded(date);
            let image_kind = null;

            if (finalType === "voice") {
              ja_html = wrapIfVoiceSpan("voice", ja_html);
              en_html = wrapIfVoiceSpan("voice", en_html || ja_html);
              image_kind = "voice";
            }

            const legacy_key = await pickLegacyKey(finalType, date, `${pendingImg.image_src}:${ja_html}`);

            const row = {
              type: finalType,
              date,
              ja_html,
              en_html,
              ja_link_text: "",
              ja_link_href: "",
              en_link_text: "",
              en_link_href: "",
              image_src: pendingImg.image_src,
              image_kind,
              enabled: "TRUE",
              view_date,
              media_type: "image",
              media_src: null,
              poster_src: null,
              legacy_key,
            };

            const newId = await insertPost(env, row);
            await env.KV.delete(keyPendingImage(userId));

            if (replyToken) await lineReply(env, replyToken, `✅ 投稿しました (ID:${newId ?? "?"})\n[${finalType.toUpperCase()}] date=${date}`, userId);
            continue;
          }
        }

        // 通常投稿ロジック
        let { type, content, explicit } = detectTypeAndContent(text);
        // ✅ 画像が pending で、かつ NEXT/T: で種別が固定されている場合
        //    ここで type を強制（ただし本文側で明示プレフィックス指定があるときは本文を優先）
        const pendingImageObj0 = await kvGetJson(env, keyPendingImage(userId));
        if (pendingImageObj0?.forcedType && !explicit) {
          type = pendingImageObj0.forcedType;
        }

        const date = extractDatePadded(content) || todayJstDatePadded();
        const urlInText = extractUrl(content);
        const contentNoUrl = urlInText ? content.replace(urlInText, "").trim() : content;

        let ai;
        try {
          ai = await generateJaEn(env, contentNoUrl, type === "news");
        } catch (e) {
          await kvLogDebug(env, { where: "generateJaEn:failed", err: errorText(e), type, contentPreview: short(contentNoUrl, 200), ts: Date.now() }, TTL_DEBUG, "general");
          ai = { ja: contentNoUrl, en: "", btnJa: "詳細を見る", btnEn: "View Details" };
        }

        const pendingImageObj = await kvGetJson(env, keyPendingImage(userId));
        if (pendingImageObj) await env.KV.delete(keyPendingImage(userId));
        const pendingImageSrc = pendingImageObj?.image_src || null;

        const pendingVideo2 = await kvGetJson(env, keyPendingVideo(userId));
        let media_type = "image";
        let media_src = null;
        let poster_src = null;

        if (pendingVideo2?.stage === "await_text" && pendingVideo2?.video_key && pendingVideo2?.poster_key) {
          media_type = "video";
          media_src = pendingVideo2.video_key;
          poster_src = pendingVideo2.poster_key;
          await env.KV.delete(keyPendingVideo(userId));
        }

        let ja_html = ai.ja;
        let en_html = ai.en;

        let ja_link_text = "";
        let en_link_text = "";
        let ja_link_href = "";
        let en_link_href = "";

        let image_src = null;
        let image_kind = null;

        const view_date = viewDateFromPadded(date);

        if (type === "news") {
          if (urlInText) {
            ja_link_text = ai.btnJa || "詳細を見る";
            en_link_text = ai.btnEn || "View Details";
            ja_link_href = urlInText;
            en_link_href = urlInText;
          }
          image_src = pendingImageSrc || null;
        } else if (type === "archive") {
          image_src = pendingImageSrc || null;
        } else {
          ja_html = wrapIfVoiceSpan("voice", ai.ja);
          en_html = wrapIfVoiceSpan("voice", ai.en || ai.ja);
          image_src = pendingImageSrc || null;
          image_kind = image_src ? "voice" : null;
        }

        let hashSource = "";
        if (type === "news") hashSource = ja_link_href || contentNoUrl;
        else hashSource = contentNoUrl;

        const legacy_key = await pickLegacyKey(type, date, hashSource);

        const row = {
          type,
          date,
          ja_html,
          en_html,
          ja_link_text,
          ja_link_href,
          en_link_text,
          en_link_href,
          image_src,
          image_kind,
          enabled: "TRUE",
          view_date,
          media_type,
          media_src,
          poster_src,
          legacy_key,
        };

        const newId = await insertPost(env, row);

        if (replyToken) {
          await lineReply(
            env,
            replyToken,
            `✅ 更新完了 (ID: ${newId ?? "?"})\n[${type.toUpperCase()}] ${contentNoUrl.slice(0, 20)}${contentNoUrl.length > 20 ? "..." : ""}`,
            userId
          );
        }
        continue;
      }
    } catch (e) {
      const err = errorText(e);
      console.error("LINE event error:", err);

      await kvLogDebug(
        env,
        {
          where: "processLineWebhook:event_catch",
          err,
          hasReplyToken: !!event?.replyToken,
          msgType: event?.message?.type,
          msgId: event?.message?.id,
          textPreview: (event?.message?.text || "").slice(0, 120),
          ts: Date.now(),
        },
        TTL_DEBUG,
        "line"
      );
    }
  }
}

// =============================
// Part 6/6
// =============================

// -----------------------------
// CSV decode + parse (RFC4180-ish)
// -----------------------------
function decodeCSV(arrayBuffer, contentType) {
  const ct = (contentType || "").toLowerCase();
  const m = ct.match(/charset\s*=\s*([^\s;]+)/i);
  if (m) {
    const enc = m[1].replace(/["']/g, "");
    try {
      return new TextDecoder(enc).decode(arrayBuffer);
    } catch {}
  }

  let utf8 = "";
  try {
    utf8 = new TextDecoder("utf-8").decode(arrayBuffer);
  } catch {
    utf8 = "";
  }
  if (utf8 && utf8.charCodeAt(0) === 0xfeff) utf8 = utf8.slice(1);
  const repUtf8 = (utf8.match(/\uFFFD/g) || []).length;
  if (utf8 && repUtf8 === 0) return utf8;

  let sjis = "";
  try {
    sjis = new TextDecoder("shift_jis").decode(arrayBuffer);
  } catch {
    sjis = "";
  }
  const score = (s) => {
    const rep = (s.match(/\uFFFD/g) || []).length;
    const moj = (s.match(/縲|繝|譁|蠕|蟷|豎|迚/g) || []).length;
    return rep * 100 + moj * 3;
  };
  if (!sjis) return utf8 || "";
  return score(sjis) < score(utf8) ? sjis : utf8;
}

function parseCSV(text) {
  if (!text) return [];
  text = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);

  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i++) {
    const c = text[i];

    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else field += c;
    } else {
      if (c === '"') inQuotes = true;
      else if (c === ",") {
        row.push(field);
        field = "";
      } else if (c === "\n") {
        row.push(field);
        rows.push(row);
        row = [];
        field = "";
      } else field += c;
    }
  }
  row.push(field);
  rows.push(row);

  const head = (rows.shift() || []).map((h) => (h || "").trim());
  if (!head.length) return [];

  return rows
    .filter((r) => r.some((v) => (v || "").trim() !== ""))
    .map((cols) => {
      const o = {};
      head.forEach((h, idx) => (o[h] = (cols[idx] ?? "").trim()));
      return o;
    });
}

function toDatePadded(s) {
  const t = nz(s).trim();
  const m = t.match(/^(\d{4})\.(\d{1,2})\.(\d{1,2})$/);
  if (!m) return t;
  const y = m[1];
  const mm = String(parseInt(m[2], 10)).padStart(2, "0");
  const dd = String(parseInt(m[3], 10)).padStart(2, "0");
  return `${y}.${mm}.${dd}`;
}

function pickLegacyKeyFromCsv(type, r, normalizedDate) {
  const date = (normalizedDate ?? nz(r.date)).trim();
  if (type === "archive" && date) return `archive:date:${date}`;

  const id = nz(r.id).trim();
  if (id && date) return `${type}:id:${id}:${date}`;
  if (id) return `${type}:id:${id}`;
  if (date) return `${type}:date:${date}`;
  return `${type}:row:${crypto.randomUUID()}`;
}

// -----------------------------
// Auth helpers
// -----------------------------
function debugAuthorized(req, env) {
  const token = req.headers.get("x-debug-token") || (req.headers.get("authorization") || "").replace(/^Bearer\s+/i, "");
  if (!env.DEBUG_TOKEN) return false;
  return token && token === env.DEBUG_TOKEN;
}
function importAuthorized(req, env) {
  const token = req.headers.get("x-import-token") || (req.headers.get("authorization") || "").replace(/^Bearer\s+/i, "");
  if (!env.IMPORT_TOKEN) return false;
  return token && token === env.IMPORT_TOKEN;
}

export default {
  async fetch(req, env, ctx) {
    if (req.method === "OPTIONS") return new Response("", { headers: withCors() });

    const url = new URL(req.url);

    if (url.pathname === "/health") {
      return json({ ok: true, version: VERSION });
    }

    if (url.pathname === "/debug-last") {
      if (!debugAuthorized(req, env)) return textOut("forbidden", 403);

      const scope = normalizeDebugScope(url.searchParams.get("scope"));
      const lastKey =
        (await env.KV.get(keyDebugLast(scope))) ||
        (await env.KV.get(keyDebugLast("general"))) ||
        (scope === "general" ? await env.KV.get("debug:last") : null);

      if (!lastKey) return json({ ok: true, scope, lastKey: null, log: null });

      const raw = await env.KV.get(lastKey);
      let parsed = null;
      try {
        parsed = raw ? JSON.parse(raw) : null;
      } catch {
        parsed = raw || null;
      }
      return json({ ok: true, scope, lastKey, log: parsed });
    }

    if (url.pathname.startsWith("/media/") && req.method === "GET") {
      return await handleMedia(url, req, env);
    }

    // Alias: /api/news|voice|archive
    if (/^\/api\/(news|voice|archive)$/.test(url.pathname)) {
      const t = url.pathname.split("/").pop();
      const u = new URL(url.toString());
      u.pathname = "/posts";
      u.searchParams.set("type", t);
      return await handlePosts(u, env);
    }

    if (url.pathname === "/line-webhook" && req.method === "POST") {
      const sig = req.headers.get("x-line-signature") || "";
      const raw = await req.arrayBuffer();

      const okSig = await verifyLineSignature(env, raw, sig);
      if (!okSig) return textOut("bad signature", 401);

      let payload;
      try {
        payload = JSON.parse(new TextDecoder().decode(raw));
      } catch {
        return textOut("bad json", 400);
      }

      try {
        ctx?.waitUntil?.(processLineWebhook(env, payload));
      } catch (e) {
        await kvLogDebug(env, { where: "fetch:waitUntil_failed", err: errorText(e), ts: Date.now() }, TTL_DEBUG, "general");
      }
      return textOut("OK");
    }

    if (url.pathname === "/posts") {
      return await handlePosts(url, env);
    }

    // ✅ /import（必ず token）
    if (url.pathname === "/import") {
      if (!importAuthorized(req, env)) return textOut("forbidden", 403);

      let scanned = 0;
      let inserted = 0;
      let ignored = 0;

      // ✅ DO NOTHING（要件）
      const q = `
        INSERT INTO posts
        (type,date,ja_html,en_html,ja_link_text,ja_link_href,en_link_text,en_link_href,
         image_src,image_kind,enabled,view_date,media_type,media_src,poster_src,
         legacy_key,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))
        ON CONFLICT(legacy_key) DO NOTHING
      `;

      try {
        for (const type of ["news", "voice", "archive"]) {
          const res = await fetch(CSV[type]);
          if (!res.ok) {
            return new Response(`CSV fetch failed: ${type} ${res.status} ${res.statusText}`, {
              status: 502,
              headers: withCors({ "Content-Type": "text/plain; charset=utf-8" }),
            });
          }

          const buf = await res.arrayBuffer();
          const csvText = decodeCSV(buf, res.headers.get("content-type"));
          const rows = parseCSV(csvText);

          for (const r of rows) {
            const dateRaw = nz(r.date).trim();
            if (!dateRaw) continue;

            const date = type === "archive" ? toDatePadded(dateRaw) : dateRaw;
            scanned++;

            const ja_html = nz(r.ja_html) || (type === "archive" ? nz(r.title_ja) : "");
            const en_html = nz(r.en_html) || (type === "archive" ? nz(r.title_en) : "");

            const ja_link_text = nz(r.ja_link_text);
            const ja_link_href = nz(r.ja_link_href);
            const en_link_text = nz(r.en_link_text);
            const en_link_href = nz(r.en_link_href);

            const image_src = nz(r.image_src).trim() || null;
            const image_kind = nz(r.image_kind).trim() || null;

            const enabled = normalizeBoolTextDefaultTrue(r.enabled);

            const view_date = nz(r.view_date).trim() || viewDateFromPadded(date) || null;

            const media_type = nz(r.media_type).trim() || "image";
            const media_src = nz(r.media_src).trim() || null;
            const poster_src = nz(r.poster_src).trim() || null;

            const legacy_key = pickLegacyKeyFromCsv(type, r, date);

            const out = await env.DB.prepare(q)
              .bind(
                type,
                date,
                ja_html,
                en_html,
                ja_link_text,
                ja_link_href,
                en_link_text,
                en_link_href,
                image_src,
                image_kind,
                enabled,
                view_date,
                media_type,
                media_src,
                poster_src,
                legacy_key
              )
              .run();

            const changes = out?.meta?.changes ?? 0;
            if (changes > 0) inserted++;
            else ignored++;
          }
        }

        await kvLogDebug(env, { where: "import:done", scanned, inserted, ignored, ts: Date.now() }, TTL_DEBUG, "db");
        return json({ scanned, inserted, ignored });
      } catch (e) {
        await kvLogDebug(env, { where: "import:error", err: errorText(e), ts: Date.now() }, TTL_DEBUG, "db");
        return new Response(`import error: ${errorText(e)}`, {
          status: 500,
          headers: withCors({ "Content-Type": "text/plain; charset=utf-8" }),
        });
      }
    }

    return textOut("hosei api alive");
  },
};