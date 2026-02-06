// worker.js — hosei-content-api + LINE ingest
// (GitHub images + R2 video/poster + OpenAI(JSON Schema) + Gemini fallback -> D1)
// + KV scoped debug (openai/gemini/line/db/general) + OpenAI request-id/usage logs
//
// - GET  /health
// - GET  /import        : (legacy CSV -> D1) optional
// - GET  /posts?type=   : posts JSON
// - GET  /api/{type}    : alias to /posts?type=
// - GET  /media/<key>   : R2 object proxy (video/poster)
// - POST /line-webhook  : LINE ingress (admin only)
// - GET  /debug-last    : last debug log (admin only, key=ADMIN_USER_ID, scope=general|openai|gemini|line|db)
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
//   OPENAI_MODEL
//   GEMINI_API_KEY
//   GEMINI_MODEL
//   GITHUB_TOKEN
//   GITHUB_OWNER
//   GITHUB_REPO
//   GITHUB_BRANCH
//
// Optional Secrets (debug):
//   DEBUG_OPENAI    = "1"  // OpenAI raw_ok log
//   DEBUG_LOG_BODY  = "1"  // longer error body in KV

const VERSION =
  "hosei-content-api-2026-02-06-jsonschema+gemini-hybrid-waituntil-voiceprefixfix+pushfallback+kvdebug+openailogs+chat-edit+multidelete";

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
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
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
// Debug KV (scoped last pointer)
// -----------------------------
const TTL_DEBUG = 24 * 60 * 60; // 24h

function keyDebug(scope = "general") {
  return `debug:${scope}:${Date.now()}:${crypto.randomUUID()}`;
}
function keyDebugLast(scope = "general") {
  return `debug:last:${scope}`;
}

async function kvLogDebug(env, payload, ttl = TTL_DEBUG, scope = "general") {
  try {
    const k = keyDebug(scope);
    await env.KV.put(k, JSON.stringify(payload), { expirationTtl: ttl });
    await env.KV.put(keyDebugLast(scope), k, { expirationTtl: ttl }); // scoped last pointer
    // backward compat
    await env.KV.put("debug:last", k, { expirationTtl: ttl });
    return k;
  } catch {
    return null;
  }
}

// OpenAI/Gemini debug helpers
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
//   - Voiceは「今のまま」＝ V: を付けても付けなくても voice 扱い
//   - ただし本文には V: を残さない
// -----------------------------
function detectTypeAndContent(text) {
  const t = nz(text).trim();
  let type = "voice";

  // 判定（news/archive優先）
  if (/^(ニュース|ニュース：|N：|N:|に：|に:)/i.test(t)) type = "news";
  else if (/^(アーカイブ|アーカイブ：|A：|A:|あ：|あ:)/i.test(t)) type = "archive";
  else if (/^(V：|V:|v：|v:|ボイス|voice|VOICE)[:：\s]/.test(t)) type = "voice";

  // 先頭プレフィックスを削除（V: も確実に消す）
  const content = t
    .replace(
      /^(ニュース|アーカイブ|ボイス|VOICE|voice|ニュース：|アーカイブ：|N：|A：|V：|N:|A:|V:|に：|あ：|に:|あ:|v：|v:)\s*[:：]?\s*/i,
      ""
    )
    .trim();

  return { type, content };
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
  const y = m[1] ? String(m[1]) : String(new Date(Date.now() + 9 * 60 * 60 * 1000).getUTCFullYear());
  const mo = String(parseInt(m[2], 10)).padStart(2, "0");
  const da = String(parseInt(m[3], 10)).padStart(2, "0");
  return `${y}.${mo}.${da}`;
}
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

// -----------------------------
// ✅ PATCH: Delete command (single/multi/range, tolerant spaces)
// -----------------------------
function parseDeleteIds(text) {
  const s = nz(text)
    .replace(/[「」『』"]/g, "")
    .trim()
    .replace(/\s+/g, " ");

  // 例:
  // 削除:55
  // 削除 : ID: 55
  // 消去: 55,56
  // 削除: 55-60
  const m = s.match(/^(削除|消去|さ)\s*[:：]\s*(?:id\s*[:：]\s*)?(.+)$/i);
  if (!m) return null;

  const rest = (m[2] || "").trim();

  // 範囲 55-60
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

  // 複数 55,56,57（空白区切りも許容）
  const parts = rest.split(/[,\s]+/).filter(Boolean);
  const ids = parts
    .map((x) => parseInt(x.replace(/[^0-9]/g, ""), 10))
    .filter((n) => Number.isFinite(n) && n > 0);

  return ids.length ? Array.from(new Set(ids)) : null;
}

// -----------------------------
// ✅ PATCH: Edit commands
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
  const m = s.match(/^(JA|EN|BTNJA|BTNEN)\s*[:：]\s*([\s\S]+)$/i);
  if (!m) return null;
  return { field: m[1].toUpperCase(), value: (m[2] || "").trim() };
}

// -----------------------------
// LINE content fetch
// -----------------------------
async function fetchLineMessageContent(env, messageId) {
  const url = `https://api-data.line.me/v2/bot/message/${messageId}/content`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${env.LINE_CHANNEL_ACCESS_TOKEN}` },
  });
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
    body: JSON.stringify({
      message: `Upload ${filename} from LINE`,
      content: b64,
      branch,
    }),
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`GitHub upload failed: ${res.status} ${t.slice(0, 400)}`);
  }

  return filename;
} // -----------------------------
// R2 keys
// -----------------------------
function r2KeyForVideo(userId, messageId) {
  const ym = todayJstDatePadded().slice(0, 7).replace(".", "");
  return `media/video/${ym}/${userId}/${messageId}.mp4`;
}
function r2KeyForPoster(userId, messageId, ext = "jpg") {
  const ym = todayJstDatePadded().slice(0, 7).replace(".", "");
  return `media/poster/${ym}/${userId}/${messageId}.${ext}`;
}

// -----------------------------
// OpenAI generation (Responses API + response_format json_schema)
// -----------------------------
function pickOutputTextFromResponses(data) {
  const ot = nz(data?.output_text).trim();
  if (ot) return ot;

  const out = data?.output;
  if (Array.isArray(out)) {
    for (const item of out) {
      const contents = item?.content;
      if (!Array.isArray(contents)) continue;
      for (const c of contents) {
        const t = nz(c?.text).trim();
        if (t) return t;
      }
    }
  }
  return "";
}

function safeJsonParse(s) {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

async function openaiResponsesJsonSchema(env, { system, user, schemaName = "hosei_copy_schema", maxTokens = 400 }) {
  const model = env.OPENAI_MODEL || "gpt-5-mini-2025-08-07";
  if (!env.OPENAI_API_KEY) throw new Error("Missing OPENAI_API_KEY");

  // NOTE (2026-02): In the Responses API, structured output moved from
  //   response_format -> text.format
  // Ref: https://platform.openai.com/docs/api-reference/responses/create
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
        // Responses API expects these fields at the top level (not nested).
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
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${env.OPENAI_API_KEY}`,
    },
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
        outTextPreview: short(outText, 600),
        ts: Date.now(),
      },
      TTL_DEBUG,
      "openai"
    );
  }

  if (!outText) {
    await kvLogDebug(
      env,
      {
        where: "openaiResponsesJsonSchema:no_output_text",
        model,
        schemaName,
        response_id: respId || null,
        x_request_id: xrid || null,
        usage,
        dataKeys: Object.keys(data || {}),
        durMs,
        ts: Date.now(),
      },
      TTL_DEBUG,
      "openai"
    );
    throw new Error("OpenAI response has no output text");
  }

  const obj = safeJsonParse(outText);
  if (!obj) {
    await kvLogDebug(
      env,
      {
        where: "openaiResponsesJsonSchema:json_parse_failed",
        model,
        schemaName,
        response_id: respId || null,
        x_request_id: xrid || null,
        usage,
        outTextPreview: short(outText, 1200),
        durMs,
        ts: Date.now(),
      },
      TTL_DEBUG,
      "openai"
    );
    throw new Error("OpenAI JSON parse failed (despite json_schema)");
  }

  const ja = nz(obj.ja).trim();
  const en = nz(obj.en).trim();
  const btnJa = nz(obj.btnJa).trim();
  const btnEn = nz(obj.btnEn).trim();

  if (!ja || !btnJa || !btnEn) {
    await kvLogDebug(
      env,
      {
        where: "openaiResponsesJsonSchema:missing_fields",
        model,
        schemaName,
        response_id: respId || null,
        x_request_id: xrid || null,
        usage,
        objPreview: { ja: short(ja, 120), en: short(en, 120), btnJa, btnEn },
        durMs,
        ts: Date.now(),
      },
      TTL_DEBUG,
      "openai"
    );
    throw new Error("OpenAI JSON missing required fields");
  }

  await kvLogDebug(
    env,
    {
      where: "openaiResponsesJsonSchema:ok",
      model,
      schemaName,
      response_id: respId || null,
      x_request_id: xrid || null,
      usage,
      jaPreview: short(ja, 160),
      enPreview: short(en, 160),
      durMs,
      ts: Date.now(),
    },
    TTL_DEBUG,
    "openai"
  );

  return { ja, en, btnJa, btnEn };
}

// -----------------------------
// Gemini fallback (Translate EN / optional full generation)
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

  const model = env.GEMINI_MODEL || "gemini-2.0-flash";
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent`;
  const body = { contents: [{ parts: [{ text: prompt }] }] };

  const t0 = Date.now();
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-goog-api-key": env.GEMINI_API_KEY,
    },
    body: JSON.stringify(body),
  });
  const durMs = Date.now() - t0;

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    await kvLogDebug(
      env,
      {
        where: "geminiGenerateText:http_error",
        status: res.status,
        body: t.slice(0, 800),
        model,
        durMs,
        ts: Date.now(),
      },
      TTL_DEBUG,
      "gemini"
    );
    throw new Error(`Gemini error: ${res.status} ${t.slice(0, 300)}`);
  }

  const data = await res.json();
  const text = pickGeminiText(data);
  if (!text) {
    await kvLogDebug(
      env,
      {
        where: "geminiGenerateText:empty",
        model,
        keys: Object.keys(data || {}),
        durMs,
        ts: Date.now(),
      },
      TTL_DEBUG,
      "gemini"
    );
    throw new Error("Gemini returned empty text");
  }

  return text;
}

async function geminiTranslateEn(env, jaText) {
  const prompt = `Translate the following Japanese into natural English for a website (concise, no extra commentary). Output ONLY the English text.\n\nJapanese:\n${jaText}`;
  const en = await geminiGenerateText(env, prompt);
  return en.trim();
}

// -----------------------------
// Unified generation (OpenAI json_schema + Gemini fallback)
// -----------------------------
async function generateJaEn(env, content, forNews) {
  const sys = `You are a bilingual copy editor for a Japanese artist website.
You MUST output STRICT JSON that conforms to the provided JSON Schema (no markdown, no extra text).

Rules:
- "ja": natural Japanese, concise (website-ready)
- "en": natural English translation of "ja" (must not be empty)
- "btnJa"/"btnEn": button labels (use sensible defaults if no URL context)
- Prefer single-line text (no line breaks unless necessary for <br>)
- Always perform at least one minor edit to improve readability (punctuation/wording), unless the input is already perfect.
- If the input contains a URL, do NOT include the URL in "ja"/"en".`;

  const prompt = forNews
    ? `Input:\n${content}\n\n(News item: keep it short, neutral, informative.)`
    : `Input:\n${content}`;

  let ja = nz(content).trim();
  let en = "";
  let btnJa = "詳細を見る";
  let btnEn = "View Details";

  // 1) OpenAI (main)
  try {
    const obj = await openaiResponsesJsonSchema(env, {
      system: sys,
      user: prompt,
      schemaName: forNews ? "hosei_news_copy" : "hosei_copy",
      maxTokens: 450,
    });

    ja = nz(obj.ja).trim() || ja;
    en = nz(obj.en).trim();
    btnJa = nz(obj.btnJa).trim() || btnJa;
    btnEn = nz(obj.btnEn).trim() || btnEn;

    // OpenAIのenが空/短すぎる場合はGeminiで英訳を補完する
    if (!en || en.length < 4) {
      try {
        const en2 = await geminiTranslateEn(env, ja);
        if (en2) {
          en = en2;
          await kvLogDebug(
            env,
            {
              where: "gemini:fallback_en",
              reason: "openai_en_empty_or_short",
              jaPreview: short(ja, 120),
              enPreview: short(en, 120),
              ts: Date.now(),
            },
            TTL_DEBUG,
            "gemini"
          );
        }
      } catch (e) {
        await kvLogDebug(
          env,
          {
            where: "gemini:fallback_en_failed",
            err: errorText(e),
            ts: Date.now(),
          },
          TTL_DEBUG,
          "gemini"
        );
      }
    }

    await kvLogDebug(
      env,
      {
        where: "generateJaEn:ok",
        used: "openai_main(+gemini_if_needed)",
        jaPreview: short(ja, 120),
        enPreview: short(en, 120),
        ts: Date.now(),
      },
      TTL_DEBUG,
      "general"
    );

    return { ja, en, btnJa, btnEn };
  } catch (e) {
    await kvLogDebug(
      env,
      {
        where: "generateJaEn:openai_failed",
        err: errorText(e),
        contentPreview: short(nz(content), 200),
        ts: Date.now(),
      },
      TTL_DEBUG,
      "openai"
    );
  }

  // 2) OpenAIが落ちた場合：Geminiで英訳だけでも埋める（jaは原文）
  try {
    const en2 = await geminiTranslateEn(env, ja);
    en = en2 || "";
    await kvLogDebug(
      env,
      {
        where: "generateJaEn:gemini_only",
        jaPreview: short(ja, 120),
        enPreview: short(en, 120),
        ts: Date.now(),
      },
      TTL_DEBUG,
      "gemini"
    );
  } catch (e) {
    await kvLogDebug(
      env,
      {
        where: "generateJaEn:gemini_only_failed",
        err: errorText(e),
        ts: Date.now(),
      },
      TTL_DEBUG,
      "gemini"
    );
  }

  return { ja, en, btnJa, btnEn };
}

// -----------------------------
// LINE reply/push  reply失敗→push救済
// -----------------------------
async function linePush(env, to, text) {
  const url = "https://api.line.me/v2/bot/message/push";
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${env.LINE_CHANNEL_ACCESS_TOKEN}`,
    },
    body: JSON.stringify({
      to,
      messages: [{ type: "text", text }],
    }),
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
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${env.LINE_CHANNEL_ACCESS_TOKEN}`,
    },
    body: JSON.stringify({
      replyToken,
      messages: [{ type: "text", text }],
    }),
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");

    await kvLogDebug(
      env,
      {
        where: "lineReply:failed",
        status: res.status,
        body: t.slice(0, 400),
        textPreview: (text || "").slice(0, 160),
        ts: Date.now(),
      },
      TTL_DEBUG,
      "line"
    );

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
// DB helpers
// -----------------------------
function pickLegacyKey(type, date) {
  if (type === "archive" && date) return `archive:date:${date}`; // 増殖防止
  return `${type}:${date}:${crypto.randomUUID()}`;
}

async function insertPost(env, row) {
  const q = `
    INSERT INTO posts
      (type,date,ja_html,en_html,ja_link_text,ja_link_href,en_link_text,en_link_href,
       image_src,image_kind,enabled,view_date,
       media_type,media_src,poster_src,legacy_key,created_at)
    VALUES
      (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?, datetime('now'))
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

async function softDeleteById(env, id) {
  const q = `UPDATE posts SET enabled='FALSE' WHERE id=?`;
  const out = await env.DB.prepare(q).bind(id).run();
  const changes = out?.meta?.changes ?? 0;
  const ok = changes > 0;

  await kvLogDebug(env, { where: "softDeleteById", id, ok, changes, ts: Date.now() }, TTL_DEBUG, "db");
  return ok;
}

// -----------------------------
// ✅ PATCH: get/update helpers for editing
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

function escapeHtml(s) {
  return nz(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function wrapIfVoiceSpan(type, htmlOrText) {
  const t = nz(htmlOrText).trim();
  if (type === "voice") {
    if (/^<span>[\s\S]*<\/span>$/.test(t)) return t;
    return `<span>${escapeHtml(t)}</span>`;
  }
  return t;
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

async function softDeleteMany(env, ids) {
  let ok = 0;
  for (const id of ids) {
    const out = await env.DB.prepare(`UPDATE posts SET enabled='FALSE' WHERE id=?`).bind(id).run();
    if ((out?.meta?.changes ?? 0) > 0) ok++;
  }
  return ok;
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
// ✅ PATCH: Editing KV
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
} // -----------------------------
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

// archive date normalize: "2022.7.6" -> "2022.07.06"
function toDatePadded(s) {
  const t = nz(s).trim();
  const m = t.match(/^(\d{4})\.(\d{1,2})\.(\d{1,2})$/);
  if (!m) return t;
  const y = m[1];
  const mm = String(parseInt(m[2], 10)).padStart(2, "0");
  const dd = String(parseInt(m[3], 10)).padStart(2, "0");
  return `${y}.${mm}.${dd}`;
}
function toViewDateFromPaddedDate(padded) {
  return viewDateFromPadded(padded);
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
// /posts handler (functionized)
// -----------------------------
async function handlePosts(url, env) {
  const type = url.searchParams.get("type") || "news";
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
  return json(results);
}

// -----------------------------
// /media/<key> で R2配信（動画/サムネ用）
// -----------------------------
async function handleMedia(url, req, env) {
  const key = decodeURIComponent(url.pathname.replace(/^\/media\//, ""));
  if (!key) return textOut("missing key", 400);

  const obj = await env.R2.get(key);
  if (!obj) return textOut("not found", 404);

  const headers = new Headers(withCors());
  const ct = obj.httpMetadata?.contentType || "application/octet-stream";
  headers.set("Content-Type", ct);
  headers.set("Accept-Ranges", "bytes");
  headers.set("Cache-Control", "public, max-age=31536000, immutable");

  return new Response(obj.body, { status: 200, headers });
}

// -----------------------------
// LINE processing (waitUntil target)
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

      // image
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
            await lineReply(
              env,
              replyToken,
              "🖼 サムネを受け取りました。続けて本文（N:/に: / V: / A:/あ:）を送ってください。",
              userId
            );
          }
          continue;
        }

        const { bytes, contentType } = await fetchLineMessageContent(env, msg.id);
        const fileName = await uploadImageToGitHub(env, { bytes, contentType, messageId: msg.id });

        await env.KV.put(keyPendingImage(userId), fileName, { expirationTtl: TTL_PENDING });

        if (replyToken) {
          await lineReply(
            env,
            replyToken,
            `📷 画像を保存しました（${fileName}）。続けて本文（に:/N:/A:/あ:/V:）を送ってください。`,
            userId
          );
        }
        continue;
      }

      // video
      if (msg.type === "video") {
        const { bytes, contentType } = await fetchLineMessageContent(env, msg.id);
        const videoKey = r2KeyForVideo(userId, msg.id);

        await env.R2.put(videoKey, bytes, {
          httpMetadata: { contentType: contentType || "video/mp4" },
        });

        await kvPutJson(env, keyPendingVideo(userId), {
          stage: "await_poster",
          video_key: videoKey,
          video_message_id: msg.id,
        });

        if (replyToken) {
          await lineReply(env, replyToken, "🎥 動画を受け取りました。続けてサムネ画像を送ってください。", userId);
        }
        continue;
      }

      // text
      if (msg.type === "text") {
        const text = nz(msg.text).trim();

        // ---- ✅ 編集 終了/取消 ----
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

        // ---- ✅ 編集 開始 ----
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
            `JA:\n${nz(row.ja_html)}\n\n` +
            `EN:\n${nz(row.en_html)}\n\n` +
            `修正はこう送ってください：\n` +
            `JA: ... / EN: ... / BTNJA: ... / BTNEN: ...\n` +
            `終わるとき：完了　やめる：取消`;

          if (replyToken) await lineReply(env, replyToken, msgOut, userId);
          continue;
        }

        // ---- ✅ 編集中のフィールド更新 ----
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
          if (upd.field === "JA") {
            const newJa = wrapIfVoiceSpan(row.type, upd.value);
            ok = await updatePostFields(env, row.id, { ja_html: newJa });
          } else if (upd.field === "EN") {
            const newEn = wrapIfVoiceSpan(row.type, upd.value);
            ok = await updatePostFields(env, row.id, { en_html: newEn });
          } else if (upd.field === "BTNJA") {
            ok = await updatePostFields(env, row.id, { ja_link_text: upd.value });
          } else if (upd.field === "BTNEN") {
            ok = await updatePostFields(env, row.id, { en_link_text: upd.value });
          }

          if (replyToken) {
            await lineReply(
              env,
              replyToken,
              ok ? `✅ ${upd.field} を更新しました (ID:${row.id})` : `⚠️ 更新できませんでした (ID:${row.id})`,
              userId
            );
          }
          continue;
        }

        // ---- ✅ 削除（単発/複数/範囲） ----
        const delIds = parseDeleteIds(text);
        if (delIds) {
          const n = await softDeleteMany(env, delIds);
          if (replyToken) {
            await lineReply(env, replyToken, `🗑️ 非表示にしました：${n}/${delIds.length} 件\n(${delIds.join(", ")})`, userId);
          }
          continue;
        }

        // ---- ここから既存の通常投稿ロジック ----
        const { type, content } = detectTypeAndContent(text);
        const date = extractDatePadded(content) || todayJstDatePadded();
        const urlInText = extractUrl(content);
        const contentNoUrl = urlInText ? content.replace(urlInText, "").trim() : content;

        let ai;
        try {
          ai = await generateJaEn(env, contentNoUrl, type === "news");
        } catch (e) {
          await kvLogDebug(
            env,
            {
              where: "generateJaEn:failed",
              err: errorText(e),
              type,
              contentPreview: short(contentNoUrl, 200),
              ts: Date.now(),
            },
            TTL_DEBUG,
            "general"
          );
          ai = { ja: contentNoUrl, en: "", btnJa: "詳細を見る", btnEn: "View Details" };
        }

        const pendingImage = await env.KV.get(keyPendingImage(userId));
        if (pendingImage) await env.KV.delete(keyPendingImage(userId));

        const pendingVideo = await kvGetJson(env, keyPendingVideo(userId));
        let media_type = "image";
        let media_src = null;
        let poster_src = null;

        if (pendingVideo?.stage === "await_text" && pendingVideo?.video_key && pendingVideo?.poster_key) {
          media_type = "video";
          media_src = pendingVideo.video_key;
          poster_src = pendingVideo.poster_key;
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

        let view_date = null;

        if (type === "news") {
          if (urlInText) {
            ja_link_text = ai.btnJa || "詳細を見る";
            en_link_text = ai.btnEn || "View Details";
            ja_link_href = urlInText;
            en_link_href = urlInText;
          }
          image_src = pendingImage || null;
        } else if (type === "archive") {
          image_src = pendingImage || null;
          view_date = viewDateFromPadded(date);
        } else {
          // VOICE（ルールは今のまま）
          ja_html = `<span>${ai.ja}</span>`;
          en_html = `<span>${ai.en}</span>`;
          image_src = pendingImage || null;
          image_kind = image_src ? "voice" : null;
          view_date = viewDateFromPadded(date);
        }

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
          legacy_key: pickLegacyKey(type, date),
        };

        const newId = await insertPost(env, row);

        if (replyToken) {
          await lineReply(
            env,
            replyToken,
            `✅ 更新完了 (ID: ${newId ?? "?"})\n[${type.toUpperCase()}] ${contentNoUrl.slice(0, 20)}${
              contentNoUrl.length > 20 ? "..." : ""
            }`,
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

// -----------------------------
// Worker
// -----------------------------
export default {
  async fetch(req, env, ctx) {
    if (req.method === "OPTIONS") return new Response("", { headers: withCors() });

    const url = new URL(req.url);

    if (url.pathname === "/health") {
      return json({ ok: true, version: VERSION });
    }

    // Debug last (admin only)  scope=general|openai|gemini|line|db
    if (url.pathname === "/debug-last") {
      const k = url.searchParams.get("key") || "";
      if (!k || k !== env.ADMIN_USER_ID) return textOut("forbidden", 403);

      const scope = url.searchParams.get("scope") || "general";
      const lastKey = (await env.KV.get(`debug:last:${scope}`)) || (await env.KV.get("debug:last"));
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

    // LINE webhook (waitUntil)
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

      // Immediately return 200; process async
      try {
        ctx?.waitUntil?.(processLineWebhook(env, payload));
      } catch (e) {
        await kvLogDebug(env, { where: "fetch:waitUntil_failed", err: errorText(e), ts: Date.now() }, TTL_DEBUG, "general");
      }
      return textOut("OK");
    }

    // /posts
    if (url.pathname === "/posts") {
      return await handlePosts(url, env);
    }

    // /import (optional)
    if (url.pathname === "/import") {
      let scanned = 0;
      let upserted = 0;
      let ignored = 0;

      const q = `
        INSERT INTO posts
        (type,date,ja_html,en_html,ja_link_text,ja_link_href,en_link_text,en_link_href,
         image_src,image_kind,enabled,view_date,media_type,media_src,poster_src,
         legacy_key,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?, datetime('now'))
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

      try {
        await env.DB.prepare(`
          UPDATE posts
          SET legacy_key = 'archive:date:' || date
          WHERE type='archive'
            AND legacy_key LIKE 'archive:id:%'
        `).run();

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

            const view_date =
              nz(r.view_date).trim() ||
              (type === "archive" ? toViewDateFromPaddedDate(date) : "") ||
              null;

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
            if (changes > 0) upserted++;
            else ignored++;
          }
        }

        await kvLogDebug(env, { where: "import:done", scanned, upserted, ignored, ts: Date.now() }, TTL_DEBUG, "db");
        return json({ scanned, upserted, ignored });
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
