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

