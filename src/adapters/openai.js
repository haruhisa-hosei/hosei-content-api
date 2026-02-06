import { nz, short, safeJsonParse } from "../core/strings.js";
import { kvLogDebug } from "./kvDebug.js";
import { TTL_DEBUG } from "../keys/kvKeys.js";

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

export async function openaiResponsesJsonSchema(env, { system, user, schemaName = "hosei_copy_schema", maxTokens = 400 }) {
  const model = env.OPENAI_MODEL || "gpt-5-mini-2025-08-07";
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
    response_format: { type: "json_schema", json_schema: jsonSchema },
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
