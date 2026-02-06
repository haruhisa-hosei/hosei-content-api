// src/adapters/openai.js
import { nz, short, safeJsonParse } from "../core/strings.js";
import { kvLogDebug } from "./kvDebug.js";
import { TTL_DEBUG } from "../keys/kvKeys.js";
import { fetchWithTimeout } from "../core/fetchTimeout.js";

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
  // Responses API returns structured output that varies by model and format.
  // Try common fields first, then scan the output tree.

  // 1) output_text (common)
  const ot = nz(data?.output_text).trim();
  if (ot) return ot;

  // 2) text.value (some variants)
  const tv = nz(data?.text?.value ?? data?.text).trim();
  if (tv) return tv;

  // 3) Scan output[] -> content[]
  const out = data?.output;
  if (Array.isArray(out)) {
    for (const item of out) {
      const itText = nz(item?.text ?? item?.output_text).trim();
      if (itText) return itText;

      const contents = item?.content;
      if (Array.isArray(contents)) {
        for (const c of contents) {
          // json_schema responses may come back as "output_json" or "json" payloads
          // depending on model/version.
          if (c && typeof c === "object") {
            const jsonPayload = c?.json ?? c?.output_json ?? c?.parsed ?? null;
            if (jsonPayload && typeof jsonPayload === "object") {
              try {
                return JSON.stringify(jsonPayload);
              } catch {
                // ignore
              }
            }
          }

          const direct = nz(c?.text ?? c?.value).trim();
          if (direct) return direct;

          const nested = nz(c?.text?.value ?? c?.text).trim();
          if (nested) return nested;
        }
      }
    }
  }

  return "";
}

export async function openaiResponsesJsonSchema(env, { system, user, schemaName = "hosei_copy_schema", maxTokens = 400 }) {
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
        // NOTE: The Responses API expects the schema fields at this level
        // (not nested under `json_schema`).
        name: jsonSchema.name,
        strict: jsonSchema.strict,
        schema: jsonSchema.schema,
      },
    },
    max_output_tokens: maxTokens,
  };

  const t0 = Date.now();
  const res = await fetchWithTimeout(
    "https://api.openai.com/v1/responses",
    {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${env.OPENAI_API_KEY}`,
    },
    body: JSON.stringify(body),
    },
    12000
  );

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
