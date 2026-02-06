import { kvLogDebug } from "../utils/kvDebug.js";

const OPENAI_API = "https://api.openai.com/v1/responses";

function pickOutputTextFromResponses(data) {
  // Prefer convenience fields if present
  if (typeof data?.output_text === "string" && data.output_text.trim()) {
    return data.output_text;
  }

  // Scan structured output
  const out = data?.output;
  if (Array.isArray(out)) {
    for (const item of out) {
      const content = item?.content;
      if (!Array.isArray(content)) continue;
      for (const c of content) {
        // Common shapes:
        // - { type: "output_text", text: "..." }
        // - { type: "text", text: "..." }
        // - { type: "output_text", output_text: "..." }
        const t =
          (typeof c?.text === "string" && c.text) ||
          (typeof c?.output_text === "string" && c.output_text);
        if (t && String(t).trim()) return String(t);
      }
    }
  }

  // Some SDKs return { output: [{ type: "message", content: [{ text: "..." }]}] }
  // If none matched, return empty and let caller decide.
  return "";
}

export async function openaiResponsesJsonSchema(env, { model, schema, prompt, temperature = 0.2, max_output_tokens = 800 }) {
  if (!env.OPENAI_API_KEY) throw new Error("Missing OPENAI_API_KEY");

  const body = {
    model,
    input: prompt,
    max_output_tokens,
    temperature,
    text: {
      format: {
        type: "json_schema",
        name: "result",
        schema,
        strict: true,
      },
    },
  };

  const res = await fetch(OPENAI_API, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    await kvLogDebug(env, "openai", "openai_failed", { status: res.status, json });
    throw new Error(`OpenAI error: ${res.status} ${JSON.stringify(json)}`);
  }

  const outText = pickOutputTextFromResponses(json);
  if (!outText) {
    await kvLogDebug(env, "openai", "openai_no_output_text", { json });
    throw new Error("OpenAI response has no output text (check model/tool output format)");
  }

  // JSON Schema output is expected to be raw JSON text
  const parsed = JSON.parse(outText);
  return parsed;
}
