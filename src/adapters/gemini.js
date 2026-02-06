import { nz } from "../core/strings.js";
import { kvLogDebug } from "./kvDebug.js";
import { TTL_DEBUG } from "../keys/kvKeys.js";

function pickGeminiText(json) {
  const t =
    json?.candidates?.[0]?.content?.parts?.map((p) => nz(p?.text)).join("") ||
    json?.candidates?.[0]?.content?.parts?.[0]?.text ||
    "";
  return nz(t).trim();
}

export async function geminiGenerateText(env, prompt) {
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

export async function geminiTranslateEn(env, jaText) {
  const prompt = `Translate the following Japanese into natural English for a website (concise, no extra commentary). Output ONLY the English text.\n\nJapanese:\n${jaText}`;
  const en = await geminiGenerateText(env, prompt);
  return en.trim();
}
