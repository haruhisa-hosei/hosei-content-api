import { nz } from "../core/strings.js";
import { kvLogDebug } from "./kvDebug.js";
import { TTL_DEBUG } from "../keys/kvKeys.js";
import { fetchWithTimeout } from "../core/fetchTimeout.js";

function pickGeminiText(json) {
  const t =
    json?.candidates?.[0]?.content?.parts?.map((p) => nz(p?.text)).join("") ||
    json?.candidates?.[0]?.content?.parts?.[0]?.text ||
    "";
  return nz(t).trim();
}

export async function geminiGenerateText(env, prompt) {
  if (!env.GEMINI_API_KEY) throw new Error("Missing GEMINI_API_KEY");

  // Some model ids (especially *preview) are not enabled for v1beta or generateContent.
  // We'll retry on NOT_FOUND with a known stable default.
  const primaryModel = (env.GEMINI_MODEL || "").trim() || "gemini-2.0-flash";
  const fallbackModel = "gemini-2.0-flash";
  const body = { contents: [{ parts: [{ text: prompt }] }] };

  const t0 = Date.now();
  async function call(model) {
    const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent`;
    return await fetchWithTimeout(
      url,
      {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-goog-api-key": env.GEMINI_API_KEY,
    },
    body: JSON.stringify(body),
      },
      12000
    );
  }

  let usedModel = primaryModel;
  let res = await call(primaryModel);
  const durMs = Date.now() - t0;

  // Retry on 404 (model not found / not supported)
  if (!res.ok && res.status === 404 && primaryModel !== fallbackModel) {
    const t404 = await res.text().catch(() => "");
    await kvLogDebug(
      env,
      {
        where: "geminiGenerateText:http_error",
        status: 404,
        body: t404.slice(0, 800),
        model: primaryModel,
        note: "retry_with_fallback_model",
        durMs,
        ts: Date.now(),
      },
      TTL_DEBUG,
      "gemini"
    );
    // reset timer for retry
    const t1 = Date.now();
    usedModel = fallbackModel;
    res = await call(fallbackModel);
    // update durMs to include retry time
    // (use a new variable to avoid const reassignment above)
    const durMs2 = Date.now() - t1;
    if (!res.ok) {
      const t2 = await res.text().catch(() => "");
      await kvLogDebug(
        env,
        {
          where: "geminiGenerateText:http_error",
          status: res.status,
          body: t2.slice(0, 800),
        model: fallbackModel,
          durMs: durMs2,
          ts: Date.now(),
        },
        TTL_DEBUG,
        "gemini"
      );
      throw new Error(`Gemini error: ${res.status} ${t2.slice(0, 300)}`);
    }
  }

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    await kvLogDebug(
      env,
      {
        where: "geminiGenerateText:http_error",
        status: res.status,
        body: t.slice(0, 800),
        model: usedModel,
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
        model: usedModel,
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
