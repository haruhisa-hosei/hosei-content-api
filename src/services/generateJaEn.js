import { nz, short, errorText } from "../core/strings.js";
import { kvLogDebug } from "../adapters/kvDebug.js";
import { TTL_DEBUG } from "../keys/kvKeys.js";
import { openaiResponsesJsonSchema } from "../adapters/openai.js";
import { geminiTranslateEn } from "../adapters/gemini.js";

export async function generateJaEn(env, content, forNews) {
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

    if (!en || en.length < 4) {
      try {
        const en2 = await geminiTranslateEn(env, ja);
        if (en2) en = en2;
        await kvLogDebug(env, { where: "gemini:fallback_en", ts: Date.now() }, TTL_DEBUG, "gemini");
      } catch (e) {
        await kvLogDebug(env, { where: "gemini:fallback_en_failed", err: errorText(e), ts: Date.now() }, TTL_DEBUG, "gemini");
      }
    }

    await kvLogDebug(
      env,
      { where: "generateJaEn:ok", used: "openai_main(+gemini_if_needed)", jaPreview: short(ja, 120), enPreview: short(en, 120), ts: Date.now() },
      TTL_DEBUG,
      "general"
    );
    return { ja, en, btnJa, btnEn };
  } catch (e) {
    await kvLogDebug(env, { where: "generateJaEn:openai_failed", err: errorText(e), ts: Date.now() }, TTL_DEBUG, "openai");
  }

  try {
    en = (await geminiTranslateEn(env, ja)) || "";
    await kvLogDebug(env, { where: "generateJaEn:gemini_only", ts: Date.now() }, TTL_DEBUG, "gemini");
  } catch (e) {
    await kvLogDebug(env, { where: "generateJaEn:gemini_only_failed", err: errorText(e), ts: Date.now() }, TTL_DEBUG, "gemini");
  }

  return { ja, en, btnJa, btnEn };
}
