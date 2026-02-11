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

