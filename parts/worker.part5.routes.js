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

