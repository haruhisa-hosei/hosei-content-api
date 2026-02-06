// src/services/processLineWebhook.js
import { TTL_DEBUG, TTL_PENDING, TTL_EDITING } from "../config.js";

import { detectTypeAndContent, parseDeleteIds, parseEditCancel, parseEditEnd, parseEditFieldUpdate, parseEditStart } from "../core/commands.js";
import { todayJstDatePadded, extractDatePadded, viewDateFromPadded } from "../core/dates.js";
import { extractUrl } from "../core/urls.js";
import { nz, short } from "../core/strings.js";
import { escapeHtml } from "../core/html.js";
import { extFromContentType } from "../core/media.js";
import { errorText } from "../core/errors.js";

import { kvLogDebug } from "../adapters/kvDebug.js";
import { kvGetJson, kvPutJson } from "../adapters/kvJson.js";

import { lineReply } from "../adapters/lineApi.js";
import { fetchLineMessageContent } from "../adapters/lineContent.js";
import { uploadImageToGitHub } from "../adapters/github.js";

import { r2KeyForVideo, r2KeyForPoster } from "../adapters/r2.js";
import { generateJaEn } from "./generateJaEn.js";

import { insertPost, getPostById, updatePostFields, softDeleteMany } from "../adapters/db.js";
import { keyPendingImage, keyPendingVideo, keyEditing } from "../keys/kvKeys.js";

function wrapIfVoiceSpan(type, htmlOrText) {
  const t = nz(htmlOrText).trim();
  if (type === "voice") {
    if (/^<span>[\s\S]*<\/span>$/.test(t)) return t;
    return `<span>${escapeHtml(t)}</span>`;
  }
  return t;
}

export async function processLineWebhook(env, payload) {
  const events = payload?.events || [];

  for (const event of events) {
    try {
      const userId = event?.source?.userId;
      if (!userId) continue;

      // 管理者以外は無視
      if (env.ADMIN_USER_ID && userId !== env.ADMIN_USER_ID) continue;

      const replyToken = event?.replyToken;
      const msg = event?.message;
      if (!msg?.type) continue;

      // -------------------------
      // image
      // -------------------------
      if (msg.type === "image") {
        const pendingVideo = await kvGetJson(env, keyPendingVideo(userId));

        // 動画の「サムネ待ち」なら、これは poster として R2 に保存
        if (pendingVideo?.stage === "await_poster" && pendingVideo?.video_message_id) {
          const { bytes, contentType } = await fetchLineMessageContent(env, msg.id);
          const ext = extFromContentType(contentType);
          const posterKey = r2KeyForPoster(userId, pendingVideo.video_message_id, ext);

          await env.R2.put(posterKey, bytes, { httpMetadata: { contentType } });

          pendingVideo.stage = "await_text";
          pendingVideo.poster_key = posterKey;
          await kvPutJson(env, keyPendingVideo(userId), pendingVideo, TTL_PENDING);

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

        // 通常の画像（GitHubへ）
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

      // -------------------------
      // video -> R2
      // -------------------------
      if (msg.type === "video") {
        const { bytes, contentType } = await fetchLineMessageContent(env, msg.id);
        const videoKey = r2KeyForVideo(userId, msg.id);

        await env.R2.put(videoKey, bytes, {
          httpMetadata: { contentType: contentType || "video/mp4" },
        });

        await kvPutJson(
          env,
          keyPendingVideo(userId),
          { stage: "await_poster", video_key: videoKey, video_message_id: msg.id },
          TTL_PENDING
        );

        if (replyToken) {
          await lineReply(env, replyToken, "🎥 動画を受け取りました。続けてサムネ画像を送ってください。", userId);
        }
        continue;
      }

      // -------------------------
      // text (AI生成 + DB保存 + reply失敗→push救済)
      // -------------------------
      if (msg.type === "text") {
        const text = nz(msg.text).trim();

        // ---- 編集：終了/取消 ----
        if (parseEditEnd(text)) {
          await env.KV.delete(keyEditing(userId));
          if (replyToken) await lineReply(env, replyToken, "✅ 編集モードを終了しました。", userId);
          continue;
        }
        if (parseEditCancel(text)) {
          await env.KV.delete(keyEditing(userId));
          if (replyToken) await lineReply(env, replyToken, "🟡 編集をキャンセルしました。", userId);
          continue;
        }

        // ---- 編集：開始 ----
        const editId = parseEditStart(text);
        if (editId) {
          const row = await getPostById(env, editId);
          if (!row) {
            if (replyToken) await lineReply(env, replyToken, `⚠️ ID:${editId} が見つかりませんでした。`, userId);
            continue;
          }

          await env.KV.put(keyEditing(userId), JSON.stringify({ id: row.id, type: row.type }), { expirationTtl: TTL_EDITING });

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

        // ---- 編集中：フィールド更新 ----
        const editing = await kvGetJson(env, keyEditing(userId));
        const upd = parseEditFieldUpdate(text);

        if (editing && upd) {
          const row = await getPostById(env, editing.id);
          if (!row) {
            await env.KV.delete(keyEditing(userId));
            if (replyToken) await lineReply(env, replyToken, "⚠️ 対象が消えました。編集モードを解除しました。", userId);
            continue;
          }

          let ok = false;
          if (upd.field === "JA") ok = await updatePostFields(env, row.id, { ja_html: wrapIfVoiceSpan(row.type, upd.value) });
          else if (upd.field === "EN") ok = await updatePostFields(env, row.id, { en_html: wrapIfVoiceSpan(row.type, upd.value) });
          else if (upd.field === "BTNJA") ok = await updatePostFields(env, row.id, { ja_link_text: upd.value });
          else if (upd.field === "BTNEN") ok = await updatePostFields(env, row.id, { en_link_text: upd.value });

          if (replyToken) {
            await lineReply(env, replyToken, ok ? `✅ ${upd.field} を更新しました (ID:${row.id})` : `⚠️ 更新できませんでした (ID:${row.id})`, userId);
          }
          continue;
        }

        // ---- 削除（単発/複数/範囲） ----
        const delIds = parseDeleteIds(text);
        if (delIds) {
          const n = await softDeleteMany(env, delIds);
          if (replyToken) await lineReply(env, replyToken, `🗑️ 非表示にしました：${n}/${delIds.length} 件\n(${delIds.join(", ")})`, userId);
          continue;
        }

        // ---- 通常投稿ロジック ----
        const { type, content } = detectTypeAndContent(text);
        const date = extractDatePadded(content) || todayJstDatePadded();
        const urlInText = extractUrl(content);
        const contentNoUrl = urlInText ? content.replace(urlInText, "").trim() : content;

        // ① AI生成（OpenAI json_schema main + Gemini補完）
        let ai;
        try {
          ai = await generateJaEn(env, contentNoUrl, type === "news");
        } catch (e) {
          await kvLogDebug(
            env,
            { where: "generateJaEn:failed", err: errorText(e), type, contentPreview: short(contentNoUrl, 200), ts: Date.now() },
            TTL_DEBUG,
            "general"
          );
          ai = { ja: contentNoUrl, en: "", btnJa: "詳細を見る", btnEn: "View Details" };
        }

        // pending image (GitHub)
        const pendingImage = await env.KV.get(keyPendingImage(userId));
        if (pendingImage) await env.KV.delete(keyPendingImage(userId));

        // pending video (R2)
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

        // row build
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
          // VOICE（V: 付いても付かなくても voice / 本文から V: は除去済み）
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
          legacy_key: `${type}:${date}:${crypto.randomUUID()}`,
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
      await kvLogDebug(
        env,
        {
          where: "processLineWebhook:event_catch",
          err: errorText(e),
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
