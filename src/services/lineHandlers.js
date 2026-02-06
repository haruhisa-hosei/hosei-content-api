// src/services/lineHandlers.js
import { lineReplyWithFallback } from "../adapters/lineApi.js";
import { fetchLineMessageContent } from "../adapters/lineContent.js";
import { uploadImageToGitHub, extFromContentType } from "../adapters/github.js";
import { r2KeyForVideo, r2KeyForPoster } from "../adapters/r2.js";
import { kvLogDebug } from "../adapters/kvDebug.js";

import { generateJaEn } from "./generateJaEn.js";

import {
  insertPost,
  getPostById,
  updatePostFields,
  softDeleteMany,
} from "../adapters/db.js";

import {
  detectTypeAndContent,
  parseDeleteIds,
  parseEditCancel,
  parseEditEnd,
  parseEditFieldUpdate,
  parseEditStart,
} from "../core/commands.js";

import { escapeHtml } from "../core/html.js";
import { extractUrl } from "../core/urls.js";
import { todayJstDatePadded, extractDatePadded, viewDateFromPadded } from "../core/dates.js";
import { nz, short } from "../core/strings.js";

import {
  TTL_PENDING,
  TTL_EDITING,
  keyPendingImage,
  keyPendingVideo,
  keyEditing,
  TTL_DEBUG,
} from "../keys/kvKeys.js";

async function kvGetJson(env, key) {
  const raw = await env.KV.get(key);
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}
async function kvPutJson(env, key, obj, ttl) {
  await env.KV.put(key, JSON.stringify(obj), { expirationTtl: ttl });
}

function wrapIfVoiceSpan(type, htmlOrText) {
  const t = nz(htmlOrText).trim();
  if (type === "voice") {
    if (/^<span>[\s\S]*<\/span>$/.test(t)) return t;
    return `<span>${escapeHtml(t)}</span>`;
  }
  return t;
}

export async function handleLineImageEvent(env, event) {
  const userId = event?.source?.userId;
  const replyToken = event?.replyToken;
  const msg = event?.message;
  if (!userId || !msg?.id) return;

  // 動画フロー中（poster待ち）ならR2 posterとして保存
  const pendingVideo = await kvGetJson(env, keyPendingVideo(userId));
  if (pendingVideo?.stage === "await_poster" && pendingVideo?.video_message_id) {
    const { bytes, contentType } = await fetchLineMessageContent(env, msg.id);
    const ext = extFromContentType(contentType);
    const posterKey = r2KeyForPoster(userId, pendingVideo.video_message_id, ext);

    await env.R2.put(posterKey, bytes, { httpMetadata: { contentType } });

    pendingVideo.stage = "await_text";
    pendingVideo.poster_key = posterKey;
    await kvPutJson(env, keyPendingVideo(userId), pendingVideo, TTL_PENDING);

    if (replyToken) {
      await lineReplyWithFallback(
        env,
        replyToken,
        "🖼 サムネを受け取りました。続けて本文（N:/に: / V: / A:/あ:）を送ってください。",
        userId
      );
    }
    return;
  }

  // 通常：画像をGitHubへ
  const { bytes, contentType } = await fetchLineMessageContent(env, msg.id);
  const fileName = await uploadImageToGitHub(env, { bytes, contentType, messageId: msg.id });

  await env.KV.put(keyPendingImage(userId), fileName, { expirationTtl: TTL_PENDING });

  if (replyToken) {
    await lineReplyWithFallback(
      env,
      replyToken,
      `📷 画像を保存しました（${fileName}）。続けて本文（に:/N:/A:/あ:/V:）を送ってください。`,
      userId
    );
  }
}

export async function handleLineVideoEvent(env, event) {
  const userId = event?.source?.userId;
  const replyToken = event?.replyToken;
  const msg = event?.message;
  if (!userId || !msg?.id) return;

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
    await lineReplyWithFallback(env, replyToken, "🎥 動画を受け取りました。続けてサムネ画像を送ってください。", userId);
  }
}

export async function handleLineTextEvent(env, event, text) {
  const userId = event?.source?.userId;
  const replyToken = event?.replyToken;
  const t = nz(text).trim();
  if (!userId || !replyToken) return;

  // ---- 編集: 終了/取消 ----
  if (parseEditEnd(t)) {
    await env.KV.delete(keyEditing(userId));
    await lineReplyWithFallback(env, replyToken, "✅ 編集モードを終了しました。", userId);
    return;
  }
  if (parseEditCancel(t)) {
    await env.KV.delete(keyEditing(userId));
    await lineReplyWithFallback(env, replyToken, "🟡 編集をキャンセルしました。", userId);
    return;
  }

  // ---- 編集: 開始 ----
  const editId = parseEditStart(t);
  if (editId) {
    const row = await getPostById(env, editId);
    if (!row) {
      await lineReplyWithFallback(env, replyToken, `⚠️ ID:${editId} が見つかりませんでした。`, userId);
      return;
    }

    await env.KV.put(keyEditing(userId), JSON.stringify({ id: row.id, type: row.type }), { expirationTtl: TTL_EDITING });

    const msgOut =
      `✏️ 編集モード (ID:${row.id} / ${String(row.type).toUpperCase()})\n\n` +
      `JA:\n${nz(row.ja_html)}\n\n` +
      `EN:\n${nz(row.en_html)}\n\n` +
      `修正はこう送ってください：\n` +
      `JA: ... / EN: ... / BTNJA: ... / BTNEN: ...\n` +
      `終わるとき：完了　やめる：取消`;

    await lineReplyWithFallback(env, replyToken, msgOut, userId);
    return;
  }

  // ---- 編集中のフィールド更新 ----
  const editing = await kvGetJson(env, keyEditing(userId));
  const upd = parseEditFieldUpdate(t);
  if (editing && upd) {
    const row = await getPostById(env, editing.id);
    if (!row) {
      await env.KV.delete(keyEditing(userId));
      await lineReplyWithFallback(env, replyToken, "⚠️ 対象が消えました。編集モードを解除しました。", userId);
      return;
    }

    let ok = false;
    if (upd.field === "JA") ok = await updatePostFields(env, row.id, { ja_html: wrapIfVoiceSpan(row.type, upd.value) });
    else if (upd.field === "EN") ok = await updatePostFields(env, row.id, { en_html: wrapIfVoiceSpan(row.type, upd.value) });
    else if (upd.field === "BTNJA") ok = await updatePostFields(env, row.id, { ja_link_text: upd.value });
    else if (upd.field === "BTNEN") ok = await updatePostFields(env, row.id, { en_link_text: upd.value });

    await lineReplyWithFallback(env, replyToken, ok ? `✅ ${upd.field} を更新しました (ID:${row.id})` : `⚠️ 更新できませんでした (ID:${row.id})`, userId);
    return;
  }

  // ---- 削除（単発/複数/範囲） ----
  const delIds = parseDeleteIds(t);
  if (delIds) {
    const n = await softDeleteMany(env, delIds);
    await lineReplyWithFallback(env, replyToken, `🗑️ 非表示にしました：${n}/${delIds.length} 件\n(${delIds.join(", ")})`, userId);
    return;
  }

  // ---- 通常投稿 ----
  const { type, content } = detectTypeAndContent(t);
  const date = extractDatePadded(content) || todayJstDatePadded();

  const urlInText = extractUrl(content);
  const contentNoUrl = urlInText ? content.replace(urlInText, "").trim() : content;

  // ① AI生成（OpenAI→Gemini→最終フォールバック）
  let ai = null;
  try {
    ai = await generateJaEn(env, contentNoUrl, type === "news");
  } catch (e) {
    await kvLogDebug(
      env,
      { where: "handleLineTextEvent:generate_failed", err: errorText(e), type, contentPreview: short(contentNoUrl, 200), ts: Date.now() },
      TTL_DEBUG,
      "general"
    );
    ai = { ja: contentNoUrl, en: "", btnJa: "詳細を見る", btnEn: "View Details" };
  }

  const pendingImage = await env.KV.get(keyPendingImage(userId));
  if (pendingImage) await env.KV.delete(keyPendingImage(userId));

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
    // VOICE（仕様維持：spanで包む）
    ja_html = `<span>${escapeHtml(ai.ja)}</span>`;
    en_html = `<span>${escapeHtml(ai.en)}</span>`;
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
  };

  const newId = await insertPost(env, row);

  await lineReplyWithFallback(
    env,
    replyToken,
    `✅ 更新完了 (ID: ${newId ?? "?"})\n[${type.toUpperCase()}] ${contentNoUrl.slice(0, 20)}${contentNoUrl.length > 20 ? "..." : ""}`,
    userId
  );
}
