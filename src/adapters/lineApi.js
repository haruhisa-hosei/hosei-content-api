// src/adapters/lineApi.js
import { kvLogDebug } from "./kvDebug.js";
import { TTL_DEBUG } from "../keys/kvKeys.js";
import { errorText } from "../core/errors.js";

export async function lineReply(env, replyToken, text) {
  const url = "https://api.line.me/v2/bot/message/reply";
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${env.LINE_CHANNEL_ACCESS_TOKEN}`,
    },
    body: JSON.stringify({
      replyToken,
      messages: [{ type: "text", text }],
    }),
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`LINE reply failed: ${res.status} ${t.slice(0, 200)}`);
  }
}

export async function linePush(env, to, text) {
  const url = "https://api.line.me/v2/bot/message/push";
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${env.LINE_CHANNEL_ACCESS_TOKEN}`,
    },
    body: JSON.stringify({
      to,
      messages: [{ type: "text", text }],
    }),
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`LINE push failed: ${res.status} ${t.slice(0, 200)}`);
  }
}

/**
 * ✅ ③ push fallback 付き返信
 * replyToken が死んでる/期限切れ等で reply が落ちたら push で救済
 */
export async function lineReplyWithFallback(env, replyToken, text, fallbackToUserId) {
  try {
    await lineReply(env, replyToken, text);
  } catch (e) {
    await kvLogDebug(
      env,
      {
        where: "lineReplyWithFallback:reply_failed",
        err: errorText(e),
        textPreview: (text || "").slice(0, 160),
        ts: Date.now(),
      },
      TTL_DEBUG,
      "line"
    );

    if (!fallbackToUserId) throw e;

    try {
      await linePush(env, fallbackToUserId, text);
      await kvLogDebug(
        env,
        { where: "lineReplyWithFallback:pushed", ts: Date.now() },
        TTL_DEBUG,
        "line"
      );
    } catch (e2) {
      await kvLogDebug(
        env,
        { where: "lineReplyWithFallback:push_failed", err: errorText(e2), ts: Date.now() },
        TTL_DEBUG,
        "line"
      );
      throw e2;
    }
  }
}
