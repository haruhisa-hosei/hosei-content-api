import { errorText } from "../core/strings.js";
import { kvLogDebug } from "./kvDebug.js";
import { TTL_DEBUG } from "../keys/kvKeys.js";

export async function fetchLineMessageContent(env, messageId) {
  const url = `https://api-data.line.me/v2/bot/message/${messageId}/content`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${env.LINE_CHANNEL_ACCESS_TOKEN}` },
  });
  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`LINE content fetch failed: ${res.status} ${t.slice(0, 200)}`);
  }
  const contentType = res.headers.get("content-type") || "application/octet-stream";
  const bytes = await res.arrayBuffer();
  return { bytes, contentType };
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

export async function lineReply(env, replyToken, text, fallbackToUserId) {
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

    await kvLogDebug(
      env,
      {
        where: "lineReply:failed",
        status: res.status,
        body: t.slice(0, 400),
        textPreview: (text || "").slice(0, 160),
        ts: Date.now(),
      },
      TTL_DEBUG,
      "line"
    );

    if (fallbackToUserId) {
      try {
        await linePush(env, fallbackToUserId, text);
        await kvLogDebug(env, { where: "lineReply:failed_but_pushed", status: res.status, ts: Date.now() }, TTL_DEBUG, "line");
        return;
      } catch (e) {
        await kvLogDebug(env, { where: "lineReply:push_fallback_failed", err: errorText(e), ts: Date.now() }, TTL_DEBUG, "line");
      }
    }

    throw new Error(`LINE reply failed: ${res.status} ${t.slice(0, 200)}`);
  }
}
