// src/adapters/lineApi.js
import { kvLogDebug } from "./kvDebug.js";
import { errorText } from "../core/errors.js";

export async function lineReply(env, replyToken, text, fallbackToUserId = "") {
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

    // 返信失敗ログ
    await kvLogDebug(
      env,
      {
        where: "lineReply:failed",
        status: res.status,
        body: t.slice(0, 400),
        textPreview: (text || "").slice(0, 160),
        ts: Date.now(),
      },
      24 * 60 * 60,
      "line"
    );

    // ③ push fallback
    if (fallbackToUserId) {
      try {
        await linePush(env, fallbackToUserId, text);
        await kvLogDebug(
          env,
          { where: "lineReply:failed_but_pushed", status: res.status, ts: Date.now() },
          24 * 60 * 60,
          "line"
        );
        return;
      } catch (e) {
        await kvLogDebug(
          env,
          { where: "lineReply:push_fallback_failed", err: errorText(e), ts: Date.now() },
          24 * 60 * 60,
          "line"
        );
      }
    }

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
