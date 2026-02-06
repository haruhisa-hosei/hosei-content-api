import { kvLogDebug } from "../utils/kvDebug.js";

const LINE_API = "https://api.line.me/v2/bot";

async function lineApiFetch(env, path, body) {
  if (!env.LINE_CHANNEL_ACCESS_TOKEN) {
    throw new Error("Missing LINE_CHANNEL_ACCESS_TOKEN (Cloudflare Worker secret)");
  }

  const res = await fetch(`${LINE_API}${path}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.LINE_CHANNEL_ACCESS_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  const text = await res.text();
  if (!res.ok) throw new Error(`LINE API ${res.status}: ${text}`);
  return text ? JSON.parse(text) : { ok: true };
}

export async function lineReply(env, replyToken, messages) {
  return lineApiFetch(env, "/message/reply", { replyToken, messages });
}

export async function linePush(env, to, messages) {
  return lineApiFetch(env, "/message/push", { to, messages });
}

// reply失敗時にpushでフォールバック（replyToken失効対策）
// 宛先は ADMIN_USER_ID を優先（あなたの運用では管理者＝自分）
export async function lineReplyWithFallback(env, replyToken, userId, messages) {
  try {
    const r = await lineReply(env, replyToken, messages);
    await kvLogDebug(env, "line", "lineReply_ok", { ok: true });
    return r;
  } catch (err) {
    await kvLogDebug(env, "line", "lineReply_failed", String(err && (err.stack || err.message || err)));

    const to = env.ADMIN_USER_ID || userId;
    if (to) {
      try {
        const r2 = await linePush(env, to, messages);
        await kvLogDebug(env, "line", "linePush_fallback_ok", { ok: true, to: to.slice(0, 6) + "..." });
        return r2;
      } catch (err2) {
        await kvLogDebug(env, "line", "linePush_fallback_failed", String(err2 && (err2.stack || err2.message || err2)));
      }
    }

    throw err;
  }
}
