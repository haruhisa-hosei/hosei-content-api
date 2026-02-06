// src/adapters/lineApi.js
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
