// src/adapters/lineContent.js
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
