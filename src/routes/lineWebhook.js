import { verifyLineSignature } from "../utils/lineSignature.js";
import { processLineWebhook } from "../services/processLineWebhook.js";

export async function handleLineWebhook(req, env, ctx) {
  const bodyRaw = await req.text();

  const ok = verifyLineSignature(bodyRaw, req.headers.get("x-line-signature"), env.LINE_CHANNEL_SECRET);
  if (!ok) return new Response("Bad signature", { status: 401 });

  let payload;
  try {
    payload = JSON.parse(bodyRaw);
  } catch {
    return new Response("Bad JSON", { status: 400 });
  }

  // NOTE:
  // - waitUntilに投げると replyToken が失効しやすく、LINE返信が「返らない」に見える
  // - デフォルトは inline 実行にして、必要なら USE_WAITUNTIL=1 で切替可能にする
  const useWaitUntil = String(env.USE_WAITUNTIL || "0") === "1";

  if (useWaitUntil) {
    try {
      ctx?.waitUntil?.(processLineWebhook(env, payload));
    } catch {
      processLineWebhook(env, payload);
    }
    return new Response("OK");
  }

  // Inline processing (default): keeps replyToken valid and makes debugging easier.
  await processLineWebhook(env, payload);
  return new Response("OK");
}
