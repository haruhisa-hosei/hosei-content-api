import { textOut } from "../core/http.js";
import { verifyLineSignature } from "../core/lineSignature.js";
import { kvLogDebug } from "../adapters/kvDebug.js";
import { errorText } from "../core/errors.js";

import processLineWebhook from "../services/processLineWebhook.js";

export async function handleLineWebhook(req, env, ctx) {
  const sig = req.headers.get("x-line-signature") || "";
  const raw = await req.arrayBuffer();

  const okSig = await verifyLineSignature(env, raw, sig);
  if (!okSig) return textOut("bad signature", 401);

  let payload;
  try {
    payload = JSON.parse(new TextDecoder().decode(raw));
  } catch {
    return textOut("bad json", 400);
  }

  // 即 200 を返して、裏で処理
  try {
    ctx?.waitUntil?.(processLineWebhook(env, payload));
  } catch (e) {
    await kvLogDebug(env, { where: "handleLineWebhook:waitUntil_failed", err: errorText(e), ts: Date.now() }, 86400, "general");
  }

  return textOut("OK");
}
