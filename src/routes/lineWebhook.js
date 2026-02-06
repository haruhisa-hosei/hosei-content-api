// src/routes/lineWebhook.js
// LINE Messaging API webhook entry

import { textOut } from "../core/http.js";
import { verifyLineSignature } from "../adapters/lineSignature.js";
import { processLineWebhook } from "../services/processLineWebhook.js";

export async function handleLineWebhook(req, env, ctx) {
  if (req.method !== "POST") return textOut("method not allowed", 405);

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

  // Return ASAP; do heavy work async
  try {
    if (ctx?.waitUntil) {
      ctx.waitUntil(processLineWebhook(env, payload));
    } else {
      // Local / non-CF runtimes: fire-and-forget
      processLineWebhook(env, payload).catch(() => {});
    }
  } catch {
    // Never fail webhook due to waitUntil availability
    processLineWebhook(env, payload).catch(() => {});
  }

  return textOut("OK");
}
