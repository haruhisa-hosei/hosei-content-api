// src/routes/lineWebhook.js
import { verifyLineSignature } from "../adapters/lineSignature.js";
import { processLineWebhook } from "../services/processLineWebhook.js";

export async function handleLineWebhook(req, env, ctx) {
  const sig = req.headers.get("x-line-signature") || "";
  const raw = await req.arrayBuffer();

  const okSig = await verifyLineSignature(env, raw, sig);
  if (!okSig) return new Response("bad signature", { status: 401 });

  let payload;
  try {
    payload = JSON.parse(new TextDecoder().decode(raw));
  } catch {
    return new Response("bad json", { status: 400 });
  }

  try {
    ctx?.waitUntil?.(processLineWebhook(env, payload));
  } catch {
    processLineWebhook(env, payload);
  }

  return new Response("OK");
}
