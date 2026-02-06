import { arrayBufferToBase64, timingSafeEqual } from "./cryptoBase64.js";

export async function verifyLineSignature(env, rawBody, signatureB64) {
  if (!env.LINE_CHANNEL_SECRET) return false;
  if (!signatureB64) return false;
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(env.LINE_CHANNEL_SECRET),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const mac = await crypto.subtle.sign("HMAC", key, rawBody);
  const got = arrayBufferToBase64(mac);
  return timingSafeEqual(got, signatureB64);
}
