import { keyDebug, keyDebugLast, TTL_DEBUG } from "../keys/kvKeys.js";

export async function kvLogDebug(env, payload, ttl = TTL_DEBUG, scope = "general") {
  try {
    const k = keyDebug(scope);
    await env.KV.put(k, JSON.stringify(payload), { expirationTtl: ttl });
    await env.KV.put(keyDebugLast(scope), k, { expirationTtl: ttl });
    await env.KV.put("debug:last", k, { expirationTtl: ttl });
    return k;
  } catch {
    return null;
  }
}
