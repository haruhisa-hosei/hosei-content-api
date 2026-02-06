import { json, textOut } from "../core/http.js";

export async function handleDebugLast(url, env) {
  const k = url.searchParams.get("key") || "";
  if (!k || k !== env.ADMIN_USER_ID) return textOut("forbidden", 403);

  const scope = url.searchParams.get("scope") || "general";
  const lastKey = (await env.KV.get(`debug:last:${scope}`)) || (await env.KV.get("debug:last"));
  if (!lastKey) return json({ ok: true, scope, lastKey: null, log: null });

  const raw = await env.KV.get(lastKey);
  let parsed = null;
  try {
    parsed = raw ? JSON.parse(raw) : null;
  } catch {
    parsed = raw || null;
  }
  return json({ ok: true, scope, lastKey, log: parsed });
}
