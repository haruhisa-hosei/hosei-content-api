export async function kvGetJson(env, key) {
  const raw = await env.KV.get(key);
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export async function kvPutJson(env, key, obj, ttlSeconds) {
  await env.KV.put(key, JSON.stringify(obj), { expirationTtl: ttlSeconds });
}
