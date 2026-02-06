export function nz(v) {
  return (v ?? "").toString();
}

export function clampInt(v, def, min, max) {
  const n = parseInt(v ?? "", 10);
  if (Number.isNaN(n)) return def;
  return Math.min(Math.max(n, min), max);
}

export function errorText(e) {
  try {
    return (e && (e.stack || e.message)) || String(e);
  } catch {
    return "unknown error";
  }
}

export function short(s, n = 400) {
  s = nz(s);
  return s.length > n ? s.slice(0, n) + "..." : s;
}

export function safeJsonParse(s) {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}
