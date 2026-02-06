// src/core/errors.js
export function errorText(e) {
  try {
    return (e && (e.stack || e.message)) || String(e);
  } catch {
    return "unknown error";
  }
}
