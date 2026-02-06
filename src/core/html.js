import { nz } from "./strings.js";

export function escapeHtml(s) {
  return nz(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

export function wrapIfVoiceSpan(type, htmlOrText) {
  const t = nz(htmlOrText).trim();
  if (type === "voice") {
    if (/^<span>[\s\S]*<\/span>$/.test(t)) return t;
    return `<span>${escapeHtml(t)}</span>`;
  }
  return t;
}
