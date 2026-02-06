import { nz } from "./strings.js";

export function todayJstDatePadded() {
  const now = new Date(Date.now() + 9 * 60 * 60 * 1000);
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  return `${y}.${m}.${d}`;
}

export function extractDatePadded(content) {
  const c = nz(content);
  const m = c.match(/(?:(\d{4})[.\/年])?(\d{1,2})[.\/月](\d{1,2})日?/);
  if (!m) return null;
  const y = m[1] ? String(m[1]) : String(new Date(Date.now() + 9 * 60 * 60 * 1000).getUTCFullYear());
  const mo = String(parseInt(m[2], 10)).padStart(2, "0");
  const da = String(parseInt(m[3], 10)).padStart(2, "0");
  return `${y}.${mo}.${da}`;
}

export function viewDateFromPadded(padded) {
  const m = nz(padded).match(/^(\d{4})\.(\d{2})\.(\d{2})$/);
  if (!m) return padded || null;
  const y = m[1];
  const mo = String(parseInt(m[2], 10));
  const da = String(parseInt(m[3], 10));
  return `${y}.${mo}.${da}`;
}

export function toDatePadded(s) {
  const t = nz(s).trim();
  const m = t.match(/^(\d{4})\.(\d{1,2})\.(\d{1,2})$/);
  if (!m) return t;
  const y = m[1];
  const mm = String(parseInt(m[2], 10)).padStart(2, "0");
  const dd = String(parseInt(m[3], 10)).padStart(2, "0");
  return `${y}.${mm}.${dd}`;
}
