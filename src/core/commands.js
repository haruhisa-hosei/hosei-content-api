import { nz } from "./strings.js";

export function normalizeBoolTextDefaultTrue(v) {
  const s = nz(v).trim().toLowerCase();
  if (!s) return "TRUE";
  if (s === "true" || s === "1" || s === "yes") return "TRUE";
  if (s === "false" || s === "0" || s === "no") return "FALSE";
  return s.toUpperCase() === "FALSE" ? "FALSE" : "TRUE";
}

export function detectTypeAndContent(text) {
  const t = nz(text).trim();
  let type = "voice";

  if (/^(ニュース|ニュース：|N：|N:|に：|に:)/i.test(t)) type = "news";
  else if (/^(アーカイブ|アーカイブ：|A：|A:|あ：|あ:)/i.test(t)) type = "archive";
  else if (/^(V：|V:|v：|v:|ボイス|voice|VOICE)[:：\s]/.test(t)) type = "voice";

  const content = t
    .replace(
      /^(ニュース|アーカイブ|ボイス|VOICE|voice|ニュース：|アーカイブ：|N：|A：|V：|N:|A:|V:|に：|あ：|に:|あ:|v：|v:)\s*[:：]?\s*/i,
      ""
    )
    .trim();

  return { type, content };
}

export function parseDeleteIds(text) {
  const s = nz(text)
    .replace(/[「」『』"]/g, "")
    .trim()
    .replace(/\s+/g, " ");

  const m = s.match(/^(削除|消去|さ)\s*[:：]\s*(?:id\s*[:：]\s*)?(.+)$/i);
  if (!m) return null;

  const rest = (m[2] || "").trim();

  const r = rest.match(/^(\d+)\s*-\s*(\d+)$/);
  if (r) {
    const a = parseInt(r[1], 10);
    const b = parseInt(r[2], 10);
    if (Number.isFinite(a) && Number.isFinite(b)) {
      const lo = Math.min(a, b);
      const hi = Math.max(a, b);
      const ids = [];
      for (let i = lo; i <= hi; i++) ids.push(i);
      return ids;
    }
  }

  const parts = rest.split(/[,\s]+/).filter(Boolean);
  const ids = parts
    .map((x) => parseInt(x.replace(/[^0-9]/g, ""), 10))
    .filter((n) => Number.isFinite(n) && n > 0);

  return ids.length ? Array.from(new Set(ids)) : null;
}

export function parseEditStart(text) {
  const s = nz(text).trim().replace(/\s+/g, "");
  const m = s.match(/^編集[:：](\d+)$/);
  return m ? m[1] : null;
}
export function parseEditEnd(text) {
  const s = nz(text).trim();
  return /^(完了|終了|end)$/i.test(s);
}
export function parseEditCancel(text) {
  const s = nz(text).trim();
  return /^(取消|キャンセル|中止|cancel)$/i.test(s);
}
export function parseEditFieldUpdate(text) {
  const s = nz(text).trim();
  const m = s.match(/^(JA|EN|BTNJA|BTNEN)\s*[:：]\s*([\s\S]+)$/i);
  if (!m) return null;
  return { field: m[1].toUpperCase(), value: (m[2] || "").trim() };
}
