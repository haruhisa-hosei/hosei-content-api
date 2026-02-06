import { nz } from "./strings.js";

/**
 * decode CSV: supports UTF-8 and UTF-16LE when detected.
 */
export function decodeCSV(buf, contentType) {
  const u8 = new Uint8Array(buf);

  // UTF-16LE BOM
  if (u8.length >= 2 && u8[0] === 0xff && u8[1] === 0xfe) {
    return new TextDecoder("utf-16le").decode(u8);
  }
  // UTF-16BE BOM
  if (u8.length >= 2 && u8[0] === 0xfe && u8[1] === 0xff) {
    return new TextDecoder("utf-16be").decode(u8);
  }

  // heuristic: if lots of 0x00 bytes, likely UTF-16
  let zeros = 0;
  for (let i = 0; i < Math.min(u8.length, 2000); i++) if (u8[i] === 0) zeros++;
  if (zeros > 20) {
    // assume LE (Google often)
    return new TextDecoder("utf-16le").decode(u8);
  }

  return new TextDecoder("utf-8").decode(u8);
}

/**
 * Basic CSV parser with quotes.
 * Returns array of objects keyed by header row.
 */
export function parseCSV(csvText) {
  const s = nz(csvText).replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  const lines = splitCsvLines(s);
  if (!lines.length) return [];

  const header = parseCsvRow(lines[0]).map((h) => nz(h).trim());
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const cols = parseCsvRow(lines[i]);
    if (!cols.length || cols.every((c) => !nz(c).trim())) continue;

    const obj = {};
    for (let j = 0; j < header.length; j++) {
      obj[header[j]] = cols[j] ?? "";
    }
    rows.push(obj);
  }
  return rows;
}

function splitCsvLines(s) {
  const out = [];
  let cur = "";
  let inQ = false;

  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '"') {
      const next = s[i + 1];
      if (inQ && next === '"') {
        cur += '"';
        i++;
      } else {
        inQ = !inQ;
      }
      continue;
    }
    if (!inQ && ch === "\n") {
      out.push(cur);
      cur = "";
      continue;
    }
    cur += ch;
  }
  if (cur) out.push(cur);
  return out;
}

function parseCsvRow(line) {
  const out = [];
  let cur = "";
  let inQ = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      const next = line[i + 1];
      if (inQ && next === '"') {
        cur += '"';
        i++;
      } else {
        inQ = !inQ;
      }
      continue;
    }
    if (!inQ && ch === ",") {
      out.push(cur);
      cur = "";
      continue;
    }
    cur += ch;
  }
  out.push(cur);
  return out;
}
