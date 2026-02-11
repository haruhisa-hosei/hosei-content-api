// =============================
// Part 6/6
// =============================

// -----------------------------
// CSV decode + parse (RFC4180-ish)
// -----------------------------
function decodeCSV(arrayBuffer, contentType) {
  const ct = (contentType || "").toLowerCase();
  const m = ct.match(/charset\s*=\s*([^\s;]+)/i);
  if (m) {
    const enc = m[1].replace(/["']/g, "");
    try {
      return new TextDecoder(enc).decode(arrayBuffer);
    } catch {}
  }

  let utf8 = "";
  try {
    utf8 = new TextDecoder("utf-8").decode(arrayBuffer);
  } catch {
    utf8 = "";
  }
  if (utf8 && utf8.charCodeAt(0) === 0xfeff) utf8 = utf8.slice(1);
  const repUtf8 = (utf8.match(/\uFFFD/g) || []).length;
  if (utf8 && repUtf8 === 0) return utf8;

  let sjis = "";
  try {
    sjis = new TextDecoder("shift_jis").decode(arrayBuffer);
  } catch {
    sjis = "";
  }
  const score = (s) => {
    const rep = (s.match(/\uFFFD/g) || []).length;
    const moj = (s.match(/縲|繝|譁|蠕|蟷|豎|迚/g) || []).length;
    return rep * 100 + moj * 3;
  };
  if (!sjis) return utf8 || "";
  return score(sjis) < score(utf8) ? sjis : utf8;
}

function parseCSV(text) {
  if (!text) return [];
  text = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);

  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i++) {
    const c = text[i];

    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else field += c;
    } else {
      if (c === '"') inQuotes = true;
      else if (c === ",") {
        row.push(field);
        field = "";
      } else if (c === "\n") {
        row.push(field);
        rows.push(row);
        row = [];
        field = "";
      } else field += c;
    }
  }
  row.push(field);
  rows.push(row);

  const head = (rows.shift() || []).map((h) => (h || "").trim());
  if (!head.length) return [];

  return rows
    .filter((r) => r.some((v) => (v || "").trim() !== ""))
    .map((cols) => {
      const o = {};
      head.forEach((h, idx) => (o[h] = (cols[idx] ?? "").trim()));
      return o;
    });
}

function toDatePadded(s) {
  const t = nz(s).trim();
  const m = t.match(/^(\d{4})\.(\d{1,2})\.(\d{1,2})$/);
  if (!m) return t;
  const y = m[1];
  const mm = String(parseInt(m[2], 10)).padStart(2, "0");
  const dd = String(parseInt(m[3], 10)).padStart(2, "0");
  return `${y}.${mm}.${dd}`;
}

function pickLegacyKeyFromCsv(type, r, normalizedDate) {
  const date = (normalizedDate ?? nz(r.date)).trim();
  if (type === "archive" && date) return `archive:date:${date}`;

  const id = nz(r.id).trim();
  if (id && date) return `${type}:id:${id}:${date}`;
  if (id) return `${type}:id:${id}`;
  if (date) return `${type}:date:${date}`;
  return `${type}:row:${crypto.randomUUID()}`;
}

// -----------------------------
// Auth helpers
// -----------------------------
function debugAuthorized(req, env) {
  const token = req.headers.get("x-debug-token") || (req.headers.get("authorization") || "").replace(/^Bearer\s+/i, "");
  if (!env.DEBUG_TOKEN) return false;
  return token && token === env.DEBUG_TOKEN;
}
function importAuthorized(req, env) {
  const token = req.headers.get("x-import-token") || (req.headers.get("authorization") || "").replace(/^Bearer\s+/i, "");
  if (!env.IMPORT_TOKEN) return false;
  return token && token === env.IMPORT_TOKEN;
}

export default {
  async fetch(req, env, ctx) {
    if (req.method === "OPTIONS") return new Response("", { headers: withCors() });

    const url = new URL(req.url);

    if (url.pathname === "/health") {
      return json({ ok: true, version: VERSION });
    }

    if (url.pathname === "/debug-last") {
      if (!debugAuthorized(req, env)) return textOut("forbidden", 403);

      const scope = normalizeDebugScope(url.searchParams.get("scope"));
      const lastKey =
        (await env.KV.get(keyDebugLast(scope))) ||
        (await env.KV.get(keyDebugLast("general"))) ||
        (scope === "general" ? await env.KV.get("debug:last") : null);

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

    if (url.pathname.startsWith("/media/") && req.method === "GET") {
      return await handleMedia(url, req, env);
    }

    // Alias: /api/news|voice|archive
    if (/^\/api\/(news|voice|archive)$/.test(url.pathname)) {
      const t = url.pathname.split("/").pop();
      const u = new URL(url.toString());
      u.pathname = "/posts";
      u.searchParams.set("type", t);
      return await handlePosts(u, env);
    }

    if (url.pathname === "/line-webhook" && req.method === "POST") {
      const sig = req.headers.get("x-line-signature") || "";
      const raw = await req.arrayBuffer();

      const okSig = await verifyLineSignature(env, raw, sig);
      if (!okSig) return textOut("bad signature", 401);

      let payload;
      try {
        payload = JSON.parse(new TextDecoder().decode(raw));
      } catch {
        return textOut("bad json", 400);
      }

      try {
        ctx?.waitUntil?.(processLineWebhook(env, payload));
      } catch (e) {
        await kvLogDebug(env, { where: "fetch:waitUntil_failed", err: errorText(e), ts: Date.now() }, TTL_DEBUG, "general");
      }
      return textOut("OK");
    }

    if (url.pathname === "/posts") {
      return await handlePosts(url, env);
    }

    // ✅ /import（必ず token）
    if (url.pathname === "/import") {
      if (!importAuthorized(req, env)) return textOut("forbidden", 403);

      let scanned = 0;
      let inserted = 0;
      let ignored = 0;

      // ✅ DO NOTHING（要件）
      const q = `
        INSERT INTO posts
        (type,date,ja_html,en_html,ja_link_text,ja_link_href,en_link_text,en_link_href,
         image_src,image_kind,enabled,view_date,media_type,media_src,poster_src,
         legacy_key,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))
        ON CONFLICT(legacy_key) DO NOTHING
      `;

      try {
        for (const type of ["news", "voice", "archive"]) {
          const res = await fetch(CSV[type]);
          if (!res.ok) {
            return new Response(`CSV fetch failed: ${type} ${res.status} ${res.statusText}`, {
              status: 502,
              headers: withCors({ "Content-Type": "text/plain; charset=utf-8" }),
            });
          }

          const buf = await res.arrayBuffer();
          const csvText = decodeCSV(buf, res.headers.get("content-type"));
          const rows = parseCSV(csvText);

          for (const r of rows) {
            const dateRaw = nz(r.date).trim();
            if (!dateRaw) continue;

            const date = type === "archive" ? toDatePadded(dateRaw) : dateRaw;
            scanned++;

            const ja_html = nz(r.ja_html) || (type === "archive" ? nz(r.title_ja) : "");
            const en_html = nz(r.en_html) || (type === "archive" ? nz(r.title_en) : "");

            const ja_link_text = nz(r.ja_link_text);
            const ja_link_href = nz(r.ja_link_href);
            const en_link_text = nz(r.en_link_text);
            const en_link_href = nz(r.en_link_href);

            const image_src = nz(r.image_src).trim() || null;
            const image_kind = nz(r.image_kind).trim() || null;

            const enabled = normalizeBoolTextDefaultTrue(r.enabled);

            const view_date = nz(r.view_date).trim() || viewDateFromPadded(date) || null;

            const media_type = nz(r.media_type).trim() || "image";
            const media_src = nz(r.media_src).trim() || null;
            const poster_src = nz(r.poster_src).trim() || null;

            const legacy_key = pickLegacyKeyFromCsv(type, r, date);

            const out = await env.DB.prepare(q)
              .bind(
                type,
                date,
                ja_html,
                en_html,
                ja_link_text,
                ja_link_href,
                en_link_text,
                en_link_href,
                image_src,
                image_kind,
                enabled,
                view_date,
                media_type,
                media_src,
                poster_src,
                legacy_key
              )
              .run();

            const changes = out?.meta?.changes ?? 0;
            if (changes > 0) inserted++;
            else ignored++;
          }
        }

        await kvLogDebug(env, { where: "import:done", scanned, inserted, ignored, ts: Date.now() }, TTL_DEBUG, "db");
        return json({ scanned, inserted, ignored });
      } catch (e) {
        await kvLogDebug(env, { where: "import:error", err: errorText(e), ts: Date.now() }, TTL_DEBUG, "db");
        return new Response(`import error: ${errorText(e)}`, {
          status: 500,
          headers: withCors({ "Content-Type": "text/plain; charset=utf-8" }),
        });
      }
    }

    return textOut("hosei api alive");
  },
};