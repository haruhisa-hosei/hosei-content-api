import { CSV } from "../config.js";
import { withCors, json } from "../core/http.js";
import { decodeCSV, parseCSV } from "../core/csv.js";
import { nz, errorText } from "../core/strings.js";
import { normalizeBoolTextDefaultTrue } from "../core/commands.js";
import { toDatePadded, viewDateFromPadded } from "../core/dates.js";
import { kvLogDebug } from "../adapters/kvDebug.js";
import { TTL_DEBUG } from "../keys/kvKeys.js";
import { pickLegacyKeyFromCsv } from "../adapters/db.js";

export async function handleImport(env) {
  let scanned = 0;
  let upserted = 0;
  let ignored = 0;

  const q = `
    INSERT INTO posts
    (type,date,ja_html,en_html,ja_link_text,ja_link_href,en_link_text,en_link_href,
     image_src,image_kind,enabled,view_date,media_type,media_src,poster_src,
     legacy_key,created_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?, datetime('now'))
    ON CONFLICT(legacy_key) DO UPDATE SET
      type         = excluded.type,
      date         = excluded.date,
      ja_html      = excluded.ja_html,
      en_html      = excluded.en_html,
      ja_link_text = excluded.ja_link_text,
      ja_link_href = excluded.ja_link_href,
      en_link_text = excluded.en_link_text,
      en_link_href = excluded.en_link_href,
      image_src    = excluded.image_src,
      image_kind   = excluded.image_kind,
      enabled      = excluded.enabled,
      view_date    = excluded.view_date,
      media_type   = excluded.media_type,
      media_src    = excluded.media_src,
      poster_src   = excluded.poster_src
  `;

  try {
    await env.DB.prepare(`
      UPDATE posts
      SET legacy_key = 'archive:date:' || date
      WHERE type='archive'
        AND legacy_key LIKE 'archive:id:%'
    `).run();

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

        const view_date =
          nz(r.view_date).trim() ||
          (type === "archive" ? viewDateFromPadded(date) : "") ||
          null;

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
        if (changes > 0) upserted++;
        else ignored++;
      }
    }

    await kvLogDebug(env, { where: "import:done", scanned, upserted, ignored, ts: Date.now() }, TTL_DEBUG, "db");
    return json({ scanned, upserted, ignored });
  } catch (e) {
    await kvLogDebug(env, { where: "import:error", err: errorText(e), ts: Date.now() }, TTL_DEBUG, "db");
    return new Response(`import error: ${errorText(e)}`, {
      status: 500,
      headers: withCors({ "Content-Type": "text/plain; charset=utf-8" }),
    });
  }
}
