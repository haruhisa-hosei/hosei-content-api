import { kvLogDebug } from "./kvDebug.js";
import { TTL_DEBUG } from "../keys/kvKeys.js";
import { nz } from "../core/strings.js";

export function pickLegacyKey(type, date) {
  if (type === "archive" && date) return `archive:date:${date}`;
  return `${type}:${date}:${crypto.randomUUID()}`;
}

export async function insertPost(env, row) {
  const q = `
    INSERT INTO posts
      (type,date,ja_html,en_html,ja_link_text,ja_link_href,en_link_text,en_link_href,
       image_src,image_kind,enabled,view_date,
       media_type,media_src,poster_src,legacy_key,created_at)
    VALUES
      (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))
  `;
  await env.DB.prepare(q)
    .bind(
      row.type,
      row.date,
      row.ja_html,
      row.en_html,
      row.ja_link_text,
      row.ja_link_href,
      row.en_link_text,
      row.en_link_href,
      row.image_src,
      row.image_kind,
      row.enabled,
      row.view_date,
      row.media_type,
      row.media_src,
      row.poster_src,
      row.legacy_key
    )
    .run();

  const got = await env.DB.prepare(`SELECT id FROM posts WHERE legacy_key=? LIMIT 1`).bind(row.legacy_key).first();
  const id = got?.id ?? null;

  await kvLogDebug(env, { where: "insertPost:ok", id, type: row.type, date: row.date, ts: Date.now() }, TTL_DEBUG, "db");
  return id;
}

export async function softDeleteMany(env, ids) {
  let ok = 0;
  for (const id of ids) {
    const out = await env.DB.prepare(`UPDATE posts SET enabled='FALSE' WHERE id=?`).bind(id).run();
    if ((out?.meta?.changes ?? 0) > 0) ok++;
  }
  return ok;
}

export async function getPostById(env, id) {
  const row = await env.DB.prepare(`
    SELECT
      id, type, date, view_date,
      ja_html, en_html,
      ja_link_text, ja_link_href,
      en_link_text, en_link_href,
      image_src, image_kind,
      media_type, media_src, poster_src,
      enabled
    FROM posts
    WHERE id=?
    LIMIT 1
  `).bind(id).first();
  return row || null;
}

export async function updatePostFields(env, id, fieldsObj) {
  const keys = Object.keys(fieldsObj || {});
  if (!keys.length) return false;

  const setSql = keys.map((k) => `${k}=?`).join(", ");
  const values = keys.map((k) => fieldsObj[k]);

  const q = `UPDATE posts SET ${setSql} WHERE id=?`;
  const out = await env.DB.prepare(q).bind(...values, id).run();
  const changes = out?.meta?.changes ?? 0;

  await kvLogDebug(env, { where: "updatePostFields", id, keys, changes, ts: Date.now() }, TTL_DEBUG, "db");
  return changes > 0;
}

export function pickLegacyKeyFromCsv(type, r, normalizedDate) {
  const date = (normalizedDate ?? nz(r.date)).trim();
  if (type === "archive" && date) return `archive:date:${date}`;

  const id = nz(r.id).trim();
  if (id && date) return `${type}:id:${id}:${date}`;
  if (id) return `${type}:id:${id}`;
  if (date) return `${type}:date:${date}`;
  return `${type}:row:${crypto.randomUUID()}`;
}
