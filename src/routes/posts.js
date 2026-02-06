import { clampInt } from "../core/strings.js";
import { json } from "../core/http.js";

export async function handlePosts(url, env) {
  const type = url.searchParams.get("type") || "news";
  const onlyEnabled = (url.searchParams.get("onlyEnabled") ?? "1") !== "0";
  const limit = clampInt(url.searchParams.get("limit"), 20, 1, 500);
  const offset = clampInt(url.searchParams.get("offset"), 0, 0, 1_000_000);

  const baseSql = `
    SELECT
      id, type, date, view_date,
      ja_html, en_html,
      ja_link_text, ja_link_href,
      en_link_text, en_link_href,
      image_src, image_kind,
      media_type, media_src, poster_src,
      enabled,
      legacy_key, created_at, updated_at
    FROM posts
    WHERE type=?
  `;

  const flagSql = onlyEnabled ? ` AND enabled='TRUE' ` : ``;

  const dateKeyExpr = `
    REPLACE(REPLACE(REPLACE(REPLACE(date,'.',''),'/',''),' ',''),':','')
  `;

  const orderSql =
    type === "voice"
      ? `
    ORDER BY created_at DESC, id DESC
    LIMIT ? OFFSET ?
  `
      : `
    ORDER BY
      LENGTH(${dateKeyExpr}) DESC,
      ${dateKeyExpr} DESC,
      id DESC
    LIMIT ? OFFSET ?
  `;

  const stmt = env.DB.prepare(baseSql + flagSql + orderSql).bind(type, limit, offset);
  const { results } = await stmt.all();
  return json(results);
}
