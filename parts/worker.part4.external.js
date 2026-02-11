// =============================
// Part 4/6
// =============================

// -----------------------------
// LINE reply/push
// -----------------------------
async function linePush(env, to, text) {
  const url = "https://api.line.me/v2/bot/message/push";
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${env.LINE_CHANNEL_ACCESS_TOKEN}` },
    body: JSON.stringify({ to, messages: [{ type: "text", text }] }),
  });
  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`LINE push failed: ${res.status} ${t.slice(0, 200)}`);
  }
}

async function lineReply(env, replyToken, text, fallbackToUserId) {
  const url = "https://api.line.me/v2/bot/message/reply";
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${env.LINE_CHANNEL_ACCESS_TOKEN}` },
    body: JSON.stringify({ replyToken, messages: [{ type: "text", text }] }),
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    await kvLogDebug(env, { where: "lineReply:failed", status: res.status, body: t.slice(0, 400), textPreview: (text || "").slice(0, 160), ts: Date.now() }, TTL_DEBUG, "line");

    if (fallbackToUserId) {
      try {
        await linePush(env, fallbackToUserId, text);
        await kvLogDebug(env, { where: "lineReply:failed_but_pushed", status: res.status, ts: Date.now() }, TTL_DEBUG, "line");
        return;
      } catch (e) {
        await kvLogDebug(env, { where: "lineReply:push_fallback_failed", err: errorText(e), ts: Date.now() }, TTL_DEBUG, "line");
      }
    }

    throw new Error(`LINE reply failed: ${res.status} ${t.slice(0, 200)}`);
  }
}

// -----------------------------
// SHA-1 helper (legacy_key hashing)
// -----------------------------
async function sha1hex(s) {
  if (!s) return "";
  const buf = await crypto.subtle.digest("SHA-1", new TextEncoder().encode(s));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

// -----------------------------
// legacy_key
// -----------------------------
async function pickLegacyKey(type, date, contentOrUrl = "") {
  if (type === "archive" && date) return `archive:date:${date}`;

  if (type === "news") {
    const h = await sha1hex(contentOrUrl);
    return `news:${date}:${h.slice(0, 10)}`;
  }
  if (type === "voice") {
    const h = await sha1hex(contentOrUrl);
    return `voice:${date}:${h.slice(0, 10)}`;
  }

  return `${type}:${date}:${crypto.randomUUID()}`;
}

// -----------------------------
// DB insert (UPSERT)
// -----------------------------
async function insertPost(env, row) {
  const q = `
    INSERT INTO posts
      (type,date,ja_html,en_html,ja_link_text,ja_link_href,en_link_text,en_link_href,
       image_src,image_kind,enabled,view_date,
       media_type,media_src,poster_src,legacy_key,created_at)
    VALUES
      (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?, datetime('now'))
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

async function softDeleteMany(env, ids) {
  let ok = 0;
  for (const id of ids) {
    const out = await env.DB.prepare(`UPDATE posts SET enabled='FALSE' WHERE id=?`).bind(id).run();
    if ((out?.meta?.changes ?? 0) > 0) ok++;
  }
  return ok;
}

// -----------------------------
// get/update helpers for editing
// -----------------------------
async function getPostById(env, id) {
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

async function updatePostFields(env, id, fieldsObj) {
  const keys = Object.keys(fieldsObj || {});
  if (!keys.length) return false;

  const setSql = keys.map((k) => `${k}=?`).join(", ");
  const values = keys.map((k) => fieldsObj[k]);

  const q = `UPDATE posts SET ${setSql} WHERE id=?`;
  const out = await env.DB.prepare(q).bind(...values, id).run();
  const changes = out?.meta?.changes ?? 0;
  return changes > 0;
}

// -----------------------------
// Pending KV
// -----------------------------
const TTL_PENDING = 20 * 60;
function keyPendingImage(userId) {
  return `pending_image:${userId}`;
}
function keyPendingVideo(userId) {
  return `pending_video:${userId}`;
}

// -----------------------------
// ✅ Next-type KV (set destination before sending image)
//  - User can send: NEXT:voice / NEXT:news / NEXT:archive (also 日本語)
//  - Applied to the next incoming image, then auto-cleared
// -----------------------------
const TTL_NEXTTYPE = 30 * 60; // 30min
function keyNextType(userId) {
  return `next_type:${userId}`;
}
function normalizeTypeWord(raw) {
  const t = nz(raw).trim().toLowerCase();
  if (t === "news" || t === "voice" || t === "archive") return t;
  if (t.includes("ニュー")) return "news";
  if (t.includes("アーカ")) return "archive";
  if (t.includes("ボイ") || t.includes("voice")) return "voice";
  return null;
}
function parseNextTypeCommand(text) {
  const s = nz(text).trim();
  const m = s.match(/^NEXT\s*[:：]\s*(.+)$/i);
  if (!m) return null;
  return normalizeTypeWord(m[1]);
}
async function setNextType(env, userId, type) {
  if (!type) return;
  await env.KV.put(keyNextType(userId), type, { expirationTtl: TTL_NEXTTYPE });
}
async function consumeNextType(env, userId) {
  const t = await env.KV.get(keyNextType(userId));
  if (t) await env.KV.delete(keyNextType(userId));
  return t ? normalizeTypeWord(t) : null;
}

async function kvGetJson(env, key) {
  const raw = await env.KV.get(key);
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}
async function kvPutJson(env, key, obj, ttl = TTL_PENDING) {
  await env.KV.put(key, JSON.stringify(obj), { expirationTtl: ttl });
}

// -----------------------------
// Editing KV
// -----------------------------
const TTL_EDITING = 30 * 60; // 30min
function keyEditing(userId) {
  return `editing:${userId}`;
}
async function setEditing(env, userId, obj) {
  await env.KV.put(keyEditing(userId), JSON.stringify(obj), { expirationTtl: TTL_EDITING });
}
async function getEditing(env, userId) {
  return await kvGetJson(env, keyEditing(userId));
}
async function clearEditing(env, userId) {
  await env.KV.delete(keyEditing(userId));
}

// -----------------------------
// Delete command
// -----------------------------
function parseDeleteIds(text) {
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

// -----------------------------
// ✅ Edit commands (+ TYPE + DATE support)
// -----------------------------
function parseEditStart(text) {
  const s = nz(text).trim().replace(/\s+/g, "");
  const m = s.match(/^編集[:：](\d+)$/);
  return m ? m[1] : null;
}
function parseEditEnd(text) {
  const s = nz(text).trim();
  return /^(完了|終了|end)$/i.test(s);
}
function parseEditCancel(text) {
  const s = nz(text).trim();
  return /^(取消|キャンセル|中止|cancel)$/i.test(s);
}
function parseEditFieldUpdate(text) {
  const s = nz(text).trim();
  const m = s.match(/^(JA|EN|BTNJA|BTNEN|TYPE|DATE)\s*[:：]\s*([\s\S]+)$/i);
  if (!m) return null;
  return { field: m[1].toUpperCase(), value: (m[2] || "").trim() };
}

// -----------------------------
// LINE content fetch
// -----------------------------
async function fetchLineMessageContent(env, messageId) {
  const url = `https://api-data.line.me/v2/bot/message/${messageId}/content`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${env.LINE_CHANNEL_ACCESS_TOKEN}` } });
  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`LINE content fetch failed: ${res.status} ${t.slice(0, 200)}`);
  }
  const contentType = res.headers.get("content-type") || "application/octet-stream";
  const bytes = await res.arrayBuffer();
  return { bytes, contentType };
}

// -----------------------------
// GitHub upload (images/)
// -----------------------------
function extFromContentType(ct) {
  const s = (ct || "").toLowerCase();
  if (s.includes("png")) return "png";
  if (s.includes("webp")) return "webp";
  if (s.includes("gif")) return "gif";
  return "jpg";
}

async function uploadImageToGitHub(env, { bytes, contentType, messageId }) {
  const owner = env.GITHUB_OWNER || "haruhisa-hosei";
  const repo = env.GITHUB_REPO || "haruhisa-hosei-site";
  const branch = env.GITHUB_BRANCH || "main";
  if (!env.GITHUB_TOKEN) throw new Error("Missing GITHUB_TOKEN");

  const ext = extFromContentType(contentType);
  const date = todayJstDatePadded().replace(/\./g, "");
  const filename = `voice_${date}_${messageId}_${Math.floor(Math.random() * 1000)}.${ext}`;
  const path = `images/${filename}`;
  const b64 = arrayBufferToBase64(bytes);

  const url = `https://api.github.com/repos/${owner}/${repo}/contents/${path}`;
  const res = await fetch(url, {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${env.GITHUB_TOKEN}`,
      Accept: "application/vnd.github+json",
      "User-Agent": "hosei-worker",
    },
    body: JSON.stringify({ message: `Upload ${filename} from LINE`, content: b64, branch }),
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`GitHub upload failed: ${res.status} ${t.slice(0, 400)}`);
  }

  return filename; // GitHub filename
}

// -----------------------------
// R2 keys (image/video/poster)
// -----------------------------
function r2KeyForVideo(userId, messageId) {
  const ym = todayJstDatePadded().slice(0, 7).replace(".", "");
  return `media/video/${ym}/${userId}/${messageId}.mp4`;
}
function r2KeyForPoster(userId, messageId, ext = "jpg") {
  const ym = todayJstDatePadded().slice(0, 7).replace(".", "");
  return `media/poster/${ym}/${userId}/${messageId}.${ext}`;
}
function r2KeyForImage(userId, messageId, ext = "jpg") {
  const ym = todayJstDatePadded().slice(0, 7).replace(".", "");
  return `media/image/${ym}/${userId}/${messageId}.${ext}`;
}

