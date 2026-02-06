import { arrayBufferToBase64 } from "../core/cryptoBase64.js";
import { todayJstDatePadded } from "../core/dates.js";

export function extFromContentType(ct) {
  const s = (ct || "").toLowerCase();
  if (s.includes("png")) return "png";
  if (s.includes("webp")) return "webp";
  if (s.includes("gif")) return "gif";
  return "jpg";
}

export async function uploadImageToGitHub(env, { bytes, contentType, messageId }) {
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
    body: JSON.stringify({
      message: `Upload ${filename} from LINE`,
      content: b64,
      branch,
    }),
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`GitHub upload failed: ${res.status} ${t.slice(0, 400)}`);
  }

  return filename;
}
