import { textOut, withCors } from "../core/http.js";

export async function handleMedia(url, req, env) {
  const key = decodeURIComponent(url.pathname.replace(/^\/media\//, ""));
  if (!key) return textOut("missing key", 400);

  const obj = await env.R2.get(key);
  if (!obj) return textOut("not found", 404);

  const headers = new Headers(withCors());
  const ct = obj.httpMetadata?.contentType || "application/octet-stream";
  headers.set("Content-Type", ct);
  headers.set("Accept-Ranges", "bytes");
  headers.set("Cache-Control", "public, max-age=31536000, immutable");

  return new Response(obj.body, { status: 200, headers });
}
