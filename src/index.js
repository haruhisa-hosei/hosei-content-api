import { withCors, json, textOut } from "./core/http.js";
import { VERSION } from "./config.js";

import { handlePosts } from "./routes/posts.js";
import { handleMedia } from "./routes/media.js";
import { handleImport } from "./routes/import.js";
import { handleDebugLast } from "./routes/debugLast.js";

// ★追加する（次の手順で作る）
import { handleLineWebhook } from "./routes/lineWebhook.js";

export default {
  async fetch(req, env, ctx) {
    if (req.method === "OPTIONS") return new Response("", { headers: withCors() });

    const url = new URL(req.url);

    if (url.pathname === "/health") {
      return json({ ok: true, version: VERSION });
    }

    if (url.pathname === "/debug-last") {
      return await handleDebugLast(url, env);
    }

    if (url.pathname.startsWith("/media/") && req.method === "GET") {
      return await handleMedia(url, req, env);
    }

    if (/^\/api\/(news|voice|archive)$/.test(url.pathname)) {
      const t = url.pathname.split("/").pop();
      const u = new URL(url.toString());
      u.pathname = "/posts";
      u.searchParams.set("type", t);
      return await handlePosts(u, env);
    }

    if (url.pathname === "/line-webhook" && req.method === "POST") {
      return await handleLineWebhook(req, env, ctx);
    }

    if (url.pathname === "/posts") {
      return await handlePosts(url, env);
    }

    if (url.pathname === "/import") {
      return await handleImport(env);
    }

    return textOut("hosei api alive");
  },
};
