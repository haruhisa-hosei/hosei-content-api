// src/index.js (MODULAR entry)
import { VERSION } from "./config.js";
import { withCors, json, textOut } from "./core/http.js";

import { handlePosts } from "./routes/posts.js";
import { handleMedia } from "./routes/media.js";
import { handleImport } from "./routes/import.js";
import { handleDebugLast } from "./routes/debugLast.js";
import { handleLineWebhook } from "./routes/lineWebhook.js";

export default {
  async fetch(req, env, ctx) {
    if (req.method === "OPTIONS") return new Response("", { headers: withCors() });

    const url = new URL(req.url);

    // health
    if (url.pathname === "/health") {
      return json({ ok: true, version: VERSION });
    }

    // debug last (admin only)
    if (url.pathname === "/debug-last") {
      return await handleDebugLast(req, env);
    }

    // media proxy (R2)
    if (url.pathname.startsWith("/media/") && req.method === "GET") {
      return await handleMedia(req, env);
    }

    // alias: /api/news|voice|archive
    if (/^\/api\/(news|voice|archive)$/.test(url.pathname)) {
      const t = url.pathname.split("/").pop();
      const u = new URL(url.toString());
      u.pathname = "/posts";
      u.searchParams.set("type", t);
      return await handlePosts(new Request(u.toString(), req), env);
    }

    // line webhook
    if (url.pathname === "/line-webhook" && req.method === "POST") {
      return await handleLineWebhook(req, env, ctx);
    }

    // posts
    if (url.pathname === "/posts") {
      return await handlePosts(req, env);
    }

    // import (optional)
    if (url.pathname === "/import") {
      return await handleImport(req, env);
    }

    return textOut("hosei api alive");
  },
};
