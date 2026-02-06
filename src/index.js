// src/index.js
// Cloudflare Worker entry (modular)

import { VERSION } from "./config.js";
import { json, textOut, withCors } from "./core/http.js";
import { handleLineWebhook } from "./routes/lineWebhook.js";

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);

    if (req.method === "OPTIONS") {
      return new Response("", { headers: withCors() });
    }

    if (url.pathname === "/health") {
      return json({ ok: true, version: VERSION });
    }

    if (url.pathname === "/line-webhook") {
      return await handleLineWebhook(req, env, ctx);
    }

    return textOut("alive");
  },
};
