// src/index.js
import { handleLineWebhook } from "./routes/lineWebhook.js";

function textOut(s, status = 200, headers = {}) {
  return new Response(s, {
    status,
    headers: { "Content-Type": "text/plain; charset=utf-8", ...headers },
  });
}

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);

    if (req.method === "OPTIONS") {
      return new Response("", {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type,Authorization",
        },
      });
    }

    if (url.pathname === "/health") {
      return new Response(
        JSON.stringify({ ok: true, version: "modular-linewebhook-only" }),
        { headers: { "Content-Type": "application/json; charset=utf-8" } }
      );
    }

    if (url.pathname === "/line-webhook" && req.method === "POST") {
      return await handleLineWebhook(req, env, ctx);
    }

    return textOut("alive");
  },
};
