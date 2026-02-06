import { CORS } from "../config.js";

export function withCors(headers = {}) {
  return { ...CORS, ...headers };
}

export function json(data, status = 200, headers = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: withCors({ "Content-Type": "application/json; charset=utf-8", ...headers }),
  });
}

export function textOut(s, status = 200, headers = {}) {
  return new Response(s, {
    status,
    headers: withCors({ "Content-Type": "text/plain; charset=utf-8", ...headers }),
  });
}
