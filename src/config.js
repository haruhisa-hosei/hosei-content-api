export const VERSION =
  "hosei-content-api-2026-02-06-jsonschema+gemini-hybrid-waituntil-voiceprefixfix+pushfallback+kvdebug+openailogs+chat-edit+multidelete";

export const CSV = {
  news: "https://docs.google.com/spreadsheets/d/e/2PACX-1vQSkBOovAHzdZWtA0Z-KRe27h5ZzGFi5Bq2G7Bp0Mv4sQ-2C9urIYy8oR9IaMf7xdSR9M_iww2zMbG-/pub?gid=0&single=true&output=csv",
  voice:
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vQSkBOovAHzdZWtA0Z-KRe27h5ZzGFi5Bq2G7Bp0Mv4sQ-2C9urIYy8oR9IaMf7xdSR9M_iww2zMbG-/pub?gid=793239367&single=true&output=csv",
  archive:
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vQSkBOovAHzdZWtA0Z-KRe27h5ZzGFi5Bq2G7Bp0Mv4sQ-2C9urIYy8oR9IaMf7xdSR9M_iww2zMbG-/pub?gid=260654898&single=true&output=csv",
};

export const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
};

export const TTL_DEBUG = 24 * 60 * 60; // 24h
export const TTL_PENDING = 20 * 60; // 20min
export const TTL_EDITING = 30 * 60; // 30min
