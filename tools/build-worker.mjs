// tools/build-worker.mjs
import fs from "fs";
import path from "path";

const PARTS = [
  "parts/worker.part1.meta.js",
  "parts/worker.part2.html.js",
  "parts/worker.part3.parse.js",
  "parts/worker.part4.external.js",
  "parts/worker.part5.routes.js",
  "parts/worker.part6.entry.js",
];

const INTENT_PATH = "intent/parts.txt";
const OUT_PATH = "src/worker.js";

function readIntentList() {
  if (!fs.existsSync(INTENT_PATH)) return [];
  return fs
    .readFileSync(INTENT_PATH, "utf8")
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter((s) => s && !s.startsWith("#"));
}

function assertSafeIntent(intent) {
  // 空＝宣言なし（現状は“検証用”なので許可）
  if (intent.length === 0) return;

  // intent はファイル名（例: worker.part3.parse.js）だけを書く想定
  const allowed = new Set(PARTS.map((p) => path.basename(p)));
  for (const name of intent) {
    if (!allowed.has(name)) {
      throw new Error(
        `intent/parts.txt に不正な指定: ${name}\n` +
          `許可: ${[...allowed].join(", ")}`
      );
    }
  }
}

const intent = readIntentList();
assertSafeIntent(intent);

// ✅ 連結は絶対に join("")（改行・整形は一切触らない）
const built = PARTS.map((p) => fs.readFileSync(p, "utf8")).join("");

fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });
fs.writeFileSync(OUT_PATH, built, "utf8");

console.log(`Assembled: ${OUT_PATH}`);
console.log(`Intent: ${intent.length ? intent.join(", ") : "(empty=allow all)"}`);
