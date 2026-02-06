// src/services/lineHandlers.js
import { lineReply } from "../adapters/lineApi.js";

function nz(v) {
  return (v ?? "").toString();
}

export async function handleLineTextEvent(env, event, text) {
  const replyToken = event?.replyToken;
  const t = nz(text).trim();

  // ここは将来：N:/V:/A: 判定→D1保存→返信
  // 今は「webhookが通って返信できる」だけ確認
  if (replyToken) {
    await lineReply(env, replyToken, `OK: ${t.slice(0, 80)}`);
  }
}
