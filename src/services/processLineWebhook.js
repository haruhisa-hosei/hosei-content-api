// src/services/processLineWebhook.js
import { errorText } from "../core/errors.js";
import { handleLineTextEvent } from "./lineHandlers.js";
// 将来追加するだけ（今は未使用でもOK）
// import { handleLineImageEvent, handleLineVideoEvent } from "./lineHandlers.js";

export async function processLineWebhook(env, payload) {
  const events = payload?.events || [];

  for (const event of events) {
    try {
      const userId = event?.source?.userId;
      if (!userId) continue;

      // 管理者以外は無視
      if (env.ADMIN_USER_ID && userId !== env.ADMIN_USER_ID) continue;

      const msg = event?.message;
      const type = msg?.type;
      if (!type) continue;

      if (type === "text") {
        await handleLineTextEvent(env, event, msg.text);
        continue;
      }

      // image/video等は「壊さず」なので今は何もしない
      // if (type === "image") await handleLineImageEvent(env, event, msg);
      // if (type === "video") await handleLineVideoEvent(env, event, msg);
    } catch (e) {
      console.error("processLineWebhook:event error", errorText(e));
    }
  }
}

// Backward-compat default export (so older imports like `import processLineWebhook from ...` won't break)
export default processLineWebhook;
