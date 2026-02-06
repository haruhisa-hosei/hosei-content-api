// src/services/processLineWebhook.js
import { errorText } from "../core/errors.js";
import { handleLineTextEvent } from "./lineHandlers.js";

export async function processLineWebhook(env, payload) {
  const events = payload?.events || [];

  for (const event of events) {
    try {
      const userId = event?.source?.userId;
      if (!userId) continue;

      // 管理者以外は無視
      if (env.ADMIN_USER_ID && userId !== env.ADMIN_USER_ID) continue;

      const msg = event?.message;
      if (!msg?.type) continue;

      if (msg.type === "text") {
        await handleLineTextEvent(env, event, msg.text);
      } else {
        // image/video等はここで分岐して後日拡張
        // 今は「壊さず」なので無視でOK
      }
    } catch (e) {
      console.error("processLineWebhook:event error", errorText(e));
    }
  }
}
