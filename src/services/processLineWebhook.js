import { kvLogDebug } from "../utils/kvDebug.js";
import { handleLineTextEvent, handleLineImageEvent } from "./lineHandlers.js";

export async function processLineWebhook(env, payload) {
  try {
    const events = payload?.events || [];
    for (const ev of events) {
      const type = ev?.type;
      const msgType = ev?.message?.type;

      if (type !== "message") continue;

      if (msgType === "text") {
        await handleLineTextEvent(env, ev);
      } else if (msgType === "image") {
        await handleLineImageEvent(env, ev);
      }
    }
  } catch (err) {
    console.error("processLineWebhook fatal:", err);
    try {
      await kvLogDebug(
        env,
        "line",
        "processLineWebhook:fatal",
        String(err && (err.stack || err.message || err))
      );
    } catch {}
  }
}
