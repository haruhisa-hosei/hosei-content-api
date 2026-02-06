import { todayJstDatePadded } from "../core/dates.js";

export function r2KeyForVideo(userId, messageId) {
  const ym = todayJstDatePadded().slice(0, 7).replace(".", "");
  return `media/video/${ym}/${userId}/${messageId}.mp4`;
}
export function r2KeyForPoster(userId, messageId, ext = "jpg") {
  const ym = todayJstDatePadded().slice(0, 7).replace(".", "");
  return `media/poster/${ym}/${userId}/${messageId}.${ext}`;
}
