import { TTL_DEBUG, TTL_PENDING, TTL_EDITING } from "../config.js";

export { TTL_DEBUG, TTL_PENDING, TTL_EDITING };

export function keyDebug(scope = "general") {
  return `debug:${scope}:${Date.now()}:${crypto.randomUUID()}`;
}
export function keyDebugLast(scope = "general") {
  return `debug:last:${scope}`;
}

export function keyPendingImage(userId) {
  return `pending_image:${userId}`;
}
export function keyPendingVideo(userId) {
  return `pending_video:${userId}`;
}

export function keyEditing(userId) {
  return `editing:${userId}`;
}
