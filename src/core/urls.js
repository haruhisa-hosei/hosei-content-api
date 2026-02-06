import { nz } from "./strings.js";

export function extractUrl(content) {
  const m = nz(content).match(/https?:\/\/[\w!?\/\+\-_~=;.,*&@#$%()'[\]]+/);
  return m ? m[0] : "";
}
