// src/worker.js
// Safe switcher: monolith <-> modular (no separate Worker needed)
//
// How to use:
// - Default: monolith (USE_MODULAR != "1")
// - To enable modular at runtime: set Worker variable USE_MODULAR="1"
//
// This lets you test modular without losing the ability to instantly revert
// (just flip the variable back to "0" / delete it).

import monolith from "./worker.monolith.js";
import modular from "./index.js";

export default {
  async fetch(req, env, ctx) {
    const useModular = (env && (env.USE_MODULAR === "1" || env.USE_MODULAR === "true")) ? true : false;
    try {
      return useModular
        ? await modular.fetch(req, env, ctx)
        : await monolith.fetch(req, env, ctx);
    } catch (e) {
      // If modular crashes unexpectedly, fail-safe to monolith.
      // (Keeps prod alive even if you flip USE_MODULAR by mistake.)
      try {
        return await monolith.fetch(req, env, ctx);
      } catch {
        throw e;
      }
    }
  },
};
