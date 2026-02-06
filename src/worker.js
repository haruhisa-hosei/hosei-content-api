import monolith from "./worker.monolith.js";
import modular from "./index.js"; // ここは後で作る（今は仮でOK）

const USE_MODULAR = false; // ←最初は絶対 false

export default USE_MODULAR ? modular : monolith;
