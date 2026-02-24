/**
 * tiny-parquet/writer — WASM Parquet Writer
 * Supports: Node.js, Browser, Cloudflare Workers, Vercel Edge, Deno, Bun
 * WASM size: ~179KB
 */

let wasm;
let initPromise = null;

// ── Heap / object table ──────────────────────────────────────────────────────
const heap = new Array(128).fill(undefined);
heap.push(undefined, null, true, false);
let heap_next = heap.length;
let stack_pointer = 128;

function addHeapObject(obj) {
  if (heap_next === heap.length) heap.push(heap.length + 1);
  const idx = heap_next;
  heap_next = heap[idx];
  heap[idx] = obj;
  return idx;
}
function addBorrowedObject(obj) {
  if (stack_pointer === 1) throw new Error('out of js stack');
  heap[--stack_pointer] = obj;
  return stack_pointer;
}
function getObject(idx) { return heap[idx]; }
function dropObject(idx) {
  if (idx < 132) return;
  heap[idx] = heap_next;
  heap_next = idx;
}
function takeObject(idx) {
  const ret = getObject(idx);
  dropObject(idx);
  return ret;
}

// ── Memory helpers ───────────────────────────────────────────────────────────
let cachedUint8 = null;
function getUint8() {
  if (cachedUint8 === null || cachedUint8.byteLength === 0)
    cachedUint8 = new Uint8Array(wasm.memory.buffer);
  return cachedUint8;
}
let cachedDV = null;
function getDV() {
  if (cachedDV === null || cachedDV.buffer.detached === true ||
    (cachedDV.buffer.detached === undefined && cachedDV.buffer !== wasm.memory.buffer))
    cachedDV = new DataView(wasm.memory.buffer);
  return cachedDV;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
decoder.decode(); // warm up
let WASM_VECTOR_LEN = 0;

function passStringToWasm(arg, malloc, realloc) {
  if (realloc === undefined) {
    const buf = encoder.encode(arg);
    const ptr = malloc(buf.length, 1) >>> 0;
    getUint8().subarray(ptr, ptr + buf.length).set(buf);
    WASM_VECTOR_LEN = buf.length;
    return ptr;
  }
  let len = arg.length;
  let ptr = malloc(len, 1) >>> 0;
  const mem = getUint8();
  let offset = 0;
  for (; offset < len; offset++) {
    const code = arg.charCodeAt(offset);
    if (code > 0x7F) break;
    mem[ptr + offset] = code;
  }
  if (offset !== len) {
    if (offset !== 0) arg = arg.slice(offset);
    ptr = realloc(ptr, len, len = offset + arg.length * 3, 1) >>> 0;
    const view = getUint8().subarray(ptr + offset, ptr + len);
    const ret = encoder.encodeInto(arg, view);
    offset += ret.written;
    ptr = realloc(ptr, len, offset, 1) >>> 0;
  }
  WASM_VECTOR_LEN = offset;
  return ptr;
}
function getStringFromWasm(ptr, len) {
  return decoder.decode(getUint8().subarray(ptr >>> 0, (ptr >>> 0) + len));
}
function getArrayU8(ptr, len) {
  ptr = ptr >>> 0;
  return getUint8().subarray(ptr, ptr + len);
}
function isLikeNone(x) { return x === undefined || x === null; }
function handleError(f, args) {
  try { return f.apply(this, args); }
  catch (e) { wasm.__wbindgen_export3(addHeapObject(e)); }
}

// ── WASM imports ─────────────────────────────────────────────────────────────
function getImports() {
  const wbg = { __proto__: null };
  wbg.__wbg___wbindgen_is_falsy_7b47cfa682bded80 = (a) => !getObject(a);
  wbg.__wbg___wbindgen_number_get_3330675b4e5c3680 = (arg0, arg1) => {
    const obj = getObject(arg1);
    const ret = typeof obj === 'number' ? obj : undefined;
    getDV().setFloat64(arg0 + 8, isLikeNone(ret) ? 0 : ret, true);
    getDV().setInt32(arg0, !isLikeNone(ret), true);
  };
  wbg.__wbg___wbindgen_string_get_7b8bc463f6cbeefe = (arg0, arg1) => {
    const obj = getObject(arg1);
    const ret = typeof obj === 'string' ? obj : undefined;
    const ptr1 = isLikeNone(ret) ? 0 : passStringToWasm(ret, wasm.__wbindgen_export, wasm.__wbindgen_export2);
    const len1 = WASM_VECTOR_LEN;
    getDV().setInt32(arg0 + 4, len1, true);
    getDV().setInt32(arg0, ptr1, true);
  };
  wbg.__wbg___wbindgen_throw_89ca9e2c67795ec1 = (a, b) => {
    throw new Error(getStringFromWasm(a, b));
  };
  wbg.__wbg_get_229657ec2da079cd = (a, i) => addHeapObject(getObject(a)[i >>> 0]);
  wbg.__wbg_get_89f3a4c398b4872e = (...args) => handleError((a, b) => {
    return addHeapObject(Reflect.get(getObject(a), getObject(b)));
  }, args);
  wbg.__wbg_isArray_fe5201bfdab7e39d = (a) => Array.isArray(getObject(a));
  wbg.__wbg_length_f875d3a041bab91a = (a) => getObject(a).length;
  wbg.__wbg_length_feaf2a40e5f9755a = (a) => getObject(a).length;
  wbg.__wbg_new_with_length_3217a89bbca17214 = (a) => addHeapObject(new Uint8Array(a >>> 0));
  wbg.__wbg_set_76943c82a5e79352 = (a, b, c) => getObject(a).set(getArrayU8(b, c));
  wbg.__wbindgen_cast_0000000000000001 = (a, b) => addHeapObject(getStringFromWasm(a, b));
  wbg.__wbindgen_object_drop_ref = (a) => takeObject(a);
  return { './parquet_writer_bg.js': wbg };
}

// ── WASM loader (universal) ──────────────────────────────────────────────────
async function loadWasm() {
  const imports = getImports();

  // Node.js
  if (typeof process !== 'undefined' && process.versions?.node) {
    const { readFileSync } = await import('node:fs');
    const { dirname, join } = await import('node:path');
    const { fileURLToPath } = await import('node:url');
    const __dirname = dirname(fileURLToPath(import.meta.url));
    const bytes = readFileSync(join(__dirname, '..', 'wasm', 'writer.wasm'));
    const { instance } = await WebAssembly.instantiate(bytes, imports);
    return instance.exports;
  }

  // Edge / Browser — resolve relative to this module
  const wasmUrl = new URL('../wasm/writer.wasm', import.meta.url);
  const response = await fetch(wasmUrl);
  const bytes = await response.arrayBuffer();
  const { instance } = await WebAssembly.instantiate(bytes, imports);
  return instance.exports;
}

// ── Init (lazy singleton) ────────────────────────────────────────────────────
async function init() {
  if (initPromise) return initPromise;
  initPromise = loadWasm().then(exports => {
    wasm = exports;
    cachedUint8 = null;
    cachedDV = null;
    return wasm;
  });
  return initPromise;
}

// ── Public API ───────────────────────────────────────────────────────────────
/**
 * Write a Parquet file from columnar data.
 *
 * @param {Array<{name: string, type: string}>} schema - Column definitions.
 *   Supported types: 'string', 'int32', 'int64', 'float32', 'float64', 'boolean', 'timestamp'
 * @param {Record<string, any[]>} data - Columnar data keyed by column name.
 * @param {Object} [config] - Optional configuration.
 * @param {string} [config.compression='snappy'] - 'snappy' | 'none'
 * @returns {Promise<Uint8Array>} The Parquet file bytes.
 *
 * @example
 * const bytes = await writeParquet(
 *   [{ name: 'url', type: 'string' }, { name: 'ts', type: 'int64' }],
 *   { url: ['https://example.com'], ts: [1708000000] },
 *   { compression: 'snappy' }
 * );
 */
export async function writeParquet(schema, data, config = {}) {
  await init();
  const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
  try {
    wasm.writeParquet(
      retptr,
      addBorrowedObject(schema),
      addBorrowedObject(data),
      addBorrowedObject(config),
    );
    const r0 = getDV().getInt32(retptr + 0, true);
    const r1 = getDV().getInt32(retptr + 4, true);
    const r2 = getDV().getInt32(retptr + 8, true);
    if (r2) throw takeObject(r1);
    return takeObject(r0);
  } finally {
    wasm.__wbindgen_add_to_stack_pointer(16);
    heap[stack_pointer++] = undefined;
    heap[stack_pointer++] = undefined;
    heap[stack_pointer++] = undefined;
  }
}
