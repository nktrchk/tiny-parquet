/**
 * tiny-parquet/reader — WASM Parquet Reader
 * Supports: Node.js, Browser, Cloudflare Workers, Vercel Edge, Deno, Bun
 * WASM size: ~140KB
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

const decoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
decoder.decode(); // warm up

function getStringFromWasm(ptr, len) {
    ptr = ptr >>> 0;
    return decoder.decode(getUint8().subarray(ptr, ptr + len));
}
function getArrayU8(ptr, len) {
    ptr = ptr >>> 0;
    return getUint8().subarray(ptr, ptr + len);
}
function isLikeNone(x) { return x === undefined || x === null; }
function handleError(f, args) {
    try { return f.apply(this, args); }
    catch (e) { wasm.__wbindgen_export(addHeapObject(e)); }
}

// ── WASM imports ─────────────────────────────────────────────────────────────
function getImports() {
    const wbg = { __proto__: null };

    wbg.__wbg___wbindgen_throw_be289d5034ed271b = (a, b) => {
        throw new Error(getStringFromWasm(a, b));
    };
    wbg.__wbg_length_32ed9a279acd054c = (a) => getObject(a).length;
    wbg.__wbg_new_361308b2356cecd0 = () => addHeapObject(new Object());
    wbg.__wbg_new_3eb36ae241fe6f44 = () => addHeapObject(new Array());
    wbg.__wbg_prototypesetcall_bdcdcc5842e4d77d = (arg0, arg1, arg2) => {
        Uint8Array.prototype.set.call(getArrayU8(arg0, arg1), getObject(arg2));
    };
    wbg.__wbg_push_8ffdcb2063340ba5 = (a, b) => getObject(a).push(getObject(b));
    wbg.__wbg_set_6cb8631f80447a67 = (...args) => handleError((a, b, c) => {
        return Reflect.set(getObject(a), getObject(b), getObject(c));
    }, args);
    wbg.__wbindgen_cast_0000000000000001 = (a) => addHeapObject(a);
    wbg.__wbindgen_cast_0000000000000002 = (a, b) => addHeapObject(getStringFromWasm(a, b));
    wbg.__wbindgen_object_drop_ref = (a) => takeObject(a);

    return { './parquet_reader_bg.js': wbg };
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
        const bytes = readFileSync(join(__dirname, '..', 'wasm', 'reader.wasm'));
        const { instance } = await WebAssembly.instantiate(bytes, imports);
        return instance.exports;
    }

    // Edge / Browser — resolve relative to this module
    const wasmUrl = new URL('../wasm/reader.wasm', import.meta.url);
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
 * Read a Parquet file and return columnar data.
 *
 * @param {Uint8Array} fileBytes - Raw Parquet file bytes.
 * @param {number} [maxRows=500] - Maximum rows to decode.
 * @returns {Promise<{schema: Array<{name: string, type: string}>, data: Record<string, any[]>, numRows: number}>}
 *
 * @example
 * const { schema, data, numRows } = await readParquet(bytes);
 * // schema: [{ name: 'url', type: 'string' }, ...]
 * // data: { url: ['https://example.com'], ts: [1708000000] }
 * // numRows: 1
 */
export async function readParquet(fileBytes, maxRows = 500) {
    await init();
    const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
    try {
        wasm.readParquet(
            retptr,
            addBorrowedObject(fileBytes),
            isLikeNone(maxRows) ? 0x100000001 : (maxRows) >>> 0,
        );
        const r0 = getDV().getInt32(retptr + 0, true);
        const r1 = getDV().getInt32(retptr + 4, true);
        const r2 = getDV().getInt32(retptr + 8, true);
        if (r2) throw takeObject(r1);
        return takeObject(r0);
    } finally {
        wasm.__wbindgen_add_to_stack_pointer(16);
        heap[stack_pointer++] = undefined;
    }
}
