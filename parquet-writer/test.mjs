/**
 * Test: Load WASM module, write Parquet with all supported types, verify with pyarrow.
 * Run: node test.mjs
 * Output: tmp/test_output.parquet
 */
import { readFileSync, writeFileSync, mkdirSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const TMP = join(__dirname, 'tmp');
mkdirSync(TMP, { recursive: true });

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
let wasm;
let cachedUint8 = null;
function getUint8() {
    if (cachedUint8 === null || cachedUint8.byteLength === 0)
        cachedUint8 = new Uint8Array(wasm.memory.buffer);
    return cachedUint8;
}
let cachedDV = null;
function getDV() {
    if (cachedDV === null || cachedDV.buffer !== wasm.memory.buffer)
        cachedDV = new DataView(wasm.memory.buffer);
    return cachedDV;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
decoder.decode();
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

// ── Build imports ────────────────────────────────────────────────────────────
function getImports() {
    const wbg = { __proto__: null };
    wbg.__wbg___wbindgen_is_falsy_e623e5b815413d00 = (a) => !getObject(a);
    wbg.__wbg___wbindgen_number_get_8ff4255516ccad3e = (arg0, arg1) => {
        const obj = getObject(arg1);
        const ret = typeof obj === 'number' ? obj : undefined;
        getDV().setFloat64(arg0 + 8, isLikeNone(ret) ? 0 : ret, true);
        getDV().setInt32(arg0, !isLikeNone(ret), true);
    };
    wbg.__wbg___wbindgen_string_get_72fb696202c56729 = (arg0, arg1) => {
        const obj = getObject(arg1);
        const ret = typeof obj === 'string' ? obj : undefined;
        const ptr1 = isLikeNone(ret) ? 0 : passStringToWasm(ret, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len1 = WASM_VECTOR_LEN;
        getDV().setInt32(arg0 + 4, len1, true);
        getDV().setInt32(arg0, ptr1, true);
    };
    wbg.__wbg___wbindgen_throw_be289d5034ed271b = (a, b) => {
        throw new Error(getStringFromWasm(a, b));
    };
    wbg.__wbg_get_9b94d73e6221f75c = (a, i) => addHeapObject(getObject(a)[i >>> 0]);
    wbg.__wbg_get_b3ed3ad4be2bc8ac = (...args) => handleError((a, b) => {
        return addHeapObject(Reflect.get(getObject(a), getObject(b)));
    }, args);
    wbg.__wbg_isArray_d314bb98fcf08331 = (a) => Array.isArray(getObject(a));
    wbg.__wbg_length_32ed9a279acd054c = (a) => getObject(a).length;
    wbg.__wbg_length_35a7bace40f36eac = (a) => getObject(a).length;
    wbg.__wbg_new_with_length_a2c39cbe88fd8ff1 = (a) => addHeapObject(new Uint8Array(a >>> 0));
    wbg.__wbg_set_cc56eefd2dd91957 = (a, b, c) => getObject(a).set(getArrayU8(b, c));
    wbg.__wbindgen_cast_0000000000000001 = (a, b) => addHeapObject(getStringFromWasm(a, b));
    wbg.__wbindgen_object_drop_ref = (a) => takeObject(a);
    return { './parquet_flake_bg.js': wbg };
}

// ── Load WASM ────────────────────────────────────────────────────────────────
const wasmPath = join(__dirname, '..', 'app', 'lib', 'parquet_flake_bg.wasm');
const wasmBytes = readFileSync(wasmPath);
const compiled = await WebAssembly.compile(wasmBytes);
const instance = await WebAssembly.instantiate(compiled, getImports());
wasm = instance.exports;

// ── Test data with ALL supported types ───────────────────────────────────────
const schema = [
    { name: 'name', type: 'string' },
    { name: 'city', type: 'string' },
    { name: 'age', type: 'int32' },
    { name: 'count', type: 'int64' },
    { name: 'created_at', type: 'timestamp_millis' },
    { name: 'score', type: 'float64' },
    { name: 'rating', type: 'float32' },
    { name: 'active', type: 'boolean' },
    { name: 'metadata', type: 'json' },
];

const now = Date.now();
const data = {
    name: ['Alice', 'Bob', 'Charlie', '日本語テスト'],
    city: ['Berlin', 'London', 'Tokyo', 'München'],
    age: [30, 25, 35, 28],
    count: [100000, 200000, 300000, 400000],
    created_at: [now, now - 86400000, now - 172800000, now - 259200000],
    score: [98.5, 87.3, 92.1, 76.8],
    rating: [4.5, 3.2, 4.8, 3.9],
    active: [true, false, true, true],
    metadata: ['{"role":"admin"}', '{"role":"user"}', '', '{"emoji":"🎉"}'],
};

console.log('Writing test parquet...');
const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
try {
    wasm.writeParquet(retptr, addBorrowedObject(schema), addBorrowedObject(data), addBorrowedObject({ compression: 'snappy' }));
    const r0 = getDV().getInt32(retptr + 0, true);
    const r1 = getDV().getInt32(retptr + 4, true);
    const r2 = getDV().getInt32(retptr + 8, true);
    if (r2) {
        console.error('❌ WASM error:', takeObject(r1));
        process.exit(1);
    }
    const result = takeObject(r0);
    const outPath = join(TMP, 'test_output.parquet');
    writeFileSync(outPath, result);
    console.log(`✅ Written ${result.byteLength} bytes → ${outPath}`);
} finally {
    wasm.__wbindgen_add_to_stack_pointer(16);
    heap[stack_pointer++] = undefined;
    heap[stack_pointer++] = undefined;
    heap[stack_pointer++] = undefined;
}
