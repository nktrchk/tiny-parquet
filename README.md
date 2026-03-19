 ```
  __   __                                                       __   
 |  |_|__.-----.--.--.______.-----.---.-.----.-----.--.--.-----|  |_ 
 |   _|  |     |  |  |______|  _  |  _  |   _|  _  |  |  |  -__|   _|
 |____|__|__|__|___  |      |   __|___._|__| |__   |_____|_____|____|
               |_____|      |__|                |__|                 

```

```
  ┌───────────────────────────────────────────────────────────┐
  │ TINY PARQUET                                     [ 306KB ]│
  ├───────────────┬───────────────┬───────────────┬───────────┤
  │ WASM + RUST   │ ZERO DEPS     │ ACCESS: R+W   │ STATUS: OK│
  ├───────────────┴───────────────┴───────────────┴───────────┤
  │ Fast, minimal Apache Parquet engine for the Edge.         │
  └───────────────────────────────────────────────────────────┘
```

<p align="center">
  <strong>The only Parquet library that fits on Cloudflare Workers free tier and Vercel Edge.</strong>
</p>

<p align="center">
  <a href="https://www.npmjs.com/package/tiny-parquet"><img src="https://img.shields.io/npm/v/tiny-parquet.svg?style=flat-square&color=cb3837" alt="npm version"></a>
  <a href="https://www.npmjs.com/package/tiny-parquet"><img src="https://img.shields.io/npm/dm/tiny-parquet.svg?style=flat-square&color=blue" alt="npm downloads"></a>
  <a href="https://github.com/nktrchk/tiny-parquet/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-green.svg?style=flat-square" alt="license"></a>
</p>

Two functions — `writeParquet` and `readParquet`. Pass in plain JS objects, get compact Parquet bytes (and vice versa). Encoding, compression, dictionary — all handled inside Rust/WASM. Zero JS deps.

---

## Why

```
  parquet-wasm   3,500 KB   ❌  Too fat for Vercel Edge & CF free tier
  duckdb-wasm    8,000 KB   ❌  Way too fat
  parquetjs        500 KB   ❌  Node.js only
  tiny-parquet     306 KB   ✅  Runs everywhere
```

---

## Install & Quick Start

```bash
npm install tiny-parquet
```

```js
import { writeParquet, readParquet } from 'tiny-parquet';

const bytes = await writeParquet(
  [
    { name: 'country', type: 'string' },
    { name: 'score',   type: 'int32' },
    { name: 'active',  type: 'boolean' },
  ],
  {
    country: ['US', 'UK', 'DE', 'US', 'UK'],
    score:   [95, 82, 77, 91, 88],
    active:  [true, false, true, true, true],
  }
);
// Dictionary encoding kicks in automatically for 'country' → 76% smaller files

const { schema, data, numRows } = await readParquet(bytes);
```

Subpath imports for smaller bundles:
```js
import { readParquet }  from 'tiny-parquet/reader';  // 133KB
import { writeParquet } from 'tiny-parquet/writer';  // 173KB
```

---

## API

### `writeParquet(schema, data, config?)`

| Param | Type | Description |
|-------|------|-------------|
| `schema` | `Array<{ name, type }>` | Column definitions |
| `data` | `Record<string, any[]>` | Columnar data keyed by column name |
| `config` | `object` | Options (see below) |
| **Returns** | `Promise<Uint8Array>` | Raw Parquet file bytes |

**Config options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `compression` | `'snappy' \| 'none'` | `'snappy'` | Page compression |
| `dictionary` | `boolean` | `true` | Dictionary encoding for string columns |

**Supported types:** `string`, `int32`, `int64`, `float32`, `float64`, `boolean`, `timestamp`

### `readParquet(bytes, maxRows?)`

| Param | Type | Description |
|-------|------|-------------|
| `bytes` | `Uint8Array` | Raw Parquet file bytes |
| `maxRows` | `number` | Max rows to decode (default: `500`) |
| **Returns** | `Promise<{ schema, data, numRows }>` | Parsed result |

---

## Dictionary Encoding

Automatically applied to string columns when cardinality is low relative to row count. Produces standard Parquet `RLE_DICTIONARY` pages — readable by DuckDB, Spark, Arrow, everything.

```
  5,000 rows × 2 string columns (10 countries + 5 browsers)
  ─────────────────────────────────────────────────
  Dict ON:    4,811 bytes
  Dict OFF:  20,043 bytes
  Savings:      76%
```

Disable per-write: `{ dictionary: false }`.

---

## Performance

Benchmarked on Node.js (Apple Silicon), post-WASM warmup.

```
  Rows       Write       Read       File Size
  ──────────────────────────────────────────────
    1,000      0.8 ms     0.4 ms       17 KB
   10,000      6.8 ms     5.7 ms      168 KB
  100,000     69.3 ms    48.7 ms    1,676 KB
```

**~20 MB/s writes, ~35 MB/s reads.** A 100KB write takes ~5ms — invisible inside a Cloudflare Worker request.

---

## Runs Everywhere

Cloudflare Workers · Vercel Edge · Deno · Bun · Node.js ≥18 · Browser · AWS Lambda · Fastly Compute · Electron

---

## Examples

### Cloudflare Worker
```js
import { writeParquet } from 'tiny-parquet/writer';

export default {
  async fetch(request, env) {
    const bytes = await writeParquet(
      [{ name: 'path', type: 'string' }, { name: 'ts', type: 'timestamp' }],
      { path: [new URL(request.url).pathname], ts: [Date.now()] },
    );
    await env.R2_BUCKET.put(`logs/${Date.now()}.parquet`, bytes);
    return new Response('OK');
  }
};
```

### Next.js Edge Route
```js
import { readParquet } from 'tiny-parquet/reader';
export const runtime = 'edge';

export async function GET() {
  const res = await fetch('https://data.example.com/events.parquet');
  const { data, numRows } = await readParquet(new Uint8Array(await res.arrayBuffer()));
  return Response.json({ numRows, sample: data });
}
```

---

## Anatomy

```
  writer.wasm  173KB   Rust + parquet2 + snappy + dict encoding
  reader.wasm  133KB   Rust + parquet2 + snappy + dict decoding
  writer.js    ~120 LOC   JS glue (WASM loader + memory bridge)
  reader.js    ~100 LOC   JS glue
  ─────────────────────────────────────────────────────────────
  Total:       306KB · 0 dependencies
```

### Build from Source
```bash
# Requires: rustup target add wasm32-unknown-unknown, wasm-bindgen-cli, wasm-opt
cd parquet-writer
cargo build --target wasm32-unknown-unknown --release
wasm-bindgen target/wasm32-unknown-unknown/release/parquet_writer.wasm \
  --out-dir pkg --target web
wasm-opt --enable-bulk-memory --enable-nontrapping-float-to-int \
  pkg/parquet_writer_bg.wasm -o ../wasm/writer.wasm -Oz

cd ../parquet-reader  # same steps
```

---

## Roadmap

| Feature | Size Cost | Status |
| :--- | :--- | :--- |
| **Snappy Compression** | included | ✅ Done |
| **Dictionary Encoding** | +8 KB | ✅ Done |
| **Column Pruning** | +0 KB | Next |
| **Row Group Control** | +5 KB | Planned |
| **Nested Types** | +60 KB | Planned |

---

## FAQ

**Q: How does this compare to `parquet-wasm`?**
A: `parquet-wasm` is full-featured at 3.5MB. `tiny-parquet` is 9x smaller — flat schemas, essential types, edge-first.

**Q: How do you keep it so small?**
A: Focused feature set + aggressive WASM optimization (`-Oz`, LTO, `codegen-units=1`). Subpath imports let you bundle only reader or writer.

**Q: What about TypeScript?**
A: Full `.d.ts` declarations included. Just import and go.

---

## Changelog

### v0.3.0 — Dictionary Encoding *(unstable)*

**What changed:**
- String columns now use **dictionary encoding** (`RLE_DICTIONARY`) automatically when cardinality is low
- Reader updated to decode both `PLAIN` and `RLE_DICTIONARY` encoded pages
- New config option `{ dictionary: false }` to opt out per write (default: ON)
- WASM binaries recompiled with updated toolchain (Rust 1.93, wasm-bindgen 0.2.112, wasm-opt 128)
- Writer JS glue updated — new `__wbindgen_is_undefined` import added

**What's new for users:**
- Clickstream data (country, browser, device...) → **41% smaller** files with zero config change
- **7% faster writes** — single-pass Vec-based encoder, no HashMap, no pre-scan
- **Faster reads** — less data to decompress
- Files are standard Parquet — validated with **PyArrow** (Apache Arrow reference implementation)
- Dictionary + Snappy compression stack for maximum savings
- Existing files still read correctly (full backward compatibility)

**What didn't change:**
- API unchanged — `writeParquet(schema, data, config)` / `readParquet(bytes)`
- All 27 existing tests pass, zero regressions

**⚠️ Breaking (internal):** WASM binaries were recompiled. If you vendor the `.wasm` files, update both `writer.wasm` and `reader.wasm` together.

### v0.2.0 — Snappy + Reader
- Snappy compression (default)
- `readParquet` — full read support
- Subpath imports (`tiny-parquet/reader`, `tiny-parquet/writer`)

### v0.1.0 — Initial Release
- `writeParquet` — flat schemas, 7 types
- Pure Rust/WASM, zero JS deps

---

## License

[MIT](./LICENSE) © [nktrchk](https://github.com/nktrchk)

<p align="center">
  <sub>Built by <a href="https://github.com/nktrchk">nktrchk</a> / <a href="https://enrich.sh">enrich.sh</a> — used in production processing millions of events.</sub>
</p>
