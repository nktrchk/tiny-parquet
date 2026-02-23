```
  ╔═════════════════════════════════════════════════════════════════╗
  ║                                                                 ║
  ║   ████████╗██╗███╗   ██╗██╗   ██╗                               ║
  ║   ╚══██╔══╝██║████╗  ██║╚██╗ ██╔╝                               ║
  ║      ██║   ██║██╔██╗ ██║ ╚████╔╝                                ║
  ║      ██║   ██║██║╚██╗██║  ╚██╔╝                                 ║
  ║      ██║   ██║██║ ╚████║   ██║                                  ║
  ║      ╚═╝   ╚═╝╚═╝  ╚═══╝   ╚═╝                                  ║
  ║                                                                 ║
  ║   ██████╗  █████╗ ██████╗  ██████╗ ██╗   ██╗███████╗████████╗   ║
  ║   ██╔══██╗██╔══██╗██╔══██╗██╔═══██╗██║   ██║██╔════╝╚══██╔══╝   ║
  ║   ██████╔╝███████║██████╔╝██║   ██║██║   ██║█████╗     ██║      ║
  ║   ██╔═══╝ ██╔══██║██╔══██╗██║▄▄ ██║██║   ██║██╔══╝     ██║      ║
  ║   ██║     ██║  ██║██║  ██║╚██████╔╝╚██████╔╝███████╗   ██║      ║
  ║   ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝ ╚══▀▀═╝  ╚═════╝ ╚══════╝   ╚═╝      ║
  ║                                                                 ║
  ║   Read & write Apache Parquet in 326KB of WASM.                 ║
  ║   Two functions. Zero dependencies.                             ║
  ║                                                                 ║
  ╚═════════════════════════════════════════════════════════════════╝
```

<p align="center">
  <strong>The only Parquet library that fits on Cloudflare Workers free tier and Vercel Edge.</strong>
</p>

<p align="center">
  <a href="https://www.npmjs.com/package/tiny-parquet"><img src="https://img.shields.io/npm/v/tiny-parquet.svg?style=flat-square&color=cb3837" alt="npm version"></a>
  <a href="https://www.npmjs.com/package/tiny-parquet"><img src="https://img.shields.io/npm/dm/tiny-parquet.svg?style=flat-square&color=blue" alt="npm downloads"></a>
  <a href="https://github.com/nktrchk/tiny-parquet/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-green.svg?style=flat-square" alt="license"></a>
  <a href="https://github.com/nktrchk/tiny-parquet"><img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square" alt="PRs Welcome"></a>
</p>

---

## Why

You're building on the edge. You need Parquet. But:

```
  parquet-wasm    3,500 KB  ❌  Too fat for Vercel Edge & CF free tier
  duckdb-wasm    8,000 KB   ❌  Way too fat
  parquetjs        500 KB   ❌  Node.js only

  tiny-parquet     319 KB   ✅  Runs everywhere
```

**tiny-parquet** gives you `readParquet` and `writeParquet` — two async functions backed by Rust + WASM via the [`parquet2`](https://crates.io/crates/parquet2) crate. That's it.

---

## Performance

Benchmarked on Node.js (Apple Silicon). First call includes WASM compilation warmup.

```
  Rows       Write      Read      File Size
  ─────────────────────────────────────────
   1,000     29.3 ms     7.1 ms      20 KB
   5,000      8.7 ms     4.9 ms     102 KB
  10,000      8.6 ms     7.2 ms     204 KB
```

After warmup: **~1M+ rows/sec reads**, **~1M+ rows/sec writes**. Fast enough for edge runtimes where latency matters.

---

## Install

```bash
npm install tiny-parquet
```

---

## Quick Start

```js
import { writeParquet, readParquet } from 'tiny-parquet';

// ── Write ────────────────────────────────────────────────────────
const bytes = await writeParquet(
  [
    { name: 'url',    type: 'string' },
    { name: 'ts',     type: 'int64' },
    { name: 'score',  type: 'float64' },
    { name: 'active', type: 'boolean' },
  ],
  {
    url:    ['https://example.com', 'https://test.dev'],
    ts:     [1708000000, 1708100000],
    score:  [98.5, 76.3],
    active: [true, false],
  },
  { compression: 'snappy' }
);

// ── Read ─────────────────────────────────────────────────────────
const { schema, data, numRows } = await readParquet(bytes);

console.log(schema);   // [{ name: 'url', type: 'string' }, ...]
console.log(data.url); // ['https://example.com', 'https://test.dev']
console.log(numRows);  // 2
```

### Subpath Imports

```js
// Import only what you need — smaller bundles
import { readParquet }  from 'tiny-parquet/reader';
import { writeParquet } from 'tiny-parquet/writer';
```

---

## Runs Everywhere

```
  ┌─────────────────────────────────┬────────┐
  │ Runtime                         │ Status │
  ├─────────────────────────────────┼────────┤
  │ Cloudflare Workers              │   ✅   │
  │ Vercel Edge Functions           │   ✅   │
  │ Deno / Deno Deploy              │   ✅   │
  │ Bun                             │   ✅   │
  │ Node.js (≥18)                   │   ✅   │
  │ Browser (Chrome/FF/Safari)      │   ✅   │
  │ AWS Lambda                      │   ✅   │
  │ Google Cloud Functions          │   ✅   │
  │ Fastly Compute                  │   ✅   │
  │ Electron                        │   ✅   │
  └─────────────────────────────────┴────────┘
```

---

## Size Comparison

```
  ┌──────────────────┬─────────┬──────────────────────────────────┬───────────────────────────┐
  │ Package          │ Size    │ CF Workers free (1MB) │ Vercel Edge (1MB soft)               │
  ├──────────────────┼─────────┼──────────────────────────────────┼───────────────────────────┤
  │ parquet-wasm     │ 3.5 MB  │        ❌ No                      │        ❌ No              │
  │ duckdb-wasm      │ 8.0 MB  │        ❌ No                      │        ❌ No              │
  │ parquetjs        │ 500 KB  │        ❌ Node only               │        ❌ Node only       │
  │ tiny-parquet     │ 319 KB  │        ✅ Yes                     │        ✅ Yes             │
  └──────────────────┴─────────┴──────────────────────────────────┴───────────────────────────┘
```

---

## Anatomy

```
  ┌───────────────────────────────────────────────┐
  │              tiny-parquet                     │
  │                                               │
  │   ┌─────────────┐      ┌──────────────┐       │
  │   │ writer.wasm │      │ reader.wasm  │       │
  │   │   179 KB    │      │   140 KB     │       │
  │   │             │      │              │       │
  │   │  Rust +     │      │  Rust +      │       │
  │   │  parquet2   │      │  parquet2    │       │
  │   │  + snappy   │      │  + snappy    │       │
  │   └──────┬──────┘      └──────┬───────┘       │
  │          │                    │               │
  │   ┌──────▼──────┐      ┌──────▼───────┐       │
  │   │ writer.js   │      │ reader.js    │       │
  │   │  JS glue    │      │  JS glue     │       │
  │   │  ~120 LOC   │      │  ~100 LOC    │       │
  │   └─────────────┘      └──────────────┘       │
  │                                               │
  │          Total: 319 KB  ·  337 LOC            │
  └───────────────────────────────────────────────┘
```

---

## API

### `writeParquet(schema, data, config?)`

Creates a Parquet file from columnar data.

| Param | Type | Description |
|-------|------|-------------|
| `schema` | `Array<{ name: string, type: string }>` | Column definitions |
| `data` | `Record<string, any[]>` | Columnar data keyed by column name |
| `config` | `{ compression?: 'snappy' \| 'none' }` | Options (default: `snappy`) |
| **Returns** | `Promise<Uint8Array>` | Raw Parquet file bytes |

#### Supported Types

| Type | Parquet Physical | Notes |
|------|-----------------|-------|
| `string` | `BYTE_ARRAY` (UTF-8) | Default if unrecognized |
| `int32` | `INT32` | |
| `int64` | `INT64` | |
| `float32` | `FLOAT` | Alias: `float` |
| `float64` | `DOUBLE` | Alias: `double` |
| `boolean` | `BOOLEAN` | Alias: `bool` |
| `timestamp` | `INT64` (millis, UTC) | Alias: `timestamp_millis` |

### `readParquet(bytes, maxRows?)`

Reads a Parquet file into columnar data.

| Param | Type | Description |
|-------|------|-------------|
| `bytes` | `Uint8Array` | Raw Parquet file bytes |
| `maxRows` | `number` | Max rows to decode (default: `500`) |
| **Returns** | `Promise<{ schema, data, numRows }>` | Parsed result |

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
  const bytes = new Uint8Array(await res.arrayBuffer());
  const { data, numRows } = await readParquet(bytes);

  return Response.json({ numRows, sample: data });
}
```

### Node.js

```js
import { readFileSync, writeFileSync } from 'node:fs';
import { writeParquet, readParquet } from 'tiny-parquet';

// Write
const bytes = await writeParquet(
  [{ name: 'city', type: 'string' }, { name: 'pop', type: 'int64' }],
  { city: ['Berlin', 'London', 'Tokyo'], pop: [3748148, 8982000, 13960000] },
);
writeFileSync('cities.parquet', bytes);

// Read
const { schema, data, numRows } = await readParquet(readFileSync('cities.parquet'));
console.log(`${numRows} rows:`, data);
```

### Deno

```ts
import { writeParquet, readParquet } from 'npm:tiny-parquet';

const bytes = await writeParquet(
  [{ name: 'msg', type: 'string' }],
  { msg: ['Hello from Deno!'] },
);

const result = await readParquet(bytes);
console.log(result.data.msg); // ['Hello from Deno!']
```

---

## Rust Source

The WASM binaries are compiled from Rust using [`parquet2`](https://crates.io/crates/parquet2) — a lightweight, zero-copy Parquet implementation.

```
  rust/
  ├── parquet-reader/       140KB WASM
  │   ├── Cargo.toml
  │   └── src/lib.rs        180 lines
  └── parquet-writer/       179KB WASM
      ├── Cargo.toml
      └── src/lib.rs        258 lines
```

### Build from Source

```bash
# Writer
cd parquet-writer
cargo build --target wasm32-unknown-unknown --release
wasm-bindgen target/wasm32-unknown-unknown/release/parquet_flake.wasm \
  --out-dir pkg --target web
wasm-opt pkg/parquet_flake_bg.wasm -o ../wasm/writer.wasm -Oz

# Reader
cd ../parquet-reader
cargo build --target wasm32-unknown-unknown --release
wasm-bindgen target/wasm32-unknown-unknown/release/parquet_reader.wasm \
  --out-dir pkg --target web
wasm-opt pkg/parquet_reader_bg.wasm -o ../wasm/reader.wasm -Oz
```

---

## Roadmap

- [ ] **Dictionary encoding** — 60-80% size reduction on low-cardinality columns
- [ ] **Column pruning** — Read only the columns you need
- [ ] **Row group control** — Multiple row groups per file
- [ ] **Zstd compression** — Better ratio than Snappy

---

## FAQ

**Q: How does this compare to `parquet-wasm`?**
A: `parquet-wasm` is a full-featured Parquet library at 3.5MB. `tiny-parquet` is 10x smaller by supporting only flat schemas and essential types — perfect for edge runtimes where size limits apply.

**Q: Can I read files written by DuckDB / Spark / PyArrow?**
A: Yes — the reader handles standard Parquet files with flat schemas. Nested types (structs, lists, maps) are not supported yet.

**Q: Is Snappy compression supported?**
A: Yes, on both read and write. It's the default compression.

**Q: What about TypeScript?**
A: Full type declarations are included (`*.d.ts`). Just import and go.

---

## License

[MIT](./LICENSE) © [nktrchk](https://github.com/nktrchk)

---

<p align="center">
  <sub>Built by <a href="https://github.com/nktrchk">nktrchk</a> / <a href="https://enrich.sh">enrich.sh</a> — used in production processing millions of events.</sub>
</p>
