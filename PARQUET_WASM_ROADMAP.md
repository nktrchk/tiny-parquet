# tiny-parquet — WASM Parquet for Edge & Browser

**npm name: `tiny-parquet`** — confirmed available ✅

## Why Open Source This

### Personal Benefits
- **Portfolio piece**: Rust + WASM + data infra + byte-level file format knowledge = rare combo that signals senior/staff-level engineering
- **HN front page**: "319KB Parquet reader/writer for Edge" is exactly the kind of post that gets 300+ upvotes
- **GitHub stars**: Realistic target 500-2K stars — a niche utility with no competition
- **Networking**: DuckDB team, Cloudflare devrel, Vercel edge team, data infra founders will find you
- **Inbound for enrich.sh**: Every npm install page and README says "Built by enrich.sh" — free top-of-funnel forever
- **Low maintenance**: 2 functions, stable Parquet spec, ~5 issues/year max

### Business Benefits
- **Credibility**: Shows enrich.sh is built on real infrastructure, not wrappers
- **Community contributions**: Someone will add Zstd, column pruning, or Node.js streaming for free
- **Hiring signal**: Engineers want to work at companies that open source good infra
- **Moat is the product**: The Parquet format is open spec — the moat is enrich.sh's pipeline, not the codec

## Where It Runs

WASM runs everywhere JavaScript runs. Zero native dependencies.

| Runtime | Status | How WASM loads | Use case |
|---------|--------|---------------|----------|
| **Vercel Edge Functions** | ✅ Works | `import wasm` | Next.js edge routes |
| **Cloudflare Workers** | ✅ Production | `import wasm` | Edge serverless |
| **Deno / Deno Deploy** | ✅ Works | `import wasm` or `fetch()` | Edge serverless |
| **Bun** | ✅ Works | Native WASM support | Fast serverless |
| **Browser (Chrome/FF/Safari)** | ✅ Works | `fetch()` + `WebAssembly.instantiate` | Data tools, CSV→Parquet apps |
| **Node.js** | ✅ Works | `fs.readFileSync` | CLI tools, scripts, backends |
| **AWS Lambda** | ✅ Works | `fs.readFileSync` | Serverless (faster cold start) |
| **Google Cloud Functions** | ✅ Works | `fs.readFileSync` | Serverless |
| **Fastly Compute** | ✅ Works | WASM native | Edge compute |
| **Electron** | ✅ Works | Same as browser | Desktop data tools |
| **React Native** | ⚠️ Partial | JSC has WASM support on newer versions | Mobile data apps |

**Key insight:** parquet-wasm (3.5MB) often gets rejected by edge runtimes due to size limits. tiny-parquet (319KB) fits everywhere.

### Edge Runtime Size Limits

| Platform | WASM size limit | parquet-wasm fits? | tiny-parquet fits? |
|----------|----------------|-------------------|-------------------|
| Cloudflare Workers (free) | 1MB | ❌ | ✅ |
| Cloudflare Workers (paid) | 10MB | ✅ | ✅ |
| Vercel Edge | 1MB (soft), 4MB hard | ❌ | ✅ |
| Deno Deploy | 10MB | ✅ | ✅ |
| Fastly Compute | 100MB | ✅ | ✅ |
| AWS Lambda | 50MB (layer) | ✅ | ✅ |

**tiny-parquet is the only option for Cloudflare free tier and Vercel Edge.** That's the pitch.

---
| Writer | 179KB | 199 lines | `parquet2` | `writeParquet(schema, data, config)` |
| Reader | 140KB | 138 lines | `parquet2` | `readParquet(bytes, maxRows)` |
| **Total** | **319KB** | **337 lines** | | **2 functions** |

For comparison: `parquet-wasm` = 3.5MB, `duckdb-wasm` = 8MB.

### Supported types
- `string` (UTF8), `int32`, `int64`, `float64`, `boolean`, `timestamp`
- Flat schemas only (no nested structs/lists/maps)

### Not supported (yet)
- Dictionary encoding
- Compression (Snappy, Zstd, LZ4)
- Nested types (struct, list, map)
- Row group metadata merging (for compaction)
- Predicate pushdown / column pruning on read

---

## Upgrade Roadmap

### Phase 1: Dictionary Encoding (Priority: High)
**Why:** Low-cardinality string columns (`country`, `browser_name`, `domain_category`) repeat heavily. Dictionary encoding replaces repeated strings with integer IDs.

**Impact:** 60-80% size reduction on columns like `country` (200 unique values across 100K rows → 200 strings + 100K int32 refs instead of 100K strings).

**Implementation:**
- [ ] Enable `Encoding::RleDictionary` in the Rust writer for string columns
- [ ] Auto-detect: use dictionary if cardinality < 10% of row count, plain otherwise
- [ ] Reader already handles dictionary pages (parquet2 does this transparently)
- [ ] Config option: `{ dictionary: true }` (default: true)

**Estimate:** 2-3 hours (Rust changes + WASM recompile)

### Phase 2: Snappy Compression (Priority: Medium)
**Why:** Block-level compression on top of dictionary encoding. Snappy is the Parquet default — fast decompression, moderate ratio.

**Impact:** Additional 30-40% size reduction. Combined with dictionary: **~75-85% total reduction**.

**Implementation:**
- [ ] Add `snap` crate to Cargo.toml (pure Rust, no C deps — WASM-safe)
- [ ] Set compression to Snappy per column group
- [ ] Reader: add Snappy decompression (parquet2 handles this with feature flag)
- [ ] Config option: `{ compression: 'snappy' | 'none' }` (default: snappy)

**Estimate:** 1-2 hours

**WASM size impact:** +20-30KB (snap crate is tiny)

### Phase 3: Row Group Control (Priority: For Compactor)
**Why:** Compactor needs to write files with multiple row groups (one per source file) OR merge row groups efficiently.

**Implementation:**
- [ ] Writer: accept `rowGroupSize` config (default: all rows in one group)
- [ ] Writer: support multiple `writeRowGroup()` calls before `finish()`
- [ ] Reader: expose row group metadata (count, row count per group, byte offsets)

**Estimate:** 3-4 hours

### Phase 4: Column Pruning on Read (Priority: Nice-to-have)
**Why:** When querying, users often need only 3-4 columns out of 20+. Reading only needed columns = less memory + faster.

**Implementation:**
- [ ] `readParquet(bytes, { columns: ['url', 'timestamp', 'country'] })`
- [ ] Skip column chunks not in the selection list
- [ ] parquet2 supports this natively via column selection

**Estimate:** 2 hours

---

## Open Source Plan: `tiny-parquet`

### Positioning
> Read and write Parquet files in 319KB of WASM. Two functions. Zero dependencies.
> ✅ Vercel Edge  ✅ Cloudflare Workers  ✅ Deno  ✅ Bun  ✅ Node.js  ✅ Browser

Lead with the problem ("Parquet on edge runtimes"), not the platform. List all runtimes so every developer thinks "this works for me".

### Competitive landscape

| Package | Size | Edge? | Read | Write | Compression |
|---------|------|-------|------|-------|-------------|
| `parquet-wasm` | 3.5MB | ❌ Too big for Vercel/CF free | ✅ | ✅ | ✅ |
| `duckdb-wasm` | 8MB | ❌ Way too big | ✅ | ❌ | ✅ |
| `parquetjs` | 500KB (JS) | ❌ Node only | ✅ | ✅ | Partial |
| **`tiny-parquet`** | **319KB** | **✅ Runs everywhere** | ✅ | ✅ | 🔜 |

### Target audience
1. **Cloudflare Workers / Deno Deploy / Vercel Edge** — can't use 3.5MB WASM
2. **Browser apps** — data tools, CSV→Parquet converters, local-first analytics
3. **Serverless** — Lambda/GCF where cold start matters (319KB vs 3.5MB)

### Package structure
```
tiny-parquet/
├── package.json
├── README.md
├── LICENSE (MIT)
├── src/
│   ├── reader.js          # JS glue (browser + Worker variants)
│   ├── writer.js          # JS glue
│   ├── reader.wasm        # 140KB
│   └── writer.wasm        # 179KB
├── rust/
│   ├── reader/            # Rust source
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   └── writer/
│       ├── Cargo.toml
│       └── src/lib.rs
└── examples/
    ├── cloudflare-worker/
    ├── browser/
    └── node/
```

### API (public)
```js
import { readParquet } from 'tiny-parquet/reader';
import { writeParquet } from 'tiny-parquet/writer';

// Write
const bytes = await writeParquet(
  [{ name: 'url', type: 'string' }, { name: 'ts', type: 'int64' }],
  [{ url: 'https://example.com', ts: 1708000000 }],
  { dictionary: true, compression: 'snappy' }
);

// Read
const { schema, data, numRows } = await readParquet(bytes);
// schema: [{ name: 'url', type: 'string' }, ...]
// data: { url: ['https://example.com'], ts: [1708000000] }
// numRows: 1
```

### Launch checklist
- [ ] Extract reader/writer from enrich backend into standalone repo
- [ ] Adapt JS glue to support both `import wasm` (Worker) and `fetch()` (browser)
- [ ] Add Node.js support (`fs.readFileSync` for WASM loading)
- [ ] Write README with benchmarks vs parquet-wasm
- [ ] Publish to npm: `npm publish --access public`
- [ ] Post on HN / Twitter / Reddit r/javascript

### Marketing angle
- "10x smaller than parquet-wasm"
- "The only Parquet library that fits on Vercel Edge and Cloudflare Workers free tier"
- "Read and write Parquet in 319KB of WASM"
- README footer: Built by nktrchk / [enrich.sh](https://enrich.sh) — used in production

### Launch plan
1. Post on Hacker News: "Show HN: tiny-parquet — 319KB Parquet reader/writer for edge runtimes"
2. Tweet thread showing size comparison + code example
3. Reddit: r/javascript, r/webdev, r/rust
4. Dev.to blog post: "Why I built a 10x smaller Parquet library"

---

## Build Commands (for reference)

```bash
# Writer
cd parquet-writer
cargo build --target wasm32-unknown-unknown --release
wasm-bindgen target/wasm32-unknown-unknown/release/parquet_flake.wasm --out-dir pkg --target web
wasm-opt pkg/parquet_flake_bg.wasm -o pkg/parquet_flake_bg.wasm -Oz

# Reader
cd parquet-reader
cargo build --target wasm32-unknown-unknown --release
wasm-bindgen target/wasm32-unknown-unknown/release/parquet_reader.wasm --out-dir pkg --target web
wasm-opt pkg/parquet_reader_bg.wasm -o pkg/parquet_reader_bg.wasm -Oz
```
