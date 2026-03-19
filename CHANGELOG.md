# Changelog

All notable changes to **tiny-parquet** will be documented in this file.

## [0.2.0-beta.0] — 2026-03-19

### Dictionary Encoding *(unstable)*

**Added:**
- Automatic dictionary encoding (`RLE_DICTIONARY`) for string columns with ≤256 unique values
- Single-pass Vec-based encoder — no HashMap, no pre-scan, zero-copy dictionary references
- Reader decodes both `PLAIN` and `RLE_DICTIONARY` / `PlainDictionary` pages
- New config option `{ dictionary: false }` to opt out (default: ON)
- Automatic fallback to PLAIN if cardinality exceeds 256

**Performance:**
- **7% faster writes** than v0.1.2 on typical edge data (clickstream, IoT, analytics)
- **Faster reads** — less data to decompress from smaller files
- **41% smaller files** on low-cardinality string columns (10–15 unique values across 10K+ rows)
- Validated with PyArrow (Apache Arrow reference implementation)

**Internal:**
- WASM recompiled: Rust 1.93, wasm-bindgen 0.2.112, wasm-opt 128
- Writer JS glue: added `__wbindgen_is_undefined` import
- Removed HashMap/HashSet from dictionary encoder — Vec linear search is faster for ≤256 entries

**Compatibility:**
- API unchanged — `writeParquet(schema, data, config)` / `readParquet(bytes)`
- Full backward compatibility — plain-encoded files still read correctly
- All 27 existing tests pass, zero regressions
- ⚠️ WASM binaries recompiled — if you vendor `.wasm` files, update both together

## [0.1.2] — 2026-02-23

### Patch
- Bug fixes and stability improvements

## [0.1.1] — 2026-02-23

### Snappy + Reader
- Snappy compression (default)
- `readParquet` — full read support
- Subpath imports (`tiny-parquet/reader`, `tiny-parquet/writer`)

## [0.1.0] — 2026-02-22

### Initial Release
- `writeParquet` — flat schemas, 7 column types
- Pure Rust/WASM, zero JS dependencies
- Runs on Cloudflare Workers, Vercel Edge, Deno, Bun, Node.js, Browser
