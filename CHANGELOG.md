# Changelog

## 0.2.0-beta.0 — Dictionary Encoding

String columns now get **dictionary encoded** automatically. If your column has ≤256 unique values (countries, browsers, event types...), we build a dictionary and store indices instead of repeating strings. Standard Parquet `RLE_DICTIONARY` — readable by DuckDB, Spark, Arrow, Pandas, everything.

**Numbers on 10K clickstream rows (3 string + 2 numeric cols, 50 runs median):**

```
         Dict      Plain
Write    7.3 ms    7.8 ms    ← 7% faster
Read     6.3 ms    6.9 ms    ← 9% faster
Size      72 KB    121 KB    ← 41% smaller
```

How: single-pass Vec-based encoder. No HashMap, no pre-scan. Linear search over ≤256 entries is faster than hashing. Falls back to PLAIN automatically if cardinality is too high.

New config: `{ dictionary: false }` to opt out. Default is ON.

WASM recompiled with Rust 1.93 / wasm-bindgen 0.2.112. If you vendor `.wasm` files, update both writer and reader together.

---

## 0.1.2

Bug fixes.

---

## 0.1.1 — Snappy + Reader

Added `readParquet`. Snappy compression on by default. Subpath imports: `tiny-parquet/reader`, `tiny-parquet/writer`.

---

## 0.1.0 — Initial Release

`writeParquet` — flat schemas, 7 types, pure Rust/WASM, zero deps. 306KB total.
