# Contributing to tiny-parquet

First off — thanks for being here! Whether it's a bug report, a feature idea, or a PR, every contribution helps make `tiny-parquet` better. 🙏

## Found a Bug?

Open an [issue](https://github.com/nktrchk/tiny-parquet/issues) with:

- What you expected
- What actually happened
- Runtime (Node, Deno, CF Workers, browser, etc.)
- A minimal code snippet if possible

## Have an Idea?

Open an issue and describe what you'd like to see. Even rough ideas are welcome — we can figure out the details together.

## Want to Submit a PR?

1. **Fork** the repo and create a branch from `main`
2. **Make your changes** — keep them focused and small if possible
3. **Test** that things work (`node _test/test.js` or similar)
4. **Open a PR** with a short description of what and why

That's it. No complex processes, no CLA.

### Building from Source

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

You'll need: Rust, `wasm-bindgen-cli`, and `wasm-opt` (from [binaryen](https://github.com/WebAssembly/binaryen)).

### Code Style

Nothing strict. Just keep it consistent with what's already there. Clean, readable, no over-engineering.

## Questions?

Open an issue or start a [discussion](https://github.com/nktrchk/tiny-parquet/discussions). No question is too small.

---

**MIT Licensed** — your contributions will be released under the same license.
