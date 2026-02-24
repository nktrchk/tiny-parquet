import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { writeParquet, readParquet } from '../src/index.js';

// ── Helpers ──────────────────────────────────────────────────────────────────

const schema = [
    { name: 'url', type: 'string' },
    { name: 'ts', type: 'int64' },
    { name: 'score', type: 'float64' },
    { name: 'active', type: 'boolean' },
];

function makeData(n) {
    return {
        url: Array.from({ length: n }, (_, i) => `https://example.com/page/${i}`),
        ts: Array.from({ length: n }, (_, i) => Date.now() + i),
        score: Array.from({ length: n }, () => Math.random() * 100),
        active: Array.from({ length: n }, () => Math.random() > 0.5),
    };
}

async function timeWrite(n, opts = { compression: 'snappy' }) {
    const data = makeData(n);
    const t0 = performance.now();
    const bytes = await writeParquet(schema, data, opts);
    return { ms: performance.now() - t0, bytes };
}

async function timeRead(bytes, n) {
    const t0 = performance.now();
    await readParquet(bytes, n);
    return { ms: performance.now() - t0 };
}

// Warm up WASM before perf tests
await writeParquet(schema, makeData(10));

// ── Throughput thresholds ────────────────────────────────────────────────────
// These are intentionally conservative — roughly 5x below observed perf
// so they catch catastrophic regressions without flaking on slower machines.

describe('write throughput', () => {
    it('1K rows under 50ms', async () => {
        const { ms } = await timeWrite(1_000);
        console.log(`    1K write: ${ms.toFixed(1)}ms`);
        assert.ok(ms < 50, `expected <50ms, got ${ms.toFixed(1)}ms`);
    });

    it('10K rows under 100ms', async () => {
        const { ms } = await timeWrite(10_000);
        console.log(`    10K write: ${ms.toFixed(1)}ms`);
        assert.ok(ms < 100, `expected <100ms, got ${ms.toFixed(1)}ms`);
    });

    it('100K rows under 500ms', async () => {
        const { ms } = await timeWrite(100_000);
        console.log(`    100K write: ${ms.toFixed(1)}ms`);
        assert.ok(ms < 500, `expected <500ms, got ${ms.toFixed(1)}ms`);
    });
});

describe('read throughput', () => {
    it('1K rows under 50ms', async () => {
        const { bytes } = await timeWrite(1_000);
        const { ms } = await timeRead(bytes, 1_000);
        console.log(`    1K read: ${ms.toFixed(1)}ms`);
        assert.ok(ms < 50, `expected <50ms, got ${ms.toFixed(1)}ms`);
    });

    it('10K rows under 100ms', async () => {
        const { bytes } = await timeWrite(10_000);
        const { ms } = await timeRead(bytes, 10_000);
        console.log(`    10K read: ${ms.toFixed(1)}ms`);
        assert.ok(ms < 100, `expected <100ms, got ${ms.toFixed(1)}ms`);
    });

    it('100K rows under 500ms', async () => {
        const { bytes } = await timeWrite(100_000);
        const { ms } = await timeRead(bytes, 100_000);
        console.log(`    100K read: ${ms.toFixed(1)}ms`);
        assert.ok(ms < 500, `expected <500ms, got ${ms.toFixed(1)}ms`);
    });
});

describe('file size', () => {
    it('1K rows under 50KB', async () => {
        const { bytes } = await timeWrite(1_000);
        const kb = bytes.length / 1024;
        console.log(`    1K size: ${kb.toFixed(0)}KB`);
        assert.ok(kb < 50, `expected <50KB, got ${kb.toFixed(0)}KB`);
    });

    it('10K rows under 500KB', async () => {
        const { bytes } = await timeWrite(10_000);
        const kb = bytes.length / 1024;
        console.log(`    10K size: ${kb.toFixed(0)}KB`);
        assert.ok(kb < 500, `expected <500KB, got ${kb.toFixed(0)}KB`);
    });

    it('100K rows under 5MB', async () => {
        const { bytes } = await timeWrite(100_000);
        const kb = bytes.length / 1024;
        console.log(`    100K size: ${kb.toFixed(0)}KB`);
        assert.ok(kb < 5120, `expected <5MB, got ${kb.toFixed(0)}KB`);
    });
});

describe('compression ratio', () => {
    it('snappy is at least 10% smaller than uncompressed', async () => {
        const data = makeData(10_000);
        const snappy = await writeParquet(schema, data, { compression: 'snappy' });
        const none = await writeParquet(schema, data, { compression: 'none' });
        const ratio = (1 - snappy.length / none.length) * 100;
        console.log(`    compression savings: ${ratio.toFixed(1)}% (${none.length}B → ${snappy.length}B)`);
        assert.ok(ratio > 10, `expected >10% savings, got ${ratio.toFixed(1)}%`);
    });
});

describe('latency at edge-scale (1K rows = typical flush)', () => {
    it('write + read roundtrip under 30ms', async () => {
        const data = makeData(1_000);
        const t0 = performance.now();
        const bytes = await writeParquet(schema, data, { compression: 'snappy' });
        await readParquet(bytes, 1_000);
        const ms = performance.now() - t0;
        console.log(`    1K roundtrip: ${ms.toFixed(1)}ms`);
        assert.ok(ms < 30, `expected <30ms, got ${ms.toFixed(1)}ms`);
    });
});

// ── Throughput by file size (edge-relevant) ──────────────────────────────────
// These match the documented benchmarks in README.md
// Thresholds are ~5x above observed to avoid CI flakes

describe('write throughput by file size', () => {
    it('~10KB write under 20ms', async () => {
        const { ms, bytes } = await timeWrite(559);
        const kb = (bytes.length / 1024).toFixed(0);
        console.log(`    ~10KB write: ${ms.toFixed(1)}ms (${kb}KB)`);
        assert.ok(ms < 20, `expected <20ms, got ${ms.toFixed(1)}ms`);
    });

    it('~100KB write under 50ms', async () => {
        const { ms, bytes } = await timeWrite(5_922);
        const kb = (bytes.length / 1024).toFixed(0);
        console.log(`    ~100KB write: ${ms.toFixed(1)}ms (${kb}KB)`);
        assert.ok(ms < 50, `expected <50ms, got ${ms.toFixed(1)}ms`);
    });

    it('~500KB write under 200ms', async () => {
        const { ms, bytes } = await timeWrite(29_809);
        const kb = (bytes.length / 1024).toFixed(0);
        console.log(`    ~500KB write: ${ms.toFixed(1)}ms (${kb}KB)`);
        assert.ok(ms < 200, `expected <200ms, got ${ms.toFixed(1)}ms`);
    });
});

describe('read throughput by file size', () => {
    it('~10KB read under 20ms', async () => {
        const { bytes } = await timeWrite(559);
        const { ms } = await timeRead(bytes, 559);
        console.log(`    ~10KB read: ${ms.toFixed(1)}ms`);
        assert.ok(ms < 20, `expected <20ms, got ${ms.toFixed(1)}ms`);
    });

    it('~100KB read under 50ms', async () => {
        const { bytes } = await timeWrite(5_922);
        const { ms } = await timeRead(bytes, 5_922);
        console.log(`    ~100KB read: ${ms.toFixed(1)}ms`);
        assert.ok(ms < 50, `expected <50ms, got ${ms.toFixed(1)}ms`);
    });

    it('~500KB read under 200ms', async () => {
        const { bytes } = await timeWrite(29_809);
        const { ms } = await timeRead(bytes, 29_809);
        console.log(`    ~500KB read: ${ms.toFixed(1)}ms`);
        assert.ok(ms < 200, `expected <200ms, got ${ms.toFixed(1)}ms`);
    });
});

// ── Mixed clickstream schema ─────────────────────────────────────────────────

const mixedSchema = [
    { name: 'path', type: 'string' },
    { name: 'status', type: 'int32' },
    { name: 'latency', type: 'float64' },
    { name: 'ts', type: 'timestamp' },
    { name: 'bot', type: 'boolean' },
    { name: 'ua', type: 'string' },
    { name: 'bytes', type: 'int64' },
];

function makeMixedData(n) {
    return {
        path: Array.from({ length: n }, (_, i) => `/page/${i}`),
        status: Array.from({ length: n }, () => [200, 301, 404, 500][Math.random() * 4 | 0]),
        latency: Array.from({ length: n }, () => Math.random() * 500),
        ts: Array.from({ length: n }, (_, i) => Date.now() + i),
        bot: Array.from({ length: n }, () => Math.random() > 0.8),
        ua: Array.from({ length: n }, () => 'Mozilla/5.0 (compatible; bot/1.0)'),
        bytes: Array.from({ length: n }, () => Math.floor(Math.random() * 100000)),
    };
}

describe('mixed schema throughput (7-col clickstream)', () => {
    it('~10KB mixed write under 20ms', async () => {
        const data = makeMixedData(304);
        const t0 = performance.now();
        const bytes = await writeParquet(mixedSchema, data, { compression: 'snappy' });
        const ms = performance.now() - t0;
        const kb = (bytes.length / 1024).toFixed(0);
        console.log(`    ~10KB mixed write: ${ms.toFixed(1)}ms (${kb}KB)`);
        assert.ok(ms < 20, `expected <20ms, got ${ms.toFixed(1)}ms`);
    });

    it('~100KB mixed write under 50ms', async () => {
        const data = makeMixedData(3_346);
        const t0 = performance.now();
        const bytes = await writeParquet(mixedSchema, data, { compression: 'snappy' });
        const ms = performance.now() - t0;
        const kb = (bytes.length / 1024).toFixed(0);
        console.log(`    ~100KB mixed write: ${ms.toFixed(1)}ms (${kb}KB)`);
        assert.ok(ms < 50, `expected <50ms, got ${ms.toFixed(1)}ms`);
    });

    it('~500KB mixed write under 200ms', async () => {
        const data = makeMixedData(16_912);
        const t0 = performance.now();
        const bytes = await writeParquet(mixedSchema, data, { compression: 'snappy' });
        const ms = performance.now() - t0;
        const kb = (bytes.length / 1024).toFixed(0);
        console.log(`    ~500KB mixed write: ${ms.toFixed(1)}ms (${kb}KB)`);
        assert.ok(ms < 200, `expected <200ms, got ${ms.toFixed(1)}ms`);
    });

    it('~500KB mixed roundtrip under 300ms', async () => {
        const data = makeMixedData(16_912);
        const t0 = performance.now();
        const bytes = await writeParquet(mixedSchema, data, { compression: 'snappy' });
        await readParquet(bytes, 16_912);
        const ms = performance.now() - t0;
        console.log(`    ~500KB mixed roundtrip: ${ms.toFixed(1)}ms`);
        assert.ok(ms < 300, `expected <300ms, got ${ms.toFixed(1)}ms`);
    });
});
