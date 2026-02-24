import { writeParquet, readParquet } from '../src/index.js';

// ── Schemas ──────────────────────────────────────────────────────────────────

const schemas = {
    narrow: [
        { name: 'id', type: 'int32' },
        { name: 'value', type: 'string' },
    ],
    wide: [
        { name: 'url', type: 'string' },
        { name: 'ts', type: 'int64' },
        { name: 'score', type: 'float64' },
        { name: 'active', type: 'boolean' },
    ],
    mixed: [
        { name: 'path', type: 'string' },
        { name: 'status', type: 'int32' },
        { name: 'latency', type: 'float64' },
        { name: 'ts', type: 'timestamp' },
        { name: 'bot', type: 'boolean' },
        { name: 'ua', type: 'string' },
        { name: 'bytes', type: 'int64' },
    ],
};

function makeData(schema, n) {
    const d = {};
    for (const col of schema) {
        switch (col.type) {
            case 'string':
                d[col.name] = Array.from({ length: n }, (_, i) => `https://example.com/${col.name}/${i}`);
                break;
            case 'int32':
                d[col.name] = Array.from({ length: n }, (_, i) => i);
                break;
            case 'int64':
            case 'timestamp':
                d[col.name] = Array.from({ length: n }, (_, i) => Date.now() + i);
                break;
            case 'float64':
                d[col.name] = Array.from({ length: n }, () => Math.random() * 1000);
                break;
            case 'boolean':
                d[col.name] = Array.from({ length: n }, () => Math.random() > 0.5);
                break;
        }
    }
    return d;
}

// ── Find row count that produces ~target file size ───────────────────────────

async function findRowsForSize(schema, targetKB) {
    // Start with an estimate, binary search to get close
    let lo = 1, hi = 200_000, best = 10;
    // Quick calibration: measure 100 rows to estimate bytes/row
    const cal = await writeParquet(schema, makeData(schema, 100), { compression: 'snappy' });
    const bpr = cal.length / 100;
    best = Math.max(1, Math.round((targetKB * 1024) / bpr));

    // Refine once
    const check = await writeParquet(schema, makeData(schema, best), { compression: 'snappy' });
    const ratio = (targetKB * 1024) / check.length;
    best = Math.max(1, Math.round(best * ratio));

    return best;
}

// ── Benchmark runner ─────────────────────────────────────────────────────────

async function bench(label, schema, rows) {
    const data = makeData(schema, rows);

    const t0 = performance.now();
    const bytes = await writeParquet(schema, data, { compression: 'snappy' });
    const writeMs = performance.now() - t0;

    const t1 = performance.now();
    await readParquet(bytes, rows);
    const readMs = performance.now() - t1;

    const sizeKB = bytes.length / 1024;
    const writeTP = (sizeKB / 1024) / (writeMs / 1000); // MB/s
    const readTP = (sizeKB / 1024) / (readMs / 1000);   // MB/s

    const fmt = (v, w) => String(v).padStart(w);
    const fmtN = (n) => String(n).replace(/\B(?=(\d{3})+(?!\d))/g, ',');

    console.log(
        `  ${label.padEnd(22)}` +
        `${fmt(sizeKB.toFixed(0), 7)} KB` +
        `${fmt(fmtN(rows), 10)} rows` +
        `${fmt(writeMs.toFixed(1), 9)} ms` +
        `${fmt(readMs.toFixed(1), 9)} ms` +
        `${fmt(writeTP.toFixed(1), 9)} MB/s` +
        `${fmt(readTP.toFixed(1), 9)} MB/s`
    );
}

// ── Run ──────────────────────────────────────────────────────────────────────

console.log('Warming up WASM...');
await writeParquet(schemas.wide, makeData(schemas.wide, 10));
console.log('Warm.\n');

// ─── By file size (edge-relevant targets) ────────────────────────────────────

console.log('  By output file size');
console.log('  ──────────────────────────────────────────────────────────────────────────────────────');
console.log('  Target                  Size       Rows     Write      Read   Write TP    Read TP');
console.log('  ──────────────────────────────────────────────────────────────────────────────────────');

const sizeTargets = [1, 10, 20, 100, 500];

for (const kb of sizeTargets) {
    const rows = await findRowsForSize(schemas.wide, kb);
    await bench(`~${kb}KB (wide)`, schemas.wide, rows);
}

console.log();

// ─── Mixed schema at edge-relevant sizes ─────────────────────────────────────

console.log('  Mixed schema (7 cols — realistic clickstream)');
console.log('  ──────────────────────────────────────────────────────────────────────────────────────');

for (const kb of sizeTargets) {
    const rows = await findRowsForSize(schemas.mixed, kb);
    await bench(`~${kb}KB (mixed)`, schemas.mixed, rows);
}

console.log();

// ─── By row count (original bench) ───────────────────────────────────────────

console.log('  By row count');
console.log('  ──────────────────────────────────────────────────────────────────────────────────────');

for (const n of [1_000, 5_000, 10_000, 50_000, 100_000]) {
    await bench(`${(n / 1000)}K rows (wide)`, schemas.wide, n);
}

console.log();
