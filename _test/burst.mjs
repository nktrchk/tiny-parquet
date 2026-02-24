import { writeParquet } from '../src/index.js';

const schema = [
    { name: 'path', type: 'string' },
    { name: 'status', type: 'int32' },
    { name: 'latency', type: 'float64' },
    { name: 'ts', type: 'timestamp' },
    { name: 'bot', type: 'boolean' },
];

function makeData(n) {
    return {
        path: Array.from({ length: n }, (_, i) => `/api/event/${i}`),
        status: Array.from({ length: n }, () => [200, 301, 404][Math.random() * 3 | 0]),
        latency: Array.from({ length: n }, () => Math.random() * 500),
        ts: Array.from({ length: n }, (_, i) => Date.now() + i),
        bot: Array.from({ length: n }, () => Math.random() > 0.9),
    };
}

// warmup
await writeParquet(schema, makeData(10));

const RPS = 5_000;
// Row counts that produce roughly 10KB–100KB files
const rowCounts = [60, 120, 300, 600, 1200, 3000, 6000];

console.log(`Simulating ${RPS} sequential writes (single-threaded worst case)`);
console.log('Payload range: ~10–100KB\n');

let totalBytes = 0;
let totalMs = 0;
let maxMs = 0;
let minMs = Infinity;

const t0 = performance.now();
for (let i = 0; i < RPS; i++) {
    const rows = rowCounts[Math.random() * rowCounts.length | 0];
    const data = makeData(rows);
    const t1 = performance.now();
    const bytes = await writeParquet(schema, data, { compression: 'snappy' });
    const ms = performance.now() - t1;
    totalBytes += bytes.length;
    totalMs += ms;
    if (ms > maxMs) maxMs = ms;
    if (ms < minMs) minMs = ms;
}
const wallMs = performance.now() - t0;

console.log(`  Requests:      ${RPS}`);
console.log(`  Wall time:     ${(wallMs / 1000).toFixed(2)}s`);
console.log(`  Avg write:     ${(totalMs / RPS).toFixed(2)}ms`);
console.log(`  Min write:     ${minMs.toFixed(2)}ms`);
console.log(`  Max write:     ${maxMs.toFixed(2)}ms`);
console.log(`  Total data:    ${(totalBytes / 1024 / 1024).toFixed(1)}MB`);
console.log(`  Throughput:    ${((totalBytes / 1024 / 1024) / (wallMs / 1000)).toFixed(1)} MB/s`);
console.log(`  Effective RPS: ${Math.round(RPS / (wallMs / 1000))}/s (single thread)`);
console.log();

if (wallMs < 1000) {
    console.log(`  ✅ Single thread handles ${RPS} RPS — ${(wallMs).toFixed(0)}ms total`);
} else {
    const threadsNeeded = Math.ceil(wallMs / 1000);
    console.log(`  ⚠️  Single thread: ${Math.round(RPS / (wallMs / 1000))} RPS`);
    console.log(`  → Need ~${threadsNeeded} cores for ${RPS} RPS`);
    console.log(`  → On CF Workers / Vercel Edge: ✅ handled by platform (each request = own isolate)`);
}
