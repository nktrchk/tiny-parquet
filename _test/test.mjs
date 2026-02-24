import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { writeParquet, readParquet } from '../src/index.js';

// ── Helpers ──────────────────────────────────────────────────────────────────

function roundtrip(schema, data, opts = {}) {
    return async () => {
        const bytes = await writeParquet(schema, data, opts);
        assert.ok(bytes instanceof Uint8Array, 'writeParquet should return Uint8Array');
        assert.ok(bytes.length > 0, 'output should not be empty');

        const rowCount = Object.values(data)[0]?.length ?? 0;
        const result = await readParquet(bytes, rowCount);

        assert.ok(result.schema, 'result should have schema');
        assert.ok(result.data, 'result should have data');
        assert.equal(result.numRows, rowCount, `numRows should be ${rowCount}`);

        // Verify schema names match
        const expectedNames = schema.map(c => c.name);
        const actualNames = result.schema.map(c => c.name);
        assert.deepEqual(actualNames, expectedNames, 'schema column names should match');

        return result;
    };
}

// ── Types ────────────────────────────────────────────────────────────────────

describe('types', () => {
    it('string roundtrip', async () => {
        const schema = [{ name: 'val', type: 'string' }];
        const data = { val: ['hello', 'world', '', 'café', '🚀'] };
        const result = await roundtrip(schema, data)();
        assert.deepEqual(result.data.val, data.val);
    });

    it('int32 roundtrip', async () => {
        const schema = [{ name: 'val', type: 'int32' }];
        const data = { val: [0, 1, -1, 2147483647, -2147483648] };
        const result = await roundtrip(schema, data)();
        assert.deepEqual(result.data.val, data.val);
    });

    it('int64 roundtrip', async () => {
        const schema = [{ name: 'val', type: 'int64' }];
        const data = { val: [0, 1, -1, 9007199254740991] }; // up to Number.MAX_SAFE_INTEGER
        const result = await roundtrip(schema, data)();
        assert.deepEqual(result.data.val, data.val);
    });

    it('float32 roundtrip', async () => {
        const schema = [{ name: 'val', type: 'float32' }];
        const data = { val: [0.0, 1.5, -1.5, 3.14] };
        const result = await roundtrip(schema, data)();
        // float32 has limited precision, check approximate
        for (let i = 0; i < data.val.length; i++) {
            assert.ok(
                Math.abs(result.data.val[i] - data.val[i]) < 0.01,
                `float32[${i}]: expected ~${data.val[i]}, got ${result.data.val[i]}`
            );
        }
    });

    it('float64 roundtrip', async () => {
        const schema = [{ name: 'val', type: 'float64' }];
        const data = { val: [0.0, Math.PI, -Math.E, 1e100, Number.MIN_SAFE_INTEGER] };
        const result = await roundtrip(schema, data)();
        assert.deepEqual(result.data.val, data.val);
    });

    it('boolean roundtrip', async () => {
        const schema = [{ name: 'val', type: 'boolean' }];
        const data = { val: [true, false, true, true, false] };
        const result = await roundtrip(schema, data)();
        assert.deepEqual(result.data.val, data.val);
    });

    it('timestamp roundtrip', async () => {
        const schema = [{ name: 'val', type: 'timestamp' }];
        const now = Date.now();
        const data = { val: [now, now - 86400000, 0] };
        const result = await roundtrip(schema, data)();
        assert.deepEqual(result.data.val, data.val);
    });
});

// ── Multi-column schemas ─────────────────────────────────────────────────────

describe('mixed schemas', () => {
    it('all types in one schema', async () => {
        const schema = [
            { name: 'str', type: 'string' },
            { name: 'i32', type: 'int32' },
            { name: 'i64', type: 'int64' },
            { name: 'f64', type: 'float64' },
            { name: 'flag', type: 'boolean' },
            { name: 'ts', type: 'timestamp' },
        ];
        const data = {
            str: ['a', 'b', 'c'],
            i32: [1, 2, 3],
            i64: [100, 200, 300],
            f64: [1.1, 2.2, 3.3],
            flag: [true, false, true],
            ts: [Date.now(), Date.now() - 1000, Date.now() + 1000],
        };
        const result = await roundtrip(schema, data)();
        assert.deepEqual(result.data.str, data.str);
        assert.deepEqual(result.data.i32, data.i32);
        assert.deepEqual(result.data.i64, data.i64);
        assert.deepEqual(result.data.f64, data.f64);
        assert.deepEqual(result.data.flag, data.flag);
        assert.deepEqual(result.data.ts, data.ts);
    });

    it('typical clickstream event', async () => {
        const schema = [
            { name: 'url', type: 'string' },
            { name: 'ts', type: 'timestamp' },
            { name: 'status', type: 'int32' },
            { name: 'latency', type: 'float64' },
            { name: 'bot', type: 'boolean' },
        ];
        const data = {
            url: ['https://example.com/page', '/api/data', '/health'],
            ts: [Date.now(), Date.now(), Date.now()],
            status: [200, 404, 200],
            latency: [12.5, 45.2, 0.8],
            bot: [false, true, false],
        };
        const result = await roundtrip(schema, data)();
        assert.deepEqual(result.data.url, data.url);
        assert.deepEqual(result.data.status, data.status);
    });
});

// ── Payload sizes ────────────────────────────────────────────────────────────

describe('payload sizes', () => {
    const schema = [
        { name: 'id', type: 'int32' },
        { name: 'value', type: 'string' },
        { name: 'score', type: 'float64' },
    ];

    function makeData(n) {
        return {
            id: Array.from({ length: n }, (_, i) => i),
            value: Array.from({ length: n }, (_, i) => `item-${i}`),
            score: Array.from({ length: n }, () => Math.random() * 100),
        };
    }

    it('single row', async () => {
        const result = await roundtrip(schema, makeData(1))();
        assert.equal(result.numRows, 1);
    });

    it('10 rows', async () => {
        const result = await roundtrip(schema, makeData(10))();
        assert.equal(result.numRows, 10);
    });

    it('100 rows', async () => {
        const result = await roundtrip(schema, makeData(100))();
        assert.equal(result.numRows, 100);
    });

    it('1,000 rows', async () => {
        const result = await roundtrip(schema, makeData(1_000))();
        assert.equal(result.numRows, 1_000);
    });

    it('10,000 rows', async () => {
        const result = await roundtrip(schema, makeData(10_000))();
        assert.equal(result.numRows, 10_000);
    });

    it('50,000 rows', async () => {
        const result = await roundtrip(schema, makeData(50_000))();
        assert.equal(result.numRows, 50_000);
    });
});

// ── Compression ──────────────────────────────────────────────────────────────

describe('compression', () => {
    const schema = [{ name: 'msg', type: 'string' }];
    const data = { msg: Array.from({ length: 100 }, (_, i) => `repeated-value-${i % 10}`) };

    it('snappy (default)', async () => {
        const result = await roundtrip(schema, data, { compression: 'snappy' })();
        assert.equal(result.numRows, 100);
    });

    it('none', async () => {
        const result = await roundtrip(schema, data, { compression: 'none' })();
        assert.equal(result.numRows, 100);
    });

    it('snappy produces smaller output than none', async () => {
        const snappy = await writeParquet(schema, data, { compression: 'snappy' });
        const none = await writeParquet(schema, data, { compression: 'none' });
        assert.ok(snappy.length < none.length, `snappy (${snappy.length}B) should be smaller than none (${none.length}B)`);
    });
});

// ── Edge cases ───────────────────────────────────────────────────────────────

describe('edge cases', () => {
    it('empty strings', async () => {
        const schema = [{ name: 'val', type: 'string' }];
        const data = { val: ['', '', ''] };
        const result = await roundtrip(schema, data)();
        assert.deepEqual(result.data.val, data.val);
    });

    it('long strings', async () => {
        const schema = [{ name: 'val', type: 'string' }];
        const longStr = 'x'.repeat(10_000);
        const data = { val: [longStr, longStr] };
        const result = await roundtrip(schema, data)();
        assert.equal(result.data.val[0].length, 10_000);
    });

    it('unicode and emoji', async () => {
        const schema = [{ name: 'val', type: 'string' }];
        const data = { val: ['日本語', '한국어', '🎉🔥💯', 'Ñoño'] };
        const result = await roundtrip(schema, data)();
        assert.deepEqual(result.data.val, data.val);
    });

    it('zero values across types', async () => {
        const schema = [
            { name: 'i', type: 'int32' },
            { name: 'f', type: 'float64' },
            { name: 'b', type: 'boolean' },
        ];
        const data = { i: [0, 0, 0], f: [0.0, 0.0, 0.0], b: [false, false, false] };
        const result = await roundtrip(schema, data)();
        assert.deepEqual(result.data.i, [0, 0, 0]);
        assert.deepEqual(result.data.f, [0, 0, 0]);
        assert.deepEqual(result.data.b, [false, false, false]);
    });

    it('negative numbers', async () => {
        const schema = [
            { name: 'i32', type: 'int32' },
            { name: 'i64', type: 'int64' },
            { name: 'f64', type: 'float64' },
        ];
        const data = { i32: [-1, -100, -2147483648], i64: [-1, -999999], f64: [-0.001, -1e50] };
        const result = await roundtrip(schema, data)();
        assert.deepEqual(result.data.i32, data.i32);
        assert.deepEqual(result.data.i64, data.i64);
        assert.deepEqual(result.data.f64, data.f64);
    });

    it('single column, many rows', async () => {
        const schema = [{ name: 'id', type: 'int32' }];
        const n = 5_000;
        const data = { id: Array.from({ length: n }, (_, i) => i) };
        const result = await roundtrip(schema, data)();
        assert.equal(result.numRows, n);
        assert.equal(result.data.id[0], 0);
        assert.equal(result.data.id[n - 1], n - 1);
    });

    it('many columns, few rows', async () => {
        const cols = 20;
        const schema = Array.from({ length: cols }, (_, i) => ({ name: `col_${i}`, type: 'int32' }));
        const data = {};
        for (let i = 0; i < cols; i++) data[`col_${i}`] = [i, i * 2, i * 3];
        const result = await roundtrip(schema, data)();
        assert.equal(result.numRows, 3);
        assert.equal(result.schema.length, cols);
    });
});

// ── Output format ────────────────────────────────────────────────────────────

describe('output format', () => {
    it('produces valid Parquet magic bytes', async () => {
        const bytes = await writeParquet(
            [{ name: 'x', type: 'int32' }],
            { x: [1] },
        );
        // Parquet files start and end with "PAR1"
        const magic = String.fromCharCode(...bytes.slice(0, 4));
        const footer = String.fromCharCode(...bytes.slice(-4));
        assert.equal(magic, 'PAR1', 'should start with PAR1');
        assert.equal(footer, 'PAR1', 'should end with PAR1');
    });

    it('readParquet returns correct schema types', async () => {
        const schema = [
            { name: 'a', type: 'string' },
            { name: 'b', type: 'int32' },
            { name: 'c', type: 'boolean' },
        ];
        const data = { a: ['x'], b: [1], c: [true] };
        const result = await roundtrip(schema, data)();
        assert.equal(result.schema[0].type, 'string');
        assert.equal(result.schema[1].type, 'int32');
        assert.equal(result.schema[2].type, 'boolean');
    });
});
