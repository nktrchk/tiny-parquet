/**
 * Test dictionary encoding roundtrip
 */
import { writeParquet, readParquet } from '../src/index.js';

async function test() {
  console.log('=== Dictionary Encoding Roundtrip Test ===\n');

  const schema = [
    { name: 'country', type: 'string' },
    { name: 'browser', type: 'string' },
    { name: 'score', type: 'int32' },
    { name: 'active', type: 'boolean' },
  ];

  // Low-cardinality data — perfect for dictionary encoding
  const data = {
    country: ['US', 'UK', 'DE', 'US', 'UK', 'DE', 'US', 'UK', 'DE', 'US'],
    browser: ['Chrome', 'Firefox', 'Safari', 'Chrome', 'Chrome', 'Firefox', 'Safari', 'Chrome', 'Chrome', 'Firefox'],
    score:   [95, 82, 77, 91, 88, 73, 85, 90, 79, 84],
    active:  [true, false, true, true, true, false, true, false, true, true],
  };

  // 1) Write WITH dictionary (default)
  const bytesDict = await writeParquet(schema, data, { compression: 'snappy' });
  console.log(`Dict ON:  ${bytesDict.length} bytes`);

  // 2) Write WITHOUT dictionary
  const bytesPlain = await writeParquet(schema, data, { compression: 'snappy', dictionary: false });
  console.log(`Dict OFF: ${bytesPlain.length} bytes`);

  const savings = ((1 - bytesDict.length / bytesPlain.length) * 100).toFixed(1);
  console.log(`Savings:  ${savings}%\n`);

  // 3) Read back dict-encoded
  const resultDict = await readParquet(bytesDict, 500);
  console.log('Read dict-encoded:');
  console.log('  schema:', JSON.stringify(resultDict.schema));
  console.log('  country:', resultDict.data.country);
  console.log('  browser:', resultDict.data.browser);
  console.log('  score:', resultDict.data.score);
  console.log('  active:', resultDict.data.active);
  console.log('  numRows:', resultDict.numRows);

  // 4) Read back plain-encoded
  const resultPlain = await readParquet(bytesPlain, 500);
  console.log('\nRead plain-encoded:');
  console.log('  country:', resultPlain.data.country);
  console.log('  browser:', resultPlain.data.browser);

  // 5) Verify correctness
  const match = JSON.stringify(resultDict.data) === JSON.stringify(resultPlain.data);
  console.log(`\n✅ Dict vs Plain match: ${match}`);

  if (!match) {
    console.error('❌ MISMATCH!');
    process.exit(1);
  }

  // 6) Verify specific values
  const ok = (
    resultDict.data.country[0] === 'US' &&
    resultDict.data.country[2] === 'DE' &&
    resultDict.data.browser[1] === 'Firefox' &&
    resultDict.data.score[0] === 95 &&
    resultDict.data.active[1] === false &&
    resultDict.numRows === 10
  );
  console.log(`✅ Values correct: ${ok}`);
  if (!ok) {
    console.error('❌ VALUES WRONG!');
    process.exit(1);
  }

  console.log('\n🎉 All tests passed!');
}

test().catch(e => { console.error(e); process.exit(1); });
