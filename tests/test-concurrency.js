/**
 * Integration Test: Concurrent Reads, Writes, and Atomic Versioning
 */

import assert from 'node:assert';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { startCluster } from '../src/index.js';
import { sha256Hex } from '../src/common/crypto.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TEST_DATA_DIR = path.resolve(__dirname, '../data-test-concurrency');

async function run() {
  console.log('--- Testing Concurrent Reads, Writes, and Versioning ---');
  await fs.rm(TEST_DATA_DIR, { recursive: true, force: true });

  const cluster = await startCluster({
    gatewayPort: 9180,
    dataDir: TEST_DATA_DIR,
    nodes: [
      { nodeId: 'cnode-1', port: 9181, rack: 'rack-1' },
      { nodeId: 'cnode-2', port: 9182, rack: 'rack-1' },
      { nodeId: 'cnode-3', port: 9183, rack: 'rack-2' },
      { nodeId: 'cnode-4', port: 9184, rack: 'rack-2' },
      { nodeId: 'cnode-5', port: 9185, rack: 'rack-3' },
    ],
  });

  const gatewayUrl = 'http://127.0.0.1:9180';

  try {
    // 1. Concurrent writes: 20 distinct objects written simultaneously
    console.log('Executing 20 concurrent object PUTs...');
    const objects = Array.from({ length: 20 }, (_, i) => ({
      key: `dataset/file_${i}.dat`,
      data: Buffer.from(`Payload for object #${i} with entropy ${Math.random()}`),
    }));

    await Promise.all(objects.map(async (obj) => {
      const res = await fetch(`${gatewayUrl}/default/${obj.key}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'text/plain' },
        body: obj.data,
      });
      assert.strictEqual(res.status, 201, `PUT for ${obj.key} should succeed`);
    }));
    console.log('✓ 20 concurrent writes succeeded');

    // 2. Concurrent reads: 50 concurrent GET requests
    console.log('Executing 50 concurrent object GETs...');
    const readPromises = [];
    for (let i = 0; i < 50; i++) {
      const target = objects[i % objects.length];
      readPromises.push(async () => {
        const res = await fetch(`${gatewayUrl}/default/${target.key}`);
        assert.strictEqual(res.status, 200);
        const buf = Buffer.from(await res.arrayBuffer());
        assert.strictEqual(buf.toString(), target.data.toString(), 'Read payload matches written data');
      });
    }
    await Promise.all(readPromises.map(p => p()));
    console.log('✓ 50 concurrent reads verified with 100% data integrity');

    // 3. Concurrent updates to the same object (Versioning & MVCC)
    console.log('Executing rapid concurrent updates to single key...');
    const targetKey = 'concurrency/race-key.txt';
    const updates = Array.from({ length: 5 }, (_, i) => `Update version ${i}`);

    await Promise.all(updates.map(async (val) => {
      const res = await fetch(`${gatewayUrl}/default/${targetKey}`, {
        method: 'PUT',
        body: Buffer.from(val),
      });
      assert.strictEqual(res.status, 201);
    }));

    // Read latest version
    const latestRes = await fetch(`${gatewayUrl}/default/${targetKey}`);
    assert.strictEqual(latestRes.status, 200);
    const latestData = await latestRes.text();
    assert(updates.includes(latestData), 'Latest version must be one of the committed updates');
    console.log(`✓ Atomic updates verified. Latest value: "${latestData}"`);

  } finally {
    await cluster.shutdown();
    await fs.rm(TEST_DATA_DIR, { recursive: true, force: true });
    console.log('✓ Concurrency test completed cleanly\n');
  }
}

run().catch(err => {
  console.error('Test failed:', err);
  process.exit(1);
});
