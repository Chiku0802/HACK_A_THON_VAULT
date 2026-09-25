/**
 * Integration Test: Large Objects, Multipart Uploads, Range Reads, and Erasure Under Failure
 */

import assert from 'node:assert';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { startCluster } from '../src/index.js';
import { sha256Hex } from '../src/common/crypto.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TEST_DATA_DIR = path.resolve(__dirname, '../data-test-large');

async function run() {
  console.log('--- Testing Large Objects, Multipart Uploads, Range Reads & Erasure Coding ---');
  await fs.rm(TEST_DATA_DIR, { recursive: true, force: true });

  const cluster = await startCluster({
    gatewayPort: 9680,
    dataDir: TEST_DATA_DIR,
    chunkSize: 64 * 1024, // 64KB chunk size to test multi-chunk boundary handling
    nodes: [
      { nodeId: 'lnode-1', port: 9681, rack: 'rack-1' },
      { nodeId: 'lnode-2', port: 9682, rack: 'rack-1' },
      { nodeId: 'lnode-3', port: 9683, rack: 'rack-2' },
      { nodeId: 'lnode-4', port: 9684, rack: 'rack-2' },
      { nodeId: 'lnode-5', port: 9685, rack: 'rack-3' },
    ],
  });

  const gatewayUrl = 'http://127.0.0.1:9680';

  try {
    // 1. Multi-chunk Object (200 KB -> 4 chunks of 64KB)
    console.log('Writing 200 KB object across multiple 64KB chunks...');
    const largeBuffer = Buffer.alloc(200 * 1024);
    for (let i = 0; i < largeBuffer.length; i++) {
      largeBuffer[i] = (i * 31 + 7) & 0xFF;
    }
    const expectedChecksum = sha256Hex(largeBuffer);

    const putRes = await fetch(`${gatewayUrl}/default/large-file.bin`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/octet-stream' },
      body: largeBuffer,
    });
    assert.strictEqual(putRes.status, 201);
    const putMeta = await putRes.json();
    assert.strictEqual(putMeta.chunks.length, 4, 'Must have been partitioned into 4 chunks');
    console.log(`✓ 200 KB object written in 4 chunks (ETag: ${putMeta.etag})`);

    // 2. HTTP Byte Range Request (Range: bytes=1000-4999)
    console.log('Executing HTTP Range request: bytes=1000-4999...');
    const rangeRes = await fetch(`${gatewayUrl}/default/large-file.bin`, {
      headers: { 'Range': 'bytes=1000-4999' },
    });
    assert.strictEqual(rangeRes.status, 206, 'Must return 206 Partial Content');
    const rangeData = Buffer.from(await rangeRes.arrayBuffer());
    assert.strictEqual(rangeData.length, 4000, 'Range size must be 4000 bytes');
    const expectedSlice = largeBuffer.subarray(1000, 5000);
    assert(rangeData.equals(expectedSlice), 'Range payload matches expected slice exactly');
    console.log('✓ Byte range request verified');

    // 3. S3 Multipart Upload
    console.log('Testing S3 Multipart Upload workflow (out-of-order parts)...');
    // Step 3a: Initiate
    const initRes = await fetch(`${gatewayUrl}/default/multipart-obj.dat?uploads`, {
      method: 'POST',
    });
    assert.strictEqual(initRes.status, 200);
    const { uploadId } = await initRes.json();
    console.log(`✓ Multipart initiated: uploadId=${uploadId}`);

    // Create 3 parts
    const part1Data = Buffer.from('First part of multipart data stream. ');
    const part2Data = Buffer.from('Second part containing critical payload. ');
    const part3Data = Buffer.from('Final part completing the multipart upload.');
    const fullExpectedMultipart = Buffer.concat([part1Data, part2Data, part3Data]);

    // Upload part 2 first (out of order!)
    const p2Res = await fetch(`${gatewayUrl}/default/multipart-obj.dat?uploadId=${uploadId}&partNumber=2`, {
      method: 'PUT',
      body: part2Data,
    });
    assert.strictEqual(p2Res.status, 200);

    // Upload part 1
    const p1Res = await fetch(`${gatewayUrl}/default/multipart-obj.dat?uploadId=${uploadId}&partNumber=1`, {
      method: 'PUT',
      body: part1Data,
    });
    assert.strictEqual(p1Res.status, 200);

    // Upload part 3
    const p3Res = await fetch(`${gatewayUrl}/default/multipart-obj.dat?uploadId=${uploadId}&partNumber=3`, {
      method: 'PUT',
      body: part3Data,
    });
    assert.strictEqual(p3Res.status, 200);

    // Complete multipart
    const compRes = await fetch(`${gatewayUrl}/default/multipart-obj.dat?uploadId=${uploadId}`, {
      method: 'POST',
    });
    assert.strictEqual(compRes.status, 200);
    const compMeta = await compRes.json();
    console.log(`✓ Multipart completed: total size=${compMeta.size}`);

    // Download assembled multipart object
    const mpGetRes = await fetch(`${gatewayUrl}/default/multipart-obj.dat`);
    assert.strictEqual(mpGetRes.status, 200);
    const mpDownloaded = Buffer.from(await mpGetRes.arrayBuffer());
    assert(mpDownloaded.equals(fullExpectedMultipart), 'Multipart downloaded bytes match concatenated original parts');
    console.log('✓ Multipart stitched object verified bit-for-bit');

    // 4. Erasure Coding under Node Failure
    console.log('Testing Reed-Solomon Erasure Coding under active node failure (2+1 EC)...');
    const ecKey = 'erasure-test.bin';
    const ecData = Buffer.from('Highly resilient object protected by Reed-Solomon Galois Field erasure coding!');

    const ecPutRes = await fetch(`${gatewayUrl}/default/${ecKey}`, {
      method: 'PUT',
      headers: {
        'X-Vault-Storage-Policy': 'ERASURE',
        'X-Vault-EC-K': '2',
        'X-Vault-EC-M': '1',
      },
      body: ecData,
    });
    assert.strictEqual(ecPutRes.status, 201);
    const ecMeta = await ecPutRes.json();
    const shardNodes = ecMeta.chunks[0].shards.map(s => s.nodeId);
    console.log(`✓ Erasure shards stored on nodes: [${shardNodes.join(', ')}]`);

    // Kill 1 node holding a shard
    const victimNode = shardNodes[0];
    console.log(`Killing node ${victimNode} holding erasure shard...`);
    await fetch(`${gatewayUrl}/api/chaos/kill-node`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ nodeId: victimNode }),
    });

    // Read object back: Gateway must reconstruct data using surviving K shards!
    console.log('Reading erasure object with 1 node dead (reconstructing from surviving shards)...');
    const ecGetRes = await fetch(`${gatewayUrl}/default/${ecKey}`);
    assert.strictEqual(ecGetRes.status, 200);
    const ecDownloaded = await ecGetRes.text();
    assert.strictEqual(ecDownloaded, ecData.toString('utf8'), 'Erasure decoded payload matches original perfectly');
    console.log('✓ Reed-Solomon Erasure Coding reconstructed data with 100% fidelity despite node loss!');

  } finally {
    await cluster.shutdown();
    await fs.rm(TEST_DATA_DIR, { recursive: true, force: true });
    console.log('✓ Large objects, multipart & erasure tests completed successfully\n');
  }
}

run().catch(err => {
  console.error('Test failed:', err);
  process.exit(1);
});
