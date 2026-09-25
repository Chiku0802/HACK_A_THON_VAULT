/**
 * Integration Test: Bitrot Detection, Automatic Read-Repair, and Background Scrubber
 */

import assert from 'node:assert';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { startCluster } from '../src/index.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TEST_DATA_DIR = path.resolve(__dirname, '../data-test-bitrot');

async function run() {
  console.log('--- Testing Bitrot Detection, Read-Repair, and Background Scrubber ---');
  await fs.rm(TEST_DATA_DIR, { recursive: true, force: true });

  const cluster = await startCluster({
    gatewayPort: 9380,
    dataDir: TEST_DATA_DIR,
    nodes: [
      { nodeId: 'bnode-1', port: 9381, rack: 'rack-1' },
      { nodeId: 'bnode-2', port: 9382, rack: 'rack-1' },
      { nodeId: 'bnode-3', port: 9383, rack: 'rack-2' },
      { nodeId: 'bnode-4', port: 9384, rack: 'rack-2' },
      { nodeId: 'bnode-5', port: 9385, rack: 'rack-3' },
    ],
  });

  const gatewayUrl = 'http://127.0.0.1:9380';

  try {
    // 1. Store test object
    const key = 'docs/contracts.pdf';
    const originalContent = 'High-value legal agreement with cryptographic integrity protection against hardware bitrot!';
    console.log(`Writing test object '${key}'...`);

    const putRes = await fetch(`${gatewayUrl}/default/${key}`, {
      method: 'PUT',
      headers: { 'X-Vault-Storage-Policy': 'REPLICATION', 'X-Vault-Replicas': '3' },
      body: Buffer.from(originalContent),
    });
    assert.strictEqual(putRes.status, 201);
    const meta = await putRes.json();
    const chunk = meta.chunks[0];
    const targetNodeId = chunk.replicaNodes[0];
    const targetNodeDaemon = cluster.storageNodes.find(n => n.nodeId === targetNodeId);

    console.log(`✓ Object chunk ${chunk.chunkId} stored on [${chunk.replicaNodes.join(', ')}]`);

    // 2. Inject physical bitrot into targetNode's disk file
    console.log(`Injecting physical bitrot corruption onto disk for ${targetNodeId}...`);
    await targetNodeDaemon.disk.injectCorruption(chunk.chunkId, 5);

    // Direct read on that single node must throw CorruptionDetectedError
    await assert.rejects(async () => {
      await targetNodeDaemon.disk.readChunk(chunk.chunkId, true);
    }, /Bitrot detected/, 'Node disk read must detect corrupted SHA-256');
    console.log('✓ Disk engine successfully detected bitrot and threw CorruptionDetectedError');

    // 3. Read object via Gateway: Client gets 200 OK seamlessly, and triggers Read Repair!
    console.log('Reading object through Gateway (expecting transparent recovery & read-repair)...');
    const getRes = await fetch(`${gatewayUrl}/default/${key}`);
    assert.strictEqual(getRes.status, 200);
    const retrievedContent = await getRes.text();
    assert.strictEqual(retrievedContent, originalContent, 'Client received uncorrupted canonical data');
    console.log('✓ Client read succeeded with 100% data integrity');

    // Give Read Repair 200ms to complete background write
    await new Promise(r => setTimeout(r, 200));

    // 4. Verify that targetNodeId has been automatically repaired on disk!
    console.log(`Verifying target node ${targetNodeId} has been repaired on disk...`);
    const repaired = await targetNodeDaemon.disk.readChunk(chunk.chunkId, true);
    assert.strictEqual(repaired.payload.toString('utf8'), originalContent, 'Corrupted chunk was repaired by Read-Repair');
    console.log('✓ Read Repair verified: corrupted disk chunk restored to pristine state!');

    // 5. Test Background Scrubber
    console.log('Testing Background Scrubber with newly injected bitrot...');
    const secondNodeId = chunk.replicaNodes[1];
    const secondNodeDaemon = cluster.storageNodes.find(n => n.nodeId === secondNodeId);
    await secondNodeDaemon.disk.injectCorruption(chunk.chunkId, 3);

    // Trigger scrubber
    const scrubReport = await cluster.backgroundScrubber.runScrub();
    const corruptedReport = scrubReport.reports.find(r => r.nodeId === secondNodeId);
    assert(corruptedReport.corrupted.length >= 1, 'Scrubber must detect corrupted chunk');
    console.log(`✓ Background Scrubber successfully flagged bitrot on ${secondNodeId}`);

  } finally {
    await cluster.shutdown();
    await fs.rm(TEST_DATA_DIR, { recursive: true, force: true });
    console.log('✓ Bitrot detection and Read-Repair tests completed successfully\n');
  }
}

run().catch(err => {
  console.error('Test failed:', err);
  process.exit(1);
});
