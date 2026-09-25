/**
 * Integration Test: Merkle Tree Anti-Entropy Sync
 */

import assert from 'node:assert';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { startCluster } from '../src/index.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TEST_DATA_DIR = path.resolve(__dirname, '../data-test-anti-entropy');

async function run() {
  console.log('--- Testing Merkle Tree Anti-Entropy Synchronization ---');
  await fs.rm(TEST_DATA_DIR, { recursive: true, force: true });

  const cluster = await startCluster({
    gatewayPort: 9580,
    dataDir: TEST_DATA_DIR,
    nodes: [
      { nodeId: 'anode-1', port: 9581, rack: 'rack-1' },
      { nodeId: 'anode-2', port: 9582, rack: 'rack-1' },
      { nodeId: 'anode-3', port: 9583, rack: 'rack-2' },
      { nodeId: 'anode-4', port: 9584, rack: 'rack-2' },
      { nodeId: 'anode-5', port: 9585, rack: 'rack-3' },
    ],
  });

  const gatewayUrl = 'http://127.0.0.1:9580';

  try {
    // 1. Write an object replicated across anode-1, anode-2, anode-3
    const key = 'state-file.json';
    const payload = JSON.stringify({ cluster: 'vault', status: 'verified', entropy: Date.now() });

    console.log(`Writing replicated object '${key}'...`);
    const putRes = await fetch(`${gatewayUrl}/default/${key}`, {
      method: 'PUT',
      headers: { 'X-Vault-Storage-Policy': 'REPLICATION', 'X-Vault-Replicas': '3' },
      body: Buffer.from(payload),
    });
    assert.strictEqual(putRes.status, 201);
    const meta = await putRes.json();
    const chunk = meta.chunks[0];
    const [nodeAId, nodeBId] = chunk.replicaNodes.slice(0, 2);

    const daemonA = cluster.storageNodes.find(n => n.nodeId === nodeAId);
    const daemonB = cluster.storageNodes.find(n => n.nodeId === nodeBId);

    console.log(`✓ Object stored on ${nodeAId} and ${nodeBId}`);

    // Verify both nodes initially have the chunk
    assert(await daemonA.disk.hasChunk(chunk.chunkId), 'Node A has chunk');
    assert(await daemonB.disk.hasChunk(chunk.chunkId), 'Node B has chunk');

    // 2. Simulate silent data loss / disk omission on Node B
    console.log(`Simulating silent deletion of chunk on ${nodeBId}...`);
    await daemonB.disk.deleteChunk(chunk.chunkId);
    assert(!await daemonB.disk.hasChunk(chunk.chunkId), 'Chunk successfully removed from Node B');

    // 3. Run Anti-Entropy sync between Node A and Node B
    console.log(`Executing Merkle Anti-Entropy exchange between ${nodeAId} and ${nodeBId}...`);
    const nodeAObj = cluster.topology.getNode(nodeAId);
    const nodeBObj = cluster.topology.getNode(nodeBId);

    const syncResult = await cluster.antiEntropy.syncPeerPair(nodeAObj, nodeBObj);
    console.log(`✓ Anti-Entropy sync result:`, syncResult);
    assert.strictEqual(syncResult.synced, true);
    assert(syncResult.transferred >= 1, 'At least 1 missing chunk transferred to Node B');

    // 4. Verify Node B now has the chunk restored with bit-for-bit accuracy!
    assert(await daemonB.disk.hasChunk(chunk.chunkId), 'Node B disk now has chunk restored');
    const restored = await daemonB.disk.readChunk(chunk.chunkId, true);
    assert.strictEqual(restored.payload.toString('utf8'), payload, 'Restored chunk matches original payload');
    console.log(`✓ Node ${nodeBId} verified: chunk restored and validated via SHA-256!`);

  } finally {
    await cluster.shutdown();
    await fs.rm(TEST_DATA_DIR, { recursive: true, force: true });
    console.log('✓ Anti-Entropy test completed successfully\n');
  }
}

run().catch(err => {
  console.error('Test failed:', err);
  process.exit(1);
});
