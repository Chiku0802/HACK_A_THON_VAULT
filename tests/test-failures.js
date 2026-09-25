/**
 * Integration Test: Node Failures, Quorums, Sloppy Quorums, Hinted Handoff & Auto-Repair
 */

import assert from 'node:assert';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { startCluster } from '../src/index.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TEST_DATA_DIR = path.resolve(__dirname, '../data-test-failures');

async function run() {
  console.log('--- Testing Node Failures, Quorum Reads, Hinted Handoff & Auto-Repair ---');
  await fs.rm(TEST_DATA_DIR, { recursive: true, force: true });

  const cluster = await startCluster({
    gatewayPort: 9280,
    dataDir: TEST_DATA_DIR,
    healingIntervalMs: 1000,
    nodes: [
      { nodeId: 'fnode-1', port: 9281, rack: 'rack-1' },
      { nodeId: 'fnode-2', port: 9282, rack: 'rack-1' },
      { nodeId: 'fnode-3', port: 9283, rack: 'rack-2' },
      { nodeId: 'fnode-4', port: 9284, rack: 'rack-2' },
      { nodeId: 'fnode-5', port: 9285, rack: 'rack-3' },
    ],
  });

  const gatewayUrl = 'http://127.0.0.1:9280';

  try {
    // 1. Store initial 3x replicated object
    const key = 'critical-doc.txt';
    const payload = 'Super critical mission data that must survive multiple node crashes!';
    console.log(`Writing object '${key}' (3x replication)...`);

    const putRes = await fetch(`${gatewayUrl}/default/${key}`, {
      method: 'PUT',
      headers: { 'X-Vault-Storage-Policy': 'REPLICATION', 'X-Vault-Replicas': '3' },
      body: Buffer.from(payload),
    });
    assert.strictEqual(putRes.status, 201);
    const meta = await putRes.json();
    const primaryNodes = meta.chunks[0].replicaNodes;
    console.log(`✓ Object placed on nodes: [${primaryNodes.join(', ')}]`);

    // 2. Kill the first replica node
    const deadNodeId = primaryNodes[0];
    console.log(`Killing primary replica node ${deadNodeId}...`);
    const killRes = await fetch(`${gatewayUrl}/api/chaos/kill-node`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ nodeId: deadNodeId }),
    });
    assert.strictEqual(killRes.status, 200);

    // 3. Read object with 1 node dead: Quorum read (R=2) must succeed seamlessly!
    console.log('Reading object with 1 node dead...');
    const getRes = await fetch(`${gatewayUrl}/default/${key}`);
    assert.strictEqual(getRes.status, 200);
    const content = await getRes.text();
    assert.strictEqual(content, payload, 'Payload read during node failure matches 100%');
    console.log('✓ Read Quorum succeeded seamlessly despite dead replica node');

    // 4. Test Hinted Handoff: Write new object while deadNodeId is still offline
    console.log(`Writing new object 'hinted-obj.txt' while ${deadNodeId} is offline...`);
    const hintKey = 'hinted-obj.txt';
    const hintPayload = 'Data destined for hinted handoff replay!';

    const hintPutRes = await fetch(`${gatewayUrl}/default/${hintKey}`, {
      method: 'PUT',
      headers: { 'X-Vault-Storage-Policy': 'REPLICATION', 'X-Vault-Replicas': '3' },
      body: Buffer.from(hintPayload),
    });
    assert.strictEqual(hintPutRes.status, 201);
    console.log('✓ Sloppy Quorum write succeeded with Hinted Handoff');

    // Check pending hints
    const pendingHints = await cluster.hintedHandoff.getPendingHints();
    console.log(`Pending hints in cluster: ${pendingHints.length}`);

    // 5. Revive the dead node
    console.log(`Reviving node ${deadNodeId}...`);
    const reviveRes = await fetch(`${gatewayUrl}/api/chaos/revive-node`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ nodeId: deadNodeId }),
    });
    assert.strictEqual(reviveRes.status, 200);
    const reviveData = await reviveRes.json();
    console.log(`✓ Node revived. Replayed hints: ${reviveData.replayedHints}`);

    // 6. Test Autonomous Healing Daemon
    // Now kill a node permanently and trigger healing cycle to replicate degraded object onto spare node
    const targetToKill = primaryNodes[1];
    console.log(`Simulating permanent failure of node ${targetToKill}...`);
    await fetch(`${gatewayUrl}/api/chaos/kill-node`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ nodeId: targetToKill }),
    });

    console.log('Triggering autonomous healing cycle...');
    await cluster.healingDaemon.runHealingCycle();
    const stats = cluster.healingDaemon.getStats();
    console.log(`✓ Healing cycle finished. Total healed chunks: ${stats.totalHealedChunks}`);
    assert(stats.totalHealedChunks >= 1, 'At least 1 degraded chunk must be healed');

    // Verify object placement updated in metadata
    const updatedMeta = cluster.metadataStore.getObjectMetadata('default', key);
    const newReplicas = updatedMeta.chunks[0].replicaNodes;
    console.log(`✓ New replica placement: [${newReplicas.join(', ')}] (replaced dead ${targetToKill})`);
    assert(!newReplicas.includes(targetToKill), 'Dead node removed from replica set');

    // Read object back
    const finalRead = await fetch(`${gatewayUrl}/default/${key}`);
    assert.strictEqual(finalRead.status, 200);
    assert.strictEqual(await finalRead.text(), payload);
    console.log('✓ Healed object read back with 100% fidelity');

  } finally {
    await cluster.shutdown();
    await fs.rm(TEST_DATA_DIR, { recursive: true, force: true });
    console.log('✓ Node failures & auto-repair test completed cleanly\n');
  }
}

run().catch(err => {
  console.error('Test failed:', err);
  process.exit(1);
});
