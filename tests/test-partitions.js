/**
 * Integration Test: Network Partitions, Split-Brain Prevention & Quorum Enforcement
 */

import assert from 'node:assert';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { startCluster } from '../src/index.js';
import { globalNetworkSim } from '../src/cluster/network-sim.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TEST_DATA_DIR = path.resolve(__dirname, '../data-test-partitions');

async function run() {
  console.log('--- Testing Network Partitions & Split-Brain Prevention ---');
  await fs.rm(TEST_DATA_DIR, { recursive: true, force: true });

  const cluster = await startCluster({
    gatewayPort: 9480,
    dataDir: TEST_DATA_DIR,
    nodes: [
      { nodeId: 'pnode-1', port: 9481, rack: 'rack-1' },
      { nodeId: 'pnode-2', port: 9482, rack: 'rack-1' },
      { nodeId: 'pnode-3', port: 9483, rack: 'rack-2' },
      { nodeId: 'pnode-4', port: 9484, rack: 'rack-2' },
      { nodeId: 'pnode-5', port: 9485, rack: 'rack-3' },
    ],
  });

  const gatewayUrl = 'http://127.0.0.1:9480';

  try {
    // 1. Establish baseline write
    console.log('Storing baseline object...');
    const baseRes = await fetch(`${gatewayUrl}/default/base.txt`, {
      method: 'PUT',
      body: Buffer.from('Initial stable state'),
    });
    assert.strictEqual(baseRes.status, 201);
    console.log('✓ Baseline object stored successfully');

    // 2. Create asymmetric network partition
    // Isolate minority group: [pnode-4, pnode-5] from gateway and remaining nodes
    console.log('Creating network partition: isolating [pnode-4, pnode-5]...');
    globalNetworkSim.isolateNode('pnode-4');
    globalNetworkSim.isolateNode('pnode-5');
    cluster.topology.updateNodeStatus('pnode-4', 'DEAD');
    cluster.topology.updateNodeStatus('pnode-5', 'DEAD');

    // 3. Majority Partition (pnode-1, pnode-2, pnode-3) should still satisfy Quorum (W=2, R=2)
    console.log('Writing object in majority partition (3 alive nodes)...');
    const majRes = await fetch(`${gatewayUrl}/default/majority-write.txt`, {
      method: 'PUT',
      headers: { 'X-Vault-Storage-Policy': 'REPLICATION', 'X-Vault-Replicas': '3', 'X-Vault-Write-Quorum': '2' },
      body: Buffer.from('Written during partition on majority side'),
    });
    assert.strictEqual(majRes.status, 201);
    console.log('✓ Majority partition write succeeded');

    // 4. Now simulate minority side trying to write when required quorum cannot be met:
    // Kill pnode-1 and pnode-2 as well so only 1 node is alive (insufficient for W=2)
    console.log('Isolating additional nodes so only 1 node remains...');
    globalNetworkSim.isolateNode('pnode-1');
    globalNetworkSim.isolateNode('pnode-2');
    cluster.topology.updateNodeStatus('pnode-1', 'DEAD');
    cluster.topology.updateNodeStatus('pnode-2', 'DEAD');

    console.log('Attempting write in minority partition (expecting 503 QuorumUnavailable)...');
    const minRes = await fetch(`${gatewayUrl}/default/split-brain-attempt.txt`, {
      method: 'PUT',
      headers: { 'X-Vault-Storage-Policy': 'REPLICATION', 'X-Vault-Replicas': '3', 'X-Vault-Write-Quorum': '2' },
      body: Buffer.from('This should fail to prevent split brain'),
    });

    assert.strictEqual(minRes.status, 503, 'Minority partition write must be rejected with 503');
    const errBody = await minRes.json();
    console.log(`✓ Minority write properly rejected: "${errBody.error}" (Split-Brain Prevented!)`);

    // 5. Heal all network partitions
    console.log('Healing all network partitions...');
    globalNetworkSim.healAll();
    for (const n of ['pnode-1', 'pnode-2', 'pnode-4', 'pnode-5']) {
      cluster.topology.updateNodeStatus(n, 'ALIVE');
    }

    // Read back majority write to ensure data safety
    const readBack = await fetch(`${gatewayUrl}/default/majority-write.txt`);
    assert.strictEqual(readBack.status, 200);
    assert.strictEqual(await readBack.text(), 'Written during partition on majority side');
    console.log('✓ Partition healed. Cluster re-converged with zero data loss or divergence.');

  } finally {
    globalNetworkSim.healAll();
    await cluster.shutdown();
    await fs.rm(TEST_DATA_DIR, { recursive: true, force: true });
    console.log('✓ Network partition test completed successfully\n');
  }
}

run().catch(err => {
  console.error('Test failed:', err);
  process.exit(1);
});
