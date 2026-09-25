/**
 * Vault Object Storage - Cluster Bootstrapper & Daemon Coordinator
 * Launches 5 rack-aware storage nodes, metadata store, failure detector,
 * self-healing daemons, and S3-compatible REST gateway.
 */

import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { ClusterTopology } from './cluster/topology.js';
import { FailureDetector } from './cluster/failure-detector.js';
import { HintedHandoffManager } from './cluster/hinted-handoff.js';
import { StorageNode } from './storage/storage-node.js';
import { MetadataStore } from './metadata/metadata-store.js';
import { ReadRepairEngine } from './repair/read-repair.js';
import { BackgroundScrubber } from './repair/background-scrub.js';
import { AntiEntropySync } from './repair/anti-entropy.js';
import { HealingDaemon } from './repair/healing-daemon.js';
import { StorageGateway } from './gateway/gateway.js';
import { DashboardApiRouter } from './gateway/dashboard-api.js';
import { createLogger } from './common/logger.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DATA_DIR = path.resolve(__dirname, '../data');

export async function startCluster(customConfig = {}) {
  const logger = createLogger('Cluster');
  logger.info('====================================================');
  logger.info('   🛡️  BOOTSTRAPPING VAULT DISTRIBUTED STORAGE     ');
  logger.info('====================================================');

  const nodeConfigs = customConfig.nodes || [
    { nodeId: 'node-1', port: 9001, rack: 'rack-1' },
    { nodeId: 'node-2', port: 9002, rack: 'rack-1' },
    { nodeId: 'node-3', port: 9003, rack: 'rack-2' },
    { nodeId: 'node-4', port: 9004, rack: 'rack-2' },
    { nodeId: 'node-5', port: 9005, rack: 'rack-3' },
  ];

  const gatewayPort = customConfig.gatewayPort || 8080;
  const baseDataDir = customConfig.dataDir || ROOT_DATA_DIR;

  // 1. Initialize Topology
  const topology = new ClusterTopology(nodeConfigs.map(c => ({
    nodeId: c.nodeId,
    host: '127.0.0.1',
    port: c.port,
    rack: c.rack,
    status: 'ALIVE',
  })));

  // 2. Start Storage Nodes
  const storageNodes = [];
  for (const cfg of nodeConfigs) {
    const nodeDir = path.join(baseDataDir, 'nodes', cfg.nodeId);
    const node = new StorageNode({
      nodeId: cfg.nodeId,
      port: cfg.port,
      rack: cfg.rack,
      dataDir: nodeDir,
    });
    await node.start();
    storageNodes.push(node);
  }

  // 3. Initialize Metadata Store & WAL
  const metaDir = path.join(baseDataDir, 'metadata');
  const metadataStore = new MetadataStore(metaDir);
  await metadataStore.init();

  // 4. Initialize Hinted Handoff Manager
  const hintedHandoff = new HintedHandoffManager(topology, storageNodes);

  // 5. Initialize Failure Detector
  const failureDetector = new FailureDetector(topology, {
    heartbeatIntervalMs: customConfig.heartbeatIntervalMs || 1000,
    suspectTimeoutMs: customConfig.suspectTimeoutMs || 2500,
    deadTimeoutMs: customConfig.deadTimeoutMs || 5000,
  });

  // When failure detector spots a node returning to ALIVE, replay hints!
  failureDetector.on('node:status_change', async ({ nodeId, newStatus }) => {
    if (newStatus === 'ALIVE') {
      await hintedHandoff.replayHintsForNode(nodeId);
    }
  });
  failureDetector.start();

  // 6. Initialize Read Repair Engine
  const readRepair = new ReadRepairEngine(topology, metadataStore);

  // 7. Initialize Autonomous Healing Daemon
  const healingDaemon = new HealingDaemon(topology, metadataStore, {
    healingIntervalMs: customConfig.healingIntervalMs || 2000,
    repairBandwidthBytesPerSec: customConfig.repairBandwidthBytesPerSec || (10 * 1024 * 1024),
  });
  healingDaemon.start();

  // 8. Initialize Background Scrubber
  const backgroundScrubber = new BackgroundScrubber(topology, healingDaemon, {
    scrubIntervalMs: customConfig.scrubIntervalMs || 15000,
  });
  backgroundScrubber.start();

  // 9. Initialize Merkle Anti-Entropy Synchronizer
  const antiEntropy = new AntiEntropySync(topology, {
    antiEntropyIntervalMs: customConfig.antiEntropyIntervalMs || 20000,
  });
  antiEntropy.start();

  // 10. Initialize Dashboard API Router
  const dashboardRouter = new DashboardApiRouter({
    topology,
    metadataStore,
    storageNodes,
    healingDaemon,
    readRepair,
    hintedHandoff,
    antiEntropy,
    backgroundScrubber,
  });

  // 11. Start Storage Gateway
  const gateway = new StorageGateway({
    port: gatewayPort,
    topology,
    metadataStore,
    readRepair,
    hintedHandoff,
    healingDaemon,
    config: {
      chunkSize: customConfig.chunkSize || (1024 * 1024),
      replication: {
        replicas: 3,
        writeQuorum: 2,
        readQuorum: 2,
      },
      erasure: {
        dataShards: 4,
        parityShards: 2,
      },
    },
  });

  // Attach Dashboard API & UI Router to Gateway
  gateway.customRouter = (req, res, url) => dashboardRouter.handleRequest(req, res, url);
  await gateway.start();

  logger.info(`✨ Vault Cluster online with ${storageNodes.length} nodes across 3 racks!`);
  logger.info(`👉 Web Dashboard: http://localhost:${gatewayPort}/dashboard`);
  logger.info(`👉 S3 REST API:   http://localhost:${gatewayPort}/<bucket>/<key>`);

  return {
    gateway,
    topology,
    storageNodes,
    metadataStore,
    failureDetector,
    hintedHandoff,
    readRepair,
    healingDaemon,
    backgroundScrubber,
    antiEntropy,
    dashboardRouter,
    async shutdown() {
      logger.warn('Shutting down Vault cluster...');
      failureDetector.stop();
      healingDaemon.stop();
      backgroundScrubber.stop();
      antiEntropy.stop();
      await gateway.stop();
      for (const node of storageNodes) {
        await node.stop();
      }
      logger.info('Vault cluster cleanly shutdown');
    },
  };
}

// Auto-run if executed directly
if (process.argv[1] && process.argv[1].endsWith('index.js')) {
  startCluster().catch(err => {
    console.error('Fatal cluster error:', err);
    process.exit(1);
  });
}
