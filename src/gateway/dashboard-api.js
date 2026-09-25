/**
 * Vault Object Storage - Cluster Management & Chaos Engineering API
 * Powers the real-time visual web dashboard and CLI administration.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { globalNetworkSim } from '../cluster/network-sim.js';
import { NodeStatus } from '../common/types.js';
import { createLogger } from '../common/logger.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export class DashboardApiRouter {
  constructor({
    topology,
    metadataStore,
    storageNodes,
    healingDaemon,
    readRepair,
    hintedHandoff,
    antiEntropy,
    backgroundScrubber,
  }) {
    this.topology = topology;
    this.metadataStore = metadataStore;
    this.storageNodes = new Map(storageNodes.map(n => [n.nodeId, n]));
    this.healingDaemon = healingDaemon;
    this.readRepair = readRepair;
    this.hintedHandoff = hintedHandoff;
    this.antiEntropy = antiEntropy;
    this.backgroundScrubber = backgroundScrubber;
    this.logger = createLogger('DashboardAPI');

    this.sseClients = new Set();
  }

  registerStorageNode(node) {
    this.storageNodes.set(node.nodeId, node);
  }

  broadcastEvent(eventType, payload) {
    const msg = `event: ${eventType}\ndata: ${JSON.stringify(payload)}\n\n`;
    for (const res of this.sseClients) {
      try {
        res.write(msg);
      } catch {
        this.sseClients.delete(res);
      }
    }
  }

  async handleRequest(req, res, url) {
    const pathname = url.pathname;
    const method = req.method;

    // Static Web Dashboard files
    if (pathname === '/dashboard' || pathname === '/dashboard/' || pathname === '/dashboard/index.html') {
      await this._serveStatic(res, 'public/index.html', 'text/html');
      return true;
    }
    if (pathname === '/dashboard/app.js') {
      await this._serveStatic(res, 'public/app.js', 'application/javascript');
      return true;
    }
    if (pathname === '/dashboard/style.css') {
      await this._serveStatic(res, 'public/style.css', 'text/css');
      return true;
    }

    // SSE Events Stream: GET /api/events
    if (pathname === '/api/events' && method === 'GET') {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
      });
      res.write('\n');
      this.sseClients.add(res);

      req.on('close', () => {
        this.sseClients.delete(res);
      });
      return true;
    }

    // Cluster Status: GET /api/cluster/status
    if (pathname === '/api/cluster/status' && method === 'GET') {
      const nodesData = [];
      for (const node of this.topology.getAllNodes()) {
        const daemon = this.storageNodes.get(node.nodeId);
        let usage = { chunkCount: 0, usedBytes: 0 };
        if (daemon) {
          try {
            usage = await daemon.disk.getDiskUsage();
          } catch {}
        }
        nodesData.push({
          ...node,
          diskUsage: usage,
        });
      }

      const activeObjects = this.metadataStore.getAllActiveObjects();
      const pendingHints = await this.hintedHandoff.getPendingHints();
      const networkSim = globalNetworkSim.getSimState();
      const healingStats = this.healingDaemon.getStats();

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        nodes: nodesData,
        objects: activeObjects,
        buckets: this.metadataStore.listBuckets(),
        pendingHints,
        networkSim,
        healing: healingStats,
        readRepairCount: this.readRepair.repairCount,
        timestamp: Date.now(),
      }));
      return true;
    }

    // Chaos: Kill Node: POST /api/chaos/kill-node
    if (pathname === '/api/chaos/kill-node' && method === 'POST') {
      const body = await this._parseJsonBody(req);
      const { nodeId } = body;
      const node = this.storageNodes.get(nodeId);
      if (!node) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: `Node ${nodeId} not found` }));
        return true;
      }

      this.topology.updateNodeStatus(nodeId, NodeStatus.DEAD);
      globalNetworkSim.isolateNode(nodeId);
      this.broadcastEvent('node_killed', { nodeId });
      this.logger.warn(`Chaos: Node ${nodeId} killed via API`);

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ nodeId, status: NodeStatus.DEAD }));
      return true;
    }

    // Chaos: Revive Node: POST /api/chaos/revive-node
    if (pathname === '/api/chaos/revive-node' && method === 'POST') {
      const body = await this._parseJsonBody(req);
      const { nodeId } = body;
      const node = this.storageNodes.get(nodeId);
      if (!node) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: `Node ${nodeId} not found` }));
        return true;
      }

      globalNetworkSim.reconnectNode(nodeId);
      this.topology.updateNodeStatus(nodeId, NodeStatus.ALIVE);

      // Drain any hinted handoffs destined for this recovered node
      const replayed = await this.hintedHandoff.replayHintsForNode(nodeId);

      this.broadcastEvent('node_revived', { nodeId, replayedHints: replayed });
      this.logger.info(`Chaos: Node ${nodeId} revived, replayed ${replayed} hints`);

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ nodeId, status: NodeStatus.ALIVE, replayedHints: replayed }));
      return true;
    }

    // Chaos: Corrupt Chunk (Bitrot): POST /api/chaos/corrupt-chunk
    if (pathname === '/api/chaos/corrupt-chunk' && method === 'POST') {
      const body = await this._parseJsonBody(req);
      const { nodeId, chunkId } = body;
      const node = this.storageNodes.get(nodeId);
      if (!node) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: `Node ${nodeId} not found` }));
        return true;
      }

      const result = await node.disk.injectCorruption(chunkId);
      this.broadcastEvent('chunk_corrupted', { nodeId, chunkId });

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result));
      return true;
    }

    // Chaos: Network Partition: POST /api/chaos/partition
    if (pathname === '/api/chaos/partition' && method === 'POST') {
      const body = await this._parseJsonBody(req);
      const { groupA, groupB } = body;
      globalNetworkSim.createPartition(groupA, groupB);
      this.broadcastEvent('partition_created', { groupA, groupB });

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ partition: { groupA, groupB } }));
      return true;
    }

    // Chaos: Heal All Network Faults: POST /api/chaos/heal
    if (pathname === '/api/chaos/heal' && method === 'POST') {
      globalNetworkSim.healAll();
      for (const node of this.topology.getAllNodes()) {
        this.topology.updateNodeStatus(node.nodeId, NodeStatus.ALIVE);
        await this.hintedHandoff.replayHintsForNode(node.nodeId);
      }

      this.broadcastEvent('network_healed', {});
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ healed: true }));
      return true;
    }

    // Actions: Manual Scrub Trigger: POST /api/actions/scrub
    if (pathname === '/api/actions/scrub' && method === 'POST') {
      const report = await this.backgroundScrubber.runScrub();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(report));
      return true;
    }

    // Actions: Manual Healing Cycle: POST /api/actions/heal
    if (pathname === '/api/actions/heal' && method === 'POST') {
      await this.healingDaemon.runHealingCycle();
      const stats = this.healingDaemon.getStats();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(stats));
      return true;
    }

    // Actions: Anti-Entropy Sync: POST /api/actions/anti-entropy
    if (pathname === '/api/actions/anti-entropy' && method === 'POST') {
      await this.antiEntropy.runSync();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ synced: true }));
      return true;
    }

    return false; // Not a dashboard route
  }

  async _serveStatic(res, relativePath, mimeType) {
    const filePath = path.join(__dirname, relativePath);
    try {
      const content = await fs.readFile(filePath);
      res.writeHead(200, { 'Content-Type': mimeType });
      res.end(content);
    } catch (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end(`File not found: ${relativePath}`);
    }
  }

  async _parseJsonBody(req) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    try {
      return JSON.parse(Buffer.concat(chunks).toString('utf8'));
    } catch {
      return {};
    }
  }
}
