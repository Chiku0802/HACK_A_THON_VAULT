/**
 * Vault Object Storage - Topology & Failure Domain Placement Engine
 * Implements deterministic consistent hashing with Rack-Aware and Failure-Domain isolation.
 */

import { sha256Hex } from '../common/crypto.js';

export class ClusterTopology {
  constructor(nodes = []) {
    this.nodes = new Map(); // nodeId -> { nodeId, host, port, rack, status }
    for (const n of nodes) {
      this.addNode(n);
    }
  }

  addNode(node) {
    this.nodes.set(node.nodeId, {
      nodeId: node.nodeId,
      host: node.host || '127.0.0.1',
      port: node.port,
      rack: node.rack || 'rack-1',
      status: node.status || 'ALIVE',
    });
  }

  removeNode(nodeId) {
    this.nodes.delete(nodeId);
  }

  updateNodeStatus(nodeId, status) {
    const n = this.nodes.get(nodeId);
    if (n) {
      n.status = status;
    }
  }

  getNode(nodeId) {
    return this.nodes.get(nodeId);
  }

  getAllNodes() {
    return Array.from(this.nodes.values());
  }

  getAliveNodes() {
    return Array.from(this.nodes.values()).filter(n => n.status === 'ALIVE');
  }

  /**
   * Deterministic rendezvous (highest random weight) hashing score
   * Maps key + nodeId -> 64-bit integer score
   */
  _computeScore(key, nodeId) {
    const hash = sha256Hex(`${key}:${nodeId}`);
    return parseInt(hash.substring(0, 12), 16);
  }

  /**
   * Selects N target nodes for replication or K+M nodes for erasure coding.
   * Enforces rack/failure-domain diversity:
   * 1. Prioritizes picking nodes from distinct racks.
   * 2. Never assigns the same node twice.
   * 3. Falls back to round-robin across alive nodes if racks are limited.
   */
  selectTargetNodes(key, count, { requireAlive = true, excludeNodeIds = [] } = {}) {
    const candidates = Array.from(this.nodes.values())
      .filter(n => !excludeNodeIds.includes(n.nodeId))
      .filter(n => (requireAlive ? n.status === 'ALIVE' : true));

    if (candidates.length === 0) {
      return [];
    }

    // Sort candidates by deterministic rendezvous score for this key
    const scored = candidates.map(node => ({
      node,
      score: this._computeScore(key, node.nodeId),
    })).sort((a, b) => b.score - a.score);

    const selected = [];
    const usedRacks = new Set();
    const deferredSameRack = [];

    // First pass: select nodes from different racks
    for (const item of scored) {
      if (selected.length >= count) break;
      if (!usedRacks.has(item.node.rack)) {
        selected.push(item.node);
        usedRacks.add(item.node.rack);
      } else {
        deferredSameRack.push(item.node);
      }
    }

    // Second pass: if we still need more nodes, allow same rack
    for (const node of deferredSameRack) {
      if (selected.length >= count) break;
      selected.push(node);
    }

    return selected;
  }

  /**
   * Selects fallback surrogate nodes for Hinted Handoff when primary nodes are offline/partitioned.
   */
  selectSurrogateNodes(key, count, excludeNodeIds = []) {
    return this.selectTargetNodes(key, count, {
      requireAlive: true,
      excludeNodeIds,
    });
  }
}
