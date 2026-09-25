/**
 * Vault Object Storage - Network Simulator & Chaos Injection Engine
 * Simulates network partitions, latency spikes, and packet drops between nodes.
 */

import { PartitionError } from '../common/types.js';
import { createLogger } from '../common/logger.js';

export class NetworkSimulator {
  constructor() {
    this.partitions = new Set(); // Strings of "nodeA:nodeB" representing severed links
    this.isolatedNodes = new Set(); // Node IDs completely cut off from the cluster
    this.nodeLatencies = new Map(); // nodeId -> ms delay
    this.packetLossRates = new Map(); // nodeId -> 0.0 - 1.0 drop probability
    this.logger = createLogger('NetworkSim');
  }

  /**
   * Partitions the cluster into two or more disjoint groups.
   * Nodes in group A cannot communicate with nodes in group B.
   */
  createPartition(groupA, groupB) {
    for (const a of groupA) {
      for (const b of groupB) {
        this.partitions.add(`${a}:${b}`);
        this.partitions.add(`${b}:${a}`);
      }
    }
    this.logger.warn(`Network partition created between [${groupA.join(',')}] and [${groupB.join(',')}]`);
  }

  /**
   * Completely cuts off a single node from all peers and the gateway.
   */
  isolateNode(nodeId) {
    this.isolatedNodes.add(nodeId);
    this.logger.warn(`Node ${nodeId} has been completely isolated from the network`);
  }

  reconnectNode(nodeId) {
    this.isolatedNodes.delete(nodeId);
    // Remove individual partition links
    for (const key of Array.from(this.partitions)) {
      if (key.startsWith(`${nodeId}:`) || key.endsWith(`:${nodeId}`)) {
        this.partitions.delete(key);
      }
    }
    this.logger.info(`Node ${nodeId} reconnected to the network`);
  }

  healAll() {
    this.partitions.clear();
    this.isolatedNodes.clear();
    this.nodeLatencies.clear();
    this.packetLossRates.clear();
    this.logger.info(`All network partitions and faults healed`);
  }

  setNodeLatency(nodeId, delayMs) {
    this.nodeLatencies.set(nodeId, delayMs);
  }

  setPacketLoss(nodeId, rate) {
    this.packetLossRates.set(nodeId, Math.max(0, Math.min(1, rate)));
  }

  /**
   * Checks if communication from source to destination is blocked.
   */
  isBlocked(fromId, toId) {
    if (this.isolatedNodes.has(fromId) || this.isolatedNodes.has(toId)) {
      return true;
    }
    return this.partitions.has(`${fromId}:${toId}`) || this.partitions.has(`${toId}:${fromId}`);
  }

  /**
   * Applies simulated network conditions (drops, delays, partitions) before an RPC.
   */
  async preflightRpc(fromId, toId) {
    if (this.isBlocked(fromId, toId)) {
      throw new PartitionError(`Network partition between ${fromId} and ${toId}`);
    }

    // Packet loss simulation
    const loss = this.packetLossRates.get(toId) || 0;
    if (loss > 0 && Math.random() < loss) {
      throw new PartitionError(`Simulated packet loss communicating with ${toId}`);
    }

    // Latency simulation
    const delay = this.nodeLatencies.get(toId) || 0;
    if (delay > 0) {
      await new Promise(r => setTimeout(r, delay));
    }
  }

  getSimState() {
    return {
      isolatedNodes: Array.from(this.isolatedNodes),
      partitionLinks: Array.from(this.partitions),
      latencies: Object.fromEntries(this.nodeLatencies),
      packetLoss: Object.fromEntries(this.packetLossRates),
    };
  }
}

export const globalNetworkSim = new NetworkSimulator();
