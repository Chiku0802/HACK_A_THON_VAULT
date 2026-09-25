/**
 * Vault Object Storage - Failure Detector
 * Periodic heartbeat-based failure detector with SUSPECT and DEAD state transitions.
 */

import { EventEmitter } from 'node:events';
import { NodeStatus } from '../common/types.js';
import { NodeClient } from './node-client.js';
import { createLogger } from '../common/logger.js';

export class FailureDetector extends EventEmitter {
  constructor(topology, options = {}) {
    super();
    this.topology = topology;
    this.heartbeatIntervalMs = options.heartbeatIntervalMs || 1000;
    this.suspectTimeoutMs = options.suspectTimeoutMs || 2500;
    this.deadTimeoutMs = options.deadTimeoutMs || 5000;
    this.nodeClient = new NodeClient('failure-detector');
    this.logger = createLogger('FailureDetector');

    this.lastSeen = new Map(); // nodeId -> timestamp
    this.timer = null;
    this.isRunning = false;
  }

  start() {
    if (this.isRunning) return;
    this.isRunning = true;
    this.timer = setInterval(() => this._probeAll(), this.heartbeatIntervalMs);
    // Initial immediate probe
    this._probeAll();
  }

  stop() {
    this.isRunning = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async _probeAll() {
    const nodes = this.topology.getAllNodes();
    const now = Date.now();

    await Promise.allSettled(nodes.map(async (node) => {
      const currentStatus = node.status;
      try {
        await this.nodeClient.getNodeStatus(node);
        this.lastSeen.set(node.nodeId, now);

        if (currentStatus !== NodeStatus.ALIVE) {
          this.topology.updateNodeStatus(node.nodeId, NodeStatus.ALIVE);
          this.logger.info(`Node ${node.nodeId} transitioned from ${currentStatus} -> ALIVE`);
          this.emit('node:status_change', {
            nodeId: node.nodeId,
            oldStatus: currentStatus,
            newStatus: NodeStatus.ALIVE,
          });
        }
      } catch {
        // Heartbeat probe failed
        const last = this.lastSeen.get(node.nodeId) || 0;
        const elapsed = now - last;

        let nextStatus = currentStatus;
        if (elapsed > this.deadTimeoutMs) {
          nextStatus = NodeStatus.DEAD;
        } else if (elapsed > this.suspectTimeoutMs) {
          nextStatus = NodeStatus.SUSPECT;
        }

        if (nextStatus !== currentStatus) {
          this.topology.updateNodeStatus(node.nodeId, nextStatus);
          this.logger.warn(`Node ${node.nodeId} transitioned from ${currentStatus} -> ${nextStatus} (no response for ${elapsed}ms)`);
          this.emit('node:status_change', {
            nodeId: node.nodeId,
            oldStatus: currentStatus,
            newStatus: nextStatus,
          });
        }
      }
    }));
  }
}
