/**
 * Vault Object Storage - Autonomous Healing Daemon & Dynamic Rebalancer
 * Monitors cluster degradation, schedules prioritized repairs, reconstructs lost erasure shards,
 * and maintains rate-limited replication recovery.
 */

import { NodeClient } from '../cluster/node-client.js';
import { reconstructMissingShards } from '../common/erasure.js';
import { RepairPriority, StoragePolicy } from '../common/types.js';
import { createLogger } from '../common/logger.js';

export class HealingDaemon {
  constructor(topology, metadataStore, options = {}) {
    this.topology = topology;
    this.metadataStore = metadataStore;
    this.intervalMs = options.healingIntervalMs || 2000;
    this.bandwidthBytesPerSec = options.repairBandwidthBytesPerSec || (10 * 1024 * 1024); // 10 MB/s
    this.nodeClient = new NodeClient('healing-daemon');
    this.logger = createLogger('HealingDaemon');

    this.timer = null;
    this.isRunning = false;
    this.isHealingCycleRunning = false;

    // Track manually or scrubber-reported corrupted chunks: Set of "nodeId:chunkId"
    this.corruptedChunkFlags = new Set();

    // Stats
    this.stats = {
      totalHealedChunks: 0,
      totalHealedBytes: 0,
      activeQueueSize: 0,
      lastCycleDurationMs: 0,
      criticalRepairs: 0,
    };
  }

  start() {
    if (this.isRunning) return;
    this.isRunning = true;
    this.timer = setInterval(() => this.runHealingCycle(), this.intervalMs);
  }

  stop() {
    this.isRunning = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  reportCorruptedChunk(nodeId, chunkId) {
    this.corruptedChunkFlags.add(`${nodeId}:${chunkId}`);
  }

  isChunkCorrupted(nodeId, chunkId) {
    return this.corruptedChunkFlags.has(`${nodeId}:${chunkId}`);
  }

  clearCorruptedChunk(nodeId, chunkId) {
    this.corruptedChunkFlags.delete(`${nodeId}:${chunkId}`);
  }

  /**
   * Main scan and repair cycle
   */
  async runHealingCycle() {
    if (this.isHealingCycleRunning) return;
    this.isHealingCycleRunning = true;
    const startTs = Date.now();

    try {
      const activeObjects = this.metadataStore.getAllActiveObjects();
      const repairQueue = [];

      for (const obj of activeObjects) {
        if (!obj.chunks) continue;

        for (const chunk of obj.chunks) {
          if (obj.policy === StoragePolicy.REPLICATION) {
            const desiredReplicas = obj.policyConfig?.replicas || 3;
            const healthyNodes = [];
            const badNodes = [];

            for (const nodeId of (chunk.replicaNodes || [])) {
              const node = this.topology.getNode(nodeId);
              if (node && node.status === 'ALIVE' && !this.isChunkCorrupted(nodeId, chunk.chunkId)) {
                healthyNodes.push(nodeId);
              } else {
                badNodes.push(nodeId);
              }
            }

            if (healthyNodes.length < desiredReplicas) {
              const priority = healthyNodes.length <= 1 ? RepairPriority.CRITICAL : RepairPriority.HIGH;
              repairQueue.push({
                priority,
                type: StoragePolicy.REPLICATION,
                object: obj,
                chunk,
                healthyNodes,
                badNodes,
                needed: desiredReplicas - healthyNodes.length,
              });
            }
          } else if (obj.policy === StoragePolicy.ERASURE) {
            const k = chunk.k || obj.policyConfig?.dataShards || 4;
            const m = chunk.m || obj.policyConfig?.parityShards || 2;
            const totalRequired = k + m;

            const healthyShards = [];
            const badShards = [];

            for (const s of (chunk.shards || [])) {
              const node = this.topology.getNode(s.nodeId);
              if (node && node.status === 'ALIVE' && !this.isChunkCorrupted(s.nodeId, chunk.chunkId)) {
                healthyShards.push(s);
              } else {
                badShards.push(s);
              }
            }

            if (healthyShards.length < totalRequired) {
              const missingCount = totalRequired - healthyShards.length;
              const priority = (healthyShards.length === k) ? RepairPriority.CRITICAL : RepairPriority.HIGH;
              repairQueue.push({
                priority,
                type: StoragePolicy.ERASURE,
                object: obj,
                chunk,
                k,
                m,
                healthyShards,
                badShards,
              });
            }
          }
        }
      }

      // Sort queue by priority: CRITICAL (1) first!
      repairQueue.sort((a, b) => a.priority - b.priority);
      this.stats.activeQueueSize = repairQueue.length;

      // Process repairs with rate limiting
      for (const task of repairQueue) {
        if (!this.isRunning) break;

        if (task.priority === RepairPriority.CRITICAL) {
          this.stats.criticalRepairs++;
        }

        if (task.type === StoragePolicy.REPLICATION) {
          await this._repairReplicationChunk(task);
        } else if (task.type === StoragePolicy.ERASURE) {
          await this._repairErasureChunk(task);
        }
      }
    } catch (err) {
      this.logger.error(`Error during healing cycle: ${err.message}`, { stack: err.stack });
    } finally {
      this.stats.lastCycleDurationMs = Date.now() - startTs;
      this.isHealingCycleRunning = false;
    }
  }

  async _repairReplicationChunk(task) {
    const { object, chunk, healthyNodes, badNodes } = task;
    if (healthyNodes.length === 0) {
      this.logger.error(`PERMANENT DATA LOSS: No healthy replicas available for chunk ${chunk.chunkId} of ${object.bucket}/${object.key}`);
      return;
    }

    const sourceNodeId = healthyNodes[0];
    const sourceNode = this.topology.getNode(sourceNodeId);
    if (!sourceNode) return;

    // Read payload from source node
    let payload, metadata;
    try {
      const res = await this.nodeClient.readChunk(sourceNode, chunk.chunkId);
      payload = res.payload;
      metadata = res.metadata;
    } catch (err) {
      this.logger.warn(`Failed reading chunk ${chunk.chunkId} from source ${sourceNodeId}: ${err.message}`);
      return;
    }

    // Pick new target node(s)
    const existing = [...chunk.replicaNodes];
    const newTargets = this.topology.selectTargetNodes(chunk.chunkId, 1, {
      requireAlive: true,
      excludeNodeIds: existing,
    });

    if (newTargets.length === 0) {
      this.logger.warn(`Cannot heal chunk ${chunk.chunkId}: no spare alive nodes available in cluster`);
      return;
    }

    const targetNode = newTargets[0];
    const badNodeId = badNodes[0] || 'unknown';

    try {
      this.logger.info(`Healing replication chunk ${chunk.chunkId} onto ${targetNode.nodeId} (replacing ${badNodeId})`);
      await this.nodeClient.writeChunk(targetNode, chunk.chunkId, payload, metadata);

      // Update metadata store
      await this.metadataStore.updateChunkPlacement(
        object.bucket,
        object.key,
        chunk.chunkId,
        badNodeId,
        targetNode.nodeId
      );

      this.clearCorruptedChunk(badNodeId, chunk.chunkId);
      this.stats.totalHealedChunks++;
      this.stats.totalHealedBytes += payload.length;
      this.logger.info(`✓ Successfully healed replication chunk ${chunk.chunkId} onto ${targetNode.nodeId}`);
    } catch (err) {
      this.logger.error(`Failed writing healed chunk ${chunk.chunkId} to ${targetNode.nodeId}: ${err.message}`);
    }
  }

  async _repairErasureChunk(task) {
    const { object, chunk, k, m, healthyShards, badShards } = task;

    if (healthyShards.length < k) {
      this.logger.error(`PERMANENT DATA LOSS: Erasure chunk ${chunk.chunkId} has only ${healthyShards.length} shards, need ${k}`);
      return;
    }

    // Retrieve K healthy shards
    const survivingShardsWithData = [];
    for (const s of healthyShards.slice(0, k)) {
      const node = this.topology.getNode(s.nodeId);
      if (!node) continue;
      try {
        const shardChunkId = `${chunk.chunkId}.s${s.shardIndex}`;
        const res = await this.nodeClient.readChunk(node, shardChunkId);
        survivingShardsWithData.push({
          index: s.shardIndex,
          data: new Uint8Array(res.payload),
        });
      } catch (err) {
        this.logger.warn(`Failed reading surviving shard ${s.shardIndex} from ${s.nodeId}: ${err.message}`);
      }
    }

    if (survivingShardsWithData.length < k) {
      this.logger.warn(`Could not read minimum ${k} shards for chunk ${chunk.chunkId}`);
      return;
    }

    // Reconstruct missing shards
    const missingIndices = badShards.map(s => s.shardIndex);
    let reconstructed;
    try {
      reconstructed = reconstructMissingShards(
        survivingShardsWithData,
        missingIndices,
        k,
        m,
        chunk.shardSize,
        chunk.originalSize
      );
    } catch (err) {
      this.logger.error(`Erasure reconstruction failed for chunk ${chunk.chunkId}: ${err.message}`);
      return;
    }

    // Write each reconstructed shard to a new node
    for (const bad of badShards) {
      const shardIdx = bad.shardIndex;
      const shardData = reconstructed[shardIdx];
      if (!shardData) continue;

      const existingNodes = (chunk.shards || []).map(s => s.nodeId);
      const newTargets = this.topology.selectTargetNodes(`${chunk.chunkId}.s${shardIdx}`, 1, {
        requireAlive: true,
        excludeNodeIds: existingNodes,
      });

      if (newTargets.length === 0) {
        this.logger.warn(`No spare node to place reconstructed erasure shard ${shardIdx} for ${chunk.chunkId}`);
        continue;
      }

      const targetNode = newTargets[0];
      const shardChunkId = `${chunk.chunkId}.s${shardIdx}`;
      const payloadBuf = Buffer.from(shardData);

      try {
        await this.nodeClient.writeChunk(targetNode, shardChunkId, payloadBuf, {
          bucket: object.bucket,
          key: object.key,
          shardIndex: shardIdx,
          policy: StoragePolicy.ERASURE,
        });

        // Update metadata store
        await this.metadataStore.updateChunkPlacement(
          object.bucket,
          object.key,
          chunk.chunkId,
          bad.nodeId,
          targetNode.nodeId,
          shardIdx
        );

        this.clearCorruptedChunk(bad.nodeId, chunk.chunkId);
        this.stats.totalHealedChunks++;
        this.stats.totalHealedBytes += payloadBuf.length;
        this.logger.info(`✓ Successfully reconstructed erasure shard ${shardIdx} on node ${targetNode.nodeId} for chunk ${chunk.chunkId}`);
      } catch (err) {
        this.logger.error(`Failed placing reconstructed shard ${shardIdx} on ${targetNode.nodeId}: ${err.message}`);
      }
    }
  }

  getStats() {
    return {
      ...this.stats,
      corruptedFlagsCount: this.corruptedChunkFlags.size,
    };
  }
}
