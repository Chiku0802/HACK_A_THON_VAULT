/**
 * Vault Object Storage - Read Repair Engine
 * Executes asynchronous background repair when corrupted or outdated replicas are detected during GET.
 */

import { NodeClient } from '../cluster/node-client.js';
import { createLogger } from '../common/logger.js';

export class ReadRepairEngine {
  constructor(topology, metadataStore) {
    this.topology = topology;
    this.metadataStore = metadataStore;
    this.nodeClient = new NodeClient('read-repair');
    this.logger = createLogger('ReadRepair');
    this.repairCount = 0;
  }

  /**
   * Schedules an asynchronous repair for a corrupted or outdated replica.
   */
  scheduleRepair({
    bucket,
    key,
    chunkId,
    targetNodeId,
    canonicalPayload,
    metadata = {},
  }) {
    // Fire and forget asynchronously
    setImmediate(async () => {
      try {
        const targetNode = this.topology.getNode(targetNodeId);
        if (!targetNode || targetNode.status !== 'ALIVE') {
          this.logger.warn(`Cannot read-repair ${chunkId}: target node ${targetNodeId} is not ALIVE`);
          return;
        }

        this.logger.info(`Starting Read Repair for chunk ${chunkId} on node ${targetNodeId}`);
        await this.nodeClient.writeChunk(targetNode, chunkId, canonicalPayload, metadata);
        this.repairCount++;
        this.logger.info(`Read Repair SUCCESS for chunk ${chunkId} on node ${targetNodeId} (total repaired: ${this.repairCount})`);
      } catch (err) {
        this.logger.error(`Read Repair FAILED for chunk ${chunkId} on node ${targetNodeId}: ${err.message}`);
      }
    });
  }
}
