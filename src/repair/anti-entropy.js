/**
 * Vault Object Storage - Anti-Entropy Sync with Merkle Trees
 * Compares Merkle trees between peer nodes in O(log N) time to pinpoint and synchronize missing chunks.
 */

import { MerkleTree } from '../common/crypto.js';
import { NodeClient } from '../cluster/node-client.js';
import { createLogger } from '../common/logger.js';

export class AntiEntropySync {
  constructor(topology, options = {}) {
    this.topology = topology;
    this.intervalMs = options.antiEntropyIntervalMs || 20000;
    this.nodeClient = new NodeClient('anti-entropy');
    this.logger = createLogger('AntiEntropy');
    this.timer = null;
    this.isRunning = false;
  }

  start() {
    if (this.isRunning) return;
    this.isRunning = true;
    this.timer = setInterval(() => this.runSync(), this.intervalMs);
  }

  stop() {
    this.isRunning = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Performs an anti-entropy exchange between two storage nodes.
   */
  async syncPeerPair(nodeA, nodeB) {
    try {
      const [merkleA, merkleB] = await Promise.all([
        this.nodeClient.getMerkle(nodeA),
        this.nodeClient.getMerkle(nodeB),
      ]);

      if (merkleA.rootHash === merkleB.rootHash) {
        // Fast path: Root hashes match, trees are identical!
        return { synced: true, transferred: 0 };
      }

      this.logger.info(`Anti-entropy detected divergence between ${nodeA.nodeId} and ${nodeB.nodeId}`);

      // Reconstruct Merkle trees locally to run fast diff
      const treeA = new MerkleTree(merkleA.items || []);
      const treeB = new MerkleTree(merkleB.items || []);

      const differingKeys = MerkleTree.diff(treeA, treeB);
      this.logger.info(`Found ${differingKeys.length} differing chunks between ${nodeA.nodeId} and ${nodeB.nodeId}`);

      let transferred = 0;
      const itemsMapA = new Map((merkleA.items || []).map(i => [i.key, i]));
      const itemsMapB = new Map((merkleB.items || []).map(i => [i.key, i]));

      for (const chunkId of differingKeys) {
        const itemA = itemsMapA.get(chunkId);
        const itemB = itemsMapB.get(chunkId);

        // If A has it but B doesn't, transfer A -> B
        if (itemA && !itemB) {
          try {
            const { payload, metadata } = await this.nodeClient.readChunk(nodeA, chunkId);
            await this.nodeClient.writeChunk(nodeB, chunkId, payload, metadata);
            transferred++;
            this.logger.info(`Anti-entropy synced chunk ${chunkId} from ${nodeA.nodeId} -> ${nodeB.nodeId}`);
          } catch (err) {
            this.logger.warn(`Failed syncing chunk ${chunkId} to ${nodeB.nodeId}: ${err.message}`);
          }
        }
        // If B has it but A doesn't, transfer B -> A
        else if (itemB && !itemA) {
          try {
            const { payload, metadata } = await this.nodeClient.readChunk(nodeB, chunkId);
            await this.nodeClient.writeChunk(nodeA, chunkId, payload, metadata);
            transferred++;
            this.logger.info(`Anti-entropy synced chunk ${chunkId} from ${nodeB.nodeId} -> ${nodeA.nodeId}`);
          } catch (err) {
            this.logger.warn(`Failed syncing chunk ${chunkId} to ${nodeA.nodeId}: ${err.message}`);
          }
        }
      }

      return { synced: true, transferred };
    } catch (err) {
      this.logger.warn(`Anti-entropy exchange failed between ${nodeA.nodeId} and ${nodeB.nodeId}: ${err.message}`);
      return { synced: false, error: err.message };
    }
  }

  async runSync() {
    const aliveNodes = this.topology.getAliveNodes();
    if (aliveNodes.length < 2) return;

    // Pick random pairs of alive nodes to exchange
    for (let i = 0; i < aliveNodes.length; i++) {
      const nextIdx = (i + 1) % aliveNodes.length;
      await this.syncPeerPair(aliveNodes[i], aliveNodes[nextIdx]);
    }
  }
}
