/**
 * Vault Object Storage - Node Merkle Tree Engine for Anti-Entropy Sync
 */

import { MerkleTree } from '../common/crypto.js';

export class NodeMerkleSync {
  constructor(diskStorage) {
    this.disk = diskStorage;
    this.cachedTree = null;
    this.lastBuiltTime = 0;
  }

  async buildTree(force = false) {
    const now = Date.now();
    // Cache for 2 seconds unless forced
    if (!force && this.cachedTree && (now - this.lastBuiltTime < 2000)) {
      return this.cachedTree;
    }

    const chunks = await this.disk.listChunks();
    const items = [];

    for (const c of chunks) {
      try {
        // Fast read without payload verification for index building
        const { metadata } = await this.disk.readChunk(c.chunkId, false);
        items.push({
          key: c.chunkId,
          version: metadata.versionId || 'v1',
          hash: metadata.checksumHex,
        });
      } catch {
        // Corrupted or removed
      }
    }

    this.cachedTree = new MerkleTree(items);
    this.lastBuiltTime = now;
    return this.cachedTree;
  }

  async getRootHash() {
    const tree = await this.buildTree();
    return tree.getRootHash();
  }
}
