/**
 * Vault Object Storage - Cryptographic primitives and Merkle Tree for Anti-Entropy
 */

import { createHash } from 'node:crypto';

/**
 * Returns SHA-256 buffer of input
 */
export function sha256(data) {
  return createHash('sha256').update(data).digest();
}

/**
 * Returns SHA-256 hex string
 */
export function sha256Hex(data) {
  return createHash('sha256').update(data).digest('hex');
}

/**
 * Fast CRC32 calculation table
 */
const CRC_TABLE = new Uint32Array(256);
for (let i = 0; i < 256; i++) {
  let c = i;
  for (let k = 0; k < 8; k++) {
    c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
  }
  CRC_TABLE[i] = c >>> 0;
}

export function crc32(buf) {
  let crc = 0xFFFFFFFF;
  for (let i = 0; i < buf.length; i++) {
    crc = (crc >>> 8) ^ CRC_TABLE[(crc ^ buf[i]) & 0xFF];
  }
  return (crc ^ 0xFFFFFFFF) >>> 0;
}

/**
 * Merkle Tree Node for Anti-Entropy
 */
export class MerkleNode {
  constructor(hash, left = null, right = null, keyRange = null) {
    this.hash = hash;
    this.left = left;
    this.right = right;
    this.keyRange = keyRange; // { start, end } for subtree bounding
  }
}

/**
 * Hierarchical Merkle Tree over a set of items { key, version, hash }
 */
export class MerkleTree {
  constructor(items = []) {
    // items: array of { key: string, version: string|number, hash: string }
    this.items = [...items].sort((a, b) => a.key.localeCompare(b.key));
    this.root = this._buildTree(this.items);
  }

  _buildTree(items) {
    if (items.length === 0) {
      return new MerkleNode('e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'); // empty SHA256
    }
    if (items.length === 1) {
      const item = items[0];
      const leafHash = sha256Hex(`${item.key}:${item.version}:${item.hash}`);
      return new MerkleNode(leafHash, null, null, { start: item.key, end: item.key, item });
    }

    const mid = Math.floor(items.length / 2);
    const leftChild = this._buildTree(items.slice(0, mid));
    const rightChild = this._buildTree(items.slice(mid));

    const combinedHash = sha256Hex(`${leftChild.hash}:${rightChild.hash}`);
    const keyRange = {
      start: items[0].key,
      end: items[items.length - 1].key,
    };

    return new MerkleNode(combinedHash, leftChild, rightChild, keyRange);
  }

  getRootHash() {
    return this.root ? this.root.hash : null;
  }

  /**
   * Compares this Merkle tree with another Merkle tree in O(diff * log N) time
   * Returns array of differing keys
   */
  static diff(treeA, treeB) {
    const differingKeys = new Set();

    function traverse(nodeA, nodeB) {
      if (!nodeA && !nodeB) return;

      if (!nodeA && nodeB) {
        // All items in nodeB are missing in A
        collectAllKeys(nodeB, differingKeys);
        return;
      }

      if (nodeA && !nodeB) {
        // All items in nodeA are missing in B
        collectAllKeys(nodeA, differingKeys);
        return;
      }

      if (nodeA.hash === nodeB.hash) {
        // Subtrees are identical, skip!
        return;
      }

      // If either is a leaf
      if (!nodeA.left && !nodeA.right) {
        if (nodeA.keyRange?.item) differingKeys.add(nodeA.keyRange.item.key);
      }
      if (!nodeB.left && !nodeB.right) {
        if (nodeB.keyRange?.item) differingKeys.add(nodeB.keyRange.item.key);
      }

      if ((nodeA.left || nodeA.right) && (nodeB.left || nodeB.right)) {
        traverse(nodeA.left, nodeB.left);
        traverse(nodeA.right, nodeB.right);
      }
    }

    function collectAllKeys(node, set) {
      if (!node) return;
      if (node.keyRange?.item) {
        set.add(node.keyRange.item.key);
      }
      collectAllKeys(node.left, set);
      collectAllKeys(node.right, set);
    }

    traverse(treeA.root, treeB.root);
    return Array.from(differingKeys);
  }
}
