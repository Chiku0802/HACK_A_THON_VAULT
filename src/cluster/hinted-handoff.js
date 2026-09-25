/**
 * Vault Object Storage - Hinted Handoff Engine
 * Spools writes for temporarily unreachable or partitioned nodes onto surrogate nodes,
 * and automatically drains/replays them upon target node recovery.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { NodeClient } from './node-client.js';
import { createLogger } from '../common/logger.js';

export class HintedHandoffManager {
  constructor(topology, storageNodes = []) {
    this.topology = topology;
    this.storageNodes = new Map(storageNodes.map(n => [n.nodeId, n]));
    this.nodeClient = new NodeClient('hinted-handoff');
    this.logger = createLogger('HintedHandoff');
  }

  registerStorageNode(node) {
    this.storageNodes.set(node.nodeId, node);
  }

  /**
   * Spools a hinted write locally on a surrogate node's disk hints directory.
   */
  async storeHint(surrogateNodeId, targetNodeId, chunkId, payloadBuffer, metadata = {}) {
    const surrogate = this.storageNodes.get(surrogateNodeId);
    if (!surrogate) return false;

    const hintDir = surrogate.disk.hintsDir;
    const hintFile = path.join(hintDir, `${targetNodeId}___${chunkId}.hint`);
    const hintData = {
      targetNodeId,
      chunkId,
      metadata,
      payloadBase64: payloadBuffer.toString('base64'),
      timestamp: Date.now(),
    };

    await fs.writeFile(hintFile, JSON.stringify(hintData), 'utf8');
    this.logger.info(`Hinted handoff saved on ${surrogateNodeId} for target ${targetNodeId} (chunk: ${chunkId})`);
    return true;
  }

  /**
   * Replays all buffered hints destined for a recovered node.
   */
  async replayHintsForNode(recoveredNodeId) {
    const recoveredNode = this.topology.getNode(recoveredNodeId);
    if (!recoveredNode || recoveredNode.status !== 'ALIVE') {
      return 0;
    }

    let replayedCount = 0;

    for (const [surrogateId, node] of this.storageNodes.entries()) {
      if (surrogateId === recoveredNodeId) continue;
      const hintDir = node.disk.hintsDir;

      let files = [];
      try {
        files = await fs.readdir(hintDir);
      } catch {
        continue;
      }

      for (const f of files) {
        if (!f.startsWith(`${recoveredNodeId}___`) || !f.endsWith('.hint')) continue;
        const filePath = path.join(hintDir, f);

        try {
          const content = await fs.readFile(filePath, 'utf8');
          const hint = JSON.parse(content);
          const payload = Buffer.from(hint.payloadBase64, 'base64');

          // Send to recovered node
          await this.nodeClient.writeChunk(recoveredNode, hint.chunkId, payload, hint.metadata);
          // Delete hint on success
          await fs.unlink(filePath);
          replayedCount++;
          this.logger.info(`Successfully replayed hint from ${surrogateId} -> ${recoveredNodeId} (chunk: ${hint.chunkId})`);
        } catch (err) {
          this.logger.warn(`Failed to replay hint ${f} to ${recoveredNodeId}: ${err.message}`);
        }
      }
    }

    return replayedCount;
  }

  /**
   * Returns summary of all currently pending hints across the cluster.
   */
  async getPendingHints() {
    const pending = [];
    for (const [surrogateId, node] of this.storageNodes.entries()) {
      const hintDir = node.disk.hintsDir;
      try {
        const files = await fs.readdir(hintDir);
        for (const f of files) {
          if (f.endsWith('.hint')) {
            const [targetNodeId, chunkId] = f.replace('.hint', '').split('___');
            pending.push({ surrogateId, targetNodeId, chunkId });
          }
        }
      } catch {}
    }
    return pending;
  }
}
