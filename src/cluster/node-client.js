/**
 * Vault Object Storage - Internal Node RPC Client
 * Facilitates HTTP communication between Gateway/Coordinator and Storage Nodes with network simulation.
 */

import http from 'node:http';
import { globalNetworkSim } from './network-sim.js';
import { CorruptionDetectedError, NotFoundError, PartitionError } from '../common/types.js';

export class NodeClient {
  constructor(fromEntityId = 'gateway') {
    this.fromId = fromEntityId;
  }

  async _request({ node, method, path, headers = {}, body = null, timeout = 3000 }) {
    // Check network simulation preflight
    await globalNetworkSim.preflightRpc(this.fromId, node.nodeId);

    return new Promise((resolve, reject) => {
      const options = {
        hostname: node.host,
        port: node.port,
        path,
        method,
        headers,
        timeout,
      };

      const req = http.request(options, (res) => {
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => {
          const resBody = Buffer.concat(chunks);
          const statusCode = res.statusCode;

          if (statusCode >= 200 && statusCode < 300) {
            resolve({
              statusCode,
              headers: res.headers,
              body: resBody,
            });
          } else if (statusCode === 404) {
            reject(new NotFoundError(`Chunk not found on node ${node.nodeId}`));
          } else if (statusCode === 422) {
            reject(new CorruptionDetectedError(`Corruption detected on node ${node.nodeId}`));
          } else if (statusCode === 503) {
            reject(new PartitionError(`Node ${node.nodeId} is dead/offline`));
          } else {
            let errorMsg = `HTTP ${statusCode}`;
            try {
              const parsed = JSON.parse(resBody.toString());
              if (parsed.error) errorMsg = parsed.error;
            } catch {}
            reject(new Error(`Node ${node.nodeId} RPC failed: ${errorMsg}`));
          }
        });
      });

      req.on('timeout', () => {
        req.destroy();
        reject(new PartitionError(`RPC timeout connecting to node ${node.nodeId}`));
      });

      req.on('error', (err) => {
        reject(new PartitionError(`Connection error to node ${node.nodeId}: ${err.message}`));
      });

      if (body) {
        req.write(body);
      }
      req.end();
    });
  }

  async writeChunk(node, chunkId, dataBuffer, metadata = {}, hintTarget = null) {
    const headers = {
      'Content-Type': 'application/octet-stream',
      'Content-Length': dataBuffer.length,
      'X-Vault-Chunk-Meta': JSON.stringify(metadata),
    };
    if (hintTarget) {
      headers['X-Vault-Hint-Target'] = hintTarget;
    }

    return this._request({
      node,
      method: 'PUT',
      path: `/chunks/${encodeURIComponent(chunkId)}`,
      headers,
      body: dataBuffer,
    });
  }

  async readChunk(node, chunkId) {
    const res = await this._request({
      node,
      method: 'GET',
      path: `/chunks/${encodeURIComponent(chunkId)}`,
    });

    let metadata = {};
    if (res.headers['x-vault-chunk-meta']) {
      try {
        metadata = JSON.parse(res.headers['x-vault-chunk-meta']);
      } catch {}
    }

    return {
      payload: res.body,
      checksum: res.headers['x-vault-checksum'],
      version: res.headers['x-vault-version'],
      metadata,
    };
  }

  async deleteChunk(node, chunkId) {
    try {
      const res = await this._request({
        node,
        method: 'DELETE',
        path: `/chunks/${encodeURIComponent(chunkId)}`,
      });
      return JSON.parse(res.body.toString());
    } catch (err) {
      if (err instanceof NotFoundError) return { deleted: false };
      throw err;
    }
  }

  async getNodeStatus(node) {
    const res = await this._request({
      node,
      method: 'GET',
      path: '/status',
      timeout: 1500,
    });
    return JSON.parse(res.body.toString());
  }

  async getMerkle(node) {
    const res = await this._request({
      node,
      method: 'GET',
      path: '/merkle',
      timeout: 3000,
    });
    return JSON.parse(res.body.toString());
  }

  async triggerScrub(node) {
    const res = await this._request({
      node,
      method: 'POST',
      path: '/scrub',
      timeout: 5000,
    });
    return JSON.parse(res.body.toString());
  }

  async corruptChunk(node, chunkId) {
    const res = await this._request({
      node,
      method: 'POST',
      path: `/chunks/${encodeURIComponent(chunkId)}/corrupt`,
    });
    return JSON.parse(res.body.toString());
  }
}
