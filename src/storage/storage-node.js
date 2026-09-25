/**
 * Vault Object Storage - Storage Node Daemon
 * Represents an independent, fault-tolerant storage node with HTTP interface.
 */

import http from 'node:http';
import { DiskStorage } from './disk.js';
import { NodeMerkleSync } from './merkle.js';
import { createLogger } from '../common/logger.js';
import { CorruptionDetectedError, NotFoundError, NodeStatus } from '../common/types.js';

export class StorageNode {
  constructor({ nodeId, port, dataDir, rack = 'rack-1', host = '127.0.0.1' }) {
    this.nodeId = nodeId;
    this.port = port;
    this.host = host;
    this.rack = rack;
    this.dataDir = dataDir;
    this.disk = new DiskStorage(nodeId, dataDir);
    this.merkle = new NodeMerkleSync(this.disk);
    this.logger = createLogger(`Node:${nodeId}`);

    this.server = null;
    this.status = NodeStatus.ALIVE;
    this.isPaused = false; // For simulating latency / partition
    this.artificialLatencyMs = 0;
    this.startTime = Date.now();
  }

  async start() {
    await this.disk.init();

    return new Promise((resolve, reject) => {
      this.server = http.createServer((req, res) => this._handleRequest(req, res));
      this.server.listen(this.port, this.host, () => {
        this.logger.info(`Storage Node online at http://${this.host}:${this.port} [Rack: ${this.rack}]`);
        resolve(this);
      });
      this.server.on('error', reject);
    });
  }

  async stop() {
    this.status = NodeStatus.DEAD;
    if (this.server) {
      return new Promise((resolve) => {
        this.server.close(() => {
          this.logger.warn(`Storage Node stopped`);
          resolve();
        });
      });
    }
  }

  async _handleRequest(req, res) {
    if (this.status === NodeStatus.DEAD) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: `Node ${this.nodeId} is DEAD/OFFLINE` }));
      return;
    }

    if (this.artificialLatencyMs > 0) {
      await new Promise(r => setTimeout(r, this.artificialLatencyMs));
    }

    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const method = req.method;
    const pathname = url.pathname;

    try {
      // Endpoint: GET /status
      if (method === 'GET' && pathname === '/status') {
        const usage = await this.disk.getDiskUsage();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          nodeId: this.nodeId,
          rack: this.rack,
          status: this.status,
          port: this.port,
          host: this.host,
          diskUsage: usage,
          uptimeSec: Math.floor((Date.now() - this.startTime) / 1000),
        }));
        return;
      }

      // Endpoint: GET /chunks
      if (method === 'GET' && pathname === '/chunks') {
        const list = await this.disk.listChunks();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ nodeId: this.nodeId, count: list.length, chunks: list }));
        return;
      }

      // Endpoint: PUT /chunks/:chunkId
      if (method === 'PUT' && pathname.startsWith('/chunks/')) {
        const chunkId = decodeURIComponent(pathname.replace('/chunks/', ''));
        const hintTarget = req.headers['x-vault-hint-target'];
        let meta = {};
        if (req.headers['x-vault-chunk-meta']) {
          try {
            meta = JSON.parse(req.headers['x-vault-chunk-meta']);
          } catch {
            meta = {};
          }
        }

        const chunks = [];
        for await (const chunk of req) {
          chunks.push(chunk);
        }
        const body = Buffer.concat(chunks);

        // If this is a hinted handoff intended for a temporarily unreachable node
        if (hintTarget) {
          meta.isHint = true;
          meta.targetNodeId = hintTarget;
          meta.hintTimestamp = Date.now();
        }

        const result = await this.disk.writeChunk(chunkId, body, meta);
        res.writeHead(201, {
          'Content-Type': 'application/json',
          'ETag': result.checksum,
        });
        res.end(JSON.stringify(result));
        return;
      }

      // Endpoint: GET /chunks/:chunkId
      if (method === 'GET' && pathname.startsWith('/chunks/')) {
        const chunkId = decodeURIComponent(pathname.replace('/chunks/', ''));
        try {
          const { payload, metadata } = await this.disk.readChunk(chunkId, true);
          res.writeHead(200, {
            'Content-Type': 'application/octet-stream',
            'Content-Length': payload.length,
            'X-Vault-Checksum': metadata.checksumHex,
            'X-Vault-Version': metadata.versionId || '1',
            'X-Vault-Chunk-Meta': JSON.stringify(metadata),
          });
          res.end(payload);
        } catch (err) {
          if (err instanceof NotFoundError) {
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: err.message, code: 404 }));
          } else if (err instanceof CorruptionDetectedError) {
            this.logger.error(`Read error: bitrot corruption detected for ${chunkId}`, { error: err.message });
            res.writeHead(422, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: err.message, code: 422, corruption: true }));
          } else {
            throw err;
          }
        }
        return;
      }

      // Endpoint: HEAD /chunks/:chunkId
      if (method === 'HEAD' && pathname.startsWith('/chunks/')) {
        const chunkId = decodeURIComponent(pathname.replace('/chunks/', ''));
        try {
          const { metadata } = await this.disk.readChunk(chunkId, false);
          res.writeHead(200, {
            'Content-Length': metadata.payloadLength,
            'X-Vault-Checksum': metadata.checksumHex,
            'X-Vault-Version': metadata.versionId || '1',
          });
          res.end();
        } catch (err) {
          if (err instanceof NotFoundError) {
            res.writeHead(404);
            res.end();
          } else {
            res.writeHead(500);
            res.end();
          }
        }
        return;
      }

      // Endpoint: DELETE /chunks/:chunkId
      if (method === 'DELETE' && pathname.startsWith('/chunks/')) {
        const chunkId = decodeURIComponent(pathname.replace('/chunks/', ''));
        const deleted = await this.disk.deleteChunk(chunkId);
        res.writeHead(deleted ? 200 : 404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ chunkId, deleted }));
        return;
      }

      // Chaos Endpoint: POST /chunks/:chunkId/corrupt
      if (method === 'POST' && pathname.includes('/corrupt')) {
        const parts = pathname.split('/');
        const chunkId = decodeURIComponent(parts[2]);
        const corrupted = await this.disk.injectCorruption(chunkId);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(corrupted));
        return;
      }

      // Endpoint: GET /merkle
      if (method === 'GET' && pathname === '/merkle') {
        const tree = await this.merkle.buildTree(true);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          nodeId: this.nodeId,
          rootHash: tree.getRootHash(),
          items: tree.items,
        }));
        return;
      }

      // Endpoint: POST /scrub
      if (method === 'POST' && pathname === '/scrub') {
        const report = await this.disk.scanAndVerifyAll();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(report));
        return;
      }

      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: `Not found: ${method} ${pathname}` }));
    } catch (err) {
      this.logger.error(`Internal server error in ${method} ${pathname}`, { error: err.message, stack: err.stack });
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
  }
}
