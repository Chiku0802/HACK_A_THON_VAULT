/**
 * Vault Object Storage - High Performance S3-Compatible REST Gateway
 * Coordinates distributed reads, writes, quorums, multipart uploads, range requests, and read repair.
 */

import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { sha256Hex } from '../common/crypto.js';
import { encodeErasure, decodeErasure } from '../common/erasure.js';
import { NodeClient } from '../cluster/node-client.js';
import { StoragePolicy, QuorumUnavailableError, NotFoundError, CorruptionDetectedError } from '../common/types.js';
import { createLogger } from '../common/logger.js';

export class StorageGateway {
  constructor({
    port,
    host = '0.0.0.0',
    topology,
    metadataStore,
    readRepair,
    hintedHandoff,
    healingDaemon,
    config = {},
  }) {
    this.port = port;
    this.host = host;
    this.topology = topology;
    this.metadataStore = metadataStore;
    this.readRepair = readRepair;
    this.hintedHandoff = hintedHandoff;
    this.healingDaemon = healingDaemon;
    this.config = {
      chunkSize: config.chunkSize || (1024 * 1024), // 1MB chunk size
      defaultPolicy: config.defaultPolicy || StoragePolicy.REPLICATION,
      replicas: config.replication?.replicas || 3,
      writeQuorum: config.replication?.writeQuorum || 2,
      readQuorum: config.replication?.readQuorum || 2,
      ecK: config.erasure?.dataShards || 4,
      ecM: config.erasure?.parityShards || 2,
      ...config,
    };

    this.nodeClient = new NodeClient('gateway');
    this.logger = createLogger('Gateway');
    this.server = null;
  }

  async start() {
    return new Promise((resolve, reject) => {
      this.server = http.createServer((req, res) => this._handleRequest(req, res));
      this.server.listen(this.port, this.host, () => {
        this.logger.info(`Vault Gateway listening at http://${this.host}:${this.port}`);
        resolve(this);
      });
      this.server.on('error', reject);
    });
  }

  async stop() {
    if (this.server) {
      return new Promise((resolve) => {
        this.server.close(() => {
          this.logger.info('Vault Gateway stopped');
          resolve();
        });
      });
    }
  }

  async _handleRequest(req, res) {
    // Enable CORS for web dashboard
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, PUT, POST, DELETE, HEAD, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', '*');
    res.setHeader('Access-Control-Expose-Headers', '*');

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const method = req.method;
    const pathname = url.pathname;

    try {
      // Delegate API or dashboard routes if handled elsewhere
      if (this.customRouter && await this.customRouter(req, res, url)) {
        return;
      }

      // Root ping
      if (pathname === '/' && method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          service: 'Vault Object Storage Gateway',
          version: '1.0.0',
          status: 'ONLINE',
          nodes: this.topology.getAllNodes().length,
          aliveNodes: this.topology.getAliveNodes().length,
        }));
        return;
      }

      // S3-style routing: /:bucket or /:bucket/:key
      const parts = pathname.split('/').filter(Boolean);
      if (parts.length === 0) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Bucket name required in path' }));
        return;
      }

      const bucket = parts[0];
      const key = parts.slice(1).join('/');

      // Bucket-level operations
      if (!key) {
        if (method === 'GET') {
          // List objects in bucket
          const prefix = url.searchParams.get('prefix') || '';
          const limit = parseInt(url.searchParams.get('limit') || '1000', 10);
          const objects = this.metadataStore.listObjects(bucket, { prefix, limit });
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ bucket, objects }));
          return;
        }

        if (method === 'PUT') {
          // Create bucket
          const policy = req.headers['x-vault-storage-policy'] || this.config.defaultPolicy;
          const created = await this.metadataStore.createBucket(bucket, policy);
          res.writeHead(201, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(created));
          return;
        }

        res.writeHead(405, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Method not allowed on bucket' }));
        return;
      }

      // Object-level operations

      // Multipart Upload operations
      if (url.searchParams.has('uploads') && method === 'POST') {
        const policy = req.headers['x-vault-storage-policy'] || this.config.defaultPolicy;
        const session = this.metadataStore.initMultipart(bucket, key, policy);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ uploadId: session.uploadId, bucket, key }));
        return;
      }

      if (url.searchParams.has('uploadId')) {
        const uploadId = url.searchParams.get('uploadId');
        if (url.searchParams.has('partNumber') && method === 'PUT') {
          const partNumber = parseInt(url.searchParams.get('partNumber'), 10);
          const part = await this._handleUploadPart(req, bucket, key, uploadId, partNumber);
          res.writeHead(200, { 'Content-Type': 'application/json', 'ETag': part.etag });
          res.end(JSON.stringify(part));
          return;
        }

        if (method === 'POST') {
          // Complete multipart
          const metadata = await this.metadataStore.completeMultipart(uploadId);
          res.writeHead(200, { 'Content-Type': 'application/json', 'ETag': metadata.etag });
          res.end(JSON.stringify(metadata));
          return;
        }

        if (method === 'DELETE') {
          // Abort multipart
          const aborted = this.metadataStore.abortMultipart(uploadId);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(aborted));
          return;
        }
      }

      // Standard Object GET
      if (method === 'GET') {
        await this._handleGetObject(req, res, bucket, key, url);
        return;
      }

      // Standard Object HEAD
      if (method === 'HEAD') {
        const meta = this.metadataStore.getObjectMetadata(bucket, key);
        res.writeHead(200, {
          'Content-Length': meta.size,
          'Content-Type': meta.contentType,
          'ETag': meta.etag,
          'X-Vault-Version-Id': meta.versionId,
          'X-Vault-Storage-Policy': meta.policy,
          'Last-Modified': new Date(meta.lastModified).toUTCString(),
        });
        res.end();
        return;
      }

      // Standard Object PUT
      if (method === 'PUT') {
        await this._handlePutObject(req, res, bucket, key);
        return;
      }

      // Standard Object DELETE
      if (method === 'DELETE') {
        const result = await this.metadataStore.deleteObject(bucket, key);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
        return;
      }

      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: `Method ${method} not allowed` }));
    } catch (err) {
      this._handleError(res, err);
    }
  }

  async _handlePutObject(req, res, bucket, key) {
    const policy = req.headers['x-vault-storage-policy'] || this.config.defaultPolicy;
    const contentType = req.headers['content-type'] || 'application/octet-stream';

    // Buffer incoming payload
    const bodyChunks = [];
    for await (const chunk of req) {
      bodyChunks.push(chunk);
    }
    const fullPayload = Buffer.concat(bodyChunks);
    const overallEtag = sha256Hex(fullPayload);

    const chunkSize = this.config.chunkSize;
    const numChunks = Math.max(1, Math.ceil(fullPayload.length / chunkSize));
    const storedChunks = [];

    // Store each chunk according to policy
    for (let i = 0; i < numChunks; i++) {
      const start = i * chunkSize;
      const end = Math.min(start + chunkSize, fullPayload.length);
      const chunkData = fullPayload.subarray(start, end);
      const chunkId = `chk-${randomUUID()}`;

      if (policy === StoragePolicy.REPLICATION) {
        const replicaCount = parseInt(req.headers['x-vault-replicas'] || this.config.replicas, 10);
        const writeQuorum = parseInt(req.headers['x-vault-write-quorum'] || this.config.writeQuorum, 10);

        const chunkMeta = await this._writeReplicationChunk({
          bucket,
          key,
          chunkId,
          chunkIndex: i,
          totalChunks: numChunks,
          chunkData,
          replicaCount,
          writeQuorum,
        });
        storedChunks.push(chunkMeta);
      } else if (policy === StoragePolicy.ERASURE) {
        const k = parseInt(req.headers['x-vault-ec-k'] || this.config.ecK, 10);
        const m = parseInt(req.headers['x-vault-ec-m'] || this.config.ecM, 10);

        const chunkMeta = await this._writeErasureChunk({
          bucket,
          key,
          chunkId,
          chunkIndex: i,
          totalChunks: numChunks,
          chunkData,
          k,
          m,
        });
        storedChunks.push(chunkMeta);
      } else {
        throw new Error(`Unsupported storage policy: ${policy}`);
      }
    }

    // Atomic metadata commit to WAL and Catalog
    const metadata = await this.metadataStore.putObjectMetadata({
      bucket,
      key,
      size: fullPayload.length,
      etag: overallEtag,
      contentType,
      policy,
      policyConfig: policy === StoragePolicy.REPLICATION
        ? { replicas: this.config.replicas, writeQuorum: this.config.writeQuorum }
        : { dataShards: this.config.ecK, parityShards: this.config.ecM },
      chunks: storedChunks,
    });

    res.writeHead(201, {
      'Content-Type': 'application/json',
      'ETag': overallEtag,
      'X-Vault-Version-Id': metadata.versionId,
      'X-Vault-Storage-Policy': policy,
    });
    res.end(JSON.stringify(metadata));
  }

  async _writeReplicationChunk({
    bucket,
    key,
    chunkId,
    chunkIndex,
    totalChunks,
    chunkData,
    replicaCount,
    writeQuorum,
  }) {
    const targetNodes = this.topology.selectTargetNodes(chunkId, replicaCount, { requireAlive: true });
    if (targetNodes.length < writeQuorum) {
      throw new QuorumUnavailableError(`Cannot achieve write quorum (${writeQuorum}): only ${targetNodes.length} alive nodes available`);
    }

    const chunkChecksum = sha256Hex(chunkData);
    const metadata = {
      bucket,
      key,
      chunkId,
      index: chunkIndex,
      totalChunks,
      checksumHex: chunkChecksum,
      payloadLength: chunkData.length,
      policy: StoragePolicy.REPLICATION,
    };

    // Parallel writes with Sloppy Quorum fallback
    const writePromises = targetNodes.map(async (node) => {
      try {
        await this.nodeClient.writeChunk(node, chunkId, chunkData, metadata);
        return { success: true, nodeId: node.nodeId };
      } catch (err) {
        // Fallback to surrogate node for Hinted Handoff
        const surrogateCandidates = this.topology.selectSurrogateNodes(chunkId, 1, targetNodes.map(n => n.nodeId));
        if (surrogateCandidates.length > 0) {
          const surrogate = surrogateCandidates[0];
          try {
            await this.hintedHandoff.storeHint(surrogate.nodeId, node.nodeId, chunkId, chunkData, metadata);
            return { success: true, nodeId: surrogate.nodeId, hintedFor: node.nodeId };
          } catch (hintErr) {
            return { success: false, nodeId: node.nodeId, error: hintErr.message };
          }
        }
        return { success: false, nodeId: node.nodeId, error: err.message };
      }
    });

    const results = await Promise.all(writePromises);
    const successful = results.filter(r => r.success);

    if (successful.length < writeQuorum) {
      throw new QuorumUnavailableError(`Write quorum failed for chunk ${chunkId}: ${successful.length}/${writeQuorum} replicas written`);
    }

    return {
      chunkId,
      index: chunkIndex,
      size: chunkData.length,
      checksumHex: chunkChecksum,
      replicaNodes: successful.map(s => s.nodeId),
    };
  }

  async _writeErasureChunk({
    bucket,
    key,
    chunkId,
    chunkIndex,
    totalChunks,
    chunkData,
    k,
    m,
  }) {
    const totalShards = k + m;
    const targetNodes = this.topology.selectTargetNodes(chunkId, totalShards, { requireAlive: true });

    // Quorum for erasure coding: at least K + ceil(M/2) shards
    const minRequired = k + Math.ceil(m / 2);
    if (targetNodes.length < minRequired) {
      throw new QuorumUnavailableError(`Cannot achieve erasure write quorum (${minRequired}): only ${targetNodes.length} alive nodes available`);
    }

    const encoded = encodeErasure(chunkData, k, m);
    const storedShards = [];

    const writePromises = targetNodes.slice(0, totalShards).map(async (node, shardIdx) => {
      const shardData = Buffer.from(encoded.shards[shardIdx]);
      const shardChunkId = `${chunkId}.s${shardIdx}`;
      const shardChecksum = sha256Hex(shardData);

      const metadata = {
        bucket,
        key,
        chunkId,
        shardIndex: shardIdx,
        index: chunkIndex,
        totalChunks,
        checksumHex: shardChecksum,
        payloadLength: shardData.length,
        policy: StoragePolicy.ERASURE,
      };

      try {
        await this.nodeClient.writeChunk(node, shardChunkId, shardData, metadata);
        return { success: true, shardIndex: shardIdx, nodeId: node.nodeId, checksumHex: shardChecksum };
      } catch (err) {
        return { success: false, shardIndex: shardIdx, nodeId: node.nodeId, error: err.message };
      }
    });

    const results = await Promise.all(writePromises);
    const successful = results.filter(r => r.success);

    if (successful.length < minRequired) {
      throw new QuorumUnavailableError(`Erasure write quorum failed: only ${successful.length}/${minRequired} shards written`);
    }

    return {
      chunkId,
      index: chunkIndex,
      originalSize: chunkData.length,
      shardSize: encoded.shardSize,
      k,
      m,
      shards: successful.map(s => ({
        shardIndex: s.shardIndex,
        nodeId: s.nodeId,
        checksumHex: s.checksumHex,
      })),
    };
  }

  async _handleGetObject(req, res, bucket, key, url) {
    const versionId = url.searchParams.get('versionId');
    const meta = this.metadataStore.getObjectMetadata(bucket, key, versionId);

    // Range header support
    const rangeHeader = req.headers.range;
    let startByte = 0;
    let endByte = meta.size - 1;
    let isPartial = false;

    if (rangeHeader && rangeHeader.startsWith('bytes=')) {
      const parts = rangeHeader.replace('bytes=', '').split('-');
      startByte = parseInt(parts[0], 10) || 0;
      if (parts[1]) {
        endByte = parseInt(parts[1], 10);
      }
      isPartial = true;
    }

    // Assemble chunk buffers
    const reconstructedBuffers = [];

    for (const chunk of meta.chunks) {
      let chunkBuffer;

      if (meta.policy === StoragePolicy.REPLICATION) {
        chunkBuffer = await this._readReplicationChunk(chunk, bucket, key);
      } else if (meta.policy === StoragePolicy.ERASURE) {
        chunkBuffer = await this._readErasureChunk(chunk, bucket, key);
      } else {
        throw new Error(`Unsupported policy: ${meta.policy}`);
      }

      reconstructedBuffers.push(chunkBuffer);
    }

    const fullObjectData = Buffer.concat(reconstructedBuffers);

    if (isPartial) {
      const slice = fullObjectData.subarray(startByte, endByte + 1);
      res.writeHead(206, {
        'Content-Type': meta.contentType,
        'Content-Range': `bytes ${startByte}-${endByte}/${meta.size}`,
        'Content-Length': slice.length,
        'ETag': meta.etag,
        'X-Vault-Version-Id': meta.versionId,
      });
      res.end(slice);
    } else {
      res.writeHead(200, {
        'Content-Type': meta.contentType,
        'Content-Length': fullObjectData.length,
        'ETag': meta.etag,
        'X-Vault-Version-Id': meta.versionId,
      });
      res.end(fullObjectData);
    }
  }

  async _readReplicationChunk(chunk, bucket, key) {
    const readQuorum = this.config.readQuorum || 2;
    const candidates = chunk.replicaNodes.map(id => this.topology.getNode(id)).filter(Boolean);

    // Read from replicas concurrently
    const readPromises = candidates.map(async (node) => {
      try {
        const result = await this.nodeClient.readChunk(node, chunk.chunkId);
        return { success: true, node, ...result };
      } catch (err) {
        return {
          success: false,
          node,
          isCorruption: err instanceof CorruptionDetectedError,
          error: err.message,
        };
      }
    });

    const results = await Promise.all(readPromises);
    const valid = results.filter(r => r.success);

    if (valid.length === 0) {
      throw new QuorumUnavailableError(`Failed to read chunk ${chunk.chunkId}: all replicas unreachable or corrupted`);
    }

    const canonical = valid[0];

    // Read Repair: If any replica failed with corruption or returned an outdated/missing chunk, heal it!
    for (const r of results) {
      if (!r.success && r.isCorruption) {
        this.logger.warn(`Read repair triggered for corrupted chunk ${chunk.chunkId} on node ${r.node.nodeId}`);
        this.readRepair.scheduleRepair({
          bucket,
          key,
          chunkId: chunk.chunkId,
          targetNodeId: r.node.nodeId,
          canonicalPayload: canonical.payload,
          metadata: canonical.metadata,
        });
      }
    }

    return canonical.payload;
  }

  async _readErasureChunk(chunk, bucket, key) {
    const k = chunk.k || this.config.ecK;
    const m = chunk.m || this.config.ecM;

    const readPromises = (chunk.shards || []).map(async (s) => {
      const node = this.topology.getNode(s.nodeId);
      if (!node) return { success: false, shardIndex: s.shardIndex, error: 'Node missing' };

      const shardChunkId = `${chunk.chunkId}.s${s.shardIndex}`;
      try {
        const res = await this.nodeClient.readChunk(node, shardChunkId);
        return {
          success: true,
          shardIndex: s.shardIndex,
          node,
          data: new Uint8Array(res.payload),
        };
      } catch (err) {
        return {
          success: false,
          shardIndex: s.shardIndex,
          node,
          isCorruption: err instanceof CorruptionDetectedError,
          error: err.message,
        };
      }
    });

    const results = await Promise.all(readPromises);
    const validShards = results.filter(r => r.success);

    if (validShards.length < k) {
      throw new QuorumUnavailableError(`Erasure read failed for chunk ${chunk.chunkId}: have ${validShards.length}/${k} required shards`);
    }

    // Decode original data
    const decoded = decodeErasure(
      validShards.map(s => ({ index: s.shardIndex, data: s.data })),
      k,
      m,
      chunk.shardSize,
      chunk.originalSize
    );

    // Read repair corrupted erasure shards if any
    const corruptedShards = results.filter(r => !r.success && r.isCorruption);
    if (corruptedShards.length > 0) {
      for (const cs of corruptedShards) {
        this.healingDaemon.reportCorruptedChunk(cs.node.nodeId, chunk.chunkId);
      }
    }

    return decoded;
  }

  async _handleUploadPart(req, bucket, key, uploadId, partNumber) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const partData = Buffer.concat(chunks);
    const partEtag = sha256Hex(partData);

    const chunkId = `chk-mp-${uploadId}-p${partNumber}`;
    const chunkMeta = await this._writeReplicationChunk({
      bucket,
      key,
      chunkId,
      chunkIndex: partNumber,
      totalChunks: 1,
      chunkData: partData,
      replicaCount: this.config.replicas,
      writeQuorum: this.config.writeQuorum,
    });

    return this.metadataStore.putMultipartPart(uploadId, partNumber, partEtag, [chunkMeta], partData.length);
  }

  _handleError(res, err) {
    const code = err.code || 500;
    this.logger.error(`Gateway error [${code}]: ${err.message}`);
    res.writeHead(code, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      error: err.message,
      code,
      name: err.name,
      details: err.details || {},
    }));
  }
}
