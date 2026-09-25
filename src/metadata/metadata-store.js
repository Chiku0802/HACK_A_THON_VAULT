/**
 * Vault Object Storage - Metadata Catalog & Namespace Coordinator
 * Coordinates atomic bucket and object manifests, multi-part sessions, and WAL persistence.
 */

import { randomUUID } from 'node:crypto';
import { WriteAheadLog } from './wal.js';
import { NotFoundError, ConflictError } from '../common/types.js';
import { createLogger } from '../common/logger.js';

export class MetadataStore {
  constructor(metadataDir) {
    this.metadataDir = metadataDir;
    this.wal = new WriteAheadLog(metadataDir);
    this.buckets = new Map(); // bucketName -> { name, createdAt, defaultPolicy }
    this.objects = new Map(); // "bucket:key" -> { bucket, key, versions: [], latestVersion }
    this.multiparts = new Map(); // uploadId -> { uploadId, bucket, key, policy, policyConfig, parts: [], createdAt }
    this.logger = createLogger('Metadata');
    this.isInitialized = false;
  }

  async init() {
    if (this.isInitialized) return;

    await this.wal.init();

    // 1. Try loading snapshot
    const snapshotState = await this.wal.loadSnapshot();
    if (snapshotState) {
      if (snapshotState.buckets) {
        this.buckets = new Map(Object.entries(snapshotState.buckets));
      }
      if (snapshotState.objects) {
        this.objects = new Map(Object.entries(snapshotState.objects));
      }
    }

    // 2. Replay remaining WAL log entries
    const entries = await this.wal.replay();
    for (const entry of entries) {
      this._applyEntry(entry.action, entry.data);
    }

    // Default 'default' bucket if none exists
    if (!this.buckets.has('default')) {
      await this.createBucket('default', 'REPLICATION');
    }

    this.isInitialized = true;
    this.logger.info(`Metadata store ready: ${this.buckets.size} buckets, ${this.objects.size} objects loaded`);
  }

  _applyEntry(action, data) {
    switch (action) {
      case 'CREATE_BUCKET':
        this.buckets.set(data.name, data);
        break;
      case 'PUT_OBJECT': {
        const objKey = `${data.bucket}:${data.key}`;
        let record = this.objects.get(objKey);
        if (!record) {
          record = { bucket: data.bucket, key: data.key, versions: [], latestVersion: null };
          this.objects.set(objKey, record);
        }
        record.versions.push(data);
        record.latestVersion = data;
        break;
      }
      case 'DELETE_OBJECT': {
        const objKey = `${data.bucket}:${data.key}`;
        const record = this.objects.get(objKey);
        if (record && record.latestVersion) {
          const tombstone = {
            ...record.latestVersion,
            isDeleted: true,
            versionId: data.versionId || `del-${Date.now()}`,
            deletedAt: Date.now(),
          };
          record.versions.push(tombstone);
          record.latestVersion = tombstone;
        }
        break;
      }
      case 'UPDATE_CHUNK_PLACEMENT': {
        const objKey = `${data.bucket}:${data.key}`;
        const record = this.objects.get(objKey);
        if (record && record.latestVersion) {
          const chunks = record.latestVersion.chunks || [];
          for (const c of chunks) {
            if (c.chunkId === data.chunkId) {
              if (c.replicaNodes) {
                const idx = c.replicaNodes.indexOf(data.oldNodeId);
                if (idx !== -1) {
                  c.replicaNodes[idx] = data.newNodeId;
                } else if (!c.replicaNodes.includes(data.newNodeId)) {
                  c.replicaNodes.push(data.newNodeId);
                }
              }
              if (c.shards) {
                for (const s of c.shards) {
                  if (s.nodeId === data.oldNodeId && (data.shardIndex === undefined || s.shardIndex === data.shardIndex)) {
                    s.nodeId = data.newNodeId;
                  }
                }
              }
            }
          }
        }
        break;
      }
      default:
        break;
    }
  }

  async createBucket(name, defaultPolicy = 'REPLICATION') {
    if (this.buckets.has(name)) {
      return this.buckets.get(name);
    }
    const data = {
      name,
      createdAt: Date.now(),
      defaultPolicy,
    };
    await this.wal.append('CREATE_BUCKET', data);
    this.buckets.set(name, data);
    return data;
  }

  getBucket(name) {
    const bucket = this.buckets.get(name);
    if (!bucket) throw new NotFoundError(`Bucket '${name}' does not exist`);
    return bucket;
  }

  listBuckets() {
    return Array.from(this.buckets.values());
  }

  async putObjectMetadata({
    bucket,
    key,
    size,
    etag,
    contentType = 'application/octet-stream',
    policy = 'REPLICATION',
    policyConfig = {},
    chunks = [],
    customMeta = {},
  }) {
    this.getBucket(bucket); // Ensure bucket exists

    const versionId = `v-${Date.now()}-${randomUUID().substring(0, 8)}`;
    const metadata = {
      bucket,
      key,
      versionId,
      size,
      etag,
      contentType,
      policy,
      policyConfig,
      chunks,
      customMeta,
      isDeleted: false,
      lastModified: Date.now(),
    };

    await this.wal.append('PUT_OBJECT', metadata);

    const objKey = `${bucket}:${key}`;
    let record = this.objects.get(objKey);
    if (!record) {
      record = { bucket, key, versions: [], latestVersion: null };
      this.objects.set(objKey, record);
    }
    record.versions.push(metadata);
    record.latestVersion = metadata;

    return metadata;
  }

  getObjectMetadata(bucket, key, versionId = null) {
    this.getBucket(bucket);
    const objKey = `${bucket}:${key}`;
    const record = this.objects.get(objKey);

    if (!record || !record.latestVersion) {
      throw new NotFoundError(`Object '${key}' not found in bucket '${bucket}'`);
    }

    if (versionId) {
      const match = record.versions.find(v => v.versionId === versionId);
      if (!match || match.isDeleted) {
        throw new NotFoundError(`Version '${versionId}' of '${key}' not found or deleted`);
      }
      return match;
    }

    if (record.latestVersion.isDeleted) {
      throw new NotFoundError(`Object '${key}' was deleted in bucket '${bucket}'`);
    }

    return record.latestVersion;
  }

  listObjects(bucket, { prefix = '', limit = 1000 } = {}) {
    this.getBucket(bucket);
    const results = [];

    for (const [objKey, record] of this.objects.entries()) {
      if (!objKey.startsWith(`${bucket}:`)) continue;
      const latest = record.latestVersion;
      if (!latest || latest.isDeleted) continue;

      if (!prefix || latest.key.startsWith(prefix)) {
        results.push({
          bucket: latest.bucket,
          key: latest.key,
          versionId: latest.versionId,
          size: latest.size,
          etag: latest.etag,
          contentType: latest.contentType,
          policy: latest.policy,
          lastModified: latest.lastModified,
        });
      }

      if (results.length >= limit) break;
    }

    return results;
  }

  async deleteObject(bucket, key) {
    this.getBucket(bucket);
    const objKey = `${bucket}:${key}`;
    const record = this.objects.get(objKey);

    if (!record || !record.latestVersion || record.latestVersion.isDeleted) {
      throw new NotFoundError(`Object '${key}' not found or already deleted in bucket '${bucket}'`);
    }

    const versionId = `del-${Date.now()}`;
    await this.wal.append('DELETE_OBJECT', { bucket, key, versionId });

    const tombstone = {
      ...record.latestVersion,
      isDeleted: true,
      versionId,
      deletedAt: Date.now(),
    };
    record.versions.push(tombstone);
    record.latestVersion = tombstone;

    return { bucket, key, versionId, isDeleted: true };
  }

  /**
   * Updates chunk placement when a degraded chunk replica is migrated/repaired onto a new node.
   */
  async updateChunkPlacement(bucket, key, chunkId, oldNodeId, newNodeId, shardIndex = undefined) {
    const data = { bucket, key, chunkId, oldNodeId, newNodeId, shardIndex };
    await this.wal.append('UPDATE_CHUNK_PLACEMENT', data);
    this._applyEntry('UPDATE_CHUNK_PLACEMENT', data);
  }

  // --- Multipart Uploads ---

  initMultipart(bucket, key, policy = 'REPLICATION', policyConfig = {}) {
    this.getBucket(bucket);
    const uploadId = `mp-${Date.now()}-${randomUUID().substring(0, 8)}`;
    const session = {
      uploadId,
      bucket,
      key,
      policy,
      policyConfig,
      parts: new Map(), // partNumber -> { partNumber, etag, chunks, size }
      createdAt: Date.now(),
    };
    this.multiparts.set(uploadId, session);
    return session;
  }

  putMultipartPart(uploadId, partNumber, etag, chunks, size) {
    const session = this.multiparts.get(uploadId);
    if (!session) {
      throw new NotFoundError(`Multipart upload session '${uploadId}' not found`);
    }
    const part = { partNumber: Number(partNumber), etag, chunks, size };
    session.parts.set(part.partNumber, part);
    return part;
  }

  async completeMultipart(uploadId) {
    const session = this.multiparts.get(uploadId);
    if (!session) {
      throw new NotFoundError(`Multipart upload session '${uploadId}' not found`);
    }

    // Sort parts by part number
    const sortedParts = Array.from(session.parts.values()).sort((a, b) => a.partNumber - b.partNumber);
    if (sortedParts.length === 0) {
      throw new ConflictError(`Cannot complete multipart upload '${uploadId}': no parts uploaded`);
    }

    // Stitch all chunks together in order
    const combinedChunks = [];
    let totalSize = 0;
    let chunkIndex = 0;

    for (const p of sortedParts) {
      for (const c of p.chunks) {
        combinedChunks.push({
          ...c,
          index: chunkIndex++,
        });
      }
      totalSize += p.size;
    }

    // Combined etag: MD5/SHA256 representation of parts
    const combinedEtag = `mp-${sortedParts.map(p => p.etag).join('-').substring(0, 16)}-${sortedParts.length}`;

    const metadata = await this.putObjectMetadata({
      bucket: session.bucket,
      key: session.key,
      size: totalSize,
      etag: combinedEtag,
      policy: session.policy,
      policyConfig: session.policyConfig,
      chunks: combinedChunks,
    });

    this.multiparts.delete(uploadId);
    return metadata;
  }

  abortMultipart(uploadId) {
    const session = this.multiparts.get(uploadId);
    if (!session) throw new NotFoundError(`Multipart upload session '${uploadId}' not found`);
    this.multiparts.delete(uploadId);
    return { uploadId, aborted: true };
  }

  /**
   * Returns all active (non-deleted) objects for cluster-wide health inspection and repair.
   */
  getAllActiveObjects() {
    const list = [];
    for (const record of this.objects.values()) {
      if (record.latestVersion && !record.latestVersion.isDeleted) {
        list.push(record.latestVersion);
      }
    }
    return list;
  }
}
