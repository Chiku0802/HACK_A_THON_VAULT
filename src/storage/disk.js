/**
 * Vault Object Storage - Node Disk Storage Engine
 * Manages atomic chunk storage, structured integrity headers, and bitrot detection.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { sha256Hex, crc32 } from '../common/crypto.js';
import { CorruptionDetectedError, NotFoundError } from '../common/types.js';
import { createLogger } from '../common/logger.js';

const MAGIC_BYTES = Buffer.from('VAULT01\0'); // 8 bytes magic header

export class DiskStorage {
  constructor(nodeId, dataDir) {
    this.nodeId = nodeId;
    this.dataDir = path.resolve(dataDir);
    this.chunksDir = path.join(this.dataDir, 'chunks');
    this.walDir = path.join(this.dataDir, 'wal');
    this.hintsDir = path.join(this.dataDir, 'hints');
    this.logger = createLogger(`Disk:${nodeId}`);
  }

  async init() {
    await fs.mkdir(this.chunksDir, { recursive: true });
    await fs.mkdir(this.walDir, { recursive: true });
    await fs.mkdir(this.hintsDir, { recursive: true });
  }

  getChunkPath(chunkId) {
    // Sanitize chunkId to safe filename
    const safeName = chunkId.replace(/[^a-zA-Z0-9_\-\.]/g, '_');
    return path.join(this.chunksDir, `${safeName}.chk`);
  }

  /**
   * Writes a chunk atomically to disk with integrity header and SHA-256 checksum.
   */
  async writeChunk(chunkId, payloadBuffer, metadata = {}) {
    await this.init();

    const payload = Buffer.isBuffer(payloadBuffer) ? payloadBuffer : Buffer.from(payloadBuffer);
    const payloadHash = sha256Hex(payload);

    const fullMeta = {
      chunkId,
      timestamp: Date.now(),
      payloadLength: payload.length,
      checksumHex: payloadHash,
      ...metadata,
    };

    const metaJson = Buffer.from(JSON.stringify(fullMeta), 'utf8');
    const metaLen = metaJson.length;
    if (metaLen > 65535) {
      throw new Error('Chunk metadata exceeds maximum allowable size (64KB)');
    }

    // Header buffer: Magic (8B) + MetaLen (2B uint16) + MetaJson + Header CRC32 (4B uint32)
    const headerWithoutCrc = Buffer.alloc(8 + 2 + metaLen);
    MAGIC_BYTES.copy(headerWithoutCrc, 0);
    headerWithoutCrc.writeUInt16BE(metaLen, 8);
    metaJson.copy(headerWithoutCrc, 10);

    const headerCrc = crc32(headerWithoutCrc);
    const crcBuf = Buffer.alloc(4);
    crcBuf.writeUInt32BE(headerCrc, 0);

    const fullBuffer = Buffer.concat([headerWithoutCrc, crcBuf, payload]);

    const targetPath = this.getChunkPath(chunkId);
    const tempPath = `${targetPath}.tmp.${randomUUID()}`;

    // Write to temp file, fsync, and atomic rename
    const fileHandle = await fs.open(tempPath, 'w');
    try {
      await fileHandle.write(fullBuffer);
      await fileHandle.sync(); // Force flush to physical media
    } finally {
      await fileHandle.close();
    }

    await fs.rename(tempPath, targetPath);
    return { chunkId, bytesWritten: fullBuffer.length, checksum: payloadHash };
  }

  /**
   * Reads a chunk from disk and performs cryptographic integrity verification.
   * Throws CorruptionDetectedError on bitrot.
   */
  async readChunk(chunkId, verifyChecksum = true) {
    const targetPath = this.getChunkPath(chunkId);
    let fullBuffer;
    try {
      fullBuffer = await fs.readFile(targetPath);
    } catch (err) {
      if (err.code === 'ENOENT') {
        throw new NotFoundError(`Chunk ${chunkId} not found on node ${this.nodeId}`);
      }
      throw err;
    }

    // Minimum size: 8 (magic) + 2 (metaLen) + metaJson + 4 (crc)
    if (fullBuffer.length < 14) {
      throw new CorruptionDetectedError(`Chunk file ${chunkId} is truncated (size ${fullBuffer.length})`);
    }

    // Verify magic
    if (!fullBuffer.subarray(0, 8).equals(MAGIC_BYTES)) {
      throw new CorruptionDetectedError(`Invalid magic header in chunk ${chunkId}`);
    }

    const metaLen = fullBuffer.readUInt16BE(8);
    const metaOffset = 10;
    const crcOffset = metaOffset + metaLen;
    const payloadOffset = crcOffset + 4;

    if (fullBuffer.length < payloadOffset) {
      throw new CorruptionDetectedError(`Chunk ${chunkId} corrupted: buffer length smaller than header offset`);
    }

    // Verify header CRC
    const headerWithoutCrc = fullBuffer.subarray(0, crcOffset);
    const expectedCrc = fullBuffer.readUInt32BE(crcOffset);
    const computedCrc = crc32(headerWithoutCrc);
    if (computedCrc !== expectedCrc) {
      throw new CorruptionDetectedError(`Header CRC mismatch on chunk ${chunkId}: expected ${expectedCrc}, computed ${computedCrc}`);
    }

    const metaJsonStr = fullBuffer.subarray(metaOffset, crcOffset).toString('utf8');
    let metadata;
    try {
      metadata = JSON.parse(metaJsonStr);
    } catch (parseErr) {
      throw new CorruptionDetectedError(`Corrupted metadata JSON in chunk ${chunkId}`);
    }

    const payload = fullBuffer.subarray(payloadOffset);

    // Verify payload length
    if (payload.length !== metadata.payloadLength) {
      throw new CorruptionDetectedError(`Payload length mismatch on chunk ${chunkId}: expected ${metadata.payloadLength}, got ${payload.length}`);
    }

    // Verify payload SHA-256
    if (verifyChecksum) {
      const computedHash = sha256Hex(payload);
      if (computedHash !== metadata.checksumHex) {
        throw new CorruptionDetectedError(`Bitrot detected on chunk ${chunkId}: stored SHA-256 ${metadata.checksumHex} != computed ${computedHash}`);
      }
    }

    return { payload, metadata };
  }

  /**
   * Chaos injection: intentionally corrupt random bytes in the chunk payload on disk.
   */
  async injectCorruption(chunkId, flipCount = 5) {
    const targetPath = this.getChunkPath(chunkId);
    const fullBuffer = await fs.readFile(targetPath);
    if (fullBuffer.length <= 20) {
      throw new Error(`Chunk ${chunkId} too small to corrupt`);
    }

    // Flip bytes inside the payload section (after header)
    const metaLen = fullBuffer.readUInt16BE(8);
    const payloadStart = 10 + metaLen + 4;
    const payloadLen = fullBuffer.length - payloadStart;

    if (payloadLen <= 0) {
      throw new Error(`Chunk ${chunkId} has empty payload`);
    }

    const corrupted = Buffer.from(fullBuffer);
    for (let i = 0; i < flipCount; i++) {
      const offset = payloadStart + Math.floor(Math.random() * payloadLen);
      corrupted[offset] ^= 0xFF; // Invert bits
    }

    await fs.writeFile(targetPath, corrupted);
    this.logger.warn(`Injected bitrot corruption into chunk ${chunkId} at ${targetPath}`);
    return { chunkId, corruptedBytes: flipCount };
  }

  async deleteChunk(chunkId) {
    const targetPath = this.getChunkPath(chunkId);
    try {
      await fs.unlink(targetPath);
      return true;
    } catch (err) {
      if (err.code === 'ENOENT') return false;
      throw err;
    }
  }

  async hasChunk(chunkId) {
    const targetPath = this.getChunkPath(chunkId);
    try {
      await fs.stat(targetPath);
      return true;
    } catch {
      return false;
    }
  }

  async listChunks() {
    await this.init();
    const files = await fs.readdir(this.chunksDir);
    const chunkFiles = files.filter(f => f.endsWith('.chk'));

    const list = [];
    for (const f of chunkFiles) {
      const chunkId = f.replace(/\.chk$/, '');
      try {
        const filePath = path.join(this.chunksDir, f);
        const st = await fs.stat(filePath);
        list.push({ chunkId, sizeOnDisk: st.size, mtime: st.mtimeMs });
      } catch {
        // file could have been deleted concurrently
      }
    }
    return list;
  }

  /**
   * Scans all stored chunks and verifies cryptographic integrity against bitrot.
   */
  async scanAndVerifyAll() {
    const chunks = await this.listChunks();
    const healthy = [];
    const corrupted = [];

    for (const item of chunks) {
      try {
        const { metadata } = await this.readChunk(item.chunkId, true);
        healthy.push({ chunkId: item.chunkId, checksum: metadata.checksumHex, size: metadata.payloadLength });
      } catch (err) {
        if (err instanceof CorruptionDetectedError) {
          corrupted.push({ chunkId: item.chunkId, error: err.message });
        } else {
          corrupted.push({ chunkId: item.chunkId, error: err.message });
        }
      }
    }

    return {
      nodeId: this.nodeId,
      totalScanned: chunks.length,
      healthy,
      corrupted,
      timestamp: Date.now(),
    };
  }

  async getDiskUsage() {
    const chunks = await this.listChunks();
    let totalBytes = 0;
    for (const c of chunks) totalBytes += c.sizeOnDisk;
    return {
      chunkCount: chunks.length,
      usedBytes: totalBytes,
    };
  }
}
