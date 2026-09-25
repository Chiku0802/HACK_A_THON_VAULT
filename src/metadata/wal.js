/**
 * Vault Object Storage - Write-Ahead Log (WAL)
 * Provides crash consistency, durability, and recovery replay for metadata state.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { crc32 } from '../common/crypto.js';
import { createLogger } from '../common/logger.js';

export class WriteAheadLog {
  constructor(walDir) {
    this.walDir = path.resolve(walDir);
    this.logFile = path.join(this.walDir, 'metadata.wal');
    this.snapshotFile = path.join(this.walDir, 'metadata.snapshot');
    this.logger = createLogger('WAL');
    this.fileHandle = null;
    this.sequence = 0;
  }

  async init() {
    await fs.mkdir(this.walDir, { recursive: true });
    this.fileHandle = await fs.open(this.logFile, 'a+');
  }

  /**
   * Appends a log entry atomically: Length (uint32) + CRC32 (uint32) + JSON Payload
   */
  async append(action, data) {
    if (!this.fileHandle) await this.init();

    this.sequence++;
    const entry = {
      seq: this.sequence,
      ts: Date.now(),
      action,
      data,
    };

    const payload = Buffer.from(JSON.stringify(entry), 'utf8');
    const entryCrc = crc32(payload);

    const record = Buffer.alloc(8 + payload.length);
    record.writeUInt32BE(payload.length, 0);
    record.writeUInt32BE(entryCrc, 4);
    payload.copy(record, 8);

    await this.fileHandle.write(record);
    await this.fileHandle.sync(); // Force physical flush

    return this.sequence;
  }

  /**
   * Replays WAL and returns ordered list of valid committed actions.
   */
  async replay() {
    if (!this.fileHandle) await this.init();

    let buffer;
    try {
      buffer = await fs.readFile(this.logFile);
    } catch {
      return [];
    }

    const actions = [];
    let offset = 0;

    while (offset + 8 <= buffer.length) {
      const len = buffer.readUInt32BE(offset);
      const expectedCrc = buffer.readUInt32BE(offset + 4);

      if (offset + 8 + len > buffer.length) {
        this.logger.warn(`Truncated WAL record at offset ${offset}, stopping replay`);
        break;
      }

      const payload = buffer.subarray(offset + 8, offset + 8 + len);
      const computedCrc = crc32(payload);

      if (computedCrc !== expectedCrc) {
        this.logger.error(`WAL CRC mismatch at offset ${offset}, stopping replay`);
        break;
      }

      try {
        const entry = JSON.parse(payload.toString('utf8'));
        actions.push(entry);
        this.sequence = Math.max(this.sequence, entry.seq || 0);
      } catch (err) {
        this.logger.error(`Corrupt JSON in WAL entry at offset ${offset}: ${err.message}`);
        break;
      }

      offset += 8 + len;
    }

    this.logger.info(`Replayed ${actions.length} WAL records (last seq: ${this.sequence})`);
    return actions;
  }

  /**
   * Creates a snapshot of current state and truncates the WAL
   */
  async snapshot(currentState) {
    const snapData = Buffer.from(JSON.stringify({
      sequence: this.sequence,
      timestamp: Date.now(),
      state: currentState,
    }), 'utf8');

    const tempSnap = `${this.snapshotFile}.tmp`;
    await fs.writeFile(tempSnap, snapData, 'utf8');
    await fs.rename(tempSnap, this.snapshotFile);

    // Truncate current WAL
    if (this.fileHandle) {
      await this.fileHandle.close();
    }
    await fs.writeFile(this.logFile, Buffer.alloc(0));
    this.fileHandle = await fs.open(this.logFile, 'a+');
    this.logger.info(`Created metadata snapshot at sequence ${this.sequence}`);
  }

  async loadSnapshot() {
    try {
      const content = await fs.readFile(this.snapshotFile, 'utf8');
      const snap = JSON.parse(content);
      this.sequence = snap.sequence || 0;
      return snap.state;
    } catch {
      return null;
    }
  }

  async close() {
    if (this.fileHandle) {
      await this.fileHandle.close();
      this.fileHandle = null;
    }
  }
}
