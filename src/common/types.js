/**
 * Vault Object Storage - Common Types and Constants
 */

export const NodeStatus = {
  ALIVE: 'ALIVE',
  SUSPECT: 'SUSPECT',
  DEAD: 'DEAD',
};

export const StoragePolicy = {
  REPLICATION: 'REPLICATION',
  ERASURE: 'ERASURE',
};

export const ConsistencyLevel = {
  ONE: 'ONE',
  QUORUM: 'QUORUM',
  ALL: 'ALL',
};

export const RepairPriority = {
  CRITICAL: 1, // N-1 or M shards lost (on the brink of permanent data loss)
  HIGH: 2,     // 1 shard / replica lost
  MEDIUM: 3,   // Bitrot detected during scrubber
  LOW: 4,      // Cluster rebalance / node decommission
};

export class VaultError extends Error {
  constructor(message, code = 500, details = {}) {
    super(message);
    this.name = this.constructor.name;
    this.code = code;
    this.details = details;
  }
}

export class NotFoundError extends VaultError {
  constructor(message = 'Resource not found', details = {}) {
    super(message, 404, details);
  }
}

export class QuorumUnavailableError extends VaultError {
  constructor(message = 'Write/Read quorum could not be achieved', details = {}) {
    super(message, 503, details);
  }
}

export class CorruptionDetectedError extends VaultError {
  constructor(message = 'Data corruption or checksum mismatch detected', details = {}) {
    super(message, 422, details);
  }
}

export class ConflictError extends VaultError {
  constructor(message = 'Concurrent modification conflict', details = {}) {
    super(message, 409, details);
  }
}

export class PartitionError extends VaultError {
  constructor(message = 'Network partition prevents communication with target node', details = {}) {
    super(message, 504, details);
  }
}

export const DEFAULT_CONFIG = {
  chunkSize: 1024 * 1024, // 1MB default chunk size
  replication: {
    replicas: 3,
    writeQuorum: 2,
    readQuorum: 2,
  },
  erasure: {
    dataShards: 4,
    parityShards: 2,
  },
  heartbeatIntervalMs: 1000,
  suspectTimeoutMs: 2500,
  deadTimeoutMs: 5000,
  scrubIntervalMs: 15000,
  antiEntropyIntervalMs: 20000,
  healingIntervalMs: 2000,
  repairBandwidthBytesPerSec: 10 * 1024 * 1024, // 10MB/s token bucket
};
