/**
 * Vault Object Storage - Reed-Solomon Erasure Coding Engine
 * Implements Maximum Distance Separable (MDS) codes over Galois Field GF(2^8).
 * Enables configurable (K + M) data + parity shard partitioning and reconstruction.
 */

// Initialize GF(2^8) exponential and logarithm tables
// Irreducible primitive polynomial: x^8 + x^4 + x^3 + x^2 + 1 = 0x11D (285)
const EXP_TABLE = new Uint8Array(512);
const LOG_TABLE = new Uint8Array(256);

let val = 1;
for (let i = 0; i < 255; i++) {
  EXP_TABLE[i] = val;
  EXP_TABLE[i + 255] = val;
  LOG_TABLE[val] = i;
  val <<= 1;
  if (val >= 256) val ^= 0x11D;
}

export function gfMul(a, b) {
  if (a === 0 || b === 0) return 0;
  return EXP_TABLE[LOG_TABLE[a] + LOG_TABLE[b]];
}

export function gfDiv(a, b) {
  if (b === 0) throw new Error('Division by zero in GF(2^8)');
  if (a === 0) return 0;
  return EXP_TABLE[(LOG_TABLE[a] + 255 - LOG_TABLE[b]) % 255];
}

export function gfInv(a) {
  if (a === 0) throw new Error('Inversion of zero in GF(2^8)');
  return EXP_TABLE[255 - LOG_TABLE[a]];
}

/**
 * Inverts a K x K matrix over GF(2^8) using Gaussian Elimination.
 * Returns null if the matrix is singular.
 */
export function invertMatrix(matrix, k) {
  const m = matrix.map(row => [...row]);
  const res = Array.from({ length: k }, (_, i) => {
    const row = new Array(k).fill(0);
    row[i] = 1;
    return row;
  });

  for (let c = 0; c < k; c++) {
    // Find pivot
    let pivot = -1;
    for (let r = c; r < k; r++) {
      if (m[r][c] !== 0) {
        pivot = r;
        break;
      }
    }
    if (pivot === -1) return null; // Singular matrix

    if (pivot !== c) {
      [m[c], m[pivot]] = [m[pivot], m[c]];
      [res[c], res[pivot]] = [res[pivot], res[c]];
    }

    const pivotVal = m[c][c];
    const pivotInv = gfInv(pivotVal);

    for (let j = 0; j < k; j++) {
      m[c][j] = gfMul(m[c][j], pivotInv);
      res[c][j] = gfMul(res[c][j], pivotInv);
    }

    for (let r = 0; r < k; r++) {
      if (r !== c && m[r][c] !== 0) {
        const factor = m[r][c];
        for (let j = 0; j < k; j++) {
          m[r][j] ^= gfMul(factor, m[c][j]);
          res[r][j] ^= gfMul(factor, res[c][j]);
        }
      }
    }
  }

  return res;
}

/**
 * Builds systematic Cauchy-based Generator Matrix for K data shards and M parity shards.
 * Dimensions: (K + M) x K.
 * The top K x K is the identity matrix I_k (systematic property).
 */
export function buildGeneratorMatrix(k, m) {
  const gen = [];
  // Identity matrix for data shards
  for (let i = 0; i < k; i++) {
    const row = new Array(k).fill(0);
    row[i] = 1;
    gen.push(row);
  }
  // Cauchy matrix for parity shards: 1 / (i ^ (m + j))
  for (let i = 0; i < m; i++) {
    const row = [];
    for (let j = 0; j < k; j++) {
      row.push(gfInv(i ^ (m + j)));
    }
    gen.push(row);
  }
  return gen;
}

/**
 * Encodes a byte Buffer into K data shards and M parity shards.
 * Returns: { shards: Uint8Array[], shardSize: number, originalLength: number, k, m }
 */
export function encodeErasure(dataBuffer, k, m) {
  const originalLength = dataBuffer.length;
  const shardSize = Math.max(1, Math.ceil(originalLength / k));

  // Split into K data shards (zero-padded)
  const dataShards = [];
  for (let i = 0; i < k; i++) {
    const shard = new Uint8Array(shardSize);
    const start = i * shardSize;
    const end = Math.min(start + shardSize, originalLength);
    if (start < originalLength) {
      shard.set(dataBuffer.subarray(start, end));
    }
    dataShards.push(shard);
  }

  const gen = buildGeneratorMatrix(k, m);
  const allShards = [...dataShards];

  // Compute M parity shards
  for (let p = 0; p < m; p++) {
    const parity = new Uint8Array(shardSize);
    const matrixRow = gen[k + p];
    for (let d = 0; d < k; d++) {
      const coeff = matrixRow[d];
      const dShard = dataShards[d];
      for (let b = 0; b < shardSize; b++) {
        parity[b] ^= gfMul(coeff, dShard[b]);
      }
    }
    allShards.push(parity);
  }

  return {
    shards: allShards, // total K + M shards
    shardSize,
    originalLength,
    k,
    m,
  };
}

/**
 * Decodes original data buffer from any K surviving shards.
 * survivingShards: array of { index: number (0 <= index < K+M), data: Uint8Array }
 */
export function decodeErasure(survivingShards, k, m, shardSize, originalLength) {
  if (survivingShards.length < k) {
    throw new Error(`Insufficient shards to decode: have ${survivingShards.length}, required ${k}`);
  }

  // Fast path: if the first K shards (data shards 0..k-1) are all present
  const availableIndices = new Map(survivingShards.map(s => [s.index, s.data]));
  let hasAllDataShards = true;
  for (let i = 0; i < k; i++) {
    if (!availableIndices.has(i)) {
      hasAllDataShards = false;
      break;
    }
  }

  if (hasAllDataShards) {
    const total = new Uint8Array(shardSize * k);
    for (let d = 0; d < k; d++) {
      total.set(availableIndices.get(d), d * shardSize);
    }
    return Buffer.from(total.subarray(0, originalLength));
  }

  // Slow path: Gaussian elimination to invert submatrix
  const used = survivingShards.slice(0, k);
  const gen = buildGeneratorMatrix(k, m);
  const subMatrix = used.map(s => gen[s.index]);
  const inverted = invertMatrix(subMatrix, k);

  if (!inverted) {
    throw new Error('Singular matrix encountered during erasure decoding');
  }

  const reconstructedDataShards = [];
  for (let d = 0; d < k; d++) {
    const outShard = new Uint8Array(shardSize);
    const row = inverted[d];
    for (let s = 0; s < k; s++) {
      const coeff = row[s];
      const shardData = used[s].data;
      for (let b = 0; b < shardSize; b++) {
        outShard[b] ^= gfMul(coeff, shardData[b]);
      }
    }
    reconstructedDataShards.push(outShard);
  }

  const total = new Uint8Array(shardSize * k);
  for (let d = 0; d < k; d++) {
    total.set(reconstructedDataShards[d], d * shardSize);
  }

  return Buffer.from(total.subarray(0, originalLength));
}

/**
 * Reconstructs specific missing shard indices from surviving shards.
 * Used by the background healing daemon and read-repair to restore degraded shards!
 */
export function reconstructMissingShards(survivingShards, missingIndices, k, m, shardSize, originalLength) {
  // First decode full data
  const decodedData = decodeErasure(survivingShards, k, m, shardSize, originalLength);
  // Re-encode to produce all K+M shards
  const { shards: fullShards } = encodeErasure(decodedData, k, m);

  const restored = {};
  for (const idx of missingIndices) {
    if (idx >= 0 && idx < k + m) {
      restored[idx] = fullShards[idx];
    }
  }
  return restored;
}
