/**
 * Unit Test: Reed-Solomon Erasure Coding Engine
 */

import assert from 'node:assert';
import { encodeErasure, decodeErasure, reconstructMissingShards } from '../src/common/erasure.js';

console.log('--- Testing Reed-Solomon Erasure Coding ---');

// Test Case 1: Simple text with 4 data + 2 parity shards
{
  const data = Buffer.from('Vault: Fault-Tolerant Distributed Object Storage with Reed Solomon Erasure Coding!');
  const k = 4, m = 2;
  const encoded = encodeErasure(data, k, m);

  assert.strictEqual(encoded.shards.length, 6, 'Should generate 6 shards (4 data + 2 parity)');
  assert.strictEqual(encoded.originalLength, data.length);

  // Scenario 1: No loss (all data shards present)
  const allShards = encoded.shards.map((data, index) => ({ index, data }));
  const decodedAll = decodeErasure(allShards, k, m, encoded.shardSize, encoded.originalLength);
  assert.strictEqual(decodedAll.toString(), data.toString(), 'Decoded without loss should match exactly');

  // Scenario 2: Loss of 2 data shards (shard 0 and shard 1 lost)
  // Surviving: shard 2, shard 3, shard 4 (parity 0), shard 5 (parity 1)
  const survivingLoss2Data = [
    { index: 2, data: encoded.shards[2] },
    { index: 3, data: encoded.shards[3] },
    { index: 4, data: encoded.shards[4] },
    { index: 5, data: encoded.shards[5] },
  ];
  const decodedLoss2Data = decodeErasure(survivingLoss2Data, k, m, encoded.shardSize, encoded.originalLength);
  assert.strictEqual(decodedLoss2Data.toString(), data.toString(), 'Decoded after losing 2 data shards must match exactly');

  // Scenario 3: Loss of 1 data shard and 1 parity shard (shard 1 and shard 4 lost)
  const survivingMixed = [
    { index: 0, data: encoded.shards[0] },
    { index: 2, data: encoded.shards[2] },
    { index: 3, data: encoded.shards[3] },
    { index: 5, data: encoded.shards[5] },
  ];
  const decodedMixed = decodeErasure(survivingMixed, k, m, encoded.shardSize, encoded.originalLength);
  assert.strictEqual(decodedMixed.toString(), data.toString(), 'Decoded after losing 1 data + 1 parity must match exactly');

  // Scenario 4: Reconstructing missing shards for healing
  const reconstructed = reconstructMissingShards(survivingLoss2Data, [0, 1], k, m, encoded.shardSize, encoded.originalLength);
  assert(Buffer.from(reconstructed[0]).equals(Buffer.from(encoded.shards[0])), 'Reconstructed shard 0 matches original');
  assert(Buffer.from(reconstructed[1]).equals(Buffer.from(encoded.shards[1])), 'Reconstructed shard 1 matches original');

  console.log('✓ Test Case 1 passed: 4+2 EC encodes, decodes with 2 losses, and reconstructs');
}

// Test Case 2: Binary data and odd lengths (2+1 EC)
{
  const randomBytes = Buffer.alloc(333);
  for (let i = 0; i < randomBytes.length; i++) randomBytes[i] = (i * 37 + 13) & 0xFF;

  const k = 2, m = 1;
  const encoded = encodeErasure(randomBytes, k, m);

  // Lose shard 0 (data), survive: shard 1 (data) and shard 2 (parity)
  const surviving = [
    { index: 1, data: encoded.shards[1] },
    { index: 2, data: encoded.shards[2] },
  ];
  const decoded = decodeErasure(surviving, k, m, encoded.shardSize, encoded.originalLength);
  assert(decoded.equals(randomBytes), 'Decoded binary buffer matches original binary data');

  console.log('✓ Test Case 2 passed: 2+1 EC handles arbitrary binary buffer');
}

// Test Case 3: Insufficient shards (< K) should throw error
{
  const data = Buffer.from('hello world');
  const encoded = encodeErasure(data, 4, 2);
  const tooFew = [{ index: 0, data: encoded.shards[0] }];
  assert.throws(() => {
    decodeErasure(tooFew, 4, 2, encoded.shardSize, encoded.originalLength);
  }, /Insufficient shards/);

  console.log('✓ Test Case 3 passed: Insufficient shards fails safely');
}

console.log('All Reed-Solomon Erasure Coding tests passed successfully!\n');
