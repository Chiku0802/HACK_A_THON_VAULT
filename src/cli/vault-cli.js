#!/usr/bin/env node

/**
 * Vault Object Storage - Command Line Interface (CLI)
 */

import fs from 'node:fs/promises';
import path from 'node:path';

const GATEWAY_URL = process.env.VAULT_GATEWAY_URL || 'http://127.0.0.1:8080';

async function main() {
  const args = process.argv.slice(2);
  const command = args[0];

  if (!command || command === 'help' || command === '--help' || command === '-h') {
    printHelp();
    return;
  }

  try {
    switch (command) {
      case 'status':
      case 'cluster':
        await handleClusterStatus();
        break;

      case 'bucket':
        await handleBucket(args.slice(1));
        break;

      case 'put':
        await handlePut(args.slice(1));
        break;

      case 'get':
        await handleGet(args.slice(1));
        break;

      case 'head':
        await handleHead(args.slice(1));
        break;

      case 'delete':
      case 'rm':
        await handleDelete(args.slice(1));
        break;

      case 'list':
      case 'ls':
        await handleList(args.slice(1));
        break;

      case 'chaos':
        await handleChaos(args.slice(1));
        break;

      case 'scrub':
        await handleScrub();
        break;

      case 'heal':
        await handleHeal();
        break;

      default:
        console.error(`Unknown command: ${command}`);
        printHelp();
        process.exit(1);
    }
  } catch (err) {
    console.error(`Error: ${err.message}`);
    process.exit(1);
  }
}

function printHelp() {
  console.log(`
Vault Object Storage CLI

USAGE:
  vault <command> [arguments] [options]

COMMANDS:
  status, cluster                  Show cluster status, nodes health, and storage utilization
  bucket create <name> [--policy]  Create a new bucket (REPLICATION or ERASURE)
  put <bucket> <key> <file>        Store an object into Vault
  get <bucket> <key> [destFile]    Retrieve and verify an object from Vault
  head <bucket> <key>              Inspect object metadata
  delete <bucket> <key>            Delete an object (writes tombstone)
  list <bucket> [--prefix <str>]   List objects in a bucket
  scrub                            Run cluster-wide disk audit for silent bitrot
  heal                             Trigger immediate prioritized repair cycle
  chaos kill <nodeId>              Simulate sudden hardware death of a node
  chaos revive <nodeId>            Revive a node and drain hinted handoffs
  chaos corrupt <nodeId> <chunkId> Inject bitrot onto disk file
  chaos heal                       Heal all network partitions and revived nodes

OPTIONS:
  --policy <REPLICATION|ERASURE>   Specify durability policy (default: REPLICATION)
  --gateway <url>                  Vault gateway URL (default: http://127.0.0.1:8080)
`);
}

async function handleClusterStatus() {
  const res = await fetch(`${GATEWAY_URL}/api/cluster/status`);
  if (!res.ok) throw new Error(`Gateway returned HTTP ${res.status}`);
  const data = await res.json();

  console.log(`\n================== VAULT CLUSTER STATUS ==================`);
  console.log(`Gateway: ${GATEWAY_URL}`);
  console.log(`Total Nodes: ${data.nodes.length} | Active Objects: ${data.objects.length}`);
  console.log(`Read Repairs: ${data.readRepairCount} | Healed Chunks: ${data.healing.totalHealedChunks}`);
  console.log(`----------------------------------------------------------`);
  console.log(`NODES:`);
  for (const n of data.nodes) {
    const statusColor = n.status === 'ALIVE' ? '\x1b[32m' : '\x1b[31m';
    const used = (n.diskUsage?.usedBytes || 0) / 1024;
    console.log(`  • [${n.nodeId}] ${statusColor}${n.status}\x1b[0m Rack: ${n.rack} | Port: ${n.port} | Stored: ${n.diskUsage?.chunkCount || 0} chunks (${used.toFixed(1)} KB)`);
  }
  console.log(`==========================================================\n`);
}

async function handleBucket(args) {
  const sub = args[0];
  const name = args[1];
  if (sub !== 'create' || !name) {
    console.error('Usage: vault bucket create <name> [--policy REPLICATION|ERASURE]');
    return;
  }
  const policyIdx = args.indexOf('--policy');
  const policy = policyIdx !== -1 ? args[policyIdx + 1] : 'REPLICATION';

  const res = await fetch(`${GATEWAY_URL}/${encodeURIComponent(name)}`, {
    method: 'PUT',
    headers: { 'X-Vault-Storage-Policy': policy },
  });
  if (!res.ok) throw new Error(await res.text());
  console.log(`✓ Bucket '${name}' created with default policy: ${policy}`);
}

async function handlePut(args) {
  const bucket = args[0];
  const key = args[1];
  const filePath = args[2];
  if (!bucket || !key || !filePath) {
    console.error('Usage: vault put <bucket> <key> <filePath> [--policy REPLICATION|ERASURE]');
    return;
  }

  const policyIdx = args.indexOf('--policy');
  const policy = policyIdx !== -1 ? args[policyIdx + 1] : 'REPLICATION';

  const fileData = await fs.readFile(path.resolve(filePath));

  console.log(`Uploading ${filePath} (${fileData.length} bytes) to ${bucket}/${key} with policy: ${policy}...`);
  const res = await fetch(`${GATEWAY_URL}/${encodeURIComponent(bucket)}/${encodeURIComponent(key)}`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/octet-stream',
      'X-Vault-Storage-Policy': policy,
    },
    body: fileData,
  });

  if (!res.ok) throw new Error(await res.text());
  const data = await res.json();
  console.log(`✓ Successfully stored '${key}'!`);
  console.log(`  Version: ${data.versionId}`);
  console.log(`  ETag:    ${data.etag}`);
  console.log(`  Chunks:  ${data.chunks.length}`);
}

async function handleGet(args) {
  const bucket = args[0];
  const key = args[1];
  const destPath = args[2];
  if (!bucket || !key) {
    console.error('Usage: vault get <bucket> <key> [destFile]');
    return;
  }

  console.log(`Retrieving ${bucket}/${key} with quorum verification...`);
  const res = await fetch(`${GATEWAY_URL}/${encodeURIComponent(bucket)}/${encodeURIComponent(key)}`);
  if (!res.ok) throw new Error(await res.text());

  const data = Buffer.from(await res.arrayBuffer());
  const etag = res.headers.get('etag');

  if (destPath) {
    await fs.writeFile(path.resolve(destPath), data);
    console.log(`✓ Saved ${data.length} bytes to ${destPath} (ETag: ${etag})`);
  } else {
    // Print preview
    console.log(`✓ Retrieved ${data.length} bytes (ETag: ${etag})`);
    if (data.length <= 1024) {
      console.log(`\nContent:\n${data.toString('utf8')}`);
    }
  }
}

async function handleHead(args) {
  const bucket = args[0];
  const key = args[1];
  if (!bucket || !key) {
    console.error('Usage: vault head <bucket> <key>');
    return;
  }

  const res = await fetch(`${GATEWAY_URL}/${encodeURIComponent(bucket)}/${encodeURIComponent(key)}`, {
    method: 'HEAD',
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);

  console.log(`Object: ${bucket}/${key}`);
  console.log(`  Size:           ${res.headers.get('content-length')} bytes`);
  console.log(`  ETag:           ${res.headers.get('etag')}`);
  console.log(`  Policy:         ${res.headers.get('x-vault-storage-policy')}`);
  console.log(`  Version:        ${res.headers.get('x-vault-version-id')}`);
  console.log(`  Last-Modified:  ${res.headers.get('last-modified')}`);
}

async function handleDelete(args) {
  const bucket = args[0];
  const key = args[1];
  if (!bucket || !key) {
    console.error('Usage: vault delete <bucket> <key>');
    return;
  }

  const res = await fetch(`${GATEWAY_URL}/${encodeURIComponent(bucket)}/${encodeURIComponent(key)}`, {
    method: 'DELETE',
  });
  if (!res.ok) throw new Error(await res.text());
  console.log(`✓ Deleted '${bucket}/${key}' (tombstone recorded)`);
}

async function handleList(args) {
  const bucket = args[0] || 'default';
  const prefixIdx = args.indexOf('--prefix');
  const prefix = prefixIdx !== -1 ? args[prefixIdx + 1] : '';

  const res = await fetch(`${GATEWAY_URL}/${encodeURIComponent(bucket)}?prefix=${encodeURIComponent(prefix)}`);
  if (!res.ok) throw new Error(await res.text());
  const data = await res.json();

  console.log(`Objects in '${bucket}' (count: ${data.objects.length}):`);
  for (const obj of data.objects) {
    console.log(`  • ${obj.key.padEnd(30)} ${String(obj.size).padStart(8)} B   [${obj.policy}]   ETag: ${obj.etag.substring(0, 12)}...`);
  }
}

async function handleChaos(args) {
  const sub = args[0];
  if (sub === 'kill') {
    const nodeId = args[1];
    const res = await fetch(`${GATEWAY_URL}/api/chaos/kill-node`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ nodeId }),
    });
    console.log(await res.json());
  } else if (sub === 'revive') {
    const nodeId = args[1];
    const res = await fetch(`${GATEWAY_URL}/api/chaos/revive-node`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ nodeId }),
    });
    console.log(await res.json());
  } else if (sub === 'heal') {
    const res = await fetch(`${GATEWAY_URL}/api/chaos/heal`, { method: 'POST' });
    console.log(await res.json());
  } else {
    console.log('Chaos subcommands: kill <nodeId>, revive <nodeId>, heal');
  }
}

async function handleScrub() {
  console.log('Triggering cluster-wide disk scrub...');
  const res = await fetch(`${GATEWAY_URL}/api/actions/scrub`, { method: 'POST' });
  const data = await res.json();
  console.log(`✓ Scrub complete! Scanned ${data.nodesScanned} nodes.`);
}

async function handleHeal() {
  console.log('Triggering prioritized healing cycle...');
  const res = await fetch(`${GATEWAY_URL}/api/actions/heal`, { method: 'POST' });
  const data = await res.json();
  console.log(`✓ Healing complete: ${data.totalHealedChunks} chunks healed.`);
}

main();
