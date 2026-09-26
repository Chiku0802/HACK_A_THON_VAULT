<<<<<<< HEAD
=======
>>>>>>> origin/main

# 🛡️ Vault: Fault-Tolerant Distributed Object Storage System

**Vault** is an industrial-grade, fault-tolerant distributed object storage system engineered to reliably store, replicate, retrieve, and automatically repair large volumes of data across unreliable, independently failing storage nodes and degraded networks.

---

## 🌟 Key Features & Capabilities

- **High-Concurrency Reads & Writes**: Non-blocking streaming I/O, atomic commit with optimistic concurrency control, monotonic object versioning (MVCC), and chunked content addressing.
- **Configurable Durability & Erasure Policies**:
  - **Full Replication**: Configurable $N$ replicas with strict write quorum ($W$) and read quorum ($R$), guaranteeing strong read consistency when $W + R > N$.
  - **Reed-Solomon Erasure Coding ($K+M$)**: Pure Galois Field $GF(2^8)$ Cauchy matrix codec. Survives the loss of any $M$ arbitrary shards with only $M/K$ storage overhead (e.g., 4+2 EC gives 50% overhead compared to 200% for 3x replication).
- **Topology & Failure-Domain Awareness**: Deterministic rendezvous hashing placement that distributes chunks across distinct racks and failure domains (`rack-1`, `rack-2`, `rack-3`), preventing correlated infrastructure failures from causing data loss.
- **Node Failures & Partition Tolerance**:
  - Heartbeat-based failure detection with `ALIVE`, `SUSPECT`, and `DEAD` states.
  - **Sloppy Quorum & Hinted Handoff**: Buffers writes intended for partitioned or offline nodes onto healthy surrogate nodes and automatically drains/replays them upon node recovery.
  - **Split-Brain Prevention**: Enforces majority quorum boundaries, rejecting writes on isolated minority partitions.
- **Bitrot Detection & Automatic Self-Healing**:
  - **Cryptographic Chunk Envelopes**: Structured binary headers with payload SHA-256 and header CRC32 validation on every read.
  - **Transparent Read Repair**: Detects bitrot or stale versions during quorum reads, serves clean canonical data to the client with zero latency penalty, and asynchronously repairs the damaged node in the background.
  - **Background Disk Scrubber**: Periodically audits disk media at rest, flagging silent bitrot before multi-node degradation occurs.
  - **Merkle Tree Anti-Entropy**: Hierarchical Merkle tree exchange reconciles diverging key spaces in $O(k \log N)$ network overhead.
  - **Prioritized Autonomous Healing**: Replaces and reconstructs lost replicas and missing erasure shards with token-bucket rate limiting ($10\text{ MB/s}$) to preserve user I/O throughput.
- **S3-Compatible REST API**:
  - `PUT /<bucket>/<key>` (Object upload with headers for storage policy)
  - `GET /<bucket>/<key>` (Object download with `Range: bytes=start-end` support)
  - `HEAD /<bucket>/<key>` (Metadata inspection)
  - `DELETE /<bucket>/<key>` (Soft deletion with durable WAL tombstones)
  - `POST /<bucket>/<key>?uploads` (Multipart upload lifecycle: initiate, upload parts, complete, abort)
- **Interactive Visual Dashboard & Chaos Studio**: Real-time cluster topology map, rack gauges, interactive failure injection (Kill Node, Split Network, Inject Bitrot, Trigger Scrub/Heal), object browser, and live SSE telemetry feed.
- **Multi-Language Interfaces**: Native CLI tool (`vault`), HTTP REST API, and zero-dependency Python Client SDK (`VaultClient`).

---

## 🏗️ Architecture

```
                                  ┌────────────────────────┐
                                  │   Clients / Web UI     │
                                  └───────────┬────────────┘
                                              │ HTTP / S3 API
                                              ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   VAULT STORAGE GATEWAY                                     │
│  ┌───────────────────────┐  ┌─────────────────────────┐  ┌────────────────────────────────┐ │
│  │ S3 REST Router / Quorum│  │ Multipart Session Stager│  │ Read Repair & Range Streamer   │ │
│  └───────────┬───────────┘  └───────────┬─────────────┘  └────────────────┬───────────────┘ │
└──────────────┼──────────────────────────┼─────────────────────────────────┼─────────────────┘
               │                          │                                 │
               ▼                          ▼                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  COORDINATION SUBSYSTEM                                     │
│  ┌───────────────────────┐  ┌─────────────────────────┐  ┌────────────────────────────────┐ │
│  │ Metadata Store & WAL  │  │ Rack-Aware Topology Ring│  │ Failure Detector & Heartbeats  │ │
│  ├───────────────────────┤  ├─────────────────────────┤  ├────────────────────────────────┤ │
│  │ Prioritized Healer    │  │ Background Scrubber     │  │ Merkle Tree Anti-Entropy Sync  │ │
│  └───────────────────────┘  └─────────────────────────┘  └────────────────────────────────┘ │
└──────────────────────────────────────────────┬──────────────────────────────────────────────┘
                                               │
               ┌───────────────────────────────┼───────────────────────────────┐
               ▼                               ▼                               ▼
       ┌───────────────┐               ┌───────────────┐               ┌───────────────┐
       │    Rack 1     │               │    Rack 2     │               │    Rack 3     │
       │ ┌───────────┐ │               │ ┌───────────┐ │               │ ┌───────────┐ │
       │ │  Node 1   │ │               │ │  Node 3   │ │               │ │  Node 5   │ │
       │ └───────────┘ │               │ └───────────┘ │               │ └───────────┘ │
       │ ┌───────────┐ │               │ ┌───────────┐ │               └───────────────┘
       │ │  Node 2   │ │               │ │  Node 4   │ │
       │ └───────────┘ │               │ └───────────┘ │
       └───────────────┘               └───────────────┘
```

---

## 📊 Durability Policy Comparison

| Property | Replication ($N=3$) | Reed-Solomon Erasure Coding ($4+2$) |
| :--- | :--- | :--- |
| **Durability Guarantee** | Survives 2 node failures | Survives 2 node failures |
| **Storage Overhead** | $3.0\times$ ($200\%$ overhead) | $1.5\times$ ($50\%$ overhead) |
| **Write Quorum ($W$)** | Majority ($W=2$) | $K + \lceil M/2 \rceil$ shards ($5$ nodes) |
| **Read Quorum ($R$)** | Majority ($R=2$) | Any $K=4$ surviving shards |
| **Fault Recovery** | Copies chunk from healthy peer | Reconstructs missing shards via $GF(2^8)$ matrix inversion |
| **Ideal For** | High-churn metadata, small files | Large blobs, archives, video, backups |

---

## 🚀 Quickstart Guide

### 1. Start Cluster
Start the 5-node distributed cluster, coordinator, and REST gateway:
```bash
npm start
```
Output:
```
[INFO ] [Cluster] Bootstrapping Vault Distributed Storage
[INFO ] [Node:node-1] Storage Node online at http://127.0.0.1:9001 [Rack: rack-1]
[INFO ] [Node:node-2] Storage Node online at http://127.0.0.1:9002 [Rack: rack-1]
[INFO ] [Node:node-3] Storage Node online at http://127.0.0.1:9003 [Rack: rack-2]
[INFO ] [Node:node-4] Storage Node online at http://127.0.0.1:9004 [Rack: rack-2]
[INFO ] [Node:node-5] Storage Node online at http://127.0.0.1:9005 [Rack: rack-3]
[INFO ] [Gateway] Vault Gateway listening at http://0.0.0.0:8080
[INFO ] [Cluster] ✨ Vault Cluster online with 5 nodes across 3 racks!
👉 Web Dashboard: http://localhost:8080/dashboard
👉 S3 REST API:   http://localhost:8080/<bucket>/<key>
```

### 2. Interactive Web Dashboard
Open your browser to:
```
http://localhost:8080/dashboard
```
Features:
- Live node health and rack topology visualization.
- Interactive **Chaos Engineering Studio** (Kill Node, Revive Node, Inject Bitrot, Network Partition, Trigger Disk Scrub, Force Healing Cycle).
- Drag-and-drop object upload and live chunk placement inspector.
- Real-time Server-Sent Events (SSE) telemetry feed.

---

## 💻 CLI Usage

The built-in CLI (`vault`) provides administrative and storage commands:

```bash
# Check cluster health, nodes, and capacity
node src/cli/vault-cli.js status

# Create a bucket with Reed-Solomon Erasure Coding
node src/cli/vault-cli.js bucket create my-bucket --policy ERASURE

# Store an object
node src/cli/vault-cli.js put my-bucket dataset.csv ./dataset.csv --policy ERASURE

# Retrieve an object with quorum verification
node src/cli/vault-cli.js get my-bucket dataset.csv ./retrieved.csv

# Inspect object metadata
node src/cli/vault-cli.js head my-bucket dataset.csv

# List objects in bucket
node src/cli/vault-cli.js list my-bucket

# Delete object (durable tombstone)
node src/cli/vault-cli.js delete my-bucket dataset.csv

# Chaos: simulate hardware crash of node-2
node src/cli/vault-cli.js chaos kill node-2

# Chaos: revive node-2 and drain hinted handoffs
node src/cli/vault-cli.js chaos revive node-2

# Trigger cluster-wide disk scrub for silent bitrot
node src/cli/vault-cli.js scrub

# Trigger immediate prioritized healing cycle
node src/cli/vault-cli.js heal
```

---

## 🐍 Python Client SDK (`VaultClient`)

A zero-dependency Python client library is included in `vault_client.py`:

```python
from vault_client import VaultClient

client = VaultClient("http://127.0.0.1:8080")

# Create a bucket
client.create_bucket("ml-models", policy="ERASURE")

# Store an object (4+2 Erasure Coding)
client.put_object("ml-models", "weights.bin", b"binary model data...", policy="ERASURE")

# Retrieve object (verifies quorum & heals bitrot automatically)
data = client.get_object("ml-models", "weights.bin")

# Range read (partial slice)
partial = client.get_object("ml-models", "weights.bin", byte_range="0-1024")

# Multipart upload
upload_id = client.init_multipart("ml-models", "large-dataset.tar.gz")
client.upload_part("ml-models", "large-dataset.tar.gz", upload_id, 1, part1_bytes)
client.upload_part("ml-models", "large-dataset.tar.gz", upload_id, 2, part2_bytes)
client.complete_multipart("ml-models", "large-dataset.tar.gz", upload_id)

# Chaos & Admin
client.chaos_kill_node("node-3")
status = client.get_cluster_status()
print(f"Alive nodes: {len([n for n in status['nodes'] if n['status'] == 'ALIVE'])}")
client.trigger_heal()
client.chaos_revive_node("node-3")
```

---

## 🧪 Comprehensive Test Suite

All test suites run in isolated ephemeral clusters with zero cross-test interference:

| Test File | Description | Status |
| :--- | :--- | :--- |
| `tests/test-erasure.js` | GF($2^8$) Galois Field arithmetic, Cauchy matrix inversion, 4+2 and 2+1 encoding, 2-node loss recovery, and shard reconstruction | `PASSED` |
| `tests/test-concurrency.js` | 20 concurrent writes, 50 concurrent quorum reads, and rapid atomic versioning updates (MVCC) | `PASSED` |
| `tests/test-failures.js` | Node crash, quorum read continuity ($R=2$), sloppy quorum with hinted handoff, node recovery, and auto-healing degraded replicas onto new nodes | `PASSED` |
| `tests/test-bitrot-repair.js` | On-disk bitrot injection, SHA-256 detection, transparent Read Repair, and background disk scrubber | `PASSED` |
| `tests/test-partitions.js` | Asymmetric network partition, majority write success, minority quorum rejection (503 split-brain prevention), and partition healing | `PASSED` |
| `tests/test-anti-entropy.js` | Chunk deletion, Merkle tree root divergence, $O(\log N)$ diff discovery, and peer-to-peer chunk reconciliation | `PASSED` |
| `tests/test-large-objects.js` | Multi-chunk objects, HTTP `Range:` byte slicing, S3 multipart uploads (out-of-order parts), and erasure coding reconstruction under active node death | `PASSED` |
| `tests/test-python-sdk.py` | Full Python Client SDK validation against live cluster | `PASSED` |

Run all tests:
```bash
node tests/test-erasure.js
node tests/test-concurrency.js
node tests/test-failures.js
node tests/test-bitrot-repair.js
node tests/test-partitions.js
node tests/test-anti-entropy.js
node tests/test-large-objects.js
python tests/test-python-sdk.py
```

---

## ⚡ Performance Benchmarks

Run the benchmark suite:
```bash
npm run benchmark
```

Benchmark results on local node cluster:
- **3x Replication Write**: `~6.61 MB/s` ($p50: 8.8\text{ ms}$, $p99: 42.2\text{ ms}$)
- **3x Replication Read**: `~14.47 MB/s` ($p50: 3.6\text{ ms}$, $p99: 20.2\text{ ms}$)
- **Erasure Coding (2+2) Write**: `~6.02 MB/s` ($p50: 10.0\text{ ms}$, $p99: 20.2\text{ ms}$)
- **Recovery Time Objective (RTO)**: **`825.3 ms`** to detect a node crash, re-plan placement, and reconstruct 89 degraded chunks across surviving nodes with 100% availability!

---

## 📄 License

Apache License 2.0. Built with pride for high-scale, resilient distributed storage.
=======
# Vault_Prompta_thon
Vault: Build a fault-tolerant distributed object storage system capable of storing, replicating, retrieving, and repairing large volumes of data across unreliable and independently failing storage nodes. 
>>>>>>> a10d09c14b75664c19d1c060a6694515b542842d
=======
# HACK_A_THON_VAULT
1st repo
>>>>>>> 1150efc17dce41a282c14ff106a18c3d047805e7
