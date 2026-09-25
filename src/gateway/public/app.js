/**
 * Vault Object Storage - Web Dashboard Frontend Logic
 */

let currentClusterData = null;

async function fetchClusterStatus() {
  try {
    const res = await fetch('/api/cluster/status');
    if (!res.ok) return;
    const data = await res.json();
    currentClusterData = data;
    renderCluster(data);
  } catch (err) {
    console.error('Error fetching cluster status:', err);
  }
}

function renderCluster(data) {
  // Update header stats
  const totalNodes = data.nodes.length;
  const aliveNodes = data.nodes.filter(n => n.status === 'ALIVE').length;
  document.getElementById('stat-nodes').textContent = `${aliveNodes}/${totalNodes}`;
  document.getElementById('stat-objects').textContent = data.objects.length;
  document.getElementById('stat-repairs').textContent = data.healing.totalHealedChunks + data.readRepairCount;

  const clusterBadge = document.getElementById('stat-cluster-badge');
  if (aliveNodes === totalNodes) {
    clusterBadge.textContent = 'OPTIMAL';
    clusterBadge.className = 'badge badge-alive';
  } else if (aliveNodes >= 2) {
    clusterBadge.textContent = 'DEGRADED';
    clusterBadge.className = 'badge badge-suspect';
  } else {
    clusterBadge.textContent = 'CRITICAL';
    clusterBadge.className = 'badge badge-dead';
  }

  // Group nodes by rack
  const racksMap = new Map();
  for (const node of data.nodes) {
    const rackId = node.rack || 'rack-default';
    if (!racksMap.has(rackId)) racksMap.set(rackId, []);
    racksMap.get(rackId).push(node);
  }

  const racksContainer = document.getElementById('racks-container');
  racksContainer.innerHTML = '';

  for (const [rackId, nodes] of racksMap.entries()) {
    const rackDiv = document.createElement('div');
    rackDiv.className = 'rack-box';

    const header = document.createElement('div');
    header.className = 'rack-header';
    header.textContent = `📁 ${rackId} (${nodes.length} nodes)`;
    rackDiv.appendChild(header);

    const nodesGrid = document.createElement('div');
    nodesGrid.className = 'nodes-grid';

    for (const node of nodes) {
      const card = document.createElement('div');
      card.className = `node-card ${node.status.toLowerCase()}`;

      const top = document.createElement('div');
      top.className = 'node-top';
      top.innerHTML = `
        <span class="node-id">${node.nodeId}</span>
        <span class="badge badge-${node.status.toLowerCase()}">${node.status}</span>
      `;
      card.appendChild(top);

      const stats = document.createElement('div');
      stats.className = 'node-stat';
      const chunks = node.diskUsage?.chunkCount || 0;
      const bytes = formatBytes(node.diskUsage?.usedBytes || 0);
      stats.innerHTML = `
        <div>Port: ${node.port}</div>
        <div>Stored: <strong>${chunks}</strong> chunks (${bytes})</div>
      `;
      card.appendChild(stats);

      const actions = document.createElement('div');
      actions.className = 'node-actions';

      if (node.status === 'ALIVE') {
        const killBtn = document.createElement('button');
        killBtn.className = 'btn btn-xs btn-danger';
        killBtn.textContent = 'Kill Node';
        killBtn.onclick = () => killNode(node.nodeId);
        actions.appendChild(killBtn);

        const corruptBtn = document.createElement('button');
        corruptBtn.className = 'btn btn-xs btn-warning';
        corruptBtn.textContent = 'Corrupt Bit';
        corruptBtn.onclick = () => corruptNodeRandom(node.nodeId);
        actions.appendChild(corruptBtn);
      } else {
        const reviveBtn = document.createElement('button');
        reviveBtn.className = 'btn btn-xs btn-success';
        reviveBtn.textContent = 'Revive Node';
        reviveBtn.onclick = () => reviveNode(node.nodeId);
        actions.appendChild(reviveBtn);
      }

      card.appendChild(actions);
      nodesGrid.appendChild(card);
    }

    rackDiv.appendChild(nodesGrid);
    racksContainer.appendChild(rackDiv);
  }

  // Render Objects Table
  renderObjectsTable(data.objects);
}

function renderObjectsTable(objects) {
  const tbody = document.getElementById('objects-tbody');
  document.getElementById('objects-count').textContent = `${objects.length} objects`;

  if (objects.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" class="text-center">No objects stored yet. Use form above to upload.</td></tr>';
    return;
  }

  tbody.innerHTML = '';
  for (const obj of objects) {
    const tr = document.createElement('tr');

    const policyBadge = obj.policy === 'ERASURE'
      ? '<span class="badge badge-ec">Reed-Solomon 4+2</span>'
      : '<span class="badge badge-rep">3x Replication</span>';

    const shortEtag = (obj.etag || '').substring(0, 12) + '...';
    const numChunks = (obj.chunks || []).length;

    tr.innerHTML = `
      <td><strong>${escapeHtml(obj.bucket)}</strong>/${escapeHtml(obj.key)}</td>
      <td>${formatBytes(obj.size)}</td>
      <td>${policyBadge}</td>
      <td><code>${shortEtag}</code></td>
      <td><span class="badge badge-alive">${numChunks} chunk(s)</span></td>
      <td>
        <button class="btn btn-xs btn-outline" onclick="downloadObject('${encodeURIComponent(obj.bucket)}', '${encodeURIComponent(obj.key)}')">⬇️ Get</button>
        <button class="btn btn-xs btn-secondary" onclick="inspectObject('${encodeURIComponent(obj.bucket)}', '${encodeURIComponent(obj.key)}')">🔍 Shards</button>
        <button class="btn btn-xs btn-danger" onclick="deleteObject('${encodeURIComponent(obj.bucket)}', '${encodeURIComponent(obj.key)}')">🗑️</button>
      </td>
    `;
    tbody.appendChild(tr);
  }
}

// Log message helper
function appendLog(msg, type = 'info') {
  const logDiv = document.getElementById('events-log');
  const entry = document.createElement('div');
  entry.className = `log-entry log-${type}`;
  const ts = new Date().toLocaleTimeString();
  entry.innerHTML = `<span class="log-ts">[${ts}]</span> ${escapeHtml(msg)}`;
  logDiv.appendChild(entry);
  logDiv.scrollTop = logDiv.scrollHeight;
}

// SSE Events Connection
function initEventSource() {
  const evt = new EventSource('/api/events');
  evt.addEventListener('node_killed', (e) => {
    const data = JSON.parse(e.data);
    appendLog(`🚨 Node ${data.nodeId} KILLED (hardware failure simulated)`, 'error');
    fetchClusterStatus();
  });
  evt.addEventListener('node_revived', (e) => {
    const data = JSON.parse(e.data);
    appendLog(`✅ Node ${data.nodeId} REVIVED. Replayed ${data.replayedHints} hinted writes.`, 'success');
    fetchClusterStatus();
  });
  evt.addEventListener('chunk_corrupted', (e) => {
    const data = JSON.parse(e.data);
    appendLog(`⚠️ Injected bitrot corruption into chunk ${data.chunkId} on node ${data.nodeId}!`, 'warn');
    fetchClusterStatus();
  });
  evt.addEventListener('partition_created', (e) => {
    const data = JSON.parse(e.data);
    appendLog(`✂️ Network Partition created: [${data.groupA}] isolated from [${data.groupB}]`, 'warn');
    fetchClusterStatus();
  });
  evt.addEventListener('network_healed', () => {
    appendLog(`💚 All network partitions healed and links restored!`, 'success');
    fetchClusterStatus();
  });
}

// --- Action Handlers ---

async function killNode(nodeId) {
  appendLog(`Sending KILL command to ${nodeId}...`, 'warn');
  await fetch('/api/chaos/kill-node', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ nodeId }),
  });
  fetchClusterStatus();
}

async function reviveNode(nodeId) {
  appendLog(`Sending REVIVE command to ${nodeId}...`, 'info');
  await fetch('/api/chaos/revive-node', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ nodeId }),
  });
  fetchClusterStatus();
}

async function corruptNodeRandom(nodeId) {
  // Find a chunk on that node
  if (!currentClusterData) return;
  let targetChunkId = null;
  for (const obj of currentClusterData.objects) {
    for (const c of obj.chunks) {
      if (c.replicaNodes && c.replicaNodes.includes(nodeId)) {
        targetChunkId = c.chunkId;
        break;
      }
      if (c.shards) {
        const s = c.shards.find(sh => sh.nodeId === nodeId);
        if (s) {
          targetChunkId = `${c.chunkId}.s${s.shardIndex}`;
          break;
        }
      }
    }
    if (targetChunkId) break;
  }

  if (!targetChunkId) {
    alert(`No chunks currently stored on node ${nodeId} to corrupt.`);
    return;
  }

  await fetch('/api/chaos/corrupt-chunk', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ nodeId, chunkId: targetChunkId }),
  });
}

async function downloadObject(bucket, key) {
  appendLog(`Fetching ${bucket}/${key} with quorum verification...`, 'info');
  try {
    const res = await fetch(`/${bucket}/${key}`);
    if (!res.ok) {
      const err = await res.json();
      appendLog(`Failed to download ${key}: ${err.error}`, 'error');
      alert(`Download error: ${err.error}`);
      return;
    }
    const blob = await res.blob();
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = key.split('/').pop() || 'downloaded.bin';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    appendLog(`✓ Downloaded ${key} (${formatBytes(blob.size)}) - ETag verified!`, 'success');
  } catch (err) {
    appendLog(`Network error downloading ${key}: ${err.message}`, 'error');
  }
}

async function deleteObject(bucket, key) {
  if (!confirm(`Delete object ${bucket}/${key}?`)) return;
  await fetch(`/${bucket}/${key}`, { method: 'DELETE' });
  appendLog(`Deleted object ${bucket}/${key}`, 'info');
  fetchClusterStatus();
}

function inspectObject(bucket, key) {
  if (!currentClusterData) return;
  const decodedBucket = decodeURIComponent(bucket);
  const decodedKey = decodeURIComponent(key);
  const obj = currentClusterData.objects.find(o => o.bucket === decodedBucket && o.key === decodedKey);
  if (!obj) return;

  const modalTitle = document.getElementById('modal-title');
  const modalBody = document.getElementById('modal-body');
  modalTitle.textContent = `Placement & Shard Map: ${obj.bucket}/${obj.key}`;

  let html = `<p class="text-muted" style="margin-bottom:12px">Policy: <strong>${obj.policy}</strong> | Size: <strong>${formatBytes(obj.size)}</strong> | ETag: <code>${obj.etag}</code></p>`;

  for (const c of (obj.chunks || [])) {
    html += `<div class="shard-card">
      <div style="font-weight:600; font-size:12px; margin-bottom:6px">Chunk #${c.index} (ID: <code>${c.chunkId}</code>)</div>`;

    if (obj.policy === 'REPLICATION') {
      html += `<div>Replicas stored on nodes:</div><div style="display:flex; gap:6px; margin-top:4px">`;
      for (const n of (c.replicaNodes || [])) {
        html += `<span class="badge badge-alive">${n}</span>`;
      }
      html += `</div>`;
    } else if (obj.policy === 'ERASURE') {
      html += `<div>Erasure shards (K=${c.k}, M=${c.m}):</div><div style="display:grid; grid-template-columns:repeat(auto-fill, minmax(140px, 1fr)); gap:6px; margin-top:6px">`;
      for (const s of (c.shards || [])) {
        const type = s.shardIndex < c.k ? 'Data' : 'Parity';
        html += `<div style="background:var(--bg-secondary); padding:4px 6px; border-radius:4px; font-size:11px">
          Shard ${s.shardIndex} (${type}) &rarr; <strong>${s.nodeId}</strong>
        </div>`;
      }
      html += `</div>`;
    }

    html += `</div>`;
  }

  modalBody.innerHTML = html;
  document.getElementById('chunk-modal').classList.remove('hidden');
}

// Upload Form
document.getElementById('upload-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const bucket = document.getElementById('upload-bucket').value.trim();
  const key = document.getElementById('upload-key').value.trim();
  const policy = document.getElementById('upload-policy').value;
  const fileInput = document.getElementById('upload-file');

  if (!fileInput.files || fileInput.files.length === 0) {
    alert('Please select a file to upload');
    return;
  }

  const file = fileInput.files[0];
  appendLog(`Uploading ${key} (${formatBytes(file.size)}) using ${policy}...`, 'info');

  try {
    const res = await fetch(`/${encodeURIComponent(bucket)}/${encodeURIComponent(key)}`, {
      method: 'PUT',
      headers: {
        'Content-Type': file.type || 'application/octet-stream',
        'X-Vault-Storage-Policy': policy,
      },
      body: file,
    });

    if (res.ok) {
      appendLog(`✓ Upload complete for ${key}! Quorum committed to WAL.`, 'success');
      document.getElementById('upload-key').value = '';
      fileInput.value = '';
      fetchClusterStatus();
    } else {
      const err = await res.json();
      appendLog(`Upload failed: ${err.error}`, 'error');
      alert(`Upload failed: ${err.error}`);
    }
  } catch (err) {
    appendLog(`Upload error: ${err.message}`, 'error');
  }
});

// Chaos Button Listeners
document.getElementById('btn-corrupt-random').onclick = async () => {
  if (!currentClusterData || currentClusterData.nodes.length === 0) return;
  const alive = currentClusterData.nodes.filter(n => n.status === 'ALIVE');
  if (alive.length === 0) return;
  const randomNode = alive[Math.floor(Math.random() * alive.length)];
  await corruptNodeRandom(randomNode.nodeId);
};

document.getElementById('btn-partition-half').onclick = async () => {
  if (!currentClusterData) return;
  const nodes = currentClusterData.nodes.map(n => n.nodeId);
  const mid = Math.ceil(nodes.length / 2);
  const groupA = nodes.slice(0, mid);
  const groupB = nodes.slice(mid);

  appendLog(`Partitioning [${groupA.join(',')}] from [${groupB.join(',')}]...`, 'warn');
  await fetch('/api/chaos/partition', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ groupA, groupB }),
  });
  fetchClusterStatus();
};

document.getElementById('btn-heal-network').onclick = async () => {
  appendLog('Healing all network partitions and restoring connections...', 'info');
  await fetch('/api/chaos/heal', { method: 'POST' });
  fetchClusterStatus();
};

document.getElementById('btn-trigger-scrub').onclick = async () => {
  appendLog('Triggering cluster-wide disk scrub for silent bitrot...', 'info');
  const res = await fetch('/api/actions/scrub', { method: 'POST' });
  const data = await res.json();
  appendLog(`Disk scrub complete: scanned ${data.nodesScanned} nodes.`, 'success');
  fetchClusterStatus();
};

document.getElementById('btn-trigger-heal').onclick = async () => {
  appendLog('Triggering prioritized autonomous healing cycle...', 'info');
  const res = await fetch('/api/actions/heal', { method: 'POST' });
  const data = await res.json();
  appendLog(`Healing cycle completed: ${data.totalHealedChunks} chunks healed!`, 'success');
  fetchClusterStatus();
};

document.getElementById('btn-trigger-ae').onclick = async () => {
  appendLog('Triggering peer Merkle tree anti-entropy exchange...', 'info');
  await fetch('/api/actions/anti-entropy', { method: 'POST' });
  appendLog('Merkle anti-entropy exchange complete.', 'success');
  fetchClusterStatus();
};

document.getElementById('btn-refresh').onclick = fetchClusterStatus;
document.getElementById('btn-clear-logs').onclick = () => {
  document.getElementById('events-log').innerHTML = '';
};
document.getElementById('btn-close-modal').onclick = () => {
  document.getElementById('chunk-modal').classList.add('hidden');
};

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function escapeHtml(str) {
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// Initial bootstrap
fetchClusterStatus();
initEventSource();
setInterval(fetchClusterStatus, 2000);
