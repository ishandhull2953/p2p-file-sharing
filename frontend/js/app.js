import { Graph } from './graph.js';
import { countComponents, kruskalMST, bfsPath, dfsPath, dijkstraPath } from './algorithms.js';
import { TransferManager } from './transfer.js';

const NS = 'http://www.w3.org/2000/svg';
const VBW = 640, VBH = 480;
const GRID_CELL_LIMIT = 300; // above this many chunks, show a progress bar instead of a grid
const noop = () => {};

const svg = document.getElementById('net');
const graph = new Graph();
let selfId = null;
let currentAlgo = 'bfs';
let activePath = null;   // path currently highlighted on the graph (during/around a transfer)
let mstOn = false;       // header MST toggle state
let sendState = null;    // {totalChunks, sent, startedAt} while a send is in progress
let recvState = null;    // {transferId, totalChunks, startedAt} while a receive is in progress
let hoveredNode = null;

/* ---------------------------------------------------------------
 * Trace log
 * --------------------------------------------------------------- */
function log(msg, cls) {
  const box = document.getElementById('trace');
  const line = document.createElement('div');
  line.className = 'line' + (cls ? ' ' + cls : '');
  line.textContent = msg;
  box.appendChild(line);
  box.scrollTop = box.scrollHeight;
}

/* ---------------------------------------------------------------
 * Socket.IO
 * --------------------------------------------------------------- */
const socket = io();

const transferManager = new TransferManager({
  graph,
  socket,
  getSelfId: () => selfId,
  log,
  ui: {
    onLinkState(neighborId, state) {
      const n = graph.nodes.get(neighborId);
      if (n) n.linkState = state;
      render();
    },
    onLatencyMeasured(neighborId, rttMs) {
      if (selfId) graph.addEdge(selfId, neighborId, rttMs);
      render();
    },
    onSendStart(transferId, meta) {
      sendState = { totalChunks: meta.totalChunks, sent: 0, startedAt: performance.now(), sizeBytes: meta.fileSize };
      document.getElementById('transferMeta').innerHTML =
        `Sending <b>${meta.fileName}</b> (${meta.fileSize.toLocaleString()} bytes, ${meta.totalChunks} chunks)`;
      document.getElementById('downloadWrap').style.display = 'none';
      setupProgressUI(meta.totalChunks);
      setChunkStatsVisible(true);
    },
    onPathUsed(path) { activePath = path; startPacketAnimation(path); render(); },
    onChunkSent(idx, total) {
      if (sendState) sendState.sent = idx + 1;
      markProgress(idx, total);
      advancePacket((idx + 1) / total);
      updateChunkStats(sendState);
    },
    onSendComplete() { log('Send complete.', 'ok'); stopPacketAnimation(); },

    onIncomingPath(path) { activePath = path; startPacketAnimation(path); render(); },
    onReceiveStart(transferId, rec) {
      recvState = { transferId, totalChunks: rec.totalChunks, startedAt: performance.now(), sizeBytes: rec.fileSize };
      document.getElementById('transferMeta').innerHTML =
        `Receiving <b>${rec.fileName}</b> (${rec.fileSize.toLocaleString()} bytes, ${rec.totalChunks} chunks)`;
      document.getElementById('downloadWrap').style.display = 'none';
      setupProgressUI(rec.totalChunks);
      setChunkStatsVisible(true);
    },
    onReceiveChunk(transferId, rec) {
      markProgress(rec.chunks.length - 1, rec.totalChunks);
      advancePacket(rec.bytesReceived / rec.fileSize);
      updateChunkStats(recvState, rec.bytesReceived);
    },
    onReceiveComplete(transferId, rec) {
      const blob = new Blob(rec.chunks, { type: rec.mimeType });
      const url = URL.createObjectURL(blob);
      const dl = document.getElementById('downloadLink');
      dl.href = url; dl.download = rec.fileName;
      document.getElementById('downloadWrap').style.display = 'block';
      log(`Transfer complete — ${rec.bytesReceived.toLocaleString()} bytes reassembled at ${selfId}`, 'ok');
      stopPacketAnimation();
    },
    onRelayNotice(path) {
      activePath = path;
      startPacketAnimation(path);
      render();
    },
  },
});

socket.on('connect', () => {
  const b = document.getElementById('serverBadge');
  b.innerHTML = '<span class="live-dot"></span>server: connected';
  b.className = 'live-badge';
});
socket.on('disconnect', () => {
  const b = document.getElementById('serverBadge');
  b.innerHTML = '<span class="live-dot"></span>server: disconnected';
  b.className = 'live-badge down';
});

socket.on('init', (data) => {
  selfId = data.selfId;
  transferManager.setIceServers(data.iceServers);
  document.getElementById('selfBadge').innerHTML = `You are <b>${selfId}</b>`;
  graph.syncFromServer(data.topology, selfId);
  graph.layoutCircular(VBW, VBH, -6);
  render();
  log(`Registered as ${selfId}`, 'hi');
  data.neighborsToConnect.forEach((n) => {
    log(`Connecting to ${n} (offering)...`);
    transferManager.connectToNeighbor(n, true);
  });
});

socket.on('topology-update', ({ topology, event }) => {
  const previousIds = new Set(graph.nodes.keys());
  graph.syncFromServer(topology, selfId);
  for (const id of previousIds) {
    if (!graph.nodes.has(id)) transferManager.closeNeighbor(id);
  }
  graph.layoutCircular(VBW, VBH, -6);
  render();
  if (event) log(event, 'warn');
});

socket.on('signal', (msg) => transferManager.handleSignal(msg));

/* ---------------------------------------------------------------
 * SVG rendering
 * --------------------------------------------------------------- */
function el(tag, attrs) {
  const e = document.createElementNS(NS, tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  return e;
}

function render() {
  svg.innerHTML = '';
  const pathEdgeSet = new Set(), pathNodeSet = new Set();
  const mstEdgeSet = new Set();
  const targetId = document.getElementById('receiverSelect')?.value || null;

  if (activePath) {
    activePath.forEach(id => pathNodeSet.add(id));
    for (let i = 0; i < activePath.length - 1; i++) {
      pathEdgeSet.add([activePath[i], activePath[i + 1]].sort().join('-'));
    }
  }
  if (mstOn) {
    kruskalMST(graph, noop).forEach(([a, b]) => mstEdgeSet.add([a, b].sort().join('-')));
  }

  for (const [a, b, w] of graph.allEdgesOnline()) {
    const na = graph.nodes.get(a), nb = graph.nodes.get(b);
    const key = [a, b].sort().join('-');
    let cls = 'edge-line';
    if (pathEdgeSet.has(key)) cls += ' path';
    else if (mstEdgeSet.has(key)) cls += ' mst';
    else if (a === selfId || b === selfId) {
      const other = a === selfId ? b : a;
      cls += transferManager.linkState(other) === 'open' ? ' open' : ' pending';
    } else {
      cls += Number.isFinite(w) ? ' open' : ' pending';
    }
    svg.appendChild(el('line', { x1: na.x, y1: na.y, x2: nb.x, y2: nb.y, class: cls }));
    const mx = (na.x + nb.x) / 2, my = (na.y + nb.y) / 2;
    const label = Number.isFinite(w) ? `${w}ms` : '…';
    svg.appendChild(el('rect', { x: mx - 15, y: my - 9, width: 30, height: 15, rx: 5, class: 'edge-weight-bg' }));
    const wt = el('text', { x: mx, y: my + 2, class: 'edge-weight', 'text-anchor': 'middle' });
    wt.textContent = label;
    svg.appendChild(wt);
  }

  for (const [id, n] of graph.nodes) {
    const g = el('g', { class: 'node-group', transform: `translate(${n.x},${n.y})` });
    const cls = ['node-circle'];
    if (!n.online) cls.push('offline');
    else if (pathNodeSet.has(id)) cls.push('path');
    else if (mstOn && [...mstEdgeSet].some(k => k.includes(id))) cls.push('mst');
    if (id === selfId) cls.push('self');
    else if (id === targetId && !pathNodeSet.has(id)) cls.push('target');
    g.appendChild(el('circle', { r: 22, class: cls.join(' ') }));
    const label = el('text', { y: 4, class: 'node-label', 'text-anchor': 'middle' });
    label.textContent = id;
    g.appendChild(label);
    g.addEventListener('mouseenter', () => { hoveredNode = id; showTooltip(id); });
    g.addEventListener('mouseleave', () => { hoveredNode = null; hideTooltip(); });
    svg.appendChild(g);
  }

  ensurePacketElement();
  updateHeaderStats();
  updatePeerList();
  updateReceiverSelect();
  updateRoutePreview();
  updateRoutingTable();
}

function updateHeaderStats() {
  document.getElementById('statOnline').textContent = graph.onlineIds().length;
  document.getElementById('statComponents').textContent = countComponents(graph);
  const onlineEdges = graph.allEdgesOnline();
  document.getElementById('statEdges').textContent = onlineEdges.length;
  const measured = onlineEdges.map(([, , w]) => w).filter(Number.isFinite);
  const avg = measured.length ? Math.round(measured.reduce((s, w) => s + w, 0) / measured.length) : null;
  document.getElementById('statAvgLatency').textContent = avg !== null ? `${avg}ms` : '–';
}

function updatePeerList() {
  const list = document.getElementById('peerList');
  list.innerHTML = '';
  document.getElementById('peerCount').textContent = graph.nodes.size;
  for (const [id, n] of graph.nodes) {
    const row = document.createElement('div');
    row.className = 'peer-row';
    const isSelf = id === selfId;
    const isNeighbor = graph.adj.get(selfId)?.has(id);
    let stateLabel = '';
    let stateCls = '';
    if (isSelf) { stateLabel = 'you'; }
    else if (isNeighbor) {
      const state = transferManager.linkState(id);
      stateLabel = state === 'open' ? 'connected' : state === 'connecting' ? 'connecting…' : 'no link';
      stateCls = state === 'open' ? 'open' : state === 'connecting' ? 'connecting' : '';
    } else {
      stateLabel = 'via relay';
    }
    row.innerHTML = `
      <span class="dot ${n.online ? '' : 'off'}"></span>
      <span class="pid ${isSelf ? 'self' : ''}">${id}</span>
      <span class="link-state ${stateCls}">${stateLabel}</span>
    `;
    list.appendChild(row);
  }
}

function updateReceiverSelect() {
  const sel = document.getElementById('receiverSelect');
  const prev = sel.value;
  sel.innerHTML = '';
  for (const id of graph.onlineIds()) {
    if (id === selfId) continue;
    sel.appendChild(new Option(id, id));
  }
  if ([...sel.options].some(o => o.value === prev)) sel.value = prev;
}

/* ---------------------------------------------------------------
 * Live route preview (recomputed on every render — target/algo/topology change)
 * --------------------------------------------------------------- */
function updateRoutePreview() {
  const box = document.getElementById('routePreview');
  const targetId = document.getElementById('receiverSelect')?.value;
  if (!selfId || !targetId) { box.textContent = ''; box.className = 'route-preview'; return; }
  const finder = currentAlgo === 'bfs' ? bfsPath : currentAlgo === 'dfs' ? dfsPath : dijkstraPath;
  const path = finder(graph, selfId, targetId, noop);
  if (!path) {
    box.className = 'route-preview err';
    box.textContent = `No route to ${targetId} — partition detected`;
    return;
  }
  box.className = 'route-preview';
  const hops = path.length - 1;
  let extra = '';
  if (currentAlgo === 'dijkstra') {
    let total = 0;
    for (let i = 0; i < path.length - 1; i++) {
      const w = graph.adj.get(path[i])?.get(path[i + 1]);
      if (Number.isFinite(w)) total += w;
    }
    extra = ` · ${total}ms`;
  }
  box.innerHTML = `<span class="path">${path.join(' → ')}</span> · ${hops} hop${hops === 1 ? '' : 's'}${extra}`;
}

/* ---------------------------------------------------------------
 * Routing table tab (real distances/hops/next-hop from self)
 * --------------------------------------------------------------- */
function updateRoutingTable() {
  const tbody = document.getElementById('routingTableBody');
  if (!tbody || !selfId) return;
  tbody.innerHTML = '';
  for (const [id, n] of graph.nodes) {
    if (id === selfId) continue;
    const tr = document.createElement('tr');
    let latencyText = '—', hopsText = '—', nextHopText = '—';
    if (n.online) {
      const dPath = dijkstraPath(graph, selfId, id, noop);
      const bPath = bfsPath(graph, selfId, id, noop);
      if (dPath) {
        let total = 0;
        for (let i = 0; i < dPath.length - 1; i++) {
          const w = graph.adj.get(dPath[i])?.get(dPath[i + 1]);
          if (Number.isFinite(w)) total += w;
        }
        latencyText = `${total}ms`;
        nextHopText = dPath.length > 1 ? dPath[1] : '—';
      }
      if (bPath) hopsText = String(bPath.length - 1);
    }
    tr.innerHTML = `
      <td class="peer-id">${id}</td>
      <td><span class="status-dot ${n.online ? '' : 'off'}"></span>${n.online ? 'online' : 'offline'}</td>
      <td>${latencyText}</td>
      <td>${hopsText}</td>
      <td>${nextHopText}</td>
    `;
    tbody.appendChild(tr);
  }
}

/* ---------------------------------------------------------------
 * Node hover tooltip
 * --------------------------------------------------------------- */
function showTooltip(id) {
  hideTooltip();
  const n = graph.nodes.get(id);
  if (!n) return;
  const rect = svg.getBoundingClientRect();
  const scaleX = rect.width / VBW, scaleY = rect.height / VBH;
  const wrap = document.getElementById('svgwrap');
  const tip = document.createElement('div');
  tip.className = 'graph-tooltip';
  tip.id = 'graphTooltip';
  const degree = graph.adj.get(id)?.size ?? 0;
  tip.innerHTML = `<b>${id}</b>degree ${degree} · ${n.online ? 'online' : 'offline'}`;
  tip.style.left = `${n.x * scaleX + (rect.left - wrap.getBoundingClientRect().left)}px`;
  tip.style.top = `${n.y * scaleY + (rect.top - wrap.getBoundingClientRect().top)}px`;
  wrap.appendChild(tip);
}
function hideTooltip() {
  document.getElementById('graphTooltip')?.remove();
}

/* ---------------------------------------------------------------
 * Real packet animation — driven by actual chunk-send/receive progress,
 * not a fake timer. Position is interpolated along the current path's
 * segments proportional to real bytes transferred so far.
 * --------------------------------------------------------------- */
let packetEl = null;
let packetPathRef = null;

function ensurePacketElement() {
  if (packetPathRef && !svg.contains(packetEl)) {
    packetEl = el('circle', { r: 5, class: 'packet' });
    svg.appendChild(packetEl);
    placePacketAt(0);
  }
}
function startPacketAnimation(path) {
  packetPathRef = path;
  packetEl = el('circle', { r: 5, class: 'packet' });
  svg.appendChild(packetEl);
  placePacketAt(0);
}
function stopPacketAnimation() {
  packetEl?.remove();
  packetEl = null;
  packetPathRef = null;
}
function placePacketAt(progress) {
  if (!packetEl || !packetPathRef || packetPathRef.length < 2) return;
  const segCount = packetPathRef.length - 1;
  const segLen = 1 / segCount;
  const segIdx = Math.min(segCount - 1, Math.floor(progress / segLen));
  const localT = Math.min(1, (progress - segIdx * segLen) / segLen);
  const a = graph.nodes.get(packetPathRef[segIdx]);
  const b = graph.nodes.get(packetPathRef[segIdx + 1]);
  if (!a || !b) return;
  packetEl.setAttribute('cx', a.x + (b.x - a.x) * localT);
  packetEl.setAttribute('cy', a.y + (b.y - a.y) * localT);
}
function advancePacket(progress) {
  placePacketAt(Math.max(0, Math.min(1, progress)));
}

/* ---------------------------------------------------------------
 * Chunk / progress UI
 * --------------------------------------------------------------- */
function setupProgressUI(totalChunks) {
  const grid = document.getElementById('chunkGrid');
  const track = document.getElementById('progressTrack');
  if (totalChunks <= GRID_CELL_LIMIT) {
    track.style.display = 'none';
    grid.style.display = 'grid';
    grid.innerHTML = '';
    for (let i = 0; i < totalChunks; i++) {
      const c = document.createElement('div');
      c.className = 'chunk-cell';
      c.id = 'chunk-' + i;
      grid.appendChild(c);
    }
  } else {
    grid.style.display = 'none';
    track.style.display = 'block';
    document.getElementById('progressFill').style.width = '0%';
  }
  document.getElementById('statChunkCount').textContent = totalChunks;
}

function markProgress(idx, total) {
  if (total <= GRID_CELL_LIMIT) {
    const cell = document.getElementById('chunk-' + idx);
    if (cell) cell.classList.add('done');
  } else {
    const pct = Math.min(100, Math.round(((idx + 1) / total) * 100));
    document.getElementById('progressFill').style.width = pct + '%';
  }
}

function setChunkStatsVisible(visible) {
  document.getElementById('chunkStats').style.display = visible ? 'grid' : 'none';
}

function updateChunkStats(state, bytesOverride) {
  if (!state) return;
  const elapsedSec = Math.max(0.05, (performance.now() - state.startedAt) / 1000);
  const bytesDone = bytesOverride !== undefined
    ? bytesOverride
    : (state.sent / state.totalChunks) * state.sizeBytes;
  const speedMBs = (bytesDone / 1e6) / elapsedSec;
  document.getElementById('statSpeed').textContent = `${speedMBs.toFixed(1)} MB/s`;
  const doneCount = bytesOverride !== undefined
    ? Math.round((bytesOverride / state.sizeBytes) * state.totalChunks)
    : state.sent;
  document.getElementById('statRatio').textContent = `${doneCount}/${state.totalChunks}`;
}

/* ---------------------------------------------------------------
 * Real SHA-256 file hash
 * --------------------------------------------------------------- */
async function computeFileHash(file) {
  const box = document.getElementById('hashBox');
  const text = document.getElementById('hashText');
  box.style.display = 'flex';
  text.textContent = 'computing hash…';
  try {
    const buf = await file.arrayBuffer();
    const digest = await crypto.subtle.digest('SHA-256', buf);
    const hex = [...new Uint8Array(digest)].map(b => b.toString(16).padStart(2, '0')).join('');
    text.textContent = hex;
  } catch (e) {
    text.textContent = 'hash unavailable';
  }
}

/* ---------------------------------------------------------------
 * UI wiring
 * --------------------------------------------------------------- */
document.getElementById('btnChurn').onclick = () => socket.emit('request-churn');
document.getElementById('btnClearView').onclick = () => { activePath = null; stopPacketAnimation(); render(); };
document.getElementById('mstToggle').onclick = () => {
  mstOn = !mstOn;
  document.getElementById('mstToggle').classList.toggle('on', mstOn);
  render();
};
document.querySelectorAll('.algo-toggle button').forEach((b) => {
  b.onclick = () => {
    currentAlgo = b.dataset.algo;
    document.querySelectorAll('.algo-toggle button').forEach(x => x.classList.toggle('active', x === b));
    updateRoutePreview();
  };
});
document.getElementById('receiverSelect').addEventListener('change', updateRoutePreview);
document.getElementById('fileInput').addEventListener('change', (e) => {
  const file = e.target.files[0];
  if (file) computeFileHash(file);
  else document.getElementById('hashBox').style.display = 'none';
});

document.querySelectorAll('.tab-btn').forEach((btn) => {
  btn.onclick = () => {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b === btn));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.toggle('active', p.dataset.tabPanel === btn.dataset.tab));
  };
});

document.getElementById('btnSend').onclick = async () => {
  const fileInput = document.getElementById('fileInput');
  const receiverId = document.getElementById('receiverSelect').value;
  if (!fileInput.files.length) { log('Select a file before sending.', 'err'); return; }
  if (!receiverId) { log('No receiver available yet.', 'err'); return; }
  const btn = document.getElementById('btnSend');
  btn.disabled = true;
  try {
    await transferManager.sendFile(fileInput.files[0], receiverId, currentAlgo);
  } finally {
    btn.disabled = false;
  }
};
