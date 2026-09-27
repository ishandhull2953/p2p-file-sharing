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
      // rec.chunks is now a single merged ArrayBuffer (from _finalizeReceive)
      const blob = new Blob(rec.chunks, { type: rec.mimeType });
      const url = URL.createObjectURL(blob);
      const dl = document.getElementById('downloadLink');
      dl.href = url; dl.download = rec.fileName;
      document.getElementById('downloadWrap').style.display = 'block';
      log(`Transfer complete — ${rec.bytesReceived.toLocaleString()} bytes reassembled at ${selfId}`, 'ok');
      stopPacketAnimation();
    },
    onVerifyResult(transferId, ok, expectedHash) {
      const badge = document.getElementById('hashVerifyBadge');
      if (!badge) return;
      badge.style.display = 'flex';
      badge.className = 'hash-verify-badge ' + (ok ? 'ok' : 'fail');
      badge.querySelector('.hv-text').textContent = ok
        ? `SHA-256 ✓  ${expectedHash.slice(0, 16)}…`
        : `SHA-256 MISMATCH — file may be corrupt`;
    },
    onChunkAck(transferId, chunkIndex, received, total) {
      // Optional: could light up individual chunk cells in a different color.
      // For now just track silently (the NACK logic handles reliability).
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
    // Use the same sha256Hex helper that transfer.js uses — it has a
    // pure-JS fallback for plain-HTTP / LAN-IP contexts where crypto.subtle
    // is not available.
    let hex;
    if (typeof crypto !== 'undefined' && crypto.subtle) {
      const digest = await crypto.subtle.digest('SHA-256', buf);
      hex = [...new Uint8Array(digest)].map(b => b.toString(16).padStart(2, '0')).join('');
    } else {
      // Inline the same pure-JS fallback so app.js stays self-contained.
      hex = _sha256FallbackApp(buf);
    }
    text.textContent = hex;
  } catch (e) {
    text.textContent = 'hash unavailable';
  }
}

// Minimal SHA-256 fallback used only by computeFileHash above.
// The authoritative copy (with comments) lives in transfer.js → _sha256Fallback().
function _sha256FallbackApp(buffer) {
  const K=[0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2];
  let h0=0x6a09e667,h1=0xbb67ae85,h2=0x3c6ef372,h3=0xa54ff53a,h4=0x510e527f,h5=0x9b05688c,h6=0x1f83d9ab,h7=0x5be0cd19;
  const data=new Uint8Array(buffer),bitLen=data.length*8;
  const padLen=(data.length%64<56)?56-(data.length%64):120-(data.length%64);
  const padded=new Uint8Array(data.length+padLen+8);
  padded.set(data);padded[data.length]=0x80;
  const dv=new DataView(padded.buffer);
  dv.setUint32(padded.length-4,bitLen>>>0,false);
  dv.setUint32(padded.length-8,Math.floor(bitLen/0x100000000),false);
  const rotr=(x,n)=>(x>>>n)|(x<<(32-n));
  for(let i=0;i<padded.length;i+=64){
    const w=new Uint32Array(64);
    for(let j=0;j<16;j++)w[j]=dv.getUint32(i+j*4,false);
    for(let j=16;j<64;j++){const s0=rotr(w[j-15],7)^rotr(w[j-15],18)^(w[j-15]>>>3);const s1=rotr(w[j-2],17)^rotr(w[j-2],19)^(w[j-2]>>>10);w[j]=(w[j-16]+s0+w[j-7]+s1)>>>0;}
    let [a,b,c,d,e,f,g,h]=[h0,h1,h2,h3,h4,h5,h6,h7];
    for(let j=0;j<64;j++){const S1=rotr(e,6)^rotr(e,11)^rotr(e,25);const ch=(e&f)^(~e&g);const t1=(h+S1+ch+K[j]+w[j])>>>0;const S0=rotr(a,2)^rotr(a,13)^rotr(a,22);const maj=(a&b)^(a&c)^(b&c);const t2=(S0+maj)>>>0;h=g;g=f;f=e;e=(d+t1)>>>0;d=c;c=b;b=a;a=(t1+t2)>>>0;}
    h0=(h0+a)>>>0;h1=(h1+b)>>>0;h2=(h2+c)>>>0;h3=(h3+d)>>>0;h4=(h4+e)>>>0;h5=(h5+f)>>>0;h6=(h6+g)>>>0;h7=(h7+h)>>>0;
  }
  return [h0,h1,h2,h3,h4,h5,h6,h7].map(v=>v.toString(16).padStart(8,'0')).join('');
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
