/**
 * P2P Mesh — Signaling Server
 * ---------------------------
 * This server does NOT move any file data. Its only jobs are:
 *   1. Register peers as they connect and assign them an id (P1, P2, ...)
 *   2. Maintain the network topology graph (which peers should link to which)
 *   3. Relay WebRTC signaling messages (SDP offers/answers, ICE candidates)
 *      between browsers so they can establish direct RTCPeerConnections
 *   4. Broadcast topology changes (join / leave / forced churn) to everyone
 *
 * Once two peers' WebRTC data channels are open, file chunks travel directly
 * browser-to-browser (or browser-to-browser-to-browser for multi-hop relays)
 * — this server is never in that data path.
 */

const path = require('path');
const os = require('os');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

/** Every non-internal IPv4 address this machine has on its network interfaces. */
function getLanAddresses() {
  const addrs = [];
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) addrs.push(iface.address);
    }
  }
  return addrs;
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  pingInterval: 3000, // check peer health every 3 seconds
  pingTimeout: 3000,   // prune dead peers after 3 seconds of missed pong
});

const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, '..', 'frontend')));

/* ---------------------------------------------------------------------
 * TURN credentials (for real cross-network peer connections)
 * ------------------------------------------------------------------- *
 * STUN alone only helps peers punch through simple/no NATs. Two peers on
 * genuinely different networks (or behind restrictive/corporate WiFi) often
 * need a TURN relay to connect at all. Rather than hardcode a public shared
 * demo TURN credential (unreliable — it's shared by thousands of unrelated
 * projects and gets rate-limited), this fetches YOUR OWN free TURN
 * credentials from Metered.ca if you've signed up for one.
 *
 * To enable this (free, ~2 minutes):
 *   1. Sign up at https://dashboard.metered.ca/signup
 *   2. Note the "app name" shown on your dashboard (e.g. "myapp" — this
 *      becomes myapp.metered.live)
 *   3. Go to Dashboard → Developers to find your Secret Key (API key)
 *   4. Fill in the two constants below.
 *
 * If left blank, the server falls back to public STUN only, which is fine
 * for same-machine or same-WiFi testing but may not work across stricter
 * networks.
 */
const METERED_APP_NAME = process.env.METERED_APP_NAME || '';
const METERED_API_KEY = process.env.METERED_API_KEY || '';
const FALLBACK_ICE_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' },
];
const ICE_CACHE_MS = 5 * 60 * 1000;
let cachedIceServers = null;
let cachedAt = 0;

async function getIceServers() {
  if (!METERED_APP_NAME || !METERED_API_KEY) return FALLBACK_ICE_SERVERS;
  if (cachedIceServers && Date.now() - cachedAt < ICE_CACHE_MS) return cachedIceServers;
  try {
    const url = `https://${METERED_APP_NAME}.metered.live/api/v1/turn/credentials?apiKey=${METERED_API_KEY}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const servers = await res.json();
    if (!Array.isArray(servers) || !servers.length) throw new Error('empty response');
    cachedIceServers = servers;
    cachedAt = Date.now();
    console.log(`[i] Using your Metered TURN credentials (${servers.length} ICE servers)`);
    return servers;
  } catch (e) {
    console.log(`[!] Could not fetch Metered TURN credentials (${e.message}) — falling back to STUN only`);
    return FALLBACK_ICE_SERVERS;
  }
}

/* ---------------------------------------------------------------------
 * In-memory topology state
 * ------------------------------------------------------------------- */
let peerCounter = 0;
const nodes = new Map();      // peerId -> { online: true }
// An edge is created when peers are selected as neighbors. Its weight remains
// null until a live WebRTC data-channel ping measures the link's RTT.
const edges = new Map();      // "Pa-Pb" (sorted) -> { a, b, weight }
const socketByPeer = new Map(); // peerId -> socket.id
const peerBySocket = new Map(); // socket.id -> peerId

function edgeKey(a, b) { return [a, b].sort().join('-'); }

function addEdge(a, b, weight) {
  edges.set(edgeKey(a, b), { a, b, weight });
}

function removePeerEdges(id) {
  for (const [key, e] of edges) {
    if (e.a === id || e.b === id) edges.delete(key);
  }
}

function serializeTopology() {
  return {
    nodes: [...nodes.entries()].map(([id, n]) => ({ id, online: n.online })),
    edges: [...edges.values()],
  };
}

function broadcastTopology(event) {
  io.emit('topology-update', { topology: serializeTopology(), event });
}

function getPeerDegree(id) {
  let count = 0;
  for (const e of edges.values()) {
    if (e.a === id || e.b === id) count++;
  }
  return count;
}

function healMesh() {
  const onlinePeers = [...nodes.keys()].filter(id => nodes.get(id).online);
  if (onlinePeers.length < 2) return;

  // Build Disjoint Set Union (DSU) to check graph connectivity
  const parent = new Map(onlinePeers.map(id => [id, id]));
  function find(x) {
    if (parent.get(x) !== x) parent.set(x, find(parent.get(x)));
    return parent.get(x);
  }
  function union(a, b) {
    const ra = find(a), rb = find(b);
    if (ra !== rb) parent.set(ra, rb);
  }

  for (const e of edges.values()) {
    if (parent.has(e.a) && parent.has(e.b)) union(e.a, e.b);
  }

  // Group peers by connected component
  const components = new Map();
  for (const id of onlinePeers) {
    const root = find(id);
    if (!components.has(root)) components.set(root, []);
    components.get(root).push(id);
  }

  let healed = false;
  const compList = [...components.values()];

  // If there are multiple disconnected partitions, bridge them!
  for (let i = 0; i < compList.length - 1; i++) {
    const groupA = compList[i];
    const groupB = compList[i + 1];
    groupA.sort((x, y) => getPeerDegree(x) - getPeerDegree(y));
    groupB.sort((x, y) => getPeerDegree(x) - getPeerDegree(y));
    const pA = groupA[0];
    const pB = groupB[0];
    if (pA && pB && !edges.has(edgeKey(pA, pB))) {
      addEdge(pA, pB, null);
      healed = true;
      console.log(`[Mesh Healing] Bridged partition: ${pA} <--> ${pB}`);
      const socketA = socketByPeer.get(pA);
      if (socketA) {
        io.to(socketA).emit('mesh-connect', { targetPeerId: pB, isOfferer: true });
      }
    }
  }

  // Ensure every node has at least 2 connections (or N-1 if N <= 2)
  const targetDegree = Math.min(2, onlinePeers.length - 1);
  for (const id of onlinePeers) {
    if (getPeerDegree(id) < targetDegree) {
      const candidates = onlinePeers.filter(other => other !== id && !edges.has(edgeKey(id, other)));
      if (candidates.length > 0) {
        candidates.sort((x, y) => getPeerDegree(x) - getPeerDegree(y));
        const partner = candidates[0];
        addEdge(id, partner, null);
        healed = true;
        console.log(`[Mesh Healing] Re-linking under-connected peer ${id} <--> ${partner}`);
        const socketId = socketByPeer.get(id);
        if (socketId) {
          io.to(socketId).emit('mesh-connect', { targetPeerId: partner, isOfferer: true });
        }
      }
    }
  }

  if (healed) {
    broadcastTopology('Mesh self-healed');
  }
}

/* ---------------------------------------------------------------------
 * Socket.IO handlers
 * ------------------------------------------------------------------- */
io.on('connection', async (socket) => {
  const peerId = 'P' + (++peerCounter);
  nodes.set(peerId, { online: true });
  socketByPeer.set(peerId, socket.id);
  peerBySocket.set(socket.id, peerId);

  // Degree-balanced mesh joining:
  // Sort existing online peers by degree ascending (fewest connections first)
  const existing = [...nodes.keys()].filter(id => id !== peerId && nodes.get(id).online);
  existing.sort((a, b) => getPeerDegree(a) - getPeerDegree(b));

  // Determine healthy connection count:
  // 1 peer -> 1 link; 2 peers -> 2 links (triangle); 3+ peers -> 2 to 3 links
  const desiredLinks = Math.min(existing.length, existing.length <= 2 ? existing.length : Math.min(3, 2 + (existing.length % 2)));
  const chosenNeighbors = existing.slice(0, desiredLinks);
  const neighborsToConnect = [];

  chosenNeighbors.forEach(n => {
    addEdge(peerId, n, null);
    neighborsToConnect.push(n);
  });

  console.log(`[+] ${peerId} connected — will offer to: ${neighborsToConnect.join(', ') || '(none, first peer)'}`);

  const iceServers = await getIceServers();

  // Tell the new peer who it is and who to reach out to (it will be the WebRTC offerer)
  socket.emit('init', {
    selfId: peerId,
    topology: serializeTopology(),
    neighborsToConnect,
    iceServers,
  });

  broadcastTopology(`${peerId} joined`);

  socket.on('signal', ({ to, data }) => {
    const targetSocketId = socketByPeer.get(to);
    if (targetSocketId) {
      io.to(targetSocketId).emit('signal', { from: peerId, data });
    } else {
      console.log(`[!] ${peerId} tried to signal ${to} (${data.type}) but no such peer/socket was found`);
    }
  });

  // Only either endpoint of an existing link may update its latency. RTT is
  // measured in the browser over the WebRTC data channel, not on this server.
  socket.on('link-latency', ({ neighborId, latencyMs }) => {
    const key = edgeKey(peerId, neighborId);
    const edge = edges.get(key);
    const latency = Number(latencyMs);
    if (!edge || !Number.isFinite(latency) || latency <= 0 || latency > 60000) return;

    const roundedLatency = Math.round(latency);
    if (edge.weight === roundedLatency) return;
    edge.weight = roundedLatency;
    broadcastTopology();
  });

  socket.on('request-churn', () => {
    const onlineIds = [...nodes.keys()].filter(id => nodes.get(id).online && id !== peerId);
    if (onlineIds.length === 0) return;
    const victim = onlineIds[Math.floor(Math.random() * onlineIds.length)];
    const victimSocketId = socketByPeer.get(victim);
    console.log(`[~] Forced churn: disconnecting ${victim}`);
    if (victimSocketId) {
      io.sockets.sockets.get(victimSocketId)?.disconnect(true);
    }
  });

  socket.on('disconnect', () => {
    nodes.delete(peerId);
    removePeerEdges(peerId);
    socketByPeer.delete(peerId);
    peerBySocket.delete(socket.id);
    console.log(`[-] ${peerId} disconnected`);
    broadcastTopology(`${peerId} left`);

    // Automatically heal the mesh after a peer disconnects
    healMesh();
  });
});

server.listen(PORT, () => {
  const lanAddrs = getLanAddresses();
  console.log(`\nP2P Mesh signaling server running.\n`);
  if (METERED_APP_NAME && METERED_API_KEY) {
    console.log(`TURN: using your Metered.ca credentials (app: ${METERED_APP_NAME})`);
  } else {
    console.log(`TURN: not configured — using public STUN only (fine for same machine/WiFi;`);
    console.log(`      cross-network peers may fail to connect). See the comment near the top`);
    console.log(`      of this file for how to add your own free TURN credentials.`);
  }
  console.log(`On THIS laptop, open:      http://localhost:${PORT}`);
  if (lanAddrs.length) {
    console.log(`From OTHER laptops/phones on the same WiFi/LAN, open:`);
    lanAddrs.forEach(ip => console.log(`                           http://${ip}:${PORT}`));
  } else {
    console.log(`No LAN network interface detected — other devices won't be able to reach this server.`);
  }
  console.log(`\nEach browser/tab that opens one of these URLs becomes a real, independent peer.`);
  console.log(`Windows users: if other laptops can't connect, allow Node.js through the Windows Firewall`);
  console.log(`(or temporarily allow port ${PORT} on Private networks) when prompted, or via Windows Defender Firewall settings.\n`);
});