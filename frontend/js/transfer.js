import { bfsPath, dfsPath, dijkstraPath, buildDSU } from './algorithms.js';
import { TransferStorage } from './storage.js';

// Baseline fallback if the server doesn't hand us TURN credentials (see server.js
// for how to configure your own free Metered.ca TURN credentials). STUN-only is
// enough for same-machine or same-WiFi testing, but cross-network peers behind
// stricter NATs/firewalls may need a real TURN relay to connect at all.
const FALLBACK_ICE_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' },
];
// Keep small transfers cheap, but avoid creating thousands of 16 KB pieces for
// larger files.  64 KB is deliberately capped to remain data-channel friendly.
const MIN_CHUNK_SIZE = 16 * 1024;
const MAX_CHUNK_SIZE = 64 * 1024;
const TARGET_CHUNKS_PER_FILE = 128;
const CHUNK_PACING_MS = 20;   // small delay between chunk sends (lets the UI animate)
const MAX_BUFFERED_AMOUNT = 256 * 1024; // don't queue more than this much unsent data on a channel
const BUFFER_DRAIN_TIMEOUT_MS = 8000;
const LATENCY_PROBE_INTERVAL_MS = 3000;
// How long (ms) the sender waits for an ACK from the receiver before timing out a batch
const ACK_TIMEOUT_MS = 4000;
// How long (ms) the sender waits for NACK/retransmit round-trip to settle
const NACK_WAIT_MS = 400;
// How many retransmit rounds are attempted before giving up
const MAX_RETRANSMIT_ROUNDS = 5;
// Keep batches small so a topology change can be noticed and routed around
// before a whole file is handed to a relay with a broken downstream link.
const MAX_BATCH_CHUNKS = 32;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/**
 * Pick a chunk size from the file size. Sizes are rounded to KiB so transfer
 * logs are readable and the result is stable for files of similar sizes.
 */
function getChunkSize(fileSize) {
  if (fileSize <= MIN_CHUNK_SIZE) return Math.max(1, fileSize);

  const targetSize = Math.ceil(fileSize / TARGET_CHUNKS_PER_FILE);
  const roundedToKiB = Math.ceil(targetSize / 1024) * 1024;
  return Math.min(MAX_CHUNK_SIZE, Math.max(MIN_CHUNK_SIZE, roundedToKiB));
}

/** Wait until a data channel's outgoing buffer has drained below the threshold. */
async function waitForBufferedAmountBelow(dc, threshold, log) {
  const start = performance.now();
  while (dc.bufferedAmount > threshold) {
    if (performance.now() - start > BUFFER_DRAIN_TIMEOUT_MS) {
      throw new Error(`send buffer stayed above ${threshold} bytes for over ${BUFFER_DRAIN_TIMEOUT_MS}ms`);
    }
    await sleep(15);
  }
}

/**
 * Compute SHA-256 of an ArrayBuffer and return the hex string.
 *
 * crypto.subtle is only available in "secure contexts" (HTTPS or localhost).
 * When accessed over plain HTTP on a LAN IP address it is undefined, so we
 * fall back to a small pure-JS SHA-256 implementation.
 */
async function sha256Hex(buffer) {
  if (typeof crypto !== 'undefined' && crypto.subtle) {
    const digest = await crypto.subtle.digest('SHA-256', buffer);
    return [...new Uint8Array(digest)].map(b => b.toString(16).padStart(2, '0')).join('');
  }
  // Pure-JS SHA-256 fallback (RFC 6234 / FIPS 180-4)
  return _sha256Fallback(buffer);
}

function _sha256Fallback(buffer) {
  // Pre-computed round constants (first 32 bits of fractional parts of cube
  // roots of the first 64 primes).
  const K = [
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2,
  ];
  // Initial hash values (first 32 bits of fractional parts of square roots of the first 8 primes)
  let h0=0x6a09e667,h1=0xbb67ae85,h2=0x3c6ef372,h3=0xa54ff53a,
      h4=0x510e527f,h5=0x9b05688c,h6=0x1f83d9ab,h7=0x5be0cd19;

  const data = new Uint8Array(buffer);
  const bitLen = data.length * 8;

  // Pre-processing: padding
  const padLen = (data.length % 64 < 56) ? 56 - (data.length % 64) : 120 - (data.length % 64);
  const padded = new Uint8Array(data.length + padLen + 8);
  padded.set(data);
  padded[data.length] = 0x80;
  // Append bit length as 64-bit big-endian (JS numbers are safe up to 2^53)
  const dv = new DataView(padded.buffer);
  dv.setUint32(padded.length - 4, bitLen >>> 0, false);
  dv.setUint32(padded.length - 8, Math.floor(bitLen / 0x100000000), false);

  const rotr = (x, n) => (x >>> n) | (x << (32 - n));

  for (let i = 0; i < padded.length; i += 64) {
    const w = new Uint32Array(64);
    for (let j = 0; j < 16; j++) w[j] = dv.getUint32(i + j * 4, false);
    for (let j = 16; j < 64; j++) {
      const s0 = rotr(w[j-15],7) ^ rotr(w[j-15],18) ^ (w[j-15] >>> 3);
      const s1 = rotr(w[j-2],17) ^ rotr(w[j-2],19)  ^ (w[j-2] >>> 10);
      w[j] = (w[j-16] + s0 + w[j-7] + s1) >>> 0;
    }
    let [a,b,c,d,e,f,g,h] = [h0,h1,h2,h3,h4,h5,h6,h7];
    for (let j = 0; j < 64; j++) {
      const S1   = rotr(e,6) ^ rotr(e,11) ^ rotr(e,25);
      const ch   = (e & f) ^ (~e & g);
      const temp1 = (h + S1 + ch + K[j] + w[j]) >>> 0;
      const S0   = rotr(a,2) ^ rotr(a,13) ^ rotr(a,22);
      const maj  = (a & b) ^ (a & c) ^ (b & c);
      const temp2 = (S0 + maj) >>> 0;
      h=g; g=f; f=e; e=(d+temp1)>>>0;
      d=c; c=b; b=a; a=(temp1+temp2)>>>0;
    }
    h0=(h0+a)>>>0; h1=(h1+b)>>>0; h2=(h2+c)>>>0; h3=(h3+d)>>>0;
    h4=(h4+e)>>>0; h5=(h5+f)>>>0; h6=(h6+g)>>>0; h7=(h7+h)>>>0;
  }

  return [h0,h1,h2,h3,h4,h5,h6,h7]
    .map(v => v.toString(16).padStart(8,'0')).join('');
}

/**
 * TransferManager owns every RTCPeerConnection/RTCDataChannel this peer
 * holds, and implements the application-layer relay protocol on top of
 * them:
 *
 *  1. Every chunk carries (fileId, chunkIndex, totalChunks) metadata in a
 *     binary header prepended to the raw payload.
 *  2. The receiver persists chunks into IndexedDB as they arrive, maintaining
 *     receivedChunks across sessions and avoiding browser RAM exhaustion.
 *  3. Pause and Resume: transfers can be paused mid-flight by sender or receiver;
 *     state is preserved in IndexedDB and resumes from the exact checkpoint.
 *  4. Receiver tracks received chunk indices and replies with ACK / NACK messages.
 *  5. WebRTC disconnection detection mid-transfer triggers automatic rerouting.
 *  6. Retransmit phase: only missing chunks are requested and resent.
 *  7. Receiver performs SHA-256 integrity verification upon reassembly.
 */
export class TransferManager {
  constructor({ graph, socket, getSelfId, log, ui }) {
    this.graph = graph;
    this.socket = socket;
    this.getSelfId = getSelfId;
    this.log = log;
    this.ui = ui; // { onLinkState, onLatencyMeasured, onSendStart, onPathUsed, onChunkSent, onSendComplete,
                  //   onReceiveStart, onReceiveChunk, onReceiveComplete, onRelayNotice, onVerifyResult,
                  //   onTransferPaused, onTransferResumed, onStorageProgress }

    this.peerConnections = new Map(); // neighborId -> RTCPeerConnection
    this.dataChannels = new Map();    // neighborId -> RTCDataChannel
    this.pendingCandidates = new Map(); // neighborId -> ICE candidates queued before remote desc is set
    this.channelState = new Map();    // neighborId -> in-progress batch state for THIS incoming channel
    this.activeIncoming = new Map();  // transferId -> assembled-so-far record (destination side only)
    this.latencyProbeTimers = new Map(); // neighborId -> interval id
    this.latencyProbeSentAt = new Map(); // "neighbor:probeId" -> performance timestamp
    this.iceServers = FALLBACK_ICE_SERVERS;

    // Persistent storage via IndexedDB
    this.storage = new TransferStorage();

    // Pause/Resume tracking
    this.pausedTransfers = new Set();    // set of transferIds currently paused
    this._resumeResolvers = new Map();   // transferId -> resolver function
    this.activeSenders = new Map();      // transferId -> sender metadata { currentPath, ... }
    this.completedTransfers = new Set(); // transferIds that receiver finished
    this.failedRoutes = new Map();       // transferId -> { failedPeer, chunkIndex, reason }

    // Pending ACK/NACK resolvers: transferId -> { resolveAck, resolveNack }
    this._ackResolvers = new Map();
  }

  /** Called once, right after 'init' arrives, with whatever ICE servers the server handed us. */
  setIceServers(servers) {
    if (Array.isArray(servers) && servers.length) this.iceServers = servers;
  }

  linkState(neighborId) {
    const dc = this.dataChannels.get(neighborId);
    if (dc && dc.readyState === 'open') return 'open';
    if (this.peerConnections.has(neighborId)) return 'connecting';
    return 'none';
  }

  /* -------------------------------------------------------------
   * Connection setup
   * ----------------------------------------------------------- */

  connectToNeighbor(neighborId, isOfferer) {
    let pc = this.peerConnections.get(neighborId);
    if (pc) {
      if (!['failed', 'closed'].includes(pc.connectionState)) return;
      this.closeNeighbor(neighborId);
    }
    pc = new RTCPeerConnection({ iceServers: this.iceServers });
    this.peerConnections.set(neighborId, pc);
    if (!this.pendingCandidates.has(neighborId)) this.pendingCandidates.set(neighborId, []);

    this.log(`RTCPeerConnection created for ${neighborId} (${isOfferer ? 'offerer' : 'answerer'})`);

    pc.onicecandidate = (e) => {
      if (e.candidate) {
        const type = e.candidate.type || 'unknown';
        this.log(`ICE candidate gathered for ${neighborId}: ${type} (${e.candidate.protocol}/${e.candidate.address || e.candidate.candidate?.split(' ')[4] || '?'})`);
        this.socket.emit('signal', { to: neighborId, data: { type: 'ice-candidate', candidate: e.candidate } });
      } else {
        this.log(`ICE candidate gathering finished for ${neighborId}`);
      }
    };
    pc.onicegatheringstatechange = () => this.log(`ICE gathering (${neighborId}): ${pc.iceGatheringState}`);
    pc.onsignalingstatechange = () => this.log(`Signaling state (${neighborId}): ${pc.signalingState}`);
    pc.onconnectionstatechange = () => {
      this.log(`Connection state (${neighborId}): ${pc.connectionState}`);
      if (['failed', 'closed'].includes(pc.connectionState)) {
        this.ui.onLinkState(neighborId, 'closed');
        this.closeNeighbor(neighborId);
      } else if (pc.connectionState === 'disconnected') {
        this.ui.onLinkState(neighborId, 'closed');
        this._handleNeighborGone(neighborId);
      }
    };
    pc.oniceconnectionstatechange = () => {
      this.log(`ICE(${neighborId}): ${pc.iceConnectionState}`, pc.iceConnectionState === 'failed' ? 'err' : undefined);
      if (pc.iceConnectionState === 'failed') {
        this.log(`Could not establish a direct or relayed route to ${neighborId} — likely a restrictive network/firewall on one side.`, 'err');
      }
    };

    if (isOfferer) {
      const dc = pc.createDataChannel('file-transfer');
      this.log(`Data channel created (offerer) for ${neighborId}, initial state: ${dc.readyState}`);
      this.setupDataChannel(neighborId, dc);
      pc.createOffer()
        .then(offer => { this.log(`Offer created for ${neighborId}`); return pc.setLocalDescription(offer); })
        .then(() => {
          this.log(`Local description set, sending offer to ${neighborId}`);
          this.socket.emit('signal', { to: neighborId, data: { type: 'offer', sdp: pc.localDescription } });
        })
        .catch(e => this.log(`Offer/setLocalDescription failed for ${neighborId}: ${e.message}`, 'err'));
    } else {
      pc.ondatachannel = (e) => {
        this.log(`Data channel received (answerer) from ${neighborId}, initial state: ${e.channel.readyState}`);
        this.setupDataChannel(neighborId, e.channel);
      };
    }
    this.ui.onLinkState(neighborId, 'connecting');
  }

  async flushCandidates(neighborId) {
    const pc = this.peerConnections.get(neighborId);
    if (!pc) return;
    const queued = this.pendingCandidates.get(neighborId) || [];
    for (const c of queued) {
      try {
        if (c && pc.remoteDescription) {
          await pc.addIceCandidate(new RTCIceCandidate(c));
        }
      } catch (_) { /* best-effort */ }
    }
    this.pendingCandidates.set(neighborId, []);
  }

  async handleSignal({ from, data }) {
    let pc = this.peerConnections.get(from);
    this.log(`Signal received from ${from}: ${data.type}`);
    if (data.type === 'offer') {
      const selfId = this.getSelfId();
      const isOfferCollision = pc && pc.signalingState !== 'stable';
      const isPolite = selfId ? selfId.localeCompare(from) > 0 : true;

      if (isOfferCollision) {
        if (!isPolite) {
          this.log(`Offer collision with ${from} — ignoring incoming offer (impolite peer)`, 'warn');
          return;
        }
        this.log(`Offer collision with ${from} — rolling back local description (polite peer)`, 'warn');
        try {
          await pc.setLocalDescription({ type: 'rollback' });
        } catch (_) {
          this.closeNeighbor(from);
          pc = null;
        }
      }

      if (!pc) { this.connectToNeighbor(from, false); pc = this.peerConnections.get(from); }
      try {
        await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
        this.log(`Remote offer applied from ${from}`);
        await this.flushCandidates(from);
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);
        this.log(`Sending answer to ${from}`);
        this.socket.emit('signal', { to: from, data: { type: 'answer', sdp: pc.localDescription } });
      } catch (e) {
        this.log(`Failed to process offer from ${from}: ${e.message}`, 'err');
      }
    } else if (data.type === 'answer') {
      if (pc) {
        try {
          await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
          this.log(`Remote answer applied from ${from}`);
          await this.flushCandidates(from);
        } catch (e) {
          this.log(`Failed to process answer from ${from}: ${e.message}`, 'err');
        }
      } else {
        this.log(`Got an answer from ${from} but no matching connection exists`, 'err');
      }
    } else if (data.type === 'ice-candidate') {
      if (pc && pc.remoteDescription) {
        try { await pc.addIceCandidate(new RTCIceCandidate(data.candidate)); } catch (e) { this.log(`addIceCandidate failed for ${from}: ${e.message}`, 'err'); }
      } else {
        const q = this.pendingCandidates.get(from) || [];
        q.push(data.candidate);
        this.pendingCandidates.set(from, q);
        this.log(`Queued early ICE candidate from ${from} (no remote description yet)`);
      }
    }
  }

  setupDataChannel(neighborId, dc) {
    dc.binaryType = 'arraybuffer';
    this.dataChannels.set(neighborId, dc);
    dc.onopen = () => {
      this.log(`Data channel open: ${neighborId}`, 'ok');
      this.ui.onLinkState(neighborId, 'open');
      this.startLatencyProbes(neighborId);
    };
    dc.onclose = () => {
      this.stopLatencyProbes(neighborId);
      this.log(`Data channel closed: ${neighborId}`, 'warn');
      this.ui.onLinkState(neighborId, 'closed');
      this._handleNeighborGone(neighborId);
    };
    dc.onerror = (e) => this.log(`Data channel error (${neighborId}): ${e?.error?.message || 'unknown'}`, 'err');
    dc.onmessage = (e) => this.handleIncomingMessage(neighborId, e.data);
  }

  _handleNeighborGone(neighborId) {
    this.log(`Neighbor ${neighborId} gone — sender will reroute if a transfer is in progress.`, 'warn');
  }

  startLatencyProbes(neighborId) {
    this.stopLatencyProbes(neighborId);
    const probe = () => {
      const dc = this.dataChannels.get(neighborId);
      if (!dc || dc.readyState !== 'open') return;
      const id = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      this.latencyProbeSentAt.set(`${neighborId}:${id}`, performance.now());
      dc.send(JSON.stringify({ type: 'latency-ping', id }));
    };
    probe();
    this.latencyProbeTimers.set(neighborId, setInterval(probe, LATENCY_PROBE_INTERVAL_MS));
  }

  stopLatencyProbes(neighborId) {
    const timer = this.latencyProbeTimers.get(neighborId);
    if (timer) clearInterval(timer);
    this.latencyProbeTimers.delete(neighborId);
    for (const key of this.latencyProbeSentAt.keys()) {
      if (key.startsWith(`${neighborId}:`)) this.latencyProbeSentAt.delete(key);
    }
  }

  closeNeighbor(neighborId) {
    const pc = this.peerConnections.get(neighborId);
    if (pc) pc.close();
    this.peerConnections.delete(neighborId);
    this.dataChannels.delete(neighborId);
    this.channelState.delete(neighborId);
    this.pendingCandidates.delete(neighborId);
    this.stopLatencyProbes(neighborId);
  }

  /* -------------------------------------------------------------
   * Pause / Resume Control
   * ----------------------------------------------------------- */

  pauseTransfer(transferId) {
    if (!transferId) return;
    this.pausedTransfers.add(transferId);
    this.storage.updateTransferStatus(transferId, 'paused');
    this.log(`Transfer ${transferId} paused. State & chunks stored in IndexedDB.`, 'warn');

    // Notify peer
    const rec = this.activeIncoming.get(transferId);
    if (rec && rec.senderPath) {
      // Receiver initiated pause: send towards sender along reversePath
      const revPath = [...rec.senderPath].reverse();
      this._sendControlWithPath({ type: 'transfer-pause', transferId, path: revPath });
    } else {
      // Sender initiated pause: send towards receiver along active path
      const senderRecord = this.activeSenders.get(transferId);
      if (senderRecord && senderRecord.currentPath) {
        this._sendControlWithPath({ type: 'transfer-pause', transferId, path: senderRecord.currentPath });
      }
    }

    this.ui.onTransferPaused?.(transferId);
  }

  resumeTransfer(transferId) {
    if (!transferId) return;
    this.pausedTransfers.delete(transferId);
    this.storage.updateTransferStatus(transferId, 'active');

    // Unblock sender loop if waiting
    const resolver = this._resumeResolvers.get(transferId);
    if (resolver) {
      this._resumeResolvers.delete(transferId);
      resolver();
    }

    // Notify peer
    const rec = this.activeIncoming.get(transferId);
    if (rec && rec.senderPath) {
      const revPath = [...rec.senderPath].reverse();
      this._sendControlWithPath({ type: 'transfer-resume', transferId, path: revPath });
    } else {
      const senderRecord = this.activeSenders.get(transferId);
      if (senderRecord && senderRecord.currentPath) {
        this._sendControlWithPath({ type: 'transfer-resume', transferId, path: senderRecord.currentPath });
      }
    }

    this.log(`Transfer ${transferId} resumed from IndexedDB checkpoint.`, 'ok');
    this.ui.onTransferResumed?.(transferId);
  }

  _sendControlWithPath(msg) {
    if (!msg.path || msg.path.length < 2) return;
    const selfId = this.getSelfId();
    const hopIndex = msg.path.indexOf(selfId);
    if (hopIndex === -1 || hopIndex >= msg.path.length - 1) return;
    const nextHop = msg.path[hopIndex + 1];
    const dc = this.dataChannels.get(nextHop);
    if (dc && dc.readyState === 'open') {
      dc.send(JSON.stringify(msg));
    }
  }

  _handleRemotePause(fromNeighborId, msg) {
    const isDest = this._relayControlMessage(msg);
    if (isDest) {
      this.pausedTransfers.add(msg.transferId);
      this.storage.updateTransferStatus(msg.transferId, 'paused');
      this.log(`Transfer ${msg.transferId} paused by remote peer. Chunks preserved in IndexedDB.`, 'warn');
      this.ui.onTransferPaused?.(msg.transferId);
    }
  }

  _handleRemoteResume(fromNeighborId, msg) {
    const isDest = this._relayControlMessage(msg);
    if (isDest) {
      this.pausedTransfers.delete(msg.transferId);
      this.storage.updateTransferStatus(msg.transferId, 'active');
      const resolver = this._resumeResolvers.get(msg.transferId);
      if (resolver) {
        this._resumeResolvers.delete(msg.transferId);
        resolver();
      }
      this.log(`Transfer ${msg.transferId} resumed by remote peer.`, 'ok');
      this.ui.onTransferResumed?.(msg.transferId);
    }
  }

  _relayControlMessage(msg) {
    const selfId = this.getSelfId();
    if (!msg.path || !Array.isArray(msg.path)) return true;
    const hopIndex = msg.path.indexOf(selfId);
    if (hopIndex === -1) return true;
    const isDestination = hopIndex === msg.path.length - 1;
    if (isDestination) return true;

    const nextHop = msg.path[hopIndex + 1];
    const outDc = this.dataChannels.get(nextHop);
    if (outDc && outDc.readyState === 'open') {
      outDc.send(JSON.stringify(msg));
      this.log(`Relayed ${msg.type} towards ${msg.path[msg.path.length - 1]} via ${nextHop}`);
    } else {
      this.log(`Cannot relay ${msg.type} — link to ${nextHop} is not open`, 'warn');
    }
    return false;
  }

  /** Tell the sender that an intermediate relay can no longer reach its next hop. */
  _reportRouteFailure(st, chunkIndex, reason) {
    if (!st.path) return;
    if (st.routeFailureReported) return;
    st.routeFailureReported = true;
    this.log(`Cannot forward chunk ${chunkIndex} — ${reason}`, 'err');
    this._sendControlWithPath({
      type: 'route-failed',
      transferId: st.transferId,
      failedPeer: st.forwardTo,
      chunkIndex,
      reason,
      path: [...st.path].reverse(),
    });
  }

  _handleRouteFailed(fromNeighborId, msg) {
    if (!this._relayControlMessage(msg)) return;
    this.failedRoutes.set(msg.transferId, {
      failedPeer: msg.failedPeer,
      chunkIndex: Number.isInteger(msg.chunkIndex) ? msg.chunkIndex : 0,
      reason: msg.reason || 'a relay lost its next hop',
    });
    this.log(`Route failure reported by relay: ${msg.reason || msg.failedPeer}. Rerouting…`, 'warn');
  }

  /* -------------------------------------------------------------
   * Receiving / relaying
   * ----------------------------------------------------------- */

  handleIncomingMessage(fromNeighborId, data) {
    if (typeof data === 'string') {
      let msg;
      try { msg = JSON.parse(data); } catch (e) { this.log(`Bad control message from ${fromNeighborId}: ${e.message}`, 'err'); return; }

      if      (msg.type === 'file-meta')       this.handleMeta(fromNeighborId, msg);
      else if (msg.type === 'transfer-abort')  this.channelState.delete(fromNeighborId);
      else if (msg.type === 'latency-ping')    this.replyToLatencyPing(fromNeighborId, msg);
      else if (msg.type === 'latency-pong')    this.recordLatency(fromNeighborId, msg);
      else if (msg.type === 'chunk-ack')       this._handleAck(fromNeighborId, msg);
      else if (msg.type === 'request-nack')    this._handleRequestNack(fromNeighborId, msg);
      else if (msg.type === 'nack-report')     this._handleNackReport(fromNeighborId, msg);
      else if (msg.type === 'retransmit-meta') this.handleMeta(fromNeighborId, msg);
      else if (msg.type === 'transfer-pause')  this._handleRemotePause(fromNeighborId, msg);
      else if (msg.type === 'transfer-resume') this._handleRemoteResume(fromNeighborId, msg);
      else if (msg.type === 'transfer-complete-ack') this._handleTransferCompleteAck(fromNeighborId, msg);
      else if (msg.type === 'route-failed') this._handleRouteFailed(fromNeighborId, msg);
      return;
    }
    this.handleChunk(fromNeighborId, data);
  }

  replyToLatencyPing(fromNeighborId, msg) {
    const dc = this.dataChannels.get(fromNeighborId);
    if (dc?.readyState === 'open') dc.send(JSON.stringify({ type: 'latency-pong', id: msg.id }));
  }

  recordLatency(fromNeighborId, msg) {
    const key = `${fromNeighborId}:${msg.id}`;
    const sentAt = this.latencyProbeSentAt.get(key);
    if (sentAt === undefined) return;
    this.latencyProbeSentAt.delete(key);
    const rttMs = Math.max(1, Math.round(performance.now() - sentAt));
    this.ui.onLatencyMeasured?.(fromNeighborId, rttMs);
    this.socket.emit('link-latency', { neighborId: fromNeighborId, latencyMs: rttMs });
  }

  async handleMeta(fromNeighborId, msg) {
    const selfId = this.getSelfId();
    const hopIndex = msg.path.indexOf(selfId);
    const isDestination = hopIndex === msg.path.length - 1;

    if (isDestination) {
      if (!msg.continuation || !this.activeIncoming.has(msg.transferId)) {
        // Query IndexedDB for any previously stored chunks for this transfer
        const storedChunks = await this.storage.getStoredChunkIndices(msg.transferId);
        const existingChunks = new Array(msg.totalChunks);

        this.activeIncoming.set(msg.transferId, {
          fileName: msg.fileName, fileSize: msg.fileSize, mimeType: msg.mimeType,
          chunkSize: msg.chunkSize,
          totalChunks: msg.totalChunks,
          chunks: existingChunks,
          receivedChunks: storedChunks,
          bytesReceived: 0,
          startedAt: performance.now(),
          sha256: msg.sha256 || null,
          senderPath: msg.path,
        });

        await this.storage.saveTransferMeta({
          transferId: msg.transferId,
          fileName: msg.fileName,
          fileSize: msg.fileSize,
          mimeType: msg.mimeType,
          chunkSize: msg.chunkSize,
          totalChunks: msg.totalChunks,
          sha256: msg.sha256 || null,
          senderPath: msg.path,
          status: 'active',
          isSender: false,
        });

        this.ui.onReceiveStart(msg.transferId, this.activeIncoming.get(msg.transferId));
        if (storedChunks.size > 0) {
          this.log(`IndexedDB checkpoint found: ${storedChunks.size}/${msg.totalChunks} chunks already persisted!`, 'ok');
          this.ui.onStorageProgress?.(msg.transferId, storedChunks.size, msg.totalChunks);
        }
      }
      this.ui.onIncomingPath?.(msg.path);
      this.channelState.set(fromNeighborId, {
        transferId: msg.transferId, isDestination: true,
        batchChunks: msg.batchChunks, receivedInBatch: 0, startIndex: msg.startIndex,
      });
      const rec = this.activeIncoming.get(msg.transferId);
      if (rec) rec.senderPath = msg.path;
      const incoming = this.activeIncoming.get(msg.transferId);
      this.log(
        `Receiving "${incoming?.fileName || msg.fileName || 'file'}" — batch of ${msg.batchChunks} chunk(s) via ${fromNeighborId}`,
        'hi',
      );
    } else {
      const nextHop = msg.path[hopIndex + 1];
      const outDc = this.dataChannels.get(nextHop);
      this.ui.onRelayNotice(msg.path, selfId);
      if (outDc && outDc.readyState === 'open') {
        outDc.send(JSON.stringify(msg));
        this.channelState.set(fromNeighborId, {
          transferId: msg.transferId, isDestination: false,
          forwardTo: nextHop, path: msg.path,
          batchChunks: msg.batchChunks, receivedInBatch: 0,
        });
        this.log(`Relaying "${msg.fileName}" toward ${msg.path[msg.path.length - 1]} — next hop ${nextHop}`, 'hi');
      } else {
        this.log(`Cannot relay — link to ${nextHop} is not open`, 'err');
      }
    }
  }

  handleChunk(fromNeighborId, buffer) {
    const st = this.channelState.get(fromNeighborId);
    if (!st) return;
    if (!(buffer instanceof ArrayBuffer)) {
      this.log(`Expected binary chunk from ${fromNeighborId}, got ${Object.prototype.toString.call(buffer)} — dropping`, 'err');
      return;
    }

    // Decode header: [chunkIndex:u32][tidLen:u16][transferId:utf8][payload]
    const view = new DataView(buffer);
    const chunkIndex = view.getUint32(0, true);
    const tidLen     = view.getUint16(4, true);
    const enc        = new TextDecoder();
    const transferId = enc.decode(new Uint8Array(buffer, 6, tidLen));
    const payload    = buffer.slice(6 + tidLen);

    st.receivedInBatch++;

    if (st.isDestination) {
      const rec = this.activeIncoming.get(transferId);
      if (rec) {
        if (!rec.receivedChunks.has(chunkIndex)) {
          rec.chunks[chunkIndex] = payload;
          rec.receivedChunks.add(chunkIndex);
          rec.bytesReceived += payload.byteLength;

          // Persist chunk to IndexedDB
          this.storage.saveChunk(transferId, chunkIndex, payload).catch(err => {
            console.error('[IndexedDB] saveChunk error:', err);
          });

          this.ui.onReceiveChunk(transferId, rec, chunkIndex);
          this.ui.onStorageProgress?.(transferId, rec.receivedChunks.size, rec.totalChunks);
        }

        // Send ACK back toward sender
        this._sendAck(transferId, rec, chunkIndex);

        // Check if all chunks received
        if (rec.receivedChunks.size >= rec.totalChunks) {
          this._finalizeReceive(transferId, rec);
        }
      }
    } else {
      // Relay: forward binary buffer as-is
      const outDc = this.dataChannels.get(st.forwardTo);
      if (outDc && outDc.readyState === 'open') {
        try {
          outDc.send(buffer);
        } catch (error) {
          this._reportRouteFailure(st, chunkIndex, error.message);
        }
      } else {
        this._reportRouteFailure(st, chunkIndex, `link to ${st.forwardTo} is not open`);
      }
    }

    if (st.receivedInBatch >= st.batchChunks) this.channelState.delete(fromNeighborId);
  }

  _sendAck(transferId, rec, chunkIndex) {
    this._sendControlWithPath({
      type: 'chunk-ack', transferId, chunkIndex,
      received: rec.receivedChunks.size, total: rec.totalChunks,
      path: [...rec.senderPath].reverse(),
    });
  }

  _handleAck(fromNeighborId, msg) {
    if (!this._relayControlMessage(msg)) return;
    const resolver = this._ackResolvers.get(msg.transferId);
    if (resolver?.resolveAck) {
      resolver.resolveAck(msg);
    }
    this.ui.onChunkAck?.(msg.transferId, msg.chunkIndex, msg.received, msg.total);
  }

  _handleTransferCompleteAck(fromNeighborId, msg) {
    if (!this._relayControlMessage(msg)) return;
    const resolver = this._ackResolvers.get(msg.transferId);
    if (resolver?.resolveNack) {
      resolver.resolveNack({ missing: [] });
    }
  }

  async _handleRequestNack(fromNeighborId, msg) {
    if (!this._relayControlMessage(msg)) return;
    const rec = this.activeIncoming.get(msg.transferId);
    let missing = [];
    if (rec) {
      for (let i = 0; i < rec.totalChunks; i++) {
        if (!rec.receivedChunks.has(i)) missing.push(i);
      }
    } else if (this.completedTransfers.has(msg.transferId)) {
      missing = []; // Already completed and verified
    } else {
      const meta = await this.storage.getTransferMeta(msg.transferId);
      if (meta && meta.status === 'completed') {
        missing = [];
      } else {
        return;
      }
    }
    this._sendControlWithPath({
      type: 'nack-report', transferId: msg.transferId, missing,
      path: [...msg.path].reverse(),
    });
    this.log(`NACK: ${missing.length} chunk(s) missing for ${msg.transferId}`, missing.length ? 'warn' : 'ok');
  }

  _handleNackReport(fromNeighborId, msg) {
    if (!this._relayControlMessage(msg)) return;
    const resolver = this._ackResolvers.get(msg.transferId);
    if (resolver?.resolveNack) {
      resolver.resolveNack(msg);
    }
  }

  async _finalizeReceive(transferId, rec) {
    this.completedTransfers.add(transferId);
    this.activeIncoming.delete(transferId);

    // Notify the sender along the reverse multi-hop path.
    this._sendControlWithPath({
      type: 'transfer-complete-ack', transferId,
      path: [...rec.senderPath].reverse(),
    });

    // Retrieve all chunks from IndexedDB (or fallback to memory if IndexedDB unavailable)
    let chunks = await this.storage.getAllChunks(transferId, rec.totalChunks);
    if (!chunks || chunks.filter(Boolean).length < rec.totalChunks) {
      chunks = rec.chunks;
    }

    const total = chunks.reduce((s, c) => s + (c?.byteLength || 0), 0);
    const merged = new Uint8Array(total);
    let offset = 0;
    for (let i = 0; i < rec.totalChunks; i++) {
      const chunk = chunks[i];
      if (chunk) { merged.set(new Uint8Array(chunk), offset); offset += chunk.byteLength; }
    }
    const finalBuffer = merged.buffer;

    // Mark completed in IndexedDB
    await this.storage.updateTransferStatus(transferId, 'completed');

    // SHA-256 verification
    let hashOk = null;
    if (rec.sha256) {
      const receivedHash = await sha256Hex(finalBuffer);
      hashOk = receivedHash === rec.sha256;
      this.log(
        hashOk
          ? `SHA-256 ✓  ${receivedHash}`
          : `SHA-256 MISMATCH! expected ${rec.sha256}, got ${receivedHash}`,
        hashOk ? 'ok' : 'err',
      );
      this.ui.onVerifyResult?.(transferId, hashOk, rec.sha256);
    }

    this.ui.onReceiveComplete(transferId, {
      ...rec,
      chunks: [finalBuffer],
      bytesReceived: total,
      hashOk,
    });
  }

  /* -------------------------------------------------------------
   * Sending
   * ----------------------------------------------------------- */

  _encodeChunk(chunkIndex, transferId, payloadBuffer) {
    const enc = new TextEncoder();
    const tidBytes = enc.encode(transferId);
    const header = new ArrayBuffer(6 + tidBytes.byteLength);
    const hv = new DataView(header);
    hv.setUint32(0, chunkIndex, true);
    hv.setUint16(4, tidBytes.byteLength, true);
    new Uint8Array(header, 6).set(tidBytes);

    const out = new Uint8Array(header.byteLength + payloadBuffer.byteLength);
    out.set(new Uint8Array(header), 0);
    out.set(new Uint8Array(payloadBuffer), header.byteLength);
    return out.buffer;
  }

  async sendFile(file, receiverId, algo) {
    const selfId = this.getSelfId();
    const dsu = buildDSU(this.graph);
    this.log(`DSU.connected(${selfId}, ${receiverId})?`, 'hi');
    if (!dsu.connected(selfId, receiverId)) {
      this.log(`  find(${selfId})=${dsu.find(selfId)}, find(${receiverId})=${dsu.find(receiverId)} — different components`, 'err');
      this.log(`No path exists — ${selfId} and ${receiverId} are in separate network partitions.`, 'err');
      return;
    }
    this.log(`  same root (${dsu.find(selfId)}) — connected`, 'ok');

    const buffer = await file.arrayBuffer();
    const chunkSize = getChunkSize(buffer.byteLength);
    const totalChunks = Math.max(1, Math.ceil(buffer.byteLength / chunkSize));
    const rawChunks = [];
    for (let i = 0; i < totalChunks; i++) {
      rawChunks.push(buffer.slice(i * chunkSize, Math.min(buffer.byteLength, (i + 1) * chunkSize)));
    }

    const transferId = `${selfId}-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
    this.log(`Chunking ${file.name}: ${buffer.byteLength} bytes → ${totalChunks} chunks of up to ${chunkSize}B`, 'hi');

    // Pre-compute SHA-256 hash
    const fileHash = await sha256Hex(buffer);
    this.log(`SHA-256 (sender): ${fileHash}`, 'hi');

    const framedChunks = rawChunks.map((c, idx) => this._encodeChunk(idx, transferId, c));

    // Save transfer record to IndexedDB
    await this.storage.saveTransferMeta({
      transferId,
      fileName: file.name,
      fileSize: buffer.byteLength,
      chunkSize,
      totalChunks,
      sha256: fileHash,
      status: 'active',
      isSender: true,
      receiverId,
    });

    this.activeSenders.set(transferId, {
      file, receiverId, algo, totalChunks, framedChunks, currentPath: null
    });

    this.ui.onSendStart(transferId, { fileName: file.name, fileSize: buffer.byteLength, totalChunks });

    this._ackResolvers.set(transferId, { resolveAck: null, resolveNack: null });

    let sentIndex = 0;
    let continuation = false;
    let stallGuard = 0;
    let routeWaitAttempts = 0;
    let noProgressStreak = 0;

    while (sentIndex < totalChunks) {
      const pendingFailure = this.failedRoutes.get(transferId);
      if (pendingFailure) {
        this.failedRoutes.delete(transferId);
        sentIndex = Math.min(sentIndex, pendingFailure.chunkIndex);
        this.log(`Rerouting from chunk ${sentIndex}; relay lost ${pendingFailure.failedPeer}.`, 'warn');
      }

      // Check pause
      while (this.pausedTransfers.has(transferId)) {
        await new Promise(r => { this._resumeResolvers.set(transferId, r); });
      }

      const beforeThisRound = sentIndex;
      const pathFinder = algo === 'bfs' ? bfsPath : algo === 'dfs' ? dfsPath : dijkstraPath;
      const path = pathFinder(this.graph, selfId, receiverId, this.log);
      if (!path) {
        if (routeWaitAttempts++ < 50) {
          this.log('Waiting for the mesh to heal before routing…', 'warn');
          await sleep(250);
          continue;
        }
        this.log('Route lost entirely — aborting transfer.', 'err');
        return;
      }
      routeWaitAttempts = 0;

      const senderRec = this.activeSenders.get(transferId);
      if (senderRec) senderRec.currentPath = path;

      const nextHop = path[1];
      const dc = this.dataChannels.get(nextHop);
      if (!dc || dc.readyState !== 'open') {
        stallGuard++;
        if (stallGuard > 50) { this.log(`Giving up — link to ${nextHop} never opened.`, 'err'); return; }
        await sleep(200);
        continue;
      }
      stallGuard = 0;

      const batchChunks = Math.min(MAX_BATCH_CHUNKS, totalChunks - sentIndex);
      const meta = {
        type: 'file-meta', transferId, path, totalChunks, startIndex: sentIndex,
        batchChunks, fileName: file.name, fileSize: buffer.byteLength, chunkSize,
        mimeType: file.type || 'application/octet-stream', continuation,
        sha256: fileHash,
      };
      try {
        await waitForBufferedAmountBelow(dc, MAX_BUFFERED_AMOUNT, this.log);
        dc.send(JSON.stringify(meta));
      } catch (e) {
        this.log(`Failed to send file-meta to ${nextHop}: ${e.message} — retrying`, 'err');
        await sleep(200);
        continue;
      }
      this.ui.onPathUsed(path);
      this.log(`Sending via ${path.join(' → ')} (chunks ${sentIndex}-${totalChunks - 1})`, 'hi');

      let brokenMidBatch = false;
      const batchStart = sentIndex;
      for (let i = 0; i < batchChunks; i++) {
        // Pause check
        while (this.pausedTransfers.has(transferId)) {
          await new Promise(r => { this._resumeResolvers.set(transferId, r); });
        }

        const idx = batchStart + i;
        const failure = this.failedRoutes.get(transferId);
        if (failure) {
          this.failedRoutes.delete(transferId);
          sentIndex = Math.min(sentIndex, failure.chunkIndex);
          this.log(`Relay route failed at chunk ${failure.chunkIndex}; selecting a new path.`, 'warn');
          brokenMidBatch = true;
          break;
        }
        const framed = framedChunks[idx];
        if (!(framed instanceof ArrayBuffer)) {
          this.log(`Internal error: framed chunk ${idx}/${totalChunks} is not ArrayBuffer — aborting`, 'err');
          return;
        }

        const curDc = this.dataChannels.get(nextHop);
        if (!curDc || curDc.readyState !== 'open') {
          this.log(`Hop to ${nextHop} dropped mid-transfer — rerouting from chunk ${idx}`, 'warn');
          brokenMidBatch = true;
          break;
        }
        try {
          await waitForBufferedAmountBelow(curDc, MAX_BUFFERED_AMOUNT, this.log);
          curDc.send(framed);
        } catch (e) {
          this.log(`Send failed at chunk ${idx}: ${e.message} — rerouting`, 'err');
          brokenMidBatch = true;
          break;
        }
        sentIndex = idx + 1;
        this.ui.onChunkSent(idx, totalChunks);
        await sleep(CHUNK_PACING_MS);
      }
      continuation = true;
      if (brokenMidBatch) { await sleep(250); continue; }
      if (sentIndex === beforeThisRound) {
        noProgressStreak++;
        if (noProgressStreak > 20) {
          this.log('Giving up — repeated failures with no progress. Check the browser console for WebRTC errors.', 'err');
          return;
        }
      } else {
        noProgressStreak = 0;
      }
    }

    this.log(`All ${totalChunks} chunks handed off — starting NACK/retransmit phase.`, 'ok');

    // NACK retransmit phase
    const receiverConfirmed = await this._retransmitMissing(
      transferId, framedChunks, selfId, receiverId, algo, totalChunks,
    );

    this.activeSenders.delete(transferId);
    if (!receiverConfirmed) {
      await this.storage.updateTransferStatus(transferId, 'failed');
      this.log('Transfer incomplete — receiver never confirmed every chunk.', 'err');
      this.ui.onSendFailed?.(transferId);
      this._ackResolvers.delete(transferId);
      return;
    }

    await this.storage.updateTransferStatus(transferId, 'completed');

    this.log(`Transfer complete.`, 'ok');
    this.ui.onSendComplete(transferId);
    this._ackResolvers.delete(transferId);
  }

  async _retransmitMissing(transferId, framedChunks, selfId, receiverId, algo, totalChunks) {
    const resolvers = this._ackResolvers.get(transferId);
    if (!resolvers) return false;

    for (let round = 0; round < MAX_RETRANSMIT_ROUNDS; round++) {
      // Pause check
      while (this.pausedTransfers.has(transferId)) {
        await new Promise(r => { this._resumeResolvers.set(transferId, r); });
      }

      await sleep(NACK_WAIT_MS);

      const pathFinder = algo === 'bfs' ? bfsPath : algo === 'dfs' ? dfsPath : dijkstraPath;
      const path = pathFinder(this.graph, selfId, receiverId, this.log);
      if (!path) { this.log('No route to receiver for NACK round — stopping retransmit.', 'warn'); continue; }
      const nextHop = path[1];
      const dc = this.dataChannels.get(nextHop);
      if (!dc || dc.readyState !== 'open') {
        this.log(`Link to ${nextHop} not open for NACK round ${round + 1} — skipping.`, 'warn');
        continue;
      }

      const nackPromise = new Promise(resolve => { resolvers.resolveNack = resolve; });
      dc.send(JSON.stringify({ type: 'request-nack', transferId, path }));
      this.log(`NACK round ${round + 1}: waiting for missing-chunk report…`, 'hi');

      let report;
      try {
        report = await Promise.race([
          nackPromise,
          sleep(ACK_TIMEOUT_MS).then(() => ({ missing: null })),
        ]);
      } finally {
        resolvers.resolveNack = null;
      }

      if (!report || report.missing === null) {
        this.log(`NACK round ${round + 1}: timed out waiting for report — will retry.`, 'warn');
        continue;
      }

      if (report.missing.length === 0) {
        this.log(`NACK round ${round + 1}: receiver has all ${totalChunks} chunks ✓`, 'ok');
        return true;
      }

      this.log(`NACK round ${round + 1}: retransmitting ${report.missing.length} missing chunk(s)`, 'warn');

      const retransmitMeta = {
        type: 'retransmit-meta',
        transferId, path,
        totalChunks,
        batchChunks: report.missing.length,
        startIndex: report.missing[0],
        continuation: true,
      };
      try {
        await waitForBufferedAmountBelow(dc, MAX_BUFFERED_AMOUNT, this.log);
        dc.send(JSON.stringify(retransmitMeta));
      } catch (e) {
        this.log(`Failed to send retransmit-meta: ${e.message}`, 'err');
        continue;
      }

      for (const idx of report.missing) {
        // Pause check
        while (this.pausedTransfers.has(transferId)) {
          await new Promise(r => { this._resumeResolvers.set(transferId, r); });
        }

        const framed = framedChunks[idx];
        const curDc = this.dataChannels.get(nextHop);
        if (!curDc || curDc.readyState !== 'open') {
          this.log(`Link dropped during retransmit — will retry in next round.`, 'warn');
          break;
        }
        try {
          await waitForBufferedAmountBelow(curDc, MAX_BUFFERED_AMOUNT, this.log);
          curDc.send(framed);
        } catch (e) {
          this.log(`Retransmit failed at chunk ${idx}: ${e.message}`, 'err');
          break;
        }
        this.ui.onChunkSent(idx, totalChunks);
        await sleep(CHUNK_PACING_MS);
      }
    }

    this.log(`Retransmit rounds exhausted (${MAX_RETRANSMIT_ROUNDS}). Receiver may have missing chunks.`, 'warn');
    return false;
  }
}
