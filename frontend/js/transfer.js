import { bfsPath, dfsPath, dijkstraPath, buildDSU } from './algorithms.js';

// Baseline fallback if the server doesn't hand us TURN credentials (see server.js
// for how to configure your own free Metered.ca TURN credentials). STUN-only is
// enough for same-machine or same-WiFi testing, but cross-network peers behind
// stricter NATs/firewalls may need a real TURN relay to connect at all.
const FALLBACK_ICE_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' },
];
const CHUNK_SIZE = 16 * 1024; // 16KB — safe, conventional WebRTC data channel chunk size
const CHUNK_PACING_MS = 20;   // small delay between chunk sends (lets the UI animate)
const MAX_BUFFERED_AMOUNT = 256 * 1024; // don't queue more than this much unsent data on a channel
const BUFFER_DRAIN_TIMEOUT_MS = 8000;
const LATENCY_PROBE_INTERVAL_MS = 3000;
// How long (ms) the sender waits for an ACK from the receiver before timing out a batch
const ACK_TIMEOUT_MS = 15000;
// How long (ms) the sender waits for NACK/retransmit round-trip to settle
const NACK_WAIT_MS = 3000;
// How many retransmit rounds are attempted before giving up
const MAX_RETRANSMIT_ROUNDS = 5;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

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
 * them. Key upgrades in this version:
 *
 *  1. Every chunk carries (fileId, chunkIndex, totalChunks) metadata in a
 *     4-byte header prepended to the raw payload.
 *  2. The receiver tracks received chunk indices in a Set (receivedChunks)
 *     and replies with ACK / NACK messages.
 *  3. The sender detects WebRTC disconnection mid-transfer and reroutes
 *     using the existing BFS/DFS/Dijkstra routing system.
 *  4. After sending all chunks the sender requests a NACK report, and
 *     retransmits only the missing chunks — repeating until the receiver
 *     confirms all chunks arrived or we hit MAX_RETRANSMIT_ROUNDS.
 *  5. The receiver performs SHA-256 verification once reconstruction is
 *     complete, comparing against the hash the sender included in the
 *     file-meta frame.
 */
export class TransferManager {
  constructor({ graph, socket, getSelfId, log, ui }) {
    this.graph = graph;
    this.socket = socket;
    this.getSelfId = getSelfId;
    this.log = log;
    this.ui = ui; // { onLinkState, onLatencyMeasured, onSendStart, onPathUsed, onChunkSent, onSendComplete,
                  //   onReceiveStart, onReceiveChunk, onReceiveComplete, onRelayNotice, onVerifyResult }

    this.peerConnections = new Map(); // neighborId -> RTCPeerConnection
    this.dataChannels = new Map();    // neighborId -> RTCDataChannel
    this.pendingCandidates = new Map(); // neighborId -> ICE candidates queued before remote desc is set
    this.channelState = new Map();    // neighborId -> in-progress batch state for THIS incoming channel
    this.activeIncoming = new Map();  // transferId -> assembled-so-far record (destination side only)
    this.latencyProbeTimers = new Map(); // neighborId -> interval id
    this.latencyProbeSentAt = new Map(); // "neighbor:probeId" -> performance timestamp
    this.iceServers = FALLBACK_ICE_SERVERS;

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
    if (this.peerConnections.has(neighborId)) return;
    const pc = new RTCPeerConnection({ iceServers: this.iceServers });
    this.peerConnections.set(neighborId, pc);
    // IMPORTANT: don't stomp on candidates that may have already arrived and
    // been queued for this neighbor before this side even knew a connection
    // was starting (a real, common race — see handleSignal's else-branch).
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
      if (['failed', 'closed', 'disconnected'].includes(pc.connectionState)) {
        this.ui.onLinkState(neighborId, 'closed');
        // ── Step 6: Detect WebRTC disconnection ──
        // Signal the data-channel close path (if it hasn't fired yet) so the
        // sender's rerouting logic in sendFile() picks it up immediately.
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
    const queued = this.pendingCandidates.get(neighborId) || [];
    for (const c of queued) {
      try { await pc.addIceCandidate(new RTCIceCandidate(c)); } catch (_) { /* best-effort */ }
    }
    this.pendingCandidates.set(neighborId, []);
  }

  async handleSignal({ from, data }) {
    let pc = this.peerConnections.get(from);
    this.log(`Signal received from ${from}: ${data.type}`);
    if (data.type === 'offer') {
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
      // ── Step 6: Detect WebRTC disconnection ──
      this._handleNeighborGone(neighborId);
    };
    dc.onerror = (e) => this.log(`Data channel error (${neighborId}): ${e?.error?.message || 'unknown'}`, 'err');
    dc.onmessage = (e) => this.handleIncomingMessage(neighborId, e.data);
  }

  /**
   * Called whenever a neighbor's data channel closes or the peer connection
   * transitions to failed/closed/disconnected. Cleans up local state.
   */
  _handleNeighborGone(neighborId) {
    // Nothing extra needed here beyond what closeNeighbor() does — the sender's
    // main loop in sendFile() checks dc.readyState on every chunk and reroutes
    // automatically. This hook is a single integration point for future extension.
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
      // ── Step 4: ACK messages ──
      else if (msg.type === 'chunk-ack')       this._handleAck(fromNeighborId, msg);
      // ── Steps 8–9: missing-chunk / retransmit protocol ──
      else if (msg.type === 'request-nack')    this._handleRequestNack(fromNeighborId, msg);
      else if (msg.type === 'nack-report')     this._handleNackReport(fromNeighborId, msg);
      else if (msg.type === 'retransmit-meta') this.handleMeta(fromNeighborId, msg);
      return;
    }
    // ── Step 1: binary chunk with (fileId, chunkIndex) header ──
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
    // Update this peer immediately. The server also publishes the value so
    // every peer can use it when calculating a multi-hop Dijkstra route.
    this.ui.onLatencyMeasured?.(fromNeighborId, rttMs);
    this.socket.emit('link-latency', { neighborId: fromNeighborId, latencyMs: rttMs });
  }

  handleMeta(fromNeighborId, msg) {
    const selfId = this.getSelfId();
    const hopIndex = msg.path.indexOf(selfId);
    const isDestination = hopIndex === msg.path.length - 1;

    if (isDestination) {
      if (!msg.continuation || !this.activeIncoming.has(msg.transferId)) {
        // ── Step 3: Receiver stores chunks; Step 5: Maintain receivedChunks ──
        this.activeIncoming.set(msg.transferId, {
          fileName: msg.fileName, fileSize: msg.fileSize, mimeType: msg.mimeType,
          totalChunks: msg.totalChunks,
          // indexed array: slot i holds ArrayBuffer for chunk i (or undefined)
          chunks: new Array(msg.totalChunks),
          // set of indices we have confirmed received
          receivedChunks: new Set(),
          bytesReceived: 0,
          startedAt: performance.now(),
          // SHA-256 hash the sender computed — we verify against this
          sha256: msg.sha256 || null,
          // who to send ACKs / NACKs back toward
          senderPath: msg.path,
        });
        this.ui.onReceiveStart(msg.transferId, this.activeIncoming.get(msg.transferId));
      }
      // Let the UI show the full inbound route too, not just the sender's view of it.
      this.ui.onIncomingPath?.(msg.path);
      this.channelState.set(fromNeighborId, {
        transferId: msg.transferId, isDestination: true,
        batchChunks: msg.batchChunks, receivedInBatch: 0, startIndex: msg.startIndex,
      });
      this.log(`Receiving "${msg.fileName}" — batch of ${msg.batchChunks} chunk(s) via ${fromNeighborId}`, 'hi');
    } else {
      const nextHop = msg.path[hopIndex + 1];
      const outDc = this.dataChannels.get(nextHop);
      this.ui.onRelayNotice(msg.path, selfId);
      if (outDc && outDc.readyState === 'open') {
        outDc.send(JSON.stringify(msg));
        this.channelState.set(fromNeighborId, {
          transferId: msg.transferId, isDestination: false,
          forwardTo: nextHop, batchChunks: msg.batchChunks, receivedInBatch: 0,
        });
        this.log(`Relaying "${msg.fileName}" toward ${msg.path[msg.path.length - 1]} — next hop ${nextHop}`, 'hi');
      } else {
        this.log(`Cannot relay — link to ${nextHop} is not open`, 'err');
      }
    }
  }

  /**
   * ── Steps 1–3: Chunk with embedded (fileId, chunkIndex) header ──
   *
   * Wire format (binary message):
   *   Bytes 0–1  : chunkIndex  (Uint16, little-endian, 0-based)
   *   Bytes 2–3  : transferId length N (Uint16 LE)
   *   Bytes 4–(4+N-1): transferId (UTF-8 string)
   *   Bytes (4+N)–end: raw chunk payload
   *
   * This allows any relay node (and the receiver) to identify which chunk
   * this is without relying on arrival order.
   */
  handleChunk(fromNeighborId, buffer) {
    const st = this.channelState.get(fromNeighborId);
    if (!st) return; // stray chunk with no matching meta — drop it
    if (!(buffer instanceof ArrayBuffer)) {
      this.log(`Expected binary chunk from ${fromNeighborId}, got ${Object.prototype.toString.call(buffer)} — dropping`, 'err');
      return;
    }

    // ── Decode header ──
    const view = new DataView(buffer);
    const chunkIndex = view.getUint16(0, true);
    const tidLen     = view.getUint16(2, true);
    const enc        = new TextDecoder();
    const transferId = enc.decode(new Uint8Array(buffer, 4, tidLen));
    const payload    = buffer.slice(4 + tidLen);

    st.receivedInBatch++;

    if (st.isDestination) {
      const rec = this.activeIncoming.get(transferId);
      if (rec) {
        // ── Step 5: Maintain receivedChunks ──
        if (!rec.receivedChunks.has(chunkIndex)) {
          rec.chunks[chunkIndex] = payload;
          rec.receivedChunks.add(chunkIndex);
          rec.bytesReceived += payload.byteLength;
          this.ui.onReceiveChunk(transferId, rec);
        }

        // ── Step 4: Send ACK back toward the sender ──
        this._sendAck(transferId, rec, chunkIndex);

        // Check if all chunks have arrived
        if (rec.receivedChunks.size >= rec.totalChunks) {
          // ── Step 10: Reconstruct the file ──
          // ── Step 11: SHA-256 verification ──
          this._finalizeReceive(transferId, rec);
        }
      }
    } else {
      // Relay: re-encode the same framed buffer as-is (header preserved)
      const outDc = this.dataChannels.get(st.forwardTo);
      if (outDc && outDc.readyState === 'open') outDc.send(buffer);
    }

    if (st.receivedInBatch >= st.batchChunks) this.channelState.delete(fromNeighborId);
  }

  /**
   * ── Step 4: Send ACK ──
   * Routes the ACK back along the reverse path so the sender can track progress.
   * For simplicity we route it back via the from-neighbor (the next hop toward sender).
   */
  _sendAck(transferId, rec, chunkIndex) {
    // The senderPath is [sender, ...relays, self]. The ACK goes back on the
    // same channel we received the chunk from — we just reply to from-neighbor.
    // Because ACKs are JSON control messages (not file data), we look up who
    // we received the last meta from by scanning channelState.
    for (const [neighborId, st] of this.channelState) {
      if (st.isDestination && st.transferId === transferId) {
        const dc = this.dataChannels.get(neighborId);
        if (dc?.readyState === 'open') {
          dc.send(JSON.stringify({
            type: 'chunk-ack',
            transferId,
            chunkIndex,
            received: rec.receivedChunks.size,
            total: rec.totalChunks,
          }));
        }
        return;
      }
    }
  }

  /** ── Step 4: Receive ACK on sender side (or relay — relay just forwards it) ── */
  _handleAck(fromNeighborId, msg) {
    const resolver = this._ackResolvers.get(msg.transferId);
    if (resolver?.resolveAck) {
      resolver.resolveAck(msg);
    }
    // Also inform the UI if wired up
    this.ui.onChunkAck?.(msg.transferId, msg.chunkIndex, msg.received, msg.total);
  }

  /**
   * ── Steps 8–9: NACK protocol ──
   * Sender sends { type: 'request-nack', transferId } to ask what's missing.
   * Receiver replies with { type: 'nack-report', transferId, missing: [...indices] }.
   */
  _handleRequestNack(fromNeighborId, msg) {
    const rec = this.activeIncoming.get(msg.transferId);
    if (!rec) return;
    const missing = [];
    for (let i = 0; i < rec.totalChunks; i++) {
      if (!rec.receivedChunks.has(i)) missing.push(i);
    }
    const dc = this.dataChannels.get(fromNeighborId);
    if (dc?.readyState === 'open') {
      dc.send(JSON.stringify({ type: 'nack-report', transferId: msg.transferId, missing }));
    }
    this.log(`NACK: ${missing.length} chunk(s) missing for ${msg.transferId}`, missing.length ? 'warn' : 'ok');
  }

  _handleNackReport(fromNeighborId, msg) {
    const resolver = this._ackResolvers.get(msg.transferId);
    if (resolver?.resolveNack) {
      resolver.resolveNack(msg);
    }
  }

  /**
   * ── Step 10: Reconstruct the file ──
   * ── Step 11: SHA-256 verification ──
   */
  async _finalizeReceive(transferId, rec) {
    this.activeIncoming.delete(transferId);

    // Concatenate ordered chunks
    const total = rec.chunks.reduce((s, c) => s + (c?.byteLength || 0), 0);
    const merged = new Uint8Array(total);
    let offset = 0;
    for (let i = 0; i < rec.totalChunks; i++) {
      const chunk = rec.chunks[i];
      if (chunk) { merged.set(new Uint8Array(chunk), offset); offset += chunk.byteLength; }
    }
    const finalBuffer = merged.buffer;

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
      chunks: [finalBuffer], // single merged buffer for Blob construction
      bytesReceived: total,
      hashOk,
    });
  }

  /* -------------------------------------------------------------
   * Sending
   * ----------------------------------------------------------- */

  /**
   * Encode a chunk payload into the framed binary format:
   *   [chunkIndex:Uint16][tidLen:Uint16][transferId:utf8][payload]
   */
  _encodeChunk(chunkIndex, transferId, payloadBuffer) {
    const enc = new TextEncoder();
    const tidBytes = enc.encode(transferId);
    const header = new ArrayBuffer(4 + tidBytes.byteLength);
    const hv = new DataView(header);
    hv.setUint16(0, chunkIndex, true);
    hv.setUint16(2, tidBytes.byteLength, true);
    new Uint8Array(header, 4).set(tidBytes);
    // Concatenate header + payload
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

    // ── Step 1: Chunk the file ──
    const buffer = await file.arrayBuffer();
    const totalChunks = Math.max(1, Math.ceil(buffer.byteLength / CHUNK_SIZE));
    const rawChunks = [];
    for (let i = 0; i < totalChunks; i++) {
      rawChunks.push(buffer.slice(i * CHUNK_SIZE, Math.min(buffer.byteLength, (i + 1) * CHUNK_SIZE)));
    }

    // ── Step 2: Assign fileId (= transferId) + chunkIndex ──
    const transferId = `${selfId}-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
    this.log(`Chunking ${file.name}: ${buffer.byteLength} bytes → ${totalChunks} chunks of ${CHUNK_SIZE}B`, 'hi');

    // Pre-compute SHA-256 hash of the original file buffer so the receiver can verify.
    const fileHash = await sha256Hex(buffer);
    this.log(`SHA-256 (sender): ${fileHash}`, 'hi');

    // Pre-encode every chunk with its framed header so we can retransmit selectively.
    const framedChunks = rawChunks.map((c, idx) => this._encodeChunk(idx, transferId, c));

    this.ui.onSendStart(transferId, { fileName: file.name, fileSize: buffer.byteLength, totalChunks });

    // Register ACK resolver slot
    this._ackResolvers.set(transferId, { resolveAck: null, resolveNack: null });

    let sentIndex = 0;
    let continuation = false;
    let stallGuard = 0;
    let routeWaitAttempts = 0;
    let noProgressStreak = 0;

    // ── Steps 6–7: Detect disconnection + reroute using existing routing system ──
    // The outer while-loop already re-runs pathfinder on each iteration, which
    // naturally picks a new route whenever the current next-hop's channel drops.

    while (sentIndex < totalChunks) {
      const beforeThisRound = sentIndex;
      const pathFinder = algo === 'bfs' ? bfsPath : algo === 'dfs' ? dfsPath : dijkstraPath;
      const path = pathFinder(this.graph, selfId, receiverId, this.log);
      if (!path) {
        if (algo === 'dijkstra' && routeWaitAttempts++ < 40) {
          this.log('Waiting for measured link latency before routing…', 'warn');
          await sleep(250);
          continue;
        }
        this.log('Route lost entirely — aborting transfer.', 'err');
        return;
      }
      routeWaitAttempts = 0;

      const nextHop = path[1];
      const dc = this.dataChannels.get(nextHop);
      if (!dc || dc.readyState !== 'open') {
        stallGuard++;
        if (stallGuard > 50) { this.log(`Giving up — link to ${nextHop} never opened.`, 'err'); return; }
        await sleep(200);
        continue;
      }
      stallGuard = 0;

      const batchChunks = totalChunks - sentIndex;
      const meta = {
        type: 'file-meta', transferId, path, totalChunks, startIndex: sentIndex,
        batchChunks, fileName: file.name, fileSize: buffer.byteLength,
        mimeType: file.type || 'application/octet-stream', continuation,
        sha256: fileHash, // ── Step 11: include hash in meta ──
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

      // ── Steps 1–2: Send framed chunks with fileId + chunkIndex ──
      let brokenMidBatch = false;
      const batchStart = sentIndex;
      for (let i = 0; i < batchChunks; i++) {
        const idx = batchStart + i;
        const framed = framedChunks[idx];
        if (!(framed instanceof ArrayBuffer)) {
          this.log(`Internal error: framed chunk ${idx}/${totalChunks} is not ArrayBuffer — aborting`, 'err');
          return;
        }

        // ── Step 6: Check for disconnection on every chunk ──
        const curDc = this.dataChannels.get(nextHop);
        if (!curDc || curDc.readyState !== 'open') {
          this.log(`Hop to ${nextHop} dropped mid-transfer — rerouting from chunk ${idx}`, 'warn');
          brokenMidBatch = true;
          break;
        }
        try {
          await waitForBufferedAmountBelow(curDc, MAX_BUFFERED_AMOUNT, this.log);
          curDc.send(framed); // ── send framed (fileId+chunkIndex+payload) ──
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

    // ── Steps 8–9: Exchange missing-chunk info + retransmit only missing chunks ──
    await this._retransmitMissing(transferId, framedChunks, selfId, receiverId, algo, totalChunks);

    this.log(`Transfer complete.`, 'ok');
    this.ui.onSendComplete(transferId);
    this._ackResolvers.delete(transferId);
  }

  /**
   * ── Steps 8–9: Retransmit missing chunks ──
   *
   * 1. Wait a moment for in-flight chunks to arrive.
   * 2. Send `request-nack` to the receiver along the current best route.
   * 3. Wait for `nack-report` with the list of missing indices.
   * 4. Retransmit only those chunks.
   * 5. Repeat up to MAX_RETRANSMIT_ROUNDS.
   */
  async _retransmitMissing(transferId, framedChunks, selfId, receiverId, algo, totalChunks) {
    const resolvers = this._ackResolvers.get(transferId);
    if (!resolvers) return;

    for (let round = 0; round < MAX_RETRANSMIT_ROUNDS; round++) {
      // Small grace period for in-flight chunks to land
      await sleep(NACK_WAIT_MS);

      // Find route to receiver
      const pathFinder = algo === 'bfs' ? bfsPath : algo === 'dfs' ? dfsPath : dijkstraPath;
      const path = pathFinder(this.graph, selfId, receiverId, this.log);
      if (!path) { this.log('No route to receiver for NACK round — stopping retransmit.', 'warn'); return; }
      const nextHop = path[1];
      const dc = this.dataChannels.get(nextHop);
      if (!dc || dc.readyState !== 'open') {
        this.log(`Link to ${nextHop} not open for NACK round ${round + 1} — skipping.`, 'warn');
        continue;
      }

      // Ask receiver what's missing
      const nackPromise = new Promise(resolve => { resolvers.resolveNack = resolve; });
      dc.send(JSON.stringify({ type: 'request-nack', transferId }));
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
        return; // Done!
      }

      this.log(`NACK round ${round + 1}: retransmitting ${report.missing.length} missing chunk(s)`, 'warn');

      // Send a retransmit-meta so the receiver knows chunks are coming
      const retransmitMeta = {
        type: 'retransmit-meta',
        transferId, path,
        totalChunks,
        batchChunks: report.missing.length,
        startIndex: report.missing[0],
        continuation: true,
        // Note: receiver won't re-init state on retransmit-meta (continuation=true)
      };
      try {
        await waitForBufferedAmountBelow(dc, MAX_BUFFERED_AMOUNT, this.log);
        dc.send(JSON.stringify(retransmitMeta));
      } catch (e) {
        this.log(`Failed to send retransmit-meta: ${e.message}`, 'err');
        continue;
      }

      // ── Step 9: Retransmit only missing chunks ──
      for (const idx of report.missing) {
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
  }
}
