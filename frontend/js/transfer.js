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
 * TransferManager owns every RTCPeerConnection/RTCDataChannel this peer
 * holds, and implements the application-layer relay protocol on top of
 * them: a small JSON "file-meta" frame announcing a batch of chunks,
 * followed by the raw ArrayBuffer chunks themselves, in order. A peer
 * that isn't the destination just forwards both the meta frame and the
 * chunks on to the next hop named in the path — it never buffers the
 * whole file, only the current message.
 */
export class TransferManager {
  constructor({ graph, socket, getSelfId, log, ui }) {
    this.graph = graph;
    this.socket = socket;
    this.getSelfId = getSelfId;
    this.log = log;
    this.ui = ui; // { onLinkState, onLatencyMeasured, onSendStart, onPathUsed, onChunkSent, onSendComplete,
                  //   onReceiveStart, onReceiveChunk, onReceiveComplete, onRelayNotice }

    this.peerConnections = new Map(); // neighborId -> RTCPeerConnection
    this.dataChannels = new Map();    // neighborId -> RTCDataChannel
    this.pendingCandidates = new Map(); // neighborId -> ICE candidates queued before remote desc is set
    this.channelState = new Map();    // neighborId -> in-progress batch state for THIS incoming channel
    this.activeIncoming = new Map();  // transferId -> assembled-so-far record (destination side only)
    this.latencyProbeTimers = new Map(); // neighborId -> interval id
    this.latencyProbeSentAt = new Map(); // "neighbor:probeId" -> performance timestamp
    this.iceServers = FALLBACK_ICE_SERVERS;
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
      this.log(`Data channel closed: ${neighborId}`, 'warn'); this.ui.onLinkState(neighborId, 'closed');
    };
    dc.onerror = (e) => this.log(`Data channel error (${neighborId}): ${e?.error?.message || 'unknown'}`, 'err');
    dc.onmessage = (e) => this.handleIncomingMessage(neighborId, e.data);
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
      if (msg.type === 'file-meta') this.handleMeta(fromNeighborId, msg);
      else if (msg.type === 'transfer-abort') this.channelState.delete(fromNeighborId);
      else if (msg.type === 'latency-ping') this.replyToLatencyPing(fromNeighborId, msg);
      else if (msg.type === 'latency-pong') this.recordLatency(fromNeighborId, msg);
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
        this.activeIncoming.set(msg.transferId, {
          fileName: msg.fileName, fileSize: msg.fileSize, mimeType: msg.mimeType,
          totalChunks: msg.totalChunks, chunks: [], bytesReceived: 0, startedAt: performance.now(),
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

  handleChunk(fromNeighborId, buffer) {
    const st = this.channelState.get(fromNeighborId);
    if (!st) return; // stray chunk with no matching meta — drop it
    if (!(buffer instanceof ArrayBuffer)) {
      this.log(`Expected binary chunk from ${fromNeighborId}, got ${Object.prototype.toString.call(buffer)} — dropping`, 'err');
      return;
    }
    st.receivedInBatch++;

    if (st.isDestination) {
      const rec = this.activeIncoming.get(st.transferId);
      if (rec) {
        rec.chunks.push(buffer);
        rec.bytesReceived += buffer.byteLength;
        this.ui.onReceiveChunk(st.transferId, rec);
        if (rec.bytesReceived >= rec.fileSize) {
          this.ui.onReceiveComplete(st.transferId, rec);
          this.activeIncoming.delete(st.transferId);
        }
      }
    } else {
      const outDc = this.dataChannels.get(st.forwardTo);
      if (outDc && outDc.readyState === 'open') outDc.send(buffer);
    }

    if (st.receivedInBatch >= st.batchChunks) this.channelState.delete(fromNeighborId);
  }

  /* -------------------------------------------------------------
   * Sending
   * ----------------------------------------------------------- */

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
    const totalChunks = Math.max(1, Math.ceil(buffer.byteLength / CHUNK_SIZE));
    const chunks = [];
    for (let i = 0; i < totalChunks; i++) {
      chunks.push(buffer.slice(i * CHUNK_SIZE, Math.min(buffer.byteLength, (i + 1) * CHUNK_SIZE)));
    }
    const transferId = `${selfId}-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
    this.log(`Chunking ${file.name}: ${buffer.byteLength} bytes → ${totalChunks} chunks of ${CHUNK_SIZE}B`, 'hi');
    this.ui.onSendStart(transferId, { fileName: file.name, fileSize: buffer.byteLength, totalChunks });

    let sentIndex = 0;
    let continuation = false;
    let stallGuard = 0;
    let routeWaitAttempts = 0;
    let noProgressStreak = 0;

    while (sentIndex < totalChunks) {
      const beforeThisRound = sentIndex;
      const pathFinder = algo === 'bfs' ? bfsPath : algo === 'dfs' ? dfsPath : dijkstraPath;
      const path = pathFinder(this.graph, selfId, receiverId, this.log);
      if (!path) {
        // A newly opened channel needs one ping/pong before Dijkstra can use
        // it. Wait for that measurement/topology update rather than failing a
        // file send simply because the first probe is still in flight.
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
        const idx = batchStart + i;
        const chunk = chunks[idx];
        if (!(chunk instanceof ArrayBuffer)) {
          this.log(`Internal error: chunk ${idx}/${totalChunks} is ${Object.prototype.toString.call(chunk)}, not an ArrayBuffer — aborting send`, 'err');
          this.log(`(chunks.length=${chunks.length}, totalChunks=${totalChunks}, sentIndex=${sentIndex}, batchChunks=${batchChunks})`, 'err');
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
          curDc.send(chunk);
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

    this.log(`All ${totalChunks} chunks handed off — sender side complete.`, 'ok');
    this.ui.onSendComplete(transferId);
  }
}
