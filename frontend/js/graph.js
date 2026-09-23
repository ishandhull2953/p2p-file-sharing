/**
 * Graph — adjacency-list representation of the peer mesh.
 * Each node also carries a small amount of UI state (layout position,
 * whether we currently hold an open WebRTC data channel to it) that the
 * rendering layer in app.js reads directly.
 */
export class Graph {
  constructor() {
    this.nodes = new Map();  // id -> { online, x, y, isSelf, linkState }
    this.adj = new Map();    // id -> Map(neighborId -> weight)
  }

  addNode(id, online = true) {
    if (this.nodes.has(id)) { this.nodes.get(id).online = online; return; }
    this.nodes.set(id, { online, x: 0, y: 0, isSelf: false, linkState: 'none' });
    this.adj.set(id, new Map());
  }

  removeNode(id) {
    this.adj.delete(id);
    for (const nbrs of this.adj.values()) nbrs.delete(id);
    this.nodes.delete(id);
  }

  addEdge(a, b, weight) {
    if (!this.adj.has(a) || !this.adj.has(b)) return;
    this.adj.get(a).set(b, weight);
    this.adj.get(b).set(a, weight);
  }

  removeEdge(a, b) {
    this.adj.get(a)?.delete(b);
    this.adj.get(b)?.delete(a);
  }

  setOnline(id, val) { const n = this.nodes.get(id); if (n) n.online = val; }

  onlineIds() { return [...this.nodes.keys()].filter(id => this.nodes.get(id).online); }

  neighbors(id) {
    const n = this.nodes.get(id);
    if (!n || !n.online) return [];
    return [...this.adj.get(id).entries()].filter(([nb]) => this.nodes.get(nb)?.online);
  }

  /** All online edges, deduplicated (a-b and b-a collapsed to one entry). */
  allEdgesOnline() {
    const seen = new Set(), out = [];
    for (const id of this.onlineIds()) {
      for (const [n, w] of this.neighbors(id)) {
        const key = [id, n].sort().join('-');
        if (!seen.has(key)) { seen.add(key); out.push([id, n, w]); }
      }
    }
    return out;
  }

  /** Replace the whole graph from a server-provided topology snapshot. */
  syncFromServer(topology, selfId) {
    const incomingIds = new Set(topology.nodes.map(n => n.id));

    // remove nodes that no longer exist
    for (const id of [...this.nodes.keys()]) {
      if (!incomingIds.has(id)) this.removeNode(id);
    }
    // add/update nodes
    for (const n of topology.nodes) {
      this.addNode(n.id, n.online);
      if (n.id === selfId) this.nodes.get(n.id).isSelf = true;
    }
    // rebuild edges from scratch (cheap at this scale, avoids drift)
    for (const nbrs of this.adj.values()) nbrs.clear();
    for (const e of topology.edges) this.addEdge(e.a, e.b, e.weight);
  }

  layoutCircular(width, height, marginTop = 0) {
    const ids = [...this.nodes.keys()];
    const cx = width / 2, cy = height / 2 + marginTop, r = Math.min(width, height) / 2 - 70;
    ids.forEach((id, i) => {
      const angle = (2 * Math.PI * i / ids.length) - Math.PI / 2;
      const n = this.nodes.get(id);
      n.x = cx + r * Math.cos(angle);
      n.y = cy + r * Math.sin(angle);
    });
  }
}
