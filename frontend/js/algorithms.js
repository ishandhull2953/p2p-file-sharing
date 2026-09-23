/**
 * Pure algorithm implementations used to reason about the peer graph.
 * None of these touch the DOM or the network — they take a Graph and
 * (optionally) a logging function, and return a result.
 */

export class DSU {
  constructor(ids) {
    this.parent = new Map(ids.map(id => [id, id]));
    this.rank = new Map(ids.map(id => [id, 0]));
  }
  find(x) {
    if (this.parent.get(x) !== x) this.parent.set(x, this.find(this.parent.get(x)));
    return this.parent.get(x);
  }
  union(a, b) {
    const ra = this.find(a), rb = this.find(b);
    if (ra === rb) return false;
    const rka = this.rank.get(ra), rkb = this.rank.get(rb);
    if (rka < rkb) this.parent.set(ra, rb);
    else if (rka > rkb) this.parent.set(rb, ra);
    else { this.parent.set(rb, ra); this.rank.set(ra, rka + 1); }
    return true;
  }
  connected(a, b) { return this.find(a) === this.find(b); }
}

export function buildDSU(graph) {
  const ids = graph.onlineIds();
  const dsu = new DSU(ids);
  for (const [a, b] of graph.allEdgesOnline()) dsu.union(a, b);
  return dsu;
}

export function countComponents(graph) {
  const dsu = buildDSU(graph);
  return new Set(graph.onlineIds().map(id => dsu.find(id))).size;
}

/** Fewest-hop path between start and end (unweighted BFS). */
export function bfsPath(graph, start, end, log = () => {}) {
  const visited = new Set([start]);
  const prev = new Map();
  const queue = [start];
  log(`BFS(${start} → ${end}): queue = [${start}]`, 'hi');
  while (queue.length) {
    const cur = queue.shift();
    if (cur === end) break;
    for (const [n] of graph.neighbors(cur)) {
      if (!visited.has(n)) {
        visited.add(n); prev.set(n, cur); queue.push(n);
        log(`  visiting ${n} (from ${cur})`);
      }
    }
  }
  if (!visited.has(end)) return null;
  const path = [end];
  while (path[path.length - 1] !== start) path.push(prev.get(path[path.length - 1]));
  path.reverse();
  log(`BFS path found: ${path.join(' → ')}`, 'ok');
  return path;
}

/** First path found via depth-first traversal (unweighted). */
export function dfsPath(graph, start, end, log = () => {}) {
  const visited = new Set();
  const path = [];
  log(`DFS(${start} → ${end}): starting at ${start}`, 'hi');
  function walk(cur) {
    visited.add(cur); path.push(cur);
    if (cur === end) return true;
    for (const [n] of graph.neighbors(cur)) {
      if (!visited.has(n)) {
        log(`  descending to ${n} (from ${cur})`);
        if (walk(n)) return true;
      }
    }
    path.pop();
    return false;
  }
  if (!walk(start)) return null;
  log(`DFS path found: ${path.join(' → ')}`, 'ok');
  return path;
}

/**
 * Lowest-total-latency path. Edges without a measured RTT are deliberately
 * ignored: using them would turn an unknown cost back into a fake weight.
 */
export function dijkstraPath(graph, start, end, log = () => {}) {
  const distances = new Map(graph.onlineIds().map(id => [id, Infinity]));
  const previous = new Map();
  const unvisited = new Set(distances.keys());
  distances.set(start, 0);
  log(`Dijkstra(${start} → ${end}): minimizing measured RTT`, 'hi');

  while (unvisited.size) {
    let current = null;
    for (const id of unvisited) {
      if (current === null || distances.get(id) < distances.get(current)) current = id;
    }
    if (current === null || distances.get(current) === Infinity) break;
    unvisited.delete(current);
    if (current === end) break;

    for (const [neighbor, weight] of graph.neighbors(current)) {
      if (!unvisited.has(neighbor) || !Number.isFinite(weight)) continue;
      const candidate = distances.get(current) + weight;
      if (candidate < distances.get(neighbor)) {
        distances.set(neighbor, candidate);
        previous.set(neighbor, current);
        log(`  relax ${current} → ${neighbor}: ${candidate} ms`, 'hi');
      }
    }
  }

  if (distances.get(end) === Infinity) {
    log('No fully measured route yet — wait for link latency probes.', 'warn');
    return null;
  }
  const path = [end];
  while (path[path.length - 1] !== start) path.push(previous.get(path[path.length - 1]));
  path.reverse();
  log(`Dijkstra path found: ${path.join(' → ')} (${distances.get(end)} ms RTT)`, 'ok');
  return path;
}

/** Kruskal's MST over online edges with measured link latency. */
export function kruskalMST(graph, log = () => {}) {
  const ids = graph.onlineIds();
  const dsu = new DSU(ids);
  const edges = graph.allEdgesOnline().filter(([, , weight]) => Number.isFinite(weight)).sort((a, b) => a[2] - b[2]);
  log(`Kruskal: ${edges.length} candidate edges, sorted by latency`, 'hi');
  const mst = [];
  for (const [a, b, w] of edges) {
    if (dsu.union(a, b)) {
      mst.push([a, b, w]);
      log(`  accept ${a}-${b} (w=${w}) — merges components`, 'ok');
    } else {
      log(`  reject ${a}-${b} (w=${w}) — would create a cycle`, 'warn');
    }
  }
  const totalW = mst.reduce((s, e) => s + e[2], 0);
  log(`MST complete: ${mst.length} edges, total latency ${totalW}`, 'ok');
  return mst;
}
