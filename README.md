# P2P Mesh — Peer-to-Peer File Sharing with Dynamic Network Topology

A real client-server application: a small **signaling server** (Express + Socket.IO)
handles peer registration and WebRTC handshakes, while actual file data travels
directly between browsers over **WebRTC data channels** — including multi-hop
relaying through intermediate peers, chosen by BFS/DFS, with connectivity
checked by a Disjoint Set Union and an optional minimum-spanning-tree view of
the cheapest backbone (Kruskal's algorithm).

The server is never in the data path — it only helps peers find each other
and exchange the connection handshake (SDP offers/answers, ICE candidates).

## Project layout

```
p2p-file-sharing/
├── package.json
├── server/
│   └── server.js          Express static host + Socket.IO signaling
└── frontend/
    ├── index.html
    ├── css/style.css
    └── js/
        ├── graph.js        Graph (adjacency list) data structure
        ├── algorithms.js    DSU, BFS, DFS, Kruskal's MST
        ├── transfer.js      WebRTC connections + multi-hop transfer protocol
        └── app.js           Socket.IO wiring, rendering, UI events
```

## Run it

Requires [Node.js](https://nodejs.org) 18 or later.

```bash
cd p2p-file-sharing
npm install
npm start
```

You'll see:

```
P2P Mesh signaling server running at http://localhost:3000
```

The console output tells you exactly which URLs to use:

```
On THIS laptop, open:      http://localhost:3000
From OTHER laptops/phones on the same WiFi/LAN, open:
                           http://192.168.x.x:3000
```

- On the machine running the server, `http://localhost:3000` works fine.
- From **any other laptop or phone on the same WiFi/network**, use the
  `http://192.168.x.x:3000` address instead — `localhost` only ever means
  "this device," so typing it on a different laptop won't find your server.
- If Windows Firewall pops up asking whether to allow Node.js on private
  networks, click **Allow** — otherwise other devices can't reach it.

Every browser tab/device that opens one of those URLs becomes a **real,
independent peer** (`P1`, `P2`, `P3`, ...) — these are not simulated. Each one
gets linked to 1–2 existing peers by the server and establishes a genuine
WebRTC connection to each of them, whether that peer is a second tab on your
own laptop or a completely different device across the room.

Then, from any tab:
- Pick a **file** and a **destination peer**, choose BFS or DFS, and hit **Send file**.
  If the destination isn't a direct neighbor, the file is relayed hop-by-hop
  through intermediate peers' data channels.
- **Show MST** highlights the cheapest set of links (by simulated latency)
  that keeps everyone connected.
- **Simulate churn** asks the server to forcibly disconnect a random other
  peer, so you can watch the topology update and, if it happens mid-transfer,
  watch the sender reroute around the gap.

## Getting reliable cross-network connections (TURN setup)

Two peers on the same machine or same WiFi will almost always connect fine
with just STUN. Two peers on **different networks**, or behind a stricter
router/firewall, often can't establish a direct connection at all — WebRTC
needs a **TURN** relay as a fallback in that case.

By default this project falls back to STUN-only, which is why you might see
a link stuck on "connecting…" between peers on different networks. To fix
that, get your own free TURN credentials (2 minutes, no credit card):

1. Sign up at **https://dashboard.metered.ca/signup**
2. Note the **app name** shown on your dashboard (e.g. `myapp` — this becomes
   `myapp.metered.live`)
3. Go to **Dashboard → Developers** to find your **Secret Key** (API key)
4. Open `server/server.js` and fill in the two constants near the top:
   ```js
   const METERED_APP_NAME = 'myapp';
   const METERED_API_KEY = 'your-secret-key';
   ```
   (or set them as environment variables `METERED_APP_NAME` /
   `METERED_API_KEY` instead of editing the file)
5. Restart the server. You'll see `TURN: using your Metered.ca credentials`
   in the startup log instead of the STUN-only warning.

The free plan includes 20–50GB/month of TURN traffic, which is far more than
a class demo needs. The server fetches these credentials once and hands them
to every peer on connect — nothing else needs to change.

## Notes on scope

- Without TURN configured, NAT traversal relies on Google's public STUN
  servers — fine for same machine/WiFi, but see the TURN section above for
  connecting across stricter networks.
- Each browser tab's own "Chunks" panel reflects *its own* role in a
  transfer — the sender's tab shows send progress, the receiver's tab shows
  receive progress, and an intermediate relay's tab just logs that it's
  forwarding, since it never actually holds the whole file.
