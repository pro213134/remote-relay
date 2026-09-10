// =============================================================================
// relay/server.js â€” Remote Admin Relay Server
// Deploy to Render.com (FREE, no credit card)
// =============================================================================

const http      = require('http');
const fs        = require('fs');
const path      = require('path');
const WebSocket = require('ws');
const crypto    = require('crypto');

const PORT        = process.env.PORT || 3000;
const SECRET_KEY  = process.env.SECRET_KEY || 'changeme-set-this-in-render-env';
// Set SECRET_KEY in Render â†’ Environment Variables
// The exe must connect with ?key=SECRET_KEY

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Static file server (serves www/ folder)
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css' : 'text/css',
  '.js'  : 'application/javascript',
  '.ico' : 'image/x-icon',
  '.png' : 'image/png',
  '.woff2': 'font/woff2',
};

const httpServer = http.createServer((req, res) => {
  let urlPath = req.url.split('?')[0];
  if (urlPath === '/' || urlPath === '') urlPath = '/index.html';

  // Block path traversal
  const safe = path.normalize(urlPath).replace(/^(\.\.[\/\\])+/, '');
  const filePath = path.join(__dirname, 'www', safe);

  if (!filePath.startsWith(path.join(__dirname, 'www'))) {
    res.writeHead(403); res.end('Forbidden'); return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('404 Not Found');
      return;
    }
    const ext  = path.extname(filePath);
    const mime = MIME[ext] || 'application/octet-stream';
    res.writeHead(200, {
      'Content-Type': mime,
      'Cache-Control': 'no-cache',
    });
    res.end(data);
  });
});

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// WebSocket relay
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const wss = new WebSocket.Server({ server: httpServer });

let agentSocket   = null;  // exe connection
let controlSocket = null;  // browser connection
let agentInfo     = { ip: '', connectedAt: null };

function broadcastStatus() {
  if (!controlSocket || controlSocket.readyState !== WebSocket.OPEN) return;
  const status = {
    type: 'status',
    agentConnected: agentSocket !== null && agentSocket.readyState === WebSocket.OPEN,
    agentIp: agentInfo.ip,
    connectedAt: agentInfo.connectedAt,
    serverTime: new Date().toISOString(),
  };
  controlSocket.send(JSON.stringify(status));
}

wss.on('connection', (ws, req) => {
  const url    = new URL(req.url, `http://localhost`);
  const route  = url.pathname;
  const key    = url.searchParams.get('key');
  const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress;

  // â”€â”€ Agent endpoint (/agent?key=SECRET)
  if (route === '/agent') {
    if (key !== SECRET_KEY) {
      console.log(`[AGENT] Rejected connection from ${clientIp} â€” wrong key`);
      ws.close(4001, 'Unauthorized');
      return;
    }

    console.log(`[AGENT] Connected from ${clientIp}`);
    agentSocket = ws;
    agentInfo   = { ip: clientIp, connectedAt: new Date().toISOString() };
    broadcastStatus();

    ws.on('message', (data, isBinary) => {
      // Forward raw binary to control (browser)
      if (controlSocket && controlSocket.readyState === WebSocket.OPEN) {
        controlSocket.send(data, { binary: true });
      }
    });

    ws.on('close', () => {
      console.log('[AGENT] Disconnected');
      agentSocket = null;
      agentInfo   = { ip: '', connectedAt: null };
      broadcastStatus();
    });

    ws.on('error', err => console.error('[AGENT] Error:', err.message));
    return;
  }

  // â”€â”€ Control endpoint (/control) â€” browser dashboard
  if (route === '/control') {
    console.log(`[CONTROL] Browser connected from ${clientIp}`);
    controlSocket = ws;
    broadcastStatus();

    ws.on('message', (data, isBinary) => {
      // Forward commands to agent
      if (agentSocket && agentSocket.readyState === WebSocket.OPEN) {
        agentSocket.send(data, { binary: true });
      }
    });

    ws.on('close', () => {
      console.log('[CONTROL] Browser disconnected');
      controlSocket = null;
    });

    ws.on('error', err => console.error('[CONTROL] Error:', err.message));
    return;
  }

  // Unknown route
  ws.close(4004, 'Not Found');
});

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Ping keepalive (prevent Render free tier sleep)
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
setInterval(() => {
  if (agentSocket   && agentSocket.readyState   === WebSocket.OPEN) agentSocket.ping();
  if (controlSocket && controlSocket.readyState === WebSocket.OPEN) controlSocket.ping();
}, 25000);

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Start
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
httpServer.listen(PORT, () => {
  console.log('================================================');
  console.log('  Remote Admin Relay Server');
  console.log(`  Listening on port ${PORT}`);
  console.log(`  Secret key: ${SECRET_KEY === 'changeme-set-this-in-render-env' ? 'âš ï¸  DEFAULT (change in env!)' : 'âœ“ Set'}`);
  console.log('================================================');
});
