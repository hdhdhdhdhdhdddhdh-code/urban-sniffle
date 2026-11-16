'use strict';
// Barcode Bridge - relay server.
// Phone page  <--WebSocket-->  this server  <--WebSocket-->  laptop agent
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const INDEX = fs.readFileSync(path.join(__dirname, 'index.html'));
const ROOM_RE = /^[A-Za-z0-9_-]{12,64}$/;

// room id -> { phones: Set<ws>, laptops: Set<ws> }
const rooms = new Map();

function getRoom(id) {
  let r = rooms.get(id);
  if (!r) {
    r = { phones: new Set(), laptops: new Set() };
    rooms.set(id, r);
  }
  return r;
}

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function notifyStatus(r) {
  for (const p of r.phones) send(p, { type: 'status', laptops: r.laptops.size });
}

const server = http.createServer((req, res) => {
  const { pathname } = new URL(req.url, 'http://localhost');
  if (pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end('ok');
  }
  if (pathname === '/' || pathname === '/index.html') {
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-cache',
      'Referrer-Policy': 'no-referrer',
    });
    return res.end(INDEX);
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('not found');
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 2048 });

wss.on('connection', (ws, req) => {
  const q = new URL(req.url, 'http://localhost').searchParams;
  const id = q.get('room') || '';
  const role = q.get('role');
  if (!ROOM_RE.test(id) || (role !== 'phone' && role !== 'laptop')) {
    ws.close(1008, 'bad params');
    return;
  }

  const r = getRoom(id);
  const set = role === 'phone' ? r.phones : r.laptops;
  set.add(ws);
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  if (role === 'phone') send(ws, { type: 'status', laptops: r.laptops.size });
  else notifyStatus(r);

  ws.on('message', (data) => {
    ws.isAlive = true;
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }
    if (!msg || typeof msg !== 'object') return;

    if (role === 'phone' && msg.type === 'scan') {
      if (typeof msg.code !== 'string' || !msg.code || msg.code.length > 200) return;
      if (r.laptops.size === 0) {
        send(ws, { type: 'nack', id: msg.id, reason: 'laptop-offline' });
        return;
      }
      for (const l of r.laptops) {
        send(l, { type: 'scan', id: msg.id, code: msg.code, enter: !!msg.enter });
      }
    } else if (role === 'laptop' && msg.type === 'ack') {
      for (const p of r.phones) send(p, { type: 'ack', id: msg.id, ok: msg.ok !== false });
    }
  });

  ws.on('close', () => {
    set.delete(ws);
    if (role === 'laptop') notifyStatus(r);
    if (!r.phones.size && !r.laptops.size) rooms.delete(id);
  });
  ws.on('error', () => {});
});

// Drop dead connections.
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 30000);

server.listen(PORT, () => console.log('Barcode Bridge listening on port ' + PORT));
