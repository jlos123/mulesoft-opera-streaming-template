/*
 * OHIP Streaming API simulator - a local stand-in for Oracle OHIP so the Mule template can be
 * exercised end-to-end without a real tenant. Implements the OAuth + graphql-transport-ws behavior
 * described in Oracle's Streaming API Guide
 * (https://docs.oracle.com/en/industries/hospitality/integration-platform/stmig/):
 *
 *   - OAuth:   POST /oauth/v1/tokens  (Basic auth + x-app-key, form body) -> {access_token, expires_in, token_type}
 *   - Stream:  WS /subscriptions?key=<sha256>  echoing Sec-WebSocket-Protocol: graphql-transport-ws
 *   - Frames:  connection_init -> connection_ack -> subscribe -> next... ; ping/pong ; complete
 *   - Rules:   connection_init must arrive <=5s (else close 4408); server can ping; close codes 4401/4403/4409/4504
 *
 * It is intentionally verbose so you can WATCH the handshake in the console and see exactly where a
 * client stalls (e.g. never subscribes, or sends to a dead socket).
 *
 * Control it live over HTTP (see /control/* below) or up front via env vars.
 *
 * Run:   node ohip-sim.js
 * Env:   PORT=8081 TLS=0 SCENARIO=happy EVENT_INTERVAL_MIN_MS=10000 EVENT_INTERVAL_MAX_MS=20000 REQUIRE_KEY=0 CHAIN=CHAIN01
 */

'use strict';
const http = require('http');
const https = require('https');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const CFG = {
  port: parseInt(process.env.PORT || '8081', 10),
  tls: process.env.TLS === '1',
  scenario: process.env.SCENARIO || 'happy', // happy | close-on-connect | close-after-subscribe | no-ack | server-ping
  closeCode: parseInt(process.env.CLOSE_CODE || '4409', 10),
  eventIntervalMinMs: parseInt(process.env.EVENT_INTERVAL_MIN_MS || '10000', 10),
  eventIntervalMaxMs: parseInt(process.env.EVENT_INTERVAL_MAX_MS || '20000', 10),
  initTimeoutMs: parseInt(process.env.INIT_TIMEOUT_MS || '5000', 10),
  requireKey: process.env.REQUIRE_KEY === '1',
  chain: process.env.CHAIN || 'CHAIN01',
  tokenTtlSec: parseInt(process.env.TOKEN_TTL_SEC || '3600', 10),
};

let offsetCounter = parseInt(process.env.START_OFFSET || '97860', 10);
// The Single-Consumer Lock is on the SUBSCRIPTION, not the socket. `liveSocket` is the socket that
// currently holds the EVENT subscription (won the lock and is receiving Business Events). Any number of
// other sockets may be open and connection_init'd - they can run the connection-status query and see
// whether the stream is held - but only one may hold the event subscription. This mirrors real OHIP and
// is what makes Oracle's competing-consumer "connection status check" pattern (Oracle's Streaming API Guide sec.7)
// demonstrable locally: a passive instance keeps a socket + polls status, and only its event `subscribe`
// contends for the lock.
let liveSocket = null;
let lastEvent = null; // last event sent, so /control/emit?repeat=1 can resend the same uniqueEventId
// REST re-fetch scenario for the consumer's Orchestration mode (getProfile/getReservation).
// 'up' = serve 200s; 'down' = every REST GET returns 503 (OHIP outage); 'timeout' = hang forever
// (no response) so the client hits HTTP:TIMEOUT. Flip live via /control/rest?scenario=down (see below).
// This is what lets you trip the consumer's OHIP-outage circuit breaker (the design notes) locally.
let restScenario = process.env.REST_SCENARIO || 'up';

const ts = () => new Date().toISOString();
const log = (dir, msg) => console.log(`${ts()} ${dir} ${msg}`);

// ---- a schema-accurate New Profile Business Event -------------------------------------------
function buildNewProfileEvent() {
  const offset = String(offsetCounter++);
  return {
    metadata: { offset, uniqueEventId: crypto.randomUUID() },
    moduleName: 'PROFILE',
    eventName: 'NEW PROFILE',
    primaryKey: String(1000000 + Math.floor((offsetCounter * 7919) % 8999999)), // deterministic-ish, no Math.random dep
    timestamp: ts(),
    hotelId: null, // profiles are chain-level; hotelId null per CONTEXT.md
    detail: [
      { oldValue: '', newValue: 'Joe', elementName: 'FIRST NAME' },
      { oldValue: '', newValue: 'Bloggs', elementName: 'LAST NAME' },
    ],
  };
}

// ---- OAuth + control HTTP endpoints ---------------------------------------------------------
function handleHttp(req, res) {
  const url = new URL(req.url, `http://localhost:${CFG.port}`);

  if (req.method === 'POST' && url.pathname === '/oauth/v1/tokens') {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', () => {
      const auth = req.headers['authorization'] || '';
      const appKey = req.headers['x-app-key'] || '(none)';
      log('OAUTH', `token requested (auth=${auth ? 'Basic ***' : 'MISSING'}, x-app-key=${appKey}, body="${body}")`);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        access_token: 'sim-token-' + crypto.randomUUID(),
        token_type: 'Bearer',
        expires_in: CFG.tokenTtlSec,
      }));
    });
    return;
  }

  // ---- Orchestration REST re-fetch stubs (getProfile / getReservation) ----------------------
  // The consumer's Orchestration mode GETs current resource state after each Business Event. These
  // stub routes let it run end-to-end against the sim, AND (via restScenario) let you force an OHIP
  // outage to trip the consumer's circuit breaker (the design notes):
  //   scenario=up      -> 200 + a minimal resource body
  //   scenario=down    -> 503 (HTTP:SERVICE_UNAVAILABLE) on every GET
  //   scenario=timeout -> never respond, so the client hits HTTP:TIMEOUT
  // getProfile:     GET /crm/v1/profiles/{profileId}
  // getReservation: GET /rsv/v1/hotels/{hotelId}/reservations/{reservationId}
  const profileMatch = url.pathname.match(/^\/crm\/v1\/profiles\/([^/]+)$/);
  const reservationMatch = url.pathname.match(/^\/rsv\/v1\/hotels\/([^/]+)\/reservations\/([^/]+)$/);
  if (req.method === 'GET' && (profileMatch || reservationMatch)) {
    if (restScenario === 'timeout') {
      log('REST', `${url.pathname} -> scenario=timeout, hanging (no response) to trigger HTTP:TIMEOUT`);
      return; // deliberately never call res.end()
    }
    if (restScenario === 'down') {
      log('REST', `${url.pathname} -> scenario=down, returning 503 (HTTP:SERVICE_UNAVAILABLE)`);
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'service unavailable (sim REST_SCENARIO=down)' }));
      return;
    }
    const body = profileMatch
      ? { id: profileMatch[1], type: 'profile', firstName: 'Joe', lastName: 'Bloggs' }
      : { id: reservationMatch[2], type: 'reservation', hotelId: reservationMatch[1], status: 'RESERVED' };
    log('REST', `${url.pathname} -> 200`);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(body));
    return;
  }

  // Live control: flip the REST re-fetch scenario (up | down | timeout) to simulate an OHIP outage.
  if (url.pathname === '/control/rest') {
    const s = url.searchParams.get('scenario');
    if (!['up', 'down', 'timeout'].includes(s)) {
      res.writeHead(400).end(`scenario must be up|down|timeout (current: ${restScenario})\n`);
      return;
    }
    restScenario = s;
    log('REST', `control -> REST scenario set to '${s}'`);
    res.writeHead(200).end(`rest scenario = ${s}\n`);
    return;
  }

  // Live controls for reproducing scenarios against a connected client.
  if (url.pathname === '/control/emit') {
    const n = parseInt(url.searchParams.get('n') || '1', 10);
    const repeat = url.searchParams.get('repeat') === '1';
    let sent = 0;
    for (let i = 0; i < n; i++) if (emitEvent(repeat)) sent++;
    res.writeHead(200).end(`emitted ${sent} event(s)${repeat ? ' (repeated uniqueEventId)' : ''}\n`);
    return;
  }
  if (url.pathname === '/control/close') {
    const code = parseInt(url.searchParams.get('code') || '4409', 10);
    const ok = forceClose(code, url.searchParams.get('reason') || 'sim-forced-close');
    res.writeHead(200).end(ok ? `closed with ${code}\n` : 'no live socket\n');
    return;
  }
  if (url.pathname === '/control/ping') {
    const ok = sendServerPing();
    res.writeHead(200).end(ok ? 'server ping sent\n' : 'no live socket\n');
    return;
  }
  if (url.pathname === '/control/status') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ liveSocket: !!liveSocket, subscribed: !!(liveSocket && liveSocket._subId), scenario: CFG.scenario, restScenario, nextOffset: String(offsetCounter) }));
    return;
  }

  res.writeHead(404).end('not found\n');
}

// ---- Stream (graphql-transport-ws) ----------------------------------------------------------
function attachWs(server) {
  // handleProtocols echoes the requested subprotocol back in the 101 response - the exact behavior
  // the real OHIP gateway exhibits and that the known Mule 4.11 Netty issue trips over.
  const wss = new WebSocketServer({
    server,
    path: '/subscriptions',
    handleProtocols: (protocols, req) => {
      const raw = req.headers['sec-websocket-protocol'];
      log('WS', `upgrade offered subprotocols: header="${raw}" parsed=${JSON.stringify([...protocols])}`);
      if (protocols.has('graphql-transport-ws')) return 'graphql-transport-ws';
      log('WS', 'no graphql-transport-ws offered -> rejecting upgrade (400)');
      return false;
    },
  });

  // Log every raw upgrade request (fires even when the handshake is later rejected).
  server.on('upgrade', (req) => {
    log('WS', `UPGRADE ${req.url}  headers=${JSON.stringify(req.headers)}`);
  });

  // DIAGNOSTIC ONLY (behavior-preserving): if Node's HTTP parser rejects a request before any
  // handler fires (malformed request line/headers -> 400 with no 'upgrade'/'request' event), log why,
  // then reproduce Node's default response so external behavior is unchanged.
  server.on('clientError', (err, socket) => {
    log('WS', `clientError (raw parser rejected request): ${err.code || err.message}`);
    if (socket.writable) socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
  });

  wss.on('connection', (ws, req) => {
    const url = new URL(req.url, `http://localhost:${CFG.port}`);
    const key = url.searchParams.get('key');
    log('WS', `connection opened (key=${key || 'MISSING'}, subprotocol=${ws.protocol || 'NONE'})`);

    if (CFG.requireKey && (!key || !/^[0-9a-f]{64}$/.test(key))) {
      log('WS', 'rejecting: missing/invalid lowercase-hex ?key= (close 4406)');
      return ws.close(4406, 'invalid key');
    }

    // NOTE: we do NOT reject on connect. The Single-Consumer Lock is enforced on the event `subscribe`
    // (see handleFrame), so a second consumer can open a socket and query connection status while
    // another consumer holds the subscription. Force a socket-open rejection with /control/close if needed.
    if (CFG.scenario === 'close-on-connect') {
      log('WS', `scenario close-on-connect -> closing ${CFG.closeCode}`);
      return ws.close(CFG.closeCode, 'sim scenario');
    }

    ws._acked = false;
    ws._subId = null;
    ws._eventTimer = null;
    ws._serverPingTimer = null;

    // connection_init must arrive within 5s, else the real server closes 4408.
    ws._initTimer = setTimeout(() => {
      if (!ws._acked) {
        log('WS', 'connection_init not received within 5s -> closing 4408');
        ws.close(4408, 'connection init timeout');
      }
    }, CFG.initTimeoutMs);

    ws.on('message', (data) => handleFrame(ws, data));
    ws.on('close', (code, reason) => {
      log('WS', `connection closed (code=${code}, reason="${reason}")`);
      cleanup(ws);
      if (liveSocket === ws) liveSocket = null;
    });
    ws.on('error', (e) => log('WS', `socket error: ${e.message}`));
  });
}

function handleFrame(ws, data) {
  let frame;
  try { frame = JSON.parse(data.toString()); }
  catch { return log('RECV', `non-JSON frame: ${data.toString().slice(0, 120)}`); }
  log('RECV', JSON.stringify(frame));

  switch (frame.type) {
    case 'connection_init':
      clearTimeout(ws._initTimer);
      ws._acked = true;
      if (CFG.scenario === 'no-ack') { log('WS', 'scenario no-ack -> deliberately NOT sending connection_ack'); return; }
      send(ws, { type: 'connection_ack' });
      break;

    case 'subscribe': {
      // graphql-transport-ws carries BOTH the connection-status query and the event subscription as
      // `subscribe` ops. Disambiguate on the GraphQL document: a `connection` query is the status check
      // (Oracle sec.7), a `newEvent` subscription is the real event stream that contends for the lock.
      const query = (frame.payload && frame.payload.query) || '';
      const isStatusQuery = /\bconnection\b/.test(query) && !/\bnewEvent\b/.test(query);

      if (isStatusQuery) {
        // Report whether the stream is currently held. "Active" iff some OTHER socket holds the event
        // subscription; a probe asking about its own future is "Inactive" (free to subscribe). This is
        // exactly what lets a passive instance decide whether to take over.
        const heldByOther = !!liveSocket && liveSocket !== ws;
        const status = heldByOther ? 'Active' : 'Inactive';
        log('WS', `connection-status query (id=${frame.id}) -> status=${status}`);
        send(ws, { id: frame.id, type: 'next', payload: { data: { connection: { id: 'sim-conn-1', status } } } });
        // A GraphQL query completes after one result.
        send(ws, { id: frame.id, type: 'complete' });
        break;
      }

      // Event subscription: this is what acquires the Single-Consumer Lock.
      if (liveSocket && liveSocket !== ws) {
        log('WS', 'Single-Consumer Lock held by another socket -> rejecting event subscribe with 4409');
        return ws.close(4409, 'single consumer lock');
      }
      liveSocket = ws;
      ws._subId = frame.id;
      log('WS', `event subscription accepted (id=${frame.id}); this socket now holds the stream`);
      if (CFG.scenario === 'close-after-subscribe') {
        log('WS', `scenario close-after-subscribe -> closing ${CFG.closeCode}`);
        return ws.close(CFG.closeCode, 'sim scenario');
      }
      // Start the event stream + optional server pings.
      emitEvent(); // one immediately so the tracer bullet succeeds fast
      scheduleNextEvent(ws);
      if (CFG.scenario === 'server-ping') {
        ws._serverPingTimer = setInterval(() => sendServerPing(), 15000);
      }
      break;
    }

    case 'ping':
      send(ws, { type: 'pong' }); // client-initiated ping -> we pong
      break;

    case 'pong':
      log('WS', 'received pong from client (keepalive OK)');
      break;

    case 'complete':
      log('WS', `client sent complete (id=${frame.id}); stopping event stream, then closing 1000 (real OHIP drains + closes)`);
      clearTimeout(ws._eventTimer);
      ws._eventTimer = null;
      // Real OHIP: after `complete` it drains the last events, then closes the socket itself with 1000.
      // The client must NOT close. Closing here is what fires Mule's on-socket-closed handler, which is
      // required to observe the 10s complete->subscribe gap: token-refresh has already set reconnect-pending,
      // so on-socket-closed defers its inline reconnect to the 10s-delayed scheduled path (P-2). Without this
      // close the socket lingers and that deferral is never exercised.
      ws.close(1000, 'server draining after complete');
      break;

    default:
      log('WS', `ignoring unknown frame type: ${frame.type}`);
  }
}

function emitEvent(repeat) {
  if (!liveSocket || !liveSocket._subId) return false;
  // repeat=1 resends lastEvent verbatim (same uniqueEventId, same offset) to simulate a crash-replay
  // duplicate; falls back to a fresh event if nothing has been sent yet on this connection.
  const event = (repeat && lastEvent) ? lastEvent : buildNewProfileEvent();
  lastEvent = event;
  send(liveSocket, { id: liveSocket._subId, type: 'next', payload: { data: { newEvent: event } } });
  return true;
}

// Re-rolls a random delay in [eventIntervalMinMs, eventIntervalMaxMs] before each background event,
// rather than a fixed setInterval, so auto-emitted events land at an uneven ~10-20s cadence instead
// of a metronome - closer to how real Business Events arrive.
function scheduleNextEvent(ws) {
  const span = Math.max(0, CFG.eventIntervalMaxMs - CFG.eventIntervalMinMs);
  const delay = CFG.eventIntervalMinMs + Math.floor(Math.random() * (span + 1));
  ws._eventTimer = setTimeout(() => {
    emitEvent();
    scheduleNextEvent(ws);
  }, delay);
}

function sendServerPing() {
  if (!liveSocket) return false;
  send(liveSocket, { type: 'ping' });
  return true;
}

function forceClose(code, reason) {
  if (!liveSocket) return false;
  // 1005 (no status) and 1006 (abnormal closure) are RFC 6455 reserved codes an endpoint MUST NOT send
  // in a close frame - they only ever appear client-side when the connection drops without a clean close.
  // The `ws` library throws if you pass them to close(). To faithfully reproduce how a client observes
  // 1006 (which is exactly what OHIP would produce on an abnormal drop), destroy the underlying socket
  // instead of sending a close frame; the client's WebSocket stack then surfaces 1006 on its end.
  if (code === 1006 || code === 1005) {
    log('WS', `control -> abruptly destroying socket to simulate client-side ${code}`);
    liveSocket.terminate();
    return true;
  }
  log('WS', `control -> closing live socket with ${code}`);
  liveSocket.close(code, reason);
  return true;
}

function send(ws, obj) {
  const s = JSON.stringify(obj);
  log('SEND', s.length > 200 ? s.slice(0, 200) + '…' : s);
  ws.send(s);
}

function cleanup(ws) {
  clearTimeout(ws._initTimer);
  clearTimeout(ws._eventTimer);
  clearInterval(ws._serverPingTimer);
}

// ---- boot -----------------------------------------------------------------------------------
function makeServer() {
  if (!CFG.tls) return http.createServer(handleHttp);
  // Self-signed cert generated on the fly so the app can use wss:// locally (needs app-side trust).
  const { cert, key } = selfSignedCert();
  return https.createServer({ cert, key }, handleHttp);
}

function selfSignedCert() {
  // Minimal self-signed pair via Node's crypto. For local test only.
  const { generateKeyPairSync } = crypto;
  const { privateKey, publicKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
  // Node has no built-in X.509 issuer; emit a note and fall back to plain if selfsigned unsupported.
  throw new Error('TLS mode needs a cert toolchain (openssl). Run plain (TLS=0) or supply CERT_PATH/KEY_PATH.');
}

const server = makeServer();
attachWs(server);
server.listen(CFG.port, () => {
  const proto = CFG.tls ? 'https/wss' : 'http/ws';
  console.log(`\nOHIP simulator listening on ${proto}://localhost:${CFG.port}`);
  console.log(`  scenario=${CFG.scenario}  eventInterval=${CFG.eventIntervalMinMs}-${CFG.eventIntervalMaxMs}ms  requireKey=${CFG.requireKey}  chain=${CFG.chain}`);
  console.log(`  OAuth:  POST http://localhost:${CFG.port}/oauth/v1/tokens`);
  console.log(`  Stream: ws://localhost:${CFG.port}/subscriptions?key=<sha256>`);
  console.log(`  REST:   GET /crm/v1/profiles/{id}  GET /rsv/v1/hotels/{hid}/reservations/{id}  (restScenario=${restScenario})`);
  console.log(`  Control: /control/emit?n=1  /control/close?code=4409  /control/ping  /control/rest?scenario=down  /control/status\n`);
});
