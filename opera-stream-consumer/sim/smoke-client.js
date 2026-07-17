/*
 * Throwaway smoke client: proves the simulator implements the full graphql-transport-ws lifecycle
 * (OAuth -> handshake -> connection_init -> ack -> subscribe -> next). Not part of the template;
 * just validates the sim before the Mule app is pointed at it.
 */
'use strict';
const crypto = require('crypto');
const WebSocket = require('ws');
const PORT = process.env.PORT || 8081;
const base = `http://localhost:${PORT}`;

async function getToken() {
  const res = await fetch(`${base}/oauth/v1/tokens`, {
    method: 'POST',
    headers: {
      Authorization: 'Basic ' + Buffer.from('client:secret').toString('base64'),
      'x-app-key': 'my-app-key',
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: 'grant_type=client_credentials&scope=urn:opc:hgbu:ws:__myscopes__',
  });
  return res.json();
}

(async () => {
  const tok = await getToken();
  console.log('[client] token:', tok.access_token.slice(0, 20) + '…', 'expires_in', tok.expires_in);

  const keyHash = crypto.createHash('sha256').update('my-app-key').digest('hex');
  const ws = new WebSocket(`ws://localhost:${PORT}/subscriptions?key=${keyHash}`, 'graphql-transport-ws');

  ws.on('open', () => {
    console.log('[client] socket open, subprotocol =', ws.protocol);
    ws.send(JSON.stringify({ type: 'connection_init', payload: { Authorization: 'Bearer ' + tok.access_token, 'x-app-key': 'my-app-key' } }));
  });

  let events = 0;
  ws.on('message', (d) => {
    const f = JSON.parse(d.toString());
    console.log('[client] recv:', f.type, f.type === 'next' ? `(eventName=${f.payload.data.newEvent.eventName}, offset=${f.payload.data.newEvent.metadata.offset})` : '');
    if (f.type === 'connection_ack') {
      ws.send(JSON.stringify({ id: crypto.randomUUID(), type: 'subscribe', payload: { query: 'subscription { newEvent(input: { chainCode: "CHAIN01" }) { metadata { offset uniqueEventId } eventName } }' } }));
    }
    if (f.type === 'next' && ++events >= 2) {
      console.log('[client] got 2 events, closing.');
      ws.close(1000, 'done');
    }
  });
  ws.on('close', (c, r) => { console.log('[client] closed', c, r.toString()); process.exit(0); });
  ws.on('error', (e) => { console.error('[client] error', e.message); process.exit(1); });
})();
