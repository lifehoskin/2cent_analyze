// Feed tests against a fake socket: subscription, framing, chunking, and the
// reconnect path that decides whether the coverage denominator is honest.
//   node src/pm/ws.test.mjs
import assert from 'node:assert/strict';
import { BookFeed } from './ws.mjs';
import { MarketCapture } from './capture.mjs';

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Minimal stand-in for the browser WebSocket the feed expects. */
class FakeSocket {
  static instances = [];
  sent = [];
  closed = false;

  constructor(url) {
    this.url = url;
    this.listeners = new Map();
    FakeSocket.instances.push(this);
  }

  addEventListener(type, handler) {
    this.listeners.set(type, handler);
  }

  emit(type, event = {}) {
    this.listeners.get(type)?.(event);
  }

  send(data) {
    if (this.closed) throw new Error('socket is closed');
    this.sent.push(data);
  }

  close() {
    this.closed = true;
  }
}

function makeFeed(overrides = {}) {
  FakeSocket.instances = [];
  const messages = [];
  const gaps = [];
  const feed = new BookFeed({
    onMessage: (m) => messages.push(m),
    onGap: (g) => gaps.push(g),
    WebSocketImpl: FakeSocket,
    reconnectMinMs: 5,
    reconnectMaxMs: 40,
    keepaliveSeconds: 10,
    ...overrides,
  });
  return { feed, messages, gaps };
}

// --- subscription and framing ----------------------------------------------
const { feed, messages, gaps } = makeFeed();
assert.equal(feed.setAssets(['a', 'b']), true, 'a new set connects');
assert.equal(feed.connectionCount, 1);

const socket = FakeSocket.instances[0];
socket.emit('open');
assert.deepEqual(JSON.parse(socket.sent[0]), { assets_ids: ['a', 'b'], type: 'market' },
  'the subscription names every asset');

// The first frame is an array of snapshots, later frames are single objects.
socket.emit('message', { data: JSON.stringify([{ event_type: 'book', asset_id: 'a' }, { event_type: 'book', asset_id: 'b' }]) });
assert.equal(messages.length, 2, 'an array frame yields one message per element');
socket.emit('message', { data: JSON.stringify({ event_type: 'price_change', market: 'c1' }) });
assert.equal(messages.length, 3);
socket.emit('message', { data: 'PONG' });
assert.equal(messages.length, 3, 'keepalive traffic is not a book message');
socket.emit('message', { data: JSON.stringify({ no: 'event type' }) });
assert.equal(messages.length, 3, 'frames without an event_type are ignored');

// --- unchanged sets must not churn the sockets ------------------------------
assert.equal(feed.setAssets(['b', 'a']), false, 'the same set in another order is a no-op');
assert.equal(FakeSocket.instances.length, 1, 'no reconnect for an unchanged set');
assert.equal(feed.setAssets(['a', 'b', 'c']), true, 'a genuine change updates the live socket');
assert.equal(FakeSocket.instances.length, 1);
assert.equal(socket.closed, false, 'existing books stay connected');
assert.deepEqual(JSON.parse(socket.sent.at(-1)), {assets_ids:['c'], operation:'subscribe'});
feed.setAssets(['a', 'c']);
assert.deepEqual(JSON.parse(socket.sent.at(-1)), {assets_ids:['b'], operation:'unsubscribe'});
const beforeLate = messages.length;
socket.emit('message', {data:JSON.stringify({event_type:'book',asset_id:'b'})});
assert.equal(messages.length, beforeLate, 'late removed-token snapshots do not resurrect books');
feed.stop();

// --- chunking ---------------------------------------------------------------
const many = makeFeed({ assetsPerConnection: 2 });
many.feed.setAssets(['a', 'b', 'c', 'd', 'e']);
assert.equal(many.feed.connectionCount, 3, '5 assets over 3 sockets at 2 each');
assert.equal(FakeSocket.instances.length, 3);
FakeSocket.instances.forEach((s) => s.emit('open'));
const subscribed = FakeSocket.instances.flatMap((s) => JSON.parse(s.sent[0]).assets_ids);
assert.deepEqual(subscribed.sort(), ['a', 'b', 'c', 'd', 'e'], 'every asset lands on exactly one socket');
many.feed.setAssets(['a','b','c','d','e','f']);
assert.equal(FakeSocket.instances.length, 3, 'adding fills spare capacity without reconnects');
assert.equal(FakeSocket.instances[0].sent.length, 1, 'unrelated full socket untouched');
many.feed.setAssets(['a','c','d','e','f','g']);
assert.deepEqual(JSON.parse(FakeSocket.instances[0].sent.at(-1)), {assets_ids:['g'],operation:'subscribe'});
many.feed.stop();

// --- reconnect and the gap record -------------------------------------------
const dropped = makeFeed();
dropped.feed.setAssets(['a']);
const first = FakeSocket.instances[0];
first.emit('open');
assert.equal(dropped.gaps.length, 0, 'a clean first connect is not a gap');

first.emit('close');
await delay(30);
assert.ok(FakeSocket.instances.length >= 2, 'the feed reconnects on its own');
const second = FakeSocket.instances[1];
second.emit('open');
assert.equal(dropped.gaps.length, 1, 'the outage is recorded');
const [gap] = dropped.gaps;
assert.equal(gap.reason, 'reconnect');
assert.equal(gap.assets, 1);
assert.ok(gap.durationMs >= 5, `gap is timed, got ${gap.durationMs}ms`);
assert.ok(Date.parse(gap.endedAt) >= Date.parse(gap.startedAt), 'the window is ordered');
assert.deepEqual(JSON.parse(second.sent[0]).assets_ids, ['a'], 'the reconnect resubscribes');

// Backoff grows rather than hammering the endpoint.
const before = FakeSocket.instances.length;
second.emit('close');
await delay(8);
second.emit('close');
await delay(8);
assert.ok(FakeSocket.instances.length - before <= 2, 'repeated drops back off, not spin');

dropped.feed.stop();
const afterStop = FakeSocket.instances.length;
await delay(60);
assert.equal(FakeSocket.instances.length, afterStop, 'a stopped feed does not reconnect');

console.log('all feed tests passed');

// Complete frames and stream invalidation; late events from retired sockets
// must not clear a replacement book or schedule another reconnect.
const resets = [];
const batches = [];
const raw = makeFeed({ onBatch: (batch, meta) => batches.push([batch, meta]),
  onReset: (assets, reason) => resets.push([assets, reason]) });
raw.feed.setAssets(['a', 'b']);
const old = FakeSocket.instances[0];
old.emit('open');
old.emit('message', { data: JSON.stringify([
  { event_type: 'book', asset_id: 'a' }, { event_type: 'book', asset_id: 'b' },
]) });
assert.equal(batches.length, 1);
assert.equal(batches[0][0].length, 2);
assert.ok(Number.isFinite(batches[0][1].monotonicMs));
assert.ok(Number.isFinite(Date.parse(batches[0][1].receivedAt)));
old.emit('error');
assert.equal(resets.at(-1)[1], 'error');
assert.equal(old.closed, true);
await delay(30);
const replacement = FakeSocket.instances[1];
replacement.emit('open');
const resetCount = resets.length;
old.emit('close');
old.emit('message', { data: JSON.stringify({ event_type: 'book', asset_id: 'a' }) });
assert.equal(resets.length, resetCount);
assert.equal(batches.length, 1);
raw.feed.stop();
assert.equal(resets.at(-1)[1], 'unsubscribe');

const changing = makeFeed();
changing.feed.setAssets(['a']);
changing.feed.setAssets(['a','b']);
const pending = FakeSocket.instances[0];
pending.emit('open');
assert.deepEqual(JSON.parse(pending.sent[0]).assets_ids, ['a','b']);
pending.emit('error');
changing.feed.setAssets(['b','c']);
await delay(30);
const reconnected = FakeSocket.instances[1];
reconnected.emit('open');
assert.deepEqual(JSON.parse(reconnected.sent[0]).assets_ids, ['b','c'], 'reconnect uses latest membership');
changing.feed.stop();
assert.equal(changing.feed.setAssets(['b','c']), true, 'feed can restart after stop');
changing.feed.stop();

// Recorded failure: one multi-token delta is delivered on both subscriptions.
// A slow copy must not roll the faster token back and create a fake cancel/replace.
const ownedBooks = new Map();
const rawJournal = [];
const ownerCapture = new MarketCapture({ books: ownedBooks,
  store: { add: (table, row) => rawJournal.push({table, row}) } });
const split = makeFeed({ assetsPerConnection: 1,
  onBatch: (batch, meta) => ownerCapture.processBatch(batch, meta),
  onReset: (assets, reason, meta) => ownerCapture.reset(assets, reason, meta) });
split.feed.setAssets(['a','b']);
const [sa,sb] = FakeSocket.instances;
sa.emit('open'); sb.emit('open');
const send = (socket, message) => socket.emit('message', {data: JSON.stringify(message)});
for (const [asset,socket] of [['a',sa],['b',sb]]) send(socket,
  {event_type:'book',asset_id:asset,timestamp:'1000',bids:[],asks:[]});
const change = (timestamp, size) => ({event_type:'price_change',market:'m',timestamp,
  price_changes:[{asset_id:'a',side:'BUY',price:'.02',size},
                 {asset_id:'b',side:'SELL',price:'.98',size}]});
const oldDelta = change('2000','1000');
const newDelta = change('3000','0');
send(sa,oldDelta); send(sa,newDelta); send(sb,oldDelta);
assert.equal(ownedBooks.get('a').sizeAt(.02),0,'foreign delayed copy cannot restore a removed bid');
assert.equal(ownedBooks.get('a').lastUpdate,3000,'foreign copy cannot rewind source time');
assert.equal(ownedBooks.get('b').askSizeAt(.98),1000,'owned slower leg still applies in its own order');
send(sb,newDelta);
assert.equal(ownedBooks.get('b').askSizeAt(.98),0);
const frames = rawJournal.filter(r=>r.table==='market_events' && r.row[5]==='frame');
assert.deepEqual(JSON.parse(frames.at(-2).row[6]),[oldDelta],'raw duplicate is retained intact for audit');
const aQuotes = rawJournal.filter(r=>r.table==='quote_observations' && r.row[3]==='a');
assert.equal(aQuotes.length,3,'a has initial, add and remove only');
send(sa,change('4000','1000')); send(sa,change('4000','0'));
assert.equal(ownedBooks.get('a').sizeAt(.02),0,'distinct same-millisecond updates are not deduplicated');
ownerCapture.checkpoint();
const checkpoint = JSON.parse(rawJournal.filter(r=>r.table==='market_events' && r.row[5]==='checkpoint').at(-1).row[6]);
assert.ok(checkpoint.every(b=>b.source_connection_id),'daily bootstrap records per-token source connection');
split.feed.setAssets(['b']);
send(sb,change('5000','1000'));
assert.equal(ownedBooks.has('a'),false,'foreign delta cannot resurrect unsubscribed token');
split.feed.stop();
