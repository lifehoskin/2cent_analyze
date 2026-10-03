// Module B — the websocket feed.
//
// Every disconnect is a hole in the denominator: market-hours we thought we were
// watching but were not. So a drop is never silent — it is timed, counted and
// written to the `gaps` table, and the analyzer subtracts those windows instead
// of dividing by a coverage it never had.
//
// Change subscriptions in place. Unrelated market additions must never erase
// the books whose cancellation/replace cycles we are measuring.

const WS_URL = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';
let connectionSequence = 0;

/** One socket carrying one chunk of the asset list. */
class Connection {
  #ws = null;
  #timer = null;
  #keepalive = null;
  #attempt = 0;
  #closedAt = null;
  #stopped = false;
  #ready = false;

  constructor(feed, assets) {
    this.feed = feed;
    this.assets = assets;
    this.assetSet = new Set(assets);
    this.id = `connection-${++connectionSequence}`;
  }

  open() {
    const { WebSocketImpl, url } = this.feed;
    this.#ws = new WebSocketImpl(url);
    const socket = this.#ws;

    this.#ws.addEventListener('open', () => {
      if (this.#stopped || this.#ws !== socket || this.#timer) return;
      this.#ready = true;
      this.feed.onReset?.(this.assets, 'subscribe', { connectionId: this.id });
      this.#attempt = 0;
      if (this.#closedAt !== null) {
        this.feed.onGap({
          startedAt: new Date(this.#closedAt).toISOString(),
          endedAt: new Date().toISOString(),
          durationMs: Date.now() - this.#closedAt,
          reason: 'reconnect',
          assets: this.assets.length,
        });
        this.#closedAt = null;
      }
      this.#ws.send(JSON.stringify({ assets_ids: this.assets, type: 'market' }));
      // An idle socket gets dropped; the CLOB feed answers a plain PING.
      const every = this.feed.keepaliveSeconds * 1000;
      this.#keepalive = setInterval(() => {
        try {
          this.#ws.send('PING');
        } catch {
          // the close handler owns reconnection
        }
      }, every);
      this.#keepalive.unref?.();
    });

    this.#ws.addEventListener('message', (event) => {
      if (this.#stopped || this.#ws !== socket || this.#timer) return;
      const meta = { connectionId: this.id, receivedAt: new Date().toISOString(),
        monotonicMs: performance.now(), ownedAssets: this.assetSet };
      const data = typeof event.data === 'string' ? event.data : String(event.data);
      let parsed;
      try {
        parsed = JSON.parse(data);
      } catch {
        return; // PONG and other non-JSON keepalive traffic
      }
      // The first frame after subscribing is an array of snapshots; later frames
      // are single objects.
      const batch = (Array.isArray(parsed) ? parsed : [parsed]).filter(m => m?.event_type
        // A queued snapshot after unsubscribe must not resurrect a retired book.
        && (m.event_type !== 'book' || this.assetSet.has(m.asset_id)));
      if (this.feed.onBatch) this.feed.onBatch(batch, meta);
      else for (const message of batch) {
        // A market price_change can carry both tokens even when this socket
        // subscribes to only one. Never apply its foreign leg a second time.
        if (message.event_type === 'price_change' && Array.isArray(message.price_changes)) {
          const owned = message.price_changes.filter(x => this.assetSet.has(x.asset_id));
          if (owned.length) this.feed.onMessage?.({ ...message, price_changes: owned }, meta);
        } else if (!message.asset_id || this.assetSet.has(message.asset_id)) {
          this.feed.onMessage?.(message, meta);
        }
      }
    });

    this.#ws.addEventListener('close', () => {
      if (this.#ws === socket) this.#down('close');
    });
    this.#ws.addEventListener('error', () => {
      if (this.#ws === socket) this.#down('error');
    });
  }

  #down(reason) {
    this.#ready = false;
    clearInterval(this.#keepalive);
    if (this.#stopped || this.#timer) return;
    this.feed.onReset?.(this.assets, reason, { connectionId: this.id });
    this.#closedAt ??= Date.now();
    const { reconnectMinMs, reconnectMaxMs } = this.feed;
    const wait = Math.min(reconnectMaxMs, reconnectMinMs * 2 ** this.#attempt++);
    this.feed.onStatus?.(`socket ${reason}, reconnecting in ${Math.round(wait / 1000)}s`);
    this.#timer = setTimeout(() => {
      this.#timer = null;
      this.open();
    }, wait);
    this.#timer.unref?.();
    // An error need not emit close. Retire it before opening another socket.
    try { this.#ws?.close(); } catch { /* already closed */ }
  }

  setAssets(assets) {
    const previous = new Set(this.assets);
    const desired = new Set(assets);
    const removed = this.assets.filter(a => !desired.has(a));
    const added = assets.filter(a => !previous.has(a));
    this.assets = assets;
    this.assetSet = desired;
    if (removed.length) this.feed.onReset?.(removed, 'unsubscribe', { connectionId: this.id });
    // While connecting/reconnecting, open() will subscribe to the latest set.
    if (!this.#ready || this.#stopped) return;
    try {
      if (removed.length) this.#ws.send(JSON.stringify({ assets_ids: removed, operation: 'unsubscribe' }));
      if (added.length) {
        this.feed.onReset?.(added, 'subscribe', { connectionId: this.id });
        this.#ws.send(JSON.stringify({ assets_ids: added, operation: 'subscribe' }));
      }
    } catch {
      this.#down('subscription_error');
    }
  }

  close() {
    if (!this.#stopped) this.feed.onReset?.(this.assets, 'unsubscribe', { connectionId: this.id });
    this.#stopped = true;
    this.#ready = false;
    clearInterval(this.#keepalive);
    clearTimeout(this.#timer);
    try {
      this.#ws?.close();
    } catch {
      // already gone
    }
  }
}

/** Keeps the whole asset list subscribed across however many sockets it takes. */
export class BookFeed {
  #connections = [];
  #assets = [];

  constructor({
    onMessage, onBatch, onReset, onGap, onStatus, url = WS_URL, WebSocketImpl = globalThis.WebSocket,
    assetsPerConnection = 250, keepaliveSeconds = 10,
    reconnectMinMs = 1000, reconnectMaxMs = 60000,
  }) {
    Object.assign(this, {
      onMessage, onBatch, onReset, onGap, onStatus, url, WebSocketImpl,
      assetsPerConnection, keepaliveSeconds, reconnectMinMs, reconnectMaxMs,
    });
  }

  get assets() {
    return [...this.#assets];
  }

  get connectionCount() {
    return this.#connections.length;
  }

  /** Replace the subscription set. A no-op when the set is unchanged. */
  setAssets(assetIds) {
    const next = [...new Set(assetIds)].sort();
    if (next.join() === this.#assets.join()) return false;
    this.#assets = next;
    const desired = new Set(next);
    const existing = new Set(this.#connections.flatMap(c => c.assets));
    const additions = next.filter(a => !existing.has(a));
    const retained = [];
    for (const connection of this.#connections) {
      const members = connection.assets.filter(a => desired.has(a));
      members.push(...additions.splice(0, Math.max(0, this.assetsPerConnection - members.length)));
      if (members.length) {
        connection.setAssets(members);
        retained.push(connection);
      } else connection.close();
    }
    this.#connections = retained;
    for (let i = 0; i < additions.length; i += this.assetsPerConnection) {
      const connection = new Connection(this, additions.slice(i, i + this.assetsPerConnection));
      this.#connections.push(connection);
      connection.open();
    }
    return true;
  }

  stop() {
    for (const connection of this.#connections) connection.close();
    this.#connections = [];
    this.#assets = [];
  }
}
