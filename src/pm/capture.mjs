// Public, anonymous market data only. Level changes are not wallet orders.
import { randomUUID } from 'node:crypto';
import { BookState, SweepDetector } from './book.mjs';

export const WATCH_PRICES = [0.01, 0.011, 0.02, 0.03, 0.05, 0.25, 0.4, 0.5, 0.7, 0.81, 0.85, 0.9];

/** Journal complete messages, then update every asset in a batch before callbacks. */
export class MarketCapture {
  constructor({ store, books, pairOf = () => null, onBook = () => {},
    onForget = () => {}, depthAbovePrice = 0.05, enabled = true,
    prices = WATCH_PRICES, sessionId = randomUUID() }) {
    Object.assign(this, { store, books, pairOf, onBook, onForget,
      depthAbovePrice, enabled, prices, sessionId });
    this.seq = 0;
    this.sourceConnections = new Map();
  }

  write(type, payload, meta = {}) {
    this.store.prepare?.();
    const seq = ++this.seq;
    if (this.enabled) this.store.add('market_events', [this.sessionId, seq,
      meta.receivedAt ?? new Date().toISOString(), meta.monotonicMs ?? performance.now(),
      meta.connectionId ?? null, type, JSON.stringify(payload)]);
    return seq;
  }

  reset(assets, reason, meta = {}) {
    this.write('stream_reset', { assets, reason }, meta);
    for (const asset of assets) {
      this.books.delete(asset);
      this.sourceConnections.delete(asset);
      this.onForget(asset);
    }
  }

  processBatch(messages, meta = {}) {
    const receivedAt = meta.receivedAt ?? new Date().toISOString();
    const context = { ...meta, receivedAt };
    const seq = this.write('frame', messages, context);
    // Keep the complete raw batch above. Apply each token only from the
    // connection that owns its subscription, preserving that socket's order.
    // The other outcome can be subscribed on a different socket and cause the
    // identical multi-token price_change to arrive again after newer updates.
    const accepts = asset => !meta.ownedAssets || meta.ownedAssets.has(asset);
    const before = new Map();
    const triggers = new Map();
    const initial = new Set();
    const remember = (book, trigger) => {
      if (!before.has(book.assetId)) {
        before.set(book.assetId, SweepDetector.snapshot(book, this.depthAbovePrice));
      }
      // A complete snapshot can resynchronise a book: do not call it a delta.
      if (triggers.get(book.assetId) !== 'book') triggers.set(book.assetId, trigger);
    };
    for (const message of messages) {
      if (message.event_type === 'book') {
        if (!accepts(message.asset_id)) continue;
        let book = this.books.get(message.asset_id);
        if (!book) {
          initial.add(message.asset_id);
          book = new BookState(message.asset_id, { conditionId: message.market });
          this.books.set(message.asset_id, book);
        }
        remember(book, 'book');
        book.applyBook(message);
        if (meta.connectionId) this.sourceConnections.set(message.asset_id, meta.connectionId);
      } else if (message.event_type === 'price_change') {
        for (const entry of message.price_changes ?? []) {
          if (!accepts(entry.asset_id)) continue;
          const book = this.books.get(entry.asset_id);
          // Deltas received before a full snapshot remain in the raw journal,
          // but cannot establish a complete book after a gap.
          if (!book) continue;
          remember(book, 'change');
          book.applyPriceChange(entry, message.timestamp);
        }
      } else if (message.event_type === 'tick_size_change') {
        if (!accepts(message.asset_id)) continue;
        const book = this.books.get(message.asset_id);
        const tick = Number(message.new_tick_size);
        if (book && tick > 0) book.tickSize = tick;
      }
      // last_trade_price and lifecycle messages are retained unchanged above.
      // A print alone must never mutate resting size or certify a cancellation.
    }
    for (const [asset, previous] of before) {
      const book = this.books.get(asset);
      const trigger = triggers.get(asset);
      this.observe(book, seq, receivedAt, initial.has(asset) ? 'initial' : trigger);
      this.onBook(book, initial.has(asset) ? null : previous, trigger, receivedAt);
    }
    return seq;
  }

  observe(book, seq, ts, trigger) {
    if (!this.enabled) return;
    const paired = this.books.get(this.pairOf(book.assetId));
    const bids = [...book.bidSizes()];
    const levels = this.prices.map(price => [price, book.sizeAt(price), book.askSizeAt(price),
      bids.reduce((n, [p, q]) => n + (p > price ? q : 0), 0),
      bids.reduce((n, [p, q]) => n + (p > price ? p * q : 0), 0)]);
    const crossed = b => b && b.bestBid !== null && b.bestAsk !== null
      ? Number(b.bestBid > b.bestAsk) : null;
    this.store.add('quote_observations', [this.sessionId, seq, ts, book.assetId,
      book.conditionId, trigger, book.bestBid, book.bestAsk,
      paired?.bestBid ?? null, paired?.bestAsk ?? null,
      book.lastUpdate || null, paired?.lastUpdate || null,
      crossed(book), crossed(paired), JSON.stringify(levels)]);
  }

  /** Replay anchor for every UTC file and at regular intervals; no new market clock. */
  checkpoint(meta = {}) {
    if (!this.enabled || !this.books.size) return;
    const ts = meta.receivedAt ?? new Date().toISOString();
    const seq = this.write('checkpoint', [...this.books.values()].map(b => ({ ...b.snapshot(),
      ...(this.sourceConnections.has(b.assetId) ? { source_connection_id: this.sourceConnections.get(b.assetId) } : {}),
    })),
      { ...meta, receivedAt: ts });
    for (const book of this.books.values()) this.observe(book, seq, ts, 'checkpoint');
  }
}
