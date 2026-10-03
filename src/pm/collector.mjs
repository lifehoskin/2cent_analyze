#!/usr/bin/env node
// Polymarket collector: discovery (A) and the book logger (B) in one process.
//
//   node src/pm/collector.mjs
//   node src/pm/collector.mjs --once     one discovery round, then exit
//
// Read-only throughout: no keys, no signing, no orders. Under the agent proxy,
// Node's fetch needs NODE_USE_ENV_PROXY=1; a normal machine needs nothing.
//
// Runs apart from the gg.bet collector on purpose. That one drives a browser
// that crashes and restarts; a hole in this feed is a hole in the fill-rate
// denominator, so the two must not share a fate.

import { loadConfig } from './config.mjs';
import { fetchActiveMarkets, MarketRegistry } from './gamma.mjs';
import { MarketSchedule } from './schedule.mjs';
import { FollowupTracker, sweepIdOf } from './followups.mjs';
import { FillContextQueue, contextRow } from './fill-context.mjs';
import { SweepDetector, bookSum, internalDislocation, pairedView } from './book.mjs';
import { BookFeed } from './ws.mjs';
import { MarketCapture } from './capture.mjs';
import { Store, restoreSchedulableMarkets, walletDisciplineShares } from './store.mjs';
import { GgbetTail } from './ggbet-tail.mjs';
import { MappingTable } from './mapping.mjs';
import { buildLinks, joinedRow, quoteFor } from './link.mjs';
import { fetchActivity } from './wallets.mjs';
import { fetchTrades } from './trades.mjs';
import { dueTradeMarkets } from './trade-poll.mjs';
import { UniverseJournal, universeRow } from './universe.mjs';
import { fetchResolution } from './resolve.mjs';
import { politeFetch, throttle } from './http.mjs';

const config = loadConfig();
const once = process.argv.includes('--once');

// A new day opens an empty database, so whatever we are already tracking has
// to be written into it before the next discovery round, or the book rows in
// between join against nothing.
const store = new Store(config.storage.dir, {
  onRotate: (day) => {
    for (const entry of schedule.live()) store.upsertMarket(entry.record, undefined, entry);
    capture.checkpoint();
    console.error(`[store] rolled over to ${day}, re-registered ${schedule.liveSize} markets`);
  },
});
const registry = new MarketRegistry();
// Discovery finds markets 11-15 hours before their game; the schedule decides
// when to watch them. Falling out of Gamma's page is not a reason to stop.
const schedule = new MarketSchedule(config.schedule);
const followups = new FollowupTracker(config.followups);
// Fills wait for their horizons before being written up; see fill-context.mjs.
const fillContext = new FillContextQueue(config.fillContext);
const universe = new UniverseJournal();
const detector = new SweepDetector(config.sweep);
const books = new Map();
const tail = new GgbetTail(config.ggbet.oddsHistory);
const mapping = new MappingTable(config.ggbet.mapping);
let links = new Map();
let ggbetByKey = new Map();
const counts = { rows: 0, sweeps: 0, gaps: 0, messages: 0, joined: 0, fills: 0, trades: 0,
  truncated: 0, takerFills: 0, followups: 0, observed: 0, resolved: 0, walletMarkets: 0,
  contexts: 0, unwatchableFills: 0 };
/**
 * condition ids already offered to the schedule from wallet activity, and
 * whether there was anything left to watch when we first heard of them.
 *
 * The value is the honest denominator for the fill context. A wallet's history
 * reaches back over markets that settled long before this process started, and
 * a fill in one of those cannot have a book around it — nothing was watching,
 * and nothing could have been.
 */
const walletMarkets = new Map();
/** Discipline shares of wallet activity, refreshed daily; empty means no preference. */
let quota = new Map();
/** condition id -> when its trade log was last pulled, to keep polling bounded. */
const tradesPolledAt = new Map();
const tradeMarkets = new Map();
let tradesPolling = false;
const scheduleDecisions = new Map();

const now = () => new Date().toISOString();

/** Record the book, and whatever the update did to it. */
function record(book, before, trigger, ts = now()) {
  const at = Date.parse(ts);
  const link = links.get(book.assetId);
  const ggbet = link ? ggbetByKey.get(link.ggbetKey) : null;

  // The other outcome token of this market. Both are subscribed and both books
  // are in this map already, so the fair value of one is readable off the
  // other: P(a) = 1 - P(b). No request, no outside feed, one lookup.
  const pairedId = schedule.pairOf(book.assetId);
  const paired = pairedId ? pairedView(books.get(pairedId), at) : null;

  const sweep = before && detector.observe(book, before, ts);
  if (sweep) {
    const bid = book.bestBid;
    const ask = book.bestAsk;
    const mid = bid !== null && ask !== null ? (bid + ask) / 2 : null;
    // Priced here, from the resident cache, rather than looked up afterwards.
    const quote = link ? quoteFor(link, ggbet, mid, at)
      : { fair: null, ratio: null, secondsSinceQuote: null, state: null };
    const sweepId = sweepIdOf(book.assetId, ts);
    store.add('sweeps', [
      ...sweep, sweepId, quote.fair, quote.ratio, quote.secondsSinceQuote, quote.state,
      // The dislocation is measured against the bid the sweep left behind.
      paired?.assetId ?? pairedId, paired?.bid ?? null, paired?.ask ?? null,
      paired?.askSize ?? null, paired?.lower ?? null, paired?.upper ?? null,
      paired?.mid ?? null, bookSum(sweep[5], paired?.bid ?? null),
      internalDislocation(paired?.lower ?? null, sweep[5]),
      paired?.staleSeconds ?? null,
    ]);
    followups.open(sweepId, book.assetId, book.conditionId, at, sweep[5], book.lastUpdate);
    counts.sweeps++;
  }
  store.add('book', book.metrics(ts, trigger, paired));
  counts.rows++;
  // Every snapshot feeds the horizons still open on this asset.
  followups.observe(book.assetId, book.bestBid, book.lastUpdate);

  // The self-check: this book was seen while the match was being played.
  if (book.conditionId && schedule.noteObserved(book.conditionId, at)) {
    store.markObserved(book.conditionId);
    counts.observed++;
  }

  if (ggbet) {
    store.add('joined', joinedRow(ts, link, book, ggbet));
    counts.joined++;
  }
}

/** Write whichever follow-up horizons have come due. */
function drainFollowups() {
  for (const row of followups.due()) {
    store.add('sweep_followups', row);
    counts.followups++;
  }
}

/**
 * Write up the fills whose horizons have passed.
 *
 * Reads the snapshots already stored rather than holding them in memory: at a
 * thousand watched tokens a twenty-minute buffer is hundreds of megabytes, and
 * it would be lost on every restart.
 */
function drainFillContext(now = Date.now(), jobs = null) {
  for (const job of jobs ?? fillContext.due(now)) {
    try {
      store.add('fill_context', contextRow(job, store, now));
      counts.contexts++;
    } catch (err) {
      console.error(`[fills] ${job.assetId.slice(0, 10)} at ${job.ts}: ${err.message}`);
    }
  }
}

/** Refresh the gg.bet side and the token-to-quote links it feeds. */
function relink() {
  tail.poll();
  const ggbetMarkets = tail.markets();
  ggbetByKey = new Map(ggbetMarkets.map((m) => [`${m.matchId}|${m.market}`, m]));
  links = buildLinks({
    markets: schedule.live().map((entry) => entry.record),
    ggbetMarkets,
    mapping,
    windowHours: config.ggbet.matchWindowHours,
  });
  mapping.save();
  return links.size;
}

const capture = new MarketCapture({ store, books,
  pairOf: asset => schedule.pairOf(asset), onBook: record,
  onForget: asset => detector.forget(asset), depthAbovePrice: config.sweep.depthAbovePrice,
  enabled: config.capture.enabled,
});
capture.write('capture_start', { version: 2, book_apply_policy: 'connection_owned_assets', wallets: config.wallets.addresses,
  watch_prices: capture.prices, config });

const feed = new BookFeed({
  ...config.book,
  onStatus: (text) => console.error(`[feed] ${text}`),
  onGap: (gap) => {
    counts.gaps++;
    console.error(`[feed] gap ${Math.round(gap.durationMs / 1000)}s over ${gap.assets} assets`);
    store.add('gaps', [gap.startedAt, gap.endedAt, gap.durationMs, gap.reason, gap.assets]);
  },
  onReset: (assets, reason, meta) => capture.reset(assets, reason, meta),
  onBatch: (messages, meta) => {
    counts.messages += messages.length;
    capture.processBatch(messages, meta);
  },
});

/** A collapse takes a second, but coverage has to be provable minute by minute. */
function heartbeat() {
  for (const book of books.values()) record(book, null, 'heartbeat');
}

/** Target-wallet fills, the only labelled examples available. */
let walletsPolling = false;
async function pollWallets() {
  if (walletsPolling) return;
  walletsPolling = true;
  try {
    await pollWalletsOnce();
  } catch (err) {
    console.error(`[wallets] ${err.message}`);
  } finally {
    walletsPolling = false;
  }
}

async function pollWalletsOnce() {
  // condition id -> when the wallet last touched it, so a market it traded in
  // months ago is not taken up as if it were live.
  const markets = new Map();
  for (const address of config.wallets.addresses ?? []) {
    try {
      const { rows, ceiling, types } = await fetchActivity(address);
      for (const row of rows) {
        store.add('wallets', row);
        if (!row[6]) continue;
        const at = Date.parse(row[0]);
        if (Number.isFinite(at)) markets.set(row[6], Math.max(markets.get(row[6]) ?? 0, at));
      }
      counts.fills += rows.length;
      if (ceiling) {
        console.error(`[wallets] ${address.slice(0, 10)}: hit the pagination ceiling,` +
          ` history is partial (${JSON.stringify(types)})`);
      }
    } catch (err) {
      console.error(`[wallets] ${address.slice(0, 10)}: ${err.message}`);
    }
  }
  await followWallets(markets);
  for (const [id,at] of markets) tradeMarkets.set(id, Math.max(tradeMarkets.get(id) ?? 0,at));
}

/**
 * Watch every market a studied wallet is in, whatever discipline it is.
 *
 * Discovery scans the disciplines the study is about; the wallets also trade
 * baseball and basketball, and inside one window they made 113 fills across 17
 * markets of which 2 were being watched. A market is looked up once, through
 * CLOB — the id is in the path, so it cannot answer about a different market —
 * and then held until it resolves.
 */
async function followWallets(lastTraded) {
  const naming = { ...config.labels, ...config.disciplines };
  // The activity feed reaches back thousands of records. Following every market
  // in it would subscribe to whatever the wallet held months ago — a live run
  // turned up "Will Bitcoin reach $1,000,000 by December 31" — and hold each
  // one until it resolves. Only what the wallet has touched recently is live
  // enough to be what the study is about.
  const window = (config.wallets?.followHours ?? 48) * 3600 * 1000;
  const now = Date.now();
  for (const [conditionId, at] of lastTraded) {
    if (now - at > window) continue;
    if (walletMarkets.has(conditionId)) continue;
    if (schedule.entry(conditionId)?.source === 'wallet') {
      walletMarkets.set(conditionId, true);
      continue;
    }
    try {
      await resolutionGate();
      const { closed, record } = await fetchResolution(conditionId, naming,
        (url) => politeFetch(url));
      const watchable = !closed && record.tokens?.length === 2;
      walletMarkets.set(conditionId, watchable);
      if (!watchable) continue;
      const { scheduled } = schedule.observe([record], Date.now(), { priority: true });
      for (const row of universe.observe([record], 'wallet:activity')) {
        store.add('universe', universeRow(row));
      }
      for (const entry of scheduled) {
        store.upsertMarket(entry.record, undefined, entry);
        counts.walletMarkets++;
        console.error(`[wallet] watching ${record.sport ?? 'unlabelled'}` +
          ` ${conditionId.slice(0, 12)} — ${String(record.question).slice(0, 48)}`);
      }
    } catch (err) {
      // Left out of the seen set on purpose, so the next round tries again.
      console.error(`[wallet] ${conditionId.slice(0, 12)}: ${err.message}`);
    }
  }
}

/**
 * Pull the trade log, with roles, for markets the target wallets touched.
 *
 * Only the wallets' own fills are stored. The log carries every participant's
 * trades, and a busy market holds tens of thousands of them: a first pass over
 * a fully paginated activity history wrote 214k rows and 180 MB before this was
 * scoped. All that volume exists to establish the role of a handful of fills,
 * so the market-wide figures are kept as one summary row per scan and the rest
 * is dropped.
 *
 * Markets per cycle are capped, so one round stays bounded however long the
 * wallet has been trading.
 */
async function pollTrades() {
  if (tradesPolling) return;
  tradesPolling = true;
  try { await pollTradesOnce(); }
  catch (err) { console.error(`[trades] ${err.message}`); }
  finally { tradesPolling = false; }
}

async function pollTradesOnce() {
  const every = (config.trades?.intervalSeconds ?? 120) * 1000;
  const perCycle = config.trades?.marketsPerCycle ?? 25;
  const targets = new Set((config.wallets.addresses ?? []).map((a) => a.toLowerCase()));

  for (const conditionId of dueTradeMarkets(tradeMarkets, tradesPolledAt, {every,limit:perCycle})) {
    tradesPolledAt.set(conditionId, Date.now());
    try {
      const { rows, truncated, takerShare, pages } = await fetchTrades(conditionId);
      const mine = rows.filter((row) => targets.has(row[3]));
      for (const row of mine) {
        store.add('trades', row);
        // The book around this fill is what the collection is for. It cannot be
        // read yet — the minutes after it have not happened — so it is queued.
        //
        // Unless the market was already closed when the follower first reached
        // it. Then there is no book to queue for and never was: the row would
        // be nulls with snapshot_available = 0, true and useless. 1873 such
        // rows out of 1883 buried the 10 that could be answered and read as a
        // subscription that catches half a percent of fills, when in fact it
        // caught every one it could.
        if (walletMarkets.get(row[1]) === false) {
          counts.unwatchableFills++;
          continue;
        }
        fillContext.add({
          ts: row[0], conditionId: row[1], assetId: row[2], wallet: row[3],
          side: row[4], price: row[5], size: row[6],
        }, Date.now());
      }
      counts.trades += mine.length;
      counts.takerFills += mine.filter((row) => row[7] === 'taker').length;
      // Worth keeping even though the market's own trades are not.
      store.add('trade_scans', [new Date().toISOString(), conditionId, rows.length,
        mine.length, takerShare, pages, truncated ? 1 : 0]);
      if (truncated) {
        counts.truncated++;
        console.error(`[trades] ${conditionId.slice(0, 12)}: log truncated, counts from it are short`);
      }
    } catch (err) {
      console.error(`[trades] ${conditionId.slice(0, 12)}: ${err.message}`);
    }
  }
}

/**
 * Discovery adds to the schedule and never takes away.
 *
 * A market leaving Gamma's page says nothing about the match: the page is the
 * newest 1200 markets by creation, so everything falls out of it within the
 * hour whether or not it has been played.
 */
async function discover() {
  const raw = await fetchActiveMarkets(config.gamma);
  const { tracked, all } = registry.update(raw, config.disciplines);
  const { scheduled, skipped } = schedule.observe(tracked);

  // A market that classifies fine but cannot be dated is passed over for that
  // reason, and the journal has to say so rather than claim it was subscribed.
  const undatable = new Map(skipped.map((s) => [s.conditionId, s.reason]));
  for (const row of universe.observe(all, 'gamma:startDate',
    (record) => undatable.get(record.conditionId) ?? null)) {
    store.add('universe', universeRow(row));
  }
  for (const entry of scheduled) store.upsertMarket(entry.record, undefined, entry);

  // Logged on a change only: undatable markets recur every round and the
  // journal already holds them, so repeating the count would be noise.
  if (scheduled.length) {
    console.error(`[discovery] ${tracked.length} trackable, +${scheduled.length} scheduled` +
      `${undatable.size ? `, ${undatable.size} undatable` : ''}` +
      ` | live ${schedule.liveSize}, waiting ${schedule.pendingSize}`);
  }
  return tracked;
}

/**
 * Move the subscription window. Runs on a short timer rather than on discovery,
 * because a match starts on its own clock, not on Gamma's.
 */
function tick(at = Date.now()) {
  const { added, removed } = schedule.refresh(at, {
    maxLive: config.schedule?.maxLiveMarkets ?? Infinity,
    maxLivePerSport: config.schedule?.maxLivePerSport ?? Infinity,
    quota,
  });
  for (const decision of schedule.decisions(at)) {
    const serialized = JSON.stringify(decision);
    if (scheduleDecisions.get(decision.conditionId) === serialized) continue;
    scheduleDecisions.set(decision.conditionId, serialized);
    capture.write('schedule_decision', decision, {receivedAt:new Date(at).toISOString()});
    for (const row of universe.observe([schedule.entry(decision.conditionId).record], 'schedule:restored')) {
      store.add('universe', universeRow(row));
    }
    const row = universe.decision(decision.conditionId, decision.status,
      decision.releasedAt ?? new Date(at).toISOString());
    if (row) store.add('universe', universeRow(row));
  }
  for (const entry of removed) {
    const released = universe.release(entry.conditionId);
    if (released) store.add('universe', universeRow(released));
    store.upsertMarket(entry.record, undefined, entry);
    for (const assetId of entry.record.tokens) {
      // Horizons still open are written with what was seen, not dropped.
      for (const row of followups.forget(assetId, at)) {
        store.add('sweep_followups', row);
        counts.followups++;
      }
      books.delete(assetId);
      detector.forget(assetId);
    }
  }
  for (const entry of added) store.upsertMarket(entry.record, undefined, entry);
  if (added.length || removed.length) {
    feed.setAssets(schedule.assetIds());
    console.error(`[schedule] +${added.length} -${removed.length}` +
      ` | live ${schedule.liveSize} markets, ${schedule.assetIds().length} tokens,` +
      ` waiting ${schedule.pendingSize}`);
  }
  drainFollowups();
  drainFillContext(at);
}

const resolutionGate = throttle(350);

/**
 * Let go of markets that have resolved, before their hold window runs out.
 *
 * Gamma's condition_id filter is ignored and answers about other markets, so
 * this asks CLOB, where the id is part of the path. Only markets whose match
 * has had time to finish are asked about, and only every few minutes.
 */
async function checkResolutions() {
  const naming = { ...config.labels, ...config.disciplines };
  for (const entry of schedule.dueForResolutionCheck(Date.now(),
    config.schedule?.resolutionsPerCycle ?? 20, followups.openConditions())) {
    try {
      await resolutionGate();
      const { closed, rows } = await fetchResolution(entry.conditionId, naming,
        (url) => politeFetch(url));
      if (!closed) continue;
      schedule.markResolved(entry.conditionId);
      counts.resolved++;
      // A horizon that outlives the market is not missing: the token is worth
      // its payout and there is no book left for it to recover in.
      const payout = new Map(rows.map((row) => [row[1], Number(row[4])]));
      for (const row of followups.resolved(entry.conditionId,
        (assetId) => (payout.has(assetId) ? payout.get(assetId) : null))) {
        store.add('sweep_followups', row);
        counts.followups++;
      }
    } catch (err) {
      console.error(`[resolve] ${entry.conditionId.slice(0, 12)}: ${err.message}`);
    }
  }
}

// Pick the schedule back up before asking Gamma anything. A market is only in
// Gamma's page for about an hour after it is created, and its game is 11 to 15
// hours later, so without this a restart drops the rest of the day's matches
// and never sees them again.
quota = walletDisciplineShares(config.storage.dir);
if (quota.size) {
  console.error(`[schedule] wallet activity by discipline: ${[...quota]
    .sort((a, b) => b[1] - a[1]).slice(0, 5)
    .map(([sport, share]) => `${sport} ${(share * 100).toFixed(0)}%`).join(', ')}`);
}

const restored = restoreSchedulableMarkets(config.storage.dir, {
  lookbackHours: Math.max(...Object.values(config.schedule?.holdHours ?? { default: 6 })) + 1,
});
if (restored.length) {
  const { scheduled } = schedule.observe(restored);
  console.error(`[schedule] restored ${scheduled.length} markets from earlier runs`);
}

// A network that is down at startup — a closed laptop, a dropped connection —
// must not be fatal: the discovery interval below recovers on its own.
let tracked = [];
try {
  tracked = await discover();
} catch (err) {
  console.error(`[discovery] first round failed: ${err.message}` +
    ` — continuing, will retry every ${config.gamma.intervalSeconds}s`);
}
tick();
relink();
await pollWallets();
if (once) {
  await pollTrades();
  const segment = tracked.filter((r) => r.level === 'segment').length;
  console.log(`tracking ${tracked.length} markets (${segment} in-match), database ${store.path}`);
  console.log(`schedule: ${schedule.liveSize} live now, ${schedule.pendingSize} waiting for their game`);
  console.log(`gg.bet: ${tail.rowsRead} rows, ${tail.size} markets, ${links.size} tokens linked` +
    ` (${mapping.size} mappings, ${mapping.verifiedCount} verified)`);
  // --once exits without the shutdown handler, so anything still open is
  // written here too rather than lost.
  for (const entry of schedule.live()) {
    for (const assetId of entry.record.tokens) {
      for (const row of followups.forget(assetId)) store.add('sweep_followups', row);
    }
  }
  store.flush();
  console.log(`wallets: ${counts.fills} activity rows (${store.count('wallets')} stored)`);
  const roles = store.roleCounts();
  console.log(`trades: ${store.count('trades')} stored (${roles.maker ?? 0} maker, ${roles.taker ?? 0} taker)`);
  console.log(`universe: ${universe.size} markets seen, ${universe.subscribedCount} subscribed`);
  for (const [reason, count] of universe.skipCounts().slice(0, 6)) {
    console.log(`    ${String(count).padStart(5)}  ${reason}`);
  }
  store.close();
  process.exit(0);
}

void pollTrades();
const timers = [
  setInterval(() => discover().catch((err) => console.error(`[discovery] ${err.message}`)),
    config.gamma.intervalSeconds * 1000),
  // The window turns on the match clock, so it is checked far more often than
  // markets are discovered: a ten-minute lead is worth nothing if it is only
  // acted on every 45 seconds by luck.
  setInterval(() => tick(), (config.schedule?.tickSeconds ?? 10) * 1000),
  // Runs on the short interval; dueForResolutionCheck decides which markets are
  // actually asked about, so a sweep horizon is not outlived by the schedule.
  setInterval(() => checkResolutions().catch((err) => console.error(`[resolve] ${err.message}`)),
    (config.schedule?.resolutionCheckOpenHorizonSeconds ?? 60) * 1000),
  setInterval(heartbeat, config.book.heartbeatSeconds * 1000),
  setInterval(() => capture.checkpoint(), Math.max(5, config.capture.checkpointSeconds) * 1000),
  setInterval(relink, (config.ggbet.pollSeconds ?? 5) * 1000),
  setInterval(() => pollWallets(), (config.wallets.intervalSeconds ?? 25) * 1000),
  setInterval(() => pollTrades(), (config.trades?.intervalSeconds ?? 120) * 1000),
  // Where the wallets work shifts between days, not between minutes.
  setInterval(() => { quota = walletDisciplineShares(config.storage.dir); }, 24 * 3600 * 1000),
  setInterval(() => {
    console.error(`[status] ${books.size} books | ${counts.rows} rows | ${counts.joined} joined` +
      ` | ${counts.sweeps} sweeps (${counts.followups} horizons) | ${counts.gaps} gaps` +
      ` | ${links.size} linked | ${counts.trades} trades` +
      ` | journal ${capture.enabled ? capture.seq : 'off'}` +
      ` | live ${schedule.liveSize}/waiting ${schedule.pendingSize}` +
      ` | ${counts.observed} seen in play | ${counts.contexts} fills in context` +
      `${counts.unwatchableFills ? ` (${counts.unwatchableFills} fills in markets` +
        ` already closed when found)` : ''}` +
      ` | ${universe.subscribedCount}/${universe.size} universe` +
      `${counts.truncated ? ` | ${counts.truncated} truncated` : ''} | ${store.path}`);
  }, 60000),
];

function shutdown() {
  for (const timer of timers) clearInterval(timer);
  feed.stop();
  store.flush();
  mapping.save();
  // Whatever is still open is written before the process goes away, so a
  // restart loses the tail of a horizon rather than the whole measurement.
  for (const entry of schedule.live()) {
    for (const assetId of entry.record.tokens) {
      for (const row of followups.forget(assetId)) store.add('sweep_followups', row);
    }
  }
  // Written up with whatever the horizons managed to see, rather than dropped.
  drainFillContext(Date.now(), fillContext.drain());
  console.error(`\n[status] wrote ${counts.rows} book rows, ${counts.joined} joined,` +
    ` ${counts.sweeps} sweeps, ${counts.followups} horizons, ${counts.gaps} gaps`);
  store.close();
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
