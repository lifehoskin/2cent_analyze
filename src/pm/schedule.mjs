// Module A2 — when a market is worth watching.
//
// Discovery used to double as the subscription window: the registry held
// whatever the current Gamma page returned, so a market was watched from the
// moment it appeared until newer markets pushed it out of the first
// `maxPages * pageSize` rows sorted by startDate. That window is a property of
// Polymarket's market-creation rate, not of the match: measured against the live
// API it is about half an hour deep, and a real collection came out at a median
// of 61 minutes of observation per market, ending roughly a day before the game
// was played. Every book row landed before anyone was trading, and not one fill
// of the studied wallets fell inside a watched window.
//
// So the lifecycle moves here. Discovery only ever *adds* to the schedule —
// markets are seen 11-15 hours ahead of their game — and the clock decides when
// to subscribe and when to let go:
//
//   subscribe    now >= game_start - leadMinutes  (or at once, if that passed)
//   unsubscribe  the market resolved, or now > game_start + hold(discipline)
//
// A market in the window is never dropped for being quiet or for falling out of
// Gamma's page, which is what used to happen.
//
// `end_date` deliberately does not decide the window. It is a resolution
// deadline, not the end of play: measured on live data it runs game + 6h for
// CS2 and game + 168h for ITF tennis, so unsubscribing by it would hold a
// tennis book for a week after the match ended.

/** Hours after the game start a market is still worth watching, by gg.bet sportId. */
export const DEFAULT_HOLD_HOURS = {
  esports_counter_strike: 6,
  esports_dota_2: 8,
  esports_league_of_legends: 6,
  esports_valorant: 6,
  esports_rainbow_six: 6,
  esports_call_of_duty: 6,
  tennis: 5,
  table_tennis: 2,
};

/**
 * Parse the timestamps Polymarket mixes in one payload.
 *
 * `endDate` is ISO ("2026-09-08T07:30:00Z") but `gameStartTime` comes back as
 * "2026-09-01 07:30:00+00" — a space instead of the T and a two-digit offset,
 * which `Date.parse` reads as invalid in some runtimes and as local time in
 * others. Both failures are silent and would put the whole schedule hours off.
 *
 * @returns {number|null} epoch ms
 */
export function parseTimestamp(value) {
  if (value === null || value === undefined || value === '') return null;
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value.getTime();
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;

  let text = String(value).trim();
  if (!text) return null;
  text = text.replace(' ', 'T');
  // "+00" and "-0730" are both legal in Postgres output and neither is ISO.
  text = text.replace(/([+-]\d{2})$/, '$1:00').replace(/([+-]\d{2})(\d{2})$/, '$1:$2');
  // A bare timestamp with no zone is UTC here: every Polymarket field is.
  if (!/(Z|[+-]\d{2}:\d{2})$/.test(text)) text += 'Z';
  const at = Date.parse(text);
  return Number.isNaN(at) ? null : at;
}

const HOUR = 3600 * 1000;
const isMatchWinner = (entry) => entry.record.level === 'match' && entry.record.kind === 'winner';

/**
 * When play starts, and how we know.
 *
 * @returns {{at: number, source: string}|null}
 */
export function gameStartOf(record, { holdHours = DEFAULT_HOLD_HOURS, maxAheadHours = 72 } = {}, now = Date.now()) {
  const stated = parseTimestamp(record.gameStartTime);
  if (stated !== null) return { at: stated, source: 'game_start_time' };

  // Fallback: the resolution deadline minus a typical match. Only usable where
  // the deadline is close enough to be about this match at all — the tennis
  // template sets it a week out, and backing off five hours from that would
  // schedule the subscription six days after the match finished.
  const end = parseTimestamp(record.endDate);
  if (end === null) return null;
  const hold = holdHours[record.sport] ?? holdHours.default ?? 6;
  const at = end - hold * HOUR;
  if (at - now > maxAheadHours * HOUR) return null;
  return { at, source: 'end_date_minus_typical' };
}

/**
 * The set of markets under subscription, driven by the clock rather than by
 * whatever Gamma happened to return this round.
 */
export class MarketSchedule {
  /** condition id -> entry. Entries are kept after release, so a market is not rescheduled. */
  #entries = new Map();
  #live = new Set();
  #assets = new Map();
  /** asset id -> the other token of the same market. */
  #pairs = new Map();

  constructor(config = {}) {
    this.config = {
      leadMinutes: 10,
      holdHours: DEFAULT_HOLD_HOURS,
      maxAheadHours: 72,
      resolutionCheckMinutes: 15,
      resolutionCheckAfterMinutes: 45,
      resolutionCheckOpenHorizonSeconds: 60,
      ...config,
      holdHours: { ...DEFAULT_HOLD_HOURS, ...(config.holdHours ?? {}) },
    };
  }

  /**
   * Take what discovery classified as trackable and schedule it.
   *
   * Records already in the schedule are refreshed in place — Gamma restates
   * `gameStartTime` when a match is delayed — but a released market is never
   * revived, or a finished match would be resubscribed on every round.
   *
   * `priority` marks a market the studied wallets have actually traded in. Those
   * are watched from the moment they are noticed until they resolve, whatever
   * their discipline and whatever the clock says: the whole point of the
   * collection is the book around those fills, and a market found because a
   * wallet was already in it has no useful "before the match" to wait for.
   *
   * @returns {{scheduled: object[], skipped: {conditionId: string, reason: string}[]}}
   */
  observe(records, now = Date.now(), { priority = false } = {}) {
    const scheduled = [];
    const skipped = [];
    for (const record of records) {
      const known = this.#entries.get(record.conditionId);
      // A wallet trading in a market outranks anything the schedule decided
      // about it earlier. Discovery lets a market go when its match looks over
      // — most often because Gamma listed it hours after the game, which is
      // most of what a round returns — and without this the fill that follows
      // is recorded against a book nobody is watching. Only a resolution is
      // final: there is nothing left to watch after that.
      if (known?.releasedAt && !(priority && !known.resolved)) continue;
      if (known && priority && (known.source !== 'wallet' || known.releasedAt)) {
        known.source = 'wallet';
        known.releasedAt = null;
        known.releaseReason = null;
        known.subscribeAt = Math.min(known.subscribeAt ?? now, now);
        known.holdUntil = Infinity;
        continue;
      }

      const start = priority
        ? (gameStartOf(record, this.config, now) ?? { at: now, source: 'wallet activity' })
        : gameStartOf(record, this.config, now);
      if (!start) {
        skipped.push({
          conditionId: record.conditionId,
          reason: record.gameStartTime || record.endDate
            ? 'resolution deadline too far to date the match'
            : 'no game start time and no end date',
          record,
        });
        continue;
      }

      const hold = this.config.holdHours[record.sport] ?? this.config.holdHours.default ?? 6;
      const entry = known ?? {
        conditionId: record.conditionId,
        subscribedAt: null,
        releasedAt: null,
        releaseReason: null,
        observedDuringGame: false,
        resolved: false,
        resolutionCheckedAt: 0,
        source: priority ? 'wallet' : 'gamma',
      };
      entry.record = record;
      entry.gameStart = start.at;
      entry.gameStartSource = start.source;
      entry.subscribeAt = priority ? now : start.at - this.config.leadMinutes * 60 * 1000;
      // Only a resolution releases a market the wallet is in.
      entry.holdUntil = priority ? Infinity : start.at + hold * HOUR;
      this.#entries.set(record.conditionId, entry);
      if (!known) scheduled.push(entry);
    }
    return { scheduled, skipped };
  }

  /**
   * Move the clock. Everything that came due is subscribed, everything past its
   * hold or already resolved is let go.
   *
   * @returns {{added: object[], removed: object[], live: object[]}}
   */
  refresh(now = Date.now(), { maxLive = Infinity, quota = null,
    maxLivePerSport = Infinity } = {}) {
    const added = [];
    const removed = [];
    const waiting = [];
    for (const entry of this.#entries.values()) {
      if (entry.releasedAt) continue;

      if (this.#live.has(entry.conditionId)) {
        const reason = entry.resolved ? 'resolved'
          : now > entry.holdUntil ? 'past the hold window'
          : null;
        if (reason) {
          this.#release(entry, reason, now);
          removed.push(entry);
        }
        continue;
      }

      // Not live yet. A market that resolved before we ever subscribed — a
      // match played out while the collector was down — is closed out silently.
      if (entry.resolved || now > entry.holdUntil) {
        this.#release(entry, entry.resolved ? 'resolved' : 'window passed unwatched', now);
        continue;
      }
      if (now >= entry.subscribeAt) waiting.push(entry);
    }

    // What a discipline already holds, counted after the releases above so a
    // market let go this round frees its slot immediately.
    const perSport = new Map();
    const matchWinnersPerSport = new Map();
    for (const conditionId of this.#live) {
      const entry = this.#entries.get(conditionId);
      const sport = entry?.record?.sport ?? '';
      perSport.set(sport, (perSport.get(sport) ?? 0) + 1);
      if (isMatchWinner(entry)) matchWinnersPerSport.set(sport, (matchWinnersPerSport.get(sport) ?? 0) + 1);
    }

    // Under a cap, what gets the remaining slots is decided by where the
    // wallets actually work, not by how many markets a discipline happens to
    // have on Polymarket. A market a wallet is already in never waits.
    //
    // maxLivePerSport is the harder limit, and it exists because ranking alone
    // does not bound composition. Polymarket creates tennis sub-markets at a
    // rate no other discipline comes close to — one collection came out 73%
    // tennis, the one before it 60% CS2 — and a finding measured on one of
    // those does not describe the other. Ordering only decides who goes first
    // when there is a shortage; with slots to spare the abundant discipline
    // takes them all and the sample is whatever the API had most of.
    for (const entry of this.#rank(waiting, quota)) {
      const sport = entry.record.sport ?? '';
      const full = (perSport.get(sport) ?? 0) >= maxLivePerSport;
      // Keep room for main markets discovered after numerous set/map markets.
      // Ranking only the current waiting list cannot prevent that starvation.
      // Reserve is inside the existing sport cap; unused places stay empty.
      const requested = this.config.reserveMatchWinnerPerSport?.[sport] ?? 0;
      const reserve = Number.isFinite(maxLivePerSport) && Number.isFinite(requested)
        ? Math.min(Math.max(0, Math.floor(requested)), Math.max(0, Math.floor(maxLivePerSport))) : 0;
      const unfilledReserve = Math.max(0, reserve - (matchWinnersPerSport.get(sport) ?? 0));
      const reservedForMain = !isMatchWinner(entry)
        && (perSport.get(sport) ?? 0) >= maxLivePerSport - unfilledReserve;
      if ((this.#live.size >= maxLive || full || reservedForMain) && entry.source !== 'wallet') {
        entry.deferredForCapacity = true;
        continue;
      }
      entry.deferredForCapacity = false;
      {
        perSport.set(sport, (perSport.get(sport) ?? 0) + 1);
        if (isMatchWinner(entry)) matchWinnersPerSport.set(sport, (matchWinnersPerSport.get(sport) ?? 0) + 1);
        entry.subscribedAt = new Date(now).toISOString();
        this.#live.add(entry.conditionId);
        const [a, b] = entry.record.tokens;
        for (const assetId of entry.record.tokens) this.#assets.set(assetId, entry.conditionId);
        // Keep both outcomes for matching and liquidity reconstruction. A
        // complementary quote may mirror the same order, not independent fair value.
        if (a && b) {
          this.#pairs.set(a, b);
          this.#pairs.set(b, a);
        }
        added.push(entry);
      }
    }
    return { added, removed, live: this.live() };
  }

  /**
   * Order the markets waiting to be subscribed, most wanted first.
   *
   * Wallet markets, then the disciplines the wallets have been working in —
   * measured from their own fills rather than from how many markets an API
   * page happened to hold — then in-match sub-markets, which is where the
   * strategy under study operates.
   */
  #rank(waiting, quota) {
    const live = new Map();
    for (const conditionId of this.#live) {
      const sport = this.#entries.get(conditionId)?.record?.sport ?? '';
      live.set(sport, (live.get(sport) ?? 0) + 1);
    }
    const score = (entry) => {
      if (entry.source === 'wallet') return Number.POSITIVE_INFINITY;
      const sport = entry.record.sport ?? '';
      // How far this discipline is under the share the wallets give it. Scored
      // by size rather than by a yes/no, or two disciplines both merely "under"
      // would tie and the order would fall back to whichever was seen first.
      const share = quota?.get(sport) ?? 0;
      const held = (live.get(sport) ?? 0) / Math.max(this.#live.size, 1);
      // With no quota at all every share is zero, which still spreads the slots:
      // whatever is already over-represented sorts last.
      return (share - held) + (entry.record.level === 'segment' ? 0.05 : 0);
    };
    return [...waiting].sort((a, b) => score(b) - score(a) || a.subscribeAt - b.subscribeAt);
  }

  #release(entry, reason, now) {
    entry.releasedAt = new Date(now).toISOString();
    entry.releaseReason = reason;
    this.#live.delete(entry.conditionId);
    for (const assetId of entry.record.tokens) {
      this.#assets.delete(assetId);
      this.#pairs.delete(assetId);
    }
  }

  /** A market the resolver reports closed; it stops being watched on the next refresh. */
  markResolved(conditionId) {
    const entry = this.#entries.get(conditionId);
    if (!entry) return false;
    entry.resolved = true;
    return true;
  }

  /**
   * Live markets whose resolution is worth re-checking.
   *
   * Checking costs a CLOB request per market, so it only starts once the match
   * has had time to finish and repeats no more often than configured.
   */
  dueForResolutionCheck(now = Date.now(), limit = 25, urgent = null) {
    const after = this.config.resolutionCheckAfterMinutes * 60 * 1000;
    const every = this.config.resolutionCheckMinutes * 60 * 1000;
    // A market with a sweep horizon still running is asked about on its own,
    // much sooner. The general schedule starts 45 minutes after kick-off, by
    // which time every 1, 5 and 15 minute horizon has already been written —
    // which is why resolved_before_horizon came back zero on every row ever
    // collected. It was not defaulting to zero; it was unreachable.
    const pressingEvery = (this.config.resolutionCheckOpenHorizonSeconds ?? 60) * 1000;
    const due = [];
    for (const conditionId of this.#live) {
      const entry = this.#entries.get(conditionId);
      if (!entry || entry.resolved) continue;
      const pressing = urgent ? urgent.has(conditionId) : false;
      if (!pressing && now - entry.gameStart < after) continue;
      if (now - entry.resolutionCheckedAt < (pressing ? pressingEvery : every)) continue;
      due.push(entry);
      if (due.length >= limit) break;
    }
    for (const entry of due) entry.resolutionCheckedAt = now;
    return due;
  }

  /**
   * Record that the book was actually seen while the match was being played.
   *
   * This is the self-check the whole change exists for, and it is kept live
   * rather than derived later so that a glance at markets.csv answers it.
   *
   * @returns {boolean} true the first time, so the caller can write it once
   */
  noteObserved(conditionId, now = Date.now()) {
    const entry = this.#entries.get(conditionId);
    if (!entry || entry.observedDuringGame) return false;
    if (now < entry.gameStart || now > entry.holdUntil) return false;
    entry.observedDuringGame = true;
    return true;
  }

  entry(conditionId) {
    return this.#entries.get(conditionId) ?? null;
  }

  /** Records under subscription right now. */
  live() {
    return [...this.#live].map((id) => this.#entries.get(id));
  }

  /** Decision snapshots for a change-only audit trail, including unwatched releases. */
  decisions(now = Date.now()) {
    return [...this.#entries.values()].map(entry => ({
      conditionId: entry.conditionId,
      status: entry.releasedAt ? entry.releaseReason
        : this.#live.has(entry.conditionId) ? 'subscribed'
        : now < entry.subscribeAt ? 'awaiting subscription window'
        : entry.deferredForCapacity ? 'capacity' : 'pending',
      source: entry.source,
      subscribeAt: entry.subscribeAt,
      holdUntil: Number.isFinite(entry.holdUntil) ? entry.holdUntil : null,
      gameStart: entry.gameStart,
      subscribedAt: entry.subscribedAt,
      releasedAt: entry.releasedAt,
    }));
  }

  /** Every asset id under subscription, two per market. */
  assetIds() {
    return [...this.#assets.keys()];
  }

  conditionOf(assetId) {
    return this.#assets.get(assetId) ?? null;
  }

  /** The other outcome token of the same market, or null. */
  pairOf(assetId) {
    return this.#pairs.get(assetId) ?? null;
  }

  get liveSize() {
    return this.#live.size;
  }

  get size() {
    return this.#entries.size;
  }

  /** Markets scheduled but not yet due, for the status line. */
  get pendingSize() {
    let count = 0;
    for (const entry of this.#entries.values()) {
      if (!entry.releasedAt && !this.#live.has(entry.conditionId)) count++;
    }
    return count;
  }
}
