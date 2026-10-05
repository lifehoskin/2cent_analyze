// One config file for the Polymarket side, merged over the defaults below.
//
// The gg.bet collector keeps its own CLI flags untouched; what it shares here is
// only the path to the files it already writes, which module C tails read-only.

import { existsSync, readFileSync } from 'node:fs';

export const DEFAULTS = {
  // Event-slug prefix -> gg.bet sportId. Polymarket puts the discipline in the
  // event slug ("cs2-sangal-psna-2026-08-26"), not reliably in the question:
  // a handicap market reads "Map 1 Rounds Handicap: A (-6.5) vs B (+6.5)" and
  // names no sport at all.
  disciplines: {
    cs2: 'esports_counter_strike',
    csgo: 'esports_counter_strike',
    dota2: 'esports_dota_2',
    dota: 'esports_dota_2',
    lol: 'esports_league_of_legends',
    val: 'esports_valorant',
    valorant: 'esports_valorant',
    r6: 'esports_rainbow_six',
    atp: 'tennis',
    wta: 'tennis',
    tennis: 'tennis',
    codmw: 'esports_call_of_duty',
    itf: 'tennis', // Explicit research coverage; shares the capped tennis budget.
    mlb: 'baseball',
  },
  // Additional naming aliases; only entries in disciplines are subscribed. Used
  // only to name a market after the fact: `disciplines` decides what gets
  // watched, `labels` decides what can be labelled, and conflating them would
  // silently widen the subscription universe.
  labels: {
    // ITF is explicitly enabled above with per-sport limits for this research.
    // Setka remains naming-only; do not merge the two maps.
    itf: 'tennis',
    setka: 'table_tennis',
    setkameua: 'table_tennis',
    mlb: 'baseball',
    nba: 'basketball',
    nfl: 'american_football',
    nhl: 'ice_hockey',
    epl: 'football',
    ucl: 'football',
    pol: 'politics',
    us: 'politics',
    fin1: 'finance',
    bitcoin: 'crypto',
    eth: 'crypto',
    ethereum: 'crypto',
    xrp: 'crypto',
    sol: 'crypto',
    doge: 'crypto',
  },
  gamma: { intervalSeconds: 45, pageSize: 100, maxPages: 12 },
  // The subscription window, which is the match clock and nothing else. See
  // schedule.mjs for why end_date takes no part in it: it is a resolution
  // deadline set six hours out for CS2 and a week out for tennis.
  schedule: {
    // Enough lead to have the book before the first point is played.
    leadMinutes: 30,
    tickSeconds: 10,
    // How long after the start a match can still be running, per gg.bet
    // sportId. A BO5 Dota series runs longer than a CS2 one.
    holdHours: {
      esports_counter_strike: 6,
      esports_dota_2: 8,
      esports_league_of_legends: 6,
      esports_valorant: 6,
      esports_rainbow_six: 6,
      esports_call_of_duty: 6,
      tennis: 5,
      table_tennis: 2,
      default: 6,
    },
    // Markets dated only through the resolution deadline are scheduled no
    // further ahead than this; beyond it the deadline is not about this match.
    maxAheadHours: 72,
    // A ceiling on markets watched at once. Null means no ceiling, which is the
    // default: nothing is rationed unless a machine cannot keep up. When it is
    // set, slots go by where the studied wallets have actually been trading
    // over the last week rather than by how many markets a discipline happens
    // to have on Polymarket — and a market a wallet is already in never waits,
    // whatever the ceiling says.
    maxLiveMarkets: null,
    maxLivePerSport: 150,
    // Research allocation, not a trading signal: the Oct 5 audit found 150
    // tennis set markets and zero main winners. Keep 30 of those same 150
    // places available for main winners, including later discovery arrivals.
    reserveMatchWinnerPerSport: { tennis: 30 },
    // Resolution is what really ends a subscription; the hold above is the
    // backstop. Asking costs one CLOB request per market, so it starts only
    // once the match could plausibly be over.
    resolutionCheckMinutes: 15,
    resolutionCheckAfterMinutes: 45,
    // A market with a sweep horizon still open is asked about this often
    // instead, whatever the two settings above say: the horizons are 1, 5 and
    // 15 minutes and would otherwise all close before the first question.
    resolutionCheckOpenHorizonSeconds: 60,
    resolutionsPerCycle: 20,
  },
  // Horizons, in minutes, over which the best bid after a sweep is followed.
  followups: { horizons: [1, 5, 15] },
  // The book around each wallet fill. settleSeconds is how long the write-up
  // waits for the horizons after the fill; maxAgeHours bounds how far back into
  // the wallet's trade history a fill is worth writing up at all, since
  // anything older than the collection has no snapshots behind it.
  fillContext: { settleSeconds: 900, maxAgeHours: 24 },
  book: { heartbeatSeconds: 5, reconnectMinMs: 1000, reconnectMaxMs: 60000 },
  capture: { enabled: true, checkpointSeconds: 60 },
  // The tick is per-market, not a constant: of the esports markets sampled, 58
  // run at 0.01 and 7 at 0.001. So the bid-drop rule is expressed in ticks of
  // the market itself, with an absolute floor so the 0.001 books do not fire on
  // noise. Both rules are logged separately and meant to be retuned on data.
  sweep: {
    levelsCrossed: 3,
    bidDropTicks: 3,
    bidDropMin: 0.02,
    depthWindowSeconds: 10,
    depthDropRatio: 0.5,
    depthAbovePrice: 0.05,
    // Guards learned from a first day of real collection, which logged 617k
    // "sweeps": thin books blink empty as the maker repositions, and one real
    // collapse arrives as a burst of updates.
    minBidBefore: 0.05,
    minSizeConsumed: 100,
    minDepth: 100,
    cooldownSeconds: 30,
  },
  // followHours bounds which of the wallet's markets are taken up for watching:
  // its activity history runs to thousands of records, and following all of it
  // would subscribe to long-dated markets it held months ago and hold each
  // until resolution. Recency is what makes a market the live one under study.
  wallets: { intervalSeconds: 25, followHours: 48, addresses: [] },
  // Role costs two requests per market, so it is pulled only where a target
  // wallet actually traded, and no more often than this.
  trades: { intervalSeconds: 120, marketsPerCycle: 25 },
  ggbet: {
    oddsHistory: 'odds-history.csv',
    changes: 'changes.csv',
    mapping: 'mapping.csv',
    matchWindowHours: 6,
    pollSeconds: 5,
  },
  storage: { dir: 'data' },
};

/** Shallow-merge each top-level section so a partial config file is enough. */
function merge(base, override) {
  const out = { ...base };
  for (const [key, value] of Object.entries(override ?? {})) {
    out[key] = value && typeof value === 'object' && !Array.isArray(value)
      ? { ...(base[key] ?? {}), ...value }
      : value;
  }
  return out;
}

export function loadConfig(path = 'pm.config.json') {
  if (!existsSync(path)) return { ...DEFAULTS };
  try {
    return merge(DEFAULTS, JSON.parse(readFileSync(path, 'utf8')));
  } catch (err) {
    throw new Error(`${path} is not valid JSON: ${err.message}`);
  }
}
