// Turns a raw Gamma market into the fields the collector and the analyzer need.
//
// Two things make this less trivial than reading a field:
//
// 1. The discipline is only reliable in the event slug. "Map 1 Rounds Handicap:
//    A (-6.5) vs B (+6.5)" and "Games Total: O/U 2.5" name no sport at all.
// 2. Level and kind are independent axes. "Map 3 Total Rounds: O/U 24.5" is a
//    total inside a segment, while "Games Total: O/U 2.5" is a total over the
//    whole match. Keep these distinct so research can compare their behavior.
// 3. "Completed Match", a toss and a first corner are different propositions
//    from the match winner, even when their question contains both team names.

const SEGMENT = /\b(map|game|set)\s*(\d+)\b/i;
const HANDICAP = /handicap/i;
const TOTAL = /\btotal|\bo\/u\b|over\/under/i;
const WINNER = /winner/i;
const LINE = /([+-]?\d+(?:\.\d+)?)/;
// What a total or a handicap is counted in. Order matters: "Map 3 Total Rounds"
// names two of these and the one being counted is the later one.
const UNITS = [[/kills?/i, 'kill'], [/rounds?/i, 'round'], [/games?/i, 'game'],
  [/sets?/i, 'set'], [/maps?/i, 'map']];
const VERSUS = /^(.*?)\s+vs\.?\s+(.*?)$/i;

function auxiliaryKind(market, question) {
  const type = String(market.sportsMarketType ?? '').toLowerCase();
  if (type.endsWith('_completed_match') || /\bcompleted\s+match\b/i.test(question)) return 'completed_match';
  if (type.endsWith('_toss_winner') || /\b(wins?\s+the\s+toss|toss\s+winner)\b/i.test(question)) return 'toss_winner';
  if (type === 'soccer_first_corner' || /\bfirst\s+corner\b/i.test(question)) return 'first_corner';
  if (type === 'soccer_game_corners_odd_even' || /\bcorners?\b.*\bodd\s+or\s+even\b/i.test(question)) return 'corners_odd_even';
  return null;
}

/** Gamma encodes these as JSON strings, not arrays. */
export function parseJsonField(value) {
  if (Array.isArray(value)) return value;
  if (typeof value !== 'string') return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

const slugPrefix = (slug) => (typeof slug === 'string' ? (slug.split('-')[0] || null) : null);

/**
 * Strip a competitor name down to the team.
 * Order matters: an event title reads "A vs B (BO3) - Tournament", so the
 * tournament tail has to go before the trailing "(BO3)" is even at the end.
 */
const cleanTeam = (name) =>
  name.replace(/\s+-\s+.*$/, '').replace(/\s*\([^)]*\)\s*$/, '').trim();

/**
 * Competitors, in outcome order where the outcomes name them. Totals answer
 * Over/Under, so their teams come from the question or the event title instead.
 */
function teamsOf(question, eventTitle, outcomes) {
  if (outcomes.length === 2 && !/^(over|under|yes|no)$/i.test(outcomes[0])) {
    return outcomes.map(cleanTeam);
  }
  for (const text of [question, eventTitle]) {
    const body = String(text ?? '').replace(/^[^:]*:\s*/, '');
    const match = VERSUS.exec(body);
    if (match) return [cleanTeam(match[1]), cleanTeam(match[2])];
  }
  return null;
}

function lineOf(question, kind) {
  if (kind !== 'handicap' && kind !== 'total') return null;
  // For a handicap the sign matters and belongs to the first competitor; for a
  // total the number is the line itself.
  const tail = kind === 'handicap' ? /\(([+-]\d+(?:\.\d+)?)\)/.exec(question) : null;
  const match = tail ?? LINE.exec(question.replace(SEGMENT, ''));
  return match ? Number(match[1]) : null;
}

/**
 * @param {object} market  a Gamma /markets element
 * @param {Record<string,string>} disciplines  event-slug prefix -> gg.bet sportId
 * @returns {object} normalized market record
 */
export function classifyMarket(market, disciplines = {}) {
  const question = String(market.question ?? '');
  const event = (market.events ?? [])[0] ?? {};
  const prefix = slugPrefix(event.slug);
  const auxiliary = auxiliaryKind(market, question);
  const segment = auxiliary ? null : SEGMENT.exec(question);

  const kind = auxiliary ?? (HANDICAP.test(question) ? 'handicap'
    : TOTAL.test(question) ? 'total'
    : WINNER.test(question) ? 'winner'
    : 'winner'); // a bare "A vs B (BO3) - Tournament" is the match winner

  const outcomes = parseJsonField(market.outcomes);
  const tokens = parseJsonField(market.clobTokenIds);

  return {
    conditionId: market.conditionId ?? null,
    question,
    slug: market.slug ?? null,
    eventSlug: event.slug ?? null,
    eventTitle: event.title ?? null,
    endDate: market.endDate ?? null,
    startDate: market.startDate ?? null,
    // When play actually starts. `startDate` is when the market opened for
    // orders and `endDate` is a resolution deadline; neither dates the match,
    // and the subscription window is built on this one.
    gameStartTime: market.gameStartTime ?? market.game_start_time ?? null,
    prefix,
    sport: (prefix && disciplines[prefix]) ?? null,
    level: segment ? 'segment' : 'match',
    kind,
    sportsMarketType: market.sportsMarketType ?? null,
    segmentKind: segment ? segment[1].toLowerCase() : null,
    segmentNo: segment ? Number(segment[2]) : null,
    line: lineOf(question, kind),
    // Two totals at the same level are still different questions when one
    // counts games and the other sets. Without this, "Total Sets O/U 2.5"
    // pairs with gg.bet's games total and the dislocation is meaningless.
    unit: kind === 'first_corner' || kind === 'corners_odd_even' ? 'corner'
      : kind !== 'total' && kind !== 'handicap' ? null
      : (UNITS.find(([p]) => p.test(question))?.[1] ?? null),
    teams: teamsOf(question, event.title, outcomes),
    outcomes,
    tokens,
    tickSize: Number(market.orderPriceMinTickSize ?? 0.001),
    minSize: Number(market.orderMinSize ?? 0),
    enableOrderBook: market.enableOrderBook !== false,
  };
}

/**
 * Why a market was not subscribed to, or null if it was.
 *
 * The reason is the point: without it there is no telling "the bot never went
 * here" from "the logger never looked", and that difference is the denominator
 * the whole study rests on.
 */
export function skipReason(record) {
  if (!record.sport) return `unmapped discipline: ${record.prefix ?? 'no event slug'}`;
  if (!record.enableOrderBook) return 'order book disabled';
  if (record.tokens.length !== 2) return `expected 2 tokens, got ${record.tokens.length}`;
  return null;
}

/** Markets worth subscribing to: a known discipline and a live order book. */
export function isTracked(record) {
  return skipReason(record) === null;
}
