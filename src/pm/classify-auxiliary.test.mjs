import assert from 'node:assert/strict';
import { classifyMarket, isTracked } from './classify.mjs';
import { MarketSchedule } from './schedule.mjs';

const now = Date.parse('2026-10-05T12:00:00Z');
const base = { conditionId: 'completed', question: 'M15 Fayetteville: Completed Match: Jonah Braswell vs Jordan Lee',
  sportsMarketType: 'tennis_completed_match', outcomes: '["Yes","No"]', clobTokenIds: '["a","b"]',
  events: [{ slug: 'itf-braswe1-lee7-2026-10-01', title: 'Jonah Braswell vs Jordan Lee' }],
  gameStartTime: new Date(now).toISOString() };
const classify = raw => classifyMarket(raw, { itf: 'tennis' });
const completed = classify(base);
assert.equal(completed.kind, 'completed_match');
assert.equal(completed.line, null, 'M15 tournament number is not a price/total line');
assert.equal(completed.unit, null);
assert.equal(completed.level, 'match');
assert.equal(completed.sportsMarketType, 'tennis_completed_match');
assert.ok(isTracked(completed), 'classification does not silently drop a research market');
assert.equal(classify({ ...base, sportsMarketType: undefined }).kind, 'completed_match', 'old records retain a question fallback');
for (const [sportsMarketType, question, kind] of [
  ['cricket_completed_match', 'Avengers XI vs Challenging Stars - Completed match?', 'completed_match'],
  ['cricket_toss_winner', 'Avengers XI vs Challenging Stars - Who wins the toss?', 'toss_winner'],
  ['soccer_first_corner', 'Malta vs Gibraltar: Team to Take First Corner', 'first_corner'],
  ['soccer_game_corners_odd_even', 'Malta vs Gibraltar: Total Corners Odd or Even?', 'corners_odd_even'],
]) {
  for (const type of [sportsMarketType, undefined]) {
    const r = classify({ ...base, sportsMarketType: type, question });
    assert.equal(r.kind, kind);assert.equal(r.line, null);assert.equal(r.segmentNo, null);
  }
}
const main = classify({ ...base, conditionId: 'main', question: 'Jonah Braswell vs Jordan Lee',
  sportsMarketType: 'moneyline', outcomes: '["Jonah Braswell","Jordan Lee"]', clobTokenIds: '["c","d"]' });
assert.equal(main.kind, 'winner');
const total = classify({ ...base, sportsMarketType: 'tennis_set_totals', question: 'Jonah Braswell vs Jordan Lee: Total Sets O/U 2.5' });
assert.equal(total.kind, 'total');assert.equal(total.line, 2.5);assert.equal(total.unit, 'set');
const schedule = new MarketSchedule({ reserveMatchWinnerPerSport: { tennis: 1 } });
schedule.observe([completed, main], now);
assert.deepEqual(schedule.refresh(now, { maxLivePerSport: 1 }).added.map(e => e.conditionId), ['main'],
  'completed-match markets cannot consume reserved main-winner capacity');
console.log('auxiliary classification: native types, fallback, totals and reservation integration passed');
