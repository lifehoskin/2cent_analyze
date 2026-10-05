import assert from 'node:assert/strict';
import { MarketSchedule } from './schedule.mjs';
import { DEFAULTS, loadConfig } from './config.mjs';

const now = Date.parse('2026-10-05T12:00:00Z');
const make = (id, extra = {}) => ({ conditionId: id, sport: 'tennis', level: 'segment',
  kind: 'winner', tokens: [`${id}-a`, `${id}-b`], gameStartTime: new Date(now).toISOString(), ...extra });
const schedule = new MarketSchedule({ reserveMatchWinnerPerSport: { tennis: 2 } });
schedule.observe(Array.from({ length: 6 }, (_, i) => make(`set-${i}`)), now);
assert.equal(schedule.refresh(now, { maxLivePerSport: 5 }).live.length, 3,
  'early set markets cannot consume places reserved for later main winners');
schedule.observe([make('total', { level: 'match', kind: 'total' }),
  make('main-a', { level: 'match' }), make('main-b', { level: 'match' })], now + 1);
const filled = schedule.refresh(now + 1, { maxLivePerSport: 5 });
assert.deepEqual(filled.added.map(e => e.conditionId).sort(), ['main-a', 'main-b']);
assert.equal(filled.live.length, 5, 'reservation does not increase the sport cap');
assert.equal(schedule.entry('total').subscribedAt, null, 'match totals cannot use main-winner places');
schedule.markResolved('main-a');
assert.equal(schedule.refresh(now + 2, { maxLivePerSport: 5 }).live.length, 4,
  'a released main place stays available for another main winner');
schedule.observe([make('main-c', { level: 'match' })], now + 3);
assert.equal(schedule.refresh(now + 3, { maxLivePerSport: 5 }).live.length, 5);
schedule.observe([make('wallet')], now + 4, { priority: true });
assert.equal(schedule.refresh(now + 4, { maxLivePerSport: 5 }).live.length, 6,
  'existing wallet-follow override is preserved');

for (const reserve of [0, -2, NaN]) {
  const s = new MarketSchedule({ reserveMatchWinnerPerSport: { tennis: reserve } });
  s.observe(Array.from({ length: 6 }, (_, i) => make(`plain-${i}`)), now);
  assert.equal(s.refresh(now, { maxLivePerSport: 5 }).live.length, 5);
}
const bounded = new MarketSchedule({ reserveMatchWinnerPerSport: { tennis: 20 } });
bounded.observe([make('set'), make('winner', { level: 'match' })], now);
assert.deepEqual(bounded.refresh(now, { maxLivePerSport: 1 }).added.map(e => e.conditionId), ['winner']);
const global = new MarketSchedule({ reserveMatchWinnerPerSport: { tennis: 2 } });
global.observe([make('a', { level: 'match' }), make('b', { level: 'match' })], now);
assert.equal(global.refresh(now, { maxLivePerSport: 5, maxLive: 1 }).live.length, 1);
const other = new MarketSchedule({ reserveMatchWinnerPerSport: { tennis: 2 } });
other.observe(Array.from({ length: 5 }, (_, i) => make(`cs-${i}`, { sport: 'esports_counter_strike' })), now);
assert.equal(other.refresh(now, { maxLivePerSport: 5 }).live.length, 5);
assert.equal(DEFAULTS.schedule.reserveMatchWinnerPerSport.tennis, 30);
assert.equal(loadConfig().schedule.reserveMatchWinnerPerSport.tennis, 30);
console.log('schedule reserve: capacity, late discovery, releases, wallet override, config and bounds passed');
