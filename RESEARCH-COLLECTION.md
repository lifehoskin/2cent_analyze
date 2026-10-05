# Research collection: October 5 audit update

The audit found a tennis capacity snapshot containing 150 segment markets and
no match-winner markets (112 segment totals, 38 segment winners). Raising the
limit would increase disk usage without fixing this composition problem.

`schedule.reserveMatchWinnerPerSport: { "tennis": 30 }` now keeps 30 places
inside the existing 150-market tennis cap available for markets classified as
`level=match, kind=winner`. Segment markets and match totals cannot consume
unused reserved places. Main winners can use more than 30 places if available.
This is an initial research allocation, not an optimized trading parameter.
Set `tennis: 0` to disable it. Other sports retain their existing allocation.

The existing wallet-follow exception can still exceed limits. The reservation
does not override the global market cap or evict live subscriptions. Restarting
the collector applies it to a fresh subscription schedule. It cannot recover
past books or discover markets absent from discovery results.

The raw October 5 cases support frequent anonymous size changes, but do not
replicate a universal depth-recovery entry rule. Do not tune collection toward
only successful rebounds. For the next control sample, retain the original
failed controls and select new windows using only information available at the
window start: actual two-sided book, comparable price/spread and activity,
coverage, sport and market kind. Subscription status alone is insufficient:
both original control windows were one-sided with the winning-side bid 0.999.

Keep known-order timestamps distinct from anonymous level changes. A matching
size, price and time is a candidate association; public L2 has no wallet ID.
Checkpoint/source time is not a fresh quote timestamp. Replay capture v2 using
connection ownership and validate checkpoint and reported-BBO consistency.

Validation for the reservation:

```sh
node src/pm/schedule-reserve.test.mjs
node src/pm/schedule.test.mjs
node src/pm/classify.test.mjs
```

`npm run test:pm` also discovers the new test through its existing glob.
