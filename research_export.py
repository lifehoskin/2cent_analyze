#!/usr/bin/env python3
"""Export the continuous research journal, without sweep or volume sampling.

python3 research_export.py --db data --since 2026-09-16 --until 2026-09-16
Dates select complete UTC daily files (both endpoints inclusive). Standard library only.
"""
import argparse
import csv
import datetime as dt
import gzip
import json
import sqlite3
import zipfile
from collections import Counter
from contextlib import nullcontext
from pathlib import Path

from analyze import databases
from trade_identity import unique_fills
from order_research import export_orders, load_wallets, write_csv
from order_context import creation_context


def columns(db, table):
    return [r['name'] for r in db.execute(f'PRAGMA table_info({table})')]


def export_research(paths, out, order_db=None, wallets=None, context_paths=None, overview=False,
                    requested_since=None, requested_until=None):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    if any(out.iterdir()):
        raise ValueError(f'{out} is not empty; choose a new --out directory')
    manifest = {'version': 1, 'created_at': dt.datetime.now(dt.timezone.utc).isoformat(),
                'mode': 'overview' if overview else 'full',
                'sampling': 'metadata_only' if overview else 'none',
                'files': [], 'counts': {}, 'warnings': []}
    if requested_since or requested_until:
        present_days = sorted(Path(p).name[3:13] for p in paths)
        start = dt.date.fromisoformat(requested_since or present_days[0])
        finish = dt.date.fromisoformat(requested_until or present_days[-1])
        manifest['requested_date_range'] = {'since': requested_since, 'until': requested_until,
                                            'timezone': 'UTC', 'inclusive': True}
        missing = []
        day = start
        while day <= finish:
            if day.isoformat() not in present_days:
                missing.append(day.isoformat())
            day += dt.timedelta(days=1)
        manifest['missing_daily_databases'] = missing
        if missing:
            manifest['warnings'].append('No daily databases for requested UTC dates: ' + ', '.join(missing))
    if overview:
        manifest['omitted_files'] = ['market-events.jsonl.gz', 'market-trades.jsonl.gz',
                                     'quote-observations.csv.gz']
        manifest['count_scope'] = 'market_events and event-type counts describe source databases; raw frames are NOT exported'
    connections = []
    try:
        for path in paths:
            db = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)
            db.row_factory = sqlite3.Row
            db.execute('BEGIN')
            # Pin a consistent read snapshot, including on an active WAL store.
            db.execute('SELECT count(*) FROM sqlite_master').fetchone()
            connections.append((Path(path).name, db))

        counts = Counter()
        event_name = 'capture-metadata.jsonl.gz' if overview else 'market-events.jsonl.gz'
        with gzip.open(out / event_name, 'wt', encoding='utf-8') as events, \
                (nullcontext(None) if overview else gzip.open(out / 'market-trades.jsonl.gz', 'wt', encoding='utf-8')) as prints:
            for name, db in connections:
                summary = {'database': name, 'events': 0, 'first_received_at': None,
                           'last_received_at': None}
                if not columns(db, 'market_events'):
                    manifest['warnings'].append(f'{name}: no raw journal (older collector)')
                else:
                    if overview:
                        # Count source coverage without deserializing/copying the large frame payloads.
                        for row in db.execute('SELECT event_type,count(*) AS n,min(received_at) AS first_at,'
                                              'max(received_at) AS last_at FROM market_events GROUP BY event_type'):
                            counts['market_events'] += row['n']
                            counts[row['event_type']] += row['n']
                            summary['events'] += row['n']
                            summary['first_received_at'] = min(summary['first_received_at'] or row['first_at'], row['first_at'])
                            summary['last_received_at'] = max(summary['last_received_at'] or row['last_at'], row['last_at'])
                        query = "SELECT * FROM market_events WHERE event_type IN ('capture_start','stream_reset','schedule_decision') ORDER BY session_id,seq"
                    else:
                        query = 'SELECT * FROM market_events ORDER BY session_id, seq'
                    for row in db.execute(query):
                        event = dict(row)
                        event['database'] = name
                        event['payload'] = json.loads(event.pop('payload_json'))
                        events.write(json.dumps(event, separators=(',', ':')) + '\n')
                        if overview:
                            counts['capture_metadata_exported'] += 1
                            continue
                        counts['market_events'] += 1
                        counts[event['event_type']] += 1
                        summary['events'] += 1
                        ts = event['received_at']
                        summary['first_received_at'] = min(summary['first_received_at'] or ts, ts)
                        summary['last_received_at'] = max(summary['last_received_at'] or ts, ts)
                        if event['event_type'] == 'frame':
                            for index, message in enumerate(event['payload']):
                                if message.get('event_type') != 'last_trade_price':
                                    continue
                                # Preserve the original print and the journal join, not a
                                # guessed aggressor wallet or reconstructed trade identity.
                                prints.write(json.dumps({
                                    'session_id': event['session_id'], 'frame_seq': event['seq'],
                                    'message_index': index, 'received_at': ts,
                                    'connection_id': event['connection_id'], 'message': message,
                                }, separators=(',', ':')) + '\n')
                                counts['market_trades'] += 1
                manifest['files'].append(summary)

        # Stream large tables; preserve daily provenance and evolving column sets.
        tables = ('markets', 'universe', 'gaps') if overview else ('quote_observations', 'markets', 'universe', 'gaps')
        for table in tables:
            header = list(dict.fromkeys(c for _, db in connections for c in columns(db, table)))
            with gzip.open(out / f'{table.replace("_", "-")}.csv.gz', 'wt',
                           encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(handle, ['database'] + header)
                writer.writeheader()
                for name, db in connections:
                    if not columns(db, table):
                        continue
                    for row in db.execute(f'SELECT * FROM {table}'):
                        writer.writerow({'database': name, **dict(row)})
                        counts[table] += 1

        fills = []
        for _, db in connections:
            if columns(db, 'trades'):
                fills.extend(dict(r) for r in db.execute('SELECT * FROM trades'))
        counts['fill_copies'] = len(fills)
        fills = unique_fills(fills)
        counts['unique_fills'] = len(fills)
        counts['duplicate_fills_removed'] = counts['fill_copies'] - len(fills)
        # API history is re-read into today's database. Daily file date alone
        # must not turn September 4 fills into September 19 research examples.
        days = {Path(p).name[3:13] for p in paths}
        historical = [r for r in fills if str(r.get('ts', ''))[:10] not in days]
        fills = [r for r in fills if str(r.get('ts', ''))[:10] in days]
        counts['historical_api_fills'] = len(historical)
        counts['target_fills_in_selected_days'] = len(fills)
        write_csv(out / 'historical-target-fills.csv', historical)
        if historical:
            manifest['warnings'].append(f'{len(historical)} API history fills fall outside selected UTC days; see historical-target-fills.csv')
        header = list(dict.fromkeys(c for row in fills for c in row))
        header = [c for c in header if c != 'fill_index'] + ['fill_index']
        indices = Counter()
        with (out / 'target-fills.csv').open('w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, header)
            writer.writeheader()
            for row in fills:
                key = (row['wallet'].lower(), row['asset_id'], row['side'])
                indices[key] += 1
                writer.writerow({**row, 'fill_index': indices[key]})
        if not counts['market_events']:
            manifest['warnings'].append('No raw events: collect with capture.enabled=true first')
        if order_db and Path(order_db).exists():
            days = sorted(Path(p).name[3:13] for p in paths)
            orders, order_counts = export_orders(order_db, out, wallets or set(), days[0], days[-1])
            contexts = creation_context(orders, context_paths or paths)
            write_csv(out / 'order-creation-context.csv', contexts)
            counts.update(order_counts)
            counts['order_creation_context'] = len(contexts)
            manifest['order_context_statuses'] = dict(Counter(r['context_status'] for r in contexts))
        elif order_db:
            manifest['warnings'].append('No order database: run npm run pm:orders alongside the collector')
        manifest['counts'] = dict(counts)
        (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
        (out / 'README.md').write_text((OVERVIEW_README if overview else '') + README, encoding='utf-8')
    finally:
        for _, db in connections:
            db.close()
    archive_path = Path(str(out) + '.zip')
    with zipfile.ZipFile(archive_path, 'w') as archive:
        for path in sorted(out.iterdir()):
            archive.write(path, path.name,
                          compress_type=zipfile.ZIP_STORED if path.suffix == '.gz'
                          else zipfile.ZIP_DEFLATED)
    return manifest


OVERVIEW_README = """# OVERVIEW ONLY — not a raw-book backup

This bundle omits market-events, market-trades and quote-observations.
It retains fills, signed orders when available, creation context, market registries,
discovery records, gaps, and capture-metadata (capture_start, stream_reset,
schedule_decision). Manifest event counts describe SOURCE rows, not exported frames.
No public-trade count is calculated in this mode. This is for coverage review and
selecting further research; it cannot reconstruct cancellation/replace sequences.
Keep the original daily SQLite databases for subsequent full/case exports.

The full-format reference below also describes files omitted from this overview.

---

"""


README = """# Continuous Polymarket research capture

`manifest.json` lists source UTC files, actual receive-time spans, counts and missing
raw coverage. The whole selected daily files are exported, including ordinary
periods without detected sweeps or known wallet fills. There is no row sampling.
Files may be large. Every database is read from a consistent SQLite snapshot.

## Files and replay

- `market-events.jsonl.gz`: complete parsed market messages (all prices/sizes),
  checkpoints, resets and `capture_start` with the effective configuration.
  Within each session replay ascending `seq`, across daily files. Sessions are
  independent: do not join a previous session's book to a restarted process.
  `frame` payloads are arrays. Apply only each token's owning-connection entries
  (see subscribe/unsubscribe resets and checkpoint source_connection_id), then
  calculate paired features after the entire accepted batch. A multi-token
  price_change can arrive on two sockets; applying its foreign leg again can
  rewind the book. Raw frames intentionally retain both copies for auditing.
  `book` replaces a token's entire book; `price_change.size` is the
  NEW absolute size at that side/price, not an increment. `checkpoint` replaces
  the complete known book set and retains each book's last source timestamp.
  `stream_reset` invalidates its assets until the next full `book`. Retain but
  do not apply deltas for unknown books. A gap is unknown state, not zero depth.
  A later day begins with a local checkpoint even if no new market message
  arrives at midnight. A checkpoint repeats state; it is not a fresh quote.
  Capture version 2 declares book_apply_policy=connection_owned_assets. Version 1
  checkpoints/quote_observations can contain cross-socket merge artifacts. For
  a corrected replay of v1, reconstruct ownership from resets, start from actual
  owning-socket full books, and do not overwrite them with legacy checkpoints.
- `market-trades.jsonl.gz`: every `last_trade_price` message, with a join back
  to session/frame/message index. These public prints have no reliable wallet
  attribution; they are evidence for execution, not proof who owned a bid.
- `quote-observations.csv.gz`: features after each changed frame, plus periodic
  `trigger=checkpoint` observations across all currently known books. The latter
  provide time-based control candidates without selecting on future fills.
  They are not automatically negative examples. `levels_json` is an array of
  `[price, bid_shares, ask_shares, bid_shares_strictly_above, bid_dollars_strictly_above]`.
  Levels: .01, .011, .02, .03, .05, .25, .40, .50, .70, .81, .85, .90.
  `crossed` and `paired_crossed`: 1 means bid > ask; null means no two-sided book.
  Source timestamps are exchange epoch milliseconds; receive times are UTC ISO.
  `monotonic_ms` in the journal is process-local, comparable only within session.
- `target-fills.csv`: unique observed economic fills whose trade timestamps fall
  in selected UTC daily files, keyed
  by tx hash, wallet, token, side, size and price; no-hash records are retained.
  Equal API rows cannot distinguish multiple identical logs inside a transaction.
  `fill_index` is within this export, per wallet/token/side, not lifetime history.
- `historical-target-fills.csv`: deduplicated API history outside those UTC days;
  retained separately instead of being mistaken for current coverage.
- `markets.csv.gz`: daily market registries, including paired token identifiers,
  labels and subscription windows. Preserve provenance; later files have newer
  metadata, not necessarily information that was known at a past decision time.
- `universe.csv.gz`: discovery records. Since the September 19 fix, subscribed
  means actually selected by the scheduler at least once, not mere eligibility.
  Raw `schedule_decision` events record changing window/capacity/release reasons.
  Socket health still requires resets and full snapshots. Older files cannot be
  retroactively corrected. Discovery does not cover every Polymarket market.
- `gaps.csv.gz`: legacy socket outage summaries; use asset-specific raw resets
  and the next full snapshot to invalidate research intervals precisely.

## Signed order enrichment (when the separate order collector has run)

- `chain-fills.csv`: every target receipt fill, keyed by chain/tx/log_index. No price-based exclusions.
- `signed-orders.csv`: unique chain/exchange/order_hash; original limit/amounts and signed client time,
  first/last OBSERVED fill, cumulative observed shares. Taker price improvement is retained.
- `counterparty-fills.csv`: all other fills in the same transactions, including the single aggregate taker.
- `order-creation-context.csv`: latest anonymous book BEFORE client creation minus 60/10/1/0 seconds.
  Context status rejects missing/stale/crossed/reset intervals. Source/sample age and raw quote are retained.
  Clock offsets are unknown; available does NOT certify causal ordering or continuous resting.
- `chain-rpc.jsonl.gz`: original selected tx/receipt/block responses for offline verification.
- `order-errors.csv`: unresolved decode/RPC errors across the whole order cache (not date-filtered).

Dates filter fills by block time. First observed means within THIS export, not first lifetime fill.
The client timestamp is not posting or exchange acknowledgement time. Fill age is not refresh period.
No fills/receipts for cancelled unfilled orders exist in this dataset. No resting ownership is inferred.

## Interpretation

Resting levels are anonymous aggregated liquidity. A size change can be an order,
a cancellation, an execution, or several of these. A recurring size is a candidate
fingerprint, never a labelled order from the target wallet. Compare with public
prints and confirmed wallet fills, and exclude missing/stale/crossed intervals.
Both outcome books can mirror each other. `1 - paired_ask` is not an independent
fair-value estimate and can equal this token's bid. Do not select decisions using
later winners, later fills, or future recovery. Split evaluation by match and date.
Activity/API pagination and polling can miss fills; no observed fill is not proof
that no limit order was placed. Watching a wallet's market after its first fill
does not supply its pre-entry book. Existing historical databases cannot recreate
raw events that were never logged. Use export.py for the compact summary bundle.
"""


def date_argument(value):
    return dt.date.fromisoformat(value).isoformat()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', default='data')
    parser.add_argument('--since', type=date_argument)
    parser.add_argument('--until', type=date_argument)
    parser.add_argument('--orders', help='signed-order SQLite, defaults to <db>/order-research.sqlite')
    parser.add_argument('--config', default='pm.config.json')
    parser.add_argument('--overview', action='store_true', help='compact metadata/order bundle; omit raw frames, public prints and all quote observations')
    parser.add_argument('--out')
    args = parser.parse_args()
    paths = databases(args.db, args.since)
    if args.until:
        paths = [p for p in paths if Path(p).name[3:13] <= args.until]
    if not paths:
        parser.error('no daily databases in the requested date range')
    args.out = args.out or ('research-overview-' if args.overview else 'research-export-') + dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d-%H%M%S')
    result = export_research(paths, args.out, args.orders or str(Path(args.db)/'order-research.sqlite'),
                             load_wallets(args.config), databases(args.db, None), overview=args.overview,
                             requested_since=args.since, requested_until=args.until)
    print(f'Wrote {args.out}/ and {args.out}.zip')
    print(json.dumps(result['counts'], indent=2))
    for warning in result['warnings']:
        print('WARNING: ' + warning)


if __name__ == '__main__':
    main()
