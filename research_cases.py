#!/usr/bin/env python3
"""Export selected continuous book windows directly from read-only daily databases.

python3 research_cases.py --db data --overview research-overview-YYYYMMDD-HHMMSS.zip
Only the standard library is needed. No full multi-gigabyte export is required.
"""
import argparse
import csv
import datetime as dt
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import shutil
import sqlite3
import time
import zipfile
from collections import Counter


def timestamp(value):
    result = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('timestamps must include a timezone')
    return result.astimezone(dt.timezone.utc)


def iso(value):
    return value.isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def table(archive, name):
    with archive.open(name) as source:
        stream = gzip.GzipFile(fileobj=source) if name.endswith('.gz') else source
        with io.TextIOWrapper(stream, encoding='utf-8-sig', newline='') as text:
            return list(csv.DictReader(text))


def select_cases(archive, before=900, after=900):
    contexts = table(archive, 'order-creation-context.csv')
    # A retained quote is only a candidate, not certification of clock alignment.
    assets = {r['asset_id'] for r in contexts if r.get('quote_json')}
    orders = table(archive, 'signed-orders.csv')
    markets = {}
    for row in table(archive, 'markets.csv.gz'):
        if assets & {row['asset_id_a'], row['asset_id_b']}:
            markets[row['condition_id']] = row
    mapped = {m[k] for m in markets.values() for k in ('asset_id_a', 'asset_id_b')}
    if assets - mapped:
        raise ValueError('A retained quote has no paired market metadata')
    cases = []
    for condition, market in sorted(markets.items()):
        group = [o for o in orders if o['condition_id'] == condition]
        if not group:
            continue
        cases.append({
            'condition_id': condition, 'asset_ids': [market['asset_id_a'], market['asset_id_b']],
            'slug': market['slug'],
            'since': iso(min(timestamp(o['client_created_utc']) for o in group) - dt.timedelta(seconds=before)),
            'until': iso(max(timestamp(o['last_observed_fill_utc']) for o in group) + dt.timedelta(seconds=after)),
            'order_hashes': [o['order_hash'] for o in group],
            'reason': 'all markets with any retained creation quote; includes other orders and both outcomes',
        })
    if not cases:
        raise ValueError('No markets with retained creation context; provide --selection instead')
    return cases


def project(event_type, payload, assets, condition):
    """Keep complete relevant frames/resets; project checkpoint book sets only."""
    if event_type == 'capture_start':
        return payload
    if event_type == 'checkpoint':
        # Keep even an empty checkpoint: it invalidates previously known case books.
        return [b for b in payload if str(b.get('assetId', b.get('asset_id', ''))) in assets]
    if event_type == 'stream_reset':
        return payload if assets.intersection(map(str, payload.get('assets', []))) else None
    if event_type == 'schedule_decision':
        return payload if payload.get('conditionId') == condition else None
    return payload


README = """# Targeted research cases

Source daily SQLite databases are read-only and remain unchanged. The original
overview tables are under overview/; their counts do NOT describe this subset.
case-manifest.json records selection, database coverage, counts and warnings.

Each cases/*/market-events.jsonl.gz is a SEPARATE replay window. Start with no
known books. Replay each session separately, sorting by seq (including across
UTC files); never bridge sessions or cases. Frames and relevant stream_reset
payloads are retained whole. Checkpoints are projected onto the two case tokens,
including empty sets. A checkpoint replaces the known case book set; it is not
a new exchange quote. A capture_start resets the entire session. A stream_reset
invalidates affected tokens until a full book/checkpoint establishes them again.
Ignore deltas until a valid full book exists. Missing state is unknown, not zero.

There are 120 seconds of warm-up before the requested analysis window by default.
This may contain a bootstrap checkpoint, but does NOT guarantee one or continuous
coverage. Any bootstrap gap remains unknown. Sequence gaps are expected because
unrelated market frames are omitted; they alone are not evidence of a data loss.
All matching frames within each exported window are retained, without sweep or
volume sampling. Public last_trade_price messages remain inside these frames.

Selection depends on available context around FILLED orders. It is not a random
market sample, a sample of every open/cancelled order, or a profitability study.
Receive, source, client-signing and block clocks are distinct and uncalibrated.
Anonymous book levels do not identify wallet owners. The client timestamp is not
an exchange acknowledgement or proof of continuous resting time.
"""


def export_cases(db_dir, overview, output, selection=None, before=900, after=900, warmup=120):
    if min(before, after, warmup) < 0:
        raise ValueError('window durations must be nonnegative')
    overview, output = Path(overview), Path(output)
    if output.exists():
        raise ValueError(f'{output} already exists; choose another --out')
    paths = sorted(Path(db_dir).glob('pm-????-??-??.sqlite'))
    if not paths:
        raise ValueError('No daily pm-YYYY-MM-DD.sqlite databases found in --db')
    digest = hashlib.sha256()
    with overview.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024**2), b''):
            digest.update(chunk)
    manifest = dict(version=1, mode='targeted_cases', overview=overview.name,
                    overview_sha256=digest.hexdigest(), created_at=iso(dt.datetime.now(dt.timezone.utc)),
                    warmup_seconds=warmup, selection_basis='explicit' if selection else 'retained creation quotes',
                    cases=[], warnings=[])
    with zipfile.ZipFile(overview) as source:
        cases = json.loads(Path(selection).read_text())['cases'] if selection else select_cases(source, before, after)
        if not cases:
            raise ValueError('Empty case selection')
        # Validate everything before creating the output.
        for case in cases:
            if len(set(case['asset_ids'])) != 2 or not all(case['asset_ids']):
                raise ValueError('Each case must identify two distinct outcome tokens')
            if timestamp(case['since']) > timestamp(case['until']):
                raise ValueError('Case since must precede until')
        print(f'Selected {len(cases)} markets. Reading only their time windows; output: {output}', flush=True)
        with zipfile.ZipFile(output, 'x', allowZip64=True) as archive:
            allowed = ('manifest.json', 'README.md', 'capture-metadata.jsonl.gz', 'markets.csv.gz',
                       'universe.csv.gz', 'gaps.csv.gz', 'target-fills.csv', 'historical-target-fills.csv',
                       'signed-orders.csv', 'chain-fills.csv', 'counterparty-fills.csv',
                       'order-creation-context.csv', 'order-errors.csv', 'chain-rpc.jsonl.gz')
            for name in allowed:
                if name not in source.namelist():
                    continue
                info = zipfile.ZipInfo('overview/' + name)
                info.compress_type = zipfile.ZIP_STORED if name.endswith('.gz') else zipfile.ZIP_DEFLATED
                with source.open(name) as inp, archive.open(info, 'w', force_zip64=True) as out:
                    shutil.copyfileobj(inp, out, 1024**2)
            for index, case in enumerate(cases, 1):
                slug = re.sub(r'[^a-zA-Z0-9_-]', '-', case.get('slug') or case['condition_id'])[:100]
                name = f'cases/{index:02d}-{slug}/market-events.jsonl.gz'
                lo = iso(timestamp(case['since']) - dt.timedelta(seconds=warmup))
                hi = iso(timestamp(case['until']))
                assets = set(map(str, case['asset_ids']))
                pattern = re.compile('|'.join(re.escape(json.dumps(x)) for x in assets | {case['condition_id']}))
                record = dict(case, file=name, export_since=lo, export_until=hi,
                              databases=[], missing_days=[], counts={}, sessions=[], warnings=[])
                counts = Counter(); sessions = set(); last_progress = time.monotonic()
                selected_paths = [p for p in paths if lo[:10] <= p.name[3:13] <= hi[:10]]
                dates = {p.name[3:13] for p in selected_paths}
                date = timestamp(lo).date()
                while date <= timestamp(hi).date():
                    if date.isoformat() not in dates:
                        record['missing_days'].append(date.isoformat())
                    date += dt.timedelta(days=1)
                print(f'[{index}/{len(cases)}] {slug}: {lo} .. {hi}', flush=True)
                with archive.open(name, 'w', force_zip64=True) as member:
                    with gzip.GzipFile(fileobj=member, mode='wb', compresslevel=6, mtime=0) as stream:
                        for path in selected_paths:
                            db = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)
                            db.row_factory = sqlite3.Row
                            try:
                                db.execute('BEGIN')
                                exists = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='market_events'").fetchone()
                                if not exists:
                                    record['warnings'].append(path.name + ': no market_events table'); continue
                                record['databases'].append(path.name)
                                # Production schema indexes received_at. Never scan whole days or mutate indexes.
                                rows = db.execute('SELECT * FROM market_events WHERE received_at>=? AND received_at<=? ORDER BY received_at', (lo, hi))
                                for row in rows:
                                    counts['scanned_rows'] += 1
                                    if time.monotonic() - last_progress > 10:
                                        print(f'  scanned {counts["scanned_rows"]:,}; kept {counts["exported_rows"]:,}', flush=True)
                                        last_progress = time.monotonic()
                                    kind = row['event_type']; raw = row['payload_json']
                                    if kind not in ('checkpoint', 'capture_start') and not pattern.search(raw):
                                        continue
                                    payload = project(kind, json.loads(raw), assets, case['condition_id'])
                                    if payload is None:
                                        continue
                                    event = dict(row); event.pop('payload_json')
                                    event.update(database=path.name, payload=payload)
                                    stream.write((json.dumps(event, separators=(',', ':')) + '\n').encode())
                                    counts['exported_rows'] += 1; counts[kind] += 1; sessions.add(row['session_id'])
                                    if kind == 'checkpoint' and payload:
                                        counts['nonempty_checkpoints'] += 1
                            finally:
                                db.close()
                if not counts['frame']:
                    record['warnings'].append('No matching raw frames in this window; no book coverage is established')
                if not counts['nonempty_checkpoints']:
                    record['warnings'].append('No nonempty checkpoint; replay must wait for complete book snapshots')
                record['counts'] = dict(counts); record['sessions'] = sorted(sessions)
                manifest['cases'].append(record)
                print(f'  kept {counts["exported_rows"]:,} rows; {counts["frame"]:,} frames', flush=True)
            archive.writestr('README.md', README, compress_type=zipfile.ZIP_DEFLATED)
            archive.writestr('case-manifest.json', json.dumps(manifest, indent=2) + '\n', compress_type=zipfile.ZIP_DEFLATED)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', default='data', help='directory containing daily SQLite files')
    parser.add_argument('--overview', required=True, help='the previously shared overview ZIP')
    parser.add_argument('--selection', help='optional JSON with explicit cases (including control windows)')
    parser.add_argument('--before', type=int, default=900, help='seconds before earliest client creation per market')
    parser.add_argument('--after', type=int, default=900, help='seconds after last observed fill per market')
    parser.add_argument('--warmup', type=int, default=120, help='extra replay bootstrap seconds')
    parser.add_argument('--out', help='new output ZIP; existing files are never overwritten')
    args = parser.parse_args()
    output = args.out or 'research-cases-' + dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d-%H%M%S') + '.zip'
    try:
        result = export_cases(args.db, args.overview, output, args.selection, args.before, args.after, args.warmup)
    except (ValueError, OSError, sqlite3.Error, KeyError, zipfile.BadZipFile) as exc:
        parser.exit(1, f'ERROR: {exc}\nA partial output, if present, is not a completed export.\n')
    print(f'Wrote {output} ({Path(output).stat().st_size / 1024**2:.1f} MiB)', flush=True)
    for case in result['cases']:
        for warning in case['warnings']:
            print('WARNING: ' + case['slug'] + ': ' + warning)
        if case['missing_days']:
            print('WARNING: ' + case['slug'] + ': missing daily databases ' + ', '.join(case['missing_days']))


if __name__ == '__main__':
    main()
