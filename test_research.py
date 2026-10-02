"""Regressions for duplicate fills, independent wallets and continuous export."""
import csv
import gzip
import json
import sqlite3
import subprocess
import tempfile
from pathlib import Path

from analyze_fills import build_positions
from export import select_series
from research_export import export_research
from trade_identity import unique_fills

fill = dict(ts='2026-09-16T12:00:00Z', tx_hash='0xabc', wallet='0xA', asset_id='a',
            side='BUY', size='200.0', price='0.90', fill_index='99')
assert len(unique_fills([fill, {**fill, 'size': 200, 'price': .9, 'fill_index': 2}])) == 1
other_wallet = {**fill, 'wallet': '0xB'}
assert len(build_positions([fill, fill, other_wallet])) == 2
assert len(unique_fills([{**fill, 'tx_hash': ''}] * 2)) == 2
assert len(unique_fills([fill, {**fill, 'price': '.85'}])) == 2

cheap = [dict(ts=f'2026-09-16T12:{i:02d}:00Z', bid_after=.02, size_consumed=100)
         for i in range(20)]
large = [dict(ts=f'2026-09-16T13:{i:02d}:00Z', bid_after=.999, size_consumed=1000000)
         for i in range(20)]
picked = select_series(cheap + large, 8)
assert len(picked) == 8 and sum(r['bid_after'] <= .05 for r in picked) == 6
assert len(select_series(cheap, 8)) == 8
assert len(select_series(large, 8)) == 8
assert select_series(cheap, 0) == []

ROOT = Path(__file__).resolve().parent
with tempfile.TemporaryDirectory() as tmp:
    data, out = Path(tmp) / 'data', Path(tmp) / 'out'
    # Use production Store/Capture schema; then duplicate the same API fill into
    # two daily files, reproducing repeated history polling after midnight.
    script = """
      import { Store } from './src/pm/store.mjs';
      import { MarketCapture } from './src/pm/capture.mjs';
      const store = new Store(process.argv[1], {now: () => new Date('2026-09-16T12:00:00Z')});
      const capture = new MarketCapture({store, books: new Map(), sessionId: 'export-test'});
      capture.processBatch([{event_type:'book', asset_id:'a', market:'c', timestamp:'1000',
        bids:[{price:'.02',size:'800'}], asks:[{price:'.03',size:'100'}]}]);
      capture.processBatch([{event_type:'last_trade_price',asset_id:'a',price:'.02',size:'10'}]);
      capture.checkpoint();
      store.add('trades', ['2026-09-16T12:00:00Z','c','a','0xa','BUY',.02,10,'maker','0xabc']);
      store.add('universe', ['2026-09-16T12:00:00Z','c','gamma',0,null,'capacity','A vs B','tennis','match','winner']);
      store.add('universe', ['2026-09-16T12:00:00Z','c','gamma',1,null,'','A vs B','tennis','match','winner']);
      store.close();
    """
    subprocess.run(['node', '--input-type=module', '-e', script, str(data)], cwd=ROOT, check=True)
    first = data / 'pm-2026-09-16.sqlite'
    second = data / 'pm-2026-09-17.sqlite'
    with sqlite3.connect(first) as a, sqlite3.connect(second) as b:
        assert a.execute('SELECT subscribed,reason_skipped FROM universe WHERE condition_id="c"').fetchone() == (1, '')
        a.backup(b)
        b.execute('DELETE FROM market_events')
        b.execute('DELETE FROM quote_observations')
    result = export_research([first, second], out)
    assert result['counts']['market_events'] == 3
    assert result['counts']['market_trades'] == 1
    assert result['counts']['unique_fills'] == 1
    assert result['counts']['duplicate_fills_removed'] == 1
    with gzip.open(out / 'market-events.jsonl.gz', 'rt') as handle:
        events = [json.loads(line) for line in handle]
    assert events[-1]['event_type'] == 'checkpoint'
    assert events[-1]['payload'][0]['bids'][0]['size'] == 800
    with gzip.open(out / 'quote-observations.csv.gz', 'rt') as handle:
        quotes = list(csv.DictReader(handle))
    assert len(quotes) == 2 and quotes[-1]['trigger'] == 'checkpoint'
    with (out / 'target-fills.csv').open() as handle:
        fills = list(csv.DictReader(handle))
    assert len(fills) == 1 and fills[0]['fill_index'] == '1'
    assert Path(str(out) + '.zip').exists()
    # Overview retains audit events and source counts without copying raw books.
    with sqlite3.connect(first) as db:
        db.execute('INSERT INTO market_events VALUES(?,?,?,?,?,?,?)',
                   ('export-test',4,'2026-09-16T12:00:01Z',1,None,'stream_reset',json.dumps({'assets':['a']})))
        db.execute('INSERT INTO market_events VALUES(?,?,?,?,?,?,?)',
                   ('export-test',5,'2026-09-16T12:00:02Z',2,None,'schedule_decision',json.dumps({'conditionId':'c','status':'capacity'})))
    small_out = Path(tmp) / 'overview'
    overview = export_research([first, second], small_out, overview=True)
    assert overview['mode'] == 'overview'
    assert overview['counts']['market_events'] == 5
    assert overview['counts']['capture_metadata_exported'] == 2
    assert 'market_trades' not in overview['counts']
    assert not any((small_out / n).exists() for n in overview['omitted_files'])
    with gzip.open(small_out / 'capture-metadata.jsonl.gz', 'rt') as handle:
        assert [json.loads(line)['event_type'] for line in handle] == ['stream_reset','schedule_decision']
    assert (small_out / 'target-fills.csv').read_text() == (out / 'target-fills.csv').read_text()
    assert (small_out / 'universe.csv.gz').exists()
    # The real CLI recognizes the new option and produces the promised ZIP.
    cli_out = Path(tmp) / 'cli-overview'
    subprocess.run(['python3','research_export.py','--db',str(data),'--since','2026-09-16',
                    '--until','2026-09-17','--overview','--out',str(cli_out)], cwd=ROOT,
                   check=True, capture_output=True, text=True)
    assert Path(str(cli_out) + '.zip').exists()
    missing_out = Path(tmp) / 'missing-dates'
    missing = export_research([first, second], missing_out, overview=True,
                             requested_since='2026-09-15', requested_until='2026-09-18')
    assert missing['missing_daily_databases'] == ['2026-09-15', '2026-09-18']
    assert any('No daily databases' in warning for warning in missing['warnings'])
    import zipfile
    with zipfile.ZipFile(str(missing_out) + '.zip') as archive:
        assert json.loads(archive.read('manifest.json'))['missing_daily_databases'] == missing['missing_daily_databases']
    # A later daily DB contains only re-polled history, not new fills.
    history = export_research([second], Path(tmp) / 'history')
    assert history['counts']['target_fills_in_selected_days'] == 0
    assert history['counts']['historical_api_fills'] == 1
    with (Path(tmp) / 'history' / 'historical-target-fills.csv').open() as handle:
        assert len(list(csv.DictReader(handle))) == 1
    try:
        export_research([first], out)
    except ValueError:
        pass
    else:
        raise AssertionError('existing output must not be silently removed')
    # A pre-upgrade database is usable, with an explicit missing-journal warning.
    legacy = Path(tmp) / 'pm-2026-09-15.sqlite'
    with sqlite3.connect(legacy) as db:
        db.execute('CREATE TABLE trades (ts TEXT, wallet TEXT, asset_id TEXT, side TEXT, tx_hash TEXT)')
    old = export_research([legacy], Path(tmp) / 'old')
    assert len(old['warnings']) == 2

print('all research tests passed')
