"""Bounded export regressions: bootstrap, resets, atomic frames and day rotation."""
import csv
import gzip
import io
import json
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path
from research_cases import export_cases, select_cases


def csv_bytes(rows, fields):
    f = io.StringIO(); w = csv.DictWriter(f, fields); w.writeheader(); w.writerows(rows)
    return f.getvalue().encode()


class CaseExportTests(unittest.TestCase):
    def test_preserves_replay_boundaries_and_atomic_frames(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); overview = root/'overview.zip'; out = root/'cases.zip'
            with zipfile.ZipFile(overview, 'w') as z:
                z.writestr('order-creation-context.csv', csv_bytes([
                    dict(asset_id='a', quote_json='{}'), dict(asset_id='unused', quote_json='')], ['asset_id','quote_json']))
                z.writestr('markets.csv.gz', gzip.compress(csv_bytes([
                    dict(condition_id='c',asset_id_a='a',asset_id_b='b',slug='test')],
                    ['condition_id','asset_id_a','asset_id_b','slug'])))
                z.writestr('signed-orders.csv',csv_bytes([
                    dict(condition_id='c',client_created_utc='2026-09-28T23:59:50.000Z',last_observed_fill_utc='2026-09-29T00:00:05.000Z',order_hash='o'),
                    dict(condition_id='c',client_created_utc='2026-09-29T00:00:10.000Z',last_observed_fill_utc='2026-09-29T00:00:15.000Z',order_hash='exit')],
                    ['condition_id','client_created_utc','last_observed_fill_utc','order_hash']))
            whole_frame = [dict(event_type='price_change',market='c',price_changes=[
                dict(asset_id='a',price='.02',size='1000',side='BUY'),
                dict(asset_id='b',price='.98',size='1000',side='SELL'),
                dict(asset_id='other',price='.5',size='42',side='BUY')]),
                dict(event_type='last_trade_price',asset_id='a',price='.02',size='5')]
            events = [
                ('2026-09-28T23:57:00.000Z','frame',[dict(asset_id='a',outside=True)]),
                ('2026-09-28T23:58:00.000Z','checkpoint',[dict(asset_id='a',bids=[dict(price='.02',size='50')]),dict(asset_id='b'),dict(asset_id='other')]),
                ('2026-09-28T23:59:55.000Z','frame',whole_frame),
                ('2026-09-29T00:00:00.000Z','stream_reset',dict(assets=['a','b','other'],reason='close')),
                ('2026-09-29T00:00:01.000Z','checkpoint',[]),
                ('2026-09-29T00:00:02.000Z','frame',[dict(event_type='price_change',market='c',price_changes=[dict(asset_id='a',price='.02',size='0')])]),
                ('2026-09-29T00:00:03.000Z','frame',[dict(event_type='book',asset_id='a',market='c',bids=[],asks=[])]),
                ('2026-09-29T00:00:04.000Z','frame',[dict(event_type='book',asset_id='other')]),
                ('2026-09-29T00:00:25.000Z','frame',[dict(asset_id='a',outside=True)]),
            ]
            for day in ('2026-09-28','2026-09-29'):
                with sqlite3.connect(root/f'pm-{day}.sqlite') as db:
                    db.executescript('CREATE TABLE market_events(session_id TEXT,seq INTEGER,received_at TEXT,monotonic_ms REAL,connection_id TEXT,event_type TEXT,payload_json TEXT,PRIMARY KEY(session_id,seq)); CREATE INDEX market_events_received ON market_events(received_at);')
                    for n,(ts,kind,payload) in enumerate(events,1):
                        if ts[:10]==day:
                            db.execute('INSERT INTO market_events VALUES(?,?,?,?,?,?,?)',('session',n,ts,n,'conn',kind,json.dumps(payload)))
            original = {p.name:p.read_bytes() for p in root.glob('*.sqlite')}
            result = export_cases(root,overview,out,before=0,after=0,warmup=120)
            case = result['cases'][0]
            self.assertEqual(case['order_hashes'], ['o','exit'])
            self.assertEqual(case['missing_days'], [])
            with zipfile.ZipFile(out) as z:
                rows=[json.loads(line) for line in gzip.decompress(z.read(case['file'])).splitlines()]
                self.assertEqual([r['seq'] for r in rows],[2,3,4,5,6,7])
                self.assertEqual(rows[1]['payload'],whole_frame)
                self.assertEqual(rows[0]['payload'],[dict(asset_id='a',bids=[dict(price='.02',size='50')]),dict(asset_id='b')])
                self.assertEqual(rows[2]['payload']['assets'],['a','b','other'])
                self.assertEqual(rows[3]['payload'],[])
                self.assertIn('overview/signed-orders.csv', z.namelist())
                self.assertEqual(json.loads(z.read('case-manifest.json'))['mode'],'targeted_cases')
            self.assertEqual(original,{p.name:p.read_bytes() for p in root.glob('*.sqlite')})
            with self.assertRaises(ValueError):export_cases(root,overview,out)

    def test_missing_day_and_no_frames_are_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            with sqlite3.connect(root/'pm-2026-09-28.sqlite') as db:db.execute('CREATE TABLE placeholder(x)')
            with zipfile.ZipFile(root/'overview.zip','w') as z:z.writestr('manifest.json','{}')
            (root/'selection.json').write_text(json.dumps(dict(cases=[dict(condition_id='c',asset_ids=['a','b'],slug='gap',since='2026-09-29T00:10:00Z',until='2026-09-29T00:11:00Z')])) )
            r=export_cases(root,root/'overview.zip',root/'out.zip',root/'selection.json')
            self.assertEqual(r['cases'][0]['missing_days'],['2026-09-29'])
            self.assertTrue(r['cases'][0]['warnings'])


if __name__=='__main__':unittest.main()
