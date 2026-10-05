#!/usr/bin/env python3
"""Read-only signed-order enrichment. Run beside the book collector.

python3 order_research.py --watch --db data
Only transaction/receipt/block read RPCs are permitted. No keys or trading calls.
"""
from contextlib import closing
import argparse
import csv
import gzip
import json
import re
import sqlite3
import time
import urllib.request
from pathlib import Path
from order_decode import decode_transaction, summarize_orders

RPCS = ('https://polygon-bor-rpc.publicnode.com', 'https://polygon.drpc.org')
METHODS = {'eth_chainId', 'eth_getTransactionByHash', 'eth_getTransactionReceipt', 'eth_getBlockByHash'}
HASH = re.compile(r'^0x[0-9a-fA-F]{64}$')


def readonly(path):
    db = sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    return db


def open_store(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.executescript('''PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS rpc_cache (cache_key TEXT PRIMARY KEY, method TEXT, params_json TEXT, result_json TEXT);
    CREATE TABLE IF NOT EXISTS transactions (tx_hash TEXT PRIMARY KEY, status TEXT, checked_at REAL, error TEXT);
    CREATE TABLE IF NOT EXISTS chain_fills (chain_id INTEGER, tx_hash TEXT, log_index INTEGER,
      wallet TEXT, side TEXT, block_timestamp INTEGER, record_json TEXT,
      PRIMARY KEY(chain_id,tx_hash,log_index));
    CREATE INDEX IF NOT EXISTS chain_fills_time ON chain_fills(block_timestamp);
    ''')
    return db


class Rpc:
    def __init__(self, db, endpoints=RPCS):
        self.db, self.endpoints, self.checked = db, endpoints, set()

    def _request(self, endpoint, method, params):
        request = urllib.request.Request(endpoint,
            data=json.dumps(dict(jsonrpc='2.0',id=1,method=method,params=params)).encode(),
            headers={'Content-Type':'application/json','User-Agent':'Mozilla/5.0 (compatible; Polymarket-order-research/1.0)'})
        with urllib.request.urlopen(request, timeout=25) as response:
            payload = json.load(response)
        if payload.get('error') or payload.get('result') is None:
            raise ValueError('RPC error or missing historical result: '+str(payload.get('error', 'null'))[:200])
        return payload['result']

    def call(self, method, params):
        if method not in METHODS:
            raise ValueError('RPC method is not read-only allowlisted')
        key = json.dumps([method, params], separators=(',', ':'))
        old = self.db.execute('SELECT result_json FROM rpc_cache WHERE cache_key=?', (key,)).fetchone()
        if old:
            result = json.loads(old[0])
            if self._is_mined_result(method, result):
                return result
            # Older versions cached pending transactions permanently. Discard
            # that transient response so the normal retry can observe mining.
            self.db.execute('DELETE FROM rpc_cache WHERE cache_key=?', (key,))
            self.db.commit()
        errors = []
        for endpoint in self.endpoints:
            try:
                if endpoint not in self.checked:
                    if int(self._request(endpoint, 'eth_chainId', []),16) != 137:
                        raise ValueError('RPC is not Polygon chain 137')
                    self.checked.add(endpoint)
                result = self._request(endpoint, method, params)
                if not self._is_mined_result(method, result):
                    raise ValueError('transaction not mined')
                self.db.execute('INSERT OR REPLACE INTO rpc_cache VALUES(?,?,?,?)',
                                (key, method, json.dumps(params), json.dumps(result)))
                self.db.commit()
                return result
            except (ValueError, OSError) as error:
                # Do not expose endpoint credentials in exported errors.
                errors.append(type(error).__name__+': '+str(error).split('https://')[0][:200])
        raise ValueError('; '.join(errors))

    @staticmethod
    def _is_mined_result(method, result):
        if method in ('eth_getTransactionByHash', 'eth_getTransactionReceipt'):
            return isinstance(result, dict) and result.get('blockHash') is not None and result.get('blockNumber') is not None
        return True


def load_wallets(config):
    return {a.lower() for a in json.loads(Path(config).read_text())['wallets']['addresses']}


def discover(paths, wallets, since=None, csv_path=None):
    hashes = {}
    def add(h, ts):
        if HASH.fullmatch(h or '') and (not since or str(ts)[:10] >= since):
            hashes[h.lower()] = max(hashes.get(h.lower(), ''), str(ts))
    for path in paths:
        with closing(readonly(path)) as db:
            tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            for table, wallet_column in [('wallets','address'), ('trades','wallet')]:
                if table not in tables:
                    continue
                for row in db.execute(f'SELECT * FROM {table}'):
                    if row[wallet_column].lower() in wallets and (table != 'wallets' or row['type']=='TRADE'):
                        add(row['tx_hash'], row['ts'])
    if csv_path:
        with open(csv_path, newline='', encoding='utf-8-sig') as handle:
            for r in csv.DictReader(handle):
                add(r.get('tx_hash') or r.get('transactionHash'), r.get('fill_ts_utc') or r.get('ts') or '')
    return hashes


def ingest(db, rpc, tx_hash):
    tx = rpc.call('eth_getTransactionByHash', [tx_hash])
    receipt = rpc.call('eth_getTransactionReceipt', [tx_hash])
    if not tx.get('blockHash'):
        raise ValueError('transaction not mined')
    block = rpc.call('eth_getBlockByHash', [tx['blockHash'], False])
    if tx['hash'].lower() != tx_hash.lower():
        raise ValueError('RPC returned a different transaction')
    records = decode_transaction(tx, receipt, block)
    with db:
        for r in records:
            db.execute('INSERT OR REPLACE INTO chain_fills VALUES(?,?,?,?,?,?,?)',
                       (137,r['tx_hash'],r['log_index'],r['wallet'],r['side'],r['block_timestamp'],json.dumps(r)))
        db.execute('INSERT OR REPLACE INTO transactions VALUES(?,?,?,?)', (tx_hash,'decoded',time.time(),None))
    return records


def write_csv(path, rows):
    fields = list(dict.fromkeys(k for row in rows for k in row))
    with Path(path).open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fields)
        writer.writeheader(); writer.writerows(rows)


def export_orders(path, out, wallets, since=None, until=None):
    out = Path(out); out.mkdir(parents=True, exist_ok=True)
    with closing(readonly(path)) as db:
        all_rows = [json.loads(r[0]) for r in db.execute('SELECT record_json FROM chain_fills ORDER BY block_timestamp,tx_hash,log_index')]
        all_rows = [r for r in all_rows if (not since or r['fill_utc'][:10]>=since) and (not until or r['fill_utc'][:10]<=until)]
        target = [r for r in all_rows if r['wallet'] in wallets]
        hashes = {r['tx_hash'] for r in target}
        counterparties = [r for r in all_rows if r['tx_hash'] in hashes and r['wallet'] not in wallets]
        orders = summarize_orders(target)
        for name, rows in [('chain-fills.csv',target),('signed-orders.csv',orders),('counterparty-fills.csv',counterparties)]:
            write_csv(out/name, rows)
        errors = [dict(r) for r in db.execute("SELECT * FROM transactions WHERE status != 'decoded'")]
        write_csv(out/'order-errors.csv', errors)
        # Preserve source responses for selected transactions and their blocks.
        blocks = {r['block_hash'] for r in target}
        with gzip.open(out/'chain-rpc.jsonl.gz','wt',encoding='utf-8') as f:
            for row in db.execute('SELECT method,params_json,result_json FROM rpc_cache'):
                params=json.loads(row['params_json'])
                if params and params[0] in hashes|blocks:
                    f.write(json.dumps(dict(method=row['method'],params=params,result=json.loads(row['result_json'])))+'\n')
    return orders, dict(chain_fills=len(target),signed_orders=len(orders),counterparty_fills=len(counterparties),order_errors=len(errors))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', default='data'); parser.add_argument('--config',default='pm.config.json')
    parser.add_argument('--out'); parser.add_argument('--from-csv'); parser.add_argument('--since')
    parser.add_argument('--watch',action='store_true'); parser.add_argument('--interval',type=float,default=60)
    parser.add_argument('--limit',type=int,default=100,help='maximum transactions per pass; watch drains backlog')
    args=parser.parse_args()
    if args.limit < 1 or args.interval < 1:
        parser.error('limit and interval must be positive')
    wallets=load_wallets(args.config); db=open_store(args.out or str(Path(args.db)/'order-research.sqlite')); rpc=Rpc(db)
    try:
        while True:
            hashes=discover(sorted(Path(args.db).glob('pm-????-??-??.sqlite')),wallets,args.since,args.from_csv)
            statuses={r['tx_hash']:dict(r) for r in db.execute('SELECT * FROM transactions')}
            pending=[h for h in hashes if h not in statuses or (statuses[h]['status']!='decoded' and time.time()-statuses[h]['checked_at']>=300)]
            pending.sort(key=lambda h:(h not in statuses,hashes[h]),reverse=True)
            counts=dict(decoded=0,error=0,pending=len(pending))
            for h in pending[:args.limit]:
                try:ingest(db,rpc,h);counts['decoded']+=1
                except (ValueError,KeyError,TypeError,OSError) as e:
                    with db:db.execute('INSERT OR REPLACE INTO transactions VALUES(?,?,?,?)',(h,'error',time.time(),str(e)[:500]))
                    counts['error']+=1
            print('[orders]',json.dumps(counts),flush=True)
            if not args.watch:break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        db.close()

if __name__=='__main__':main()
