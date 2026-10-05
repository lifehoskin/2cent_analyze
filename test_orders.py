"""Real-receipt and production SQLite/export regressions, no live RPC."""
import copy,gzip,json,sqlite3,subprocess,tempfile,unittest
from pathlib import Path
from decimal import Decimal
from order_decode import decode_transaction,summarize_orders
from order_research import Rpc,open_store,ingest,export_orders,discover
from order_context import creation_context
from research_export import export_research
ROOT=Path(__file__).resolve().parent
with gzip.open(ROOT/'samples-pm/order-transactions.json.gz','rt') as f:CASES=json.load(f)
TARGET='0xe0f6ee3a23385afdf446c324f9fb69364a272ee6'
class FixtureRpc:
 def __init__(self,case):self.case=case
 def call(self,method,params):return self.case[{'eth_getTransactionByHash':'tx','eth_getTransactionReceipt':'receipt','eth_getBlockByHash':'block'}[method]]
class OrderTests(unittest.TestCase):
 def test_old_pending_cache_recovers_to_mined_fill(self):
  with tempfile.TemporaryDirectory() as tmp:
   db=open_store(Path(tmp)/'orders.sqlite');c=copy.deepcopy(CASES[3]);txhash=c['tx']['hash']
   pending=dict(c['tx'],blockHash=None,blockNumber=None)
   key=json.dumps(['eth_getTransactionByHash',[txhash]],separators=(',',':'))
   db.execute('INSERT INTO rpc_cache VALUES(?,?,?,?)',(key,'eth_getTransactionByHash',json.dumps([txhash]),json.dumps(pending)));db.commit()
   class RecoveredRpc(Rpc):
    def _request(self,endpoint,method,params):
     return {'eth_getTransactionByHash':c['tx'],'eth_getTransactionReceipt':c['receipt'],'eth_getBlockByHash':c['block']}[method]
   rpc=RecoveredRpc(db,['fixture']);rpc.checked.add('fixture');ingest(db,rpc,txhash)
   self.assertEqual(db.execute('SELECT status FROM transactions WHERE tx_hash=?',(txhash,)).fetchone()[0],'decoded')
   self.assertEqual(db.execute('SELECT count(*) FROM chain_fills').fetchone()[0],5)
   self.assertEqual(json.loads(db.execute('SELECT result_json FROM rpc_cache WHERE cache_key=?',(key,)).fetchone()[0])['blockHash'],c['tx']['blockHash']);db.close()
 def test_pending_response_not_cached_and_next_pass_recovers(self):
  with tempfile.TemporaryDirectory() as tmp:
   db=open_store(Path(tmp)/'orders.sqlite');c=copy.deepcopy(CASES[3])
   class ChangingRpc(Rpc):
    ready=False
    def _request(self,endpoint,method,params):return c['tx'] if self.ready else dict(c['tx'],blockHash=None,blockNumber=None)
   rpc=ChangingRpc(db,['fixture']);rpc.checked.add('fixture')
   with self.assertRaisesRegex(ValueError,'not mined'):rpc.call('eth_getTransactionByHash',[c['tx']['hash']])
   self.assertEqual(db.execute('SELECT count(*) FROM rpc_cache').fetchone()[0],0)
   rpc.ready=True;self.assertEqual(rpc.call('eth_getTransactionByHash',[c['tx']['hash']])['blockHash'],c['tx']['blockHash']);db.close()
 def test_pending_provider_falls_back_and_mined_cache_is_reused(self):
  with tempfile.TemporaryDirectory() as tmp:
   db=open_store(Path(tmp)/'orders.sqlite');c=copy.deepcopy(CASES[3]);calls=[]
   class LaggingRpc(Rpc):
    def _request(self,endpoint,method,params):
     calls.append(endpoint)
     return dict(c['tx'],blockHash=None,blockNumber=None) if endpoint=='lagging' else c['tx']
   rpc=LaggingRpc(db,['lagging','current']);rpc.checked.update(['lagging','current'])
   for _ in range(2):self.assertEqual(rpc.call('eth_getTransactionByHash',[c['tx']['hash']])['blockHash'],c['tx']['blockHash'])
   self.assertEqual(calls,['lagging','current']);db.close()
 def test_original_and_partial(self):
  fills=[r for c in CASES[:3] for r in decode_transaction(c['tx'],c['receipt'],c['block']) if r['wallet']==TARGET]
  cs=[r for r in fills if r['condition_id'].startswith('0xe64d')];tennis=[r for r in fills if r['condition_id'].startswith('0x114d')]
  self.assertEqual(len(cs),1);self.assertAlmostEqual(cs[0]['client_age_seconds'],2155.775)
  self.assertEqual(len(tennis),2);self.assertEqual(sum(Decimal(r['filled_shares']) for r in tennis),Decimal('88.32'))
  oo=summarize_orders(tennis+tennis);self.assertEqual(len(oo),1);self.assertEqual(oo[0]['observed_fill_count'],2);self.assertEqual(oo[0]['original_shares'],'1000')
 def test_mint_and_single_taker(self):
  c=CASES[3];rs=decode_transaction(c['tx'],c['receipt'],c['block'])
  self.assertEqual(len(rs),5);self.assertEqual(sum(r['role']=='taker' for r in rs),1)
  self.assertEqual([r['limit_price'] for r in rs[:-1]],['0.05','0.04','0.02','0.01']);self.assertTrue(all(r['match_type']=='MINT' for r in rs[:-1]))
  self.assertAlmostEqual(next(r for r in rs if r['wallet']==TARGET)['client_age_seconds'],3.531)
 def test_full_ladder_preserved(self):
  c=CASES[4];rows=[r for r in decode_transaction(c['tx'],c['receipt'],c['block']) if r['wallet']=='0x8840126a43353f6cdcd77e90bf2327474a4ef51a']
  self.assertEqual(len(rows),6);self.assertEqual(len({r['client_timestamp_ms'] for r in rows}),1)
  self.assertEqual([(r['limit_price'],r['original_shares']) for r in rows],[('0.9','200'),('0.85','300'),('0.81','400'),('0.7','500'),('0.5','600'),('0.4','1000')])
  self.assertEqual(len(summarize_orders(rows)),6)
 def test_fail_closed(self):
  for field in ('receipt','input','exchange'):
   c=copy.deepcopy(CASES[3])
   if field=='receipt':c['receipt']['blockHash']='0xwrong'
   if field=='input':c['tx']['input']=c['tx']['input'][:400]
   if field=='exchange':c['tx']['to']='0x'+'1'*40
   with self.assertRaises(ValueError):decode_transaction(c['tx'],c['receipt'],c['block'])
 def test_negative_clock(self):
  c=copy.deepcopy(CASES[3]);c['block']['timestamp']=hex(int(c['block']['timestamp'],16)-100)
  r=next(r for r in decode_transaction(c['tx'],c['receipt'],c['block']) if r['wallet']==TARGET)
  self.assertLess(r['client_age_seconds'],0);self.assertEqual(r['clock_status'],'negative_age')
 def test_store_and_readonly(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'orders.sqlite';db=open_store(p);c=CASES[3]
   ingest(db,FixtureRpc(c),c['tx']['hash']);ingest(db,FixtureRpc(c),c['tx']['hash'])
   self.assertEqual(db.execute('SELECT count(*) FROM chain_fills').fetchone()[0],5)
   with self.assertRaises(ValueError):Rpc(db).call('eth_sendRawTransaction',[])
   _,counts=export_orders(p,Path(tmp)/'out',{TARGET});self.assertEqual(counts['chain_fills'],1);self.assertEqual(counts['counterparty_fills'],4);db.close()
 def test_context_and_export(self):
  with tempfile.TemporaryDirectory() as tmp:
   data=Path(tmp)/'data';c=CASES[3];r=next(r for r in decode_transaction(c['tx'],c['receipt'],c['block']) if r['wallet']==TARGET)
   script="""
    import {Store} from './src/pm/store.mjs';import {MarketCapture} from './src/pm/capture.mjs';
    const [path,asset,time]=process.argv.slice(1);const t=Number(time);
    const store=new Store(path,{now:()=>new Date(t)});const capture=new MarketCapture({store,books:new Map(),sessionId:'test'});
    for(const [delta,price] of [[-2000,'.08'],[1000,'.04']])capture.processBatch([{event_type:'book',asset_id:asset,market:'c',timestamp:String(t+delta),bids:[{price,size:'1000'}],asks:[{price:'.09',size:'50'}]}],{receivedAt:new Date(t+delta).toISOString()});store.close();
   """
   subprocess.run(['node','--input-type=module','-e',script,str(data),r['asset_id'],str(r['client_timestamp_ms'])],cwd=ROOT,check=True)
   daily=list(data.glob('pm-*.sqlite'));ctx=creation_context([r],daily)
   self.assertEqual(json.loads(ctx[-1]['quote_json'])['best_bid'],.08);self.assertEqual(ctx[-1]['context_status'],'available_unverified_clocks')
   with sqlite3.connect(daily[0]) as db:
    db.execute('INSERT INTO market_events VALUES(?,?,?,?,?,?,?)',('test',99,r['client_created_utc'],0,None,'stream_reset',json.dumps({'assets':[r['asset_id']]})))
    db.execute('INSERT INTO trades (ts,wallet,asset_id,side,tx_hash,size,price) VALUES(?,?,?,?,?,?,?)',('2026-07-01T00:00:00Z',TARGET,r['asset_id'],'BUY',r['tx_hash'],1,.02))
   self.assertEqual(creation_context([r],daily)[-1]['context_status'],'capture_reset');self.assertEqual(discover(daily,{TARGET},since='2026-09-01'),{})
   odb=data/'order-research.sqlite';db=open_store(odb);ingest(db,FixtureRpc(c),r['tx_hash']);db.close()
   m=export_research(daily,Path(tmp)/'export',odb,{TARGET},daily);self.assertEqual(m['counts']['chain_fills'],1);self.assertEqual(m['counts']['signed_orders'],1);self.assertTrue((Path(tmp)/'export.zip').exists())
   ov=export_research(daily,Path(tmp)/'overview',odb,{TARGET},daily,overview=True)
   self.assertEqual(ov['counts']['signed_orders'],1);self.assertEqual(ov['counts']['order_creation_context'],4)
   self.assertEqual((Path(tmp)/'overview/signed-orders.csv').read_text(),(Path(tmp)/'export/signed-orders.csv').read_text())
   self.assertFalse((Path(tmp)/'overview/market-events.jsonl.gz').exists())
if __name__=='__main__':unittest.main()
