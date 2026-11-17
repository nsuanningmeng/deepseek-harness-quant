"""Render execution explanations against synthetic API state, without network I/O."""
import json
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[1]


class PaperExecutionPresentationTests(unittest.TestCase):
    def run_ui(self, body):
        script = r'''
const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const source=fs.readFileSync(PAGE,'utf8').match(/<script>([\s\S]*?)<\/script>/)[1];
const elements={};
function element(id){return elements[id] ||= {textContent:'',innerHTML:'',value:'',style:{},dataset:{},addEventListener(){}};}
const sandbox={document:{getElementById:element,querySelectorAll(){return [];}},
  LW:{sidebar:{render(){}},render:{mount(id,html){element(id).innerHTML=html;}},
      api:{get(){throw Error('unexpected API request');}}},
  fetch(){throw Error('unexpected fetch');},setTimeout(){},clearTimeout(){}};
const hook='  globalThis.ui={renderAi,renderMarket,records(d,p){latest=d;panel=p;renderRecords();}};\n})();';
assert(source.includes('  load();\n})();'));
vm.runInNewContext(source.replace('  load();\n})();',hook),sandbox);
function order(){return {id:'synthetic-order',symbol:'600000.SH',side:'BUY',quantity:100,filled:0,
  limit_price:'10.00',status:'OPEN',created_at:'2026-11-13T10:00:00+08:00'};}
function check(){return {checked_at:'2026-11-13T10:01:02+08:00',quote_time:'2026-11-13T10:01:00+08:00',
  received_at:'2026-11-13T10:01:01+08:00',bid:'9.99',ask:'10.00',execution_price:'10.01',limit_price:'10.00',
  slippage_bps:5,capacity:500,remaining_before:100,filled_quantity:0,fill_id:null,
  outcome:'skipped',reason:'limit_price_not_reached',reason_label:'含滑点撮合价未达到委托限价'};}
BODY
'''.replace('PAGE', json.dumps(str(ROOT / 'ui_v2/pages/paperlive.html'))).replace('BODY', body)
        result = subprocess.run(['node', '-e', script], cwd=ROOT, text=True,
                                capture_output=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_order_explains_quote_limit_capacity_and_actual_fill_separately(self):
        self.run_ui(r'''
const o=order();o.last_match_check=check();
const d={market:{open:true},account:{orders:[o],execution_diagnostics:{coverage:'合成诊断覆盖范围'}}};
sandbox.ui.records(d,'orders');
let html=element('records-table').innerHTML;
for(const text of ['含滑点撮合价未达到委托限价','10:01:00','10:01:01','10:01:02',
  '买一 / 卖一 9.99 / 10.00 元','含滑点试算价 10.01 元','限价 10.00 元',
  '本帧可用量 500 股','本次实际成交 0 股'])assert(html.includes(text),text);
assert(!html.includes('全部成交'));
assert.equal(element('match-coverage').textContent,'合成诊断覆盖范围');
o.status='FILLED';o.filled=100;o.last_match_check={...check(),filled_quantity:100,
  fill_id:'synthetic-fill',reason_label:'本次报价已完成剩余成交',outcome:'filled'};
sandbox.ui.records(d,'orders');html=element('records-table').innerHTML;
assert(html.includes('全部成交'));assert(html.includes('本次实际成交 100 股'));
assert(html.includes('成交编号 synthetic-fill'));assert(!html.includes('撤销模拟委托'));
''')

    def test_missing_historical_diagnostic_does_not_infer_from_current_quote(self):
        self.run_ui(r'''
const o=order();o.status='CANCELLED';o.last_match_check=null;
const d={quotes:[{symbol:o.symbol,ask:'8.00'}],account:{orders:[o]}};
sandbox.ui.records(d,'orders');const html=element('records-table').innerHTML;
assert(html.includes('已撤单'));assert(html.includes('无逐帧撮合记录，无法还原未成交原因'));
assert(!html.includes('8.00'));assert(!html.includes('限价未达到'));assert(!html.includes('已成交 100'));
assert(element('match-coverage').textContent.includes('不能根据当前报价反推过去'));
''')

    def test_transient_hold_takes_precedence_over_previous_match_check(self):
        self.run_ui(r'''
const o=order();o.last_match_check=check();
const d={market:{open:true,execution_hold:{active:true,order_ids:[o.id]}},account:{orders:[o]}};
sandbox.ui.records(d,'orders');let html=element('records-table').innerHTML;
assert(html.includes('行情暂缺，挂单保留，撮合暂停'));assert(html.includes('暂停前最近检查：'));
assert(html.includes('撤销模拟委托'));
for(const change of [()=>{d.market.heartbeat_stale=true;},()=>{d.market.open=false;},
  ()=>{d.paused=true;},()=>{d.market.execution_hold.order_ids=[];},()=>{o.status='CANCELLED';}]){
  d.market={open:true,execution_hold:{active:true,order_ids:[o.id]}};d.paused=false;o.status='OPEN';change();
  sandbox.ui.records(d,'orders');html=element('records-table').innerHTML;
  assert(!html.includes('行情暂缺，挂单保留，撮合暂停'));
}
''')

    def test_untrusted_diagnostic_labels_are_escaped_and_null_price_is_unknown(self):
        self.run_ui(r'''
const o=order();o.last_match_check={...check(),reason_label:'<img src=x onerror=alert(1)>',execution_price:null};
sandbox.ui.records({account:{orders:[o]}},'orders');const html=element('records-table').innerHTML;
assert(html.includes('&lt;img'));assert(!html.includes('<img'));assert(html.includes('含滑点试算价 — 元'));
''')

    def test_waiting_or_paused_display_keeps_last_ai_failure_visible(self):
        self.run_ui(r'''
const ai={enabled:true,phase:'休市等待',last_error:'合成绑定失败',last_plan_at:'2026-11-13T10:00:00+08:00',
  council:{latest_trace:{asof:'2026-11-13T10:30:00+08:00',status:'failed'}}};
for(const [paused,waiting,waitingMarket] of [[false,true,true],[false,true,false],[true,false,false],[false,false,false]]){
  sandbox.ui.renderAi(ai,paused,waiting,waitingMarket);
  assert(element('ai-plan').textContent.includes('最近一次计划未执行：合成绑定失败'));
  assert(element('ai-plan').className.includes('pl-error'));
  assert(element('ai-state').innerHTML.includes('10:30:00'));
  assert(!element('ai-plan').textContent.includes('共同观望'));
}
''')

    def test_market_banner_explains_hold_without_overriding_manual_pause(self):
        self.run_ui(r'''
const d={market:{open:true,calendar_verified:true,execution_hold:{active:true,order_ids:['synthetic-order']}},
  activation:{enabled:true,armed:true,status:'waiting_data',checks:[]},account:{}};
sandbox.ui.renderMarket(d);
assert.equal(element('market-title').textContent,'行情暂缺，挂单保留，撮合暂停');
assert(element('market-detail').textContent.includes('原限价和风控不变'));
assert(!element('market-detail').textContent.includes('尚未开始盘中撮合'));
d.paused=true;d.activation.status='paused';sandbox.ui.renderMarket(d);
assert.equal(element('market-title').textContent,'盘中模拟已暂停');
assert(!element('market-detail').textContent.includes('挂单与资金冻结保留'));
''')
