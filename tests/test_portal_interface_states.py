"""Browser-independent portal checks with synthetic, read-only API responses."""
import json
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[1]


class PortalInterfaceStateTests(unittest.TestCase):
    def run_ui(self, body):
        script = r'''
const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const source=fs.readFileSync(PAGE,'utf8').match(/<script>([\s\S]*?)<\/script>/)[1];
function escape(value){return String(value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
const elements={};
function element(id){return elements[id] ||= {innerHTML:'',textContent:'',style:{},dataset:{}};}
let response,requested=[];
const sandbox={
  document:{getElementById:element,createElement(){return {textContent:'',get innerHTML(){return escape(this.textContent);}};}},
  LW:{sidebar:{render(){}},minuteStatus:{bind(){}},researchUpdate:{bind(){}},render:{loadEnums(){}},icons:{},
      api:{get(path){assert(['/api/live/realtime','/api/live/endpoints'].includes(path));requested.push(path);return Promise.resolve(response);}}},
  setTimeout(){},clearTimeout(){},setInterval(){},fetch(){throw Error('unexpected live network');}
};
let instrumented=source;
for(const name of ['loadPortal','loadWufu','loadRealtime','loadApiStatus']){
  assert(instrumented.includes('  '+name+'();'));
  instrumented=instrumented.replace('  '+name+'();','');
}
assert(instrumented.endsWith('})();\n'));
instrumented=instrumented.slice(0,-6)+'globalThis.portalTest={loadRealtime,loadApiStatus};\n})();\n';
vm.runInNewContext(instrumented,sandbox);
async function render(name,data){
  const original=JSON.stringify(data);response=data;sandbox.portalTest[name]();
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(JSON.stringify(data),original,'UI changed API readiness');
}
function endpoint(state,path,extra={}){
  return {path:'/api/live/'+path,state,ok:state==='ready',reachable:true,ms:12,...extra};
}
function status(rows,extra={}){
  return {ok:true,state:'ready',ready:false,total:rows.length,checked_count:rows.length,
    connected_count:rows.filter(e=>e.reachable).length,ok_count:rows.filter(e=>e.ok).length,endpoints:rows,...extra};
}
(async()=>{BODY})().catch(error=>{console.error(error);process.exitCode=1;});
'''.replace('PAGE', json.dumps(str(ROOT / 'ui_v2/pages/portal.html'))).replace('BODY', body)
        result = subprocess.run(['node', '-e', script], cwd=ROOT, text=True,
                                capture_output=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_closed_market_is_separate_from_history_optional_and_missing_data(self):
        self.run_ui(r'''
const rows=[endpoint('ready','calendar'),
  endpoint('waiting_market','realtime',{error:'基础数据、AI与账户已核验，等待下一交易时段再核验实时行情'}),
  endpoint('pending_history','chain',{reason:'前向收益需随交易日积累'}),
  endpoint('optional_unconfigured','auction',{error:'可选竞价未启用'})];
await render('loadApiStatus',status(rows));
const html=element('api-box').innerHTML,tag=element('api-status-tag').textContent;
assert(tag.includes('连接 4/4'));assert(tag.includes('当前可用 1/4'));assert(tag.includes('等待开市 1'));
assert(html.includes('data-interface-group="waiting_market"'));
assert(html.includes('基础准备已完成'));assert(html.includes('当前不可交易'));
assert(html.includes('data-interface-group="pending_history"'));assert(html.includes('前向收益需随交易日积累'));
assert(html.includes('data-interface-group="optional_unconfigured"'));assert(html.includes('可选竞价未启用'));
assert(!html.includes('data-interface-group="unready"'));assert(!html.includes('全部 4 个接口检查通过'));
const waiting=html.split('data-interface-group="waiting_market"')[1].split('data-interface-group=')[0];
assert(!waiting.includes('var(--green)'));assert(!waiting.includes('缺数据'));
''')

    def test_missing_waiting_data_and_connection_failure_remain_visible(self):
        self.run_ui(r'''
const rows=[endpoint('waiting_market','realtime'),endpoint('waiting_data','pending',{error:'分钟历史缺口'}),
  endpoint('missing_data','bars',{reason:'行情表为空'}),endpoint('connection_error','offline',{reachable:false,error:'连接被拒绝'})];
await render('loadApiStatus',status(rows));
const html=element('api-box').innerHTML;
assert(html.includes('2 项功能未就绪 · 需要处理'));
assert(html.includes('等待数据核验'));assert(html.includes('分钟历史缺口'));
assert(html.includes('缺数据'));assert(html.includes('行情表为空'));
assert(html.includes('连接失败 1 项'));assert(html.includes('连接被拒绝'));
assert(element('api-status-tag').textContent.includes('连接 3/4'));
assert(element('api-status-tag').textContent.includes('当前可用 0/4'));
''')

    def test_waiting_market_cannot_be_promoted_to_ready_by_inconsistent_flags(self):
        self.run_ui(r'''
await render('loadApiStatus',status([endpoint('waiting_market','realtime',{ok:true})],{ready:true}));
assert(element('api-status-tag').textContent.includes('当前可用 0/1'));
assert(element('api-box').innerHTML.includes('等待开市 1 项'));
assert(!element('api-box').innerHTML.includes('全部 1 个接口检查通过'));
await render('loadApiStatus',status([endpoint('ready','one'),endpoint('ready','two')],{ready:true}));
assert(element('api-status-tag').textContent.includes('当前可用 2/2'));
assert(element('api-box').innerHTML.includes('全部 2 个接口检查通过'));
''')

    def test_realtime_waiting_market_takes_precedence_over_paused_and_shows_next_open(self):
        self.run_ui(r'''
await render('loadRealtime',{ok:true,ready:false,initialized:true,state:'waiting_market',paused:true,market_open:false,
  reason:'基础数据、AI与账户已核验，等待下一交易时段再核验实时行情',next_open:'2026-11-13 09:30:00+08:00'});
const html=element('rt-box').innerHTML;
assert(html.includes('data-realtime-state="waiting_market"'));
assert(html.includes('下一交易时段：2026-11-13 09:30:00+08:00'));
assert(html.includes('基础数据、AI与账户已核验'));
assert(html.includes('当前不交易'));assert(!html.includes('var(--green)'));
assert(!html.includes('实时行情暂不可用'));assert(!html.includes('>上涨<'));
''')

    def test_realtime_data_problem_still_warns_and_external_text_is_escaped(self):
        self.run_ui(r'''
await render('loadRealtime',{ok:true,ready:false,state:'waiting_data',paused:true,reason:'历史缺数据 <img src=x onerror=alert(1)>'});
let html=element('rt-box').innerHTML;
assert(html.includes('实时行情尚未就绪'));assert(html.includes('等待数据核验'));
assert(html.includes('历史缺数据 &lt;img'));assert(!html.includes('<img'));
await render('loadRealtime',{ok:true,ready:false,state:'missing_data',paused:false,market_open:true,reason:'有效报价缺失'});
html=element('rt-box').innerHTML;
assert(html.includes('实时行情尚未就绪 · 缺数据'));assert(html.includes('有效报价缺失'));
assert(!html.includes('var(--green)'));assert(!html.includes('>上涨<'));
await render('loadRealtime',{ok:true,ready:false,state:'waiting_market',paused:true,reason:'<script>bad()</script>',next_open:'<img src=x>'});
html=element('rt-box').innerHTML;
assert(html.includes('&lt;script&gt;'));assert(html.includes('&lt;img'));assert(!html.includes('<script>'));
await render('loadApiStatus',status([endpoint('waiting_market','<img>',{reason:'<script>bad()</script>'})]));
html=element('api-box').innerHTML;
assert(html.includes('&lt;script&gt;'));assert(html.includes('&lt;img&gt;'));assert(!html.includes('<script>'));
''')


if __name__ == '__main__':
    unittest.main()
