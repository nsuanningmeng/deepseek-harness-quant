"""Exercise the local bridge and council presentation without live services."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
BRIDGE = ROOT / 'harness/home/profiles/web/plugins/dsq-quant-bridge.js'
CATALOG = ROOT / 'config/niu_personas.yaml'


class CouncilPresentationTests(unittest.TestCase):
    def run_node(self, source):
        result = subprocess.run(['node', '-e', source], cwd=ROOT, text=True,
                                capture_output=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def bridge_script(self, fixture, body):
        return r'''
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const assert = require('node:assert/strict');
const fixture = FIXTURE, source = SOURCE, yamlPath = YAML_PATH;
const routes = new Map();
const services = {
  webServer: {register(route) { routes.set(route.path, route.handler); }},
  agents: {roots() { return []; }},
  subagents: new Proxy({}, {get() { throw Error('read-only catalog started a subagent'); }})
};
const reads = [];
const guardedFs = {...fs,
  readFileSync(filename, ...args) {
    const resolved = fs.realpathSync(filename);
    assert(resolved.startsWith(fixture + path.sep), 'fixture escaped: ' + resolved);
    reads.push(resolved);
    return fs.readFileSync(filename, ...args);
  }
};
const sandbox = {
  __dirname: path.join(fixture, 'harness/home/profiles/web/plugins'),
  module: {exports: {}},
  require(name) {
    if (name === 'node:fs') return guardedFs;
    if (name === 'node:path') return path;
    if (name === 'yaml') return require(yamlPath);
    throw Error('unexpected dependency ' + name);
  }
};
vm.runInNewContext(fs.readFileSync(source, 'utf8'), sandbox);
sandbox.module.exports.apply({get(name) { return services[name]; }, effect(fn) { fn(); }});
async function getCatalog() {
  let code, data;
  await routes.get('/quant/niu/sessions')({method:'GET'}, {
    setHeader() {}, writeHead(status) { code = status; },
    end(body) { data = JSON.parse(body); }
  });
  return {code, data};
}
function writeConfig(name, value) {
  fs.writeFileSync(path.join(fixture, 'config', name), JSON.stringify(value));
}
(async () => { BODY })().catch(error => {console.error(error); process.exitCode = 1;});
'''.replace('FIXTURE', json.dumps(str(fixture))).replace('SOURCE', json.dumps(str(BRIDGE))).replace(
            'YAML_PATH', json.dumps(str(ROOT / 'harness/node_modules/yaml'))).replace('BODY', body)

    def make_bridge_fixture(self, fixture):
        (fixture / 'config').mkdir()
        catalog = yaml.safe_load(CATALOG.read_text())
        (fixture / 'config/niu_personas.yaml').write_text(json.dumps(catalog))
        (fixture / 'config/paperlive.yaml').write_text(json.dumps({
            'council': {'enabled': True, 'config_file': 'config/paperlive_council.yaml'}}))
        (fixture / 'config/paperlive_council.yaml').write_text(json.dumps({
            'schema_version': 1, 'enabled': True, 'catalog_file': 'config/niu_personas.yaml',
            'analyst_id': 'fengliu', 'reviewer_id': 'methodology'}))

    def test_catalog_preserves_all_roles_and_example(self):
        actual = yaml.safe_load(CATALOG.read_text())
        example = yaml.safe_load(CATALOG.with_name(CATALOG.name + '.example').read_text())
        self.assertEqual(actual, example)
        self.assertEqual(actual['schema_version'], 1)
        self.assertEqual({p['id']: p['name'] for p in actual['personas']}, {
            'linyuan': '林园', 'fengliu': '冯柳', 'chaoguyangjia': '炒股养家',
            'chenxiaoqun': '陈小群', 'zhangmengzhu': '章盟主',
            'zhaolaoge': '赵老哥', 'methodology': '方法论·牛散蒸馏'})

    def test_bridge_reads_catalog_and_selected_paper_roles_without_starting_chat(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory).resolve()
            self.make_bridge_fixture(fixture)
            self.run_node(self.bridge_script(fixture, r'''
const first = await getCatalog();
assert.equal(first.code, 200);
assert.equal(first.data.personas.length, 7);
assert.equal(first.data.paper_council.configured, true);
assert.deepEqual(first.data.personas.filter(p => p.paper_decision_role).map(p => [p.id,p.paper_decision_role]),
  [['fengliu','analyst'],['methodology','reviewer']]);
assert(first.data.personas.every(p => p.childId === null && p.preview === ''));
const catalog = JSON.parse(fs.readFileSync(path.join(fixture,'config/niu_personas.yaml'),'utf8'));
catalog.personas.find(p => p.id === 'fengliu').name = '本地修改的角色名称';
writeConfig('niu_personas.yaml', catalog);
assert.equal((await getCatalog()).data.personas.find(p => p.id === 'fengliu').name, '本地修改的角色名称');
writeConfig('paperlive_council.yaml', {enabled:false});
const disabled = await getCatalog();
assert.equal(disabled.data.paper_council.configured, false);
assert(disabled.data.personas.every(p => p.paper_decision_role === null));
assert(reads.every(p => ['niu_personas.yaml','paperlive.yaml','paperlive_council.yaml'].includes(path.basename(p))));
'''))

    def test_bridge_catalog_fallback_and_invalid_config_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory).resolve()
            self.make_bridge_fixture(fixture)
            self.run_node(self.bridge_script(fixture, r'''
const filename = path.join(fixture,'config/niu_personas.yaml');
fs.renameSync(filename, filename+'.example');
assert.equal((await getCatalog()).code, 200);
writeConfig('paperlive.yaml', {council:{enabled:true,config_file:'../outside.yaml'}});
const invalid = await getCatalog();
assert.equal(invalid.code,200);
assert.equal(invalid.data.paper_council.configured,false);
assert(invalid.data.personas.every(p => p.paper_decision_role === null));
const catalog = JSON.parse(fs.readFileSync(filename+'.example','utf8'));
catalog.personas.push(catalog.personas[0]);
writeConfig('niu_personas.yaml',catalog);
assert.equal((await getCatalog()).code,500);
fs.unlinkSync(filename);
fs.unlinkSync(filename+'.example');
assert.equal((await getCatalog()).code,500);
'''))

    def ui_script(self, body):
        return r'''
const fs = require('node:fs'), vm = require('node:vm'), assert = require('node:assert/strict');
const source = fs.readFileSync(PAGE, 'utf8').match(/<script>([\s\S]*?)<\/script>/)[1];
assert(source.includes('  load();\n})();'));
const elements = {};
function element(id) {
  return elements[id] ||= {textContent:'',innerHTML:'',value:'',style:{},dataset:{},addEventListener(){}};
}
const sandbox = {
  document: {getElementById:element,querySelectorAll(){return [];}},
  LW: {sidebar:{render(){}},render:{mount(id,html){element(id).innerHTML=html;}},
    api:{get(){throw Error('unexpected live API access');}}},
  fetch(){throw Error('unexpected fetch');},setTimeout(){},clearTimeout(){}
};
vm.runInNewContext(source.replace('  load();\n})();','  globalThis.renderCouncil = renderCouncil;\n})();'), sandbox);
function data() {
  return {ai:{council:{enabled:true,configured:true,config_fingerprint:'current',
    roles:[{id:'fengliu',name:'冯柳',stage:'analysis'},{id:'methodology',name:'方法论·牛散蒸馏',stage:'review'}]}},
    risk_policy:{effective:true,max_symbol_weight:.25,max_total_weight:.95,max_order_notional:12500,max_drawdown:.2}};
}
function report() {
  return {status:'approved',execution_status:'risk_processed',verification_only:false,config_fingerprint:'current',asof:'2026-11-12T10:00:00+08:00',
    analyst:{name:'冯柳',summary:'仅采纳有效证据',proposals:[{proposal_id:'p1',symbol:'600000.SH',side:'BUY',quantity:100,limit_price:10,reason:'候选依据'}]},
    reviewer:{name:'方法论·牛散蒸馏',summary:'已逐项审查',reviews:[{proposal_id:'p1',decision:'approve',reason:'复核依据'}]},
    approved_proposal_ids:['p1'],input_snapshot:{private_marker:'DO_NOT_DUMP_FULL_CONTEXT'}};
}
function render(d) {sandbox.renderCouncil(d);return element('council-report').innerHTML;}
BODY
'''.replace('PAGE', json.dumps(str(ROOT / 'ui_v2/pages/paperlive.html'))).replace('BODY', body)

    def test_ui_does_not_invent_reports_and_shows_dynamic_effective_risk(self):
        self.run_node(self.ui_script(r'''
const d=data();
assert.equal(render(d),'');
assert(element('council-badge').textContent.includes('等待盘中分析'));
assert(element('council-state').innerHTML.includes('尚无盘中双角色报告'));
assert(element('council-state').innerHTML.includes('冯柳'));
assert(element('council-state').innerHTML.includes('方法论·牛散蒸馏'));
assert(element('council-state').innerHTML.includes('冯柳（计划分析）'));
assert(element('council-state').innerHTML.includes('方法论·牛散蒸馏（独立复核）'));
const risk = element('risk-policy').innerHTML;
for(const expected of ['25%','95%','12,500 元','20%','本轮已核对生效']) assert(risk.includes(expected));
d.risk_policy.effective=false; render(d);
assert(element('risk-policy').innerHTML.includes('等待账本核对'));
'''))

    def test_ui_displays_only_real_current_approved_proposals_and_escapes_text(self):
        self.run_node(self.ui_script(r'''
const d=data(); d.ai.council.latest_trace=report();
d.ai.council.latest_trace.analyst.summary='<img src=x onerror=alert(1)>';
const html=render(d);
assert(html.includes('批准进入风控'));
assert(html.includes('复核依据'));
assert(html.includes('&lt;img src=x onerror=alert(1)&gt;'));
assert(!html.includes('<img'));
assert(!html.includes('DO_NOT_DUMP_FULL_CONTEXT'));
const trace=d.ai.council.latest_trace;
trace.reviewer.reviews[0].decision='reject'; trace.reviewer.reviews[0].reason='证据缺失';
assert(render(d).includes('证据缺失'));assert(!render(d).includes('批准进入风控'));
trace.reviewer.reviews[0].decision='approve'; trace.approved_proposal_ids=[];
assert(!render(d).includes('批准进入风控'));
trace.approved_proposal_ids=['p1']; trace.reviewer.reviews.push({...trace.reviewer.reviews[0]});
assert(render(d).includes('复核记录冲突')); assert(!render(d).includes('批准进入风控'));
'''))

    def test_ui_approved_but_not_executed_plan_never_claims_risk_processing(self):
        self.run_node(self.ui_script(r'''
const d=data();d.ai.council.latest_trace=report();
for(const reason of ['计划已过期','市场已休市','模拟盘已暂停','<img src=x onerror=alert(1)>']) {
  d.ai.council.latest_trace.execution_status='not_executed';
  d.ai.council.latest_trace.execution_error=reason;
  const html=render(d);
  assert(element('council-badge').textContent.includes('本轮未执行'));
  assert(html.includes('复核批准，本轮未执行'));
  assert(!html.includes('批准进入风控'));
  assert(html.includes(reason.startsWith('<')?'&lt;img':reason));
  assert(!html.includes('<img'));
}
delete d.ai.council.latest_trace.execution_status;
delete d.ai.council.latest_trace.execution_error;
assert(!render(d).includes('批准进入风控'));
assert(element('council-badge').textContent.includes('执行待核对'));
'''))

    def test_ui_old_validation_failed_and_disabled_states_are_not_execution_authority(self):
        self.run_node(self.ui_script(r'''
const cases=[
  [d=>d.ai.council.latest_trace.config_fingerprint='old','历史报告'],
  [d=>d.ai.council.latest_trace_current_config=false,'历史报告'],
  [d=>d.ai.council.latest_trace.verification_only=true,'连接验证'],
  [d=>d.ai.council.latest_trace.status='failed','未通过'],
  [d=>d.ai.council.latest_trace.status='unknown','待核对'],
  [d=>d.ai.council.enabled=false,'未启用'],
  [d=>d.ai.council.configured=false,'配置待核对']
];
for(const [change,label] of cases){
  const d=data(); d.ai.council.latest_trace=report(); change(d);
  assert(!render(d).includes('批准进入风控'));
  assert(element('council-badge').textContent.includes(label));
}
const d=data();d.ai.council.latest_trace=report();delete d.ai.council.latest_trace.reviewer;
assert(render(d).includes('本轮复核尚未完成'));assert(!render(d).includes('批准进入风控'));
d.ai.council.latest_trace={status:'failed',config_fingerprint:'current',error:'模型没有完成复核'};
assert(render(d).includes('未形成可执行提案'));
'''))


if __name__ == '__main__':
    unittest.main()
