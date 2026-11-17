// 量化系统 ↔ DeepSeek HARNESS 桥接插件（开源版静态化）
// 来源：dsqq-1 动态插件 v13（牛散 persona 接入 + 会话/聊天桥接）
// 部署：由 DSH_HOME/profiles/web/cordis.patch.yml 挂载为宿主组合插件行
// inject：声明硬依赖，cordis 在 webServer 等服务就绪后才 apply（避免提前 return 导致路由不注册）
const fs = require('node:fs')
const path = require('node:path')
const yaml = require('yaml')
const PROJECT_ROOT = path.resolve(__dirname, '../../../../..')
const CONFIG_ROOT = path.join(PROJECT_ROOT, 'config')
const CATALOG_FILE = path.join(CONFIG_ROOT, 'niu_personas.yaml')

function readYaml(filename, exampleFallback) {
  const selected = exampleFallback && !fs.existsSync(filename) ? filename + '.example' : filename
  const value = yaml.parse(fs.readFileSync(selected, 'utf8'))
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('invalid configuration')
  return value
}

function readPersonaCatalog() {
  const config = readYaml(CATALOG_FILE, true)
  if (config.schema_version !== 1 || !Array.isArray(config.personas) || !config.personas.length) throw new Error('invalid persona catalog')
  const catalog = Object.create(null)
  for (const item of config.personas) {
    if (!item || typeof item.id !== 'string' || !/^[a-z][a-z0-9_]*$/.test(item.id) || catalog[item.id] ||
        !['name', 'tag', 'style'].every(k => typeof item[k] === 'string' && item[k].trim()) ||
        (item.skill !== undefined && typeof item.skill !== 'string')) throw new Error('invalid persona definition')
    catalog[item.id] = { ...item, key: item.id }
  }
  return catalog
}

function configuredPaperRoles(catalog) {
  try {
    const paper = readYaml(path.join(CONFIG_ROOT, 'paperlive.yaml'), true)
    if (!paper.council || paper.council.enabled !== true) return { configured: false, roles: {} }
    const filename = path.resolve(PROJECT_ROOT, String(paper.council.config_file || ''))
    if (!filename.startsWith(CONFIG_ROOT + path.sep)) throw new Error('invalid council configuration path')
    const council = readYaml(filename, true)
    if (council.enabled !== true) return { configured: false, roles: {} }
    if (council.schema_version !== 1) throw new Error('invalid council version')
    if (path.resolve(PROJECT_ROOT, String(council.catalog_file || '')) !== CATALOG_FILE ||
        !catalog[council.analyst_id] || !catalog[council.reviewer_id] || council.analyst_id === council.reviewer_id) {
      throw new Error('invalid council roles')
    }
    return { configured: true, roles: { [council.analyst_id]: 'analyst', [council.reviewer_id]: 'reviewer' } }
  } catch (error) {
    return { configured: false, roles: {}, error: '模拟盘角色配置暂不可读，请查看实时模拟盘状态' }
  }
}

module.exports = {
  inject: ['webServer', 'sessions', 'agents', 'subagents', 'sessionTitle', 'sessionPersistence'],
  apply(ctx) {
    const webServer = ctx.get('webServer')
    const sessions = ctx.get('sessions')
    const agents = ctx.get('agents')
    const sessionTitle = ctx.get('sessionTitle')
    const subagents = ctx.get('subagents')
    const sessionPersistence = ctx.get('sessionPersistence')
    if (webServer === undefined) return

    function cors(res) {
      res.setHeader('Access-Control-Allow-Origin', '*')
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type')
      res.setHeader('Access-Control-Max-Age', '600')
    }
    function json(res, code, obj) {
      res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' })
      res.end(JSON.stringify(obj))
    }
    function requireMethod(req, res, method) {
      if (req.method === method) return true
      res.setHeader('Allow', method + ', OPTIONS')
      json(res, 405, { ok: false, error: 'method not allowed' })
      return false
    }
    async function readBody(req) {
      let body = ''
      for await (const chunk of req) body += new TextDecoder().decode(chunk)
      return body
    }
    function parseQuery(url) {
      const out = {}
      const qs = String(url || '').split('?')[1]
      if (!qs) return out
      for (const pair of qs.split('&')) {
        const i = pair.indexOf('=')
        if (i < 0) continue
        try { out[decodeURIComponent(pair.slice(0, i))] = decodeURIComponent(pair.slice(i + 1)) } catch (e) {}
      }
      return out
    }
    function defaultAgentId() {
      if (!agents) return null
      const roots = agents.roots()
      return roots[0] ? String(roots[0].id) : null
    }
    function resolveAgent(sessionId) {
      if (!agents) return null
      const id = sessionId || defaultAgentId()
      if (!id) return null
      return agents.get(id) || agents.roots().find(function (a) { return String(a.id) === id }) || null
    }
    function resolveSession(id) {
      if (sessions) {
        const s = sessions.get(id)
        if (s) return s
      }
      const agent = resolveAgent(id)
      if (agent && agent.session) return agent.session
      return null
    }
    function textOfContent(content) {
      if (typeof content === 'string') return content
      if (!Array.isArray(content)) return ''
      return content.map(function (p) {
        if (p == null) return ''
        if (typeof p === 'string') return p
        if (p.type === 'text') return String(p.text || '')
        return ''
      }).filter(Boolean).join('\n')
    }
    function isNoise(text) {
      const t = String(text || '').trim()
      if (!t) return true
      if (t.indexOf('Current runtime context. This snapshot supersedes') === 0) return true
      if (t.indexOf('【任务】你是「') === 0) return true
      return false
    }
    function extractMessages(eventsArr, limit) {
      const out = []
      const arr = Array.isArray(eventsArr) ? eventsArr : []
      for (let i = arr.length - 1; i >= 0 && out.length < (limit || 20); i--) {
        const ev = arr[i]
        if (!ev) continue
        let role = null, content = null, ts = null
        if (ev.type === 'user/message' || ev.type === 'assistant/message') {
          const d = ev.data || {}
          const m = d.message && d.message.role ? d.message : d
          role = ev.type === 'user/message' ? 'user' : 'assistant'
          content = m.content
          ts = ev.time != null ? ev.time : (d.ts != null ? d.ts : null)
        } else {
          const msg = ev.message || ev
          role = msg && msg.role
          content = msg && msg.content
          ts = (ev.ts != null ? ev.ts : (msg && msg.ts != null ? msg.ts : null))
        }
        if (role !== 'user' && role !== 'assistant') continue
        const text = textOfContent(content)
        if (isNoise(text)) continue
        out.unshift({ role: role, text: text.slice(0, 4000), ts: ts })
      }
      return out
    }
    function liveMessages(session, limit) {
      let messages = []
      if (typeof session.deriveMessages === 'function') messages = session.deriveMessages()
      else if (session.events) messages = session.events
      return extractMessages(messages, limit)
    }
    async function coldMessages(id, limit) {
      const debug = { tried: false, events: 0, error: null }
      if (!sessionPersistence || typeof sessionPersistence.readFrom !== 'function') {
        debug.error = 'no sessionPersistence'
        return { messages: null, debug: debug }
      }
      try {
        debug.tried = true
        const r = await sessionPersistence.readFrom(id, 0, fakeSignal())
        if (r && Array.isArray(r.events)) {
          debug.events = r.events.length
          return { messages: extractMessages(r.events, limit), debug: debug }
        }
        debug.error = 'no events array'
        return { messages: null, debug: debug }
      } catch (e) {
        debug.error = String((e && e.message) || e).slice(0, 200)
        return { messages: null, debug: debug }
      }
    }
    async function messagesOf(id, limit) {
      const session = resolveSession(id)
      if (session) {
        const live = liveMessages(session, limit)
        if (live.length) return { messages: live, debug: { live: true, count: live.length } }
      }
      const c = await coldMessages(id, limit)
      return { messages: c.messages || [], debug: c.debug }
    }
    function recentTextOf(list) {
      for (let i = list.length - 1; i >= 0; i--) {
        if (list[i] && list[i].text) return { text: list[i].text.slice(0, 120), role: list[i].role }
      }
      return null
    }
    function sessionTitleOf(id) {
      try {
        const session = resolveSession(id)
        if (!session) return null
        if (sessionTitle && typeof sessionTitle.get === 'function') {
          const st = sessionTitle.get(session)
          if (st && st.title) return st.title
        }
        const meta = session.meta || session.header || {}
        if (meta.title) return meta.title
        const cwd = meta.cwd
        if (cwd) {
          const parts = String(cwd).replace(/\\/g, '/').split('/').filter(Boolean)
          return parts[parts.length - 1] || null
        }
      } catch (e) {}
      return null
    }

    function buildPersona(p) {
      return '你是「牛散·' + p.name + '」人格模拟（基于公开资料牛散档案 ' + p.skill + ' 蒸馏魔改；不构成投资建议）。\n'
        + '【风格与选股视角】\n' + p.style + '\n'
        + '【你的职责】主观选股顾问：基于量化系统提供的【Pitch 候选卡】与【当前持仓】做选股决策。Pitch 卡字段含义：score=综合评分(0-100)、winrate_est=历史胜率、upside_est=预期空间、evidence=证据链、stop_plan=止损/离场计划、factors=关键因子值、risk_flags=风险标记。\n'
        + '【铁律】\n'
        + '1. 只在你拿到的 Pitch 候选卡与当前持仓范围内决策；卡外股票最多建议"关注"，绝不推荐买入。\n'
        + '2. 尊重量化系统纪律：主力仓位走 turn_low 低换手分散，pitch 高分只做卫星仓（集中持仓回撤已被实证 -41%~-60%）；跌破止损/逻辑破坏必须明确标"卖出"。\n'
        + '3. 逐卡输出：代码名称 → 你的判断（高/中/低优先级）→ 核心理由（关联你的方法论）→ 与量化信号冲突点。结论先行，精炼列表，≤400 字。\n'
        + '4. 批判自省：你是人格模拟，风格来自公开资料，存在信息滞后/幸存者偏差/造神风险；不追高、不造神。结尾注明"不构成投资建议"。\n'
        + '5. 决策结构化输出（必须遵守）：在回复【最后一行】输出一个纯 JSON 对象（不要 markdown 代码块），格式：{"niu_decisions":[{"code":"600519.SH","action":"buy","priority":"high","reason_short":"垄断成瘾质量"}]}。action 限 buy/hold/sell/watch；priority 限 high/mid/low；reason_short ≤15 字；只包含你在 Pitch 卡与当前持仓范围内明确给出的决策；本轮没有明确决策就输出 {"niu_decisions":[]}。\n'
        + '【约束】不调用任何工具、不读取文件、不联网——只基于对话中给出的数据作答。'
    }


    const personaChild = {}
    const personaSnap = {}
    const personaHinted = {}
    const niuBootLock = {}

    function fakeSignal() {
      return new AbortController().signal
    }
    async function discoverNiuChildren() {
      const out = {}
      if (!subagents || !agents) return out
      const root = defaultAgentId()
      if (!root) return out
      let entries = []
      try { entries = await subagents.listChildren(root) } catch (e) { entries = [] }
      for (const e of entries) {
        if (!e || e.kind !== 'child') continue
        const id = e.id ? String(e.id) : null
        const label = e.label || ''
        if (!id || label.indexOf('牛散·') !== 0) continue
        out[label.slice(3)] = id
      }
      return out
    }
    function ensureNiuChild(key, snapshot, definition) {
      if (personaChild[key]) return Promise.resolve(personaChild[key])
      if (niuBootLock[key]) return niuBootLock[key]
      niuBootLock[key] = (async function () {
        try {
          const found = await discoverNiuChildren()
          if (found[key]) {
            personaChild[key] = found[key]
            if (snapshot) personaSnap[key] = snapshot
            return personaChild[key]
          }
          const p = definition
          const rootId = defaultAgentId()
          const rootAgent = rootId ? agents.get(rootId) : null
          if (!subagents || !rootAgent) return null
          const firstMsg = '【任务】你是「' + p.name + '」主观选股顾问（身份与选股规则已注入你的系统设定）。'
            + (snapshot ? '\n\n当前量化 Pitch 快照：\n' + snapshot + '\n' : '')
            + '\n\n请先一句话点出你的选股风格要点（确认就位），然后直接给出第一轮选股意见。'
          const spec = {
            provider: 'spawn',
            label: '牛散·' + key,
            request: {
              prompt: [{ type: 'text', text: firstMsg }],
              parent: rootAgent,
              persona: buildPersona(p),
              toolFilter: { allow: [] }
            },
            signal: fakeSignal()
          }
          const out = await subagents.startContinuable(spec)
          personaChild[key] = String(out && out.childId)
          personaHinted[key] = true
          if (snapshot) personaSnap[key] = snapshot
          return personaChild[key]
        } catch (e) {
          console.error('niu boot ' + key + ': ' + String((e && e.message) || e))
          return null
        } finally {
          delete niuBootLock[key]
        }
      })()
      return niuBootLock[key]
    }

    ctx.effect(() => webServer.register({
      kind: 'exact', path: '/quant/sessions',
      handler: async (req, res) => {
        cors(res)
        if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return }
        if (!requireMethod(req, res, 'GET')) return
        try {
          const roots = agents ? agents.roots() : []
          const list = []
          for (let idx = 0; idx < roots.length; idx++) {
            const a = roots[idx]
            const id = a && a.id ? String(a.id) : null
            if (!id) continue
            const msgs = await messagesOf(id, 3)
            const last = recentTextOf(msgs.messages)
            list.push({
              id: id,
              title: sessionTitleOf(id) || ('会话 ' + (idx + 1)),
              preview: last ? last.text : '空会话，尚无消息',
              empty: !last,
              role: last ? last.role : '',
              ts: Date.now(),
              busy: a.status === 'running',
              current: idx === 0
            })
          }
          json(res, 200, { ok: true, sessions: list, requiresSession: list.length === 0,
            capabilities: { chat: true, sessionManagement: false },
            createSessionUrl: 'http://127.0.0.1:3080/' })
        } catch (e) { json(res, 500, { ok: false, error: String((e && e.message) || e).slice(0, 200) }) }
      }
    }))

    ctx.effect(() => webServer.register({
      kind: 'exact', path: '/quant/niu/sessions',
      handler: async (req, res) => {
        cors(res)
        if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return }
        if (!requireMethod(req, res, 'GET')) return
        try {
          const found = await discoverNiuChildren()
          const personas = readPersonaCatalog()
          const paperCouncil = configuredPaperRoles(personas)
          const catalog = []
          for (const k of Object.keys(personas)) {
            const p = personas[k]
            const childId = personaChild[k] || found[k] || null
            let preview = ''
            if (childId) {
              const msgs = await messagesOf(childId, 3)
              const last = recentTextOf(msgs.messages)
              preview = last ? last.text : ''
            }
            catalog.push({ id: k, name: p.name, tag: p.tag, skill: p.skill, childId: childId, preview: preview, paper_decision_role: paperCouncil.roles[k] || null })
          }
          json(res, 200, { ok: true, personas: catalog, paper_council: { configured: paperCouncil.configured, error: paperCouncil.error || null } })
        } catch (e) { json(res, 500, { ok: false, error: String((e && e.message) || e).slice(0, 200) }) }
      }
    }))

    ctx.effect(() => webServer.register({
      kind: 'exact', path: '/quant/niu/chat',
      handler: async (req, res) => {
        cors(res)
        if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return }
        if (!requireMethod(req, res, 'POST')) return
        let key = '', text = '', snapshot = ''
        try {
          const payload = JSON.parse((await readBody(req)) || '{}')
          key = String(payload.persona || '').trim()
          text = String(payload.text || '').trim()
          if (payload.snapshot) snapshot = String(payload.snapshot)
        } catch (e) { json(res, 400, { ok: false, error: 'bad json' }); return }
        let personas
        try { personas = readPersonaCatalog() } catch (error) { json(res, 500, { ok: false, error: '角色目录暂不可读，请检查本机配置' }); return }
        const p = personas[key]
        if (!p) { json(res, 404, { ok: false, error: 'unknown persona: ' + key }); return }
        if (!text) { json(res, 400, { ok: false, error: 'empty text' }); return }
        if (!defaultAgentId()) { json(res, 409, { ok: false, error: '请先在 HARNESS 原生界面新建根会话', requiresSession: true }); return }
        try {
          const existed = !!personaChild[key]
          const childId = await ensureNiuChild(key, snapshot || personaSnap[key] || '', p)
          if (!childId) { json(res, 500, { ok: false, error: '牛散子代理创建失败（subagents 不可用？）', persona: key }); return }
          const rootId = defaultAgentId()
          const rootAgent = rootId ? agents.get(rootId) : null
          if (!rootAgent || !subagents) { json(res, 500, { ok: false, error: 'agents/subagents 不可用' }); return }
          let content = text
          if (existed) {
            if (snapshot) personaSnap[key] = snapshot
            let extra = personaSnap[key] ? '\n\n【量化 Pitch 快照（最新）】\n' + personaSnap[key] : ''
            if (!personaHinted[key]) {
              personaHinted[key] = true
              extra += '\n\n（系统提醒：请在你的回复最后一行输出纯 JSON 决策对象 {"niu_decisions":[...]}，action 限 buy/hold/sell/watch）'
            }
            content = text + extra
          }
          await subagents.followup(rootAgent, childId, [{ type: 'text', text: content }],
            { source: { kind: 'user' }, signal: fakeSignal() })
          json(res, 200, { ok: true, accepted: true, persona: key, childId: childId })
        } catch (e) { json(res, 500, { ok: false, error: String((e && e.message) || e).slice(0, 200) }) }
      }
    }))

    ctx.effect(() => webServer.register({
      kind: 'exact', path: '/quant/niu/recent',
      handler: async (req, res) => {
        cors(res)
        if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return }
        if (!requireMethod(req, res, 'GET')) return
        try {
          const q = parseQuery(req.url)
          const key = String(q.persona || '').trim()
          if (!readPersonaCatalog()[key]) { json(res, 404, { ok: false, error: 'unknown persona: ' + key }); return }
          let childId = personaChild[key]
          if (!childId) {
            const found = await discoverNiuChildren()
            childId = found[key] || null
            if (childId) personaChild[key] = childId
          }
          if (!childId) { json(res, 200, { ok: true, persona: key, childId: null, messages: [], debug: { child: null } }); return }
          const limit = Math.max(1, Math.min(50, parseInt(q.limit || '20', 10) || 20))
          const r = await messagesOf(childId, limit)
          json(res, 200, { ok: true, persona: key, childId: childId, messages: r.messages, debug: r.debug })
        } catch (e) { json(res, 500, { ok: false, error: String((e && e.message) || e).slice(0, 200) }) }
      }
    }))

    ctx.effect(() => webServer.register({
      kind: 'exact', path: '/quant/chat2',
      handler: async (req, res) => {
        cors(res)
        if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return }
        if (!requireMethod(req, res, 'POST')) return
        let text = '', sessionId = null
        try {
          const payload = JSON.parse((await readBody(req)) || '{}')
          text = String(payload.text || '').trim()
          if (payload.sessionId) sessionId = String(payload.sessionId)
        } catch (e) { json(res, 400, { ok: false, error: 'bad json' }); return }
        if (!text) { json(res, 400, { ok: false, error: 'empty text' }); return }
        try {
          const agent = resolveAgent(sessionId)
          if (!agent) { json(res, 409, { ok: false, error: '请先在 HARNESS 原生界面新建根会话', requiresSession: true, sessionId: sessionId || defaultAgentId() }); return }
          const { createUserMessage } = await import('@deepseek-ai/dsh-llm')
          const message = createUserMessage({ content: [{ type: 'text', text }], source: { kind: 'user' } })
          if (typeof agent.followup === 'function') agent.followup(message)
          else if (typeof agent.steer === 'function') agent.steer(message)
          else { json(res, 500, { ok: false, error: 'no followup/steer' }); return }
          json(res, 200, { ok: true, accepted: true, sessionId: String(agent.id) })
        } catch (e) { json(res, 500, { ok: false, error: String((e && e.message) || e).slice(0, 200) }) }
      }
    }))

    ctx.effect(() => webServer.register({
      kind: 'exact', path: '/quant/recent',
      handler: async (req, res) => {
        cors(res)
        if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return }
        if (!requireMethod(req, res, 'GET')) return
        try {
          const q = parseQuery(req.url)
          const sessionId = q.sessionId || defaultAgentId()
          if (!sessionId) { json(res, 200, { ok: true, sessionId: null, messages: [], busy: false, requiresSession: true }); return }
          const limit = Math.max(1, Math.min(50, parseInt(q.limit || '20', 10) || 20))
          const r = await messagesOf(sessionId, limit)
          const agent = resolveAgent(sessionId)
          json(res, 200, { ok: true, sessionId: sessionId, messages: r.messages, busy: !!agent && agent.status === 'running' })
        } catch (e) { json(res, 500, { ok: false, error: String((e && e.message) || e).slice(0, 200) }) }
      }
    }))

    ctx.effect(() => webServer.register({
      kind: 'exact', path: '/quant/agents',
      handler: async (req, res) => {
        cors(res)
        if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return }
        if (!requireMethod(req, res, 'GET')) return
        try {
          const roots = agents ? agents.roots() : []
          json(res, 200, { ok: true, rootIds: roots.map(function (a) { return a && a.id ? String(a.id) : null }).filter(Boolean), ts: Date.now() })
        } catch (e) { json(res, 500, { ok: false, error: String((e && e.message) || e).slice(0, 200) }) }
      }
    }))
  },
}
