/* ui_v2/core/api.js — ★统一 API 客户端（UI v2 标准 #18 冗余+数据顺畅）
   功能：8s 超时 + 失败重试 1 次 + 同页缓存（同 URL 只拉一次）+ 60s 轮询（活跃页）
   全局命名空间 LW.api，供所有页面复用——冗余容错（不白屏）的第一道防线。
   用法：
     LW.api.get('/api/live/factors').then(function(d){...})   // 自动缓存+重试
     LW.api.poll('/api/live/factors', 60, renderFn)           // 60s 轮询
*/
(function (win) {
  'use strict';
  var cache = {};        // url -> {ts, data, status:'ok'|'err'}
  var inflight = {};     // URL + 缓存降级策略 -> Promise（防并发重复请求）

  function get(url, opts) {
    opts = opts || {};
    var timeout = opts.timeout || 8000;
    var retry = opts.retry !== undefined ? opts.retry : 1;   // 默认重试 1 次
    var bypass = opts.bypass === true;   // 强制刷新（审批/卖出后即时更新用）
    var staleFallback = opts.staleFallback !== false; // 实时执行页可禁止网络失败时返回旧缓存
    var requestKey = JSON.stringify([url, staleFallback]);
    var cached = cache[url];
    var now = Date.now();
    // 同页缓存：30s 内直接复用（页面内多个组件共用一个 API）
    if (!bypass && cached && now - cached.ts < 30000 && cached.status === 'ok') {
      return Promise.resolve(cached.data);
    }
    if (inflight[requestKey]) { return inflight[requestKey]; }

    // 每次尝试单独计时，连同响应体读取一起受超时限制。
    function attempt() {
      var controller = new AbortController();
      var timer = setTimeout(function () { controller.abort(); }, timeout);
      return Promise.resolve()
        .then(function () {
          return fetch(url, { cache: 'no-store', signal: controller.signal });
        })
        .then(function (r) {
          if (!r.ok) throw new Error('HTTP ' + r.status);
          return r.json();
        })
        .finally(function () { clearTimeout(timer); })
        .catch(function (err) {
          if (retry > 0) {
            retry--;
            return attempt();
          }
          throw err;
        });
    }

    var p = attempt()
      .then(function (data) {
        cache[url] = { ts: Date.now(), data: data, status: 'ok' };
        return data;
      })
      .catch(function (err) {
        // 重试仍失败：默认降级旧缓存，严格实时请求保留错误供调用方停用交易入口。
        if (cached) { cached.status = 'err'; }
        if (cached && staleFallback) { return cached.data; }
        throw err;
      })
      .then(function (data) { delete inflight[requestKey]; return data; })
      .catch(function (err) { delete inflight[requestKey]; throw err; });
    inflight[requestKey] = p;
    return p;
  }

  // 轮询：interval 秒拉一次（后台标签页自动降频 5min——省资源）
  function poll(url, intervalSec, renderFn) {
    var tick = function () {
      var hidden = document.hidden;
      var wait = hidden ? 300000 : (intervalSec * 1000);
      get(url).then(renderFn).catch(function () { /* 降级由 get 处理 */ });
      setTimeout(tick, wait);
    };
    tick();
  }

  // 清空缓存（审批/卖出后数据已变，下次 get 强制重拉最新）
  function clear() {
    cache = {};
    inflight = {};
  }

  // POST JSON（审批/卖出/手动更新等写操作——不带缓存、不重试）
  function post(url, body, opts) {
    opts = opts || {};
    return fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body || {}),
      cache: 'no-store'
    }).then(function (r) {
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    });
  }

  win.LW = win.LW || {};
  win.LW.api = { get: get, poll: poll, clear: clear, post: post };
})(window);
