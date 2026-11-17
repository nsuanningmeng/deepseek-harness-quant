/* Fixed-window minute download status. Reads local public state; never starts a request. */
(function () {
  'use strict';
  window.LW = window.LW || {};
  function number(value) { return value == null || value === '' || !Number.isFinite(Number(value)) ? null : Number(value); }
  function show(value) { var n=number(value);return n == null ? '—' : n.toLocaleString('zh-CN'); }
  function moment(value) { if(!value)return '尚未排定';var d=new Date(value);return Number.isNaN(d.getTime())?String(value):d.toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'}); }
  function describe(data) {
    data=data || {};var job=data.background_job || {};
    var labels={not_started:'任务尚未登记',waiting:'等待下一请求时段',downloading:'正在下载并核验',rate_limited:'官方限频，等待许可时段',complete:'本轮固定窗口已完成',failed:'任务失败，需检查原因',stopped:'后台服务已停止'};
    var label=job.verification_status==='waiting_core_update'?'等待行情缓存可核验':job.verification_status==='verifying'?'下载已记录，正在后台业务强验':job.verification_status==='failed'?'业务强验未通过':job.status==='complete' && job.deep_verified!==true?'下载单元已完成，等待业务强验':labels[job.status] || '状态待核验';
    var lines=['后台状态：'+label+' · '+(job.service_alive===true?'服务在线':'服务未在线')];
    if(job.job_id) {
      lines.push('来源：'+(job.source_label || job.source || '尚未声明')+' · '+(job.frequency || data.frequency || '频率待核验'));
      lines.push('本轮固定数据窗：'+(job.start_date || '—')+' ~ '+(job.target_date || '—')+'；跨日后仍补齐这一窗口');
      var completed=number(job.completed_symbols),expected=number(job.expected_symbols);
      var percent=completed != null && expected>0 && completed>=0 && completed<=expected ? completed/expected*100 : null;
      lines.push('下载回执完成 '+show(completed)+' / '+show(expected)+(percent==null?'':'（'+percent.toFixed(1)+'%）')+' · 回执登记 '+show(job.symbols_with_rows)+' 只 · '+show(job.rows)+' 行'+(job.deep_verified===true?' · 独立强验已通过':' · 完整窗口行数待强验'));
      var interval=number(job.request_interval_seconds);
      if(interval>0) lines.push('后台配置的请求间隔：'+show(interval)+' 秒');
      lines.push('最早下次请求：'+moment(job.next_request_at));
      var fresh=job.current_date_ready != null ? job.current_date_ready===true : job.fresh_for_current_core===true;
      lines.push('当前核心研究日：'+(job.current_core_data_date || data.current_core_data_date || '尚未核验')+' · '+(fresh?'历史窗口与当前研究日期、股票池已匹配':'尚未覆盖当前研究日期或股票池，不能当作最新全池行情'));
      if((job.frozen_window_complete===true || job.coverage_complete===true) && !fresh) lines.push('本轮完成仅代表固定历史窗口，后续新行情需要另行更新。');
      if(job.quality_warnings && Object.keys(job.quality_warnings).length) lines.push((Number(job.quality_warnings.high_low_cross_source_difference_days)>0?'跨源日内高低价存在差异；':'')+'历史分钟仅作研究，实时成交使用新鲜盘口。');
    } else lines.push('尚无固定日期任务，等待后台登记；刷新页面不会发起行情下载。');
    if(job.last_error)lines.push('最近原因：'+String(job.last_error));
    if(job.verification_error)lines.push('核验原因：'+String(job.verification_error));
    (Array.isArray(job.limitations)?job.limitations:[]).forEach(function(value){lines.push(String(value));});
    return lines.join('\n');
  }
  function render(target,data) { if(!target)return;target.style.whiteSpace='pre-wrap';target.textContent=describe(data); }
  function bind(id,options) {
    options=options || {};var target=document.getElementById(id),loading=false,timer=null,stopped=false;
    function refresh() {
      if(loading || stopped)return;loading=true;clearTimeout(timer);
      var delay=options.interval || 15000;
      return LW.api.get('/api/live/minute',{bypass:true,retry:0,staleFallback:false}).then(function(data){render(target,data);if(data.sample_state==='probing')delay=3000;})
        .catch(function(error){delay=5000;if(target)target.textContent='分钟状态读取超时或失败，5秒后重试；尚未确认就绪。'+(error.name==='AbortError'?'':' '+error.message);})
        .finally(function(){loading=false;if(!stopped)timer=setTimeout(refresh,delay);});
    }
    refresh();
    return {refresh:refresh,stop:function(){stopped=true;clearTimeout(timer);}};
  }
  LW.minuteStatus={describe:describe,render:render,bind:bind};
})();
