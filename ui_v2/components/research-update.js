/* Research update controls: queue only; all displayed progress comes from the service. */
(function () {
  'use strict';
  window.LW = window.LW || {};
  function describe(data) {
    data = data || {};
    var bootstrap = data.bootstrap || {}, pipeline = data.pipeline || {}, progress = data.progress;
    var busy = data.busy === true || data.queued === true;
    var coreSettled = data.core_ready === true && !busy;
    var percent = null, completed = null, total = null;
    if (!coreSettled && progress && typeof progress === 'object') {
      completed = Number(progress.completed != null ? progress.completed : progress.completed_units);
      total = Number(progress.total != null ? progress.total : progress.total_units);
      if (progress.percent != null && Number.isFinite(Number(progress.percent))) percent = Number(progress.percent);
    }
    if (!coreSettled && !(total > 0)) { completed = Number(bootstrap.completed_units); total = Number(bootstrap.total_units); }
    if (percent == null && total > 0 && Number.isFinite(completed)) percent = completed / total * 100;
    if (percent != null) percent = Math.min(100, Math.max(0, percent));
    var labels = {idle:'等待下次更新',core_complete:'核心研究已核验',queued:'已排队',running:'更新中',initializing:'初始化中',waiting_for_close:'等待收盘数据',waiting_for_session:'等待交易时段',complete:'本轮完成',completed:'本轮完成',failed:'本轮失败',interrupted:'已中断',unavailable:'服务不可用'};
    var phase = data.phase || data.status || bootstrap.stage || pipeline.current_step || 'idle';
    var text = data.message || labels[phase] || labels[data.status] || '请查看实际初始化进度';
    var lines = [];
    if (coreSettled) lines.push('核心研究已核验'+(data.data_date?' · 数据日 '+data.data_date:'')+'；分钟历史由独立后台任务补齐，进度见分钟数据卡片');
    if (total > 0 && Number.isFinite(completed)) lines.push('当前阶段已完成 '+completed+' / '+total+' 个数据单元');
    if (!coreSettled && progress && typeof progress === 'object' && (progress.phase || progress.stage || progress.unit)) lines.push('当前数据步骤：'+(progress.phase || progress.stage || progress.unit));
    if (!coreSettled && progress && typeof progress === 'object' && progress.rows != null) lines.push('当前已记录 '+progress.rows+' 行');
    if (!coreSettled && bootstrap.stage) lines.push('数据阶段：'+bootstrap.stage);
    if (pipeline.current_step) lines.push('分析步骤：'+pipeline.current_step);
    if (data.requested_at) lines.push('请求时间：'+data.requested_at);
    if (data.next_run_at) lines.push('下次检查：'+data.next_run_at);
    var error = data.last_error || (!coreSettled && bootstrap.last_error) || pipeline.last_error;
    if (error) lines.push('当前原因：'+String(error));
    (Array.isArray(pipeline.steps) ? pipeline.steps : []).forEach(function(step){
      if (step.status === 'running' || step.status === 'failed' || step.status === 'blocked') lines.push((step.label || step.id || '分析步骤')+' · '+step.status+(step.error?' · '+step.error:''));
    });
    return {busy:busy,percent:percent,text:String(text),detail:lines.join('\n'),phase:labels[phase] || labels[data.status] || (busy?'更新中':'等待检查')};
  }
  function status() { return LW.api.get('/api/research_update',{bypass:true,retry:0,staleFallback:false}); }
  function request() {
    return fetch('/api/research_update',{method:'POST',headers:{'Content-Type':'application/json'},cache:'no-store',body:'{}'}).then(function(response){
      return response.json().then(function(data){if(!response.ok)throw new Error(data.error || data.message || ('请求失败（'+response.status+'）'));return data;});
    });
  }
  function bind(options) {
    var button=document.getElementById(options.buttonId), submitting=false, latest=null, previouslyBusy=false;
    function el(id) { return id ? document.getElementById(id) : null; }
    function render(data) {
      latest=data;var view=describe(data);
      button.disabled=submitting||view.busy||data.service_alive===false;
      button.textContent=view.busy?'研究数据更新中':'更新研究数据';
      if(el(options.statusId))el(options.statusId).textContent=view.text;
      if(el(options.detailId))el(options.detailId).textContent=view.detail;
      if(el(options.progressId))el(options.progressId).style.display=(view.busy||view.percent!=null)?'flex':'none';
      if(el(options.fillId))el(options.fillId).style.width=(view.percent==null?0:view.percent)+'%';
      if(el(options.percentId))el(options.percentId).textContent=view.percent==null?view.phase:'阶段 '+Math.round(view.percent)+'%';
      if(previouslyBusy&&!view.busy&&options.onIdle)options.onIdle(data);
      previouslyBusy=view.busy;
      return data;
    }
    function fail(error) {
      if(el(options.statusId))el(options.statusId).textContent='研究更新状态暂不可读：'+error.message;
      button.disabled=submitting||!!(latest&&latest.service_alive===false);
    }
    function refresh() { if(submitting)return Promise.resolve();return status().then(render).catch(fail); }
    button.onclick=function(){
      if(submitting||button.disabled)return;
      submitting=true;button.disabled=true;
      if(el(options.statusId))el(options.statusId).textContent='正在提交本机更新请求…';
      request().then(function(data){submitting=false;render(data);}).catch(function(error){submitting=false;fail(error);});
    };
    refresh();
    var timer=setInterval(refresh,options.interval || 10000);
    return {refresh:refresh,stop:function(){clearInterval(timer);}};
  }
  LW.researchUpdate={describe:describe,status:status,request:request,bind:bind};
})();
