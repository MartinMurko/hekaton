'use strict';
const $ = id => document.getElementById(id);
const S = {data:null, env:'vit-3', start:0, end:0, alert:null, mode:'grouped', offset:0,
  result:null, replay:false, playing:false, cursor:null, replayRange:null, timer:null,
  generation:0, expanded:false, inFlight:false, job:null, scanPaused:false, poll:null,
  minLevel:'INFO', hidden:[], only:null, history:[], anchorNextResult:false};
let scoreChart, volumeChart, controller, searchTimer, brush=null, chartClickTimer;
const fmt = (t,seconds=false) => new Date(t*1000).toISOString().slice(11,seconds?19:16);
const date = t => new Date(t*1000).toISOString().slice(0,10);
const local = t => new Date(t*1000).toISOString().slice(0,16);
const parseTime = id => Date.parse($(id).value+'Z')/1000;
const num = n => new Intl.NumberFormat('en').format(n);
const el = (tag,cls,text) => {const x=document.createElement(tag);if(cls)x.className=cls;if(text!==undefined)x.textContent=text;return x;};
const level = value => el('span','severity '+value,value);
const signalNames = {templates:'Pattern activity',volume:'Volume drop',heartbeat:'Recurring activity',latency:'Response time',unanswered:'Message imbalance'};
const rangeText = (a,b) => `${date(a)} ${fmt(a)} → ${date(a)===date(b)?'':date(b)+' '}${fmt(b)}`;
function notice(message='') {$('global-error').hidden=!message;$('global-error').textContent=message;}
async function api(path,params={},signal) {
  const r=await fetch(path+'?'+new URLSearchParams(params),{signal});const j=await r.json();
  if(!r.ok)throw new Error(j.error||'Request failed');return j;
}
function watermark(){return S.replay?S.cursor:null;}
function allowed(t){return !S.replay||t<S.cursor;}
function scope(){return {start:S.start,end:Math.min(S.end,watermark()??Infinity)};}
function hasLogFilters(){return S.minLevel!=='INFO'||$('search').value||$('service').value||S.only;}
function syncUrl(){
  const params=new URLSearchParams({env:S.env,from:S.start*1000,to:S.end*1000});
  if(S.minLevel!=='INFO')params.set('min_level',S.minLevel||'ALL');
  if($('search').value)params.set('q',$('search').value);
  if($('service').value)params.set('service',$('service').value);
  if(S.mode==='records')params.set('view','records');
  const url=new URL(location.href);url.search=params;history.replaceState(null,'',url);
}
function renderView(){
  const grouped=S.mode==='grouped';
  $('grouped').setAttribute('aria-pressed',String(grouped));$('records').setAttribute('aria-pressed',String(!grouped));
  $('count-heading').hidden=!grouped;$('occurrence-col').hidden=!grouped;$('log-rows').closest('table').classList.toggle('individual',!grouped);
}
function resetFilters(){S.minLevel='INFO';S.only=null;$('search').value='';$('service').value='';filterChanged();}
function restoreHidden(){S.hidden=[];saveHidden();filterChanged({reuseJob:true});}
// Keep the reader's place while rows/counts change. Reusing DOM nodes also keeps
// focus and expanded details intact instead of collapsing the whole table.
function preserveLogScroll(change,{anchor=true}={}){
  const y=window.scrollY, rows=[...$('log-rows').querySelectorAll('.log-row')];
  const anchors=rows.filter(r=>{const b=r.getBoundingClientRect();return b.bottom>0&&b.top<innerHeight;}).slice(0,6).map(r=>({node:r,top:r.getBoundingClientRect().top}));
  change();
  // A replay reorders patterns as their latest occurrence changes. Following a
  // moving pattern would itself scroll the page, so keep the viewport fixed.
  if(!anchor){window.scrollTo(0,y);return;}
  if(!anchors.length)return;
  const saved=anchors.find(a=>a.node.isConnected&&!a.node.hidden);
  if(saved)window.scrollBy(0,saved.node.getBoundingClientRect().top-saved.top);
  else window.scrollTo(0,y);
}
function pauseReplay(){clearInterval(S.timer);S.timer=null;S.playing=false;renderPlayback();}
function renderPlayback(){
  $('mode-status').textContent=S.replay?(S.playing?'Replaying · '+fmt(S.cursor)+' UTC':'Replay paused · '+fmt(S.cursor)+' UTC'):'Historical data';
  $('mode-status').classList.toggle('replaying',S.playing);
  $('replay').textContent=S.playing?'Ⅱ Pause':S.replay?'▶ Resume replay':'▶ Replay range';
  $('exit-replay').hidden=!S.replay;
  $('cursor-caption').textContent=S.replay?'Replayed through '+fmt(S.cursor)+' UTC':'';
  if(S.data&&S.end>S.start){const q=scope();$('log-scope').textContent=rangeText(q.start,q.end)+' UTC · operational logs';}
}
function visibleAlerts(){return S.data.alerts.filter(a=>a.t>=S.start&&a.t<S.end&&allowed(a.t));}
function updateScope(){
  const q=scope();$('log-scope').textContent=rangeText(q.start,q.end)+' UTC · operational logs';
  $('selection-kind').hidden=!S.alert;$('selection-kind').textContent='Alert context';
  $('range-back').hidden=!S.history.length;
  if(S.data){const lo=S.data.risk[0].t,hi=watermark()??S.data.risk.at(-1).t+60;const full=S.end-S.start>=hi-lo;$('pan-left').disabled=full||S.start<=lo;$('pan-right').disabled=full||S.end>=hi;$('zoom-out').disabled=S.start<=lo&&S.end>=hi;const step=Math.max(1,Math.round((S.end-S.start)/120));$('shift-caption').textContent=`Move by ${num(step)} minutes, keeping the same duration.`;}
  $('time-picker-label').textContent=rangeText(S.start,S.end);
  if($('time-picker').hidden){$('range-start').value=local(S.start);$('range-end').value=local(S.end);}
  $('window-caption').textContent=rangeText(S.start,S.end)+' UTC';
  const recent=$('recent-ranges');recent.replaceChildren();
  if(S.history.length){recent.append(el('h3','','Recent ranges'));for(const r of S.history.slice(-3).reverse()){
    const b=el('button','',rangeText(r.start,r.end));b.addEventListener('click',()=>setWindow(r.start,r.end));recent.append(b);
  }}
}
function closePicker(){const inside=$('time-picker').contains(document.activeElement);$('time-picker').hidden=true;$('time-picker-toggle').setAttribute('aria-expanded','false');if(inside)$('time-picker-toggle').focus();}
function cancelLogs(){clearTimeout(S.poll);S.poll=null;controller?.abort();S.generation++;S.inFlight=false;}
function setWindow(start,end,{alert=null,remember=true,keepReplay=S.replay}={}){
  if(!Number.isFinite(start)||!Number.isFinite(end)||end<=start){notice('Enter a valid UTC start and end time.');return;}
  start=Math.max(Math.floor(start/60)*60,S.data.risk[0].t);end=Math.min(Math.ceil(end/60)*60,S.data.risk.at(-1).t+60,keepReplay?(S.cursor??Infinity):Infinity);
  if(end<=start){notice(keepReplay?'Choose a range before the replay cursor, or exit replay.':'Choose a range within the recorded dates.');return;}
  pauseReplay();if(remember&&S.end>S.start&&(start!==S.start||end!==S.end))S.history.push({start:S.start,end:S.end});
  S.history=S.history.slice(-12);S.replay=keepReplay;if(!keepReplay){S.cursor=null;S.replayRange=null;}
  S.start=start;S.end=end;S.alert=alert;S.offset=0;notice();closePicker();
  syncUrl();
  renderPlayback();updateScope();renderAlerts();renderAlertDetail();updateCharts();loadLogs();
}
function chooseAlert(a){setWindow(a.t-9*60,a.t+60,{alert:a});}
function backRange(){const r=S.history.pop();if(r)setWindow(r.start,r.end,{remember:false});}
function zoomOut(){const n=S.end-S.start;setWindow(S.start-n/2,S.end+n/2);}
function panRange(direction){
  const lo=S.data.risk[0].t,hi=watermark()??S.data.risk.at(-1).t+60;
  const span=Math.min(S.end-S.start,hi-lo),step=Math.max(60,Math.round(span/120)*60);
  const start=Math.min(hi-span,Math.max(lo,S.start+direction*step));setWindow(start,start+span);
}
function renderAlerts(){
  const list=$('alerts'),alerts=visibleAlerts().slice().sort((a,b)=>b.t-a.t),signature=alerts.map(a=>a.id).join(',')+'|'+S.alert?.id+'|'+S.replay;
  if(list.dataset.signature===signature)return;list.dataset.signature=signature;const scroll=list.scrollTop;list.replaceChildren();$('alert-count').textContent=alerts.length;
  if(!alerts.length){const empty=el('div','empty-state',S.replay?'No alerts reached in this range.':'No alerts in this range.');list.append(empty);return;}
  for(const a of alerts){
    const b=el('button','alert-item'+(S.alert?.id===a.id?' selected':''));b.setAttribute('aria-pressed',String(S.alert?.id===a.id));
    const top=el('div','alert-top');top.append(level('ALERT'),el('time','',date(a.t).slice(5)+' '+fmt(a.t)),el('span','alert-score','Score '+a.risk.toFixed(1)));
    b.append(top,el('div','alert-label',a.label));if(a.logger)b.append(el('span','alert-logger',a.logger));b.addEventListener('click',()=>chooseAlert(a));list.append(b);
  }
  list.scrollTop=scroll;
}
function renderAlertDetail(){
  const target=$('alert-detail');target.replaceChildren();target.hidden=!S.alert;if(!S.alert)return;
  const a=S.alert,d=el('details');d.append(el('summary','',`${fmt(a.t)} · ${a.label} · score ${a.risk.toFixed(1)} · Detector evidence`));
  const chips=el('div','contributions');for(const [k,v] of Object.entries(a.components))if(v>0)chips.append(el('span','contribution',`${signalNames[k]||k} ${v.toFixed(1)}`));d.append(chips);
  const ul=el('ul');for(const text of a.evidence)ul.append(el('li','',text));d.append(ul);target.append(d);
  if(a.logger){const b=el('button','link-button pattern-focus','Show pattern logs →');b.addEventListener('click',()=>{$('search').value=a.logger;S.only=null;filterChanged();});target.append(b);}
}
const selectionPlugin={id:'selection',afterDraw(chart){
  const {ctx,chartArea:p,scales:{x}}=chart;if(!p)return;
  const r=brush?.chart===chart?[brush.a,brush.b]:S.alert?[S.alert.t,S.alert.t+60]:null;
  if(r){const [a,b]=r.map(t=>Math.max(p.left,Math.min(p.right,x.getPixelForValue(t))));ctx.save();ctx.fillStyle='rgba(109,169,255,.14)';ctx.fillRect(Math.min(a,b),p.top,Math.max(2,Math.abs(b-a)),p.height);ctx.strokeStyle='#77acff';for(const v of [a,b]){ctx.beginPath();ctx.moveTo(v,p.top);ctx.lineTo(v,p.bottom);ctx.stroke();}ctx.restore();}
  if(S.replay){const px=x.getPixelForValue(S.cursor);if(px>=p.left&&px<=p.right){ctx.save();ctx.strokeStyle='#7fd2b3';ctx.setLineDash([3,3]);ctx.beginPath();ctx.moveTo(px,p.top);ctx.lineTo(px,p.bottom);ctx.stroke();ctx.restore();}}
}};
function chartOptions(score){return {animation:false,responsive:true,maintainAspectRatio:false,normalized:true,parsing:false,layout:{padding:{right:8}},interaction:{mode:'nearest',axis:'x',intersect:false},plugins:{legend:{display:false},tooltip:{backgroundColor:'#223047',titleColor:'#eef5ff',bodyColor:'#becde0',padding:10,filter:c=>c.dataset.label!=='Alert threshold',callbacks:{title:items=>items.length?date(items[0].parsed.x)+' '+fmt(items[0].parsed.x)+' UTC':'',label:c=>c.dataset.label+': '+(c.dataset.label==='Anomaly score'?c.parsed.y.toFixed(1):num(c.parsed.y))}}},scales:{x:{type:'linear',offset:false,min:S.start,max:S.end,grid:{display:false},ticks:{display:!score,maxTicksLimit:7,color:'#8f9fb4',font:{size:9},callback:v=>S.end-S.start>86400?date(v).slice(5):fmt(v)},border:{display:false}},y:{beginAtZero:true,grid:{color:'#253040'},ticks:{color:'#8f9fb4',font:{size:9},maxTicksLimit:4,callback:v=>num(v)},border:{display:false},afterFit:axis=>{axis.width=48;}}}};}
function initCharts(){
  scoreChart=new Chart($('score-chart'),{type:'line',data:{datasets:[]},options:chartOptions(true),plugins:[selectionPlugin]});
  volumeChart=new Chart($('volume-chart'),{type:'bar',data:{datasets:[]},options:chartOptions(false),plugins:[selectionPlugin]});
  for(const chart of [scoreChart,volumeChart]){
    const canvas=chart.canvas;let down=null;
    const at=e=>Math.floor(chart.scales.x.getValueForPixel(e.clientX-canvas.getBoundingClientRect().left)/60)*60;
    canvas.addEventListener('pointerdown',e=>{const px=e.clientX-canvas.getBoundingClientRect().left;if(px<chart.chartArea.left||px>chart.chartArea.right)return;down={x:e.clientX,t:at(e)};canvas.setPointerCapture(e.pointerId);pauseReplay();});
    canvas.addEventListener('pointermove',e=>{if(down){brush={chart,a:down.t,b:at(e)+60};chart.draw();}});
    canvas.addEventListener('pointerup',e=>{
      if(!down)return;const from=down.t,to=at(e),click=Math.abs(e.clientX-down.x)<5;down=null;brush=null;
      clearTimeout(chartClickTimer);
      if(click){const hit=visibleAlerts().find(a=>Math.abs(chart.scales.x.getPixelForValue(a.t+30)-(e.clientX-canvas.getBoundingClientRect().left))<8);chartClickTimer=setTimeout(()=>hit?chooseAlert(hit):setWindow(from,from+60),220);}
      else setWindow(Math.min(from,to),Math.max(from,to)+60);
    });
    canvas.addEventListener('dblclick',()=>{clearTimeout(chartClickTimer);zoomOut();});
    canvas.addEventListener('pointercancel',()=>{down=null;brush=null;chart.draw();});
  }
}
function updateCharts(){
  if(!S.data)return;const all=S.data.risk.filter(r=>r.t>=S.start&&r.t<S.end);
  // Overview buckets retain both peak score and total volume. Detailed windows
  // keep each minute; the chart never changes the queried evidence interval.
  const bucket=Math.max(1,Math.ceil(all.length/1400)),points=[];
  for(let i=0;i<all.length;i+=bucket){const part=all.slice(i,i+bucket);let peak=part[0];for(const r of part)if(r.risk>peak.risk)peak=r;points.push({t:peak.t,risk:peak.risk,n:part.reduce((n,r)=>n+r.n,0)/part.length,seen:part.every(r=>allowed(r.t))});}
  const score=points.map(r=>({x:r.t+30,y:r.seen?r.risk:null}));
  const volume=points.filter(r=>r.seen).map(r=>({x:r.t+30,y:r.n}));
  const alerts=visibleAlerts();
  scoreChart.data.datasets=[{label:'Anomaly score',data:score,borderColor:'#77acff',backgroundColor:'rgba(119,172,255,.06)',borderWidth:1.8,pointRadius:0,fill:true},{label:'Alert threshold',data:[{x:S.start,y:S.data.threshold},{x:Math.min(S.end,watermark()??S.end),y:S.data.threshold}],borderColor:'#967c5b',borderDash:[4,4],borderWidth:1,pointRadius:0},{label:'Alert score',data:alerts.map(a=>({x:a.t+30,y:a.risk})),showLine:false,pointStyle:'triangle',pointRadius:alerts.map(a=>a.id===S.alert?.id?7:5),pointHoverRadius:8,pointBackgroundColor:alerts.map(a=>a.id===S.alert?.id?'#ffb5af':'#ff8b84'),pointBorderColor:alerts.map(a=>a.id===S.alert?.id?'#f1f6ff':'#172131'),pointBorderWidth:1.5}];
  volumeChart.data.datasets=[{label:'Records / minute',data:volume,backgroundColor:'#607a9c',borderRadius:1,barPercentage:1,categoryPercentage:.88}];
  for(const c of [scoreChart,volumeChart]){c.options.scales.x.min=S.start;c.options.scales.x.max=S.end;c.update('none');}
}
function sourceLink(record, parent){
  const s=record.source;const line=el('div','sample-source',s.prepared?'Prepared WARN+ sample · original source unavailable':`${s.file} · byte ${num(s.offset)}`);
  if(!s.prepared){const b=el('button','link-button','View source record');b.addEventListener('click',async()=>{b.disabled=true;try{const params={env:S.env,file:s.file,offset:s.offset};if(S.replay)params.asof=S.cursor;const result=await api('/api/source',params);const pre=el('pre','',result.raw);line.append(pre);if(result.truncated)line.append(el('p','','Source preview capped at 128 KB.'));b.remove();}catch(e){b.disabled=false;line.append(el('p','',e.message));}});line.append(b);}parent.append(line);
}
function saveHidden(){try{localStorage.setItem('seedwatch.hidden.'+S.env,JSON.stringify(S.hidden));}catch{}}
function hidePattern(row){
  if(!S.hidden.some(r=>r.id===row.pattern_id))S.hidden.push({id:row.pattern_id,logger:row.logger,pattern:row.pattern});
  S.hidden=S.hidden.slice(-100);if(S.only?.id===row.pattern_id)S.only=null;saveHidden();filterChanged({reuseJob:true,hideId:row.pattern_id});
}
function onlyPattern(row){S.only={id:row.pattern_id,logger:row.logger,pattern:row.pattern};S.hidden=S.hidden.filter(r=>r.id!==row.pattern_id);saveHidden();filterChanged({reuseJob:true});}
function renderFilters(){
  $('min-level').value=S.minLevel;$('min-level').dataset.level=S.minLevel;renderView();
  const container=$('pattern-filters');container.replaceChildren();
  if(S.only){const chip=el('button','filter-chip','Only: '+S.only.logger+' ×');chip.title=S.only.pattern;chip.addEventListener('click',()=>{S.only=null;filterChanged({reuseJob:true});});container.append(chip);}
  if(S.hidden.length){
    const d=el('details');d.append(el('summary','',`${S.hidden.length} hidden ${S.hidden.length===1?'pattern':'patterns'}`));
    for(const h of S.hidden){const line=el('div','hidden-pattern');line.append(el('span','',h.logger),el('code','',h.pattern));const b=el('button','quiet','Restore');b.addEventListener('click',()=>{S.hidden=S.hidden.filter(r=>r.id!==h.id);saveHidden();filterChanged({reuseJob:true});});line.append(b);d.append(line);}container.append(d);
    const clear=el('button','link-button','Restore all');clear.addEventListener('click',restoreHidden);container.append(clear);
  }
  container.hidden=!S.only&&!S.hidden.length;
  $('reset-filters').hidden=!hasLogFilters();
}
function detail(row){
  const box=el('div','record-detail'),grid=el('dl','detail-grid');
  const field=(name,value)=>{const d=el('div');d.append(el('dt','',name),el('dd','',value));grid.append(d);};
  field('Logger',row.logger);field('Instances',row.instances?row.instances.join(', '):row.instance);
  if(row.rule_name)field('Grouping rule',row.rule_name);
  if(row.count){field('First → last',`${date(row.first/1000)} ${fmt(row.first/1000,true)} → ${date(row.last/1000)} ${fmt(row.last/1000,true)} UTC`);field('Occurrences',num(row.count)+' records across this entire range');}box.append(grid);
  const actions=el('div','detail-actions');for(const [name,fn] of [['Only this pattern',onlyPattern],['Hide this pattern',hidePattern],[row.rule_id?'Edit grouping rule':'Create grouping rule',openRulePanel]]){const b=el('button','quiet',name);b.addEventListener('click',()=>fn(row));actions.append(b);}box.append(actions);
  const patternInfo=el('details');patternInfo.append(el('summary','','Message pattern · changing values collapsed'),el('pre','',row.pattern));box.append(patternInfo);
  box.append(el('p','small muted','Representative original message'),el('pre','',row.message));if(row.message_truncated)box.append(el('p','','Message preview capped at 24 KB. Open the source record for more.'));sourceLink(row,box);
  if(row.entities?.length){const d=el('details');d.append(el('summary','',`Affected hosts / entities (${row.entities.length})`));const list=el('ul','entity-list');for(const e of row.entities)list.append(el('li','',`${e.name} · ${num(e.count)} occurrences`));d.append(list);box.append(d);}
  if(row.samples?.length>1){const d=el('details');d.append(el('summary','',`${row.samples.length} distinct source examples`));for(const r of row.samples){d.append(el('p','',`${date(r.t/1000)} ${fmt(r.t/1000,true)} UTC · ${r.instance}`),el('pre','',r.message));sourceLink(r,d);}box.append(d);}return box;
}
function pauseScan(){clearTimeout(S.poll);S.scanPaused=true;renderScanStatus();}
function collapseLogDetails(){
  for(const tr of $('log-rows').querySelectorAll('.log-row'))if(tr._detail){
    tr._detail.remove();tr._detail=null;tr.classList.remove('open');tr._cells.button.textContent='›';tr._cells.button.setAttribute('aria-expanded','false');
  }
  S.expanded=false;
}
function renderScanStatus(){
  const r=S.result;$('scan-status').hidden=!r?.pending;
  if(r?.pending){const percent=r.progress<.01?'<1%':Math.floor(r.progress*100)+'%',bytes=r.planned_bytes?' · '+num(Math.round(r.scanned_bytes/1e6))+' / '+num(Math.round(r.planned_bytes/1e6))+' MB':'';$('scan-caption').textContent=S.scanPaused?(S.expanded?'Scan paused while inspecting a log':'Scan paused'):(r.progress>=1?'Finishing counts…':'Reading and processing source logs · '+percent+bytes);$('scan-progress').value=r.progress;$('scan-toggle').textContent=S.scanPaused?'Resume scan':'Pause scan';}
}
function updateMessageCell(cell,row){
  cell.children[0].textContent=row.message;
  const summary=cell.children[1],custom=cell.children[2];
  summary.hidden=!(row.entities?.length>1);summary.textContent=summary.hidden?'':`${row.entities.length} hosts / entities · variable values grouped`;
  custom.hidden=!row.rule_name;custom.textContent=row.rule_name?'Custom rule · '+row.rule_name:'';
}
function renderRows(result){
  const tbody=$('log-rows'),existing=new Map([...tbody.querySelectorAll('.log-row')].map(tr=>[tr.dataset.key,tr])),wanted=[];
  preserveLogScroll(()=>{
  for(const [i,row] of result.rows.entries()){
    const key=S.mode+':'+(row.id||row.source.file+':'+row.source.offset+':'+row.source.record),signature=JSON.stringify(row);
    let tr=existing.get(key);
    if(tr){
      const c=tr._cells;tr._row=row;c.button.setAttribute('aria-label','Expand log '+(i+1));c.hide.setAttribute('aria-label','Hide pattern '+(i+1));
      if(tr._signature!==signature){
        c.time.firstChild.nodeValue=fmt(row.t/1000,true);c.time.lastChild.textContent=date(row.t/1000);
        c.svc.firstChild.textContent=row.service;c.svc.lastChild.textContent=row.instances?.length>1?`${row.instances.length} instances`:row.instance;
        updateMessageCell(c.msg,row);
        c.count.textContent=row.count?num(row.count):'—';tr._signature=signature;
      }
      wanted.push(tr);if(tr._detail)wanted.push(tr._detail);continue;
    }
    tr=el('tr','log-row');tr.dataset.key=key;tr.dataset.pattern=row.pattern_id;tr._signature=signature;tr._row=row;
    const control=el('td'),button=el('button','expand-button','›');button.setAttribute('aria-label','Expand log '+(i+1));button.setAttribute('aria-expanded','false');control.append(button);
    const time=el('td','log-time',fmt(row.t/1000,true));time.append(el('small','',date(row.t/1000)));
    const sev=el('td');sev.append(level(row.level));const svc=el('td');svc.append(el('div','service-name',row.service),el('div','instance-name',row.instances?.length>1?`${row.instances.length} instances`:row.instance));
    const msg=el('td');msg.append(el('div','message'),el('div','pattern-summary'),el('div','custom-rule-label'));updateMessageCell(msg,row);
    const count=el('td','numeric log-count',row.count?num(row.count):'—'),actions=el('td','row-actions'),hide=el('button','hide-pattern','Hide pattern');hide.title='Hide this pattern across services';hide.setAttribute('aria-label','Hide pattern '+(i+1));hide.addEventListener('click',e=>{e.stopPropagation();hidePattern(tr._row);});actions.append(hide);
    tr._cells={time,svc,msg,count,button,hide};
    count.hidden=S.mode==='records';tr.append(control,time,sev,svc,msg,count,actions);wanted.push(tr);
    tr.addEventListener('click',()=>{
      if(tr._detail){tr._detail.remove();tr._detail=null;tr.classList.remove('open');button.textContent='›';button.setAttribute('aria-expanded','false');S.expanded=!!tbody.querySelector('.detail-cell');return;}
      pauseReplay();pauseScan();S.expanded=true;tr.classList.add('open');button.textContent='⌄';button.setAttribute('aria-expanded','true');const expanded=el('tr');tr._detail=expanded;const cell=el('td','detail-cell');cell.colSpan=S.mode==='grouped'?7:6;cell.append(detail(tr._row));expanded.append(cell);tr.after(expanded);
    });
  }
  const keep=new Set(wanted);for(const node of [...tbody.children])if(!keep.has(node))node.remove();
  let next=tbody.firstChild;for(const node of wanted){if(node!==next)tbody.insertBefore(node,next);next=node.nextSibling;}
  S.expanded=!!tbody.querySelector('.detail-cell');
  },{anchor:S.anchorNextResult&&!S.playing});S.anchorNextResult=false;
  $('log-rows').closest('table').setAttribute('aria-busy','false');
  const label=S.mode==='grouped'?(result.total_rows===1?'group':'groups'):(result.total_rows===1?'log':'logs'),partial=result.complete?'':' so far';
  $('result-count').textContent=`${num(result.total_records)} ${result.total_records===1?'log':'logs'}${partial}${S.mode==='grouped'?' · '+num(result.total_rows)+' '+label:''}${result.hidden_records?' · '+num(result.hidden_records)+' hidden':''}${S.replay?' · processed through '+fmt(result.end)+' UTC':''}`;
  const warnings=[];if(result.notice)warnings.push(result.notice);if(result.coverage!=='original')warnings.push('Prepared WARN+ samples only. INFO and full originals require the raw exports.');
  $('log-notice').hidden=!warnings.length;$('log-notice').textContent=warnings.join(' ');
  $('page-caption').textContent=result.rows.length?`${num(S.offset+1)}–${num(S.offset+result.rows.length)} of ${num(result.total_rows)} ${label}${partial}`:'0 '+label;
  if(S.mode==='records'&&result.total_records>5100)$('page-caption').textContent+=' · newest 5,100 available; narrow the range for older records';
  $('previous-page').disabled=S.offset===0;$('next-page').disabled=S.offset+result.rows.length>=result.total_rows||S.offset>=5000;
  $('empty-state').hidden=result.rows.length>0;
  if(!result.rows.length){
    const empty=$('empty-state');empty.replaceChildren(el('strong','',result.pending?'Searching this time range…':result.interval_records===0?'No logs in this time range':'No logs match these filters'),el('span','',result.pending?'Results appear as loading continues.':result.interval_records===0?'Zoom out to inspect surrounding activity.':'Lower the minimum level or clear a filter to see more logs.'));
    if(!result.pending){const actions=el('div','empty-actions');if(result.interval_records===0){const b=el('button','quiet','Zoom out');b.addEventListener('click',zoomOut);actions.append(b);}else{if(hasLogFilters()){const b=el('button','quiet','Reset filters');b.addEventListener('click',resetFilters);actions.append(b);}else if(S.minLevel){const b=el('button','quiet','Show all levels');b.addEventListener('click',()=>{S.minLevel='';filterChanged();});actions.append(b);}if(S.hidden.length){const b=el('button','quiet','Restore hidden patterns');b.addEventListener('click',restoreHidden);actions.append(b);}}empty.append(actions);}
  }
  renderScanStatus();
}
function logParams(){const q=scope(),params={env:S.env,start:q.start,end:q.end,mode:S.mode,text:$('search').value,min_level:S.minLevel,service:$('service').value,exclude:S.hidden.map(r=>r.id).join(','),only:S.only?.id||''};if(S.replay)params.asof=S.cursor;return params;}
async function fetchLogBatch(generation,advance=1){
  clearTimeout(S.poll);S.poll=null;
  S.inFlight=true;controller=new AbortController();
  try{
    const params=S.job?{job:S.job,exclude:S.hidden.map(r=>r.id).join(','),only:S.only?.id||''}:logParams();
    if(S.job&&S.replay)Object.assign(params,{extend_to:S.cursor,asof:S.cursor});
    Object.assign(params,{offset:S.offset,limit:50,advance});
    const result=await api('/api/logs',params,controller.signal);if(generation!==S.generation)return;
    S.job=result.job;if(S.expanded){S.nextResult=result;renderScanStatus();}else{S.result=result;renderRows(result);}
    if((result.pending||(S.replay&&result.end<scope().end))&&!S.scanPaused)S.poll=setTimeout(()=>fetchLogBatch(generation),120);
  }catch(e){if(e.name==='AbortError'||generation!==S.generation)return;clearTimeout(S.poll);S.scanPaused=true;renderScanStatus();$('result-count').textContent=S.result?'Showing the last loaded results':'Logs unavailable';$('log-notice').hidden=false;$('log-notice').replaceChildren(el('span','',e.message));const retry=el('button','quiet','Retry');retry.addEventListener('click',loadLogs);$('log-notice').append(retry);if(!S.result)$('empty-state').replaceChildren(el('strong','','Could not load these logs'));}
  finally{if(generation===S.generation)S.inFlight=false;}
}
function loadLogs(){
  cancelLogs();const generation=S.generation;S.job=null;S.nextResult=null;S.scanPaused=false;S.anchorNextResult=false;preserveLogScroll(()=>{collapseLogDetails();renderFilters();updateScope();});
  $('scan-status').hidden=true;$('log-rows').closest('table').setAttribute('aria-busy','true');$('result-count').textContent='Updating results…';$('log-notice').hidden=true;$('previous-page').disabled=true;$('next-page').disabled=true;
  if(!$('log-rows').children.length){$('empty-state').hidden=false;$('empty-state').textContent='Reading and processing selected source logs…';}
  if(scope().end<=scope().start){$('empty-state').textContent='Waiting for the first replayed minute.';return;}
  fetchLogBatch(generation);
}
function filterChanged({reuseJob=false,hideId=null}={}){
  clearTimeout(searchTimer);pauseReplay();S.offset=0;syncUrl();
  if(reuseJob&&S.job){
    cancelLogs();S.expanded=false;S.nextResult=null;S.anchorNextResult=true;
    preserveLogScroll(()=>{renderFilters();if(hideId)for(const tr of $('log-rows').querySelectorAll('.log-row'))if(tr.dataset.pattern===hideId){tr._detail?.remove();tr.remove();}});
    fetchLogBatch(S.generation,0);
  }else loadLogs();
}
function page(direction){pauseScan();S.expanded=false;S.offset=Math.max(0,S.offset+direction*50);fetchLogBatch(S.generation,0);}
async function loadWorkspace(env){
  pauseReplay();cancelLogs();notice();const requested=env;
  try{
    const data=await api('/api/workspace',{env});if($('environment').value!==requested)return;
    S.data=data;S.env=env;S.replay=false;S.cursor=null;S.replayRange=null;S.alert=null;S.history=[];S.start=0;S.end=0;S.only=null;S.offset=0;S.result=null;$('log-rows').replaceChildren();
    try{const hidden=JSON.parse(localStorage.getItem('seedwatch.hidden.'+env)||'[]');S.hidden=Array.isArray(hidden)?hidden.filter(r=>r&&typeof r.id==='string'&&typeof r.logger==='string').slice(-100):[];}catch{S.hidden=[];}
    $('environment').replaceChildren(...data.envs.map(name=>{const o=el('option','',name);o.value=name;o.selected=name===env;return o;}));
    $('service').replaceChildren(el('option','','All services'),...data.services.map(s=>el('option','',s)));$('service').firstChild.value='';
    $('threshold-label').textContent='Alert threshold '+data.threshold;
    $('coverage-heading').textContent=data.coverage==='original'?'Operational logs · Times in UTC':'Operational logs · Prepared WARN+ samples only';
    $('available-range').textContent='Available: '+rangeText(data.risk[0].t,data.risk.at(-1).t+60)+' UTC';
    const stats=data.stats,run=$('run-details');run.replaceChildren();const grid=el('div','run-grid');for(const [name,value] of [['Ingested records',num(stats.lines||0)],['Source size',((stats.bytes||0)/1e9).toFixed(1)+' GB'],['Recorded ingest throughput',num(stats.lines_per_s||0)+' records/s'],['Estimated ingest peak memory',(stats.est_total_peak_mb||'—')+' MB']]){const c=el('div','muted',name);c.append(el('strong','',value));grid.append(c);}run.append(grid,el('p','','Recorded ingestion measurements, including communication traces. The workspace shows operational logs. Grouping and hidden patterns affect only the viewer; the detector, learning profile and alert policy are unchanged.'),el('p','','New queries read, parse and group the raw source files progressively. A sparse time index skips unrelated blocks; this is not a fully indexed log database. Pattern hide/restore reuses the current results. Replay adds only the newly reached interval. Expanding a message pauses the scan. No model runs during this work.'));
    const url=new URL(location.href),from=Number(url.searchParams.get('from'))/1000,to=Number(url.searchParams.get('to'))/1000;
    const sameEnv=url.searchParams.get('env')===env;
    const minimum=sameEnv?url.searchParams.get('min_level'):null;
    const levels=data.levels||['INFO','WARN','ERROR'];
    $('min-level').replaceChildren(el('option','','All levels'),...levels.map(lv=>el('option','',lv)));$('min-level').firstChild.value='';
    S.minLevel=minimum==='ALL'?'':levels.includes(minimum)?minimum:levels.includes('INFO')?'INFO':levels[0]||'';
    S.mode=sameEnv&&url.searchParams.get('view')==='records'?'records':'grouped';
    $('search').value=sameEnv?url.searchParams.get('q')||'':'';
    const service=sameEnv?url.searchParams.get('service')||'':'';$('service').value=data.services.includes(service)?service:'';
    if(url.searchParams.get('env')===env&&from&&to)setWindow(from,to,{remember:false,keepReplay:false});
    else if(data.alerts.length)setWindow(data.alerts[0].t-15*60,data.alerts[0].t+40*60,{remember:false,keepReplay:false});
    else setWindow(data.risk[0].t,data.risk[0].t+3600,{remember:false,keepReplay:false});
  }catch(e){$('environment').value=S.env;notice(e.message);}
}
function tick(){
  if(!S.playing)return;S.cursor=Math.min(S.replayRange.end,S.cursor+60);renderPlayback();renderAlerts();updateCharts();
  if(!S.inFlight&&!S.poll&&!S.scanPaused){S.offset=0;fetchLogBatch(S.generation);}
  if(S.cursor>=S.replayRange.end){pauseReplay();$('replay').textContent='↺ Replay again';}
}
function startReplay(){
  if(S.playing){pauseReplay();return;}
  const restart=!S.replay||S.cursor>=S.replayRange.end;
  if(restart){S.replayRange={start:S.start,end:S.end};S.cursor=S.start+60;S.replay=true;S.result=null;$('log-rows').replaceChildren();}
  S.start=S.replayRange.start;S.end=S.replayRange.end;S.alert=null;S.playing=true;S.offset=0;S.scanPaused=false;collapseLogDetails();renderAlertDetail();renderPlayback();updateScope();renderAlerts();updateCharts();
  if(restart)loadLogs();else if(!S.inFlight)fetchLogBatch(S.generation);
  S.timer=setInterval(tick,1000/Number($('speed').value));
}
$('environment').addEventListener('change',e=>loadWorkspace(e.target.value));
$('time-picker-toggle').addEventListener('click',()=>{const open=$('time-picker').hidden;if(open){pauseReplay();const lo=S.data.risk[0].t,hi=watermark()??S.data.risk.at(-1).t+60;for(const id of ['range-start','range-end']){$(id).min=local(lo);$(id).max=local(hi);}$('available-range').textContent=S.replay?'Replay paused at '+date(hi)+' '+fmt(hi)+' UTC. Exit replay to browse later data.':'Available: '+rangeText(lo,hi)+' UTC';$('range-start').value=local(S.start);$('range-end').value=local(scope().end);}else{closePicker();return;}$('time-picker').hidden=false;$('time-picker-toggle').setAttribute('aria-expanded','true');});
$('time-form').addEventListener('submit',e=>{e.preventDefault();setWindow(parseTime('range-start'),parseTime('range-end'));});
for(const b of document.querySelectorAll('[data-range]'))b.addEventListener('click',()=>{const end=watermark()??S.data.risk.at(-1).t+60;setWindow(b.dataset.range==='all'?S.data.risk[0].t:end-Number(b.dataset.range),end);});
$('all-data').addEventListener('click',()=>setWindow(S.data.risk[0].t,S.data.risk.at(-1).t+60,{keepReplay:false}));
$('range-back').addEventListener('click',backRange);$('zoom-out').addEventListener('click',zoomOut);
$('pan-left').addEventListener('click',()=>panRange(-1));$('pan-right').addEventListener('click',()=>panRange(1));
document.addEventListener('click',e=>{if(!e.target.closest('.time-picker-wrap'))closePicker();});document.addEventListener('keydown',e=>{if(e.key==='Escape')closePicker();});
$('replay').addEventListener('click',startReplay);$('exit-replay').addEventListener('click',()=>{pauseReplay();S.replay=false;S.cursor=null;S.replayRange=null;renderPlayback();updateScope();renderAlerts();updateCharts();loadLogs();});
$('speed').addEventListener('change',()=>{if(S.playing){clearInterval(S.timer);S.timer=setInterval(tick,1000/Number($('speed').value));}});
$('search').addEventListener('input',()=>{pauseReplay();cancelLogs();$('result-count').textContent='Updating search…';clearTimeout(searchTimer);searchTimer=setTimeout(filterChanged,200);});
$('service').addEventListener('change',filterChanged);$('min-level').addEventListener('change',e=>{S.minLevel=e.target.value;filterChanged();});
$('reset-filters').addEventListener('click',resetFilters);
for(const mode of ['grouped','records'])$(mode).addEventListener('click',()=>{if(S.mode===mode)return;S.mode=mode;filterChanged();});
$('scan-toggle').addEventListener('click',()=>{if(S.scanPaused){S.scanPaused=false;preserveLogScroll(collapseLogDetails);if(S.nextResult){S.result=S.nextResult;S.nextResult=null;}if(S.result)renderRows(S.result);if(S.result?.pending&&!S.inFlight)fetchLogBatch(S.generation);}else pauseScan();renderScanStatus();});
$('previous-page').addEventListener('click',()=>page(-1));$('next-page').addEventListener('click',()=>page(1));
$('run-toggle').addEventListener('click',()=>{const show=$('run-details').hidden;$('run-details').hidden=!show;$('run-toggle').setAttribute('aria-expanded',String(show));$('run-toggle').lastElementChild.textContent=show?'▴':'▾';});
initCharts();const initialEnv=new URL(location.href).searchParams.get('env')||'vit-3';$('environment').value=initialEnv;loadWorkspace(initialEnv);
