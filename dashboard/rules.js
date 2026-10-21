'use strict';
const RulesUI={state:null,env:null,id:null,preview:null,changed:false,wasPaused:true,request:0,busy:false};

async function postRule(path,body){
  const response=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  const result=await response.json();if(!response.ok)throw new Error(result.error||'Request failed');return result;
}
function ruleError(message=''){$('rule-error').hidden=!message;$('rule-error').textContent=message;}
function ruleDraft(){return {name:$('rule-name').value,template:$('rule-template').value,service:$('rule-service').value,level:$('rule-level').value,logger:$('rule-logger').value};}
function ruleDraftKey(){return JSON.stringify(ruleDraft());}
function ruleBusy(value){
  RulesUI.busy=value;
  for(const control of $('rules-dialog').querySelectorAll('button,input,select,textarea'))control.disabled=value;
  $('save-rule').disabled=value||RulesUI.preview!==ruleDraftKey();
  if(!value)$('new-rule').disabled=!RulesUI.state||RulesUI.state.rules.length>=50;
}
function clearRulePreview(){
  RulesUI.request++;RulesUI.preview=null;$('save-rule').disabled=true;$('rule-preview').hidden=true;
  $('rule-save-hint').textContent='Preview the pattern before saving.';ruleError();
}
function scopeLabel(rule){return [rule.service||'All services',rule.level||'All levels',rule.logger||'All loggers'].join(' · ');}
function ruleCard(rule,{removed=false}={}){
  const card=el('article','rule-card'),heading=el('div','rule-card-heading'),buttons=el('div');
  heading.append(el('strong','',rule.name),buttons);
  if(removed){const restore=el('button','quiet','Restore');restore.addEventListener('click',()=>changeRule('restore',rule.id));buttons.append(restore);}
  else{
    const edit=el('button','quiet','Edit'),remove=el('button','quiet','Remove');
    edit.setAttribute('aria-label','Edit '+rule.name);remove.setAttribute('aria-label','Remove '+rule.name);
    edit.addEventListener('click',()=>editGroupingRule(rule));remove.addEventListener('click',()=>changeRule('delete',rule.id));buttons.append(edit,remove);
  }
  card.append(heading,el('pre','',rule.template),el('div','rule-scope',scopeLabel(rule)));return card;
}
function showRuleList(){
  ruleError();$('rule-form').hidden=true;$('rules-home').hidden=false;
  const state=RulesUI.state,list=$('rules-list');list.replaceChildren();
  if(!state.rules.length)list.append(el('p','rules-empty','No saved rules yet. Messages currently use automatic grouping.'));
  for(const rule of state.rules)list.append(ruleCard(rule));
  const removed=state.removed||[];$('removed-rules').hidden=!removed.length;
  $('removed-rules-caption').textContent=`Removed rules (${removed.length})`;
  $('removed-rules-list').replaceChildren(...removed.map(r=>ruleCard(r.rule,{removed:true})));
  $('new-rule').disabled=state.rules.length>=50;
  $('manage-rules').textContent='Grouping rules'+(state.rules.length?' · '+state.rules.length:'');
}
async function openRulePanel(row=null){
  RulesUI.wasPaused=S.scanPaused;pauseReplay();pauseScan();RulesUI.changed=false;RulesUI.env=S.env;
  RulesUI.state=null;RulesUI.preview=null;ruleError();$('rules-context').textContent=S.env+' · saved on this server';
  $('rules-home').hidden=false;$('rule-form').hidden=true;$('rules-list').replaceChildren(el('p','rules-empty','Loading saved rules…'));$('new-rule').disabled=true;$('removed-rules').hidden=true;
  if(!$('rules-dialog').open)$('rules-dialog').showModal();
  try{
    const state=await api('/api/pattern-rules',{env:RulesUI.env});if(!$('rules-dialog').open)return;
    RulesUI.state=state;showRuleList();
    if(row){const existing=state.rules.find(r=>r.id===row.rule_id);editGroupingRule(existing||null,existing?null:row);}
  }catch(error){ruleError(error.message);}
}
function editGroupingRule(rule=null,row=null){
  RulesUI.id=rule?.id||null;$('rules-home').hidden=true;$('rule-form').hidden=false;
  $('rule-editor-title').textContent=rule?'Edit grouping rule':'New grouping rule';
  $('rule-name').value=rule?.name||'';
  const suggested=row?.pattern.replace(/ \| [\w ]+=.*$/,'').replace(/<[^>]+>/g,'<*>');
  $('rule-template').value=rule?.template||suggested||'';
  const options=(id,values,first,selected)=>{const empty=el('option','',first);empty.value='';$(id).replaceChildren(empty,...values.map(v=>el('option','',v)));$(id).value=values.includes(selected)?selected:'';};
  options('rule-service',S.data.services,'All services',rule?.service||row?.service||'');
  const levels=[...new Set([...(S.data.levels||[]),rule?.level,row?.level].filter(Boolean))];
  options('rule-level',levels,'All levels',rule?.level||row?.level||'');
  $('rule-logger').value=rule?.logger||row?.logger||'';
  $('rule-original').hidden=!row;$('rule-original').open=false;$('rule-original-message').textContent=row?.message||'';
  clearRulePreview();$('rule-name').focus({preventScroll:true});$('rules-dialog').scrollTop=0;
}
function previewExample(record){
  const box=el('div');box.append(el('p','preview-message-meta',`${date(record.t/1000)} ${fmt(record.t/1000,true)} UTC · ${record.service} · ${record.level}`),el('pre','',record.message));return box;
}
async function previewGroupingRule(){
  ruleError();if(!S.job){ruleError('Load some logs before previewing this rule.');return;}
  const key=ruleDraftKey(),request=++RulesUI.request;RulesUI.preview=null;ruleBusy(true);$('preview-rule').textContent='Checking samples…';
  try{
    const body={env:RulesUI.env,job:S.job,id:RulesUI.id,rule:ruleDraft(),exclude:S.hidden.map(r=>r.id),only:S.only?.id||''};
    if(S.replay)body.asof=S.cursor;
    const result=await postRule('/api/pattern-rules/preview',body);
    if(request!==RulesUI.request||!$('rules-dialog').open)return;
    const target=$('rule-preview');target.hidden=false;target.replaceChildren(el('strong','',`${num(result.matched)} of ${num(result.tested)} sampled messages match`),el('p','',`From ${num(result.patterns)} current ${result.patterns===1?'pattern':'patterns'}. This is a sample preview, not a full-range count.${result.partial_query?' The source scan is still incomplete.':''}`));
    if(result.earlier_rules.length)target.append(el('p','',`Earlier rules also match these examples: ${result.earlier_rules.join(', ')}. The first saved match takes priority.`));
    if(!result.matched)target.append(el('p','','No matches in these samples. Adjust the pattern or save it for messages outside the loaded samples.'));
    for(const sample of result.examples.slice(0,3))target.append(previewExample(sample));
    if(result.examples.length>3){const more=el('details');more.append(el('summary','','More matching examples'));for(const sample of result.examples.slice(3))more.append(previewExample(sample));target.append(more);}
    if(result.nonmatches.length){const other=el('details');other.append(el('summary','','Non-matching examples in this scope'));for(const sample of result.nonmatches)other.append(previewExample(sample));target.append(other);}
    RulesUI.preview=key;$('rule-save-hint').textContent='Saving applies this rule to historical and future log queries.';
  }catch(error){if(request===RulesUI.request)ruleError(error.message);}
  finally{if(request===RulesUI.request){ruleBusy(false);$('preview-rule').textContent='Preview matches';}}
}
async function changeRule(action,id=null){
  if(RulesUI.busy)return;
  if(action==='save'&&RulesUI.preview!==ruleDraftKey()){ruleError('Preview the current pattern before saving.');return;}
  ruleError();ruleBusy(true);
  try{
    const body={env:RulesUI.env,revision:RulesUI.state.revision,action,id};if(action==='save')body.rule=ruleDraft();
    RulesUI.state=await postRule('/api/pattern-rules',body);RulesUI.changed=true;
    showRuleList();S.only=null;S.offset=0;syncUrl();loadLogs();
    if(action==='save')$('rules-dialog').close();
  }catch(error){ruleError(error.message);}
  finally{ruleBusy(false);}
}
$('manage-rules').addEventListener('click',()=>openRulePanel());
$('close-rules').addEventListener('click',()=>$('rules-dialog').close());
$('new-rule').addEventListener('click',()=>editGroupingRule());
$('back-to-rules').addEventListener('click',showRuleList);
$('preview-rule').addEventListener('click',previewGroupingRule);
$('rule-form').addEventListener('submit',event=>{event.preventDefault();changeRule('save',RulesUI.id);});
for(const event of ['input','change'])$('rule-form').addEventListener(event,clearRulePreview);
$('rules-dialog').addEventListener('cancel',event=>{if(RulesUI.busy)event.preventDefault();});
$('rules-dialog').addEventListener('close',()=>{
  RulesUI.request++;RulesUI.busy=false;
  if(!RulesUI.changed&&!RulesUI.wasPaused&&!S.expanded){S.scanPaused=false;if(S.result?.pending&&!S.inFlight)fetchLogBatch(S.generation);renderScanStatus();}
  $('manage-rules').focus({preventScroll:true});
});
