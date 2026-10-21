// One durable identity per edit; an unknown reply is never retried with a new ID.
export const equitySymbols=['BTCUSDT','ETHUSDT'];
export function emptyEquityEdits(){return Object.fromEntries(equitySymbols.map(s=>[s,{editing:false,draft:'',revision:0,pending:null,submitting:false,error:'',notice:''}]))}
const canonicalAmount=v=>{let [whole,frac='']=String(v).split('.');whole=whole.replace(/^0+(?=.)/,'');frac=frac.replace(/0+$/,'');return whole+(frac?'.'+frac:'');};
export function createEquityEditor({states,storage,makeId=()=>crypto.randomUUID?crypto.randomUUID():Array.from(crypto.getRandomValues(new Uint8Array(24)),v=>v.toString(16).padStart(2,'0')).join('')}){
  const key=s=>'strategy-equity-pending-v1-'+s;
  const validAmount=v=>typeof v==='string'&&/^[0-9]{1,12}(?:\.[0-9]{1,8})?$/.test(v)&&Number(v)>0;
  for(const s of equitySymbols){try{const p=JSON.parse(storage.getItem(key(s))||'null');if(p&&p.symbol===s&&validAmount(p.target_equity)&&Number.isInteger(p.expected_revision)&&p.expected_revision>=0&&/^[A-Za-z0-9_-]{16,80}$/.test(p.operation_id)){Object.assign(states[s],{pending:p,editing:true,draft:p.target_equity,revision:p.expected_revision,notice:'上次保存结果待核对，请先查询原保存编号。'});}}catch{states[s].error='无法恢复保存编号，请检查浏览器存储后再编辑。';}}
  function accept(s,result){const p=states[s].pending,a=result?.adjustment;if(!p||!a||a.operation_id!==p.operation_id||a.symbol!==s||canonicalAmount(a.target_equity)!==canonicalAmount(p.target_equity)||a.revision!==p.expected_revision+1)throw Error('保存结果身份尚未确认，请查询原保存编号。');storage.removeItem(key(s));Object.assign(states[s],{pending:null,editing:false,error:'',notice:`已将核对时的策略权益调整为 ${a.target_equity} USDT；之后盈亏继续累计。`});return a;}
  return {
    begin(s,ledger){const v=states[s];if(v.pending||v.submitting)return;Object.assign(v,{editing:true,draft:ledger?.verified?Number(ledger.equity).toFixed(8).replace(/\.?0+$/,''):'100',revision:ledger?.allocation_revision??0,error:'',notice:''});},
    cancel(s){if(!states[s].pending&&!states[s].submitting)states[s].editing=false;},
    async save(s,send){const v=states[s];if(v.submitting)return;v.error='';v.notice='';try{if(!v.pending){if(!validAmount(v.draft))throw Error('请输入正数，最多12位整数和8位小数。');const p={confirmed:true,symbol:s,target_equity:v.draft,expected_revision:v.revision,operation_id:makeId()};storage.setItem(key(s),JSON.stringify(p));v.pending=p;}v.submitting=true;const result=await send(v.pending);accept(s,result);}catch(e){if(e.definite===true){storage.removeItem(key(s));v.pending=null;}v.error=e.message||'保存结果未确认，请查询原保存编号。';}finally{v.submitting=false;}},
    async query(s,read){const v=states[s];if(!v.pending||v.submitting)return;v.submitting=true;v.error='';try{const result=await read(v.pending.operation_id);if(result?.adjustment)accept(s,result);else v.notice='暂未查到该编号的已保存流水；可稍后再查，或用“重试原保存”重试同一编号。';}catch(e){v.error=e.message||'查询暂未完成，请保留原保存编号。';}finally{v.submitting=false;}}
  };
}
