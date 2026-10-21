import {createConnectionCheckSender,newerConnectionCheck} from './src/connectionCheck.mjs';
import assert from 'node:assert/strict';

let calls=0, release;
const sender=createConnectionCheckSender(1000);
const load=()=>{calls++;return new Promise(resolve=>{release=resolve;});};
const a=sender.send(load), b=sender.send(load);
assert.equal(a,b,'parallel clicks must merge');
await new Promise(resolve=>setTimeout(resolve,1));release({ok:true});await a;
assert.equal(calls,1,'inflight registration was duplicated');
let timeoutCalls=0, aborted=false;
const timed=createConnectionCheckSender(5);
await assert.rejects(timed.send(signal=>{timeoutCalls++;signal.addEventListener('abort',()=>{aborted=true;});
  return new Promise(()=>{});}), /未确认/);
assert.equal(timeoutCalls,1,'timeout must not replay registration');
assert.equal(aborted,true,'transport should receive abort');
const complete={id:'a',requested_at_ms:1,updated_at_ms:20,state:'complete'};
const accepted={id:'a',requested_at_ms:1,updated_at_ms:10,state:'accepted'};
assert.equal(newerConnectionCheck(complete,accepted).state,'complete');
const newer={id:'b',requested_at_ms:2,updated_at_ms:30,state:'accepted'};
assert.equal(newerConnectionCheck(complete,newer).id,'b');
const oldBeforeRollback={id:'old',revision:10,requested_at_ms:10000,updated_at_ms:11000,state:'complete'};
const newAfterRollback={id:'new',revision:11,requested_at_ms:1000,updated_at_ms:1000,state:'accepted'};
assert.equal(newerConnectionCheck(oldBeforeRollback,newAfterRollback),newAfterRollback,
  'new task revision must survive wall clock rollback');
assert.equal(newerConnectionCheck(newAfterRollback,oldBeforeRollback),newAfterRollback,
  'late old task must not replace newer revision');
const completedAfterRollback={...newAfterRollback,revision:12,updated_at_ms:500,state:'complete'};
assert.equal(newerConnectionCheck(newAfterRollback,completedAfterRollback),completedAfterRollback,
  'same task phase revision must survive wall clock rollback');
assert.equal(newerConnectionCheck(completedAfterRollback,{...newAfterRollback,updated_at_ms:20000}),
  completedAfterRollback,'late old phase must not replace completed revision');
const legacyBeforeRollback={id:'legacy',requested_at_ms:10000,updated_at_ms:20000,state:'complete'};
const firstRevision={...newAfterRollback,revision:1};
assert.equal(newerConnectionCheck(legacyBeforeRollback,firstRevision),firstRevision,
  'first revision must replace a legacy snapshot after clock rollback');
assert.equal(newerConnectionCheck(firstRevision,legacyBeforeRollback),firstRevision,
  'legacy late response must not replace a persisted revision');
console.log('11 frontend registration/concurrency/deadline/order scenarios passed');
