// This sender only registers a check. It never retries or changes trading intent.
export function createConnectionCheckSender(timeoutMs = 6000) {
  let inFlight = null;
  return {
    send(load) {
      if (inFlight) return inFlight;
      const controller = new AbortController();
      let timer;
      const deadline = new Promise((_, reject) => {
        timer = setTimeout(() => {
          controller.abort();
          reject(Error('连接检查受理结果尚未确认；请查看任务状态。没有自动重发。'));
        }, timeoutMs);
      });
      inFlight = Promise.race([Promise.resolve().then(() => load(controller.signal)), deadline])
        .finally(() => { clearTimeout(timer); inFlight = null; });
      return inFlight;
    },
  };
}

export function newerConnectionCheck(current, incoming) {
  if (!incoming?.id) return current || {};
  if (!current?.id) return incoming;
  // Persisted revisions span tasks and survive wall-clock rollback. Missing legacy
  // revisions rank as zero, retaining timestamp ordering between old snapshots.
  const fields = ['revision', 'requested_at_ms', 'updated_at_ms', 'completed_at_ms', 'started_at_ms'];
  for (const field of fields) {
    const oldValue = Number(current[field]) || 0, newValue = Number(incoming[field]) || 0;
    if (oldValue !== newValue) return newValue > oldValue ? incoming : current;
  }
  return incoming;
}

export const checkStateLabels = {
  accepted: '检查已登记', checking: '正在检查', waiting: '等待可执行时间',
  complete: '连接检查完成', failed: '本次检查未成功', interrupted: '检查被中断',
};

export const checkStageLabels = {
  accepted: '已登记，等待检查', permission: '合并并取得检查权限',
  budget: '核对请求额度', rest: '核对账户接口', streams: '核对行情通道',
  reconcile: '核对真实执行条件', complete: '本次检查结束',
};

export function checkPartLabel(result) {
  if (result === true || result?.ok === true || result?.state === 'healthy') return '检查正常';
  if (result === false || result?.ok === false) return '尚未恢复';
  return result?.reason || result?.state || '尚未检查';
}
