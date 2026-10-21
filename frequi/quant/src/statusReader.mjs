// Ignore a delayed status response after a newer read or a human action.
export function createStatusReader() {
  let generation = 0;
  return {
    invalidate() { generation += 1; },
    async read(load, apply, failed) {
      const request = ++generation;
      try {
        const snapshot = await load();
        if (request === generation) apply(snapshot);
      } catch (error) {
        if (request === generation) failed(error);
      }
    },
  };
}
