import assert from 'node:assert/strict';
import test from 'node:test';
import { createAdaptiveConcurrency, AdaptiveConcurrencyError } from '../dist/esm/index.js';

test('runs tasks and reports current capacity', async () => {
  const gate = createAdaptiveConcurrency({ initial: 2 });
  assert.equal(await gate.run(() => 7), 7);
  assert.deepEqual(gate.snapshot(), { limit: 2, active: 0, queued: 0, completedInWindow: 1 });
});

test('admits queued tasks FIFO when a permit becomes available', async () => {
  const gate = createAdaptiveConcurrency({ min: 1, max: 1, initial: 1, maxQueue: 2 });
  const order = [];
  let release;
  const first = gate.run(
    () =>
      new Promise((resolve) => {
        release = resolve;
      }),
  );
  const second = gate.run(() => {
    order.push('second');
  });
  const third = gate.run(() => {
    order.push('third');
  });
  assert.equal(gate.snapshot().queued, 2);
  await new Promise((resolve) => setImmediate(resolve));
  release();
  await Promise.all([first, second, third]);
  assert.deepEqual(order, ['second', 'third']);
});

test('rejects admission when the bounded queue is full', async () => {
  const gate = createAdaptiveConcurrency({ min: 1, max: 1, initial: 1, maxQueue: 0 });
  let release;
  const held = new Promise((resolve) => {
    release = resolve;
  });
  const first = gate.run(() => held);
  await new Promise((resolve) => setImmediate(resolve));
  await assert.rejects(
    gate.run(() => 2),
    AdaptiveConcurrencyError,
  );
  release();
  await first;
});

test('increases the limit after a healthy completion window', async () => {
  const gate = createAdaptiveConcurrency({
    min: 1,
    max: 4,
    initial: 1,
    windowSize: 1,
    now: (() => {
      let n = 0;
      return () => n++;
    })(),
  });
  await gate.run(() => 1);
  assert.equal(gate.snapshot().limit, 2);
});

test('decreases the limit after failed work', async () => {
  const gate = createAdaptiveConcurrency({ min: 1, max: 4, initial: 4, windowSize: 1 });
  await assert.rejects(
    gate.run(() => {
      throw new Error('failure');
    }),
    /failure/,
  );
  assert.equal(gate.snapshot().limit, 2);
});

test('decreases the limit after a latency target is exceeded', async () => {
  let time = 0;
  const gate = createAdaptiveConcurrency({
    min: 1,
    max: 4,
    initial: 4,
    windowSize: 1,
    latencyTargetMs: 5,
    now: () => time,
  });
  const run = gate.run(() => {
    time = 6;
  });
  await run;
  assert.equal(gate.snapshot().limit, 2);
});

test('aborts queued work without invoking it', async () => {
  const gate = createAdaptiveConcurrency({ min: 1, max: 1, initial: 1 });
  let release;
  const held = new Promise((resolve) => {
    release = resolve;
  });
  const first = gate.run(() => held);
  await new Promise((resolve) => setImmediate(resolve));
  const controller = new AbortController();
  let invoked = false;
  const queued = gate.run(
    () => {
      invoked = true;
    },
    { signal: controller.signal },
  );
  controller.abort(new Error('cancelled'));
  await assert.rejects(queued, /cancelled/);
  release();
  await first;
  assert.equal(invoked, false);
  assert.equal(gate.snapshot().queued, 0);
});

test('validates options, callbacks, and abort signals', async () => {
  assert.throws(() => createAdaptiveConcurrency({ initial: 5, max: 4 }), RangeError);
  assert.throws(() => createAdaptiveConcurrency({ multiplicativeDecrease: 1 }), RangeError);
  const gate = createAdaptiveConcurrency();
  assert.throws(() => gate.run(/** @type {any} */ (3)), TypeError);
  assert.throws(() => gate.run(() => 1, { signal: /** @type {any} */ ({}) }), TypeError);
});
