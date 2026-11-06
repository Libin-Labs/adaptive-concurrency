# @lp-labs/adaptive-concurrency

A FIFO concurrency gate with an explicit bounded wait queue. It adjusts its permit limit using
completion-window feedback: successful, on-time windows increase additively; failures or slow
windows decrease multiplicatively. It has zero runtime and peer dependencies.

## Install

```bash
npm install @lp-labs/adaptive-concurrency
```

The package includes ESM/CommonJS builds and TypeScript declarations. CommonJS:
`require('@lp-labs/adaptive-concurrency')`.

## Quickstart

```js
import { createAdaptiveConcurrency } from '@lp-labs/adaptive-concurrency';

const requests = createAdaptiveConcurrency({
  min: 2,
  max: 24,
  initial: 4,
  maxQueue: 100,
  windowSize: 20,
  latencyTargetMs: 300,
});
const profile = await requests.run(async () => ({ id: '42', name: 'Example' }));
console.log(profile.name);
```

## API and customization

`createAdaptiveConcurrency(options)` returns `run(task, { signal? })` and `snapshot()`. Options
include `min` (1), `max` (32), `initial` (up to 4), `maxQueue` (100), `windowSize` (10),
`latencyTargetMs` (250), `additiveIncrease` (1), `multiplicativeDecrease` (0.5), and an injectable
`now` clock. Integer capacities must be safe and valid; the multiplicative factor must be in
`(0, 1)`. A zero queue capacity admits work only when a permit is immediately available.

At the end of each completion window, the gate increases the limit by the additive amount if every
task succeeded within target; otherwise it multiplies the current limit by the decrease factor,
rounds down, and clamps to `min`. Running tasks are not cancelled when the limit falls. Queued
tasks are FIFO and can be removed with an `AbortSignal`. Overflow rejects with
`AdaptiveConcurrencyError` (`code: 'queue-full'`).

## Limitations

The feedback is intentionally local and simple, not a distributed congestion controller. Windows
are based on completions, do not account for queue wait time, and treat any task rejection as a
congestion signal. Tasks must settle for permits to be released; timeouts and retries belong to
the caller. The gate does not impose a task timeout or fairness across separate instances.

## Support

If this library is useful to you, consider supporting its maintenance with a [donation via PayPal](https://www.paypal.me/LibinPrasanth).

## Author

Created by [Libin Prasanth](https://www.linkedin.com/in/libinprasanth/). Connect on LinkedIn for updates and discussion.
