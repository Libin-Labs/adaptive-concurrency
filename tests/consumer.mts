import {
  createAdaptiveConcurrency,
  type AdaptiveConcurrencySnapshot,
} from '@lp-labs/adaptive-concurrency';
const gate = createAdaptiveConcurrency({ min: 1, max: 8, initial: 2 });
const result: Promise<number> = gate.run(() => 4);
const snapshot: AdaptiveConcurrencySnapshot = gate.snapshot();
void [result, snapshot];
