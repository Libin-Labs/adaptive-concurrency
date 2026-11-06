import { createAdaptiveConcurrency, AdaptiveConcurrencyError } from '@lp-labs/adaptive-concurrency';
const gate = createAdaptiveConcurrency({ maxQueue: 0 });
const result: Promise<string> = gate.run(() => 'ok');
const error = new AdaptiveConcurrencyError('queue-full', 'full');
void [result, error];
