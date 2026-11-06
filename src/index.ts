export interface AdaptiveConcurrencyOptions {
  min?: number;
  max?: number;
  initial?: number;
  maxQueue?: number;
  windowSize?: number;
  latencyTargetMs?: number;
  additiveIncrease?: number;
  multiplicativeDecrease?: number;
  now?: () => number;
}

export interface AdaptiveConcurrencySnapshot {
  limit: number;
  active: number;
  queued: number;
  completedInWindow: number;
}

export interface AdaptiveConcurrency {
  run<T>(task: () => T | PromiseLike<T>, options?: { signal?: AbortSignal }): Promise<T>;
  snapshot(): AdaptiveConcurrencySnapshot;
}

export class AdaptiveConcurrencyError extends Error {
  constructor(
    readonly code: 'queue-full',
    message: string,
  ) {
    super(message);
    this.name = 'AdaptiveConcurrencyError';
  }
}

interface Waiter {
  task: () => unknown;
  resolve: (value: unknown) => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  abort: () => void;
}

function integer(value: number, name: string, minimum: number): number {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new RangeError(`${name} must be a safe integer of at least ${minimum}.`);
  }
  return value;
}

export function createAdaptiveConcurrency(
  options: AdaptiveConcurrencyOptions = {},
): AdaptiveConcurrency {
  const min = integer(options.min ?? 1, 'min', 1);
  const max = integer(options.max ?? 32, 'max', min);
  const initial = integer(options.initial ?? Math.min(4, max), 'initial', min);
  if (initial > max) throw new RangeError('initial cannot exceed max.');
  const maxQueue = integer(options.maxQueue ?? 100, 'maxQueue', 0);
  const windowSize = integer(options.windowSize ?? 10, 'windowSize', 1);
  const latencyTargetMs = options.latencyTargetMs ?? 250;
  if (!Number.isFinite(latencyTargetMs) || latencyTargetMs <= 0) {
    throw new RangeError('latencyTargetMs must be positive and finite.');
  }
  const additiveIncrease = options.additiveIncrease ?? 1;
  const multiplicativeDecrease = options.multiplicativeDecrease ?? 0.5;
  if (!Number.isFinite(additiveIncrease) || additiveIncrease <= 0) {
    throw new RangeError('additiveIncrease must be positive and finite.');
  }
  if (
    !Number.isFinite(multiplicativeDecrease) ||
    multiplicativeDecrease <= 0 ||
    multiplicativeDecrease >= 1
  ) {
    throw new RangeError('multiplicativeDecrease must be greater than 0 and less than 1.');
  }
  const now = options.now ?? Date.now;
  if (typeof now !== 'function') throw new TypeError('now must be a function.');
  let limit = initial;
  let active = 0;
  let completedInWindow = 0;
  let badInWindow = false;
  let slowInWindow = false;
  const queue: Waiter[] = [];

  const observe = (duration: number, failed: boolean) => {
    completedInWindow++;
    badInWindow ||= failed;
    slowInWindow ||= duration > latencyTargetMs;
    if (completedInWindow >= windowSize) {
      limit =
        badInWindow || slowInWindow
          ? Math.max(min, Math.floor(limit * multiplicativeDecrease))
          : Math.min(max, limit + additiveIncrease);
      completedInWindow = 0;
      badInWindow = false;
      slowInWindow = false;
    }
  };
  const pump = () => {
    while (active < limit && queue.length > 0) {
      const waiter = queue.shift()!;
      waiter.signal?.removeEventListener('abort', waiter.abort);
      active++;
      const started = now();
      Promise.resolve()
        .then(waiter.task)
        .then(
          (value) => {
            observe(Math.max(0, now() - started), false);
            active--;
            pump();
            waiter.resolve(value);
          },
          (error: unknown) => {
            observe(Math.max(0, now() - started), true);
            active--;
            pump();
            waiter.reject(error);
          },
        );
    }
  };

  return {
    run<T>(task: () => T | PromiseLike<T>, config: { signal?: AbortSignal } = {}) {
      if (typeof task !== 'function') throw new TypeError('task must be a function.');
      const signal = config.signal;
      if (
        signal !== undefined &&
        (typeof signal.throwIfAborted !== 'function' ||
          typeof signal.addEventListener !== 'function' ||
          typeof signal.removeEventListener !== 'function')
      )
        throw new TypeError('Expected an AbortSignal.');
      signal?.throwIfAborted();
      return new Promise<T>((resolve, reject) => {
        const waiter: Waiter = {
          task,
          resolve: (value) => resolve(value as T),
          reject,
          abort: () => {
            const index = queue.indexOf(waiter);
            if (index < 0) return;
            queue.splice(index, 1);
            reject(signal!.reason);
          },
        };
        if (signal) waiter.signal = signal;
        if (active < limit && queue.length === 0) queue.push(waiter);
        else {
          if (queue.length >= maxQueue) {
            reject(
              new AdaptiveConcurrencyError('queue-full', 'Adaptive concurrency queue is full.'),
            );
            return;
          }
          queue.push(waiter);
        }
        signal?.addEventListener('abort', waiter.abort, { once: true });
        pump();
      });
    },
    snapshot() {
      return { limit, active, queued: queue.length, completedInWindow };
    },
  };
}
