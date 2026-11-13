/**
 * The lifetime of one in-memory Durable Object instance: its `Scope`, and
 * every fiber it runs.
 *
 * The Durable Object bridge makes one per instance. The instance's
 * constructor runs with its `Scope`, so what the constructor forks into it
 * (`Effect.forkScoped`) or acquires on it (`Effect.addFinalizer`,
 * `Effect.acquireRelease`) lasts as long as the instance. Its construction,
 * its calls and the work they hand to `waitUntil` run as its fibers.
 *
 * An engine that ends an activation but keeps the process (Rivet puts an
 * idle actor to sleep, and the node's process runs on) ends the instance
 * with {@link InstanceLifetimeHandle.close}. workerd never does: it evicts
 * the isolate, which ends the instance's work with it.
 *
 * @internal
 */
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";

/**
 * The running instance's lifetime, provided to its construction, its calls
 * and the work they hand off.
 *
 * @internal
 */
export class InstanceLifetime extends Context.Service<
  InstanceLifetime,
  {
    /**
     * Run `effect` as part of the instance: closing the instance interrupts
     * it, and once the instance is closing it is interrupted before it starts.
     */
    readonly run: <A, E>(effect: Effect.Effect<A, E>) => Promise<Exit.Exit<A, E>>;
  }
>()("alchemy/Workers/InstanceLifetime") {}

export interface InstanceLifetimeHandle {
  /** The service the instance's construction and calls run with. */
  readonly service: InstanceLifetime["Service"];
  /** The instance's own scope, provided to its constructor as `Scope.Scope`. */
  readonly scope: Scope.Scope;
  /** Whether {@link close} has been called. */
  readonly closed: () => boolean;
  /**
   * Keep {@link close} from closing the instance's scope until `promise`
   * settles (a call's scope, which closes after its fiber ends).
   */
  readonly hold: (promise: Promise<unknown>) => void;
  /**
   * End the instance, once: refuse new work, interrupt every fiber it runs
   * (its construction, its calls, the work they handed to `waitUntil`), wait
   * for the calls' scopes to close, then close the instance's scope, running
   * its finalizers in reverse order. Resolves when all of that is done.
   */
  readonly close: () => Promise<void>;
}

export const makeInstanceLifetime = (): InstanceLifetimeHandle => {
  const scope = Scope.makeUnsafe();
  const fibers = new Set<Fiber.Fiber<unknown, unknown>>();
  const held = new Set<Promise<unknown>>();
  let closing: Promise<void> | undefined;

  const run = <A, E>(effect: Effect.Effect<A, E>): Promise<Exit.Exit<A, E>> => {
    if (closing !== undefined) return Promise.resolve(Exit.interrupt());
    const fiber = Effect.runFork(effect);
    fibers.add(fiber);
    return new Promise((resolve) =>
      fiber.addObserver((exit) => {
        fibers.delete(fiber);
        resolve(exit);
      }),
    );
  };

  const hold = (promise: Promise<unknown>) => {
    held.add(promise);
    void promise.then(
      () => held.delete(promise),
      () => held.delete(promise),
    );
  };

  // Settle everything held, including what was held while waiting.
  const settleHeld = async () => {
    while (held.size > 0) await Promise.allSettled(held);
  };

  const close = () =>
    (closing ??= (async () => {
      // Interrupting a call ends its fiber; its scope then closes on its way
      // out, waiting for the work it handed off, which is interrupted too.
      while (fibers.size > 0) await Effect.runPromise(Fiber.interruptAll([...fibers]));
      await settleHeld();
      await Effect.runPromise(Scope.close(scope, Exit.void));
    })());

  return {
    service: { run },
    scope,
    closed: () => closing !== undefined,
    hold,
    close,
  };
};

/**
 * Turn the exit of work handed off in the background into the promise the
 * engine's `waitUntil` keeps: it settles when the work does, and rejects
 * only when the work failed, not when the instance's end interrupted it.
 *
 * @internal
 */
export const backgroundPromise = (exit: Promise<Exit.Exit<unknown, unknown>>) =>
  exit.then((exit) =>
    Exit.isSuccess(exit) || Cause.hasInterruptsOnly(exit.cause)
      ? undefined
      : Promise.reject(Cause.squash(exit.cause)),
  );
