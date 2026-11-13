import { describe, expect, it } from "@alchemy.run/test";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import { RuntimeContext } from "@/RuntimeContext.ts";
import { Telemetry } from "@/TelemetryRuntime.ts";
import type { DurableObjectExport } from "@/Workers/DurableObject.ts";
import {
  DurableObjectAborted,
  isAbortedExit,
  makeDurableObjectInstance,
} from "@/Workers/DurableObjectBridge.ts";
import type { WorkerBuild } from "@/Workers/Worker.ts";

class State extends Context.Service<State, { readonly value: string }>()("test/Workers/State") {}

const makeBuild = (
  constructor: DurableObjectExport["constructor"],
): WorkerBuild<DurableObjectExport> => ({
  context: Context.make(Telemetry, Layer.empty).pipe(Context.add(State, { value: "global" })),
  export: {
    kind: "durableObject",
    constructor,
    services: Context.make(State, { value: "captured" }),
  },
  runtimeContext: undefined as any,
  shape: () => ({}),
  telemetry: () => undefined,
});

const drain = (pending: readonly Promise<unknown>[]) =>
  Effect.forEach(pending, (promise) => Effect.promise(() => promise), {
    discard: true,
  });

describe("shared Durable Object instance bridge", { tags: ["unit", "local"] }, () => {
  it.effect("constructs once with provider services and closes distinct call scopes", () =>
    Effect.gen(function* () {
      let builds = 0;
      let constructions = 0;
      const scopes: Scope.Scope[] = [];
      const closed: string[] = [];
      const pending: Promise<unknown>[] = [];
      const built = makeBuild(
        Effect.gen(function* () {
          const state = yield* State;
          constructions++;
          return Effect.succeed({
            read: () =>
              Effect.gen(function* () {
                scopes.push(yield* Effect.scope);
                yield* Effect.addFinalizer(() =>
                  Effect.sync(() => {
                    closed.push(state.value);
                  }),
                );
                expect((yield* State).value).toBe(state.value);
                return state.value;
              }),
          });
        }),
      );
      const core = yield* Effect.sync(() =>
        makeDurableObjectInstance({
          build: () => {
            builds++;
            return Promise.resolve(built);
          },
          services: Context.make(State, { value: "provider" }),
          waitUntil: (promise) => {
            pending.push(promise);
          },
          dispatch: "proxy",
        }),
      );
      expect(yield* Effect.promise(() => core.dispatch("read")())).toBe("provider");
      expect(yield* Effect.promise(() => core.dispatch("read")())).toBe("provider");
      yield* drain(pending);
      expect(builds).toBe(1);
      expect(constructions).toBe(1);
      expect(scopes).toHaveLength(2);
      expect(scopes[0]).not.toBe(scopes[1]);
      expect(closed).toEqual(["provider", "provider"]);
    }),
  );

  it.effect("closes the request scope after a failed handler", () =>
    Effect.gen(function* () {
      const pending: Promise<unknown>[] = [];
      let closed = false;
      const built = makeBuild(Effect.succeed(Effect.succeed({})));
      const core = yield* Effect.sync(() =>
        makeDurableObjectInstance({
          build: () => Promise.resolve(built),
          services: Context.empty(),
          waitUntil: (promise) => {
            pending.push(promise);
          },
          dispatch: "proxy",
        }),
      );
      const result = yield* Effect.promise(() =>
        core.execute(
          () =>
            Effect.addFinalizer(() =>
              Effect.sync(() => {
                closed = true;
              }),
            ).pipe(Effect.andThen(Effect.fail("failed"))),
          (exit) => Promise.resolve(exit),
        ),
      );
      yield* drain(pending);
      expect(Exit.isFailure(result)).toBe(true);
      expect(closed).toBe(true);
    }),
  );

  for (const fail of [false, true]) {
    it.effect(
      `limits callback registration to inner initialization (${fail ? "failure" : "success"})`,
      () =>
        Effect.gen(function* () {
          let open = false;
          let seals = 0;
          const activationScope = yield* Scope.make();
          const pending: Promise<unknown>[] = [];
          const runtime: RuntimeContext["Service"] = {
            Type: "test",
            id: "worker",
            env: {},
            get: () => Effect.succeed(undefined),
            set: (key) => Effect.succeed(key),
          };
          const built = {
            ...makeBuild(
              Effect.gen(function* () {
                expect((yield* RuntimeContext).id).toBe("worker");
                expect(open).toBe(false);
                return Effect.gen(function* () {
                  expect((yield* RuntimeContext).id).toBe("worker:instance");
                  expect(yield* Effect.scope).toBe(activationScope);
                  expect(open).toBe(true);
                  if (fail) return yield* Effect.die("initialization failed");
                  return {};
                });
              }),
            ),
            runtimeContext: runtime,
          };
          const core = yield* Effect.sync(() =>
            makeDurableObjectInstance({
              build: () => Promise.resolve(built),
              services: Context.make(RuntimeContext, runtime).pipe(
                Context.add(Scope.Scope, activationScope),
              ),
              runtimeContext: (context) => ({
                ...context,
                id: `${context.id}:instance`,
              }),
              initialize: () => {
                open = true;
                return () => {
                  open = false;
                  seals++;
                };
              },
              waitUntil: (promise) => {
                pending.push(promise);
              },
              dispatch: "proxy",
            }),
          );
          const exit = yield* Effect.tryPromise(() => core.instance).pipe(Effect.exit);
          expect(Exit.isFailure(exit)).toBe(fail);
          expect(open).toBe(false);
          expect(seals).toBe(1);
          if (!fail) {
            const id = yield* Effect.promise(() =>
              core.execute(() =>
                Effect.gen(function* () {
                  expect(open).toBe(false);
                  expect(yield* Effect.scope).not.toBe(activationScope);
                  return (yield* RuntimeContext).id;
                }),
              ),
            );
            expect(id).toBe("worker:instance");
            yield* drain(pending);
          }
          yield* Scope.close(activationScope, Exit.void);
        }),
    );
  }
  describe("abort", () => {
    const makeCore = (constructor: DurableObjectExport["constructor"]) => {
      const pending: Promise<unknown>[] = [];
      const core = makeDurableObjectInstance({
        build: () => Promise.resolve(makeBuild(constructor)),
        services: Context.empty(),
        waitUntil: (promise) => {
          pending.push(promise);
        },
        dispatch: "proxy",
      });
      return { core, pending };
    };
    const exitOf = (exit: Exit.Exit<any, any>) => Promise.resolve(exit);

    it.effect("interrupts running calls, runs their finalizers and refuses later calls", () =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const finalized: string[] = [];
        let ran = 0;
        const { core, pending } = makeCore(Effect.succeed(Effect.succeed({})));
        const running = core.execute(
          () =>
            Effect.addFinalizer(() => Effect.sync(() => void finalized.push("scope"))).pipe(
              Effect.andThen(Deferred.succeed(started, undefined)),
              Effect.andThen(Effect.never),
              Effect.onInterrupt(() => Effect.sync(() => void finalized.push("interrupted"))),
            ),
          exitOf,
        );
        yield* Deferred.await(started);
        expect(core.aborted()).toBeUndefined();
        core.abort("the Session failed");
        core.abort("ignored");

        const exit = yield* Effect.promise(() => running);
        expect(isAbortedExit(exit)).toBe(true);
        expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toEqual(
          new DurableObjectAborted({ reason: "the Session failed" }),
        );
        expect(core.aborted()?.reason).toBe("the Session failed");

        const later = yield* Effect.promise(() =>
          core.execute(() => Effect.sync(() => ran++), exitOf),
        );
        expect(isAbortedExit(later)).toBe(true);
        expect(ran).toBe(0);
        // Without `onExit`, the call rejects with the defect.
        const rejected = yield* Effect.promise(() =>
          core
            .execute(() => Effect.void)
            .then(
              () => undefined,
              (error: unknown) => error,
            ),
        );
        expect(rejected).toBeInstanceOf(DurableObjectAborted);
        expect((rejected as Error).message).toBe("Durable Object aborted: the Session failed");

        yield* drain(pending);
        expect(finalized).toEqual(["interrupted", "scope"]);
      }),
    );

    it.effect("a call that aborts its own instance is interrupted", () =>
      Effect.gen(function* () {
        let after = false;
        const { core, pending } = makeCore(Effect.succeed(Effect.succeed({})));
        const exit = yield* Effect.promise(() =>
          core.execute(
            () =>
              Effect.sync(() => core.abort("self")).pipe(
                Effect.andThen(Effect.yieldNow),
                Effect.andThen(
                  Effect.sync(() => {
                    after = true;
                  }),
                ),
              ),
            exitOf,
          ),
        );
        yield* drain(pending);
        expect(isAbortedExit(exit)).toBe(true);
        expect(after).toBe(false);
      }),
    );

    it.effect("interrupts construction", () =>
      Effect.gen(function* () {
        const constructing = yield* Deferred.make<void>();
        let interrupted = false;
        const { core, pending } = makeCore(
          Effect.succeed(
            Deferred.succeed(constructing, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.onInterrupt(() =>
                Effect.sync(() => {
                  interrupted = true;
                }),
              ),
            ),
          ),
        );
        const call = core.execute(() => Effect.succeed("ran"), exitOf);
        yield* Deferred.await(constructing);
        core.abort("while constructing");
        const exit = yield* Effect.promise(() => call);
        yield* drain(pending);
        expect(isAbortedExit(exit)).toBe(true);
        expect(interrupted).toBe(true);
      }),
    );
  });
});
