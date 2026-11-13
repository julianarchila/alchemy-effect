import { describe, expect, it } from "@alchemy.run/test";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import { RuntimeContext } from "@/RuntimeContext.ts";
import { Telemetry } from "@/TelemetryRuntime.ts";
import type { DurableObjectExport } from "@/Workers/DurableObject.ts";
import { makeDurableObjectInstance } from "@/Workers/DurableObjectBridge.ts";
import { startInBackground } from "@/Workers/EventLifetime.ts";
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
          let instanceScope: Scope.Scope | undefined;
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
                  instanceScope = yield* Effect.scope;
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
              services: Context.make(RuntimeContext, runtime),
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
                  expect(yield* Effect.scope).not.toBe(instanceScope);
                  return (yield* RuntimeContext).id;
                }),
              ),
            );
            expect(id).toBe("worker:instance");
            yield* drain(pending);
          }
          yield* Effect.promise(() => core.close());
        }),
    );
  }

  describe("the instance's lifetime", () => {
    // An instance whose constructor and calls record what ends, and when.
    const makeCore = (
      constructor: DurableObjectExport["constructor"],
      pending: Promise<unknown>[] = [],
    ) =>
      makeDurableObjectInstance({
        build: () => Promise.resolve(makeBuild(constructor)),
        services: Context.empty(),
        waitUntil: (promise) => {
          pending.push(promise);
        },
        dispatch: "proxy",
      });

    it.effect("gives the constructor a scope of its own that closes when the instance ends", () =>
      Effect.gen(function* () {
        const ended: string[] = [];
        const callScopes: Scope.Scope[] = [];
        let constructorScope: Scope.Scope | undefined;
        const pending: Promise<unknown>[] = [];
        const core = makeCore(
          Effect.succeed(
            Effect.gen(function* () {
              constructorScope = yield* Effect.scope;
              yield* Effect.addFinalizer(() => Effect.sync(() => ended.push("finalizer")));
              yield* Effect.forkScoped(
                Effect.never.pipe(Effect.onInterrupt(() => Effect.sync(() => ended.push("loop")))),
              );
              return {
                read: () =>
                  Effect.gen(function* () {
                    callScopes.push(yield* Effect.scope);
                    return "ok";
                  }),
              };
            }),
          ),
          pending,
        );
        expect(yield* Effect.promise(() => core.dispatch("read")())).toBe("ok");
        yield* drain(pending);
        // The call's scope closed; the instance's didn't.
        expect(callScopes[0]).not.toBe(constructorScope);
        expect(ended).toEqual([]);
        yield* Effect.promise(() => core.close());
        // In reverse: the fiber forked last ends first.
        expect(ended).toEqual(["loop", "finalizer"]);
      }),
    );

    it.effect(
      "interrupts its calls and their background work, and closes their scopes before its own",
      () =>
        Effect.gen(function* () {
          const ended: string[] = [];
          const pending: Promise<unknown>[] = [];
          let started!: () => void;
          const running = new Promise<void>((resolve) => (started = resolve));
          const core = makeCore(
            Effect.succeed(
              Effect.gen(function* () {
                yield* Effect.addFinalizer(() => Effect.sync(() => ended.push("instance")));
                return {
                  hang: () =>
                    Effect.gen(function* () {
                      yield* Effect.addFinalizer(() => Effect.sync(() => ended.push("call scope")));
                      yield* startInBackground(
                        Effect.never.pipe(
                          Effect.onInterrupt(() => Effect.sync(() => ended.push("background"))),
                        ),
                        (promise) => pending.push(promise),
                      );
                      yield* Effect.sync(started);
                      return yield* Effect.never.pipe(
                        Effect.onInterrupt(() => Effect.sync(() => ended.push("call"))),
                      );
                    }),
                };
              }),
            ),
            pending,
          );
          const result = core.execute(
            (shape) => (shape.hang as () => Effect.Effect<never>)(),
            (exit) => Promise.resolve(exit),
          );
          yield* Effect.promise(() => running);
          yield* Effect.promise(() => core.close());
          const exit = yield* Effect.promise(() => result);
          expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
          // The call and its background work end first, in either order;
          // then the call's scope closes, then the instance's.
          expect(ended.slice(0, 2).sort()).toEqual(["background", "call"]);
          expect(ended.slice(2)).toEqual(["call scope", "instance"]);
          // The background work's promise settles without a failure.
          yield* drain(pending);
        }),
    );

    it.effect("refuses calls once ended, and ends once", () =>
      Effect.gen(function* () {
        let finalized = 0;
        let calls = 0;
        const pending: Promise<unknown>[] = [];
        const core = makeCore(
          Effect.succeed(
            Effect.gen(function* () {
              yield* Effect.addFinalizer(() => Effect.sync(() => finalized++));
              return {
                read: () => Effect.sync(() => ++calls),
              };
            }),
          ),
          pending,
        );
        yield* Effect.promise(() => core.instance);
        yield* Effect.promise(() => Promise.all([core.close(), core.close()]));
        yield* Effect.promise(() => core.close());
        const exit = yield* Effect.promise(() =>
          core.execute(
            (shape) => (shape.read as () => Effect.Effect<number>)(),
            (exit) => Promise.resolve(exit),
          ),
        );
        yield* drain(pending);
        expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
        expect(calls).toBe(0);
        expect(finalized).toBe(1);
      }),
    );

    it.effect("interrupts a construction still running", () =>
      Effect.gen(function* () {
        const ended: string[] = [];
        let started!: () => void;
        const running = new Promise<void>((resolve) => (started = resolve));
        const core = makeCore(
          Effect.succeed(
            Effect.gen(function* () {
              yield* Effect.addFinalizer(() => Effect.sync(() => ended.push("instance")));
              yield* Effect.sync(started);
              return yield* Effect.never;
            }),
          ),
        );
        const built = core.instance.then(
          () => "built",
          () => "failed",
        );
        yield* Effect.promise(() => running);
        yield* Effect.promise(() => core.close());
        expect(yield* Effect.promise(() => built)).toBe("failed");
        expect(ended).toEqual(["instance"]);
      }),
    );
  });
});
