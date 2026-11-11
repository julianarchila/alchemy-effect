/**
 * An instance that forks a fiber from its constructor into its scope, so a
 * test can see what's left on the node after RivetKit puts the actor to
 * sleep, and adds a finalizer that counts how many times it ended.
 */
import * as Rivet from "@alchemy.run/rivet";
import * as Effect from "effect/Effect";
import { Default } from "./cluster.ts";

/**
 * Per object name, the activations whose fiber still runs in this node's
 * process. The pool has one node, so every activation of a name lands here.
 */
const running = new Map<string, Set<string>>();

export default class Sleeper extends Rivet.DurableObject<Sleeper>()(
  "Sleeper",
  { main: import.meta.url },
  Effect.gen(function* () {
    return Effect.gen(function* () {
      const state = yield* Rivet.DurableObjectState;
      const activation = yield* Effect.sync(() => crypto.randomUUID());
      const live = yield* Effect.sync(() => {
        const live = running.get(state.id.name) ?? new Set<string>();
        running.set(state.id.name, live);
        live.add(activation);
        return live;
      });
      // Runs until the instance's scope closes, when the actor sleeps.
      yield* Effect.forkScoped(
        Effect.never.pipe(Effect.ensuring(Effect.sync(() => live.delete(activation)))),
      );
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          const ends = (yield* state.storage.get<number>("ends")) ?? 0;
          yield* state.storage.put("ends", ends + 1);
        }).pipe(Effect.ignore),
      );
      return {
        activation: () => Effect.succeed(activation),
        status: () =>
          Effect.gen(function* () {
            return {
              activation,
              running: [...live],
              ends: (yield* state.storage.get<number>("ends")) ?? 0,
            };
          }),
      };
    });
  }).pipe(Effect.provide(Default)),
) {}
