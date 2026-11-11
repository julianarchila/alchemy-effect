/**
 * An instance that forks a fiber from its constructor and ends it with
 * `state.onStop`, so a test can see what's left on the node after RivetKit
 * puts the actor to sleep.
 */
import * as Rivet from "@alchemy.run/rivet";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
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
      // Runs until something interrupts it: only the stop effect does.
      const fiber = yield* Effect.forkDetach(
        Effect.never.pipe(Effect.ensuring(Effect.sync(() => live.delete(activation)))),
      );
      yield* state.onStop(
        Effect.gen(function* () {
          yield* Fiber.interrupt(fiber);
          const stops = (yield* state.storage.get<number>("stops")) ?? 0;
          yield* state.storage.put("stops", stops + 1);
        }),
      );
      return {
        activation: () => Effect.succeed(activation),
        status: () =>
          Effect.gen(function* () {
            return {
              activation,
              running: [...live],
              stops: (yield* state.storage.get<number>("stops")) ?? 0,
            };
          }),
      };
    });
  }).pipe(Effect.provide(Default)),
) {}
