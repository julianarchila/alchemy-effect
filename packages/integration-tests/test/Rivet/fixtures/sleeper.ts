/**
 * An instance that leaves work running in the node's process, so a test can
 * see what's left after RivetKit puts the actor to sleep: a fiber its
 * constructor forks into the instance's scope, and work a call hands to
 * `waitUntil` (which doesn't keep the actor awake). A finalizer on the
 * instance's scope counts in storage how many times it ended.
 */
import * as Rivet from "@alchemy.run/rivet";
import * as Effect from "effect/Effect";
import { Default } from "./cluster.ts";

/**
 * Per object name, the activations whose work still runs in this node's
 * process. The pool has one node, so every activation of a name lands here.
 */
const loops = new Map<string, Set<string>>();
const held = new Map<string, Set<string>>();

const entry = (map: Map<string, Set<string>>, name: string) => {
  const set = map.get(name) ?? new Set<string>();
  map.set(name, set);
  return set;
};

export default class Sleeper extends Rivet.DurableObject<Sleeper>()(
  "Sleeper",
  { main: import.meta.url },
  Effect.gen(function* () {
    return Effect.gen(function* () {
      const state = yield* Rivet.DurableObjectState;
      const name = state.id.name;
      const activation = yield* Effect.sync(() => crypto.randomUUID());
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          const ends = (yield* state.storage.get<number>("ends")) ?? 0;
          yield* state.storage.put("ends", ends + 1);
        }).pipe(Effect.ignore),
      );
      // Runs until the instance ends.
      yield* Effect.sync(() => entry(loops, name).add(activation));
      yield* Effect.forkScoped(
        Effect.never.pipe(
          Effect.ensuring(Effect.sync(() => entry(loops, name).delete(activation))),
        ),
      );
      return {
        activation: () => Effect.succeed(activation),
        // Background work that never finishes on its own.
        hold: () =>
          Effect.gen(function* () {
            yield* Effect.sync(() => entry(held, name).add(activation));
            yield* state.waitUntil(
              Effect.never.pipe(
                Effect.ensuring(Effect.sync(() => entry(held, name).delete(activation))),
              ),
            );
            return activation;
          }),
        status: () =>
          Effect.gen(function* () {
            return {
              activation,
              loops: [...entry(loops, name)],
              held: [...entry(held, name)],
              ends: (yield* state.storage.get<number>("ends")) ?? 0,
            };
          }),
      };
    });
  }).pipe(Effect.provide(Default)),
) {}
