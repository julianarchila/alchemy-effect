/**
 * `state.abort`: construction counts the instance's boots in storage and
 * calls count in memory, so a reset shows as one more boot and the calls
 * starting over. `hang` runs until it's interrupted and records that it
 * was; the alarm aborts its instance the first time it runs.
 */
import * as Rivet from "@alchemy.run/rivet";
import * as Effect from "effect/Effect";
import { Default } from "./cluster.ts";

export default class Resettable extends Rivet.DurableObject<Resettable>()(
  "Resettable",
  { main: import.meta.url },
  Effect.gen(function* () {
    return Effect.gen(function* () {
      const state = yield* Rivet.DurableObjectState;
      const read = (key: string) => Effect.map(state.storage.get<number>(key), (n) => n ?? 0);
      const boots = (yield* read("boots")) + 1;
      yield* state.storage.put("boots", boots);
      let calls = 0;
      return {
        ping: () => Effect.sync(() => ({ boots, calls: ++calls })),
        hang: () =>
          Effect.never.pipe(Effect.onInterrupt(() => state.storage.put("hang-interrupted", boots))),
        abort: () => state.abort("test abort"),
        read: (key: string) => read(key),
        // An alarm that comes due at once and aborts its instance on its
        // first run.
        armAbortingAlarm: (retryAlarm: boolean) =>
          Effect.gen(function* () {
            yield* state.storage.put("retry-alarm", retryAlarm);
            yield* state.storage.setAlarm((yield* Effect.sync(() => Date.now())) + 200);
          }),
        alarmState: () =>
          Effect.gen(function* () {
            return { runs: yield* read("alarm-runs"), alarm: yield* state.storage.getAlarm() };
          }),
        alarm: () =>
          Effect.gen(function* () {
            const runs = (yield* read("alarm-runs")) + 1;
            yield* state.storage.put("alarm-runs", runs);
            if (runs > 1) return;
            const retryAlarm = (yield* state.storage.get<boolean>("retry-alarm")) ?? true;
            yield* state.abort("alarm abort", { retryAlarm });
          }),
      };
    });
  }).pipe(Effect.provide(Default)),
) {}
