import * as Rivet from "@alchemy.run/rivet";
import * as RivetCloud from "@alchemy.run/rivet/Cloud";
import { describe, expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as RivetApi from "@distilled.cloud/rivet";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";

const { test } = Test.make({ providers: Rivet.providers() });

const Cluster = RivetCloud.ControlPlane("Rivet");

const deploy = Effect.gen(function* () {
  const cluster = yield* Rivet.Cluster;
  const pool = yield* Rivet.Pool("Nodes", {
    endpoint: cluster.endpoint,
    token: cluster.token,
    namespace: cluster.namespace,
  });
  return {
    endpoint: cluster.endpoint as string,
    token: cluster.token as Redacted.Redacted<string>,
    namespace: pool.namespace,
    pool: pool.name,
  };
}).pipe(Effect.provide(Cluster));

/** Read the pool straight from the cluster's engine. */
const observe = (deployed: {
  endpoint: string;
  token: Redacted.Redacted<string>;
  namespace: string;
  pool: string;
}) =>
  RivetApi.pools.getPool({ pool: deployed.pool, namespace: deployed.namespace }).pipe(
    Effect.map((res) => res.pool),
    Effect.catchTag("PoolNotFound", () => Effect.undefined),
    Effect.provide(RivetApi.credentials({ token: deployed.token, endpoint: deployed.endpoint })),
  );

// The CI path: an API token reads the cluster from the Rivet Cloud API but
// gets no admin token, so the admin token comes from
// RIVET_CLOUD_ADMIN_TOKEN. Set RIVET_CLOUD_TOKEN (an API token),
// RIVET_CLOUD_ADMIN_TOKEN and RIVET_CLOUD_CLUSTER to run it. Rivet keeps a
// namespace once created, so the stage's namespace outlives the test.
describe.skipIf(!process.env.RIVET_CLOUD_ADMIN_TOKEN)(
  "RivetCloud.ControlPlane with environment credentials",
  { tags: ["provider:rivet", "provider:rivet:cloud", "live"] },
  () => {
    test.provider(
      "an API token and the admin token deploy a namespace and a pool",
      (stack) =>
        Effect.gen(function* () {
          yield* stack.destroy();

          const deployed = yield* stack.deploy(deploy);
          expect(deployed.endpoint).toMatch(/^https:\/\/.+\.onrivet\.dev$/);
          expect(Redacted.isRedacted(deployed.token)).toBe(true);
          const pool = yield* observe(deployed);
          expect(pool?.name).toBe(deployed.pool);

          yield* stack.destroy();
          expect(yield* observe(deployed)).toBeUndefined();
        }),
      { timeout: 180_000 },
    );
  },
);
