import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import type { Providers } from "../Providers.ts";
import { getCluster } from "./Api.ts";
import { Credentials } from "./Credentials.ts";

export interface ClusterProps {
  /**
   * The cluster's name.
   * @default the cluster chosen at sign-in, or `RIVET_CLOUD_CLUSTER`
   */
  cluster?: string;
}

export interface Cluster extends Resource<
  "RivetCloud.Cluster",
  ClusterProps,
  {
    /** The cluster's name. */
    name: string;
    /** The cluster's engine API, `https://<slug>-api.onrivet.dev`. */
    endpoint: string;
    /** The engine's admin token, which pools and namespaces are created with. */
    adminToken: Redacted.Redacted<string>;
  },
  never,
  Providers
> {}

/**
 * A Rivet Cloud cluster, read with the Rivet Cloud credentials: its engine
 * endpoint and admin token. A browser sign-in reads the admin token from
 * Rivet Cloud; an API token can't, so CI gives it as
 * `RIVET_CLOUD_ADMIN_TOKEN`. Rivet Cloud creates and deletes clusters in its
 * dashboard; this resource only reads one, and destroying it leaves the
 * cluster as it is. `RivetCloud.ControlPlane` uses it.
 *
 * ### Reading a cluster
 * **Example:** The cluster chosen at sign-in
 * ```typescript
 * const cluster = yield* RivetCloud.Cluster("Rivet", {});
 * ```
 *
 * @resource
 */
export const Cluster = Resource<Cluster>("RivetCloud.Cluster");

/** Neither the Cloud API nor the credentials give the cluster's admin token. */
export class ClusterAdminTokenUnavailable extends Data.TaggedError(
  "RivetCloud.ClusterAdminTokenUnavailable",
)<{
  readonly message: string;
}> {}

export const ClusterProvider = () =>
  Provider.effect(
    Cluster,
    Effect.gen(function* () {
      const credentials = yield* Credentials;
      const observe = Effect.fn(function* (cluster: string | undefined) {
        const resolved = yield* credentials;
        const name = cluster ?? resolved.cluster;
        const info = yield* getCluster(resolved, name);
        // A sign-in session reads the admin token from the Cloud API. An API
        // token can't, so CI gives it with the credentials; it is the admin
        // token of the credentials' own cluster only.
        const adminToken =
          info.admin_token != null
            ? Redacted.make(info.admin_token)
            : name === resolved.cluster
              ? resolved.adminToken
              : undefined;
        if (adminToken === undefined) {
          return yield* new ClusterAdminTokenUnavailable({
            message:
              resolved.adminToken === undefined
                ? `Rivet Cloud gave this credential no admin token for cluster '${name}': an API token can't read it. ` +
                  `Set RIVET_CLOUD_ADMIN_TOKEN (or the profile's admin token) to the admin token in the cluster's Overview → Connection information.`
                : `Rivet Cloud gave this credential no admin token for cluster '${name}', and the admin token in the credentials is cluster '${resolved.cluster}''s. ` +
                  `Deploy to '${resolved.cluster}', or sign in with the browser.`,
          });
        }
        return {
          name: info.cluster,
          endpoint: `https://${info.domain}`,
          adminToken,
        };
      });
      return {
        stables: ["name"],
        read: Effect.fn(function* ({ olds }) {
          return yield* observe(olds?.cluster);
        }),
        reconcile: Effect.fn(function* ({ news }) {
          return yield* observe(news.cluster);
        }),
        // Clusters are created and deleted in the Rivet dashboard.
        delete: () => Effect.void,
      };
    }),
  );
