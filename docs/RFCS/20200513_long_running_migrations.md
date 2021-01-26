- Feature Name: Long Running Migrations
- Status: completed
- Start Date: 2020-05-13
- Authors: Irfan Sharif, Tobias Grieger, Dan Harrison
- RFC PR: [#48843](https://github.com/cockroachdb/cockroach/issues/48843)
- Cockroach Issue: [#39182](https://github.com/cockroachdb/cockroach/issues/39182)

___**EDIT**: The work described in this RFC was implemented in parallel with the
RFC going through the review process (see PRs referring back to
[#48843](https://github.com/cockroachdb/cockroach/issues/48843)). As a result,
not all of the comments/discussions from the review process made its way back
to this document, which is being merged "as-is" for documentation purposes.
Look towards
[`pkg/migration`](https://github.com/cockroachdb/cockroach/tree/89781b1cd41e7913a08ba459fe63b237a90a4d6e/pkg/migration)
and the comments there for an up-to-date understanding of what's being
described here. We've annotated the text below with the relevant PRs
implementing them.___

# Motivation

CRDB gets older by the day, with each release bringing in changes that augment,
remove, or replace functionality that are incompatible with the previous
release. The only primitive available today to avoid undesirable behavior while
clusters are running in mixed version modes is the cluster version setting (we
provide a recap of how it works below). It lets us delay activating new
functionality (that only the new binary version understands) until the operator
guarantees to us that there are no nodes in the cluster running the old binary
(and going forward, there never will be), at which point we're free to activate
the features added in the new version.

However, it's usually difficult to determine when legacy code can be deleted.
This is because the primitive only "flips the switch" to allow new
functionality; there's no infrastructure for moving out of the old world. As a
result, legacy code piles up, and overlapping changes to the codebase are held
up or even blocked.

Work that requires a heavier lift to migrate into is looming on the horizon. We
expect to want to make invasive changes which do not fit into a framework which
simply "flips a bit" and calls it a day. If a migration encompasses serious
amounts of work that can take extended periods of time, this needs to be easy
to do for the engineer, and easy to understand for the end user.

# Background

We currently have two frameworks in place for "migrations":
(1) We have the "sqlmigrations" infrastructure where during either
  (a) initial cluster bootstrap, or
  (b) a new binary joining an existing cluster (where all nodes are on the old
      binary),
  a server grabs a specific sqlmigration lease and iterates through
  pre-defined SQL migrations (which typically add/modify system tables). The
  server running these migrations sees through their completion before serving
  requests. Servers running the new binary can therefore always assume the
  migrations have been completed (there's no need for fall back code). These
  "sqlmigrations" can be thought as "start up migrations", and the migrations
  run commands (`CREATE TABLE system.<whatever>`) that can be understood by
  previous version nodes (although, obviously, these system tables would not be
  used by older version binaries).

(2) We use a gossip-based cluster version setting. Once the operator bumps the
  cluster version via `SET CLUSTER VERSION` (we defer to the operator to
  guarantee that all servers are running the new binary, we don't assert it in
  any way), gossip disseminates the new version throughout the cluster. As each
  server receives the gossip update, it knows that it's now free to issue RPCs
  that the new version knows about (remember, we trust the operator to only bump
  the cluster version once all servers are running the new binary, though perhaps
  we can be a bit more defensive going forward). Any given server can receive
  incoming RPCs only addressable by the "new version" code without having seen
  the gossip update (yet). This is fine, as it already has the code to handle it
  appropriately (remember, again, we trusted the operator). These "cluster
  version migrations" are mostly around the safe handling of new RPCs/fields in
  RPCs.
  (To put things more concretely: the "authoritative" cluster setting can
  be found querying the `system.settings` table, running `SHOW CLUSTER SETTING
  version` on each node will only return the particular node's possibly out of
  date view of the authoritative version.)

These two frameworks existed side-by-side for a reason. Putting our
multi-tenancy hats on:

- "sqlmigrations" can be understood as only concerning data under the
  jurisdiction of individual SQL tenants, and
- "gossip-based cluster migrations" AKA "KV migrations", are a
  much lower-level framework allowing for arbitrarily complex migrations across
  KV nodes (but one that's ultimately harder to use).

There's some caveat here given that some of the existing sqlmigrations affect
system tables like `system.settings`, that are then used as part of "KV
migrations", but for now it suffices to think of this category of sqlmigrations
to only be allowed to run through the system tenant. The particulars of how
each tenant interacts with its system tables, and which of the current system
tables will no longer be accessible to individual tenants, is out of the scope
of this RFC.

(For this RFC, we'll leave "sqlmigrations" as is and instead focus on the
changes to be made to "KV migrations" to accommodate the above. This is not
just to cut scope, because of the ongoing multi-tenancy work, we're not sure
what makes sense to build here. See [#multi-tenancy](#multi-tenancy) for more
discussion.)

We would like to improve the primitives provided by this "KV migrations"
framework to make complex migrations easier to write, reason about, and
orchestrate, even if they need to perform a significant amount of work. This
entails providing explicit insight into when KV nodes in the cluster have
transitioned into a new (and out of an old) behaviour. Our existing KV
migration infrastructure has allowed us to phase in new functionality, but has
not been able to phase out old functionality. Consider the following examples:

___**EDIT**: (i) and (ii) below aren't applicable to the long-running migrations
infrastructure. See [#48436](https://github.com/cockroachdb/cockroach/issues/48436),
which is more relevant in our new multi-tenant world. (iii) was implemented in [#58088](https://github.com/cockroachdb/cockroach/pull/58088)___

(i) There's a new FK representation (as of 19.2) and we want to make sure all the
  table descriptors use it in some future version. In 19.2, when introducing
  this new representation, we added an "upgrade on read" code path that would
  be able to convert old table descriptors (the 19.1 representation) to the new
  one. For a cluster running since 19.1, that was upgraded to 19.2, and later
  to 20.1, there was the possibility that certain tables had not been
  read since 19.1 was running. Those tables were thus only identifiable using
  the old (19.1) representation, and as a result, we had to keep maintaining the
  "upgrade on read" code path in 20.1, and will continue having to do so until
  we can guarantee that there are no old table descriptors left lying around.

(ii) Jobs in 2.0 did not support resumability. Currently (as of 20.1), we
  maintain code like the
  [following](https://github.com/cockroachdb/cockroach/blob/34da566/pkg/jobs/registry.go#L1074-L1085),
  to deal with the fact that we may be handling jobs with the old
  representation.

  ```go
      if payload.Lease == nil {
        // If the lease is missing, it simply means the job does not yet support
        // resumability.
        if log.V(2) {
          log.Infof(ctx, "job %d: skipping: nil lease", *id)
        }
        continue
      }


      // If the job has no progress it is from a 2.0 cluster. If the entire cluster
      // has been upgraded to 2.1 then we know nothing is running the job and it
      // can be safely failed.

      //...
  ```

  This looks fragile, and is a maintenance overhead that we'd like to remove.

(iii) The raft truncated state key was made unreplicated in 19.1. To phase in
  this change, we introduced a per-range migration to delete the old,
  replicated key, on demand, which then in turn seeded the creation of the new,
  unreplicated truncated state key. This had the desired outcome that the
  migrated ranges could then begin using the new unreplicated raft truncated
  key. However, it came with the downside that we had to keep the "migrate on
  demand" code path around to handle the possibility of quiesced ranges, that
  had seen no activity through the lifetime of the cluster's 19.1 release
  deployment, coming back to life when running 19.2 or beyond, expecting to
  find the replicated truncated state key. This is getting to be a bit
  unwieldy.

For these examples, and more, we want something to the effect of "for every
range in the system, run this code snippet". In doing so, CRDB developers can
guarantee that following a certain migration, we're only ever going to have
worry about data that looks a certain way.

# Proposal

Assume `SET CLUSTER SETTING` is only allowable through the system tenant. We're
going to install a callback on the specific `CLUSTER SETTING` `planNode`, that
(after validating the change but before it is actually made) kicks off an
"orchestrator" process. This process sets up a job, though not using the
existing jobs infrastructure (it's going to see a bit of change over this
upcoming release, as per @ajwerner). We're instead going to be adding a
dedicated system table, much in the style of `system.jobs`, and work off of
that to provide introspection into progress, ability for embedded users to
block on migrations, process handover in case of orchestrator failure, etc.
(It's not clear what "cancellation" entails, it's likely not applicable to long
running migrations). The `SET CLUSTER SETTING` statement will blocks on the
completion of the migration.

___**EDIT**: The callback kick-starting the migration process was introduced in
[#56368](https://github.com/cockroachdb/cockroach/pull/56368). We're also
walking back on not using `system.jobs`, see
[#58183](https://github.com/cockroachdb/cockroach/issues/58183).___

(As an implementation goal, there shouldn't be anything in pkg/sql other than
the hook described above. The code for all the remaining work should be
packaged outside of it, as appropriate - it just happens to be exposed via a
SQL statement.)

How does the migration for this new system table take place? It'd be a regular
"sqlmigration", that after having run, the long running migration
infrastructure can then depend on. We can interact with this table in the same
way we maintain rangelog information within KV (thus not coupling KV and SQL
any worse that we already are).

After having setup this "long running migration job", the orchestrator will
begin executing the migration across the cluster. The orchestrator process
fetches the current and target cluster version, and determines the set of
migrations to be run to go from one to the other. For each intermediary
version, it will start off by issuing RPCs to each node in the system to inform
them of the next target version, which they'll subsequently persist to each of
their stores (using a store local key).

To gather this list of addressable nodes (running binaries that can actually
process the migrations we're going to send their way), we need to add a few
primitives that we'll describe in the [#connect-rpc](#connect-rpc) section. For
now we can assume that no decommissioned nodes running older binaries will be
allowed to rejoin the cluster at an inopportune time, or generally "improper
binary versions" will not exist in the cluster going forward. By having a
handle on "every node in the system", the orchestrator will be able to confirm
cluster health (remember: we need all the nodes in the system to be up and
running during migrations). The migration will stall if nodes are down, but not
decommissioned (which will be rectifiable by either decommissioning the node,
or bringing it back up).
We also consider the story around what happens for nodes to join the cluster
mid-migration, see
[#node-additions-during-migrations](#node-additions-during-migrations).

Note that for each node, it will first durably persist the target version,
then bump it's local version gate, and only then return to the orchestrator.
We're thus no longer reliant on gossip to propagate version bumps.

___**EDIT**: We stripped out our use of gossip from cluster versions as of
[#56480](https://github.com/cockroachdb/cockroach/pull/56480). Like mentioned
above, look towards pkg/migration for the most up-to-date view of the APIs
proposed here.___

Each "version migration", as defined by the CRDB developer, is a version tag
(proto-backed) with an optional associated hook attached. This hook has to be
idempotent, as it may have to be re-run it in case of failure (of the
orchestrator itself, or of individual nodes). This hook is also allowed to be
"long running", and is guaranteed that when run, all nodes in the system will
have seen the associated version tag (i.e. every `IsActive(version-tag)` check
will always return true, something that was not guaranteed before due to the
reliance on gossip). A rough sketch of it is presented below:

```go
type Migration func(ctx context.Context, h *Helper) error

// ...

  _ := VersionWithHook{
    Tag: roachpb.Version{Major: 20, Minor: 1, Unstable: 7},
    Hook: func (ctx context.Context, h *Helper) error {
      // Harness has already made sure that the version for this hook is active on
      // all nodes in the cluster.

      // Hooks can run SQL.
      _, err := h.Conn().Queryf(`SELECT 1`)
      if err != nil { return err }

      // Hooks can run for a long time.
      time.Sleep(time.Hour)

      // Hooks can use a *kv.DB.
      // Note that the required idempotency holds.
      _, err := h.DB().Put("some-key", "some-value")
      if err != nil { return err }
      return nil
    }
  }
```

After the version gate has been pushed out to all the nodes in the cluster, the
hook is run. Jumping ahead of what primitives are made accessible inside one of
these hooks (through `Helper` in the example above, see
[#hook-primitives](#hook-primitives) for more details), it's only after the
successful completion of a given hook that the orchestrator is free to proceed
with the next version. This version-tag + hook primitive can be used to string
together arbitrarily complex, staged migrations.

After having stepped through all the version tags and hooks as described, the
orchestrator returns back to the `SET CLUSTER SETTING` planNode, now allowing
the statement to complete. If there's a failure of any kind, seeing as how the
orchestrator is careful to externalize its running state to the "jobs"-like
system table outlined above, the operator is free to re-issue the `SET CLUSTER
SETTING` command once the system is back to a healthy state, and the
orchestrator process kicked off will find the partially completed run and take
over from there. To guard against multiple orchestrators running, we'll make
use of `pkg/leasemanager` (though, everything should be idempotent anyway).

## Connect RPC

Lets now consider the "cluster wide view" as seen by the orchestrator process.
The orchestrator needs to have a handle on all nodes that are currently part of
the cluster. It needs this information to be able to push the version gates to
each node in the cluster, and generally keep track of the progress of the
whatever migration is being orchestrated (as described in
[#proposal](#proposal) above). It needs to be able to detect if any nodes
are unavailable, and if so, it needs to error out to the operator (who would
then need to either bring those nodes back up, or decommission them, before
retrying the migration).

For this set of nodes in consideration, the orchestrator needs to exclude
previously decommissioned nodes. This is somewhat of a challenge as things
stand today, and to understand why we take a brief tangent through how CRDB
tracks node status through the decommissioning cycle. Consider the liveness
record proto at the [time of
writing](https://github.com/cockroachdb/cockroach/blob/b119486a6c3e8/pkg/kv/kvserver/storagepb/liveness.proto#L18-L44):

```go
message Liveness {
  int32 node_id = 1 [(gogoproto.customname) = "NodeID",
      (gogoproto.casttype) = "github.com/cockroachdb/cockroach/pkg/roachpb.NodeID"];
  // ...

  // The timestamp at which this liveness record expires.
  // ...
  util.hlc.LegacyTimestamp expiration = 3 [(gogoproto.nullable) = false];

  bool draining = 4;
  bool decommissioning = 5;
}
```

Note how we're only tracking if a given node is currently undergoing
the decommissioning process, not if the node has been "fully decommissioned"
for good. We simply don't represent this state anywhere authoritatively, and
instead rely on some combination of timer thresholds looking at the record
`expiration` and the `decommissioning` boolean to report a fully
"decommissioned" state. See
[this](https://github.com/cockroachdb/cockroach/blob/b119486a6c3/pkg/kv/kvserver/store_pool.go#L114-L153)
and
[this](https://github.com/cockroachdb/cockroach/blob/b119486a6c3/pkg/server/admin.go#L1691-L1699)
for examples of this kind of work around. This awkwardness aside, there's also
nothing preventing a node that was "fully decommissioned" away to then rejoin
the cluster and remain in this perpetual "decommissioning" state. All this
makes for confusing UX for our users, and added fragility around systems
looking for stronger guarantees around the decommissioning lifecycle (long
running migrations for one!). We've run into this ambiguity before in
[#45123](https://github.com/cockroachdb/cockroach/issues/45123) (see the linked
discussions).

___**EDIT**: We introduced a fully decommissioned bit in
[#50329](https://github.com/cockroachdb/cockroach/pull/50329).___


We need re-think a few things here. We want a running CRDB cluster to
_definitively_ say whether or not a certain node has been fully decommissioned.
The liveness record representing the node should instead use an enum to
represent the decommissioning/decommissioned state. We should then in turn use
this record to prevent previously decommissioned nodes from rejoining when
they're not allowed to.

___**EDIT**: We introduced the "join rpc" in
[#52526](https://github.com/cockroachdb/cockroach/pull/52526).___

This is where the "Connect RPC" comes in.
[#32574](https://github.com/cockroachdb/cockroach/issues/32574) looks to flesh
out the protocol for when a node looks to join a cluster (either an existing
one, or a fresh one that's yet to be bootstrapped). Specifically it outlines
the work required for CRDB to be able to allocate NodeIDs by means of a
dedicated "connect" RPC addressed to an already initialized node. This was
initially motivated to improve the node start up sequence that today relies on
the new node to construct a (mostly) functional KV server only to allocate
NodeIDs (by running a KV `Increment` operation).

As part of building out the Connect RPC, we propose to introduce the right
safeguards in place to prevent decommissioned nodes from rejoining a cluster.
This would rely on the decommissioning process to mark out the "fully
decommissioned" state in the liveness record at the end of the decommissioning
process. We can also use the Connect RPC to prevent nodes with mismatched
binary versions from joining the cluster.

___**EDIT**: We introduced machinery to prevent decommissioned nodes from
interacting with the cluster in
[#54936](https://github.com/cockroachdb/cockroach/pull/54936) and
[#55286](https://github.com/cockroachdb/cockroach/pull/55286).___

# Node Additions During Migrations

When a new node joins the cluster, it will discover the active cluster version
(from the already initialized node it connects to as part of the ConnectRPC),
and start off the server process with the right version gates enabled. We'll
consider what happens if there's concurrently a long running migration ongoing.

If the version the new node starts off with is the same version that the
orchestrator is looking to push out to all the nodes (the new node happened to
connect to an existing node that already found out about the version bump),
this is our happy case. The new node will start off with all the relevant
version gates enabled, and persist the same version persisted by all other
nodes. (Still, it's uncomfortable that the orchestrator doesn't know about this
new node.)

___**EDIT**: We strengthened our invariants around always persisting liveness
records in [#53842](https://github.com/cockroachdb/cockroach/pull/53842) and
[#54544](https://github.com/cockroachdb/cockroach/pull/54544).___

If the version the new node starts off with is _not_ the version the
orchestrator is currently pushing out (the new node happened to connect to a
node that has yet to find out about the version bump), we now have the case
where the orchestrator is unaware of a node running at an earlier version.
This situation only came about because the new node was able to join the
cluster right after the orchestrator read the liveness records, but before the
orchestrator was able to send out RPCs to existing nodes. To mitigate this, we
need to ensure that the orchestrator re-reads the liveness records after having
issued the RPCs and check that the list of nodes in consideration hasn't
changed. If it has, it should re-issue the RPCs (now including the new node).
It will continue looping like so until the list of nodes in the cluster is
stable.

After having successfully pushed out the version gate to all the nodes (and
now guaranteeing that new nodes will always join with the version gate
enabled), the rest of the orchestrator is safe to proceed with the rest of
migration hook.

___**EDIT**: We introduced the concept of "fence versions" to reason about safely
stepping through cluster versions while nodes were being added to the cluster.
See [#57186](https://github.com/cockroachdb/cockroach/pull/57186).___

---

## Hook Primitives: AdminMigrate, EveryNode

Lets consider again the examples introduced in [#background](#background). For
each of those migrations, we wanted something to the effect of "for every range
in the system, run this code snippet". Teasing that a bit further, what we
actually want is the ability to issue a command to across the entire keyspace
("every range") in KV, specify a version-tag, and have each range run the
migration corresponding to the specified version. Additionally, for each range
servicing the command, we also want the guarantee that _all_ of its replicas
will have applied the command. In doing so, clients issuing this command are
guaranteed that all relevant data will have been phased out of the old version.

To support this pattern of migrations (which we believe to be the most common
one), the framework will internally make use of a new `AdminMigrate` command.
`AdminMigrate` will be a ranged write command that will include, as part of the
request protobuf, a target version. When evaluating the `AdminMigrate` command,
the leaseholder replica will look up the specific (idempotent) code to
execute corresponding to the specified version, and execute it. The code
servicing these `AdminMigrate` commands is free to grab whatever mutex is
necessary to lock out relevant range processing, as needed.
Before returning to the client, `AdminMigrate` will wait for the command to
have been durably applied on all its followers. This is the mechanism that
would let us fully migrate out _all_ the data in the system, and not have to
worry about there being pre-migration versions of said data lurking about
through a release. This finally lets us remove all fallback code dealing with
pre-migration representations of things from the CRDB codebase.

___**EDIT**: The KV `Migrate` command was introduced in
[#58088](https://github.com/cockroachdb/cockroach/pull/58088).___

Taking the example of (iii), migrating out of the replicated truncated state,
the code corresponding to the `AdminMigrate` command would effectively retain
the same "migrate on demand" structure introduced to phase in the unreplicated
state (except now it's going to be run across the entire keyspace, thus
migrating all the ranges).

Another primitive the long running framework would make use of internally is an
`EveryNode` API. `EveryNode` will be the mechanism through which the
orchestrator will guarantee that every node in the cluster will have run a
specific operation. For now we're only interested in being able to durably
persist the new target version, as outlined in [#proposal](#proposal), and will
plumb through RPCs that let us address individual `Node`s, and run specific
operations (in this case, "persist new target version on each `Store` and bump
local version gates").

Going forward, if we do find ourselves needing more complicated `EveryNode`
operations (like flushing specific `Store` level queues/caches, or running all
replicas on disk that satisfy some criteria through a specific queue), the
infrastructure described here lets us build those pieces in as needed.

___**EDIT**: `EveryNode` was introduced in
[#57650](https://github.com/cockroachdb/cockroach/pull/57650), andwas
eventually changed out to be
[`UntilClusterStable`](https://github.com/cockroachdb/cockroach/blob/75ad154f19b1abd1bc0ecb7dfd58047bd70a873a/pkg/migration/helper.go#L114-L155).
As for some useful primitives available to authors of migrations, that can be
used in conjunction with `UntilClusterStable`, look towards [`service
Migration`](https://github.com/cockroachdb/cockroach/blob/75ad154f19b1abd1bc0ecb7dfd58047bd70a873a/pkg/server/serverpb/migration.proto#L55-L79),
originally introduced in [#56476](https://github.com/cockroachdb/cockroach/pull/56476).___

---

## Multi-tenancy

___**EDIT**: Look towards
[#48436](https://github.com/cockroachdb/cockroach/issues/48436) for the latest
discussion on this matter. We have yet to do any work here.___

With multi-tenancy being a thing going forward, we need to pay attention to how
SQL pod versions relate to KV versions. For the forseeable future, they'll
behave the same way they did in the pre-multi-tenant world. SQL pod versions
running at v20.1 cannot be talking to v20.2 KV nodes once finalized. SQL pods
running at v20.1 are, however, allowed to talk to v20.2 KV binaries, so long
as v20.2 is not finalized.

That aside, how would SQL pods find out about any sort of version bump?  We're
moving away from gossip based distribution of the cluster version within KV, by
replacing it with dedicated RPCs to push out version gates to individual KV
nodes. There's not going to be any such mechanism for SQL pods (by design KV
nodes will not be tracking ephemeral SQL pods). We're also stripping out gossip
dependencies out of SQL pods anyway, so what are we left with? Asked another
way: As operators for a multi-tenant deployment of CRDB, how do we
upgrade SQL pods alongside KV pods?

Lets start with ergonomic parity, we should probably introduce the equivalent
of the version gating safeguards that in KV world we're now geting out of the
Connect RPC. We'll need a way for SQL pods to discover what the active KV
version is during either (a) boot time, or (b) as a result of a dedicated
tenant command issued to the SQL pod. On finding out about a bumped version
(assuming we're the right binary and everything), the SQL pods would then
enable the appropriate version gates (code gated behind
`IsActive(sql-pod-version)`). An alternative version discovery procedure could
be powered by Rangefeeds, by having each tenant have its own cluster setting
table in the system config span, and establishing a rangefeed over it to
capture updates.

We can then make use the upgrade pattern outlined in
[#47919](https://github.com/cockroachdb/cockroach/issues/47919):

```
- Roll KV layer into new binary (Don't finalize)
- Roll tenants into new binary (Use new version)
- Finalize update at KV layer

This basically "pretends" that we start the version bump earlier than we
actually do, and all tenants receive it before any nodes in the KV layer do.
```

There's no new machinery being relied on in this kind of of upgrade procedure,
and there's nothing presented in the rest of the infrastructure here that would
prevent the kind of upgrade procedure described above (we'd have to expose the
KV version APIs and such to SQL pods, but that's a minor lift).

---

## Sequencing of work

Here's the sequencing of work that I think would go into this implementing
this RFC.
  - Make `decommissioning` an enum, and use it appropriately throughout
  - Connect RPC:
      - Read liveness record, create if not there (before returning new NodeID)
        - Invariant: Every assigned NodeID has a liveness record
      - Prevent decommissioned nodes from successfully connecting
      - Supply binary version with Connect RPC, return cluster active version
        in response (refusing to join if incompatible).
  - Scaffolding for the version-tag + hook infrastructure
  - Replace current cluster version stuff with a dummy hook that does nothing
  - NodeUpgrade RPC
  - Orchestrator Process
  - AdminMigrate
  - Testing Infrastructure around all of the above, and at each step.
  - ~~(Stretch) Clean up version usage to start using negative versions?~~

# Unresolved Questions

> "prevent previously decommissioned nodes from rejoining when they're not
> allowed to"
- How exactly is this different from recommissioning a node,
which is an "allowable operation"? Also, recommissioning too is also not
explicitly represented in the liveness proto, does it need to be?

- It's also interesting to consider the case where a node (n3) is partitioned
away, and the operator decides to decommission it through the majority still
accessible (n1 or n2). It's not sufficient to simply record the "fully
decommissioned" state in the liveness record, and only consult it when having a
node joins the cluster. In this example it's possible for the partition to heal
and n3 to continue using its connections to n1 and n2. What we want instead to
happen is for the decommissioning process to cause all connections to n3 to be
dropped. As n3 looks to reconnect, as part of connection handshake that now
consults the decommissioning state, it will be unable to do so (with a clear
reason as to to why). How does the persisting of liveness record and
dropping of connections interact? What goes first, what goes after?
Possible invariant: Once the liveness record is persisted, no future
connections should work.
  - If we write to the liveness record first, then drop connections: there's a
  brief window between the write and the drop that we might receive RPCs from n3,
  before we end up dropping. Is this fine?

  - If we drop the connections first, then write, there's a possibility for the
  connection to get re-established right between the two. Is this possible? Maybe
  something in the handshake can help prevent this?
I'm not sure what "havoc" is wreaked by these inbound RPCs, if any.

- What happens with the liveness table when there are lots and lots of
decommissioned nodes. Do we need a more compact representation of decommissioned nodes?

- When flushing out all the "fully decommissioned" nodes from the cluster, what
are all the caches we need to invalidate to remove all references of the
decommissioned node?
