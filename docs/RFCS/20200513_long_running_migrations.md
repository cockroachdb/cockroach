- Feature Name: Long Running Migrations
- Status: in-progress
- Start Date: 2020-05-13
- Authors: Irfan Sharif, Tobias Grieger, Dan Harrison
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [#39182](https://github.com/cockroachdb/cockroach/issues/39182)

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
up or even blocked. Consider the following example: TODO.

Work that requires a heavier lift to migrate into is looming on the horizon. We
expect to want to make invasive changes which do not fit into a framework which
simply "flips a bit" and calls it a day. If a migration encompasses serious
amounts of work that can take extended periods of time, this needs to be easy
to do for the engineer, and easy to understand for the end user. A list
(intended to be exhaustive) of concrete examples for both will live at the end
of this document (TODO).

# Background

We currently have two frameworks in place for "migrations":
(1) We have this "sqlmigrations" infrastructure where during either
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
multi-tenancy goggles on,
- "sqlmigrations" can be understood as only concerning data under the
  jurisdiction of individual SQL tenants, and
- "gossip-based cluster migrations" AKA "KV migrations", are a
  much lower-level framework allowing for arbitrarily complex migrations across
  KV nodes (but one that's ultimately harder to use).
(There's some caveat here given that some of the existing sqlmigrations affect
system tables like `system.settings`, that are then used as part of "KV
migrations", but for now it suffices to think of this category of sqlmigrations
to only be allowed to run through the system tenant. The particulars of how
each tenant interacts with its system tables, and which of the current system
tables will no longer be accessible to individual tenants, is out of the scope
of this RFC.)

We would like to improve the primitives provided by this "KV migrations"
framework to make complex migrations easier to write, reason about, and
orchestrate, even if they need to perform a significant amount of work. This
entails providing explicit insight into when KV nodes in the cluster have
transitioned into a new (and out of an old) behaviour. Our existing KV
migration infrastructure has allowed us to phase in new functionality, but has
not been able to phase out old functionality. Consider the following examples:

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
  maintain code like the [following](https://github.com/cockroachdb/cockroach/blob/34da566/pkg/jobs/registry.go#L1074-L1085),
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

(For this RFC, we'll leave "sqlmigrations" as is and instead focus on the
changes to be made to "KV migrations" to accommodate the above, see
[#sqlmigrations](#sqlmigrations) for more details).

# Proposal

Assume `SET CLUSTER SETTING` is only allowable through the system tenant. We're
going to install a callback on the specific `CLUSTER SETTING` planNode, that
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
their stores (using a store local key). To gather this safe list of addressable
nodes (running binaries that can actually process the migrations we're going to
send their way) in the system, we need to add a few primitives that we'll
describe in the [#connect-rpc](#connect-rpc) section. For now we can assume
that no decommissioned nodes running older binaries will be allowed to rejoin
the cluster at an inopportune time, or generally "improper binary versions"
will not exist in the cluster going forward.

Note that for each node, it will first durably persist the target version,
then bump it's local version gate, and only then return to the orchestrator.
We're thus no longer reliant on gossip to propagate version bumps.

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
over from there. To guard against multiple orchestrators running, we'll rely on
`pkg/leasemanager` (though, everything should be idempotent anyway).

```
  @tbg:
  Either way, I think at this point in the doc it's too early for this kind of
  weeds. I would leave the hooks abstract here (other than idempotent and allowed
  to be long-running), and instead work on explaining how the high-level stuff is
  made safe (node joins, etc). In particular the node registration stuff that
  I've mentioned in other comments needs to be fleshed out.
```

---

## Connect RPC

```
  @tbg:
  I kept saying that initially, but I've since realized that the connection to
  multi-tenancy is not very strong. In a sense, you can just forget about SQL
  tenants altogether. The only thing to keep an eye out is that
  https://github.com/cockroachdb/cockroach/issues/47920 remains possible (or we
  need some alternative).
```

We're going to be working on #32574 as part of the move towards multi-tenancy.
Specifically, #32574 is looking to flesh out the protocol for when a node looks
to join a cluster (either an existing one, or a new one that's yet to be
bootstrapped). As part of that work, we're going to look to store this node
registry in a system table (leveraging either an existing table, or creating a
new one altogether). Specifically we're going to track allocated node IDs,
binary versions, and decommissioning state. We'll be able to use this
information, along with current cluster version, to safeguard against nodes
with mismatched versions looking to join the cluster, and prevent
downed-but-not-decommissioned nodes from rejoining the cluster and wreaking
havoc. We may also want to assert on the operator's promise of there being no
older binaries present in the cluster when running `SET CLUSTER VERSION`.

```
  @tbg:
  I am starting to suspect that we ought to use node liveness for this simply
  because that's the mechanism for reasoning about nodes (independently, we
  suspect that we really want node liveness to go to a sql table, but there's
  nothing gained from folding that migration into this work). We've discussed
  elsewhere (sorry, don't have the issue no right now) introducing a
  "decommissioned" field (i.e. make the decommissioning bool an enum), which
  already gives us a way to prevent nodes from coming back when they're not
  allowed to. We can force a sync write to that table as part of the Connect RPC
  to prevent races that would let nodes slip in unannounced to the operator
  (outlined in another comment here).

  What needs to be in the table? I don't think it needs to have the version.
  NodeID, liveness expiration, and decommissioning status should be enough, no?
  This is because we will always query the nodes directly to find out anything
  else we may need. Only if we were somehow forced to write additional info into
  the table would I consider starting an auxiliary table right now.
```

Through the system tables used as part of #32574, the orchestrator process will
have a handle on all nodes of interest in the cluster. The orchestrator will be
able to confirm cluster health, and proceed accordingly (remember: we need all
the nodes in the system to be up and running during migrations). The upgrade
will stall if nodes are down, but not decommissioned (which is rectifiable by
either decommissioning the node, or bringing it back up).

It may be fine for new nodes (assuming the right binary version) to join the
cluster mid-way through a long-running migration, as they have no data to boot
and will be informed by the Connect RPC what version to start with, and would
be able to do so with the appropriate feature flags enabled. But, this is
another moving piece in what is already far too many moving pieces. We will err
on the side of preventing nodes from joining mid-migration given we have the
ability. It also makes writing hooks more difficult as you'd have to consider
the possibility of nodes that are aware of the latest cluster version, but
weren't instructed to run specific migrations (which may matter if they were
to, say, mutate node metadata). It also feels a bit clumsy with respect to
'progress' tracking', where the orchestrator is otherwise aware of every node
in the cluster save for this newly added one.

```
  @tbg:
  Hmm, I imagined that we would allow this and it seems too dangerous to
  prevent nodes from joining the cluster when they're at the right binary.

  But let's ignore that for now, isn't it hard to handle a node that joins
  before the bump but after the round of RPCs? That node would Connect to some
  node and be told that the old cluster version is active (and no upgrade in
  flight). It would join, but the RPC might miss it (because the operator
  goroutine hasn't found it yet).

  I think we need some guarantees here: a node signs up in the registry (let's
  just say node liveness table) before "really" starting up (probably this is
  done by the Connect RPC on behalf of the node joining). Then the operator can
  discover nodes, RPC them all, discover again (if a new node raced in, RPC
  again, and loop around until stable).

  Ok, back to preventing nodes - I don't think we can afford to do this.
  Updates might get stuck (or take long for legit reasons) and if we can't keep
  the cluster available during that time (=spin up new nodes) we have a
  problem. One of the tenets of the mechanism here is that everything is
  idempotent, and this enables new nodes joining "just fine", I think.
```

TODO: Consider the deprecation cycle proposed in the User Defined Schemas
[RFC](https://github.com/cockroachdb/cockroach/pull/48276).

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

---

## sqlmigrations

We will leave "sqlmigrations" mostly intact for the first iteration (to cut
scope, and also because of the ongoing multi-tenancy work, we’re not yet sure
what makes sense to build here). For “tenant-specific SQL migrations”, we can
have SQL pods request KV for the active version during either (a) boot time, or
(b) as a result of a dedicated tenant command issued to the SQL pod. If there
are tenant-specific SQL migrations to run, after the pod finds out about the
version bump, it will attempt to grab a lease through `pkg/leasemanager` and
run the migrations as needed. Code around this kind of migration would have to
be similarly blocked off by `IsActive(sql-pod-version)` checks. An alternative
to this version discovery process would be having each tenant have its own
cluster setting table in the system config, and establishing rangefeeds as the
mechanism for capturing updates.

(Ask @tbg: Most of the "sqlmigrations" today are system table related, and I'm not
sure how they carry over into this brave new world, so I'm not sure how much of
this even applies. There's certainly room for code unification if we considered
"sqlmigrations" as exclusively "system tenant sqlmigrations", but beyond
that, I'm not sure. Most of the "sqlmigrations", seeing as how they effect
system tables, will cause changes across tenants, and so perhaps should only be
addressable by the "host". Is each tenant going to have some form/subset of
their own system tables?)

TODO: Talk briefly about how to reason about sql pod versions and KV pod
versions.

---

## Multi-tenancy

I'm not sure if we have any tenant-specific migrations we're interested in for
now. If we do, we can implement something like above (we'll need to add KV APIs
to expose the cluster version, as we're no longer relying on the gossip backed
cluster setting mechanism for SQL pods). The rest of this doc is mostly focused
on KV-level long running migrations.

---

## Sequencing of work

TODO: Talk about the sequencing of work that would go into this implementing
this RFC. What are the different pieces?
  - Connect RPC
  - AdminMigrate Command
  - Orchestrator Process
  - NodeUpgrade RPC
  - Testing Infrastructure around all of the above.
  - ~~(Stretch) Clean up version usage to start using negative versions?~~

```
  tbg:
  - make 'decommissioning' an enum
  - Connect RPC: read liveness, create if not there (before returning new NodeID)
    --> invariant: every assigned NodeID has a liveness record. Prevent 'removed'
    nodes from successfully Connect()ing. Supply binary version with Connect RPC,
    supply cluster version with response (refuse if incompatible).
  - scaffolding for high-level migrations (i.e. whatever the real thing for
    https://github.com/cockroachdb/cockroach/compare/master...tbg:longrunning is,
    stripped down to bare minimum) and changing our current layout to use that
  - replace current cluster version stuff with a dummy hook that does nothing
    (need no orchestrator yet!)
  - orchestrator

  (next step then: AdminMigrate). I used to view getting there as a must have,
  but I no longer feel so for the release. I want us to focus our energy into
  making the high-level infra really good. If we get AdminMigrate in, great. If
  not, that's okay.

  Testing to be added at every step. The final thing should be unit testable
  without a running server (with hooks that don't try to use KV, etc). For
  example, node liveness is behind a nice abstraction. So is the orchestrator
  lease.
```
