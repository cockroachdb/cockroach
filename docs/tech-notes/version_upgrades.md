# Cluster versions and upgrades

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Cluster versions and upgrades](#cluster-versions-and-upgrades)
    - [Introduction](#introduction)
    - [Invariant between cluster and executable versions](#invariant-between-cluster-and-executable-versions)
    - [What gets controlled by cluster versions](#what-gets-controlled-by-cluster-versions)
    - [Additional invariants](#additional-invariants)
    - [Where the cluster version is persisted](#where-the-cluster-version-is-persisted)
    - [Enforcing the invariants during upgrades in single-tenancy](#enforcing-the-invariants-during-upgrades-in-single-tenancy)
    - [Complementary+legacy mechanism: startup migrations](#complementarylegacy-mechanism-startup-migrations)
    - [How these mechanisms have been extended with multi-tenancy](#how-these-mechanisms-have-been-extended-with-multi-tenancy)
        - [Startup migrations](#startup-migrations)
        - [Separate cluster versions for tenants and the KV layer](#separate-cluster-versions-for-tenants-and-the-kv-layer)
        - [Invariants in a multi-tenancy world](#invariants-in-a-multi-tenancy-world)
    - [Further reading: RFC work](#further-reading-rfc-work)

<!-- markdown-toc end -->

## Introduction

CockroachDB uses a logical value called *cluster version* to organize
the reveal of new features to users.

The cluster version is different from the *executable version* (the
version of the `cockroach` program executable) as a courtesy to our
users: it makes it possible for them to upgrade their executable
version without stopping their cluster all at once, and organize
changes to their SQL client apps separately.

## Invariant between cluster and executable versions

The cluster version is an opaque number; a labeled point on a line.
The important invariants about these points on this line are:

a) we only ever move in the increasing direction on the line,
b) we never move to point n+1 on the line until everyone agrees we are at n, and
c) we can run code to move from n to n+1.

In practice, the cluster version label looks like "vXX.Y-NNN", but
the specific fields should not be considered too much. They do
not relate directly to the executable version!

Instead, each `cockroach` executable has a range of supported
cluster versions (in the code: `minSupportedVersion` ... `latestVersion`).
If a `cockroach` command observes a cluster version earlier than its
minimum supported version, or later than its maximum supported
version, it terminates.

When we let users upgrade their `cockroach` executables, we're
careful to provide them executables that have overlapping ranges
of supported cluster versions.

For example, a cluster currently running v20.1 supports the range of
cluster versions v100-v300. We can introduce executables at v20.2
which supports cluster versions v200-v400, but only after the
cluster has been upgraded to v200 already; for otherwise the new
executables won't connect.

After all the cluster has been upgraded to the v20.2 executable,
it can be upgraded past v300, which the v20.1 executables did not support.

## What gets controlled by cluster versions

We use cluster versions as a control for two separate mechanisms:

- cluster versions are paired to *cluster upgrades*: we run
  changes to certain `system` tables and other low-level storage data
  structures as a side effect of moving from one cluster version to
  another.

- cluster versions are also used as *feature gates*: during SQL
  planning/execution, we check the current cluster version and block
  access to certain features as long as given cluster version hasn't
  been reached.

The two are related: certain features require `system` tables /
storage to be in a particular state. So we routinely introduce
features with two cluster versions: the first version upgrade
"prepares" the system state, while the new feature is still
inaccessible from SQL; then, the second version upgrade enables the
SQL feature.

## Additional invariants

For the above mechanisms to work, we need the following invariants:

- the cluster version must evolve monotonically, that is it never goes
  down. In particular, the SQL code must always *observe* it to
  increase monotonically.

  We need this because the `system` table / storage upgrades are not
  reversible, and once certain SQL features are enabled we can't
  cleanly un-enable them (e.g. temp tables).

- all nodes in a cluster either see cluster version X or X-1. There's
  never a gap of more than 1 version across the cluster.

  We need this because we also remove features over time. It's
  possible for version X+1 to introduce a replacement feature, then
  version X+2 to remove the old feature. If we allowed versions X and
  X+2 to be visible at the same time, the feature could be both
  enabled and disabled (or both old and new) in different parts of the
  cluster at the same time. We don't want that.

## Where the cluster version is persisted

The cluster version is not a regular cluster setting. It is persisted
in two difference places:

- it is persisted in the `system.settings` table, for the benefit of
  SHOW CLUSTER SETTINGS and other "superficial" SQL observability
  features.

- more importantly, it is stored in a reserved configuration field
  on each store (i.e., per store on each KV node).

The invariants are then maintained during the *upgrade and migration*
process, described below.

The special case of cluster creation (when there is no obvious "previous
version") is also handled via the upgrade process, as follows:

- When a new cluster gets initially created, the initial nodes write
  their minimum supported cluster version to their local stores. We prevent
  creating clusters in mixed-version configurations. After that, the
  version upgrade process kicks in to bring that version to the most
  recent cluster version available.
- After a cluster gets created, when adding new nodes the new nodes
  request their cluster version from the remainder of the cluster and
  persist that in their stores directly.
- When restarting a node, during node startup the code loads the
  cluster version from the stores directly.
- If at any point, when new nodes are added/restarted with a newer
  executable version than the cluster version, and a version
  upgrade is possible (and not blocked via the
  `preserve_downgrade_option` setting), the upgrade process kicks in.

## Enforcing the invariants during upgrades in single-tenancy

In the single-tenant case, the upgrade process (in
`pkg/upgrace/upgrademanager/manager.go`, `Migrate()`) enforces the
invariants as follows:

1. the current cluster version X is observed.

2. an RPC is sent to every node in the cluster, to check if that node is able to
   accept a version upgrade to X+1.
   (`pkg/server/migration.go`, `ValidateTargetClusterVersion()`)

   If any node rejects the migration at this point, the overall upgrade
   aborts.

3. the migration code from X to X+1 is run. It's checkpointed as having run.

4. the validate RPC from step 2 is sent again to every node, to check
   that the migration is still  possible.

   If that fails, the upgrade is aborted, but in such a way that when
   it is re-attempted the migration code from step 3 does not need to
   run any more (it was checkpointed).

5. another RPC is sent to every node in the cluster, to tell them
   to persist the new version X+1 in their stores and reveal the new
   value as the in-memory cluster setting `version`.
   (`pkg/server/migration.go`, `BumpClusterVersion`)

If the cluster version needs to be upgraded across multiple steps (e.g. v100 to v200),
the logic in `Migrate` will step through the above 5 steps for every intermediate
version: v101, v202, v203, etc.

It is the interlock between steps 2 and 5 that ensures the invariants are held.

## Complementary+legacy mechanism: startup migrations

The above section explains *cluster upgrades* and how they
are bound to cluster versions.

CockroachDB also contains a separate, older and legacy subsystem
called *startup migrations*, which is not well constrained by cluster
versions. (`pkg/startupmigrations`)

This mechanism is simpler: the code inside each `cockroach` binary
contains a list of *possible* startup migrations.

Whenever a node starts up (regardless of logical version), it checks
whether the startup migrations that it *can* run have already run. If
they haven't, it runs them. The migrations are idempotent so that if
two or more nodes start at the same time, it does not matter if they
are running the same migration concurrently. Certain migrations are
blocked until the cluster version has evolved past a specific value.

Startup migrations pre-date the cluster upgrade migration subsystem
described above.

We would prefer to use cluster upgrades for all its remaining uses,
but this replacement was not done yet. (see issue
https://github.com/cockroachdb/cockroach/issues/73813 ).

Here are the remaining uses:

- set the setting `diagnostics.reporting.enabled` to true unless the env var `COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING` is set to false.
- write an initial value to the `version` row in `system.settings`.
- add a row for the `root` user in `system.users`.
- add the `admin` role to `system.users` and make `root` a member of it in `system.role_members`.
- auto-generate a random UUID for `cluster.secret`.
- block the node startup if a user/role with name `public` is present in `system.users`.
- create the `defaultdb` and `postgres` empty databases.
- add the default lat/long entries to `system.locations`.
- add the `CREATELOGIN` option to roles that already have the `CREATEROLE` role option.

## How these mechanisms have been extended with multi-tenancy

### Startup migrations

The simpler, legacy startup migration mechanism has a trivial extension to multi-tenancy:

*Every time a SQL server for a secondary tenant starts, it attempts to run whichever
startup migrations it can for its tenant system tables.*

This is checked/done over and over again every time a tenant SQL server starts.

It's also possible for different tenants to "step through" their
startup migrations at different rates, and that's OK.

### Separate cluster versions for tenants and the KV layer

In a multi-tenant deployment, it's pretty important that different tenants (customers) can
introduce SQL features "at their own leisure".

So maybe tenant A wants to consume our features at cluster version X
and tenant B at cluster version Y, with X and Y far from each other.

To make this possible, we introduce the following concept:

*Each tenant has its own cluster version, separate from other tenants.*

With this introduction, we now have 4 "version" values of interest:

- the cluster version in the system tenant, which is also
  the cluster version used / stored in KV stores.

  We will call this the *storage logical version* (SLV) henceforth.

- the executable version(s) used in KV nodes.

  We will call this the *storage binary version* (SBV) henceforth.

- the cluster version in each secondary tenant (there may be several,
  and they can also be separate from the cluster version in the system
  tenant).

  We will call this the (set of) *tenant logical versions* (TLV)
  henceforth.

- the executable version used for SQL servers (which can be different
  from that used for KV nodes,
  [#84700](https://github.com/cockroachdb/cockroach/pull/84700)
  notwithstanding).

  We will call this the *tenant binary version* (TBV) henceforth.

### Invariants in a multi-tenancy world

The original single-tenancy invariants extend as follows:

- on the storage cluster:

  A) SLV must be permissible for current SBV

     (i.e. the SLV must be within supported range for the executable version of the KV nodes)

  B) storage SQL+KV layers must observe max 1 version difference for SLV, and SLV must evolve monotonically.

- on each tenant:

  C) TLV must be permissible for current TBV

     (i.e. the TLV must be within supported min/max cluster version range for the executable running the SQL servers)

  D) all SQL servers for 1 tenant must observe max 1 version difference for this tenant's TLV, and TLV must evolve monotonically.

In addition to the natural extensions above, we introduce the following *new* invariant:

E) the storage SLV must be greater or equal to all the tenants TLVs.
   (conversely: we must not allow the TLVs to move past the SLV)

   We need this because we want to simplify our engineering process: we would
   like to always make the assumption that a cluster version X can only
   be observed in a tenant after the storage cluster has reached X already (e.g.
   if we need some storage data structure to be prepared to reach cluster version X in tenants.)

   Example: our storage cluster is at v123. This constrains the
   tenants to run at v123 or earlier.  If we want to move the
   tenants to v300, we need to upgrade the storage cluster to
   v300 first.

   Another way to look at this: if some piece of SQL code -- running
   in the system tenant or secondary tenant -- observes cluster version X,
   that is sufficient to know that a) all other running SQL code is
   and will remain compatible with X and b) all of the KV APIs are and
   will remain compatible with X.

(That last invariant in turn incurs bounds on the SBV and TBV, as per
the invariants above: each possible cluster version is only
possible with particular binary version combinations. See above for examples.)

Two interesting properties are *derived* from invariants D and E together:

F) any attempt to upgrade the TLV is blocked (with an upper bound) by
   the current SLV.

   (i.e. if a user tries to upgrade their tenant from 20.1-123 to 20.2-456, but
   the storage cluster is currently at 20.1-123, the upgrade request
   fails with an error.)

G) cluster upgrades for tenants can always assume that all
   cluster upgrades in the storage cluster up to and including
   their target TLV have completed.

   (i.e. if we can implement tenant upgrades at TLV X that require
   support in the storage cluster introduced at SLV X.)

## Further reading: RFC work

The following can be useful to understand how and why these mechanisms
were introduced, but are not necessary to understand the rest of the
RFC:

- [2017: Original RFC about version upgrades](../RFCS/20170815_version_migration.md)
- [2018: Automatic upgrade finalization](../RFCS/20180411_finalize_cluster_upgrade_automatically.md)
- [2020: Long running migrations](../RFCS/20200513_long_running_migrations.md)
- [2022: Tenant version upgrades](../RFCS/20220818_tenant_version_upgrades.md)
