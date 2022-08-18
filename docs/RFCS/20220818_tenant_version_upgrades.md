- Feature Name: Tenant version upgrades & migrations
- Status: draft
- Start Date: 2022-08-18
- Authors: knz with help from dt and ajw
- RFC PR: [#86377](https://github.com/cockroachdb/cockroach/pull/86377)
- Cockroach Issues: [#66606](https://github.com/cockroachdb/cockroach/issues/66606) [#80992](https://github.com/cockroachdb/cockroach/issues/80992) [CRDB-10829](https://cockroachlabs.atlassian.net/browse/CRDB-10829)
- Jira design epic: [CRDB-14533](https://cockroachlabs.atlassian.net/browse/CRDB-14533)

# Summary

This RFC proposes a mechanism inside CockroachDB to orchestrate the
initialization of system tables, then upgrading the logical cluster
version seen by secondary tenants.

This aims to fix an active correctness bug, for which we so far
use a kludgy workaround in CC Serverless, and which will block
a generalization of multi-tenancy to all deployments.

The RFC also clarifies concepts and terminology, which we expect
will simplify conversations around migrations and increase
the ability of the CRL engineering team to reason and talk about
version upgrades.

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Summary](#summary)
- [Background](#background)
    - [Introduction](#introduction)
    - [Invariant between logical and executable versions](#invariant-between-logical-and-executable-versions)
    - [What gets controlled by logical versions](#what-gets-controlled-by-logical-versions)
    - [Additional invariants](#additional-invariants)
    - [Where the logical version is persisted](#where-the-logical-version-is-persisted)
    - [Enforcing the invariants during upgrades and migrations in single-tenancy](#enforcing-the-invariants-during-upgrades-and-migrations-in-single-tenancy)
    - [Complementary+legacy mechanism: startup migrations](#complementarylegacy-mechanism-startup-migrations)
    - [How these mechanisms have been extended with multi-tenancy](#how-these-mechanisms-have-been-extended-with-multi-tenancy)
        - [Startup migrations](#startup-migrations)
        - [New concept: separate logical versions for tenants and the KV layer](#new-concept-separate-logical-versions-for-tenants-and-the-kv-layer)
        - [Invariants in a multi-tenancy world](#invariants-in-a-multi-tenancy-world)
    - [Further reading: previous RFC work](#further-reading-previous-rfc-work)
- [Motivation](#motivation)
    - [Why we need to do this even in a shared-process world](#why-we-need-to-do-this-even-in-a-shared-process-world)
- [Technical design](#technical-design)
    - [SQL server startup guardrails](#sql-server-startup-guardrails)
    - [Version ratcheting guardrails](#version-ratcheting-guardrails)
    - [Cleanup: remove startup migrations](#cleanup-remove-startup-migrations)
    - [Drawbacks](#drawbacks)
    - [Rationale and Alternatives](#rationale-and-alternatives)
- [Explain it to folk outside of your team](#explain-it-to-folk-outside-of-your-team)
- [Unresolved questions](#unresolved-questions)

<!-- markdown-toc end -->


# Background

The following section explains how versions and upgrades work, for
readers not yet familiar with the mechanisms. It only covers just
enough of the current infrastructure to support the motivation section
that follows and the technical proposal.

We will move this section to a technical note (in the docs/tech-notes
directory) once we're happy with the RFC draft.

**Note: the background section is large, but the proposal/solution is very modest.**

## Introduction

CockroachDB uses a *logical cluster version* to organize the reveal of
new features to users.

The logical version is different from the *executable version* (the
version of the `cockroach` program executable) as a courtesy to our
users: it makes it possible for them to upgrade their executable
version without stopping their cluster all at once, and organize
changes to their SQL client apps separately.

## Invariant between logical and executable versions

The logical version is a number of the form 'X.Y-VVV', where X.Y is
the lowest executable version currently running in the cluster, or the
one before it if all nodes have been upgraded already.
The VVV part increases monotonically with (logical) version upgrades.

For example, a cluster currently running v20.1 and v20.2 executables
can be at logical version `v20.1-3344`. It can be upgraded to
`v20.1-3345` or any logical version after it with the `v20.1` prefix.

Once that cluster finishes upgrading its executable version so all
nodes run at v20.2, it can *still* operate with logical version
`v20.1-3344`, but now it can also be upgraded to `v20.2-1`.

After the logical version has been upgraded to `v20.2-VVV`, it is not
possible any more to downgrade the executable version to v20.1.

## What gets controlled by logical versions

We use logical versions as a control for two separate mechanisms:

- logical versions are paired to *cluster upgrade migrations*: we run
  changes to certain `system` tables and other low-level storage data
  structures as a side effect of moving from one logical version to
  another.

- logical versions are also used as *feature gates*: during SQL
  planning/execution, we check the current logical version and block
  access to certain features as long as given logical version hasn't
  been reached.

The two are related: certain features require `system` tables /
storage to be in a particular state. So we routinely introduce
features with two logical versions: the first version upgrade
"prepares" the system state, while the new feature is still
inaccessible from SQL; then, the second version upgrade enables the
SQL feature.

## Additional invariants

For the above mechanisms to work, we need the following invariants:

- the logical version must evolve monotonically, that is it never goes
  down. In particular, the SQL code must always *observe* it to
  increase monotonically.

  We need this because the `system` table / storage upgrades are not
  reversible, and once certain SQL features are enabled we can't
  cleanly un-enable them (e.g. temp tables).

- all nodes in a cluster either see logical version X or X-1. There's
  never a gap of more than 1 version across the cluster.

  We need this because we also remove features over time. It's
  possible for version X+1 to introduce a replacement feature, then
  version X+2 to remove the old feature. If we allowed versions X and
  X+2 to be visible at the same time, the feature could be both
  enabled and disabled (or both old and new) in different parts of the
  cluster at the same time. We don't want that.

## Where the logical version is persisted

The logical version is not a regular cluster setting. It is persisted
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
  their executable version to their local stores as logical version
  (e.g. node executable 20.1 writes 20.1-0 as logical). We prevent
  creating clusters in mixed-version configurations. After that, the
  logical upgrade process kicks in to bring that version to the most
  recent logical version available.
- After a cluster gets created, when adding new nodes the new nodes
  request their logical version from the remainder of the cluster and
  persist that in their stores directly.
- When restarting a node, during node startup the code loads the
  logical version from the stores directly.
- If at any point, when new nodes are added/restarted with a newer
  executable version than the logical version, and a logical version
  upgrade is possible (and not blocked via the
  `preserve_downgrade_option` setting), the upgrade process kicks in.

## Enforcing the invariants during upgrades and migrations in single-tenancy

In the single-tenant case, the upgrade process (in
`pkg/upgrace/upgrademanager/manager.go`, `Migrate()`) enforces the
invariants as follows:

1. the current logical version X is observed.

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

If the logical version needs to be upgraded across multiple steps (e.g. `v20.1-100` to `v20.1-200`),
the logic in `Migrate` will step through the above 5 steps for every intermediate
version: `v20.1-101`, `v20.1-102`, etc.

It is the interlock between steps 2 and 5 that ensures the invariants are held.

## Complementary+legacy mechanism: startup migrations

The above section explains *cluster upgrade migrations* and how they
are bound to logical versions.

CockroachDB also contains a separate, older and legacy subsystem
called *startup migrations*, which is not constrained by logical
versions. (`pkg/startupmigrations`)

This mechanism is simpler: the code inside each `cockroach` binary
contains a list of *possible* startup migrations.

Whenever a node starts up (regardless of logical version), it checks
whether the startup migrations that it *can* run have already run. If
they haven't, it runs them. The migrations are idempotent so that
if two or more nodes start at the same time, it does not
matter if they are running the same migration concurrently.

Startup migrations pre-date the cluster upgrade migration subsystem
described above, and is unable to respect logical version
boundaries.

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
- copy the values from the old `timeseries.storage.10s_resolution_ttl` and `timeseries.storage.30m_resolution_ttl` settings
  to their new setting names.
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

### New concept: separate logical versions for tenants and the KV layer

In a multi-tenant deployment, it's pretty important that different tenants (customers) can
introduce SQL features "at their own leisure".

So maybe tenant A wants to consume our features at logical version X
and tenant B at logical version Y, with X and Y far from each other.

To make this possible, we introduce the following concept:

*Each tenant has its own logical cluster version, separate from other tenants.*

With this introduction, we now have 4 "version" values of interest:

- the logical version in the system tenant, which is also
  the logical version used / stored in KV stores.

  We will call this the *storage logical version* (SLV) henceforth.

- the executable version(s) used in KV nodes.

  We will call this the *storage binary version* (SBV) henceforth.

- the logical version in each secondary tenant (there may be several,
  and they can also be separate from the logical version in the system
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

     (i.e. KV nodes running at 20.2 must have SLV at 20.1-XX or 20.2-XX)

  B) storage SQL+KV layers must observe max 1 version difference for SLV, and SLV must evolve monotonically.

- on each tenant:

  C) TLV must be permissible for current TBV

     (i.e. SQL servers running at 20.2 must have TLV at 20.1-XX or 20.2-XX)

  D) all SQL servers for 1 tenant must observe max 1 version difference for this tenant's TLV, and TLV must evolve monotonically.

In addition to the natural extensions above, we introduce the following *new* invariant:

E) the storage SLV must be greater or equal to all the tenants TLVs.
   (conversely: we must not allow the TLVs to move past the SLV)

   We need this because we want to simplify our engineering process: we would
   like to always make the assumption that a logical version X can only
   be observed in a tenant after the storage cluster has reached X already (e.g.
   if we need some storage data structure to be prepared to reach logical version X in tenants.)

   Example: our storage cluster is at 20.2-123. This constrains the
   tenants to run at 20.2-123 or earlier.  If we want to move the
   tenants to 20.2-300, we need to upgrade the storage cluster to
   20.2-300 first.

(That last invariant in turn incurs bounds on the SBV and TBV, as per
the invariants above: each possible logical cluster version is only
possible with particular binary version combinations. See above for examples.)

Two interesting properties are *derived* from invariants D and E together:

F) any attempt to upgrade the TLV is blocked (with an upper bound) by
   the current SLV.

   (i.e. if a user tries to upgrade their tenant from 20.1-123 to 20.2-456, but
   the storage cluster is currently at 20.1-123, the upgrade request
   fails with an error.)

G) cluster migrations for tenants can always assume that all
   cluster migrations in the storage cluster up to and including
   their target TLV have completed.

   (i.e. if we can implement tenant migrations at TLV X that require
   support in the storage cluster introduced at SLV X.)

## Further reading: previous RFC work

The following can be useful to understand how and why these mechanisms
were introduced, but are not necessary to understand the rest of the
RFC:

- [2017: Original RFC about version upgrades](20170815_version_migration.md)
- [2018: Automatic upgrade finalization](20180411_finalize_cluster_upgrade_automatically.md)
- [2020: Long running migrations](20200513_long_running_migrations.md)

# Motivation

We need to do work in this area because while we *knew* we needed to
maintain invariants (as described above) in multi-tenancy, we actually
failed to implement the code to check these invariants.

In particular:

- it's trivially possible to violate invariant D, because we do not implement
  [the version interlock described above](#enforcing-the-invariants-during-upgrades-and-migrations-in-single-tenancy) in secondary tenants.

  So different SQL pods running at different binary versions can
  observe different logical versions temporarily, and expose different
  feature sets (or even use system tables incorrectly). The TLV can
  even appear to move backward.

  We do not have a tracking issue for this yet.  A subset of the
  problem is covered in this issue
  https://github.com/cockroachdb/cockroach/issues/66606 .

- we are missing a guardrail against violations of invariants E, F and
  G: nothing blocks TLV upgrades beyond the SLV currently.

  This is tracked here: https://github.com/cockroachdb/cockroach/issues/80992

As a result of this lack of guardrails, it's possible to bring a
multitenant cluster in an invalid state, where tenants are at a
logical version beyond the level of support they need from the KV
layer.

This can cause serious UX pain and, in extreme case, outright data
corruption.

## Why we need to do this even in a shared-process world

In [this
proposal](https://github.com/cockroachdb/cockroach/pull/84700) we are
thinking about running multiple tenants inside the same process,
possibly shared with the KV layer.

Does this simplify?

Alas, it does not: it only forces the TBV to remain equal to SBV, but
it does not constraint the TLV to remain "behind" the SLV.

Additionally, it's also possible for different nodes (running
different SQL servers for the "app" tenant) to run at different TBVs, and
the SQL layer for secondary tenants does not properly persist its TLV, so
it's still possible for the TLV to briefly appear to move backward.

So invariants D, E, F and G can still be violated.

So we still need guardrails with a shared-process deployment.

# Technical design

## SQL server startup guardrails

We are going to block a SQL server from starting if its TBV is too low for the current TLV.

We will do this with a RPC on startup that will check what is the current TLV for the tenant.

## Version ratcheting guardrails

We are going to extend [the
interlock](#enforcing-the-invariants-during-upgrades-and-migrations-in-single-tenancy)
previously implemented for single-tenancy to also work in secondary
tenants:

- there will be a new "migrate" function for secondary tenants.
- at the beginning of each TLV step, it will check (with a RPC) that
  the storage cluster (SLV) is already at the same logical version or newer.
- it will also check (with a RPC across all the other SQL servers for
  the same tenant) whether the TLV upgrade is permissible, that is,
  all other SQL servers are at a proper TBV.
- it will then run its cluster upgrade migration in the tenant keyspace at the
  current logical version, and checkpoint as usual.
- it will then re-check whether the TLV bump is permissible via a RPC
  to the storage cluster (SLV verification) and every other SQL server (TBV verification).
- it will then propagate the new TLV to all SQL servers with a RPC.

We will do our best to try to reuse the existing RPC code
(`ValidateTargetClusterVersion`, `BumpClusterVersion`) where
applicable.

## Cleanup: remove startup migrations

As a side-car to the work above (but not strictly required), we can also work to remove the startup migration code.

https://github.com/cockroachdb/cockroach/issues/73813

This will also speed up the initialization of new tenants, and the start-up time of SQL servers.

## Drawbacks

None known.

## Rationale and Alternatives

None known.

"Do nothing" is not an option given the corruption risks.

# Explain it to folk outside of your team

We are extending the cluster version upgrade semantics to be the same
in CC serverless tenants and (in the future) CC dedicated with
multi-tenancy enabled.

# Unresolved questions

N/A
