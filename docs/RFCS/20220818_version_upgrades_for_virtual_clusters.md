- Feature Name: Version upgrades for virtual clusters
- Status: accepted
- Start Date: 2022-08-18
- Authors: knz with help from dt and ajw
- RFC PR: [#86377](https://github.com/cockroachdb/cockroach/pull/86377)
- Cockroach Issues: [#66606](https://github.com/cockroachdb/cockroach/issues/66606) [#80992](https://github.com/cockroachdb/cockroach/issues/80992) [CRDB-10829](https://cockroachlabs.atlassian.net/browse/CRDB-10829)
- Jira design epic: [CRDB-14533](https://cockroachlabs.atlassian.net/browse/CRDB-14533)

# Summary

This RFC proposes a mechanism inside CockroachDB to orchestrate the
initialization of system tables, then upgrading the logical cluster
version seen by VCs.

This aims to fix an active correctness bug, for which we so far
use a kludgy workaround in CC Serverless, and which will block
a generalization of cluster virtualization to all deployments.

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Summary](#summary)
- [Background](#background)
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

The remainder of the RFC requires familiarity with the (previously defined)
cluster version and upgrade mechanisms.

These are explained in an accompanying [tech
note](../tech-notes/version_upgrades.md).

In particular, the RFC depends on familiarity with the difference between:

- *storage logical version* (SLV): the cluster version in the system tenant, which is also
  the cluster version used / stored in KV stores.

- *storage binary version* (SBV): the executable version(s) used in KV nodes.

- *VC logical versions* (TLV): the cluster version in each
  VC (there may be several, and they can also be
  separate from the cluster version in the system tenant).

- *VC binary version* (TBV): the executable version used for SQL servers (which can be different
  from that used for KV nodes,
  [#84700](https://github.com/cockroachdb/cockroach/pull/84700)
  notwithstanding).

as well as the required
[invariants](../tech-notes/version_upgrades.md##invariants-in-a-multi-tenancy-world)
between them.

# Motivation

We need to do work in this area because while we *knew* we needed to
maintain invariants (as described above) with cluster virtualization, we actually
failed to implement the code to check these invariants.

In particular:

- it's trivially possible to violate invariant D, because we do not
  implement [the version
  interlock](../tech-notes/version_upgrades.md#enforcing-the-invariants-during-upgrades-in-single-tenancy)
  in VCs.

  So different SQL pods running at different binary versions can
  observe different cluster versions temporarily, and expose different
  feature sets (or even use system tables incorrectly). The TLV can
  even appear to move backward.

  We do not have a tracking issue for this yet.  A subset of the
  problem is covered in this issue
  https://github.com/cockroachdb/cockroach/issues/66606 .

- we are missing a guardrail against violations of invariants E, F and
  G: nothing blocks TLV upgrades beyond the SLV currently.

  This is tracked here: https://github.com/cockroachdb/cockroach/issues/80992

As a result of this lack of guardrails, it's possible to bring a
cluster in an invalid state, where VCs are at a cluster version beyond
the level of support they need from the KV layer.

This can cause serious UX pain and, in extreme case, outright data
corruption.

## Why we need to do this even in a shared-process world

In [this
proposal](https://github.com/cockroachdb/cockroach/pull/84700) we are
thinking about running multiple VCs inside the same process,
possibly shared with the KV layer.

Does this simplify?

Alas, it does not: it only forces the TBV to remain equal to SBV, but
it does not constraint the TLV to remain "behind" the SLV.

Additionally, it's also possible for different nodes (running
different SQL servers for the "app" VC) to run at different TBVs, and
the SQL layer for VCs does not properly persist its TLV, so
it's still possible for the TLV to briefly appear to move backward.

So invariants D, E, F and G can still be violated.

So we still need guardrails with a shared-process deployment.

# Technical design

## SQL server startup guardrails

We are going to block a SQL server from starting if its TBV is too low
for the current TLV.

We will do this with an assertion that verifies the version
immediately after the settings watcher has loaded the initial version
value from the storage cluster.

## Version ratcheting guardrails

We are going to extend [the
interlock](../tech-notes/version_upgrades.md#enforcing-the-invariants-during-upgrades-in-single-tenancy)
previously implemented without cluster virtualization to also work in
VCs:

- at all times, each SQL server will maintain an updated copy of the SLV of the
  storage cluster it's connected to. (This will use the rangefeed which we already
  exploit in `settingswatcher` to import settings into VCs.).

- there will be a "migrate" function for VCs (or, alternatively,
  an implementation of the `Cluster` interface for VCs),
  i.e. an alternative implementation of `(*upgrademanager.Manager).Migrate()`
  populated as `VersionUpgradeHook` in the `ExecutorConfig` for VCs.

  (Note: we already have a go interface, `upgrade.Cluster`. We have an
  implementation for VCs, but it does not do enough)

  - at the beginning of each TLV step, this new function will check that
    the storage cluster (SLV) is already at the same cluster version or newer.
  - it will also check (with a RPC across all the other SQL servers for
    the same VC) whether the TLV upgrade is permissible, that is,
    all other SQL servers are at a proper TBV.
  - it will then run its cluster upgrade function (in the VC keyspace) at the
    current cluster version, and checkpoint as usual.
  - it will then re-check whether the TLV bump is permissible via a RPC
    to every other SQL server (TBV verification).
  - it will then propagate the new TLV to all SQL servers with a RPC.

We will do our best to try to reuse the existing RPC code
(`ValidateTargetClusterVersion`, `BumpClusterVersion`) where
applicable.

## Cleanup: remove startup migrations

As a side-car to the work above (but not strictly required), we can also work to remove the startup migration code.

https://github.com/cockroachdb/cockroach/issues/73813

This will also speed up the initialization of new VCs, and the start-up time of SQL servers.

## Drawbacks

None known.

## Rationale and Alternatives

None known.

"Do nothing" is not an option given the corruption risks.

# Explain it to folk outside of your team

We are extending the cluster version upgrade semantics to be the same
in CC serverless tenants and (in the future) CC dedicated with
cluster virtualization enabled.

# Unresolved questions

N/A
