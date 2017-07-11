- Feature Name: Version Migration
- Status: draft
- Start Date: 2017-07-10
- Authors: Spencer Kimball
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue(s): [#15801]((https://github.com/cockroachdb/cockroach/issues/15801) [#15898](https://github.com/cockroachdb/cockroach/issues/15898)

# Summary

This RFC describes a mechanism for upgrading a CockroachDB cluster
between point releases (e.g. from 1.0 to 1.1). In particular, it
introduces the means to enforce version coherence amongst live nodes,
which is necessary to gate access to new features. It further provides
for a transition period during which all nodes settle on a new version
*before* any are able to use new, backwards-incompatible features.
During transition periods, the cluster may either be rolled back to
the prior version, or else the operator may confirm the new version as
the minimum version, allowing new, backwards-incompatible features to
run.

# Motivation

Introducing backwards-incompatible features is currently risky, as
usage could prevent orderly downgrades, or worse, could result in
inconsistencies if used during a rolling upgrade.

One example is the use of different storage for Raft's write ahead log
(#16809). Having nodes begin using this new functionality means they
cannot be downgraded without potential loss of data. To address this,
CockroachDB migration must provide a transition period where all nodes
upgrade to the version which supports the new Raft WAL storage, but do
not yet use it. At this point, the cluster can still be downgraded to
the prior version, allowing the operator to safely test the new
build. When satisfied with the new version's stability, the operator
confirms that the new version should become the minimum version, at
which point the nodes begin using the Raft WAL storage, and the window
for downgrading is closed.

Another example is the `Revert` command. Once executed, it requires
that all nodes performing reads understand how to utilize new
information to ignore reverted data during reads. If one replica out
of three were not upgraded to understand the effects of a `Revert`,
then that node could incorrectly read reverted data as if it were
still live.

Backwards-incompatible changes to *existing* functionality are even
more problematic because you'd expect them to be in active use at the
time of upgrade. In this case, there would be no way to do a rolling
upgrade without having some nodes with the old version and some nodes
with the new, backwards-incompatible version, leading to potential
inconsistencies.

We expect most point releases to contain features requiring the
migration support described in this RFC.

# Detailed design

Every node knows the version it's running, as the version is baked
into the build. Nodes gossip a `NodeDescriptor`, which will be
augmented to include the baked-in server version. Nodes also have a
minimum-supported version baked-in, which is used to sanity check that
a too-new server is not run with a too-old data directory.

In addition, there will be a cluster-wide minimum version, set by the
operator. The minimum version will be stored and gossiped as a cluster
setting. The value of the minimum version is stored in a
`ClusterVersion` protobuf, which to start will include just the
minimum version (this is a protobuf to allow the mechanism to be
expanded as needs evolve).

```
message Version {
    int major = 1 [(gogoproto.nullable) = false];
    int minor = 2 [(gogoproto.nullable) = false];
}

message ClusterVersion {
    Version minimum_version = 1 [(gogoproto.nullable) = false];
}
```

Nodes will listen for changes to the gossiped `ClusterVersion` value
and panic with an error in the event that the `minimum_version` is
greater than the baked-in server version. Each store persists
`ClusterVersion` to disk at a store-local key (similar to gossip
bootstrap info). At startup, each node reads the `ClusterVersion`
values for each store and updates all to match the one containing the
highest `minimum_version`. That value is treated as the operative
`ClusterVersion` at startup until gossip is received. If the node's
baked-in server version is less than the `minimum_version` or the
node's baked-in minimum-supported server version is greater than the
`minimum_version`, the node will exit with an error.

If a gossiped `ClusterVersion` is received where `minimum_version` is
less than a node's `minimum_version`, the node will panic.

## Upgrade process

- At the start, presumably all nodes have the same version (e.g. 1.0).
- To provide a means to restore in the event of disaster recovery, run
  an incremental backup of the database.
- Start a rolling upgrade of nodes to next version (e.g. 1.1).
- At this point, all nodes will be running version 1.1, although
  without upping the cluster-wide minimum version, no
  features requiring 1.1 can be run.
- Verify 1.1 stability without using features requiring 1.1.
- Successful burn-in? **NO**: perform a rolling downgrade to 1.0.
- Otherwise, set the cluster-wide minimum version to 1.1,
  which will allow usage of features requiring 1.1.
- In the event of a catastrophic failure or corruption due to usage of
  new features requiring 1.1, the only option is to restore from
  backup.  This is a two step process: revert nodes to run version 1.0
  and restore from the backup(s).

![Version migrations with rolling upgrades](images/version_migration.png?raw=true "Version migrations with rolling upgrades")

## SQL syntax

The operator can upgrade the cluster's version in order to use new
features available at that version via the `SET CLUSTER
MINIMUM_VERSION <version>` SQL command (run as the root
user). This returns an error if the specified version is invalid. A
valid version must be equal to the advertised version of every active
node in the cluster. On success, this updates the cluster setting
`minimum_version`.

The current cluster version can be inspected via a new `SHOW CLUSTER
MINIMUM_VERSION` SQL command.

## New features that require a version upgrade

New features which require a particular version are disallowed if that
version is less than the minimum version set for the
cluster. Each feature is responsible for validating the minimum
version before being run, and returning an error if not supported.

In some cases, this will be done at the SQL layer; in others, at the
KV layer.

## Rollout

When this mechanism first appears in a release, nodes running the
previous version will not report the version along with RPCs. The
version-checking code will simply assume that the `ClusterVersion` is
initialized to the empty version. In other words, old nodes will
report empty versions, which will match the default empty
`ClusterVersion`.

# Drawbacks

This mechanism requires the operator to indicate that the all nodes
have been safely upgraded. It's difficult to formulate an alternative
that would not require operator action but would nevertheless preserve
the option to downgrade. Although separate features could implement a
rollback path in order to downgrade even after the minimum version is
incremented, this would be a difficult effort for every backwards
incompatible feature that modifies persistent state, and is not viable
as a starting point.

This design provides for a "whole-version" upgrade, which may not be
sufficiently granular for some operators, who would prefer to enable
new functionality feature by feature to isolate potential impact on
performance and stability.

Providing a guarantee of correctness in the face of "rogue" nodes is a
non-goal. Due to gRPC's long-lived connections, and the fact that the
`minimum_version` setting may change at any time, we would
otherwise have to send information on a per-RPC basis to have
iron-clad guarantees. This is further complicated by gossip not being
allowed to perform checks with the same strictness we'd need for Raft,
DistSQL, and KV RPCs because it's the medium by which we transmit the
`minimum_version` cluster setting to all nodes.

This design opts for a straightforward approach that is not foolproof
in light of rogue nodes, defined here as nodes which may re-surface
and send RPCs from an out-of-date server version. The underlying
assumptions guiding this decision are:

- rogue nodes are rare on upgrades
- checks on gossip will happen quickly enough to ameliorate risks

# Alternatives

Have a separate setting/migration that the operator triggers for each
new non-backward compatible feature. This allows us to deprecate old
code independently and operators to control the introduction of new
features somewhat more carefully, at the cost of being more
complicated for less sophisticated users. A drawback of this approach
is that the complexity of support increases. Each of these feature
switches is a knob that controls (in many cases macro) behavior in the
system.

Automatically do upgrades when they're required for a node to boot up
(i.e. once the old code has been removed) if we're sure that all
running nodes are at a new enough version to support them. This might
be too magical to be a good solution for most operators' tastes,
though.

# Unresolved questions

None encountered while writing the RFC. TBD.
