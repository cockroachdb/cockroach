- Feature Name: Version Migration
- Status: draft
- Start Date: 2017-07-10
- Authors: Spencer Kimball
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: None found after search

# Summary

This RFC describes a mechanism for upgrading a CockroachDB cluster
between point releases (e.g. from 1.0 to 1.1). In particular, it
introduces the means to enforce version coherence amongst live nodes,
which is necessary to gate access to new features. It further provides
for a transition period during which all nodes settle on a new version
*before* any are able to use new, backwards-incompatible features.
During transition periods, the cluster may either be rolled back to
the prior version, or else the operator may confirm the new version as
the minimum required version, allowing new, backwards-incompatible
features to run.

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
confirms that the new version should become the minimum required, at
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
with the new, backwards-incompatible version.

We expect most point releases to contain features requiring the
migration support described in this RFC.

# Detailed design

Every node knows the version it's running, as the version is baked
into the build. A node's version will be supplied with every RPC
(e.g. Raft, Node, DistSQL, and Gossip). In addition, there will be a
cluster-wide minimum required version, set by the operator. Each node
also sends the minimum required version its operating under with each
RPC.

- RPCs from nodes with versions older than the minimum required
  version will fail with an error that indicates the sender should
  panic.
- If an RPC is received which specifies a minimum required version
  which is newer than the receiver's minimum required version, an
  error is returned that indicates the sender should backoff and retry
  (while the receiver waits for new gossip).
- If an RPC is received which specifies a minimum required version
  which is newer than the receiver's version, the receiver returns
  an error and panics.

The minimum required version will be stored and gossiped as a cluster
setting. The value of the minimum required version is stored in a
`ClusterVersion` protobuf, which to start will include just the
minimum required version number.

```
message ClusterVersion {
    int minimum_required_version = 1 [(gogoproto.nullable) = false];
}
```

Nodes will listen for changes to the gossiped `ClusterVersion` value
and panic with an error in the event that the
`minimum_required_version` is greater than the their version.

To see how this works, consider the following scenario involving a
non-live node that resurfaces with adverse timing. In this scenario,
the command being run is a KV command, which is evaluated and then
replicated to Raft followers.

- An errant node has gone dark and is considered non-live.
- The minimum required version has been upped, but the gossiped
  `ClusterVersion` has not yet been received by all nodes.
- The non-live node suddenly re-surfaces.
- The new feature is evaluated on a node which has received the
  incremented minimum required version.
- It attempts to send to the node that has just re-surfaced, but
  receives an error indicating it should backoff and retry while
  the errant node waits to receive the updated `ClusterVersion`.

Similarly, consider another scenario:

- An errant node has gone dark and is considered non-live.
- **The cluster has been upgraded, but the errant node is unreachable
  and is skipped.**
- The minimum required version has been upped, but the gossiped
  `ClusterVersion` has not yet been received by all nodes.
- The non-live node suddenly re-surfaces.
- The new feature is evaluated on a node which has received the
  incremented minimum required version.
- It attempts to send to the node that has just re-surfaced, but
  receives an error indicating the receiver is out of date and it
  should backoff and retry. In this case, the receiver will commit
  suicide.

## SQL syntax

The operator can upgrade the cluster's version in order to use new
features available at that version via the `SET CLUSTER VERSION
<version>` SQL command (run as the root user). This returns an error
if the specified version is invalid. A valid version must be equal to
the advertised version of every active node in the cluster. On success,
this updates the cluster setting `minimum_required_version`.

The current cluster version can be inspected via a new `SHOW CLUSTER
VERSION` SQL command.

## New features that require a version upgrade

New features which require a particular version are disallowed if that
version is less than the minimum required version set for the
cluster. Each feature is responsible for validating the required
version before being run, and returning an error if not supported.

In some cases, this will be done at the SQL layer; in others, at the
KV layer.

# Drawbacks

This mechanism requires the operator to indicate that the all nodes
have been safely upgraded. It's difficult to formulate an alternative
that would not require operator action but would nevertheless preserve
the option to downgrade.

# Alternatives

TODO

# Unresolved questions

None encountered while writing the RFC. TBD.
