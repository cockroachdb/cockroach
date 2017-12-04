- Feature Name: init command
- Status: completed
- Start Date: 2017-03-13
- Authors: @bdarnell
- RFC PR: [#14251](https://github.com/cockroachdb/cockroach/pull/14251)
- Cockroach Issue: [#5974](https://github.com/cockroachdb/cockroach/issues/5974)

# Summary

This RFC proposes a change to the cluster initialization workflow,
introducing a `cockroach init` command which can take the place of the
current logic involving the absence of a `--join` flag. This is
intended to be more compatible with various deployment tools by making
the node configuration more homogeneous.

The new procedure will be:

1. Start all nodes with the same `--join` flag.
2. Run `cockroach init --host=...`, where the `host` parameter is the
   address of one of the nodes in the cluster.

The old procedure of omitting the `--join` flag on one node will still
be permitted, but discouraged for production use.

# Motivation

All CockroachDB clusters require a one-time-only init/bootstrap step.
This is currently performed when a node is started without a `--join`
flag, relying on the admin to start exactly one node in this way. This
is fine for manual test clusters, but it is awkward to automate. One
node must be treated as "special" on its first startup, but it must
revert to normal mode (with a `--join` flag) for later restarts (or
else it could re-initialize a new cluster if it is ever restarted
without its data directory. We have solved
this
[for Kubernetes](https://github.com/cockroachdb/cockroach/blob/43f24c9042657448a0ad635b95099b75e478de41/cloud/kubernetes/cockroachdb-statefulset.yaml#L97) with
a special "init container", but this is relatively subtle logic that
must be redone for each new deployment platform.

Instead, this RFC proposes that the deployment be simplified by using
the "real" `--join` flags everywhere from the beginning, and using an
explicit action by the administrator (or another script) to bootstrap
the cluster.

# Detailed design

We introduce a new command `cockroach init` and a new RPC
`InitCluster`.

## `InitCluster` RPC

The `InitCluster` RPC is a node-level RPC that calls
`server.bootstrapCluster` (unless the cluster is already
bootstrapped). It requires `root` permissions.

## `cockroach init`

The `cockroach init` command is responsible for calling `InitCluster`.
It makes a single attempt and does not retry unless it can be certain
that the previous attempt did not succeed (for example, it could retry
on "connection refused" errors, but not on timeouts). In the event of
an ambiguous error, the admin should examine the cluster to determine
whether the `init` command needs to be retried.

## Complete example

The recommended process for starting a three-node cluster will look
like this (although it would normally be wrapped up in some sort of
orchestration tooling):

```shell
user@node1$ cockroach start --join=node1:26257,node2:26257,node3:26257 --store=/mnt/data

user@node2$ cockroach start --join=node1:26257,node2:26257,node3:26257 --store=/mnt/data

user@node3$ cockroach start --join=node1:26257,node2:26257,node3:26257 --store=/mnt/data

user@anywhere$ cockroach init --host=node1:26257
```

# Drawbacks

## Extra step

This proposal adds an extra step to cluster initialization. However,
this step could be performed at the same time as other common
post-deployment actions (such as creating databases, granting
permissions, etc), which should minimize the overall impact on
operational complexity.

## Node ID divergence

With this proposal, the assignment of node IDs and store IDs becomes
less predictable, so node IDs will be less likely to correspond to
externally-assigned host names, task IDs, etc.

# Alternatives

## Init before start

Originally, CockroachDB required an explicit bootstrapping step using
an `cockroach init` command to be run *before* starting any nodes
(this mirrors PostgreSQL's `initdb` command or MySQL's
`mysql_install_db`). This was removed because it required that the
same directory that `cockroach init` wrote to was used when starting
the real server, which is difficult to guarantee with many deployment
platforms.

## Wait for connected nodes

An earlier draft of this RFC proposed that the `cockroach init`
command take the number of nodes expected in the cluster and not
attempt to bootstrap the cluster until that number of nodes are
present. This information would be used to make the retry logic
slightly more robust, as well as giving an opportunity to present
diagnostic information to the admin when the cluster is not connecting
via gossip. This was considered too much complexity for little
benefit.

## Remove old behavior

The existing logic of automatic bootstrapping when no `--join` flag is
present could be removed, forcing all clusters to use the explicit
`init` command. This would be a conceptual simplification by removing
a redundant (and discouraged) option, but adds additional friction to
simple single-node cases.
