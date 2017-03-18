- Feature Name: init command
- Status: draft
- Start Date: 2017-03-13
- Authors: @bdarnell
- RFC PR: #14251
- Cockroach Issue: #5974

# Summary

This RFC proposes a change to the cluster initialization workflow,
introducing a `cockroach init` command which can take the place of the
current logic involving the absence of a `--join` flag. This is
intended to be more compatible with various deployment tools by making
the node configuration more homogeneous.

The new procedure will be:

1. Start all nodes with the same `--join` flag.
2. Run `cockroach init --host=... N`, where `N` is the number of nodes started.

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

The `cockroach init` command is responsible for calling `InitCluster`
exactly once. It has several safeguards to ensure that if the RPC must
be retried due to a network failure, it will be retried on the same
node if possible, to avoid bootstrapping the cluster twice on two
different nodes.

`cockroach init` takes as arguments the usual client-side flags and
the number of nodes expected in the cluster. It polls gossip via the
status endpoint until that number of nodes are present (this also
gives us an opportunity to provide diagnostic help if expected number
of nodes aren't showing up). Then it picks one deterministically
(lexicographically smallest address?) and calls `InitCluster` on it.
If it fails for any reason other than the cluster already being
bootstrapped, back off and try again.

## Gossip changes

Gossip will be modified so that all connected nodes broadcast their
address, even before the cluster has been initialized and they have
been assigned a node ID. This will allow the `init` command to detect
when all nodes have joined the gossip network.

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

## Don't wait for connected nodes

This proposal could be simplified if the `cockroach init` command
didn't take the number of expected nodes, and simply made a single
attempt to call `InitCluster`. If this failed with a network error,
the admin would be responsible for determining whether the
initialization had in fact taken place and re-running the command if
necessary. The more complex protocol described here is designed to
make the process more robust and therefore more suitable for automated
environments.

# Unresolved questions

- Should we remove/deprecate the auto-bootstrapping when no `--join`
  flag is present so that there is only one way to do it, or keep it
  as a simpler option for some environments?  **Resolved**: the existing
  behavior will remain unchanged if the `--join` flag is omitted.
