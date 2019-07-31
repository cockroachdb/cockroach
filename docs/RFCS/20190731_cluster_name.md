- Feature Name: cluster name
- Status: in-progress
- Start Date: 2019-07-31
- Authors: knz, ben, marc
- RFC PR: [#39196](https://github.com/cockroachdb/cockroach/pull/39196)
- Cockroach Issue: [#16784](https://github.com/cockroachdb/cockroach/issues/16784) [#15888](https://github.com/cockroachdb/cockroach/issues/15888) [#28408](https://github.com/cockroachdb/cockroach/issues/28408)

# Summary

New feature: a string value, called "cluster name", and checked for
equality when a newly started node joins a cluster.

Prototype implementation:  https://github.com/cockroachdb/cockroach/pull/39270

This will prevent newly added nodes to join the wrong cluster when a
user has multiple clusters running side by side.

This would be configured using another command-line parameter `--cluster-name`
alongside `--join`.

It will increase operational simplicity and the overall goal to “make
data easy”.

Other impact:

- (nice to have) the value would be shown in the admin UI, so that two UI
  screens for different clusters side-by-side can be disambiguated at
  a glance.

- (to be further considered) the value would be used as an additional
  label annotation on exported prometheus metrics. This would make it
  possible for Grafana and other monitoring solutions to more easily
  connect to multiple CockroachDB clusters simultaneously.

Out of scope: it would be good to check the cluster name for equality
in the node certificate properties. This would prevent a node from
trusting another node's certificate if not in the same cluster, even
though they may be signed by the same CA. See https://github.com/cockroachdb/cockroach/issues/28408


# Motivation

The motivation for doing this has been detail in
https://github.com/cockroachdb/cockroach/issues/16784 and
https://github.com/cockroachdb/cockroach/issues/15888

- avoid mistaken node joins
- disambiguate UI screens
- disambiguate clusters in prometheus metrics

# Guide-level explanation

The service manager in charge of starting the `cockroach` process
would activate this feature by providing the `--cluster-name` parameter
on the command line.

When the parameter is supplied, a newly created node will verify that
the other nodes that it connects to (via `--join`) have the same name
configured. If they detect a different name, the join would fail and the
user would be informed.

If the parameter is not supplied, we get the current behavior - the
join succeeds in any case.

There are two scenarios of interest:

- newly-created clusters. These would have `--cluster-name` set on
  every node from the beginning.

- previously-created clusters that don't have `--cluster-name`
  configured yet, and wish to "opt into" the system.
  For these the following process is required:

  1. restart (possibly with a version upgrade to 19.2) every node in a
     rolling fashion, specifyfing both `--cluster-name`
     `--disable-peer-name-verification` on the command line.
  2. perform another rolling restart, removing `--disable-peer-name-verification`
     from every node.

# Reference-level explanation

- The parametes `--cluster-name` and
  `--disable-peer-name-verification` are parsed for `cockroach start`
  only.

- A maximum of 256 characters (for now) is allowed for `--cluster-name`.

- Its lexical format is verified against the following regexp: `^[a-zA-Z](?:[-.a-ZA-Z0-9]*[a-zA-Z0-9]|)$`
  (allows `a.b.c` and `a123` and `a-b` but not `a.`, `123a` or `b-`.
  We choose to exclude "_" because we foresee this may want
  to become a valid hostname eventually).

- It is stored in the `server.Config` object, and propagated to the RPC
  context and the heartbeat service.

- The name is populated by the recipient of a PingRequest into the
  PingResponse object, and checked by the initiator of the heartbeat. We choose
  to check in the initiator so that the error condition can be
  reported clearly to the operator (via `log.Shout`).

- The flag `--disable-peer-name-verification` alters the verification as follows:
  when set, if *either* side has an empty cluster name, the comparison is skipped.
  In the case where both side have a name configured, the comparison is still
  performed even if `--disable-peer-name-verification` is set. This ensures
  that the name is checked to stay consistent across all nodes during
  the rolling opt-in.

- The effect of `--disable-peer-name-verification` is activated if
  either side has it set on the command line. This is necessary
  because during the rolling upgrade where `--cluster-name` is added
  with `--disable-peer-name-verification`, there is no technical
  constraint that the initiator of a heartbeat must always be a node
  that already has `--disable-peer-name-verification` set. Consider
  the following scenario:

  1. nodes upgraded to 19.2 without `--cluster-name` set
  2. n1 restarted with `--cluster-name --disable-peer-name-verification`
  3. n2 sends a heartbeat to n1. Receives a cluster name. Because
     at that point n2 does not have `--disable-peer-name-verification` (yet)
     it will perform the check and that check will fail.

  To fix this, the value of `--disable-peer-name-verification` is sent
  alongside the name in PingResponse, and combined (OR) with the local
  one on the initiator side. If either side has the flag set,
  the check is disabled.

- A new SQL built-in function `crdb_internal.cluster_name()` reports
  the configured value.

- (Optionally) reported in `statuspb.NodeStatus` and displayed in UI.

## Detailed design

How: new CLI flag, populates `server.Config`, used in hearbeat checks.

Optionally picked up by the `Nodes()` status RPC and `crdb_internal.gossip_nodes`.

## Drawbacks

None known at this time.

## Rationale and Alternatives

- Cluster setting *instead of* command line flag.

  Rejected because cluster settings are not yet available when setting
  up a fresh cluster (nodes can join each other but no storage
  available until `init` has been issued).

- *Separate*, additional easy-to-configure "display name" used in
  admin UI.

  Rejected because the need for this has not been expressed strongly
  at this time.

- Separate names for join verification and Prometheus metric labels.

  Under discussion. Proposal is to keep them the same until there
  is a reason to make them separate. When that time comes, we can
  introduce a cluster setting for the Prometheus metrics, whose
  default value comes from the command line flag.

  Another alternative is to do nothing. A current way of setting a
  cluster name label on prometheus metrics is to have a rule to attach
  the label at metric scraping time. See [example](https://github.com/cockroachdb/cockroach/blob/master/monitoring/prometheus.yml#L35).

- Avoiding the `--disable-peer-name-verification` altogether:
  we have not been able to find a protocol that lets an existing
  cluster (without a name) "opt into" a new name.

## Unresolved questions

N/A
