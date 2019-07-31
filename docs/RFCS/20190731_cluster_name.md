- Feature Name: cluster name
- Status: draft
- Start Date: 2019-07-31
- Authors: knz, ben, marc
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: #16784 #15888

# Summary

New feature: a string value, called "cluster name", and checked for
equality when a newly started node joins a cluster.

This will prevent newly added nodes to join the wrong cluster when a
user has multiple clusters running side by side.

This would be configured using another command-line parameter `--join-name`
alongside `--join`.

It will increase operational simplicity and the overall goal to “make
data easy”

Other impact:

- (should have) it would be checked for equality in the node
  certificate properties. This would prevent a node from trusting
  another node's certificate if not in the same cluster, even though
  they may be signed by the same CA.

- (nice to have) the value would be shown in the admin UI, so that two UI
  screens for different clusters side-by-side can be disambiguated at
  a glance.

- (to be further considered) the value would be used as an additional
  label annotation on exported prometheus metrics. This would make it
  possible for Grafana and other monitoring solutions to more easily
  connect to multiple CockroachDB clusters simultaneously.

# Motivation

The motivation for doing this has been detail in
https://github.com/cockroachdb/cockroach/issues/16784 and
https://github.com/cockroachdb/cockroach/issues/15888

- avoid mistaken node joins
- disambiguate UI screens
- disambiguate clusters in prometheus metrics

# Guide-level explanation

The service manager in charge of starting the `cockroach` process
would activate this feature by providing the `--join-name` parameter
on the command line.

When the parameter is supplied, the node will verify that the other
nodes that it connects to (via `--join`) have the same name
configured. If they detect a different name, the join would abort.

If the parameter is not supplied, we get the current behavior - the
join succeeds in any case.

# Reference-level explanation

- The parameter is parsed for `cockroach start` only.

- It is stored in the `server.Config` object.

- When setting up TLS, it is used to validate the node certificate.

- When a newly created node (cluster ID not known yet) connects, it is
  used to verify the identity of peers instead of the cluster
  ID. (note: we already validate the cluster ID beyond that).

- (Optionally) reported in `crdb_internal.gossip_nodes` for troubleshooting.

- (Optionally) reported in `statuspb.NodeStatus` and displayed in UI.

## Detailed design

How: new CLI flag, populates `server.Config`, used in TLS cert check, used in bootstrapping.

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

## Unresolved questions

None at this time.
