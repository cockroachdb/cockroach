- Feature Name: Decommissioning a node
- Status: draft
- Start Date: 2017-06-05
- Authors: Neeral Dodhia (neeral.dodhia@rubrik.com)
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: 6198

# Summary
When a node will be removed from a cluster, mark the node as decommissioned. At
all cost, no data can be lost.
- Drain data from node if data availability (replica count) is reduced.
- Prevent node from rejoining cluster if it is subsequently accidentally
  restarted.

# Motivation
Clusters operations are fairly common in Rubrik's customers' cluster
deployments. These include: taking a node down for maintenance; later bringing
it back up; adding a new node; and, (permanently) removing a node. All of these
should be simple to perform.
With Cassandra, there is a lot of operational complexity and difficulty with
these operations. For example, if a node is down for longer than the GC period,
all its tombstones are deleted; when it rejoins the cluster, it starts
re-replicating previously deleted entries. CockroachDb does not suffer from this
specific issue.

# Detailed design
The following scenarios are considered:
1. A node will (permanently) be removed from the cluster. The node is currently
   1. alive
   1. dead
1. A node will temporarily be down (e.g. for maintenance). 

## Permanent removal
High-level process:
1. Marks that it is being decommissioned.
1. Node does not renew its leases causing all them to be transferred.
1. Change allocator to treat decommissioned nodes like dead nodes with the
   exception that we will down-replicate even if there is no target for
   up-replication.
   1. All replicas for each replica set are up-replicated.
   1. All replicas deleted from the node.
1. If node is restarted, it exits with error on encountering marker.

Lower-level changes:
- Add `Decommissioned`, a boolean field, to `StoreIdent`. The `StoreIdent` is
  persisted to disk.
- When a node bootstraps, it checks the `StoreIdent`s for each of its stores. If
  any of the StoreIdents have `Decommissioned` set to True, then kill the
  process with an appropriate error message like "attempted to start a
  decommissioned node".
- Through the gossip network, all nodes are informed that node is being removed
  via updated `StoreDescriptor` message.
- Prevent any new ranges being allocated to target node.
- Transfer leases away from target node.
- Leaseholder of ranges with replicas on target node trigger up-replication.

### Difference in process when target is alive vs dead
If the node being removed is dead, and so, unreachable, its leases and data
would already have been transferred to other nodes. The only thing to do is
preventing it from rejoining the cluster if it were to become available. This
would require blacklisting that node and would be fairly complex: this will not
be attempted.

## Temporary removal
No changes required. The existing CockroachDb process, described below, is
sufficient.
After a node is detected as unavailable for more than
`COCKROACH_TIME_UNTIL_STORE_DEAD` (an env variable with default: 5 minutes), the
node is marked as incommunicado and the cluster decides that the node may not be
coming back and moves the data elsewhere. Its leases would have been transferred
to other nodes as soon as they weren't renewed. Ranges are up-replicated to
other nodes and down-replicated (removed) from target node. Ranges are not
down-replicated when there does not exist a target for up-replication because
this causes more work if that node rejoins the cluster; however, when permanently
removing a node, we still want to do that. Although a node can have multiple
stores, there can be at most one replica for each replica set (e.g. range) per
node.

# Drawbacks

# Alternatives
During a temporary removal, `COCKROACH_TIME_UNTIL_STORE_DEAD` could be updated
to to the length of the downtime to avoid unecessary movement of ranges.
However, it is difficult to predict the length of downtime. This is an
optimisation which can be implemented later.

# Unresolved questions
- Interface for initiating removal of a node. One suggestion is adding
  `node decommission <node-id>` to the CLI. The operation should be asynchronous
  (vs blocking). If it is blocking and it takes a long time to complete,
  connection might timeout if no response was sent to the client.
- Removing multiple nodes concurrently
