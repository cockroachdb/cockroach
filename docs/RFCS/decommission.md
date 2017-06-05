- Feature Name: Decommissioning a node
- Status: draft
- Start Date: 2017-06-05
- Authors: Neeral Dodhia (neeral.dodhia@rubrik.com)
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: 6198

# Summary
## Decommission a node
- Mark a node as decommissioned
- Prevent this node trying to join a cluster if it is accidentally restarted after it has been decommissioned.
- Move data off this node if data availability (replication count) is reduced

# Motivation
Clusters operations are fairly common in Rubrik's customers' cluster deployments. These include: taking a node down for maintenance, later bringing it back up, adding a new node and (permanently) removing a node. With Cassandra, there is a lot of operational complexity and difficulty with these operations. For example, if a node is down for longer than the GC period, all its tombstones are deleted; when it rejoins the cluster, it starts re-replicating previously deleted entries. It is also possible for a node to be removed from one cluster and added to another; although its disks should have been wiped clean. there is a small risk that they were not.sl With CockroachDb, these operations should be simple.

*** Detailed design ***
(brain dump, lots of thinking, understanding of how things work now)
There are two different scenarios.
1. A node will temporarily be down.
2. A node will permanently be removed from the cluster.

1. When a node is detected to be unavailable for more than 5 minutes, the node is marked as incommunicado; any leases held by that node are transferred; ranges are up-replicated to other nodes and removed from said node. Ranges are not up-replicated when there does not exist another node to replicate to. Although a node can have multiple stores, there can be at most one replica per node. Deleting the range does not increase the replication factor and will cause more work if that node rejoins the cluster.

Make the 5 minute limit configurable. Setting the value to -1 signifies infinity - i.e. no time limit. The worry here is forgetting to reset the value later. 

2. All leases held by the node must be transferred. All replicas on the node must be up-replicated to another node, if there exists another node to replicate to. The replicas must be deleted from the node.

- Add "hasBeenDecommissioned", a boolean field, to StoreIdent. The StoreIdent is persisted to disk.
- When a node bootstraps, it checks the StoreIdents for each of its stores. If any of the StoreIdents have "hasBeenDecommissioned" set to True, then kill the process with an appropriate error message like 'Attempted to start a decommissioned node. Please wipe stores and retry'.
- Through the gossip network, all nodes are informed that node is being decommissioned via update to StoreDescriptor message.
- Prevent any new ranges being allocated to target node
- Transfer leases away from target node
- Leaseholder of ranges with replicas on target node trigger up-replication.

Testing:
- unit test sets "hasBeenDecommissioned" before bootstrap and then confirms bootstrap fails

# Drawbacks

# Alternatives

# Unresolved questions
- Interface for initiating decommission of a node. One suggestion is adding `node decommission <node-id>` to the CLI.
- Is the decommission operation asynchronous or blocking? Could some parts be blocking?
- Is the 5 minute limit configurable?
- Decommissioning multiple nodes concurrently

