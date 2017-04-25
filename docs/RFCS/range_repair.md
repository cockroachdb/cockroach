- Feature Name: Range Repair
- Status: draft
- Start Date: 2015-08-19
- RFC PR:
- Cockroach Issue: #2149

# Summary

Add a new `Repair` service on each node that monitors all other nodes via
gossip and determines if any of them are dead. When a node dies, the service
looks through all the local ranges to see they have any replicas on the dead
node. If any exist, remove the dead replica and add the local replica to the
replication queue. See the updated replication queue RFC #2153 that this RFC
relies on. Note that the `Repair` service is only concerned with remove
replicas from dead nodes, not adding new ones, that is handled by the
replication queue.

# Motivation

Repairing the cluster when nodes disappear from the cluster is a base feature
of cockroach. The goal here is to enable repairing to be quick and effective.

# Detailed design

## Configuration
Add a new configuration setting called `timeoutUntilNodeDead` which contains
the number of seconds after which if a node was not heard from, it is
considered dead. The default value for this will be 5 minutes.

## Repair
Add a new service called `Repair` that starts when the node is started.
This new service will run until the stopper is called and have access to both
gossip and the list of stores (in order to get the list of replicas on those
stores).

Repair will also maintain a map of node IDs to last update time. To maintain
this map, a callback from gossip for node descriptors will be added.

Repair will maintain a timespan `timeUntilNextDead` which is calculated by
taking the nearest last update time for all the nodes and adding
`timeoutUntilNodeDead` and the node ID associated with the timeout.

Repair will trigger on `timeUntilNextDead` which when triggered checks to see
if that node has not been updated.

If the node is dead:

1. mark the node as dead
2. check all the ranges with local replicas to see if any had a replica on this
   now dead node
3. for any ranges that do,
   1. remove replica from the dead node
     - if this fails, add a minute long delay to the `timeUntilNextDead` for
       the node and skip to step 4
   2. add the replica to the replication queue
4. calculate the next `timeUntilNextDead` to wake up the service and add the
   trigger

If the node is not dead:

1. calculate the next `timeUntilNextDead` to wake up the service and add the
   trigger

# Drawbacks

This is a fairly simplistic approach in which we don't care if any replica is
the leader or not. By not waiting for a leader, we may run into issues of
multiple nodes trying to remove the same replica at the same time.

# Alternatives

1. Perform all of this work inside of the replication queue and not add a new
   service.
   - This would just complicate the replication queue.
1. Checking for dead nodes via the replica scanner and only on replicas with
   the leader lease instead of looking at the nodes themselves.
   - Detecting dead nodes would happen slower, but this would avoid the chance
   that more than one replica is trying to repair at the same time. However, if
   the range has no leader, we run into other issues.
2. Add the map of node id to last seen time to gossip instead of the new
   service.
   - This just complicates gossip's internals even more. If it's needed outside
   of the repair service, it can either be shared from the service directly or
   moved into gossip at that time.
4. Don't use the replicate queue at all and add the new replica directly in the
   repair service.
   - This might end up calling for a rebalance at the same time that the
   replicate queue does which might just get a little ugly. The replication
   queue will have knowledge about where the new replica should best be
   situated and it seems like a waste of time to duplicate that logic.

# Unresolved questions

If RFC #2153 isn't implemented, should we consider another option?
