- Feature Name: Decommissioning a node
- Status: completed
- Start Date: 2017-06-05
- Authors: Neeral Dodhia
- RFC PR: [#16447]
- Cockroach Issue: [#6198]

# Summary

When a node will be removed from a cluster, mark the node as decommissioned. At
all cost, no data can be lost. To this end:

- Drain data from node if data availability (replica count) is reduced.
- Prevent node from participating in cluster.

# Motivation

Cluster operations are common in production deployments, such as: taking a node
down for maintenance; later bringing it back up; adding a new node; and,
(permanently) removing a node. All of these should be simple and **safe** to
perform.

Currently, the way to remove a node from a cluster is to shut it down. There is
a period before the cluster rebalances its replicas. If another node were to go
down, for example due to power loss, then some replica sets would become
unavailable. Those replica sets which had a replica on the shut-down node and
the failed node now only have a single replica. This demonstrates why safe
removal—without risk of data loss—is required.

A typical operation in the field is to replace a set of nodes. This involves
removing the old nodes and adding their replacements. Decommissioning a node
at-a-time is inefficient. Decommissioning the first node causes data to be moved
onto nodes that are about to be decommissioned too. Then, decommissioning these
nodes leads to more data movement. Instead, it is more efficient to mark all
these nodes as being decommissioned at once. Then, the draining mechanism can
move data to nodes that will remain. The steps to replace multiple nodes are to
add the new nodes and then decommission the old ones.

# Detailed design
The following scenarios are considered:
1. A node will (permanently) be removed from the cluster. The node is currently
   1. alive, or
   1. dead.
1. A node will temporarily be down (e.g. for maintenance).

## Permanent removal

1. On any node, the user executes
   ```shell
   cockroach node decommission <nodeID>...
   ```
   The node that receives the CLI command is referred to as node A. The nodes
   specified in the CLI command are referred to as target nodes. This command is
   asynchronous and returns with list of node IDs, their status, replica count,
   and their `Draining` and `Decommissioning` flags.
1. Node A sets the `Decommissioning` flag to `true` for all target nodes in the
   node liveness table.
1. After approximately 10 seconds, each target node discovers that the
   `Decommissioning` flag has been set. The mechanism for discovery is the
   heartbeat process, which periodically updates its own entry in the node
   liveness table.
1. Each target node:
   1. sets the `Draining` flag in the node liveness table to `true`, and
   1. waits until draining has completed.
1. At this point, every target node
   1. is not holding any leases,
   1. is not accepting any new SQL connections, and
   1. is not accepting new replicas.
1. Leaseholders, necessarily non-target nodes, for ranges where a target node
   is a member of the replica set will have their replicate queue treat
   decommissioning nodes like dead replicas. This will do the right thing:
   not down-replicate if that puts us in more danger than we already are, and
   not up-replicate to more dangerous states.
   1. up-replicate to a node not in decommissioning state
      - If there are not any nodes available for up-replication, the process
        stalls. This prevents data loss and ensures availability of all ranges.
   1. down-replicate from target nodes.
1. Wait for the replica count on each target node to reach 0. To be able to do
   this for dead nodes, use meta ranges.
1. The user idempotently executes the command from step 1. To measure
   progress, track the replica count.
1. The user executes
   ```shell
   cockroach quit --decommission --host=<hostname> --port=<port>
   ```
   for each target node. This explicitly shuts down the node. Otherwise, the
   node remains up but is unable to participate in usual operations such as
   having replicas. The `--decommission` flag causes it to wait until the
   replica count on the specified node reaches 0 before initiating shutdown.
   - This command also initiates decommission. Setting decommission is
     idempotent, which is nice here. The difference with this command is that it
     blocks until the node shuts down. Users who do not want asynchronous
     external polling can choose this command, if they want to decommission a
     single node.

## Handling restarts
When a node is restarted, it may reset its `Draining` flag but its
`Decommissioned` flag remains. The above process resumes from the third step.
Hence, if a node is restarted at any point after the `Decommissioned` flag is
set, the decommissioning process will resume.

A decommissioned node can be restarted and would rejoin the cluster. However,
it would not participate in any activity. This is safe and, hence, there is no
need to prevent a decommissioned node from restarting.

When a node restarts, there is a small period when it can accept new replicas
before reading the node liveness table. This would be rare and short-lived: nodes
will not send new replicas to a decommissioned node, and so this requires both
nodes to be unaware of the decommissioning state. A decommissioned node could have
reached a replica count of 0, then be restarted and accept new replicas. In this
case, availability could be compromised if the node were shutdown immediately.
However, the `--decommission` flag to the `quit` command ensures that shutdown
is only initiated if the replica count is 0.

## Undo
```shell
cockroach node recommission <nodeID>...
```
sets the `Decommissioning` flag to `false` for target nodes. Then, the user must
restart each target node. When a node restarts, it resets its `Draining` flag.
This will allow the node to participate like normal. Node restart is required
because we cannot determine whether a node is in `Draining` because if was
previously in `Decommissioning` or for another reason.

## Dead nodes
If a target node is dead (i.e. unreachable), it can be in one of the following
states:

1. It holds unexpired leases.
1. It holds no leases; the replicas of ranges it has on-disk have not been
   rebalanced to other nodes.
1. It holds no leases; the replicas of ranges it has on-disk have been down-
   replicated from it and up-replicated to other nodes.

Regardless of which state the dead node has, it cannot set its `Draining` flag
because it is dead. Instead, its leases will expire and will be taken by other
nodes. After `server.time_until_store_dead` elapses, the replicas of its
ranges will actively be rebalanced. Wait for the replica count to reach 0 as for
live nodes.

It is possible that a dead node becomes available and rejoins the cluster. It
would discover the `Decommissioned` flag has been set and follow the
decommissioning process.

## Temporary removal

No changes required. The existing CockroachDB process, described below, is
sufficient.

During a temporary removal, `server.time_until_store_dead` could be updated
to to the length of the downtime to avoid unnecessary movement of ranges.
However, it is difficult to predict the length of downtime. This is an
optimization which could be implemented later.

After a node is detected as unavailable for more than
`server.time_until_store_dead` (an env variable with default: 5 minutes), the
node is marked as incommunicado and the cluster decides that the node may not be
coming back and moves the data elsewhere. Its leases would have been transferred
to other nodes as soon as they expired without being renewed. Ranges are
up-replicated to other nodes and down-replicated (removed) from target node.
Ranges are not down-replicated when there does not exist a target for
up-replication because this causes more work if that node rejoins the cluster;
however, when permanently removing a node, we still want to do that. Although a
node can have multiple stores, there can be at most one replica for each replica
set (e.g. range) per node.

## CLI

Two new commands and one option will be added. These have been described above.
The first two are asynchronous commands and `quit` is synchronous.

- `node decommission <nodeID>...` prompts the user for confirmation. Passing
   `--yes` as a command-line flag will skip this prompt. This returns a list of
  all nodes, their statuses, replica count, decommissioning flag and draining
  flag.
  - *Nice to have*: satisfying safety constraints as a pre-requisite. As an
    example: `number_of_nodes < max(zone.number_of_replicas_desired)`. It would
    be nice to check that the number of nodes remaining after decommissioning is
    large enough for a quorum to be reached. However, this is not as easy as it
    sounds to achieve due to e.g. ZoneConfig.
- `node recommission <nodeID>...` has similar semantics to `decommission`. It
  prints a message to the user asking them to restart the node for the change to
  take effect.
- `quit --decommission`. This is synchronous to guarantee that availability of
  a replica set of a range is not compromised.

It is possible to decommission several nodes by passing multiple nodeIDs on the
command-line.

## UI

- The UI hides nodes which are marked as dead and are also in decommissioning
  state (as per the liveness table, or rather its gossiped information). That
  should have the desired effect and is straightforward while keeping stats, etc.
- If a node is dead, it cannot remove itself from the node table in the admin UI.
  Other nodes are responsible for hiding decommissioned nodes from the admin UI.
- The action of decommissioning a node creates an event that is displayed in the
  UI.

# Drawbacks

There is no atomic way to check that decommissioning a set of nodes will leave
enough nodes for the cluster to be available. A race can occur: in a five-node
cluster, two users can simultaneously request to decommission two different
nodes each. The resulting state leaves two nodes in decommissioning state and
only one live node. Decommissioning nodes can still participate in quorums;
the replication changes cannot make progress because there are not a sufficient
number of non-decommissioned nodes. The user can discover that too many nodes
are in decommissioning state and choose which nodes to recommission. It would
be nice to proactively detect this but the effort required is disproportionate
compared to the gain.

Recently dead nodes may cause decommissioning to hang until
`server.time_until_store_dead` elapses.

Recommissioning, i.e. undoing a decommission, requires node restart. We could
avoid this: if the node is live, the coordinating process can directly tell it
to stop draining. Punting on this for now; this could be future work.

# Alternatives

If the operation were blocking and it took a long time to complete, the
connection might timeout if no response was sent to the client.

# Unresolved questions

None.

[#16447]: https://github.com/cockroachdb/cockroach/pull/16447
[#6198]: https://github.com/cockroachdb/cockroach/issues/6198
