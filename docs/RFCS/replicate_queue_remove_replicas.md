- Feature Name: Replicate Queue: Remove Replicas Enhancement
- Status: draft
- Start Date: 2015-08-18
- RFC PR: #2153
- Cockroach Issue: #620

# Summary
The Replicate Queue is currently used to add replicas to a range that is
under-replicated. It can only add replicas, and it can only handle ranges with
homogenous ReplicaAttrs (i.e. every replica requires the same attributes). This
change will enhance the replicate queue to allow for down-replication: the
removal of replicas from a range which has too many replicas. 

With this addition, the replicate queue will be able to correct the replication
of any range to its optimal replica count, regardless of how the range got into
a sub-optimal state. Most immediately, this will provide crucial support for the
highly desired "Rebalance" and "Repair" operations, but it will also provide
support any other operation which needs to relocate a replica.

This RFC will not address the issue of non-homogenous ReplicaAttrs. While not
exceptionally difficult, we are simply not yet deploying with non-homogenous
replicas and the work can be postponed.

# Motivation
Rebalancing is the major missing component from Cockroach at this point; without
it, Cockroach is constrained to running on three stores. Rebalancing will
opportunistically relocate replicas from over-utilized stores to under-utilized
stores.

Another important missing feature is Repair; detecting when a store is down, and
relocating the replicas that were on that store. The relocation of these replicas
is similar to Rebalancing, but happens in a slightly different fashion.

Regardless of the origin, every relocation operation is conceptually the
transfer of one replica in a range: the replica is removed from one store (the
source) and added to another (the target).  To work with raft, this is
explicitly divided into two separate operations: removing from the source store,
and adding to the target store. These operations can generally happen in either
order, but must be performed one at a time.

The replicate queue can be responsible for the second operation of all
relocations; whether adding or removing a replica. The simple goal of the
replicate queue is to restore a range to its ideal replication state.

With the backing of the replicate queue, both rebalance and repair operations
can be thought of as "perturbing" the ideal replication state, by adding or
removing a replica respectively. The replicate queue will restore the ideal
replication state by performing the opposite operation, and thus completing the
relocation. 

This significantly reduces the complexity of the operations which initiate a
relocation (rebalance, repair).  This will apply in general for all "perturbing"
operation; for example, a change to ReplicaAttrs.  

By attaching it to the replica scanner, the replicate queue can also
(eventually) repair unanticipated replication configurations resulting from
exotic failure situations, a highly desirable property.

# Detailed design
The basic infrastructure to add and remove individual replicas is already in
place; the etcd raft implementation has built-in support for the integrity-safe
addition or removal of single replicas, and that ability has already been
exposed to our replicas via the ChangeReplicas function.

The replicate queue already adds replicas to under-replicated ranges using
ChangeReplicas (this is how the initial replicas are created on the second and
third store of a three-node cluster).  The replicate queue does not currently
remove ranges from over-replicated ranges; however, this functionality can be
added with relatively few changes.

## Code Changes
The Replicate Queue will be augmented in the following ways:

#### replicateQueue.needsReplication
The existing `replicateQueue.needsReplication()` method will be modified to
return `true` in the case where there are either too few or too many replicas.

This method also returns the difference between the current number of replicas
and the target number; this will be negative in the case of too many replicas.

`replicateQueue.shouldQueue()` is currently using the difference returned by
`needsReplication()` as the priority for the replica; for negative numbers (too
many replicas) the method will now return the absolute value of the difference.
For *positive* numbers (too few replicas) it will now return the diffrence +
10, to ensure that missing replicas are prioritized over excess replicas (10 is
an arbitrary choice).

#### allocator
The `allocator` class is currently used by the replicate queue to select stores
as replica targets.

The class currently shares some scattered but similar logic between functions
`allocateTargetInternal()`, `RebalanceTarget()` and `ShouldRebalance()` which
determine how a given store's used capacity relates to the mean used capacity
for the cluster.

This code will be refactored into three functions:
+ `bestStore()`, which will return the "best" store in a slice of stores according to the
  capacity criteria.
+ `worstStore()`, which returns the "worst" store in a slice.
+ `evaluateStore()` function, which will simply determine if a store is "good"
  or "bad" according to the overall state of the cluster.

The functionality within allocator will be refactored to use these functions.

#### replicateQueue.process
Process will now use the following logic for a replica which requires a replication change:

1. If the range needs additional replicas:
  + The queue will use the allocator's `AllocateTarget()` method to select a target node.
  + If a target node is found, the queue will attempt to add a replica to it.
2. If the range has too many replicas:
  + The queue will grade the existing replicas using `allocator.worstStore()`.
  + The worst replica will be removed.
3. In either case, the queued replica will be re-queued to check for additional
   changes, unless it was the replica that was removed.

If an error occurs during step 1 or 2, the replica will *not* be re-queued. This
will prevent the possibility of an infinite queueing loop due to some unforseen
persistent error.

# Prequisites

## Distribution Concerns
The replication queue process will only take action on a replica that is the
range leader; however, once initiated, there is no requirement that the replica
remains the range leader through the operation.  Therefore, it is possible that
multiple replicas for the same range could be performing range repair operations
at the same time.

Compounding this, many of the stats used to evaluate the allocator's methods
(e.g. `evaluateStore()`) are derived from gossip statistics, which may be
inconsistent across stores.  Therefore, the queues on different stores could
come to different conclusions on which replica should be added or removed. If
the transactions are not properly coordinated, this could result in double adds
or double removes.

The existing ChangeReplicas does have some protection against races, but it is
not adequate in our case. 

This problem is specially captured in issue
[#2152](https://github.com/cockroachdb/cockroach/issues/2152), which includes a
solution.  That issue is a prequisite for this RFC.

# Drawbacks

# Alternatives

### Two queues
A simple alternative to the design would split the replicate queue into two
queues, an 'up-replicate' and 'down-replicate' queue. 

However, this does not make the function of the queue significantly clearer; the
goal of "right-sizing" the replication factor for a range is very clear, and
dividing it further does not add clarity.

### One queue for all possible changes
Another queue-based alternative (which represents further work) would be to combine the
rebalance, repair and replicate queues into a single queue.  This queue would be able to make more complicated decisions about the
replication state of a range. 

However, this muddles the focus of each individual part. The conditions for
rebalance are significantly different than the conditions for repair, which are
in turn significantly different than the conditions for "correcting" the
replication factor.

Dividing these into three queues is the clearest solution.

### Non-queue solution.
Another alternative would be to express this functionality as something
different than a queue - perhaps a "correction" function which is called by
other processes.

However, expression as a queue is flexible enough to meet those needs; other
processes can simply add a replica to the queue. By expressing as a queue, the
scanner can be used directly.

# Unresolved questions
