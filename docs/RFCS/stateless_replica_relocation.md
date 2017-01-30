- Feature Name: Stateless Replica Relocation
- Status: completed
- Start Date: 2015-08-19
- RFC PR: [#2171](https://github.com/cockroachdb/cockroach/pull/2171)
- Cockroach Issue: [#620](https://github.com/cockroachdb/cockroach/issues/620)

# Summary
A relocation is, conceptually, the transfer of a single replica from one store to
another. However, the implementation is necessarily the combination of two operations:

1. Creating a new replica for a range on a new store.
2. Removing a replica of the same range from a different store.

For example, by creating a replica on store Y and then removing a replica from
store X, you have in effect moved the replica from X to Y.

This RFC is suggesting an overall architectural design goal: that the decision to
make any individual operation (either a create or a remove) should be **stateless**.
In other words, the second operation in a replica relocation should not depend on a
specific invocation of the first operation.

# Motivation
For an assortment of reasons, Cockroach will often need to relocate the replicas
of a range. Most immediately, this is needed for repair (when a store dies and
its replicas are no longer usable) and rebalance (relocating replicas on
overloaded stores to stores with excess capacity).

A relocation must be expressed as a combination of two operations:

1. Creating a new replica for a range on a new store.
2. Removing a replica of the same range from a different store.

These operations can happen in either order as long as quorum is maintained in
the range's raft group after each individual operation, although one ordering may be
preferred over another.

Expressing a specific relocation (i.e. "move replica from store X to store Y")
would require maintaining some persistent state to link the two operations
involved. Storing that state presents a number of issues: where is it stored,
in memory or on disk? If it's in memory, does it have to be replicated through
raft or is it local to one node? If on disk, can the persistent state become
stale? How do you detect conflicts between two relocation operations initiated
from different places?

This RFC suggests that no such relocation state should be persisted. Instead, a
system that wants to initiate a relocation will perform only the first
operation; a different system will later detect the need for a complementary
operation and perform it. A relocation is thus completed without any state being
exchanged between those two systems.

By eliminating the need to persist any data about in-progress relocation
operations, the overall system is dramatically simplified.

# Detailed design
The implementation involves a few pieces:

1. Each range must have a persisted *target replication state*. This does not
   prescribe specific replica locations; it specifies a required count of
   replicas, along with some desired attributes for the stores where they are
   placed.
2. The core mechanic is a stateless function which can compare the immediate
   replication state of a range to its target replication state; if the target
   state is different, this function will either create or remove a replica in
   order to move the range towards the target replication state. By running
   multiple times (adding or removing a replica each time), the target
   replication state will eventually be matched.
3. Any operations that wish to *relocate* a replica need only perform the first
   operation of the relocation (either a create or a remove). This will perturb
   the range's replication state away from the target; the core function will
   later detect that mismatch, and correct it by performing the complementary
   operation of the relocation (a remove or a create).

The first piece is already present: each range has a zone configuration
which determines the target replication state for the range.

The second piece, the core mechanic, will be performed by the existing
"replicate queue" which will be renamed the "*replication queue*". This queue is
already used to add replicas to ranges which are under-replicated; it can be
enhanced to remove replicas from over-replicated ranges, thus satisfying the
basic requirements of the core mechanic.

The third piece simply informs the design of systems performing relocations; for
example, the upcoming repair and rebalance systems (still being planned).  After
identifying a relocation opportunity, these systems will perform the first
operation of the relocation (add or remove) and then insert the corresponding
replica into the local replication queue. The replication queue will then
perform the complementary operation. 

The final complication is how to ensure that the replicate queue promptly
identifies ranges outside of their ideal target state. As a queue it will be
attached to the replica scanner, but we will also want to enqueue a replica
immediately whenever we knowingly perturb the replication state. Thus,
components initiating a relocation (e.g. rebalance or repair) should immediately
enqueue their target replicas after changing the replication state. 

# Drawbacks

### Specific Move Operations
A stateless model for relocation precludes the ability to request specific
relocations; only the first operation can be made with specificity.

For example, the verb "Move replica from X to Y" cannot be expressed with
certainty; instead, only "Move replica to X from (some store)" or "Move replica
to Y from (some store)" can be expressed. The replicate queue will be responsible
for selecting an appropriate store for the complementary operation.

It is assumed that this level of specificity is simply not needed for any
relocation operations; there is no currently apparent use case where a move
between a specific pair of stores is needed.

Even if this was necessary, it might be possible to express by manipulating the
factors behind the individual stateless decisions.

### Thrashing of complementary operation
Because there is no relocation state, the possibility of "thrashing" is introduced.
For example:

1. Rebalance operation adds a new replica to the range.
2. The replicate queue picks up the range and detects the need to remove a
   replica; however, it decides to remove the replica that was just added.

This is possible if the rebalance's criteria for new replica selection are
sufficiently different from the replicate queue's selection criteria for
removing a replica.

To reduce this risk, there must be sufficient agreement in the criteria between
the operations; a Rebalance operation should avoid adding a new replica if
there's a realistic chance that the replicate queue will immediately remove it.

This can realized by adding a "buffer" zone between the different criteria; that
is, when selecting a replica to add, the new replica should be *significantly*
removed from the criteria for removing a replica, thus reducing the chances that
it will be selected.

When properly tuned to avoid thrashing, the stateless nature could instead be
considered a positive because it can respond to changes during the interim;
consider if, due to a delay, the relative health of nodes changes and the
originally selected replica is no longer a good option. The stateless system
will respond correctly to this situation.

### Delay in complementary operation
By separating the two operations, we are possibly delaying the application of
the second operation. For example, the replicate queue could be very busy, or an
untimely crash could result in the range being under- or over-replicated without
being in the replicate queue on any store.

However, many of these concerns are allayed by the existence of repair; if the
node goes down, the repair system will add the range to the replicate queue on
another store.

Even in an exotic failure scenario, the stateless design will eventually detect
the replication anomaly through the normal operation of the replica scanner.

### Lack of non-voting replicas
Our raft implementation currently lacks support for non-voting members; as a
result, some types of relocation will temporarily make the effected range more
fragile. 

When initially creating a replica, it is very far behind the current state of
the range and thus needs to receive a snapshot. It may take some time before the
range fully catches up and can take place in quorum commits.

However, without non-voting replicas we have no choice but to add the new
replica as a full member, thus changing the quorum requirements of the group. In
the case of odd-numbered ranges, this will increase the quorum count by one,
with the new range unable to be part of a quorum decision. This increases the
chance of losing quorum until that replica is caught up, thus reducing
availability.

This could be mitigated somewhat without having to completely add-non voting
replicas; in preparation for adding a replica, we could manually generate a
snapshot and send it to the node *before* adding it to the raft configuration.
This would decrease the window of time between adding the replica and having it
fully catch up.

"Lack of Non-voting replicas" is listed this as a drawback because going forward
with relocation *without* non-voting replication introduces this fragility,
regardless of how relocation decisions are made. Stateless relocation will still
work correctly when non-voting replicas are implemented; there will simply be a
delay in the case where a replica is added first (e.g. rebalance), with the
removal not taking place until the non-voting replica catches up and is upgraded
to a full group member. This is not trivial, but will still allow for stateless
decisions.

# Alternatives
The main alternative would be some sort of stateful system, where relocation
operations would be expressed explicitly (i.e. "Move replica from X to Y") and
then seen to completion. For reasons outlined in the "motivation" section, this
is considered sub-optimal when making distributed decisions.

### Relocation Master
The ultimate expression of this would be the designation of a "Relocation
master", a single node that makes all relocation decisions for the entire
cluster

There is enough information in gossip for a central node to make acceptable
decisions about replica movement. In some ways the individual decisions would
be worse, because they would be unable to consider current raft state; however,
in aggregate the decisions could be much better, because the central master
could consider groups of relocations together. For example, it would be able to
avoid distributed pitfalls such as under-loaded nodes being suddenly inundated
with requests for new replicas. It would be able to more quickly and correctly
move replicas to new nodes introduced to the system.

This system would also have an easier time communicating the individual
relocation decisions that were made. This could be helpful for debugging or
tweaking relocation criteria.

However, It's important to note that a relocation master is not entirely
incompatible with the "stateless" design; the relocation master could simply be
the initiator of the stateless operations. You could thus get much of the
improved decision making and communication of the master without having to move
to a stateful design.

# Unresolved questions

### Raft leader constraint
The biggest unresolved issue is the requirement for relocation decisions to be
constrained to the raft leader.

For example, there is no firm need for a relocation operation to be initiated
from a range leader; a secondary replica can safely initiate an add or remove
replica, with the safety being guaranteed by a combination of raft and
cockroach's transactions.

However, if the criteria for an operation wants to consider raft state (such as
which nodes are behind in replication), those decisions could only be made from
the raft leader (which is the only node that has the full raft state).
Alternatively, an interface could be provided for non-leader members to query
that information from the leader. 
