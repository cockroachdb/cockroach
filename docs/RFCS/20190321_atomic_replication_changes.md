- Feature Name: Atomic Replication Changes
- Status: draft
- Start Date: 2019-03-21
- Authors: Tobias Grieger
- RFC PR: TBD
- Cockroach Issue: #12768 (also #6782)

# Summary

We currently use the simple membership change mechanism which uses the property
that quorums of adjacent configurations which differ only by one replica have
overlapping quorums; see Chapter 4 of the [Raft thesis] for details. This
mechanism forces us to temporarily enter undesirable configurations in which an
availability zone outage can cause a loss of quorum.

Implement Raft's **Joint Consensus** membership change protocol (also Chapter
4), thus carrying out replication changes that allow the addition and removal of
multiple replicas at the same time, to avoid ever having to enter these
undesirable configurations.

In particular, allow surviving the outage of availability zone (or region) when
replicating across three availability zones (or regions) while replicas are
being moved.

[Raft thesis]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf

# Motivation

Consider the replication configuration from the summary, that is, a range is
replicated threefold, with one replica in each of the three available
availability zones.

|AZ1|AZ2|AZ3|
|---|---|---|
| 1 | 2 | 3 |

This configuration allows for an outage of at most one availability zones, since
two replicas need to be online to avoid unavailability of the Raft group.

Assume that the node containing replica `1` is overloaded and wants to pass its
replica to another node (in `AZ1`).

In today's code, we can only carry out a single addition or removal of a replica
atomically. Moving a replica thus has to be carried out over two steps, a removal
and an addition (or in reverse order).

Quorum in the initial configuration is two out of three. If we chose to remove `1`
first, we end up in this configuration:

|AZ1|AZ2|AZ3|
|---|---|---|
|   | 2 | 3 |

which does not survive an outage of either `AZ2` or `AZ3`, since a quorum for a
group of size two is two, and only one replica would be left if either AZ
failed. (We also avoid downreplicating first for other reasons having to do with
the difference between temporary and permanent unavailability).

What if instead, we added a replica first?

|AZ1 |AZ2|AZ3|
|----|---|---|
|1, 4| 2 | 3 |

Now we have four replicas, for which a quorum needs three participants. If `AZ1`
goes down, this is no longer true and the range is unavailable.

This problem can be worked around by moving to more availability zones and/or a higher
replication factor, but often more availability zones are not actually available within
a region, or customers may be unwilling or at least decidedly unenthusiastic about the
overhead of these solutions.

Atomic replication changes obviate workarounds since they allow transitioning into the
final configuration

|AZ1 |AZ2|AZ3|
|----|---|---|
| 4  | 2 | 3 |

in one logical step, without entering a vulnerable configuration.

This is not the only motivation. We're also in some situations currently unable
to rebalance replicas between stores without rebalancing through an auxiliary
node (#6782); this is for the same reasons as above, with "nodes" replacing
"availability zones". (Solving that problem is a medium-sized follow-up project
after atomic replication changes are implemented; we do not subsume it here).

Additionally, much of our rebalancing logic has been built around the constraint
that only addition or removal of a replica is possible in each step. Without that
requirement, we can make changes faster and with more reliable code. In fact,
corresponding changes to the allocator and replicate queue will be needed to
take advantage of joint consensus once it is available.

# Guide-level explanation

Conceptually, atomic replication changes contain little surprising detail.
Ignoring what happens above Raft for a moment, each Raft peer stores an active
configuration. A configuration is little more than a list of members of the Raft
group, but is critical for correctness as it determines how many
acknowledgements are needed to win a leadership election and to consider a log
entry as durably committed.

For example, for a Raft group with three members, their configurations may be

| replica 1 | replica 2 | replica 3|
|----|---|---|
| 1,2,3  | 1,2,3 | 1,2,3 |

During a traditional (i.e. non-joint) replica change, say, adding a single
`replica 4`, a log entry describing the configuration is appended to the log and
committed, resulting in a transition over to the new configuration (at this
point, we're intentionally vague about when exactly a configuration activates).
Below, each row represents a point in time.

| replica 1 | replica 2 | replica 3 | replica 4 |
|----|---|---|---|
| 1,2,3  | 1,2,3 | 1,2,3 |  |
| **1,2,3,4**  | 1,2,3 | 1,2,3 |  |
| 1,2,3,4  | 1,2,3 | **1,2,3,4** |  |
| 1,2,3,4  | **1,2,3,4** | 1,2,3,4 | 1,2,3 |
| 1,2,3,4  | 1,2,3,4 | 1,2,3,4 | **1,2,3,4** |

For example, if `replica 2` decided to call an election and collected votes
at a point in time before the fourth row, it would declare itself winner once
it had one two votes. At or after the fourth row, it's aware that in fact it
would need three votes (three is the quorum for a four-member group).

When exactly a Raft peer switches to the new configuration is an interesting
question to ask. The answer is that it happens when the entry is appended to a
peer's log. Notably that is not what we do today; a later section goes into
detail on why we want to roll back that decision.

Now let's consider joint consensus. The way it works is that peers switch first
to an intermediate configuration (the "joint configuration") in which a quorum
is only achieved if a quorum for both the old and the new configuration is
achieved. When that joint configuration is considered safely rolled out (details
on that later), another config change is carried out that moves the group into
its final configuration.

So why does Raft use a simpler, more restricted configuration change protocol?
It's because a single configuration change transition is actually safe when only
one replica is added or removed at a time, due to the fact that for a set of
peers `{1, .., n, n+1}`, any quorum of `{1, .., n}` overlaps any other quorum of
`{1, .., n, n+1}` and vice versa (see the argument in section 3.6.3 of the Raft
thesis). In arguing that complex configuration changes are achievable by
one-at-a-time configuration changes, the complexity of introducing joint
consensus and its more elaborate vote counting is avoided.

However, as discussed above, this argument does not apply to our use case, in
which configuration changes are frequent enough to have to avoid entering
vulnerable configurations.

Returning to the use case of replacing a replica discussed earlier and this time
considering joint consensus, we want to ultimately carry out a config change
that replaces the configuration

```
1 2 3
```

with (assuming we want to replace `replica 2`)

```
1 3 4
```

Denoting the joint configuration of `A` and `B` as `A && B` we might see
the full configuration change play out as follows:


| replica 1 | replica 2 | replica 3 | replica 4
|----|---|---|---|
| 1,2,3  | 1,2,3 | 1,2,3 |  |
| **`1,2,3 && 1,3,4`**  | 1,2,3 | 1,2,3 |  |
| `1,2,3 && 1,3,4`  | **`1,2,3 && 1,3,4`** | 1,2,3 |  |
| `1,2,3 && 1,3,4`  | `1,2,3 && 1,3,4` | **`1,2,3 && 1,3,4`** | 1,2,3 |
| `1,2,3 && 1,3,4`  | `1,2,3 && 1,3,4` | `1,2,3 && 1,3,4` | **`1,2,3 && 1,3,4`** |
| **`1,3,4`**  | `1,2,3 && 1,3,4` | `1,2,3 && 1,3,4` | `1,2,3 && 1,3,4` |
| `1,3,4`  | `1,2,3 && 1,3,4` | `1,2,3 && 1,3,4` | **`1,3,4`** |
| `1,3,4`  | **`1,3,4`** | **`1,3,4`** | `1,3,4` |

Note that since the joint configuration is available as long as both of its
constituent configurations are available, it remains available during outages
that would be survived by both of its constituent configurations, as desired.

## Integration into CockroachDB

So far, we have only discussed how Raft Joint Consensus works. To integrate it
with CockroachDB, we need to do more work and in this section discuss a
simplified version of how replication changes are carried out.

The replication configuration of a CockroachDB Range is stored in the
`RangeDescriptor`, which is a replicated key which lives in a plane parallel to
the range's `StartKey`. For purposes of KV request routing, we also keep
synchronous copies of the range descriptor in the meta ranges (irrelevant here).

A (simplified) replication change in today's code (i.e. no joint consensus) is
carried out as follows. Remember that we can only add or remove a replica,
nothing else, and we chose to add one (removal is similar).

- a `Store` on which a replica should be added is chosen.
- a transaction is initiated.
- the transaction reads the current range descriptor, schematically written as
`Replicas{Store 1, Store 2, Store 3}`.
- a new `ReplicaDescriptor` describing the new `Replica` is added to the
descriptor (in-mem), say: `Replicas{ Store 1, Store 2, Store 3, Store 7}`.
- the transaction writes the updated descriptor.
- the transaction attempts to commit, that is, proposed an
`EndTransactionRequest` containing a `ChangeReplicasTrigger`.
- the trigger causes the commit to be proposed as a Raft configuration change,
i.e. Raft is made aware that a new peer is being added (via `ProposeConfChange`);
Raft uses this only to prevent multiple configuration changes from overlapping).
- when the trigger applies on any of the member `Replica`s, it
    - a) updates the in-memory state related to the range descriptor.
    - b) calls into Raft to update the replication configuration (`ApplyConfChange`).
- the replication change is complete.

When using joint consensus, we can chose an arbitrary replication change. For
reasons of observability, we want the `RangeDescriptor` to accurately (as much
as possible) reflect the configuration of the Raft group at all times. This
implies that we have to update the `RangeDescriptor` in two steps, once to have
it reflect the joint consensus, and then again to move to the final
configuration. This implies breaking the whole replication change up into two
transactions, the first one to move into the joint configuration, and the second
one to move out of it.

In the example below, we want to completely move all three replicas of the range
to new stores. All going well, the sequence of events would be

- three `Store`s on which `Replica`s should be added are chosen, say `Store 4, Store 5, Store 6`.
- a transaction is initiated.
- the transaction reads the current descriptor, say `Replicas{Store 1, Store 2, Store 3}`.
- the transaction writes an updated descriptor `Replicas{Store 4, Store 5, Store
6}, Joint{Store 1, Store 2, Store 3}`.
- the transaction proposes and commits with a trigger as before, reflecting the
joint configuration `1, 2, 3 && 4, 5, 6`.
- a new transaction is initiated.
- the range descriptor is read and the `Joint` portion containing the old set of stores removed.
- the transaction writes the resulting descriptor `Replicas{Store 4, Store 5, Store 6}`.
- the transaction commits with a trigger, reflecting the Raft configuration `4, 5, 6`.

By moving from one transaction to two, we have to worry about non-atomicity. We
do this by ensuring that if the first transaction happens and then the process
carrying out the second one is interrupted, the joint configuration is swiftly
rolled back (i.e. changed back to the old configuration) by anyone observing the
joint state (this is likely easier than trying to complete the change, details
are left to the reference section).

# Reference-level explanation

In this section, we discuss technical complexities, safety, test plans, and
auxiliary work items required to make this project successful.

## Acceptance testing

The reference acceptance test to consider this work completed is the following.

1. six node cluster across three availability zones, with constraints set up in
   various ways (locality, explicit constraints) matching POCs or real deployments
2. with aggressive rebalancing settings (incentivizing lateral replica movement)
3. the workload is
    - import TPCC w=max dataset (1)
    - import TPCC w=max dataset (2)
    - drop the second copy of the data set (to exercise range merges' rebalancing)
    - run TPCC w=max for two hours (but the achieved efficiency doesn't matter,
    and tolerate errors)
4. throughout the import and TPCC run,
    - SCATTER the TPCC tables (once they're visible after `IMPORT`, otherwise skip)
    - bring one AZ down (i.e. nodes matching its constraint killed)
    - check absence of unavailable ranges (metrics and/or`crdb_internal.ranges_no_leases`)
    - SCATTER
    - check for unavailability again
    - AZ (nodes) are brought back up
    - wait a few minutes
    - repeat

This exercises both the main desired guarantee of surviving an AZ outage when
running with a replica in each of three AZs, as well auxiliary unavailabilities
that may occur due to auxiliary mechanisms carrying out replication changes.

Variants of this test that remove the testing of SCATTER or range merge should
be available to better isolate problems.

TODO(tbg): are there other topologies that should be verified?

## Switching to append-time (i.e. true Raft) membership changes

In today's `etcd/raft` code, peers activate configuration changes when they
apply the corresponding log entry ("apply-time") as opposed to when they are
appended to the log ("append-time"; the official method described in the [Raft
thesis]). In assembling this document, concerns have arisen with the apply-time
approach and with the strategy of generalizing it to include joint consensus.
The final suggestion in this section will be to implement joint consensus
"append-time", as outlined in the Raft thesis. There is also a hypothetical
variant, "commit-time", which is somewhat less fraught with error than
"apply-time" but still shares enough of the correctness concerns to not present
an alternative.

### Append-time

This is the official Raft way of changing membership. The most recent
configuration in a peer's log is the one it will use for any voting/commit
decisions it may make, and it is considered "complete" when it is known to have
committed (before that point, additional configuration changes are dropped). In
particular, a configuration entry added to the leader's log will be used to
determine who to replicate the entry to, and what quorum to use.

The membership change first moves from the old configuration into a joint
configuration, in which quorum requires both a majority of the old configuration
and the desired final configuration. Having rolled out this joint configuration,
another configuration change into the new configuration can be carried out
safely.

### Apply-time

We start with a an example which will result in split brain. Note that this
example does not apply to the implementation in `etcd/raft` because of
implementation details which we'll discuss later, but we will then present a way
in which the modified example seems to apply to `etcd/raft` as well. For now, it
is enough to imagine an implementation of Raft that fully follows the specs but
happens to allow the history presented below.

The anomaly revolves around the fact that log entries may be committed without
this being known to all peers (via the leader-communicated commit index).

We refer to a log entry on follower X that is committed but not known to have
committed on that follower as "implicitly committed". Implicitly committed
entries are common: an entry is committed when it is durably appended to the
logs on a quorum of the replicas, but it is only in the next round of
communication from the leader that followers can possibly learn of this fact.
 
A follower cannot apply an entry that is implicitly committed, so by carrying
out two rounds of membership changes which are both implicitly committed on a
follower, that follower will continue to use a double-lagging configuration.
This in turn violates the invariant central to the simple membership change
protocol, namely that only adjacent membership changes are in use concurrently,
and allows that follower to win an election that leads to split brain.

We walk through the example step by step. For the log, **bold** will denote the commit
index, and square brackets the `[applied index]`. For example, in the following log

e1 [e2] e3 **e4** e5

The commit index is the index of the entry `e4`, but the log has only been applied
up to (and including) `e2`.

#### Add a first implicitly committed membership change

Peer three is the leader and we start with a log containing a fully replicated
empty entry `e` (this doesn't matter) that all peers know is committed (bold)

|ID | Cfg   | Log   |
|:---:|:-----:| :-----|
| 1 | 1,2,3 | **[e]**     |
| 2 | 1,2,3 | **[e]**     |
| 3 | 1,2,3 | **[e]**     |

`3` now proposes an `A1 = ADD_REPLICA(4)`. It arrives in both `1` and `2`'s logs, and
the leader considers it committed, but the messages informing `1` and `2` of that fact
are dropped. We assume `4` comes up and gets caught up all the way.

|ID | Cfg   | Log |
|:---:|:-----:| :-----|
| 1 | 1,2,3 | **[e]** A1 |
| 2 | 1,2,3 | **[e]** A1 |
| 3 | 1,2,3,4 | e **[A1]** |
| 4 | 1,2,3,4 | e **[A1]** |

Peers `3` and `4` commit and apply the first configuration change `A1`. The
leader `3` thus uses it for future replication decisions.

#### Add a second implicitly committed membership change

Next, Peer `3` wants to carry out `A2 = REMOVE_REPLICA(1)`. It needs three out
of four acks for this, and the previous game repeats. Let's say `1` doesn't even
receive the entry nor the fact that it commits (once it does); `2` gets the entry
but never learns that it commits, and `3` and `4` get the entry and commit it, too.

|ID | Cfg   | Log |
|:---:|:-----:| :-----|
| 1 | 1,2,3 | **[e]** A1 |
| 2 | 1,2,3 | **[e]** A1 A2 |
| 3 | 1,2,3,4 | e [A1] **A2** |
| 4 | 1,2,3,4 | e [A1] **A2** |

A moment later, `3` and `4` apply the config change `A2` and begin using it.

|ID | Cfg   | Log |
|:---:|:-----:| :-----|
| 1 | 1,2,3 | **[e]** A1 |
| 2 | 1,2,3 | **[e]** A1 A2 |
| 3 | **2,3,4** | e A1 **[A2]** |
| 4 | **2,3,4** | e A1 **[A2]** |

#### Campaign using a doubly stale configuration

Now there's a network partition between `{1, 2}` and `{3,4}`. `2` calls an
election and `1` votes for it. Since `2` is using the initial configuration,
this is enough to consider itself winner, and it steps up as a leader.

But `3` also still considers itself leader, and even more, is actually able to
make progress perfectly well despite there being a leader at a higher term
already (`2`). At this point, all is already lost, but we'll keep going anyway.

Let's say `3` commits and applies some more data records (for example user
writes) which it can do since `{3,4}` is a quorum of `{2,3,4}`:

|ID | Cfg   | Log |
|:---:|:-----:| :-----|
| 1 | 1,2,3 | **[e]** A1 |
| 2 | 1,2,3 | **[e]** A1 A2 |
| 3 | **2,3,4** | e A1 A2 x y **[z]** |
| 4 | **2,3,4** | e A1 A2 x y **[z]** |

in the meantime, `{1,2}` also sees some incoming proposals, though they're only
queued in the log at the leader `2`:

|ID | Cfg   | Log |
|:---:|:-----:| :-----|
| 1 | 1,2,3 | **[e]** A1 |
| 2 | 1,2,3 | **[e]** A1 A2 a b c |
| 3 | **2,3,4** | e A1 A2 x y **[z]** |
| 4 | **2,3,4** | e A1 A2 x y **[z]** |

`2` now begins to do the work that has been queued up. It distributes the log to
`1` (which is enough to commit it) and lets `1` know:

|ID | Cfg   | Log |
|:---:|:-----:| :-----|
| 1 | 1,2,3 | [e] A1 A2 a b **c** |
| 2 | 1,2,3 | [e] A1 A2 a b **c** |
| 3 | **2,3,4** | e A1 A2 x y **[z]** |
| 4 | **2,3,4** | e A1 A2 x y **[z]** |

Now `2` applies the newly committed log indexes. First it sees two configuration
changes which it will activate for future quorum decisions (there won't be any
in this example), and then it applies `a`, `b`, and `c` to the state machine
(which also definitely tells clients that the commands were successfully
committed). `1` does the same.

Next, the partition heals. The two leaders get in touch with each other, and one
is going to overwrite the others' log, replacing committed records (unless some
assertion kills one or both leaders; doesn't matter -- the damage is done).

There are variations of this argument that use the fact that the commit index
known to a peer can regress when peers restart, so moving to commit-time instead
of applied-time activation of configuration changes does not prevent this kind
of problem. However, apply-time is strictly worse. For one, there is no
requirement that replicas need to apply committed changes at all, that is, they
can lag behind as much as they want, and can use configuration changes many
generations old. There are very straightforward counter-examples found in this
alone, though we opt for one that is more intricate to show the difficulties in
trying to patch the algorithm.

#### Modifying the example for etcd/raft

The above examples will not cause problems in the `etcd/raft` implementation,
though this seems largely incidental. In `etcd/raft`, the commit index is
communicated with each new append, which leads to the second membership change
to force the first one to be explicitly committed. This upholds the invariant
that only adjacent configurations may be active concurrently *if it also forces
the first change to be applied in the same Raft cycle*.

It turns out that a `etcd/raft` follower does not necessarily apply the config
change immediately when it is marked as committed, because `etcd/raft` limits
the size of the total slice of entries handed to userspace for application to
the state machine. This mechanism exists because loading too many entries into
memory is dangerous, but in effect it allows us to fix the example: by adding
regular commands to the logs as "fillers" and assuming that `2`, we can
arbitrarily increase the number of Raft cycles until `2` actually applies the
first configuration change. However, it will help commit the second
configuration in the meantime, though, so that `3` and `4` can start using it
already. This puts us back in the situation needed to end up in split brain: `1`
and `2` use the initial config `{1,2,3}`, while `3` and `4` use the final one
`{2,3,4}`.

It's also worth pointing out that decoupling the application of commands to the
state machine from their commit further is interesting from a performance point
of view. As rare as this counterexample is today (never observed as far as I
can tell), it may become less rare in the future.

Note also that there's an "ingredient" we haven't used yet: the commit index
may, in principle, regress because there's no requirement to sync it to disk
durably. `etcd/raft`  tends to sync it quite frequently because it asks to have
it synced whenever a log entry is appended. This is another area in which future
performance improvements may rip open new holes.

### The example for append-time activation

On the other hand, revising this example with append-time activation, everything
will work safely. This is because it doesn't matter whether an entry is explicitly
or implicitly committed. All that matters is whether it's in the log; appending to
the log is further always synced (i.e. entries are durable), so node restarts are
not going to help find interesting behavior.

### History of apply-time activation

The history of apply-time activation appears to indicate that the gravity of the
divergence between the two membership change protocols was [not apparent at the
time][apply-time discussion].

In what may have been a tongue-in-cheek comment, the author of Raft [Diego
Ongaro][ongardie], has referred to `etcd`'s membership changes as
[bastardized](https://groups.google.com/d/msg/raft-dev/t4xj6dJTP6E/5-HIkcJ5r80J),
at the very least suggesting that they don't consider it a viable alternative.

[apply-time discussion]: https://github.com/etcd-io/etcd/issues/2397#issuecomment-76817382
[ongardie]: https://ongardie.net/diego/

No compelling reason for `etcd/raft` having chosen apply-time activation in the
first place was discovered while researching this RFC. One concern that came up
was that append-time activation naively would require combing the log for config
changes in some scenarios, notably including while instantiating a Raft group,
but this concern is addressed in this RFC. Simplicity was also mentioned
repeatedly, though that appears to be subjective.

## Implementation notes for append-time activation

The implementation will have to concern itself primarily with three problems:

1. Migration. `etcd/raft` today does not use Joint Consensus; the corresponding
API needs to be introduced, ideally in a way that does not break existing
applications that don't want to use Joint Consensus.
    - avoid interaction between (then) legacy membership changes and Joint
    Consensus.
    - decide whether one-time use of Joint Consensus prohibits later use of
    simple membership changes.
2. Discovery of the active configuration. A configuration may be introduced via
an `append` (the usual path), but it may also be removed from the log (unusual,
during leadership changes), and it needs to be loaded from the log when Raft is
instantiated.
    - avoid performance concerns (due to scanning the whole log); this is
    particularly pressing in the CockroachDB use case in which many Raft groups
    instantiate at the same time and on the same underlying storage engine.
    - do not require the application code to participate in this tracking

TODO(tbg): rework everything below

### Falling back to previous configuration

Configuration log entries are used before they may be committed. They may thus
be replaced by another entry, which is a situation the peer has to detect and
react to by discovering the previous configuration from the log (but the entries
replacing the current tail of the log could contain a newer configuration entry,
so that has to be taken into account).

Additionally, log truncations need to preserve the most recently truncated
configuration change (i.e. it needs to be kept in `TruncatedState`) to account
for the (steady state) case in which no more recent configuration change is in
the logs. Naively, log truncation could unmarshal each truncated entry to check
for config changes, but this is too slow (log truncations are frequent).
Instead, this work is only done for indexes less than or equal to the highest
log index at which a configuration change was ever observed by the peer. This
ensures that all configuration changes will be discovered, and avoids doing
extra work in the common case of no recent configuration changes.

### Mismatch between config and visible `RangeDescriptor`

At a higher level, we lose the property that the visible `RangeDescriptor` (i.e.
the `RangeDescriptor` as observed by a `Replica` state machine at a given
applied index) reflects the Raft peer's configuration. This has minor
implications on debuggability because we'll need to expose the Raft group's
configuration separately, for the rare case that they should differ. For
example, a `Replica` may be unavailable because it is in a joint configuration,
but if the corresponding log entry has not been applied (perhaps because it
never made it to a quorum of the joint configuration; the new configuration may
have lost quorum) we need to be able to observe the joint configuration, not
just the `RangeDescriptor` (which will only reflect the old peers, which have
not lost quorum).

We would not need the `ApplyConfChange` callback as a side effect of applying
a `ChangeReplicaTrigger` as we do today.

### Fast group instantiation

Instantiating a Raft group will need to be made performant enough now that the
config must, in principle, be discovered from the Raft log. We can scan the log
from `AppliedIndex+1` to the end, falling back to the `RangeDescriptor` for
describing the configuration in the common case in which no unapplied config
change is found. It is common for "idle" ranges to have
`AppliedIndex==LastIndex`, in which case the `RangeDescriptor` could be used
without any additional I/O, making this suitable for our use case of potentially
tens of thousands of replicas.

## Detection and avoidance of vulnerable configurations

With today's code, entering vulnerable configurations is sometimes unavoidable,
but there are also situations in which we enter vulnerable configurations with-
out there being any pressing need to do so; see #36025 for a current example in
which a range is downreplicated from five to two replicas, with one of the two
replicas residing on an unavailable node (for an immediate and completely
avoidable loss of quorum). Similarly, even on the regular replicate queue paths,
we seem to spend much more time in vulnerable configurations than strictly
necessary.

Since we introduce atomic replication changes as a means to an end -
establishing confidence in the survivability of a given deployment topology - we
must absolutely rule out that a vulnerable configuration would be entered
anyway.

The root problem here is that access to replication changes is given too
liberally. the merge queue, the `StoreRebalancer`, `RELOCATE_RANGE`, and the
replicate queue (which includes `SCATTER`) all get to make replication changes,
sometimes competing with each other. However, only the replicate queue appears
to have an awareness of when a replication change would compromise availability.

Its detection mechanisms need to be streamlined, fortified and forced in the
path of any replication change that may occur for whatever reason. The fact that
we see questionable replication behavior in practice (but not in our tests) also
suggests a gap in our testing.

We must also audit range merges in particular. Range merges need to colocate the
replica sets of the two adjacent ranges involved in a merge. We must make sure
to not merge ranges when doing so would violate constraints during the
colocation, as this could constitute a vulnerable configuration. Merges
currently check whether the merged range would become eligible for a split right
away, which may already cover this case, but this needs to be verified.

An ongoing investigation in this area is taking place in
[#12768](https://github.com/cockroachdb/cockroach/issues/12768#issuecomment-475913661).

## Arbitrary configuration changes vs. atomic swap of a Replica

Atomically replacing a Replica is the main desired use case, but restricting to
this at the Raft level does not seem to result in a reduction in complexity. We
thus propose to implement full joint consensus at the Raft level, and to set up
the replication changes inside of CockroachDB to in principle support arbitrary
changes as well. Actually scheduling arbitrary changes is out of scope; enabling
atomic swaps is the main end goal.

## Updating the allocator / replicate queue to use atomic replication changes

The current allocator and replication queue are very much bought into the "one
addition or removal at a time" restriction currently present and is generally
an area in need of attention.

We need to make the changes prudently to improve the overall clarity of the code.
The allocator should, given the current `RangeDescriptor` and all the auxiliary
information it needs to make a decision, which includes

- current availability of nodes
- statistics
- zone configurations, constraints, localities

output a desired target configuration that today may only be reachable over
several discrete steps. This configuration will then be used for the atomic
replication change.

### Updating auxiliary consumers of `RangeDescriptor`

`RangeDescriptor`s are exposed and accessed in a variety of places, most notably
the range status debug page and `crdb_internal.ranges{_no_leases}`. Both of
these need to find a way to faithfully reflect joint configurations, or to at
least tolerate them. All uses of `RangeDescriptor`s in our code based need to be
checked for adjustments.

### Rolling back unfinished replication changes

Replicas encountered in a joint configuration by the replicate queue will be
queued with high priority and will be moved back to the pre-joint configuration
(i.e. rewrite the `RangeDescriptor` transactionally back to its previous state
including a config change trigger as usual)

This is a safer option than moving to the new configuration since the new replicas
may no longer be seeded with preemptive snapshots. (This concern will go away if
we introduce learner replicas, but even then it's likely not worth it to change
this to work the other way).

## Drawbacks

The change is a significant engineering project with an opportunity cost.

## Rationale and Alternatives

Historically our strongest argument against atomic replication changes has been
the lack of available engineering bandwidth for what is a fairly complex change.

There's no known alternative to atomic replication changes via joint consensus
other than perhaps switching to some other consensus/replication protocol
altogether. There's no vetted (or unvetted) candidate at this point and any such
project's complexity would exceed that of the present proposal magnitudes more

This leaves as the only reasonable alternative not introducing atomic
replication changes and channeling energy into minimizing the time spent in
vulnerable replication configurations instead. At the end of the day this
is a judgment call, but it appears that there is an understanding that that
will not be enough.

### Flexible Paxos

The [Flexible Paxos](https://arxiv.org/pdf/1608.06696v1.pdf) paper points out that
an even-numbered configuration (such as the vulnerable configuration from the
example in the introduction) only needs a quorum of `n/2` for committing entries
as long as `(n/2) + 1` votes are still required for leader election. This suggests
an alternative (restricting generality to the example for simplicity) to making
that specific scenario (and ones like it) safe by forcing the leader to be alone
in its AZ during the even configuration.

If `3` is the leader, and we add a fourth replica `4` to any but the leader's `AZ`,
we end up in a configuration like this. The claim is that this survives outage of
one `AZ`.

|AZ1 |AZ2|AZ3|
|----|---|---|
|1, 4| 2 | 3 |

1. if `AZ3` goes down, the leader is dead. But three replicas survive, and
that's enough to elect a new leader and to continue. If the leader is in `AZ1`
the resulting configuration is unsafe, even after `AZ3` comes back.
2. if `AZ2` dies, three replicas including the leader are online, so the group can
 actually tolerate one more non-leader outage (before it stops being able to make progress).
3. if `AZ1` dies, progress can still be made, though a new leader can not be elected.
However, this is not necessary since the leader `3` was not in `AZ1`.

After an AZ failure resolves, the group may end up in a vulnerable configuration
since we won't be able to control the location of a potential new leaseholder,
so it may end up being colocated with the doubly-occupied `AZ`.

Nevertheless, all in all, this seems worth considering. We'd need to either
transfer leaders or exclude the AZ containing the leader as a target for adding
an even-numbered replica (under some additional constraint on the replication
factor).

## Unresolved questions

Determine whether the FPaxos solution outlined above can be sufficient.
