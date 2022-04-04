- Feature Name: Inconsistent Recovery from Permanent Loss of Quorum
- Status: accepted
- Start Date: 2021-09-23
- Authors: Tobias Grieger, Oleg Afanasyev
- RFC PR: [#72527](https://github.com/cockroachdb/cockroach/pull/72527)
- Cockroach Issue: https://github.com/cockroachdb/cockroach/issues/17186

# Motivation and Summary

Our story around recovering from loss of quorum is officially nonexistent (“restore from a backup into a new cluster” is
the official guidance), and yet internal tools exist and are used with some regularity. Our customers (internal and
external) are understandably asking for official tools and guidelines. Even if it may be hard, at least in the 22.1
timeframe, to improve our tooling to be suitable for end users, significant improvements are possible.

We propose a plan to productionize our tooling for recovery from loss of quorum for use in situations in which operators
want to restore service and allow as much data to be recovered, without regard for its consistency. The plan revolves
around extensions of `cockroach debug unsafe-remove-dead-replicas` that primarily improve the UX, detect (& avoid doing
harm in) situations in which the tool cannot be used, and give an indication of the internal state of the cluster
expected after the recovery operation.

We will have an official, documented, and well-tested ergonomical tool that customers both internal and external can
use. This will fortify our story around high availability. The tools will form a solid foundation for improved
functionality, such as making the process “push-button” against the running cluster. There is also a possibility to
reuse parts of the suggested machinery to build a tool to roll back a production cluster to a known consistent state.

# Technical design

## Status Quo

A quick reminder about the status quo. The recovery protocol is:

* Determine the dead nodes A, B, …
* Stop all nodes in the cluster
* Invoke `./cockroach debug unsafe-remove-dead-replicas --dead-store-ids=A,B,...` on each store in the system
* Restart the cluster.

`unsafe-remove-dead-replicas` iterates through all Range Descriptors on the store. It then makes the (unjustified, but
often enough correct) assumptions that for any Range needing recovery,

* **(AllLive)** Any Replica found on any store is live (i.e. not waiting for replicaGC)
* **(Covering)** There is a replica for each range that needs recovery, i.e. we can cover the entire keyspace.
* **(ConsistentRangeDescriptor)** All Replicas of a given Range (across multiple stores) have the same view of the range
  descriptor.
* **(RaftLog)** the surviving replicas’ applied state is not “missing” any committed operations that affect the
  RangeDescriptor. (Such a missing operation could be in the follower’s log but be unapplied, or the follower could not
  even have learned about it).

On each store, based on **(ConsistentRangeDescriptor)**, the process can deterministically (across all stores) determine
from the RangeDescriptor (and the input set of lost nodes) the ranges that need recovery and choose per-range designated
survivors. At present, it chooses from the surviving Replicas than which is the voter with the highest StoreID. On this
Replica, it rewrites the Replica Descriptor in-place to contain only that voter as its sole member. When that store then
comes back online, and the system ranges of the cluster are functional, that range can thus make progress. It will
up-replicate and in doing so fix up the meta2 entries (which initially reflect the outdated range descriptor). Fixing up
the meta2 record will lead to any additional non-chosen survivors to become eligible for replicaGC.

The above process is how the tool was originally conceived of. We realized that it can also be used in a rolling-restart
fashion. The upshot of that strategy is that it avoids downtime of the entire cluster in cases where only a few ranges
need recovery. In that case, the strategy is as follows:

* Decide which range to recover first. This doesn’t matter much if all ranges are user ranges, but if one of them is a
  critical system range, that one needs to come first - as availability of system ranges is a prerequisite for rejoining
  the cluster. (For example, if the liveness range is down, the cluster is effectively down and restarted nodes won’t
  get past the boot sequence; recovering the liveness range is going to be the most important action)
* Take down the node which is going to be the designated survivor for the chosen range (i.e. voter with highest storeID)
  and run the tool on it.
* Restart that node. The chosen range should now be available again and upreplicate.
* Repeat the process until all ranges are recovered.

A caveat observed with this approach has been that if there are multiple replicas remaining for a range (as likely the
case with system ranges, which prefer 5x replication), bringing the designated survivor back does not necessarily
restore availability for that range. This is because the non-designated survivors are still “around” initially and are
present both in the meta2 records and cached in the DistSenders. Any requests routed to them will hang (or, once we
have https://github.com/cockroachdb/cockroach/issues/33007, fail-fast), which requires targeted additional restarts of
these non-designated survivors to unblock service. In short, the rolling-restart strategy works, but it’s very manual
and in the worst case causes longer downtime than the stop-the-world approach.

## Step 1: Improved Offline Approach

We first fortify the stop-the-world approach for safety and improved choice of designated survivors.

### Data Collection

We pointed out above that in the status quo, lots of assumptions are made about the state being consistent across the
various stores, which allowed for local decision-making. This assumption is obviously not going to hold in general, and
the possible problems arising from this make us anxious about proliferation of usage of the tool. We propose to add an
explicit distributed data collection step that allows picking the “best” designated survivor and that also catches
classes of situations in which the tool could cause undesired behavior. When such a situation is detected, the tool
would refuse to work, and an override would have to be provided; one would be in truly unchartered territory in this
case, with no guarantees given whatsoever. Over time, we can then evolve the tool to handle more corner cases.

The distributed data collection step is initially an offline command, i.e. a
command `./cockroach debug recover collect-info -store=<store>` is run directly against the store directories. Much of
the current code in `unsafe-remove-dead-replicas` is reused: an iteration over all range descriptors takes place, but
now information about all of the replicas is written to a file or printed to stdout.

This includes:
- Current StoreID
- RangeDescriptor
- RangeAppliedIndex
- RaftCommittedIndex
- Whether the unapplied Raft log contains any changes to the range descriptor.

These files are collected in a central location, and we proceed to the next step:

### Making a Plan

`./cockroach debug recover make-plan <files>` is invoked. This

* Prunes the set of input replicas to be generationally consistent. For example, if we “only” have a replica r1=[a,z) at
  generation 5 and a replica r2=[c,z) at generation 7, we have to discard r1 (since r1@gen5 is an old replica; it must
  have split, perhaps multiple times, to become a left hand side that touches r2). This leaves a hole [a,c) in the
  keyspace, i.e. would fail the next step.  
  Note that this step also eliminates duplicate survivors whose range descriptor does not agree. For example, if we have
  a replica r1=[a,z)@gen5 and r1=[a,z)@gen7, we’ll eliminate the first. Note that this is an example of a general class
  of scenarios in which a split happened that the survivors don’t know about; in this case the right-hand side might
  exist in the cluster (and may not even need recovery). We thus detect this and will fail below, due to a hole in the
  keyspace.  
  This step ensures that *(AllLive)* and *(ConsistentRangeDescriptor)* are true for the remaining set of replicas.
* For all ranges that have multiple replicas surviving, pick one with the highest RaftCommittedIndex, ties for
  RaftCommittedIndex would be resolved in favor of highest StoreID. (See “Follow-up Work” for a comment on this)
* Verifies that the remaining set of replicas covers the entire keyspace. Due to the generational consistency, there
  will be no partial overlap, i.e. we have a partition if there are no holes. This verifies **(Covering)**.
* If any of the remaining replicas indicates an unapplied change to the range descriptor in the log, complain. This
  verifies **(RaftLog)**. (We can likely do something more advanced here, but let’s delegate for later).
* Write out this resulting partition to a file; also separately list the replicas that were *not* chosen. This is the
  recovery plan. If there were any problems above, don’t write the plan unless a `--force` flag is specified.

Note that choosing the highest CommittedIndex isn’t necessarily the best option. If we are to determine with full
accuracy the most up-to-date replica, we need to either perform an offline version of Raft log reconciliation (which
requires O(len-unapplied-raft-log) of space in the data collection in the worst case, i.e. a non-starter) or we need to
resuscitate multiple survivors per range if available (but this raises questions about nondeterminism imparted by the
in-place rewriting of the RangeDescriptor). Conceptually it would be desirable to replace the rewrite step with an
“append” to the RaftLog.

### Rewriting

Now it’s time to rewrite range descriptors. The recovery plan is distributed to each node, and on each
store `./cockroach recover apply-plan --store=<store> <plan-file>` is invoked. This first creates an LSM checkpoint,
just in case. Then, for all replicas in the plan for which the local StoreID is chosen as the designated survivor, the
rewrite of the range descriptor is carried out to have the designated survivor as the single voter. All other replicas
of ranges that need recovery but for which the local store’s replica was not chosen are replicaGC’ed; this is done to
prevent these replicas from trying to handle requests once the store rejoins the cluster. In addition to the above step,
we also mark each store as having undergone a recovery procedure, for example by writing a store-local marker key, or by
copying the recovery plan into the store’s auxiliary data directory. This can then be included in debug.zip and metrics
to indicate to our Support team that inconsistent recovery was applied to the cluster should it cause problems in the
future.

## Step 2: Half-Online approach

The stop-the-world approach can be unnecessarily intrusive.

The cluster doesn't have to be stopped for the full duration of all the recovery stages, It only needs to be stopped to
apply a change to the store once unavailable replicas are identified and recovery operations planned. Moreover, since
ranges that lost quorum can't progress anyway, update could be performed in a rolling fashion.

With this in mind, recovery procedure could be automated to:

* Create recovery plan by pointing cli at a single node. The node or the cli itself would do a fanout to collect the
  data. Data is similar to what Ranges endpoint provides and might be even sufficient for replica info collection
  purposes.
  * Note that collection phase should only be using gossip and Ranges RPC or the like so that it could still work when
    KV is unavailable due to system ranges losing quorum.
* Copy plan to each node.
* Operator performs rolling restart of nodes the usual way.

This approach avoids all the error-prone manual steps to collect and distribute data, but also avoids the “harder”
engineering problem of applying a recovery plan to a running cluster.

We keep the fully-offline step since it’s a good fallback; nodes may not even start or connect gossip when things are
sufficiently down.

## Drawbacks

Once unsafe recovery tool is readily available, operators could start using it when not appropriate if it proves
successful. As data inconsistency will not always happen and that would give a sense of false safety.

## Follow-up work

In both Offline and Half-Online approach we could provide a more detailed report of what data is potentially
inconsistent after the recovery. Report would contain data about which tables, indexes and key ranges are affected. E.g.
table foo.bar with keys in range ['a', 'b'). And provide recommendations on how to restore data consistency where
possible. E.g. secondary indices affected by range recovery could be dropped and recreated.

As a next step, we can make the tool fully-online, i.e. automate the data collection and recovery step behind a cli/API
endpoint, thus providing a "panic button" that will try to get the cluster back to an available (albeit possibly
userdata-inconsistent) state from which a new cluster can be seeded. This must not use most KV primitives (to avoid
relying on any parts of the system that are unavailable), instead we need to operate at the level of Gossip node
membership (for routing) and special RPC endpoints. The range report (ranges endpoint) should be sufficient for the data
collection phase with some hardening and possibly inclusion of some additional pieces of information. The active
recovery step can be carried out by a store-addressed recovery RPC that instructs the store to eject the in-memory
incarnations of the designated-survivor-replicas from the Store, replacing them with placeholders owned by the recovery
RPC, which will thus have free reign over the portion of the engines owned by these replicas (and it can thus carry out
the same operations unsafe-remove-dead-replicas would).

We can also explore situations in which stages one and two refuse to carry out the recovery and provides solutions at
least to some of them. However, due to the nature of data loss, almost any internal invariant KV or the SQL subsystem
have can be violated, so a "provably safe" recovery protocol is not the goal.

### Picking the optimal survivor

In the current proposal, in the presence of multiple surviving followers for a range we pick the one with the largest
committed index as the designated survivor. This isn’t always the best option - followers have a “lagging” view of what
is committed, so a more up-to-date follower could have a smaller committed index than another, less up-to-date follower.
Determining the optimal choice requires performing Raft log reconciliation, which requires adding the indexes of term
changes to the output of the data collection step. It seems awkward to duplicate parts of the Raft logic, and so we
caution against taking this step. The CommitedIndex approach will generally deliver near-optimal results. We could
consider picking the largest LastIndex instead. This too has the (theoretical) potential to be the wrong choice: if a
new leader takes over and aborts a large uncommitted log tail, we may pick the follower that has not learned about the
existence of this new leader yet, and would thus pick the divergent log tail, possibly not only missing writes but
outright committing a number of writes that never should have taken effect.

### Best effort consistent recovery to the state in the past

Stage four is a confluence of consistent and inconsistent recovery. By including resolved timestamp information in the
data collection step, we can stage a stop-the-world restart of the cluster which bootstraps a new cluster in-place while
preserving the user data and range split boundaries. (Certain exceptions apply, for example non-MVCC operations may
cause anomalies, and of course each part of the keyspace needs to have at least one replica remaining). This tool also
paves, in principle, a way for backing up and restoring a cluster without any data movement, using (suitably enhanced to
capture some extra state atomically) LSM snapshots.

# Explain it to folk outside of your team

CockroachDB being an ACID compliant database always prioritizes consistency of the data over availability. Internaly we
rely on raft consensus protocol between replicas of the data to achieve low level consistency. This means that if
consensus can't be achieved due to permanent loss of nodes database can not proceed any further. While this is a
desirable property, some clients prefer to relax it for the cases where downtime is unacceptable or inconsistency could
be tolerated temporarily.

This proposal describes possible solution based on the internal approach and ways to make it generally usable and less
error-prone by adding automation and safeguards.

# Unresolved questions

Best way to collect information about replicas in half-online and online approach - should cli fan-out to all nodes as
instructed or should it delegate to a single node to perform fan-out operations. The tradeoff is cli fan-out will
require operator to provide all node addresses to guarantee collection, but that would work in all cases. Maybe we can
do both approaches with cli fan-out being a fallback if we can't collect from all nodes.
