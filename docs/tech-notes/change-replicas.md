# Up-replicating replicas with snapshots

This tech note briefly explains the end-to-end process of up-replicating a range, starting from the `replicateQueue` to
sending Learner snapshots. This is meant to serve as a high level overview of the code paths taken, and more detailed
descriptions can be found in tech notes specific to each area.

## Introduction

Raft snapshots are necessary when a follower replica is unable to catch up using the existing Raft logs of the leader.
Such a scenario occurs when an existing replica falls too far behind the rest of the Raft group such that we've
truncated the Raft log above it. On the other hand, Learner snapshots are necessary when we add a new replica (due to a
rebalancing operation) and the new replica needs a snapshot to up-replicate. We should make a small distinction
that `Learner snapshots` are sent during rebalancing operations by the `replicateQueue` and `Raft snapshots`
are sent by the `raftSnapshotQueue` when a replica falls behind. However, there is no difference in the way the
snapshots are sent, generated or applied. For example, in the case where the replication factor is increased, the Raft
group would need to create a new replica from a clean slate. To up-replicate this replica, we would send it a Learner
snapshot of the full range data.

In this note, we will focus on the scenario where a Learner snapshot is needed for the up-replication and rebalancing of
replicas.

## ReplicateQueue

As a brief overview, the `replicateQueue` manages a queue of replicas and is responsible for processing replicas for
rebalancing. For each replica that holds the lease, it invokes the allocator to compute decisions on whether any placement changes are needed
and the `replicateQueue` executes these changes. Then the `replicateQueue` calls `changeReplicas` to act on these change
requests and repeats until all changes are complete. 

## ChangeReplicas API

The `AdminChangeReplicas` API exposed by KV is mainly responsible for atomically changing replicas of a range. These
changes include non-voter promotions, voter/non-voter swaps, additions and removals. The change is performed in a
distributed transaction and takes effect when the transaction is committed. There are many details involved in these
changes, but for the purposes of this note, we will focus on up-replication and the process of sending Learner
snapshots.

During up-replication, `ChangeReplicas` runs a transaction to add a new learner replica to the range. Learner
replicas are new replicas that receive Raft traffic but do not participate in quorum; allowing the range to remain
highly available during the replication process.

Once the learners have been added to the range, they are synchronously sent a Learner snapshot from the leaseholder
(which is typically, but not necessarily, the Raft leader) to up-replicate. There is some nuance here as Raft snapshots
are typically automatically sent by the existing Raft snapshot queue. However, we are synchronously sending a snapshot
here in the replicateQueue to quickly catch up learners. To prevent a race with the raftSnapshotQueue, we lock snapshots
to learners and non-voters on the current leaseholder store while processing the change. In addition, we also place a
lock on log truncation to ensure we don't truncate the Raft
log while a snapshot is inflight, preventing wasted snapshots. However, both of these locks are the best effort as
opposed to guaranteed as the lease can be transferred and the new leaseholder could still truncate the log.

## Sending Snapshots

The snapshot process itself is broken into three parts: generating, transmitting and applying the snapshot. This process
is the same whether it is invoked by the `replicateQueue` or `raftSnapshotQueue`.

### Generating the snapshot

A snapshot is a bulk transfer of all replicated data in a range and everything the replica needs to be a member of a
Raft group. It consists of a consistent view of the state of some replica of a range as of an applied index. The
`GetSnapshot` method gets a storage engine snapshot which reflects the replicated state as of the applied index the
snapshot is generated at, and creates an iterator from the storage snapshot. A storage engine snapshot is important to
ensure that multiple iterators created at different points in time see a consistent replicated state that does not
change while the snapshot is streamed. In our current code, the engine snapshot is not necessary for correctness as only
one iterator is constructed.

### Transmitting the snapshot

The snapshot transfer is sent through a bi-directional stream of snapshot requests and snapshot responses. However,
before the actual range data is sent, the streaming rpc first sends a header message to the recipient store and blocks
until the store responds to accept or reject the snapshot data. 

The recipient checks the following conditions before accepting the snapshot. We currently allow one concurrent snapshot
application on a receiver store at a time. Consequently, if there are snapshot applications already in-flight on the
receiver, incoming snapshots are throttled or rejected if they wait too long. The receiver then checks whether its store
has a compatible replica present to have the snapshot applied. In the case of up-replication, we expect the learner
replica to be present on the receiver store. However, if the snapshot overlaps an existing replica or replica
placeholder on the receiver, the snapshot will be rejected as well. Once these conditions have been verified, a response
message will be sent back to the sender.

Once the recipient has accepted, the sender proceeds to use the iterator to read chunks of
size `kv.snapshot_sender.batch_size` from storage into memory. The batch size is enforced as to not hold on to a
significant chunk of data in memory. Then, in-memory batches are created to send the snapshot data in streaming grpc
message chunks to the recipient. Note that snapshots are rate limited depending on cluster settings as to not overload
the network. Finally, once all the data is sent, the sender sends a final message to the receiver and waits for a
response from the recipient indicating whether the snapshot was a success. On the receiver side, the key-value pairs are
used to construct multiple SSTs for direct ingestion, which prevents the receiver from holding the entire snapshot in
memory.

### Applying the snapshot

Once the snapshot is received on the receiver, defensive checks are applied to ensure the correct snapshot is received.
It ensures there is an initialized learner replica or creates a placeholder to accept the snapshot. Finally, the Raft
snapshot message is handed to the replica’s Raft node, which will apply the snapshot. The placeholder or learner is then
converted to a fully initialized replica. 
