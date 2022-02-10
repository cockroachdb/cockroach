# Up-replicating replicas with snapshots

This tech note briefly explains the end-to-end process of up-replicating a range, starting from the replicateQueue to
sending Raft snapshots. This is meant to serve as a high level overview of the code paths taken, and more detailed
descriptions can be found in tech notes specific to each area.

## Introduction

Raft snapshots are necessary when a follower replica is unable to catch up using the existing Raft logs of the leader.
Such a scenario occurs when a configuration change or rebalancing operation requires a new follower to be added. For
example, in the case where the replication factor is increased, the Raft Group would need to create a new replica from a
clean slate. To up-replicate this replica, we need to send it an initial snapshot of the full range data.

In this note, we will focus on the scenario where a preemptive snapshot is needed for the up-replication and rebalancing
of replicas.

## ReplicateQueue

As a brief overview, the replicateQueue manages a queue of replicas and is responsible for processing replicas for
rebalancing. For each replica, it decides whether any placement changes are needed and subsequently invokes the
allocator to compute decisions to repair its range. Then it calls changeReplicas to act on these change requests and
repeats until all changes are complete. Note that the ReplicateQueue only acts on ranges for which the local store is
the current leaseholder.

## ChangeReplicas

The ChangeReplicas method is mainly responsible for atomically changing replicas of a range. These changes include
replica promotions, voter/non-voter swaps, additions and removals. The change is performed in a distributed transaction
and takes effect when the transaction is committed. There are many details involved in these changes, but for the
purposes of this note, we will focus on up-replication and the process of sending Raft learner snapshots.

During up-replication, ChangeReplicas runs a transaction that adds all new replicas as learner replicas to the range.
Learner replicas are new replicas that receive Raft traffic but do not participate in quorum; allowing the range to
remain highly available during the replication process.

Once these learners are initialized in the range, they are synchronously sent a Raft snapshot from the leader to
up-replicate. There is some nuance here as Raft snapshots are typically automatically sent by the existing Raft snapshot
queue. However, we are synchronously sending a snapshot here in the replicateQueue to quickly catch up learners. To
prevent a race with the raftSnapshotQueue, we lock learner snapshots on the current node while processing the change.

## Sending Snapshots

The snapshot process itself is broken into three parts: generating, transmitting and applying the snapshot. This process
is the same whether it is invoked by the replicateQueue or raftSnapshotQueue.

### Generating the snapshot

A snapshot is a bulk transfer of all replicated data in a range and everything the replica needs to be a member of a
Raft group. It consists of a consistent view of the state of some replica of a range as of an applied index. The
GetSnapshot method gets a storage engine snapshot of the replica data at a moment in time and creates an iterator from
the storage snapshot.

### Transmitting the snapshot

The snapshot transfer is sent through a bi-directional stream of snapshot requests and snapshot responses. However,
before the actual range data is sent, the streaming rpc first sends a header message to the recipient store and blocks
until the store responds to accept or reject the snapshot data. Once accepted, the sender proceeds to use the iterator
to read and send snapshot data in streaming grpc message chunks. Note that snapshots are rate limited depending on
cluster settings as to not overload the network. Finally, once all the data is sent, the sender sends a final message to
the receiver and waits for a response from the recipient indicating whether the snapshot was a success.

### Applying the snapshot

Once the snapshot is received on the receiver, defensive checks are applied to ensure the correct snapshot is received.
The key-value pairs are used to construct multiple SSTs for direct ingestion. It ensures there is an initialized learner
replica or creates a placeholder to accept the snapshot. Finally, the Raft snapshot message is handed to the replica’s
Raft node, which will apply the snapshot. The placeholder or learner is then converted to a fully initialized replica. 
