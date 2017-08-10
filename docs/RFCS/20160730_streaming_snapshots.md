- Feature Name: streaming_snapshots
- Status: in-progress
- Start Date: 2016-07-30
- Authors: bdarnell
- RFC PR: [#8151](https://github.com/cockroachdb/cockroach/pull/8151)
- Cockroach Issue: [#7551](https://github.com/cockroachdb/cockroach/issues/7551)

# Summary

This RFC proposes sending raft snapshots via a new streaming protocol,
separate from regular raft messages. This will provide better control
of concurrent snapshots and reduce peak memory usage.

# Motivation

`etcd/raft` transmits snapshots as a single blob, all of which is held
in memory at once (several times over, due to the layers of encoding).
This forces us to limit range sizes to a small fraction of available
memory so that snapshot handling does not exhaust available memory.

Additionally, `etcd/raft` does not give us much control over when
these snapshots are sent, and despite our attempts to limit concurrent
snapshot use (including throttling in the `Snapshot()` method itself
and reservations in the replication queue), it is likely for multiple
snapshots to be sent around the same time, amplifying memory problems.

Finally, our current raft transport protocol is based on asynchronous
messaging, making it difficult for the sender of a message to know
that it has been processed and a new message can be sent.

The changes proposed in this RFC will
- Allow nodes to signal whether or not they are able to accept a
  snapshot before it is sent
- Inform the sender of a snapshot when it has been applied
  successfully (or failed)
- Allow snapshots to be applied in chunks instead of all at once

# Detailed design

These changes will be implemented in two phases. In the first phase,
we introduce the new network protocol and use it to ensure that both
senders and receivers can limit the number of snapshots they are
processing at once. In the second phase we modify the `applySnapshot`
method to be aware of the streaming protocol and process the snapshots
in smaller chunks.

## Network protocol

We introduce a new streaming RPC in the `MultiRaft` GRPC service:

``` protocol-buffer
message SnapshotRequest {
  message Header {
    optional roachpb.RangeDescriptor range_descriptor = 1 [(gogoproto.nullable) = false];

    // The inner raft message is of type MsgSnap, and its snapshot data contains a UUID.
    optional RaftMessageRequest raft_message_request = 2 [(gogoproto.nullable) = false];

    // The estimated size of the range, to be used in reservation decisions.
    optional int64 range_size = 3 [(gogoproto.nullable) = false];

    // can_decline is set on preemptive snapshots, but not those generated
    // by raft because at that point it is better to queue up the stream
    // than to cancel it.
    optional bool can_decline = 4 [(gogoproto.nullable) = false];
  }

  optional Header header = 1;

  // A RocksDB BatchRepr. Multiple kv_batches may be sent across multiple request messages.
  optional bytes kv_batch = 2 [(gogoproto.customname) = "KVBatch"];

  // These are really raftpb.Entry, but we model them as raw bytes to avoid
  // roundtripping through memory. They are separate from the kv_batch to
  // allow flexibility in log implementations.
  repeated bytes log_entries = 3;

  optional bool final = 4 [(gogoproto.nullable) = false];
}

message SnapshotResponse {
  enum Status {
    ACCEPTED = 1;
    APPLIED = 2;
    ERROR = 3;
    DECLINED = 4;
  }
  optional Status status = 1 [(gogoproto.nullable) = false];
  optional string message = 2 [(gogoproto.nullable) = false];
}

service MultiRaft {
  ...
  rpc RaftSnapshot (stream SnapshotRequest) returns (stream SnapshotResponse) {}
}
```

The protocol is inspired by HTTP's `Expect: 100-continue` mechanism.
The sender creates a `RaftSnapshot` stream and sends a
`SnapshotRequest` containing only a `Header` (no other message
includes a `Header`). The recipient may either accept the snapshot by
sending a response with `status=ACCEPTED`, reject the snapshot
permanently (for example, if it has a conflicting range) by sending a
response with `status=ERROR` and closing the stream, or stall the
snapshot temporarily (for example, if it is currently processing too
many other snapshots) by doing nothing and keeping the stream open.
The recipient may make this decision either using the reservation
system or by a separate store-wide counter of pending snapshots.

When the snapshot has been accepted, the sender sends one or more
additional `SnapshotRequests`, each containing KV data and/or log
entries (no log entries are sent before the last KV batch). The last
request will have the `final` flag set. After receiving a `final`
message, the recipient will apply the snapshot. When it is done, it
sends a second response, with `status=APPLIED` or `status=ERROR` and
closes the stream.

## Sender implementation

When a snapshot is required a multi-step interaction takes place
between `etcd/raft` and the `Replica`. First, raft calls
`replica.Snapshot()` to generate and encode the snapshot data (along
with some metadata). Second, the `Ready` struct will include an
outgoing `MsgSnap` containing that data and the recipient's ID. Since
the `Snapshot()` call does not say where the snapshot is to be sent,
some indirection is necessary.

`Replica.Snapshot` will generate a UUID and capture a RocksDB
snapshot. The UUID is returned to raft as the contents of the snapshot
(along with the metadata required by raft). The UUID and RocksDB
snapshot are saved in attributes of the `Replica`. In
`sendRaftMessage`, we inspect all outgoing `MsgSnap` messages. If
it doesn't match our saved UUID, we discard the message (this
shouldn't happen). If it matches, we begin to send SnapshotRequests as
described above; the `MsgSnap` is held to be sent in the snapshot's
`Header`.

## Recipient implementation

Applying snapshots in a streaming fashion introduces some subtleties
around concurrency, so the initial implementation of streaming
snapshots will continue to apply the snapshot as one unit.

### Phase 1

The recipient will accumulate all `SnapshotRequests` in memory, into a
rocksdb `Batch`, keyed by the UUID from the header `raftpb.Message`.
It sends the header's `MsgSnap` into raft, and if `raft.Ready` returns
a snapshot to be applied with the given UUID (this is not guaranteed),
the buffered snapshot will be committed.

### Phase 2

In phase 2, chunks of the snapshot are applied as they come in,
instead of a single RocksDB batch. Because this leaves the replica in
a visibly inconsistent state, it cannot be used for anything else
during this process.

In this mode, the `MsgSnap` is sent to raft at the beginning of the
process instead of the end. If raft tells us to apply the snapshot, we
destroy our existing data to make room for the snapshot. Once we have
done so, we cannot do anything else with this replica (including
sending any raft messages, especially the `MsgAppResp` that raft asks
us to send when it gives us the snapshots) until we have consumed and
applied the entire stream of snapshot data.

Error handling here is tricky: we've already discarded our old data,
so we can't do anything else until we apply a snapshot. If the stream
is closed without sending the final snapshot packet, we must mark the
replica as corrupt.

# Drawbacks

- More complexity
- Phase 2 introduces new sources of replica corruption errors
- More exposure to raft implementation details

# Alternatives

- Upstream changes to the `raft.Storage` interface (to pass the
  recipient ID to the `Snapshot` method) could simplify things a bit
  on the sender side.

# Unresolved questions

- Can this replace the reservation system, or do we need both? It
  should probably be integrated, but they're not quite the same - for
  preemptive snapshots we want to return `DECLINED` so the replication
  queue can pick another target, but raft-generated snapshots cannot
  be declined (since they cannot be retried elsewhere) and instead
  should be queued up until space is available.
- It should be possible to recover from a failed snapshot by receiving
  a new snapshot without marking the replica as corrupt. What would be
  required to make this work?
