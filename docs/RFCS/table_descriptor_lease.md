- Feature Name: table_descriptor_lease
- Status: draft
- Start Date: 2015-10-09
- RFC PR: [#2810](https://github.com/cockroachdb/cockroach/pull/2036)
- Cockroach Issue: [#2036](https://github.com/cockroachdb/cockroach/issues/2036)

# Summary

Implement a table descriptor lease service to allow safe usage of
cached table descriptors.

# Motivation

Table descriptors contain the schema for a single table and are
utilized by nearly every SQL operation. Fast access to the table
descriptor is critical for good performance. Reading the table
descriptor from the KV store on every operation adds significant
latency.

Table descriptors are currently distributed to every node in the
cluster via gossipping of the system config (see
[schema_distribution](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/schema_gossip.md)). Unfortunately,
it is not safe to use these gossipped table descriptors in almost any
circumstance. Consider the statements:

```sql
  CREATE TABLE test (key INT PRIMARY KEY, value INT);
  CREATE INDEX foo ON test (value);
  INSERT INTO test VALUES (1, 2);
```

Depending on when the gossip of the schema change is received, the
`INSERT` statement may either see no cached descriptor for the `test`
table (which is safe because it will fall back to reading from the KV
store), the descriptor as it looks after the `CREATE TABLE` statement
or the descriptor as it looks after the `CREATE INDEX` statement. If
the `INSERT` statement does not see the `CREATE INDEX` statement then
it will merrily insert the values into `test` without creating the
entry for the `foo` index, thus leaving the table data in an
inconsistent state.

It is easy to show similar problematic sequences of operations for
`DELETE`, `UPDATE` and `SELECT`. Fortunately, a solution exists in the
literature: break up the schema change into discrete steps for which
any two consecutive versions of the schema are safe for concurrent
use. For example, to add an index to a table we would perform the
following steps:

* Add the index to the table descriptor but mark it as delete-only:
  update and delete operations which would delete entries for the
  index do so, but insert and update operations which would add new
  elements and read operations do not use the index.
* Wait for the table descriptor change to propagate to all nodes in
  the cluster and all uses of the previous version to finish.
* Mark the index as write-only: insert, update and delete operations
  would add entries for the index, but read operations would not use
  the index.
* Wait for the table descriptor change to propagate to all nodes in
  the cluster and all uses of the previous version to finish.
* Backfill the index.
* Mark the index in the table descriptor as read-write.
* Wait for the table descriptor change to propagate to all nodes in
  the cluster and all uses of the previous version to finish.

This RFC is focused on how to wait for the table descriptor change
propagate to all nodes in the cluster. Additionally, we need
mechanisms to ensure that multiple updates to a table descriptor are
not performed concurrently and that write operations never commit once
a table descriptor is too old (i.e. not one of the two most recent
versions).

At a high-level, we want to provide read and write leases for table
descriptors. When a schema change is to be performed, the operation
first needs to grab a write lease for the descriptor. The write lease
ensures that no concurrent schema change will be performed and that
there is only one active version of the descriptor in the cluster.
Note that these are essentially the same constraint: there are only
two active versions of a descriptor during a schema change.

Before using a table descriptor for a DML operation (i.e. `SELECT`,
`INSERT`, `UPDATE`, `DELETE`, etc), the operation needs to obtain a
read lease for the descriptor. The lease will be guaranteed valid for
a significant period of time (on the order of minutes) and the lease
period can be extended as long as a scheme change is not
performed. When the operation completes it will release the read
lease.

# Detailed design

The lease for a table will be managed and dispensed by a per-table
lease service. The node running the lease service will be the raft
leader of the range containining the first key in the table
(i.e. `/<tableID>`). The lease master for a table will provide the
following service:

```proto
message WatchRequest {
  optional int32 nodeID;
  optional int32 descID;
  optional int32 version;
}

message WatchResponse {
  enum State {
    CURRENT
    DRAINING
    DEAD
  }
  optional int32 descID;
  optional int32 version;
  optional State state;
}

message AcquireRequest {
  optional int32 nodeID;
  optional int32 descID;
  optional int32 version;
}

message AcquireResponse {
  optional message<Error> error;
  optional message<TableDescriptor> desc;
  optional int64 expiration;
}

message ReleaseRequest {
  optional int32 nodeID;
  optional int32 descID;
  optional int32 version;
  optional bool all;  // Release all leases held by the node.
}

message ReleaseResponse {
  optional message<Error> error;
}

message PublishRequest {
  optional message<TableDescriptor> desc;
}

message PublishResponse {
  optional message<Error> error;
}

service TableLeaseService {
  // Watch registers the node for updates to descriptor versions. A
  // watch should be registered before any other lease operation.
  rpc Watch(WatchRequest) returns (WatchResponse) {}
  
  // Acquire acquires a read lease for the specified descriptor and
  // version. The lease is associated with the specified node. An
  // error will be returned if the specified version is not the most
  // recent version for the descriptor.
  rpc Acquire(AcquireRequest) returns (AcquireResponse) {}

  // Release allows a read lease to be released before it
  // expires. This should be used when a watch request indicates a new
  // version of a descriptor is available and the lease holder
  // determines it is no longer using the old version.
  rpc Release(ReleaseRequest) returns (ReleaseResponse) {}

  // Publish publishes a table descriptor change, writing the table
  // descriptor to the KV store on success and notifying all watches
  // of the new version. An error is returned if the version of the
  // table descriptor is not equal to the current version + 1. An
  // error is returned if there are two active versions of the table
  // descriptor.
  rpc Publish(PublishRequest) returns (PublishResponse) {}
}
```

Each node will maintain a map from `<descriptorID, version>` to
`<TableDescriptor, expiration, localRefCount>`. When the node needs to
use a descriptor for an operation it will look in its local map. If
the descriptor is not there the node will register a `Watch` with the
lease master in order to receive a stream of version updates for the
table descriptor. Next it will send an `Acquire` message to the lease
master which will return the desired `TableDescriptor` along with the
expiration of the lease. The node will maintain the invariant that a
new reference to a descriptor will only be added to the most recent
version. When the reference count for an older version falls to 0 the
node will send a `Release` message to the lease master. The local
reference count will be incremented when a transaction first uses a
table and decremented when the transaction commits/aborts.

A `Watch` request will watch for activity to a particular version of a
descriptor. The watch request will wait until the state of the
specified version has changed. A node can use a watch request to be
notified when a new version of a descriptor is created by watching for
the next descriptor version. And a schema change operation can watch
for a descriptor version to become `DEAD` (i.e. have no leases on it).

When performing a schema change operation, the modification to the
table descriptor is performed using the `Publish` operation which will
fail if the version is invalid (i.e. too old) or if there are already
two active versions of the descriptor in the cluster. On success, the
`Publish` operation will write the new descriptor to the KV store and
notify all watches of the new descriptor which will cause those nodes
to stop using the old descriptor for new operations. As existing
operations complete they will release their local reference to the old
descriptor which will cause the reference count to drop to 0 and
result in the lease being released. When all of the leases for the old
descriptor have been released, the lease master will send out a
notification to the watches that the version is `DEAD`.

Leases will be granted for a duration measured in minutes (we'll
assume 5m for the rest of this doc, though experimentation may tune
this number). A node will renew recently used descriptors before their
leases expires and ensure that when handing out a descriptor for local
usage the lease will not expire for at least `<leaseDuration>/2`
minutes.

Lease state will be stored in a `leaseTable`:

```sql
CREATE TABLE leaseTable (
  DescID     INT,
  Version    INT,
  NodeID     INT,
  Expiration TIMESTAMP,
  PRIMARY KEY (DescID, Version, NodeID)
)
```

Entries in the lease table will be added and removed as leases are
acquired and released. A background goroutine will periodically delete
expired leases. The master will maintain the invariant that for a
given descriptor there are only two active versions (i.e. non-expired
versions) in the lease table and that the two versions correspond to
the table descriptor in the KV store and its immediate predecessor.

When the lease master is unavailable (because it has died and a new
master has not yet started), new leases cannot be granted but existing
leases remain valid until they expire.

When a node starts it will scan the `leaseTable` to find out what
leases were held by its previous incarnation. The newly started node
cannot know if any transactions are still being committed that are
using those leases (i.e. the `EndTransation` was sent but is stil
winding its way through raft committal), so it will ensure that those
leases are not released until they expire.

When a node holding leases dies permanently or becomes unresponsive
(e.g. detached from the network) the lease master will have to wait
for any leases that node held to expire. This will result in an
inability to perform more than one schema modification step to the
descriptors referred to by those leases. The maximum amount of time
this condition can exist is the lease duration.

# Drawbacks

* This is complex. Perhaps overly complex. Where can it be simplified?

* The watch mechanism is described with the assumption of streaming
  responses. Unless we adopt gRPC sooner, we'll have to fallback to
  some sort of cursor mechanism.

# Alternatives

* We could use an existing lock service such as etcd or Zookeeper. The
  primary benefit would be the builtin watch functionality. We would
  still need the logic for local reference counts.

* The lease master could avoid storing the lease state, but we would
  have to wait for the lease duration at master startup before
  allowing any schema modifications. There might be some hacks around
  this. For example, The master can persist only the largest lease
  expiration and wait until that expiration passes before allowing
  schema modifications. On the other hand, persisting the lease state
  allows inspection of it via the SQL console.

* Keeping track of local references to descriptor versions in order to
  early release leases adds complexity. We could just wait for leases
  to expire, though that would cause a 3-step schema modification to
  take at least 10m to complete.

* We could avoid having a lease master. Nodes would add/remove leases
  directly to/from the `leaseTable`. Publish would require some sort
  of conditional put on the new version. Watching would be replaced by
  polling. While this seems feasible, it also seems quite a bit more
  subtle than a centralized coordinator for leases for a table.

* We could continue to distribute descriptors using gossip and add
  some sort of high-priority flag to indicate that we want to skip the
  current 2s/hop delay in gossip. This would replace the watch
  mechanism for discovery of new descriptors, though we would still
  need a watch mechanism for discovery of `DEAD` descriptors.

# Unresolved questions

* We need a mechanism to enforce that write operations do not commit
  if they reference a table descriptor lease which has expired. The F1
  paper talks about a "write fence" mechanism in spanner which allows
  a transaction to specify a time before which the transaction needs
  to commit otherwise it should be aborted.

* Is `leaseDuration/2` the right minimum time to give an operation for
  usage of a lease? Note that the maximum time is unbounded if the
  node can successfully extend the lease duration during the
  operation. If the lease can't be extended (e.g. because of a
  concurrent schema modification) then the operation will need to be
  aborted. So the question becomes how common are operations that will
  take longer than `leaseDuration/2` and how problematic is it if they
  are aborted?

* How to direct RPCs to the leader for a particular range? The table
  leasing code is SQL specific, but all existing special handling for
  leader operations happens in `storage`.

* Do we have to worry about clock skew? The expiration times are
  significantly above expected cluster clock offsets.

* This RFC doesn't address the full complexity of table descriptor
  schema changes. For example, when adding an index the node
  performing the operation might die while backfilling the index
  data. We'll almost certainly want to parallelize the backfilling
  operation. And we'll want to disallow dropping an index that is
  currently being added. These issues are considered outside of the
  scope of this RFC.
