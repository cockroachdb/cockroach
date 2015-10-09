- Feature Name: table_descriptor_lease
- Status: draft
- Start Date: 2015-10-09
- RFC PR:
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
  the cluster.
* Mark the index as write-only: insert, update and delete operations
  would add entries for the index, but read operations would not use
  the index.
* Wait for the table descriptor change to propagate to all nodes in
  the cluster.
* Backfill the index.
* Mark the index in the table descriptor as read-write.
* Wait for the table descriptor change to propagate to all nodes in
  the cluster.

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

Leases will be managed and dispensed by a centralized lease
service. Initially there will be a single lease service master, but a
future enhancement to shard by descriptor ID should be
straightforward.

The lease master will be "elected" by performing a conditional put to
a known key in the KV store. The value of the key will be the node ID
of the master concatened with an expiration timestamp:

```
  /lease-master -> /<nodeID>/<expiration>
```

When a node starts up it will read the `/lease-master` key and see if
there is an active master. If the expiration time is in the past the
node will attempt to become the master itself by conditionally putting
the key. A background goroutine will periodically check to make sure
the existing master is still active and attempt to become the master
if it is not. The lease master itself will periodically refresh a
comfortable amount of time before it expires. If we set the expiration
time to 20s in the future, the master might refresh its status every
10s.

The lease master will provide the following service for read leases:

```proto
message WatchRequest {
  optional int32 nodeID;
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
  rpc Watch(WatchRequest) returns (stream WatchResponse) {}
  
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

When a node starts up it will register a `Watch` with the lease master
in order to receive a stream of version updates for all
descriptors. Each node will maintain a map from `<descriptorID,
version>` to `<TableDescriptor, expiration, localRefCount>`. When the
node needs to use a descriptor for an operation it will look in its
local map. If the descriptor is not there it will be read from the KV
store and a lease will be acquired by sending an `Acquire` message to
the lease master. The node will maintain the invariant that a new
reference to a descriptor will only be added to the most recent
version. When the reference count for an older version falls to 0 the
node will send a `Release` message to the lease master.

The `Watch` responses will notify the node when a new version of a
descriptor is available which it will then read from the KV store if
the node has a lease for the descriptor.

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
minutes. When a node starts it will issue a `Release` message with the
`all` field set to true to indicate it wants to release any leases
held by its previous incarnation. This will ensure that a transient
failure does not require the lease expiration to occur before allowing
a schema modification. When a node dies permanently or becomes
unresponsive (e.g. detached from the network) the lease master will
have to wait for any leases that node held to expire. This will result
in an inability to perform more than one schema modification step to
the descriptors referred to by those leases. The maximum amount of
time this condition can exist is the lease duration.

If the lease master dies a new master will be "elected" within
20s. Lease state will be stored in a `leaseTable` that will be read
when the master starts:

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

# Drawbacks

* This is complex. Perhaps overly complex. Where can it be simplified?

* The watch mechanism is described with the assumption of streaming
  responses. Unless we adopt gRPC sooner, we'll have to fallback to
  some sort of cursor mechanism.

# Alternatives

* The master could avoid storing the lease state, but we would have to
  wait for the lease duration at master startup before allowing any
  schema modifications. There might be some hacks around this. For
  example, if the master starts up and it is the first master (because
  no `/lease-master` key existed) then it can avoid
  waiting. Alternately, the master can persist only the largest lease
  expiration and wait until that expiration passes before allowing
  schema modifications. On the other hand, persisting the lease state
  allows inspection of it via the SQL console.

* Keeping track of local references to descriptor versions in order to
  early release leases adds complexity. We could just wait for leases
  to expire, though that would cause a 3-step schema modification to
  take at least 10m to complete.

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

* Do we have to worry about clock skew? The expiration times are
  significantly above expected cluster clock offsets.

* This RFC doesn't address the full complexity of table descriptor
  schema changes. For example, when adding an index the node
  performing the operation might die while backfilling the index
  data. We'll almost certainly want to parallelize the backfilling
  operation. And we'll want to disallow dropping an index that is
  currently being added. These issues are considered outside of the
  scope of this RFC.
