- Feature Name: table_descriptor_lease
- Status: completed
- Start Date: 2015-10-09
- RFC PR: [#2810](https://github.com/cockroachdb/cockroach/pull/2036)
- Cockroach Issue: [#2036](https://github.com/cockroachdb/cockroach/issues/2036)

# Summary

Implement a table descriptor lease mechanism to allow safe usage of
cached table descriptors.

# Motivation

Table descriptors contain the schema for a single table and are
utilized by nearly every SQL operation. Fast access to the table
descriptor is critical for good performance. Reading the table
descriptor from the KV store on every operation adds significant
latency.

Table descriptors are currently distributed to every node in the
cluster via gossipping of the system config (see
[schema_gossip](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/schema_gossip.md)). Unfortunately,
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

This RFC is focused on how to wait for the table descriptor change to
propagate to all nodes in the cluster. More accurately, it is focused
on how to determine when the previous version of the descriptor is no
longer in use. Additionally, we need mechanisms to ensure that
multiple updates to a table descriptor are not performed concurrently
and that write operations never commit once a table descriptor is too
old (i.e. not one of the two most recent versions).

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

The design maintains two invariants:

* Leases can only be granted on the newest version of a
  descriptor. This ensures that once a new version of a descriptor is
  created, no new leases will be granted for the old version.
* There can be at most 2 active versions of a table in the cluster at
  any time.

Table descriptors will be extended with a version number that is
incremented on every change to the descriptor:

```proto
message TableDescriptor {
  ...
  optional uint32 id;
  optional uint32 version;
  optional util.hlc.Timestamp modification_time;
  ...
}
```

Leases will be tied to a specific version of the descriptor. Lease
state will be stored in a new `system.lease` table:

```sql
CREATE TABLE system.lease (
  DescID     INT,
  Version    INT,
  NodeID     INT,
  // Timestamp is a wall time in microseconds. This is a
  // microsecond rounded timestamp produced from the timestamp
  // of the transaction inserting a new row.
  Expiration TIMESTAMP,
  PRIMARY KEY (DescID, Version, NodeID, Expiration)
)
```

Entries in the lease table will be added and removed as leases are
acquired and released. A background goroutine running on the lease holder
for the system range will periodically delete expired leases
 (not yet implemented).

Leases will be granted for a duration measured in minutes (we'll
assume 5m for the rest of this doc, though experimentation may tune
this number). A node will acquire a lease before using it in an
operation and may release the lease when the last local operation
completes that was using the lease and a new version of the descriptor
exists. When a new version exists all new transactions use a new lease
on the new version even when the older lease is still in use by older
transactions.

The lease holder of the range containing a table descriptor will gossip the
most recent version of that table descriptor using the gossip key
`"table-<descID>"` and the value will be the version number. The
gossiping of the most recent table versions allows nodes to
asynchronously discover when a new table version is written. But note
that it is not necessary for correctness as the protocol for acquiring
a lease ensures that only the most recent version of a descriptor can
have a new lease granted on it.

All timestamps discussed in this document are references to database
timestamps.

## Lease acquisition

Lease acquisition will perform the following steps in a transaction with
timestamp `L`:

* `SELECT descriptor FROM system.descriptor WHERE id = <descID>`
* `lease.expiration = L + lease_duration` rounded to microseconds (DTimestamp).
* `INSERT INTO system.lease VALUES (<desc.ID>, <desc.version>, <nodeID>, <lease.expiration>)`

The `lease` is used by future commands requesting a table descriptor.
Nodes will maintain a map from `<descID, version>` to
a lease: `<TableDescriptor, expiration, localRefCount>`. The local
reference count will be incremented when a transaction first uses a table
and decremented when the transaction commits/aborts. When the node
discovers a new version of the table, either via gossip or by
acquiring a lease and discovering the version has changed it can
release the lease on the old version when there are no more local
references:

* `DELETE FROM system.lease WHERE (DescID, Version, NodeID, Expiration) = (<descID>, <version>, <nodeID>, <lease.expiration>)`

When a new lease is created, the `ModificationTime` from the table in the
lease is fed into hlc.Clock.Update so that all new transactions pick timestamps
greater than the last modification timestamp.

## Incrementing the version

A schema change operation needs to ensure that there is only one version of
a descriptor in use in the cluster before incrementing the version of the
descriptor. The operation will perform the following steps
transactionally using timestamp `SC`:

* `SELECT descriptor FROM system.descriptor WHERE id = <descID>`
* `SELECT COUNT(DISTINCT version) FROM system.lease WHERE descID = <descID> AND version = <prevVersion> AND expiration > DTimestamp(SC) - 1us` == 0
* Perform the edits to the descriptor.
* Increment `desc.Version`.
* `UPDATE system.descriptor WHERE id = <descID> SET descriptor = <desc>`

We subtract `1us` from the transaction time to protect us from precision
loss due to rounding to create a DTimestamp from a database timestamp.

The schema change operation only scans the leases with the previous
version so as to not cause a lot of aborted transactions trying to
acquire leases on the new version of the table.

Note that the updating of the table descriptor will cause the table
version to be gossiped alerting nodes to the new version and causing
them to release leases on the old version. The expectation is that
nodes will fairly quickly transition to using the new version and
release all leases to the old version allowing another step in the
schema change operation to take place.

When a node holding leases dies permanently or becomes unresponsive
(e.g. detached from the network) schema change operations will have to
wait for any leases that node held to expire. This will result in an
inability to perform more than one schema modification step to the
descriptors referred to by those leases. The maximum amount of time
this condition can exist is the lease duration (5m). Note that
attempting to release leases granted by a previous incarnation of a
node is not safe because an in-flight but uncommitted transaction
using those leases might still exist in the system.

## Discussion

It is valuable to consider various scenarios to check lease usage correctness.
Assume `SC` is the timestamp of the latest schema change:
* `L < SC`: The lease will contain a table descriptor with the previous version
and will write a lease record using timestamp `L` which will be seen by the schema
change which uses a timestamp `SC`. As long as the lease is not released another
schema change cannot use a `timestamp <= lease.expiration`
* `L == SC`: Impossible because both transactions read what the other writes,
so one will push the other.
* `L > SC`: The lease will read the version of the table descriptor written by the
schema change.

A transaction using a lease with database timestamp `T` is valid iff:
1. `T >= ModificationTime` of the descriptor in the lease to prevent the
use of a descriptor "from the future".
2. `T < D` where `D` is the timestamp of the lease release transaction. Since
the transaction timestamp and the release transaction timestamp are picked on
the same node, and the release follows the last transaction using the release
this is always true.
3. The transaction commits before a `timestamp > hlc.Timestamp(lease.expiration)`
is handed over to a version increment schema change transaction on the cluster.

This last condition is harder to manage. At a minimum
`T < hlc.Timestamp(lease.expiration)`. What happens if the
transaction lasts longer than `hlc.Timestamp(lease.expiration)`?
Is the use of timestamp `T` still valid? 

* For a read only transaction it is certainly true that it can continue on
indefinitely with timestamp `T`.
* For a transaction that writes, it can write inconsistent data past the
expiration time.

For example, consider a lease acquisition before an index is added to a table.
A transaction uses a timestamp when the index was absent and attempts to INSERT
a row without an index entry for a row in the table after the lease expires.
If it so happens that the index backfill is complete and the index is added,
the index will become inconsistent. The transaction must commit before the
backfill starts!

A transaction that doesn't meet these constraints is restarted.

# Transaction deadline and extending leases

As described above a transaction that writes must complete before the
lease expiration deadline. More specifically, the transaction must have written
all its intents before a schema change version increment can use:
`timestamp > lease.expiration`, to ensure that the schema change backfill
operations sees the intents. This is resolved in cockroachdb by ensuring that
a write transaction has an execution `deadline` set to the lease expiration time.

The cluster has to enforce that all write intents written by the transaction
have been written out before the table descriptor version is incremented. This
is enforced on the node receiving the end-transaction, requiring
 `deadline > node-timestamp + maxoffset`, where `node-timestamp` is the
current timestamp on the node receiving the end-transaction and
 `maxoffset` the cap on the clock skew uncertainty across the cluster.

Whenever a lease is about to expire, a new lease is acquired for
the table extending the deadline. If the new lease has a new table
version the transaction is restarted.

An alternative that we **do not implement** extends a lease periodically
as long as it is still in use. Lease extension for descriptor `desc` can
be performed at timestamp `E` using:

* Use the same table descriptor `desc` for the new lease.
* `SELECT descriptor FROM system.descriptor WHERE id = <descID>`
* check that the version of the descriptor read is either desc.version or desc.version + 1
* `lease.expiration = E + lease_duration` rounded to microseconds (DTimestamp).
* `INSERT INTO system.lease VALUES (<desc.ID>, <desc.version>, <nodeID>, <lease.expiration>)`
* release old lease.

At this time we feel that adding this lease extension mechanism in order to
prevent restarts caused by version changes is unnecessary.

# Accommodating schema changes within transactions

A node acquires a lease on a table descriptor using a transaction created
for this purpose (instead of using the transaction that triggered the
lease acquisition), and the transaction triggering the lease acquisition
must take further precautions to prevent hitting a deadlock with
the node's lease acquiring transaction. A transaction that runs a
CREATE TABLE followed by other operations on the table will hit
a deadlock situation where the table descriptor hasn't
yet been committed while the node is trying to acquire a lease
on the table descriptor using a separate transaction. The commands
following the CREATE TABLE trying to acquire a table lease
will block on their own transaction that has written out a
new uncommitted table.

A similar situation happens when a table exists but a node
has no lease on the table, and a transaction runs a schema change
that modifies the table and subsequently runs other commands
referencing the table. Care has to be taken to first acquire
a table lease before running the transaction. While it is
possible to acquire the lease in this way before running an
ALTER TABLE it is not possible to do the same in the CREATE TABLE case.

Commands within a transaction would like to see the schema
changes made within the transaction reducing the chance of
user surprise. Both this requirement and the deadlock
prevention requirement discussed above are solved through a
solution where table descriptors modified within a transaction
are cached specifically for the use of the transaction, with the
transaction not needing a lease for the table.

# Drawbacks

* The lack of a central authority for a lease places additional stress
  on the correct implementation of the transactions to acquire a lease
  and publish a new version of a descriptor.

* Since we don't keep old versions of the descriptor around, a
  transaction which straddles both a node restart and a bump to the
  descriptor version will be aborted because it won't be able to get a
  lease to the old version of the descriptor. If this becomes a
  problem in practice we can think about mechanisms to retrieve the
  old version of the descriptor if a lease on that version still
  exists from a previous incarnation of the descriptor.

# Alternatives

* Earlier versions of this proposal utilized a centralized lease
  service. Such a service has some conceptual niceties (a single
  authority for managing the lease state of a table), yet introduces
  another service that must be squeezed into the system. Such a lease
  service would undoubtedly store state in the KV layer as well. Given
  that the KV layer provides robust transactions the benefit is
  smaller than it might otherwise have been.

* We could use an existing lock service such as etcd or Zookeeper. The
  primary benefit would be the builtin watch functionality, but we can
  get some of that functionality from gossip. We would still need the
  logic for local reference counts.

* Keeping track of local references to descriptor versions in order to
  early release leases adds complexity. We could just wait for leases
  to expire, though that would cause a 3-step schema modification to
  take at least 10m to complete.

# Unresolved questions

* Keeping only a single version of the descriptor in
  `systems.descriptor` table means that a node using `v1` that dies
  and restarts will have no way of accessing `v1` if the version has
  advanced to `v2`. This is problematic if a transaction straddles
  that restart boundary and would cause any such transaction to be
  aborted, even if the lease the previous incarnation of the node held
  is still valid.

* Gossip currently introduces a 2s/hop delay in transmitting gossip
  info. It would be nice to figure out how to introduce some sort of
  "high-priority" flag for gossipping of descriptor version info to
  reduce the latency in notifying nodes of a new descriptor version.

* Is `leaseDuration/2` the right minimum time to give an operation for
  usage of a lease? Note that the maximum time is unbounded if the
  node can successfully extend the lease duration during the
  operation. If the lease can't be extended (e.g. because of a
  concurrent schema modification) then the operation will need to be
  aborted. So the question becomes how common are operations that will
  take longer than `leaseDuration/2` and how problematic is it if they
  are aborted?

* This RFC doesn't address the full complexity of table descriptor
  schema changes. For example, when adding an index the node
  performing the operation might die while backfilling the index
  data. We'll almost certainly want to parallelize the backfilling
  operation. And we'll want to disallow dropping an index that is
  currently being added. These issues are considered outside of the
  scope of this RFC.
