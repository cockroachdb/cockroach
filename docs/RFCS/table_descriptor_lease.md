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
  Expiration TIMESTAMP,
  PRIMARY KEY (DescID, Version, NodeID, Expiration)
)
```

Entries in the lease table will be added and removed as leases are
acquired and released. A background goroutine running on the lease holder
for the system range will periodically delete expired leases.

Leases will be granted for a duration measured in minutes (we'll
assume 5m for the rest of this doc, though experimentation may tune
this number). A node will acquire a lease before using it in an
operation and may release the lease when the last local operation
completes that was using the lease and a new version of the descriptor
exists.

The lease holder of the range containing a table descriptor will gossip the
most recent version of that table descriptor using the gossip key
`"table-<descID>"` and the value will be the version number. The
gossiping of the most recent table versions allows nodes to
asynchronously discover when a new table version is written. But note
that it is not necessary for correctness as the protocol for acquiring
a lease ensures that only the most recent version of a descriptor can
have a new lease granted on it.

Lease acquisition will perform the following steps in a transaction:

* `SELECT descriptor FROM system.descriptor WHERE id = <descID>`
* `INSERT INTO system.lease VALUES (<desc.ID>, <desc.version>, <nodeID>, <expiration>)`

Nodes will maintain a map from `<descID, version>` to
`<TableDescriptor, expiration, localRefCount>`. The local reference
count will be incremented when a transaction first uses a table and
decremented when the transaction commits/aborts. When the node
discovers a new version of the table, either via gossip or by
acquiring a lease and discovering the version has changed it can
release the lease on the old version when there are no more local
references:

* `DELETE FROM system.lease WHERE (DescID, Version, NodeID, Expiration) = (<descID>, <version>, <nodeID>, <expiration>)`

A schema change operation needs to ensure that there is only one
version of a descriptor in use in the cluster before a mutation of the
descriptor. The operation will perform the following steps
transactionally:

* `SELECT descriptor FROM system.descriptor WHERE id = <descID>`
* Set `desc.ModificationTime` to `now()`.
* `SELECT DISTINCT version FROM system.lease WHERE descID = <descID> AND expiration > <desc.ModificationTime>`
* Ensure that there is only one active version which is equal to the current version.
* Perform the edits to the descriptor.
* Increment `desc.Version`.
* `UPDATE system.descriptor WHERE id = <descID> SET descriptor = <desc>`

The table descriptor `ModificationTime` would be fed to the
`hlc.Clock.Update` whenever a lease is acquired to ensure that the node
is not allowed to use a descriptor "from the future".

Note that the updating of the table descriptor will cause the table
version to be gossipped alerting nodes to the new version and causing
them to release leases on the old version. The expectation is that
nodes will fairly quickly transition to using the new version and
release all leases to the old version allowing another step in the
schema change operation to take place. The schema change operation
must poll the `system.lease` table waiting for the number of active
versions of a descriptor to fall to 1 or 0. This polling will be
performed at a short interval (1s), perhaps with a small amount of
back-off.

The polling loop needs to be mildly careful to not cause a lot of
aborted transactions trying to acquire leases on the new version of
the table. It will do this by performing a simpler initial check
looking for any non-expired leases of the old version:

```sql
SELECT COUNT(DISTINCT version) FROM system.lease WHERE descID = <descID> AND version = <prevVersion> AND expiration > now()`
```

This pre-check is only scanning the previous version of the descriptor
for which no new leases will be added.

When a node holding leases dies permanently or becomes unresponsive
(e.g. detached from the network) schema change operations will have to
wait for any leases that node held to expire. This will result in an
inability to perform more than one schema modification step to the
descriptors referred to by those leases. The maximum amount of time
this condition can exist is the lease duration (5m). Note that
attempting to release leases granted by a previous incarnation of a
node is not safe because an in-flight but uncommitted transaction
using those leases might still exist in the system.

As described above, leases will be retained for the lifetime of a
transaction. In a multi-statement transaction we need to ensure that
the same version of each table is used throughout the transaction. To
do this we will add the descriptor IDs and versions to the transaction
structure. When a node receives a SQL statement within a
multi-statement transaction, it will gather leases for all the tables
in the transaction proto. If any of the leases are at a version
incompatible with the version provided, it will abort the transaction.

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
