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
[schema_gossip](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20150720_schema_gossip.md)). Unfortunately,
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
on how to determine when a previous version of the descriptor is no
longer in use for read-write operations.

Separately, we implement another lease mechanism not discussed here
called a schema change lease. When a schema change is to be performed,
the operation first acquires a schema change lease for the table.
The schema change lease ensures that only the lease holder can execute
the state machine for a schema change and update the table descriptor.

Before using a table descriptor for a DML operation (i.e. `SELECT`,
`INSERT`, `UPDATE`, `DELETE`, etc), the operation needs to obtain a
table lease for the descriptor. The lease will be guaranteed valid for
a significant period of time (on the order of minutes). When the operation completes it will release the table lease.

# Detailed design

The design maintains two invariant:
* Two safe versions: A transaction at a particular timestamp is allowed to use
one of two versions of a table descriptor.
* Two leased version: There can be valid leases on at most the 2 latest
versions of a table in the cluster at any time. Leases are usually granted
on the latest version.

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

A table descriptor at a version `v` has a validity period spanning from
its `ModificationTime` until the `ModificationTime` of the table descriptor at
version `v + 2`: [`ModificationTime`, `ModificationTime[v+2]`). A transaction
at time `T` can safely use one of two table descriptors versions: the two
versions with highest `ModificationTime` less than or equal to `T`.
Once a table descriptor at `v` has been written, the validity period of the
table descriptor at `v - 2` is fixed. A node can cache a copy of
`v-2` along with its fixed validity window and use it for
transactions whose timestamps fall within its validity window.

Leases are needed because the validity window of the latest versions
(`v` and `v - 1` ) are unknown (`v + 1` hasn't yet been written).
Since table descriptors can be written at any time, this design is
about defining a frontier for an undefined validity window,
and guaranteeing that the frontier lies within the as of yet
to be defined validity window. We call such a validity window
a temporary validity window for the version. The use of a cached copy
of a table descriptor is allowed while enforcing the temporary
validity window.

Leases will be tied to a specific version of the descriptor. Lease
state will be stored in a new `system.lease` table:

```sql
CREATE TABLE system.lease (
  DescID     INT,
  Version    INT,
  NodeID     INT,
  # Expiration is a wall time in microseconds. This is a
  # microsecond rounded timestamp produced from the timestamp
  # of the transaction inserting a new row. It would ideally
  # be an hlc.timestamp but cannot be changed now without much
  # difficulty.
  Expiration TIMESTAMP,
  PRIMARY KEY (DescID, Version, Expiration, NodeID)
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
a lease ensures that only the two recent versions of a descriptor can
have a new lease granted on it.

All timestamps discussed in this document are references to database
timestamps.

## Lease acquisition

Lease acquisition will perform the following steps in a transaction with
timestamp `L`:

* `SELECT descriptor FROM system.descriptor WHERE id = <descID>`
* `lease.expiration = L + lease_duration` rounded to microseconds (SQL DTimestamp).
* `INSERT INTO system.lease VALUES (<desc.ID>, <desc.version>, <nodeID>,<lease.expiration>)`

The `lease` is used by transactions that fall within its temporary
validity window. Nodes will maintain a map from `<descID, version>` to
a lease: `<TableDescriptor, expiration, localRefCount>`. The local
reference count will be incremented when a transaction first uses a table
and decremented when the transaction commits/aborts. When the node
discovers a new version of the table, either via gossip or by
acquiring a lease and discovering the version has changed it can
release the lease on the old version when there are no more local
references:

* `DELETE FROM system.lease WHERE (DescID, Version, NodeID, Expiration) = (<descID>, <version>, <nodeID>, <lease.expiration>)`

## Incrementing the version

A schema change operation needs to ensure that there is only one version of
a descriptor in use in the cluster before incrementing the version of the
descriptor. The operation will perform the following steps transactionally
using timestamp `SC`:

* `SELECT descriptor FROM system.descriptor WHERE id = <descID>`
* Set `desc.ModificationTime` to `SC`
* `SELECT COUNT(DISTINCT version) FROM system.lease WHERE descID = <descID> AND version = <prevVersion> AND expiration > DTimestamp(SC)` == 0
* Perform the edits to the descriptor.
* Increment `desc.Version`.
* `UPDATE system.descriptor WHERE id = <descID> SET descriptor = <desc>`

The schema change operation only scans the leases with the previous
version so as to not cause a lot of aborted transactions trying to
acquire leases on the new version of the table. The above schema change
transaction is retried in a loop until it suceeds.

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
this condition can exist is the lease duration (5m).

As described above, leases will be retained for the lifetime of a
transaction. In a multi-statement transaction we need to ensure that
the same version of each table is used throughout the transaction. To
do this we will add the descriptor IDs and versions to the transaction
structure. When a node receives a SQL statement within a
multi-statement transaction, it will attempt to use the table version
specified.

While we normally acquire a lease at the latest version, occasionally a
transaction might require a lease on a previous version because it falls
before the validity window of the latest version:

A table descriptor at version `v - 1` can be read using timestamp
`ModificationTime - 1ns` where `ModificationTime` is for table descriptor
at version `v`. Note that this method can be used to read a table descriptor
at any version.

A lease can be acquired on a previous version using a transaction at
timestamp `P` by running the following:
* `SELECT descriptor FROM system.descriptor WHERE id = <descID>`
* check that `desc.version == v + 1`
* `lease.expiration = P + lease_duration` rounded to microseconds (DTimestamp).
* `INSERT INTO system.lease VALUES (<desc.ID>, <v>, <nodeID>, <lease.expiration>)`

## Correctness

It is valuable to consider various scenarios to check lease usage correctness.
Assume `SC` is the timestamp of the latest schema change descriptor modification
time, while `L` is the timestamp of a transaction that has acquired a lease:
* `L < SC`: The lease will contain a table descriptor with the previous version
and will write a row in the lease table using timestamp `L` which will be seen
by the schema change which uses a timestamp `SC`. As long as the lease is not
released another schema change cannot use a `timestamp <= lease.expiration`
* `L > SC`: The lease will read the version of the table descriptor written by the
schema change.
* `L == SC`: If the lease reads the descriptor first, the schema change will see
the read in the read timestamp cache and will get restarted. If the schema
change writes the descriptor at the new version first, the lease will read the new
descriptor and create a lease with the new version.

Temporary validity window for a leased table descriptor is either one of:
1. `[ModificationTime, D)`: where `D` is the timestamp of the lease
release transaction. Since a transaction with timestamp `T` using a lease
and a release transaction originate on the same node, the release follows
the last transaction using the lease, `T < D` is always true.
2. `[ModificationTime, hlc.Timestamp(lease.expiration))` is valid
because the actual stored table version during the window is guaranteed
to be at most off by 1.

Note that two transactions with timestamp T~1~ and T~2~ using versions
`v` and `v+2` respectively that touch some data in common, are guaranteed
to have a serial execution order T~1~ < T~2~. This property is important
when we want to positively guarantee that one transaction sees the effect
of another.

Note that the real time at which a transaction commits will be different
from the wall time in its database timestamp. On an idle range, transactions
may be allowed to commit with timestamps far in the past (although
the read timestamp cache will not permit writing with a very old timestamp).
The expiration of a table descriptor lease does not imply that all
transactions using that lease have finished. Even if a transaction commits
later in time, CRDB guarantees serializability of transactions thereby
sometimes aborting old transactions that attempting to write using an
old timestamp.

Examples of transactions that need serial execution that use
version `v` and `v+2`:
1. A transaction attempts to DELETE a row using a descriptor without
an index, and commits after the row is being acted on by an UPDATE
seeing an index in the WRITE_ONLY state. The DELETE is guaranteed
to see the UPDATE and be aborted, or the UPDATE sees the delete tombstone.
2. A transaction attempts to run a DELETE on a table in the DELETE_ONLY
state and the transaction commits during the backfill. The DELETE is
guaranteed to be seen by the backfill, or aborted.
3. A transaction attempts to run an UPDATE on a table with an index in
the WRITE_ONLY state and the transaction commits when the index is readable
via a SELECT. The UPDATE is either guaranteed to be seen by the SELECT,
or be aborted.

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
that modifies the table without incrementing the version, and
subsequently runs other commands referencing the table.
Care has to be taken to first acquire a table lease before running
the transaction. While it is possible to acquire the lease in this
way before running an ALTER TABLE it is not possible to do the same
in the CREATE TABLE case.

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

* Gossip currently introduces a 2s/hop delay in transmitting gossip
  info. It would be nice to figure out how to introduce some sort of
  "high-priority" flag for gossipping of descriptor version info to
  reduce the latency in notifying nodes of a new descriptor version.

* This RFC doesn't address the full complexity of table descriptor
  schema changes. For example, when adding an index the node
  performing the operation might die while backfilling the index
  data. We'll almost certainly want to parallelize the backfilling
  operation. And we'll want to disallow dropping an index that is
  currently being added. These issues are considered outside of the
  scope of this RFC.
