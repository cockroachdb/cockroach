- Feature Name: schema_lease
- Status: draft
- Start Date: 2019-01-03
- RFC PR: [#28628](https://github.com/cockroachdb/cockroach/pull/28628)
- Cockroach Issue: [#23978](https://github.com/cockroachdb/cockroach/issues/23978)

# Summary

Implement a schema-lease mechanism to allow safe usage of cached schema.
Transactions can use cached schema while maintaining data consistency.

# Motivation

The motivation for the need for cached schema and table descriptor leases
is described in [table_descriptor_lease](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20151009_table_descriptor_lease.md).
This RFC is a description of a new system to replace table-leases.
The old implementation of table-leases is unable to support the
following new requirements:

1. Scalability: Clusters need to scale to cache thousands of descriptors on hundreds
of nodes while using less than 1% of resources on managing leases. [#23510](https://github.com/cockroachdb/cockroach/issues/23510)
From a product standpoint, we'd like to demo cockroach as a multi-tenant database
supporting 100s of `CREATE DATABASE` each holding 100s of `CREATE TABLE`.
2. Quality: Leases should live with a reasonable expiration time on them so that
transactions do not hit deadline expiration errors/retries. Transactions
should not block on lease acquisition.
3. Lease databases: The current system only supports table descriptor leases.
There is a need to lease database descriptors to better
reason about transactions modifying database descriptors. The
current implementation has a rather flimsy implementation of a database
cache. [#14332](https://github.com/cockroachdb/cockroach/issues/14332)

# Guide-level explanation

For the rest of the discussion parts of the schema like databases and
tables are called schema elements.

The entire schema is defined as:
1. The name to id map for the databases and tables.
2. The database descriptors.
3. The table descriptors.

The system maintains consistent data by ensuring:
1. A schema change is broken up into as many incremental changes/versions
as necessary such that a cluster using two adjacent versions for two
transactions using the same timestamp will not violate data consistency.
2. A new schema change version is rolled out ensuring that the cluster only
uses two adjacent versions of a schema element at a timestamp.

In other words, the design maintains two invariant:
* Two safe versions: A transaction at a particular timestamp is allowed to use
one of two versions of a schema element (The version in the store at the timestamp
and the previous version).
* Two leased version: There can be valid schema leases with at most 2 latest
versions of any schema element in the cluster at any time. Leases are usually
granted on a schema with the latest version of a schema element.

This RFC doesn't discuss caching policy: Should every node cache the entire schema?
Should every node cache all older versions of the schema? This RFC doesn't
discuss schema caching policy (LRU, etc), and only discusses leases
which are required for the correct use of a cached schema.

The new leasing mechanism scales the leasing mechanism by having a node
acquire a single lease on the entire schema and tying it to a particular
node liveness epoch. The system proposed here in the steady state with
no schema changes and no changes in node liveness epoches
will have each node lease the schema once with no refreshes of leases.

# Reference-level explanation

## Detailed design

As is existing today a descriptor will continue to keep a version number
that is incremented on every change to the descriptor. This is only needed
as a debugging tool. The `ModificationTime` on the descriptor can be deprecated
in favor of the MVCC timestamp on the descriptor and is not discussed here.

```proto
message TableDescriptor {
  ...
  optional uint32 id;
  optional uint32 version;
  optional util.hlc.Timestamp modification_time;
  ...
}
```

### Why leases?

The `ModificationTime` in this discussion is the database hlc timestamp at which
a descriptor is modified. If there was no caching of schema on the
cluster, a schema element at a version `v` would have a validity
period spanning from its `ModificationTime` until the `ModificationTime`
of the element at version `v + 1`: [`ModificationTime`, `ModificationTime[v+1]`),
where the validity window is the span of timestamps for transactions
that are allowed to use the schema.

However, we want to support caching of schema, therefore the validity
period of a schema element is extended, to allow one update to
the schema without locking/flushing the cache. Care is taken to
ensure that the database can maintain consistency with this extension.
A schema element at version `v` has a validity window spanning from
its `ModificationTime` until the `ModificationTime` of the element at
version `v + 2`: [`ModificationTime`, `ModificationTime[v+2]`). A transaction
at time `T` can safely use one of two versions: the two
versions with highest `ModificationTime` less than or equal to `T`.

Once a schema element at `v` has been written, the validity window of the
element at `v - 2` is fixed. A node can cache a copy of
`v-2` along with its fixed validity window and use it for
transactions whose timestamps fall within its validity window.

Leases are needed because the validity window of the latest versions
(`v` and `v - 1` ) are unknown (`v + 1` hasn't yet been written).
Since schema can be written at any time, this design is
about defining a frontier for an undefined validity window,
and guaranteeing that the frontier lies within the as of yet
to be defined validity window. We call such a validity window
a temporary validity window for the version, and it is enforced
while using a cached copy of a schema element.

### Leases

The new design scales the older mechanism by:
1. Introducing a lease on the entire schema, thereby scaling well with
the number of tables/nodes.
2. Introduces a leasing mechanism on top of node liveness epochs to
prevent needing to continuously refresh the leases. The leases do
not have a fixed expiration time, which is extended automatically
as time marches forward.

Leases will be tied to a specific database timestamp. A table or database
that is part of this lease must be read at the hlc timestamp of the lease,
therefore a lease represents a specific version of databases and tables.
Leases will be stored in a new `system.schema_lease` table:

Leases will be tied to a specific hlc timestamp snapshot of the schema.
Lease state will be stored in a new `system.schema_lease` table:

```sql
CREATE TABLE system.schema_lease (
  NodeID     INT,
  # The node liveness epoch of the node creating this lease.
  # The lease expiration is a moving hlc.Timestamp frontier:
  # F = last_known_epoch_expiration.
  Epoch      INT,
  // HLC timestamp tied to this lease. The lease is written
  // at this timestamp, and this timestamp is used to read
  // the schema for this lease. This is split up into two
  // columns to allow flexible sql operations with it.
  HLCWallTime   INT,
  HLCLogical INT,

  PRIMARY KEY (HLCWallTime, HLCLogicalTime, NodeID, Epoch)
)
```

In truth, NodeID and Epoch are the only two fields required here.
Another field is required only to disambiguate during lease transition time
when a node can be using two leases: A new lease, and an old lease
being used by extant transactions. We could have used a UUID
but decided to use the HLC timestamp because it serves the same
purpose as UUID, and provides the means to query the lease table
via SQL using the lease creation time.

Entries in the lease table will be added and removed as leases are
acquired and released. A lease is valid until last_known_epoch_expiration
A node will acquire a lease before using it to set a deadline for an
operation. It will release the lease after a new lease is acquired and
when the last local operation using it completes. When a new lease
exists all new transactions use the new lease even when the older lease
is still in use by older transactions.

All timestamps discussed in this document are references to database
timestamps.

### Lease acquisition/release

Lease acquisition will perform the following steps in a transaction:

	* `liveness := nodeLiveness.Self()` (not associated with the transaction)
	* L := txn.CommitTimestamp()
	* `INSERT INTO system.schema_lease VALUES (<nodeID>, <liveness.Epoch>, L.WallTime, L.Logical)`

Where a lease is associated with the write timestamp `L` of the lease
and the set of descriptors at a timestamp `L`. Note that this lease
acquisition mechanism doesn't read any descriptors, and doesn't required
that the descriptors be cached by the node.

The temporary validity window of a descriptor with this lease
is: [`ModificationTime`, `F`), where `ModificationTime` is the
modification time of a descriptor in the lease and
`F = last_known_epoch_expiration`.
Note: an old epoch doesn't update its `last_known_epoch_expiration`
and therefore its frontier `F` will eventually expire.

The lease is used by transactions that fall within its temporary
validity window. A node will need to renew a lease whenever:
1. The epoch of the node changes or,
2. One of the descriptors is modified.

A node will release an older lease when it is not in use through:

* `DELETE FROM system.schema_lease WHERE (NodeID, Epoch, HLCWallTime, HLCLogical) = (<nodeID>, <epoch>, <L.WallTime>, <L.Logical>)`

### Using the descriptor cache and leases

A node will maintain a schema cache containing schema elements
with many versions. Each schema element version will have an HLC expiration
timestamp associated with it and is valid for [`ModificationTime`, `expiration`).
A transaction can use a schema element version only if it's
commit timestamp `T` is within [`ModificationTime`, `expiration`).

Older versions of a schema element have a fixed expiration timestamp.
The latest version of a schema element will always be tied to a lease
and its associated dynamic frontier expiration `F``,
where `F = last_known_epoch_expiration`.

A transaction can pick valid descriptors from the descriptor cache
using its origTimestamp, but it might get pushed later with a greater
commit timestamp `T` (origTimestamp <= `T`) . To ensure that `T` doesn't get
pushed beyond `expiration` the transaction sets its deadline to `expiration`.
Since `expiration` is always marching forward for leased descriptors, it is
best that a transaction set its deadline at the very end just before commit
time, so as to pick the greatest possible deadline. A transaction using a
fixed-timestamp need not set a deadline because its commit timestamp is
the same as origTimestamp.

The lease should never be released while a transaction is using a
cache entry associate with it because the deadline `F` is only valid
as long as the lease is valid.

Once a lease is no longer referred by any transaction it can be released
using a transaction with timestamp `D`. A transaction at
timestamp `T` that used the lease and saw a table at version `v` is
guaranteed to have `T < D`. We will later discuss how a schema change
modifying the table to version `v+2` with a timestamp `SC` has to wait
such that `D <= SC`. Therefore it follows that `T < SC`, ensuring the
correct use of the descriptor.

While normally a transaction can use descriptors from the latest
lease, occasionally a transaction might fall before the lease
timestamp. It is allowed to use a descriptor from the lease as
long as the `ModificationTime` of the descriptor is less than or
equal to the transaction's timestamp. If the lease is unable to
satisfy this the descriptor cache will read a version from the
store, and set its deadline to the `ModificationTime` of the next version.
This is possible because older descriptor versions have fixed validity
window.

### Incrementing the version via a schema change

A schema change operation needs to ensure that there is only one version of
a descriptor leased in the cluster before incrementing the version of the
descriptor. The operation will perform the following steps transactionally
using timestamp `SC`:

* Get (descriptor, modificationTimestamp) for id = <descID>
* Restart if leases written before the modificationTimestamp exist
* Perform the edits to the descriptor.
* Increment `desc.Version`.
* `UPDATE system.descriptor WHERE id = <descID> SET descriptor = <desc>`

The above schema change transaction is retried in a loop until it suceeds.

Updating a schema element will notify all nodes of the schema change
(described below), triggering all nodes to refresh their schema-lease.
When a node holding leases dies permanently or becomes unresponsive
(e.g. detached from the network) schema change operations might have to
wait for any leases that node held to be deleted. This will result in an
inability to perform more than one schema modification step to a descriptor.
The minimum amount of time this condition can exist is the expiration
timestamp of the liveness record for the node epoch. After the node
epoch expires, another node in the cluster will delete the lease at a
prior epoch for the disconnected node. This is explained below.

### Notification of a schema change

A schema element update will cause the system config to be gossiped
alerting nodes to the new version and causing them to release leases.
The expectation is that nodes will fairly quickly acquire new leases
and transition to using the new version and release all leases to the
old version allowing the next step (upgrade to next version) in the
schema change operation to take place.

This notification mechanism is however quite heavyweight and unlikely
to scale well. Once rangefeed/changefeeds is declared very stable we can
considering having each node register for change notifications via
that mechanism.

Another problem with the current gossip notification is that it is
unreliable. A regular polling mechanism will run every 5 minutes to
double check if schema has changed.

### Scan with incremental diff

The `Scan` operation will be extended to support a fromTimestamp parameter.
`Scan(startKey, endKey, fromTimestamp)` and will return all the KV
entries along with their timestamps, for all keys in [`startKey`, `endKey`)
with a `writeTimestamp > fromTimestamp`. Such a modified `Scan` will be
used by:
1. Read all descriptors that have been modified since the last lease:
Whenever a descriptor has been modified and a node needs to renew a lease
on the entire schema, it is inefficient to refresh the schema cache by
reading all the descriptors from the prior lease from the store at the
new lease timestamp. `Scan` allows the node to see the diff from the last lease.
2. Read all descriptor versions: When a transaction needs to access a descriptor
from a prior version (a long running transaction with an old timestamp), it is
more efficient to read all versions of a descriptor using a single operation.

### Deleting abandoned leases

Normally a node owing a lease will delete a lease whose `F` has expired on
its own due to a change in epoch, however this might not happen when a node crashes.
A background goroutine running on all nodes will periodically delete leases
whose frontier `F` have expired. A node reads the node liveness records from
the entire cluster. If it sees a liveness record for node `N` :
`{Epoch: E, Expiration: Q}`, it is allowed to delete all leases
for node `N` with `epoch < E` using a transaction with timestamp
`A`, where `A > Q`.

The correctness of this approach follows from the observation that
`Q > P` where `P` is the highest expiration timestamp for epoch `E-1`.
This is a property offered to us by node liveness records. Therefore,
`Q` is greater than any frontier `F` used by any node in the cluster.

Note: `Q` is the lowest expiration time seen by a node for epoch `E`,
so `A` will come to pass.

Note: `Q` already incorporates time uncertainty by having the clock
offset added to it.

### Backward compatibility with expiration based leases

A node that has been restarted with the latest version and not yet upgraded
to the latest version will:
1. Start writing epoch based leases and begin using them.
2. Continue to write expiration based leases for all cached descriptors associated
with an epoch based lease. This is to accommodate the fact that older version nodes
could be running a schema change.
2. The schema change rollout mechanism will lookup leases in both `system.lease` and
`system.schema_lease`. This accommodates older version nodes that acquire expiration
based leases.

Once the cluster has been upgraded the nodes will stop writing expiration based leases.

The above scheme has the complexity that it needs to support both the expiration
and new epoch based leases for one version.

A following version of the software will delete table `system.lease`, freeing
its descriptor-id for the future.

### Correctness

It is valuable to consider various scenarios to check lease usage correctness.
Assume `M` is the timestamp of the latest schema change descriptor modification
time, while `L` is the timestamp of a transaction that has acquired a lease:
* `L < M`: The lease will contain a table descriptor with the previous version
and will write a row in the lease table using timestamp `L` which will be seen
by a schema change using a timestamp > `M`. As long as the lease is not
released the schema change will be blocked.
* `L >= M`: The lease will use the latest version of the table descriptor.
A schema changer can move forward and update the descriptor.

Temporary validity window for a leased descriptor is either one of:
1. `[ModificationTime, D)`: the lease is explicitly released by the node
owner where `D` is the timestamp of the lease release transaction. Since a
transaction with timestamp `T` using a lease and a release transaction
originate on the same node, the release follows the last transaction using
the lease, implying that `T < D` is always true
2. `[ModificationTime, F)` is valid because the actual stored table
version during the window is guaranteed to be at most greater by 1.
A schema changer seeing the lease will not be able to increment the version
for the descriptor.

Note that two transactions with timestamp T~1~ and T~2~ using versions
`v` and `v+2` respectively that touch some data in common, are guaranteed
to have a serial execution order T~1~ < T~2~. This property is important
when we want to positively guarantee that one transaction sees the effect
of another.

Note that the real time at which a transaction commits will be different
from the wall time in its database timestamp. On an idle range, transactions
may be allowed to commit with timestamps far in the past (although
the read timestamp cache will not permit writing with a very old timestamp).
The expiration of a lease does not imply that all
transactions using that lease have finished. Even if a transaction commits
later in time, CRDB guarantees serializability of transactions thereby
sometimes aborting old transactions that attempting to write using an
old timestamp.

Examples of transactions that need serial execution when using
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

### Accommodating schema changes within transactions

(discussion copied from table descriptor lease RFC. It is unchanged)

A node acquires a lease using a transaction created
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
has no lease, and a transaction runs a schema change
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

## Drawbacks

* A lease is renewed whenever a schema change occurs even if the
schema elements involved in the change are not in use.

## Alternatives


## Unresolved questions


