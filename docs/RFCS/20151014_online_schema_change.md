- Feature Name: online_schema_change
- Status: completed
- Start Date: 2015-10-14
- RFC PR: [#2842](https://github.com/cockroachdb/cockroach/pull/2842)
- Cockroach Issue: [#2036](https://github.com/cockroachdb/cockroach/issues/2036)

# Summary

Implement online schema change: adding or removing an index or column
to a table without blocking access to the table.

# Motivation

The existing schema change operations such as adding or removing an
index are performed as a single transaction. This approach was
convenient for initial implementation but infeasible for a table with
any significant amount of data. Additionally, the single transaction
approach necessitates reading the table descriptor on every SQL
operation which is a significant performance bottleneck.

We will implement online schema change which breaks down a high-level
schema change operation such as `CREATE INDEX` into a series of
discrete steps in such a way that user transactions are never blocked
from accessing the table and yet never leave table or index data in an
invalid state.

Online schema change will be built on top of [table descriptor
leases](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20151009_table_descriptor_lease.md)
which describes the mechanism for asynchronously distributing
modifications to table descriptors. This RFC is concerned with the
actual steps of performing a schema change.

# Detailed design

Table descriptors will be enhanced to contain information about an
on-going schema change operation:

```proto
message TableDescriptor {
  ...

  oneof mutation {
    optional IndexDescriptor add_index;
    optional IndexDescriptor drop_index;
    optional ColumnDescriptor add_column;
    optional ColumnDescriptor drop_column;
  }
}
```

Additionally, index and column descriptors will be enhanced to contain
a state:

```proto
enum State {
  DELETE_ONLY;
  WRITE_ONLY;
  PUBLIC;
}
```

The `DELETE_ONLY` state specifies that `UPDATE` and `DELETE`
operations should only remove keys from the index or for the
column. The `WRITE_ONLY` state specifies that `INSERT`, `UPDATE` and
`DELETE` operations should maintain the data for the index or column:
adding or deleting as necessary. For both states, the column or index
is not used for read operations. Lastly, the `PUBLIC` state allows the
index or column to be used normally.

The state for an index or column moves from `DELETE_ONLY` to
`WRITE_ONLY` to `PUBLIC` or in reverse. It is invalid to jump from the
`DELETE_ONLY` to `PUBLIC` state or vice versa. By moving the state in
these discrete steps it can be shown that that a cluster will not
create invalid data when two consecutive versions of the descriptor
are in use concurrently. The table lease mechanism will be used to
ensure that there are only two active versions of a descriptor in use
in a cluster and the scheme change implementation will ensure that
these two versions follow the state transition invariant. The state
transition diagram (Figure 3) in [Online, Asynchronous Schema Change
in
F1](http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/41376.pdf)
is a useful reference.

When an index is added to a table a potentially time consuming
backfill operation needs to be performed to create the index
data. This operation is performed when the index is in the
`WRITE_ONLY` state and completed before moving the index to the
`PUBLIC` state. Backfilling creates index entries for every row in the
table. As such, it is infeasible for this backfilling to be performed
transactionally: the transaction would create an unreasonable number
of write intents and user transactions could cause the transaction to
abort. Instead, backfill will be performed as a series of small
transactions that process a fraction of the table at a time:

```go
  startKey := MakeTableIndexKey(desc.ID, desc.Primary)
  endKey := startKey.PrefixEnd()
  for startKey != endKey {
    var lastKey roachpb.Key
    err := db.Txn(func(ctx context.Context, txn *Txn) error {
      txn.SetPriority(VeryLowPriority)
      scan, err := txn.Scan(startKey, endKey, 1000)
      if err != nil {
        return err
      }
      lastKey = getLastKeyOfFullRowInScan(scan)
      b := makeIndexKeysBatch(scan)
      return txn.CommitInBatch(ctx, b)
    })
    if err != nil {
      // Abort!
    }
    startKey = lastKey.Next()
  }
```

The above pseudo-code is intended to give the gist of how backfilling
will work. We'll iterate over the primary index transactionally
retrieving keys and generating index entries. We perform the
backfilling work transactionally in order to avoid anomalies involving
deletions. Backfilling might duplicate work performed by concurrent
insertions or updates, but the blind overwrites of identical data are
safe.

Dropping an index involves a single `DelRange` operation. Dropping a
column is more involved and will be performed as a back-delete
process, similar to the above backfill process, which loops over the
primary index for the table and deletes the column keys.

Since schema change operations are potentially long running they need
to be restartable or abortable if the node performing them dies. We
accomplish this by performing the schema change operation for a table
on a well known node: the replica holding the range lease for the
first range of the table (i.e. containing the key `/<tableID>`). When
a node receives a schema change operation such as `CREATE INDEX` it
will forward the operation to this "table lease holder". When the table
lease holder restarts it will load the associated table descriptor and
restart or abort the schema change operation. Note that aborting a
schema change operation needs to maintain the invariant that the
descriptor version only increase.

For initial implementation simplicity, we will only allow a single
schema change operation at a time per table. This restriction can be
lifted in the future if we see a benefit in allowing concurrent schema
changes on a table (e.g. concurrently adding multiple indexes).

# Drawbacks

* None. This is rock solid.

# Alternatives

* No real good ones. We could try to come up with some sort of global
  table lock mechanism and then synchronously perform the
  backfill. This seems as difficult as the current proposal and much
  worse for the user experience.

# Unresolved questions

* Is there a way to avoid performing the backfilling work
  transactionally? Is it worth optimizing? Delete, in particular,
  seems problematic. If we scan a portion of the primary index
  non-transactionally and then generate index keys, a concurrent
  delete can come in and delete one of the rows and not generate a
  delete of the index key unless we left a "tombstone" deletion for
  the index key.

* If the node performing the backfill gets restarted we should figure
  out a way to avoid restarting the backfill from scratch. One thought
  is that the backfill operation can periodically checkpoint the high
  water mark of its progress: either in the descriptor itself (taking
  care not to bump the version) or in a separate backfill checkpoint
  table.

* Figure out how to distribute the backfill work. Ideally we would
  have each range of the primary index generate and write the index
  keys. Given that this is not an urgent need, I feel this is best
  left to a separate RFC.
