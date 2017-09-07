- Feature Name: sql_interleaved_tables
- Status: completed
- Start Date: 2016-06-24
- Authors: Daniel Harrison
- RFC PR: [#7465](https://github.com/cockroachdb/cockroach/pull/7465)
- Cockroach Issue: [#2972](https://github.com/cockroachdb/cockroach/issues/2972)

# Summary

Support interleaving the data for sql tables such that the rows alternate in the
kv map. This allows a user to optimize reads on tables that are frequently
joined.

The following example illustrates interleaving order data next to the associated
customers. This means that a join on `customer.id = order.customer_id` will be
computable without remote kv lookups.

    Customer 1 data
      Customer 1 Order 1 data
      Customer 1 Order 2 data
    Customer 2 data
      Customer 2 Order 1 data

# Motivation

A join of two tables accesses different parts of the kv map that are usually on
different ranges. If data from one table is frequently accessed by joining it to
another table, it is advantageous for performance to keep them nearby in the kv
map so they're likely to be on the same range. Additionally, secondary index
data can be interleaved, keeping it close to the primary data.

# Detailed design

Interleaved tables will be surfaced to users by a syntax addition: `CREATE TABLE
orders (customer_id INT, order_id INT, cost DECIMAL, PRIMARY KEY (customer_id,
order_id)) INTERLEAVE IN PARENT customers (field_1, field_2)`.

It is required that the field types of the primary key of the target (or
"parent") table are a prefix of the field types of the primary key of the
created (or "interleaved") table. In the interest of explicitness, the declared
fields must match this common prefix.

Alternately, a secondary index could be interleaved into the parent table
instead. The syntax for this is `CREATE INDEX name (fields) INTERLEAVE INTO
parent` or `CREATE TABLE ... (..., INDEX name (fields) INTERLEAVE INTO parent)`.
Note that each of a table's indexes (primary and secondary) can be interleaved
into different parent tables.

It is currently not possible to change the primary index of a table after
creation; similarly the interleave relationship of the primary index also cannot
be changed. A secondary index's interleave can be changed by adding a new
secondary index and then dropping the old one.

It is frequently desirable to enforce that there is never a row in an
interleaved table without a corresponding row in the parent table. This can be
done with a table-level foreign key constaint, but a shorthand will also be
created: `INTERLEAVE IN PARENT customers ON DELETE CASCADE|RESTRICT`. `ON DELETE
CASCADE` will delete interleaved rows if a parent is deleted. `ON DELETE
RESTRICT` will allow removal of parent rows with no interleaved rows, but will
error if interleaved rows would be orphaned. A missing `ON DELETE` clause will
fall back to table-level (or no) constraints.

A new field, `repeated Interleaves InterleaveDescriptor` will be added to
TableDescriptor. `InterleaveDescriptor` will contain `Parent sqlbase.TableID`
with the ID of the parent table, `Index sqlbase.IndexID` with the id of the
index to be interleaved, and `PrefixLen` with a count of how many fields are
shared between the index and the parent primary key.

The keys for a sql table represented in the kv map are

    /<tableID>/1/<pkCol1>/<pkCol2>/0
    /<tableID>/1/<pkCol1>/<pkCol2>/<familyID>/<suffix>

Where `pkColN` is the key encoding of the Nth primary index column, there is one
row per [family of columns](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20151214_sql_column_families.md),
and `suffix` counts how many bytes are stripped off the end to determine where
ranges can split. The 0 row is always present and acts as a sentinel for the
existence of the row. The single `0` on the end is an optimization that reuses
the suffix value as a `familyID`, (it's effectively the same as `/0/1`).

A table `orders` interleaved into a table `customers` will be encoded as

    # Customer data
    /customers/1/<customerID>/0
    /customers/1/<customerID>/<familyID>/1
    # Order data
    /customers/1/<customerID>/<interleavedTableSentinel>/orders/<indexID>/<orderID>/0
    /customers/1/<customerID>/<interleavedTableSentinel>/orders/<indexID>/<orderID>/<familyID>/suffix

Where `interleavedTableSentinel` is the byte `0xfe`, which is the same as
`EncodeNotNullDescending` and so is available to use here.

We currently choose `suffix` such that each row in a table is forced to stay
entirely in one range. To avoid size limitations on the interleaved table, its
rows are treated similarly (i.e., not forced into the same range as the parent
table). In practice, they'll be on the same range most of the time.

Range splits are currently along hard boundaries. It would be advantageous to
introduce prefer-not-to-split boundaries where splits are allowed, but avoided
if possible. These would be placed between interleaved table rows so that they
stay on the same range if possible, but not at the cost of unwieldy range sizes.
This is considered future work and out of scope for the initial implementation.

This encoding supports interleaving multiple tables into one parent. It also
supports arbitrary levels of nesting: grandparent and great-grandparent
interleaves.

Interleaving row data of multiple tables inherently adds complexity to scans and
deletes. Deletes such as `DELETE FROM orders WHERE id < 100` currently rely on a
fast path which will have to be disabled for interleaved tables, unless the
table was declared as `ON DELETE CASCASE`. Fortunately, it is expected that
`CASCADE` will be the common case, and so it may not be a problem in practice.
If it is, a new kv operation with knowledge of sql keys can be created to delete
a range of data while skipping interleaved rows.

When scanning either a parent or interleaved table, the scan code will have to
skip over the other table's data. This logic will initially be implemented using
existing kv operations. One approach is to iterate the entire range and ignore
the other table's rows. Or, when a key from the other table is enountered, a
`Scan` can be constructed to skip past it. The former may overfetch many keys
and the latter may involve many round-trips, but the two approaches can be
combined to minimize the worst case behaviors. Additionally, with distributed
SQL, this logic would normally run on the range lease holder so it would be a local
operation. Finally, if performance issues do surface in practice, a new kv
operation, similar to the one mentioned for deletes, can be created which scans
a range of data while skipping interleaved rows.

# Drawbacks

- Even with the mentioned optimizations, the scan and delete-range operations
will be slower for any table interleaved with another. This can be mitigated
somewhat by pushing knowledge of the sql key structure down into kv, but this
still won't be quite as fast and will add additional complexity to kv. However,
adding the feature doesn't affect the speed of non-interleaved tables.

# Alternatives

- A syntax extension could be added to, when the data is known to be small,
allow the user to force all rows of an interleaved table to stay with their
parent rows. Cockroach avoids tuning parameters when possible, so instead the
prefer-not-to-split is offered as future work.
