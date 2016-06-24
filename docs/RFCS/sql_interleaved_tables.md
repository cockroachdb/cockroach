- Feature Name: sql_interleaved_tables
- Status: draft
- Start Date: 2016-06-24
- Authors: Daniel Harrison
- RFC PR: [7465](https://github.com/cockroachdb/cockroach/pull/7465)
- Cockroach Issue: [2972](https://github.com/cockroachdb/cockroach/issues/2972)


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
map so they're likely to be on the same range.

# Detailed design

Interleaved tables will be surfaced to users by a syntax addition: `CREATE TABLE
orders (customer_id INT, order_id INT, cost DECIMAL, PRIMARY KEY (customer_id,
order_id)) INTERLEAVE IN PARENT customers`.

It is required that the field types of the primary key of the target (or
"parent") table are a prefix of the field types of the primary key of the
created (or "interleaved") table. Alternately, a secondary index could be
interleaved into the parent table instead. The syntax for this is `CREATE INDEX
name (fields) INTERLEAVE INTO parent`. Note that each of a table's indexes
(primary and secondary) can be interleaved into different parent tables.

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
fall back to table-level (or no) contraints.

A new field, `repeated Interleaves InterleaveDescriptor` will be added to
TableDescriptor. `InterleaveDescriptor` will contain `Parent sqlbase.TableID`
with the ID of the parent table, `Index sqlbase.IndexID` with the id of the
index to be interleaved, and `PrefixLen` with a count of how many fields are
shared between the index and the parent primary key.

The keys for a sql table represented in the kv map are

    /<tableID>/1/<pkCol1>/<pkCol2>/0
    /<tableID>/1/<pkCol1>/<pkCol2>/<familyID>/<suffix>

Where `pkColN` is the key encoding of the Nth primary index column, there is one
row per [family of columns](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/sql_column_families.md),
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

Where `interleavedTableSentinel` is 109, which is selected as the largest value
encodable in a single byte with `util/encoding.EncodeUVarintAscending`. This
effectively limits the number of families in a table to 108.

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
deletes. When scanning either a parent or interleaved table, the scan code will
have to be changed to jump over the other table's data. This can be done by
using kv `Seek`s or by scanning every key and discarding the irrelvant ones,
each with different performance tradeoffs. This logic will initially be
encapsulated in the sql `RowFetcher`, but can be pushed down if necessary for
performance. Deletes currently rely on a fast path when removing a range, which
will have to be disabled for interleaved tables. They also use key-range bulk
deletions for rows and the endpoints of these ranges will have to be tweaked.

# Drawbacks

- The 108 column family limit is not a hard one, but a table with interleaved
data and more than 108 families will likely have poor performance.

# Alternatives

- A syntax extension could be added to, when the data is known to be small,
allow the user to force all rows of an interleaved table to stay with their
parent rows. Cockroach avoids tuning parameters when possible, so instead the
prefer-not-to-split is offered as future work.
