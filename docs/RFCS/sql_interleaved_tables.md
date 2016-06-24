- Feature Name: sql_interleaved_tables
- Status: draft
- Start Date: 2016-06-24
- Authors: Daniel Harrison
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)


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
order_id)) INTERLEAVE IN PARENT customers`. It is required that the field types
of the primary key of the target (or "parent") table are a prefix of the field
types of the primary key of the created (or "interleaved") table.

A new field, `InterleavedInto sqlbase.TableID` will be added to TableDescriptor
with the ID of the parent table. `InterleavedPrefixLen` will also be added and
will store how many fields are shared between the two primary keys.

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
    /customers/1/<customerID>/<interleavedTableSentinal>/orders/<orderID>/0
    /customers/1/<customerID>/<interleavedTableSentinal>/orders/<orderID>/<familyID>/suffix

Where `interleavedTableSentinal` is 109, which is selected as the largest value
encodable in a single byte with `util/encoding.EncodeUVarintAscending`. This
effectively limits the number of families in a table to 108.

We currently choose `suffix` so that each row in a table is forced to stay
entirely in one range. To avoid size limitations on the interleaved table, its
rows are treated similarly (i.e., not forced into the same range as the parent
table). In practice, they'll be on the same range most of the time.

This encoding supports interleaving multiple tables into one parent. It also
supports arbitrary levels of nesting: grandparent and great-grandparent
interleaves.

# Drawbacks

- The 108 column family limit is not a hard one, but a table with interleaved
data and more than 108 families will likely have poor performance.

# Alternatives

- We can save the byte that encodes the interleaved table id if we give up the
ability to interleave more than one table into a single parent along with some
amount of self-description in the keys.

# Unresolved questions

- When the interleaved data will be small, should the user be able to force all
rows of an interleaved table to stay with their parent rows?

- Should a foreign key constraint be automatically created between the shared
prefix of the interleaved and parent tables? (Probably not. It's not necessary
for the mechanics of interleaving tables and the user can do this if they like.)
