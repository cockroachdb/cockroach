- Feature Name: sql_column_groups
- Status: draft
- Start Date: 2015-12-14
- RFC PR:
- Cockroach Issue:

# Summary

Store multiple columns for a row in a single value to amortize the
overhead of keys and the fixed costs associated with values. The
mapping of a column to a column group will be determined when a column
is added to the table.

# Motivation

The SQL to KV mapping currently utilizes a key/value pair per
non-primary key column. The key is structured as:

```
/<tableID>/<indexID>/<primaryKeyColumns...>/<columnID>
```

Keys also have an additional suffix imposed by the MVCC
implementation: ~10 bytes of timestamp.

The value is a proto (note, this is what is stored on disk, we also
include a timestamp when transmitting over the network):

```
message Value {
  optional bytes raw_bytes;
  optional fixed32 checksum;
  optional ValueType tag;
}
```

Consider the following table:

```
CREATE TABLE kv (
  k INT PRIMARY KEY,
  v INT
)
```

A row for this table logically has 16 bytes of data (`INTs` are 64-bit
values). The current SQL to KV mapping will create 2 key/value pairs:
a sentinel key for the row and the key/value for column `v`. These two
keys imply an overhead of ~28 bytes (timestamp + checksum) when stored
on disk and more than that when transmitted over the network (disk
storage takes advantage of prefix compression of the keys). We can cut
this overhead in half by storing only a single key/value for the
row. The savings are even more significant as more columns are added
to the table.

Note that there is also per key overhead at the transaction
level. Every key written within a transaction has a "write intent"
associated with it and these intents need to be resolved when the
transaction is committed. Reducing the number of keys per row reduces
this overhead.

# Detailed design

The structure for table keys will be changed to:

```
/<tableID>/<indexID>/<primaryKeyColumns...>/<groupID>
```

The value proto will remain almost the same, but the `tag` field will
be removed and instead of encoding the column value directly into the
`raw_bytes` field we'll encoding a series of `<columnID>/<columnVal>`
pairs where `<columnID>` is varint encoded and `<columnVal>` is
encoded per its type using the `util/encoding` routines. Similar to
the existing convention, `NULL` column values will not be present in
the value.

The `ColumnDescriptor` proto will be extended with a `GroupID`
field. `GroupIDs` will be allocated per-table using a new
`TableDescriptor.NextGroupID` field. `GroupID(0)` will be always be
written for a row and will take the place of the sentinel key. We
could even omit the `/<groupID>` key suffix when encoding `GroupID(0)`
to make the encoding identical to the previous sentinel key encoding.

When a column is added to a table it will be assigned to a group. A
group will be selected so that raw column data size for the columns in
the group is less than some fixed size (e.g. 256 bytes). Note that
assigning every column to its own group would be effectively
equivalent to the current SQL to KV mapping.

# Drawbacks

* Grouping multiple columns into a single key/value pair will
necessitate changes to scan, insert, update and delete. Update will
require special attention as the `NULL`-ing of a column will be
performed via a `Put` instead of a `Del`.

# Alternatives

* We could introduce a richer KV layer api that pushes knowledge of
columns from the SQL layer down to the KV layer. This is not as
radical as it sounds as it is essentially the Bigtable/HBase
API. Specifically, we could enhance KV to know about rows and columns
where columns are identified by integers but not restricted to a
predefined schemas. This is actually a somewhat more limited version
of the Bigtable API which allows columns to be arbitrary strings and
also has the concept of column families. The upside to this approach
is that the encoding of the data at the MVCC layer would be
unchanged. We'd still have a key/value per column. But we'd get
something akin to prefix compression at the network level where
setting or retrieving multiple columns for a single row would only
send the row key once. Additionally, we could likely get away with a
single intent per row as opposed to an intent per column in the
existing system. The downside to this approach is that it appears to
be much more invasive than the column group change. Would every
consumer of the KV api need to change?

# Unresolved questions

# Performance Experiments

For the above `kv` table, we can approximate the benefit of this
change for a benchmark by not writing the sentinel key for each
row. The numbers below show the delta for that change using the `kv`
table structure described above (instead of the 1 column table
currently used in the `{Insert,Scan}` benchmarks).

```
name                   old time/op    new time/op    delta
Insert1_Cockroach-8       983µs ± 1%     948µs ± 0%   -3.53%    (p=0.000 n=9+9)
Insert10_Cockroach-8     1.72ms ± 1%    1.34ms ± 0%  -22.05%   (p=0.000 n=10+9)
Insert100_Cockroach-8    8.52ms ± 1%    4.99ms ± 1%  -41.42%  (p=0.000 n=10+10)
Scan1_Cockroach-8         348µs ± 1%     345µs ± 1%   -1.07%  (p=0.002 n=10+10)
Scan10_Cockroach-8        464µs ± 1%     419µs ± 1%   -9.68%  (p=0.000 n=10+10)
Scan100_Cockroach-8      1.33ms ± 1%    0.95ms ± 1%  -28.61%  (p=0.000 n=10+10)
```

While the benefit is fairly small for single row insertions, this is
only benchmarking the simplest of tables. We'd expect a bigger benefit
for tables with more columns.
