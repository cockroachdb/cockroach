- Feature Name: sql_column_groups
- Status: draft
- Start Date: 2015-12-14
- Authors: Daniel Harrison, Peter Mattis
- RFC PR: [#6712](https://github.com/cockroachdb/cockroach/pull/6712)
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

The value proto will remain almost the same. The raw_bytes field
currently holds a 4 byte CRC followed by the bytes for a single
"value" encoding (as opposed to the util/encoding "key" encodings) of
the `<columnVal>`. This latter part will be replaced by a series of
`<columnID>/<columnVal>` pairs where `<columnID>` is varint encoded
and `<columnVal>` is encoded using the same "value" encoding as
before. Similar to the existing convention, `NULL` column values will
not be present in the value.

The `ColumnDescriptor` proto will be extended with a `GroupID`
field. `GroupIDs` will be allocated per-table using a new
`TableDescriptor.NextGroupID` field. `GroupID(0)` will be always be
written for a row and will take the place of the sentinel key. We
could even omit the `/<groupID>` key suffix when encoding `GroupID(0)`
to make the encoding identical to the previous sentinel key encoding.

When a column is added to a table it will be assigned to a group. This
assignment will be done with a set of heuristics, but will be
override-able by the user at column creation using a SQL syntax
extension. Note that assigning every column to its own group would be
effectively equivalent to the current SQL to KV mapping.

## MetaRFC Note

In the interest of focusing the discussion, specific heuristics for
automatically assigning columns to groups will be considered out of
scope for the first commit of this RFC. Instead, the previous behavior
of one group per column will be used if not user specified. In
parallel to the implementation, heuristics will be reopened for
discussion. Resolving this satisfactorily is considered _required_ for
the scope of this work.

# Drawbacks

* This will change the on disk format of SQL tables. To maintain the
beta guarantees, we will either have to support both the old and the
new version or provide a migration tool.

* `UPDATE` will now have to fetch the previous values of every column
in a group being modified. However, we already have to scan before
every `UPDATE` so, at worst, this is just returning more fields from
the scan than before.

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

* Another alternative would be to omit the sentinel key when there is
non-NULL/non-primary-key column. For example, we could omit the
sentinel key in the following table because we know there will always
be one KV pair:

  ```
CREATE TABLE kv (
  k INT PRIMARY KEY,
  v INT NOT NULL
)
```

# Unresolved questions

* We've essentially developed our own encoding to use in the raw_bytes
field of the Value proto, which is giving up a lot of the benefits of
protos. Should we move away from the protobuf completely?

* Should column groups have names and metadata?

* Should changing the group of a column be available as an ALTER COLUMN command?

# Performance Experiments

Note: This is from @petermattis's original doc, so it might be out of
date. The conclusion is still valid though.

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
