Structured data encoding in CockroachDB SQL
===========================================

This document describes how the CockroachDB (CRDB) SQL layer encodes
structured data into key-value pairs (KVs), detailing the corner cases
of this logic as of March 2017 while glossing over the type-specific
encoding methods. The primary author is David Eisenstat
&lt;<eisen@cockroachlabs.com>&gt;.

See also the Cockroach Labs blog post [SQL in CockroachDB: Mapping Table
Data to Key-Value
Storage](https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/),
which predates column families, interleaving, and composite encoding.

Tables (primary indexes)
------------------------

A table consists of a rectangular array of SQL data and some metadata.
The metadata include a unique table ID, a nonempty list of primary-key
columns with an ascending/descending designation for each, and some
information about each column. Each column has a numeric ID that is
unique within the table, a SQL type, and a column family ID. A column
family is a maximal subset of columns with the same column family ID.
For more details, see
[pkg/sql/sqlbase/structured.proto](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/sqlbase/structured.proto).

CRDB encodes each row of a table as one or more KVs, one per column
family as needed. KV keys consist of several fields:

1.  The table ID
2.  The ID of the primary index (see Indexes below)
3.  The row data from the primary-key columns
4.  A column family ID.

KV values consist of the non-null row data from the non–primary-key
columns in the column family specified in the corresponding KV key. If
there is no non-null data, then the KV is not written unless the column
family ID is zero, indicating the special column family to which the
primary-key columns belong. The purpose of this exception is to retain
rows whose non–primary-key data is all null.

The relevant source code for encoding is in
[pkg/sql/rowwriter.go](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/rowwriter.go).
The relevant source code for decoding is in
[pkg/sql/sqlbase/rowfetcher.go](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/sqlbase/rowfetcher.go).

### Key encoding

CRDB creates KV keys by encoding the fields that comprise the key and
concatenating. This encoding has an inverse because no field code word
is a proper prefix of another field code word (i.e., the field code is
prefix-free).

Field code words start with a byte that conveys the type of the field
(which is coarser than the SQL type if that field happens to be a SQL
datum; see the Go function `EncodeTableKey`
([pkg/sql/sqlbase/table.go](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/sqlbase/table.go)))
and, for some types, the length of the code word and/or the encoding.
For more details, see
[pkg/util/encoding/encoding.go](https://github.com/cockroachdb/cockroach/blob/master/pkg/util/encoding/encoding.go).

The field code also has an order property so that the SQL layer and the
KV layer use the same order for primary keys, a necessity for efficient
partial scans. The mathematical encoding function enc for ascending
primary-key columns satisfies

> For data *x* and *y*, enc(*x*) ≤ enc(*y*) if and only if *x* ≤ *y*.

The mathematical encoding function enc for descending primary-key
columns satisfies

> For data *x* and *y*, enc(*x*) ≤ enc(*y*) if and only if *x* ≥ *y*.

See also the Go type `EncDatum`
([pkg/sql/sqlbase/encoded\_datum.go](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/sqlbase/encoded_datum.go)).

### Value encoding

KV values start with a four-byte checksum covering the whole KV and a
one-byte value type
([pkg/roachpb/data.proto](https://github.com/cockroachdb/cockroach/blob/master/pkg/roachpb/data.proto)
and
[pkg/roachpb/data.go](https://github.com/cockroachdb/cockroach/blob/master/pkg/roachpb/data.go)).
For most column families, the value type is `TUPLE`, which indicates the
following encoding. For each column in the column family sorted by ID,
append the joint encoding of the difference between this column ID and
the previous one (just the column ID if this column is the first) and
the datum type, followed by the datum encoding. The function responsible
for the joint encoding is `encodeValueTag`
([pkg/util/encoding/encoding.go](https://github.com/cockroachdb/cockroach/blob/master/pkg/util/encoding/encoding.go)),
which constructs an unsigned integer with the datum type in the low four
bits and the column ID difference in the high bits and then applies a
variable-length encoding. If the datum type is the special value
`SentinelType`, then the actual type follows as an unsigned integer with
the same variable-length encoding.

There is a backward-compatible encoding for column families (other than
column family ID zero) with one column (whose ID is the
`DefaultColumnID` of the column family). This encoding predates column
families. The relevant function is `MarshalColumnValue`
([pkg/sql/sqlbase/table.go](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/sqlbase/table.go)).

### Composite encoding

Some datum types – collated strings and (soon) decimals – have values
that compare equal but are not identical, e.g., 1.0 and 1.000. As keys,
1.0 and 1.000 must have the same encoding (by the order property of the
encoding function), but this encoding does not suffice to decode the
original key. (Collated strings use collation keys as defined by the
Unicode Collation Algorithm.) The solution is composite encoding – put
some information in the value as well.

As currently implemented, composite encoding causes data in the affected
columns to be encoded as values too (hence they appear in KV values for
the column family with ID zero). The value encoding does not require an
order property, so there is no loss of information.

### Interleaving

Tables can be interleaved in other tables. The parent table must have a
primary key that is a prefix (not necessarily proper) of the child
table. To encode a KV key for a row in the child table, encode it as if
it were a row in the parent table, then append the interleaving sentinel
(`EncodeNotNullDescending` in
[pkg/util/encoding/encoding.go](https://github.com/cockroachdb/cockroach/blob/master/pkg/util/encoding/encoding.go))
followed by the usual encoding starting with the table ID but without
the columns that appear in the parent table. The sentinel informs the
decoder that the row does not belong to the parent table.

Note that the parent table may itself be interleaved.

Indexes (secondary indexes)
---------------------------

In order to unify the handling of SQL tables and indexes, CRDB stores
the authoritative table data in what it terms the “primary index”. SQL
indexes are secondary indexes. All indexes have an ID that is unique
within their table.

The user-specified metadata for secondary indexes include a list of
indexed columns with an ascending/descending designation for each, which
determines the order in which rows are indexed, and a disjoint list of
stored columns, which are duplicated in the index for fast retrieval.
From these lists, CRDB derives a list of “extra” columns, which is the
union of the stored columns and the non-indexed primary-key columns.

Users also specify whether a secondary index is unique or not. Unique
secondary indexes impose a constraint on the table data that no two rows
be equal on the indexed columns.

### Key encoding

The main encoding function for secondary indexes is
`EncodeSecondaryIndex` in
[pkg/sql/sqlbase/table.go](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/sqlbase/table.go).
Each row gives rise to one KV per secondary index, whose KV key has
fields mirroring the primary index encoding:

1.  The table ID
2.  The index ID
3.  The row data from the indexed columns
4.  For non-unique indexes always and unique indexes when the indexed
    row data has a null, the row data from the extra columns.

Unique indexes relegate the data in extra columns to the KV value so
that the KV layer detects constraint violations. The special case for
rows with indexed nulls arises from the fact that null does not equal
null, hence such rows cannot be involved in a violation. They need a
unique KV key nonetheless, as do rows in non-unique indexes, which is
achieved by encoding the data in non-indexed primary-key columns in the
KV key. As a convenience, data in stored columns are kept in the KV key
as well in these cases.

Note that to ease decoding, rows with indexed nulls in unique indexes
store the extra columns both in the KV key and the KV value.

### Value encoding

While unique indexes store extra columns in the KV value, they do not
use the encoding described above for primary indexes. Instead, they use
usual key encoding, concatenate the encoded bytes, and store the result
as a `BYTES` value.

Non-unique indexes use a `BYTES` value too, which is empty when there is
no composite encoding.

### Composite encoding

Since secondary indexes use key encoding for all indexed and extra
columns, every column whose key encoding does not suffice for decoding
needs to be encoded again. The encoded bytes appear at the end of the
`BYTES` value (indexed columns followed by extra columns).

### Interleaving

Secondary indexes can be interleaved in tables. The list of primary-key
columns of the table must be a prefix (not necessarily proper) of the
list of indexed columns. The encoding changes are the same as for
table-table interleaving.
