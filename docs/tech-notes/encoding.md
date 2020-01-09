Structured data encoding in CockroachDB SQL
===========================================

Like many databases, CockroachDB (CRDB) encodes SQL data into key-value
(KV) pairs. The format evolves over time, with an eye toward backward
compatibility. This document describes format version 3 in detail except
for how CRDB encodes primitive values ([pkg/util/encoding/encoding.go]).

The Cockroach Labs blog post [SQL in CockroachDB: Mapping Table Data to
Key-Value Storage] covers format version 1, which predates column
families, interleaving, and composite encoding. Format version 2
introduced column families, covered in [Implementing Column Families in
CockroachDB]. See also the [column families RFC] and the [interleaving
RFC].

This document was originally written by David Eisenstat
&lt;<eisen@cockroachlabs.com>&gt;.

Tables (primary indexes)
------------------------

SQL tables consist of a rectangular array of data and some metadata. The
metadata include a unique table ID; a nonempty list of primary key
columns, each with an ascending/descending designation; and some
information about each column. Each column has a numeric ID that is
unique within the table, a SQL type, and a column family ID. A column
family is a maximal subset of columns with the same column family ID.
For more details, see [pkg/sql/sqlbase/structured.proto].

Each row of a table gives rise to one or more KV pairs, one per column
family as needed (see subsection NULL below). CRDB stores primary key
data in KV keys and other data in KV values so that it can use the KV
layer to prevent duplicate primary keys. For encoding, see
[pkg/sql/rowwriter.go]. For decoding, see
[pkg/sql/sqlbase/multirowfetcher.go].

### Key encoding

KV keys consist of several fields:

1.  The table ID
2.  The ID of the primary index (see section Indexes below)
3.  The primary key of the row, one field per primary key column in list
    order
4.  The column family ID.
5.  When the previous field is nonzero (non-sentinel), its length in
    bytes.

CRDB encodes these fields individually and concatenates the resulting
bytes. The decoder can determine the field boundaries because the field
encoding is [prefix-free].

Encoded fields start with a byte that indicates the type of the field.
For primary key fields, this type has a one-to-many relationship with
the SQL datum type. The SQL types `STRING` and `BYTES`, for example,
share an encoding. The relationship will become many-to-many when CRDB
introduces a [new `DECIMAL` encoding], since the old decoder will be
retained for backward compatibility.

The format of the remaining bytes depends on the field type. The details
(in [pkg/util/encoding/encoding.go]) are irrelevant here except that,
for primary key fields, these bytes have the following order property.
Consider a particular primary key column and let enc be the mathematical
function that maps SQL data in that column to bytes.

-   If the column has an ascending designation, then for data *x* and
    *y*, enc(*x*) ≤ enc(*y*) if and only if *x* ≤ *y*.
-   If the column has a descending designation, then for data *x* and
    *y*, enc(*x*) ≤ enc(*y*) if and only if *x* ≥ *y*.

In conjunction with prefix freedom, the order property ensures that the
SQL layer and the KV layer sort primary keys the same way.

For more details on primary key encoding, see `EncodeTableKey`
([pkg/sql/sqlbase/table.go]). See also `EncDatum`
([pkg/sql/sqlbase/encoded\_datum.go]).

### Value encoding

KV values consist of

1.  A four-byte checksum covering the whole KV pair
2.  A one-byte value type (see the enumeration `ValueType` in
    [pkg/roachpb/data.proto])
3.  Data from where the row specified in the KV key intersects the
    specified column family, including composite encodings of primary
    key columns that are members of the specified column family.

The value type defaults to `TUPLE`, which indicates the following
encoding. (For other values, see subsection Single-column column
families below.) For each column in the column family sorted by column
ID, encode the column ID difference and the datum encoding type
(unrelated to the value type!) jointly, followed by the datum itself.
The column ID difference is the column ID minus the previous column ID
if this column is not the first, else the column ID. The joint encoding
is commonly one byte, which displays conveniently in hexadecimal as the
column ID difference followed by the datum encoding type.

The Go function that performs the joint encoding is `encodeValueTag`
([pkg/util/encoding/encoding.go]), which emits an unsigned integer with
a variable-length encoding. The low four bits of the integer contain the
datum encoding type. The rest contain the column ID difference. As an
alternative for datum encoding types greater than 14, `encodeValueTag`
sets the low four bits to `SentinelType` (15) and emits the actual datum
encoding type next.

**Note:** Values for sequences are a special case: the sequence value is
encoded as if the sequence were a one-row, one-column table, with the
key structured in the usual way: `/Table/<id>/<index>/<pk val>/<family>`.
However, the value is a bare int64; it doesn't use the encoding
specified here. This is because it is incremented using the KV
`Increment` operation so that the increment can be done in one
roundtrip, not a read followed by a write as would be required by a
normal SQL `UPDATE`.

An alternative design would be to teach the KV Inc operation to
understand SQL value encoding so that the sequence could be encoded
consistently with tables, but that would break the KV/SQL abstraction
barrier.

The code that performs generation of keys and values for primary indexes
can be found in `prepareInsertOrUpdateBatch`([pkg/sql/row/writer.go]).

### Sentinel KV pairs

The column family with ID 0 is special because it contains the primary
key columns. The KV pairs arising from this column family are called
sentinel KV pairs. CRDB emits sentinel KV pairs regardless of whether
the KV value has other data, to guarantee that primary keys appear in at
least one KV pair. (Even if there are other column families, their KV
pairs may be suppressed; see subsection NULL below.)

Note that in system tables that use multiple column families, such as
system.zones or system.namespace, there may not be any sentinel KV pair at all.
This is because of the fact that the database writes to these system tables
using raw KV puts and does not include the logic to write a sentinel KV. KV
decoding code that needs to understand system tables must be aware of this
possibility.

### Single-column column families

Before column families (i.e., in format version 1), non-sentinel KV keys
had a column ID where the column family ID is now. Non-sentinel KV
values contained exactly one datum, whose encoding was indicated by the
one-byte value type (see `MarshalColumnValue` in
[pkg/sql/sqlbase/table.go]). Unlike the `TUPLE` encoding, this encoding
did not need to be prefix-free, which was a boon for strings.

On upgrading to format version 2 or higher, CRDB puts each existing
column in a column family whose ID is the same as the column ID. This
allows backward-compatible encoding and decoding. The encoder uses the
old format for single-column column families when the ID of that column
equals the `DefaultColumnID` of the column family
([pkg/sql/sqlbase/structured.proto]).

### NULL

SQL `NULL` has no explicit encoding in tables (primary indexes).
Instead, CRDB encodes each row as if the columns where that row is null
did not exist. If all of the columns in a column family are null, then
the corresponding KV pair is suppressed. The motivation for this design
is that adding a column does not require existing data to be re-encoded.

### Example dump

The commands below create a table and insert some data. An annotated KV
dump follows.

    CREATE TABLE accounts (
      id INT PRIMARY KEY,
      owner STRING,
      balance DECIMAL,
      FAMILY f0 (id, balance),
      FAMILY f1 (owner)
    );

    INSERT INTO accounts VALUES
      (1, 'Alice', 10000.50),
      (2, 'Bob', 25000.00),
      (3, 'Carol', NULL),
      (4, NULL, 9400.10),
      (5, NULL, NULL);

Here is the relevant output from
`cockroach debug rocksdb scan --value_hex`, with annotations.

    /Table/51/1/1/0/1489427290.811792567,0 : 0xB244BD870A3505348D0F4272
           ^- ^ ^ ^                            ^-------^-^^^-----------
           |  | | |                            |       | |||
           Table ID (accounts)                 Checksum| |||
              | | |                                    | |||
              Index ID                                 Value type (TUPLE)
                | |                                      |||
                Primary key (id = 1)                     Column ID difference
                  |                                       ||
                  Column family ID (f0)                   Datum encoding type (Decimal)
                                                           |
                                                           Datum encoding (10000.50)

    /Table/51/1/1/1/1/1489427290.811792567,0 : 0x30C8FBD403416C696365
           ^- ^ ^ ^ ^                            ^-------^-^---------
           |  | | | |                            |       | |
           Table ID (accounts)                   Checksum| |
              | | | |                                    | |
              Index ID                                   Value type (BYTES)
                | | |                                      |
                Primary key (id = 1)                       Datum encoding ('Alice')
                  | |
                  Column family ID (f1)
                    |
                    Column family ID encoding length

    /Table/51/1/2/0/1489427290.811792567,0 : 0x2C8E35730A3505348D2625A0
                ^                                          ^-----------
                2                                          25000.00

    /Table/51/1/2/1/1/1489427290.811792567,0 : 0xE911770C03426F62
                ^                                          ^-----
                2                                          'Bob'

    /Table/51/1/3/0/1489427290.811792567,0 : 0xCF8B38950A
                ^
                3

    /Table/51/1/3/1/1/1489427290.811792567,0 : 0x538EE3D6034361726F6C
                ^                                          ^---------
                3                                          'Carol'

    /Table/51/1/4/0/1489427290.811792567,0 : 0x247286F30A3505348C0E57EA
                ^                                          ^-----------
                4                                          9400.10

    /Table/51/1/5/0/1489427290.811792567,0 : 0xCB0644270A
                ^
                5

### Composite encoding

There exist decimal numbers and collated strings that are equal but not
identical, e.g., 1.0 and 1.000. This is problematic because in primary
keys, 1.0 and 1.000 must have the same encoding, which precludes
lossless decoding. Worse, the encoding of collated strings in primary
keys is defined by the [Unicode Collation Algorithm], which may not even
have [an efficient partial inverse].

When collated strings and [(soon) decimals][new `DECIMAL` encoding]
appear in primary keys, they have composite encoding. For collated
strings, this means encoding data as both a key and value, with the
latter appearing in the sentinel KV value (naturally, since the column
belongs to the column family with ID 0).

Example schema and data:

    CREATE TABLE owners (
      owner STRING COLLATE en PRIMARY KEY
    );

    INSERT INTO owners VALUES
      ('Bob' COLLATE en),
      ('Ted' COLLATE en);

Example dump:

    /Table/51/1/"\x16\x05\x17q\x16\x05\x00\x00\x00 \x00 \x00 \x00\x00\b\x02\x02"/0/1489502864.477790157,0 : 0xDC5FDAE10A1603426F62
                ^---------------------------------------------------------------                                          ^-------
                Collation key for 'Bob'                                                                                   'Bob'

    /Table/51/1/"\x18\x16\x16L\x161\x00\x00\x00 \x00 \x00 \x00\x00\b\x02\x02"/0/1489502864.477790157,0 : 0x8B30B9290A1603546564
                ^------------------------------------------------------------                                          ^-------
                Collation key for 'Ted'                                                                                'Ted'

Indexes (secondary indexes)
---------------------------

To unify the handling of SQL tables and indexes, CRDB stores the
authoritative table data in what is termed the primary index. SQL
indexes are secondary indexes. All indexes have an ID that is unique
within their table.

The user-specified metadata for secondary indexes include a nonempty
list of indexed columns, each with an ascending/descending designation,
and a disjoint list of stored columns. The first list determines how the
index is sorted, and columns from both lists can be read directly from
the index.

Users also specify whether a secondary index should be unique. Unique
secondary indexes constrain the table data not to have two rows where,
for each indexed column, the data therein are non-null and equal.

As of #42073 (after version 19.2), secondary indexes have been extended to
include support for column families. These families are the same as the ones
defined upon the table. Families will apply to the stored columns in the index.
Like in primary indexes, column family 0 on a secondary index will always be
present for a row so that each row in the index has at least one k/v entry.

### Key encoding

The main encoding function for secondary indexes is
`EncodeSecondaryIndex` in [pkg/sql/sqlbase/table.go]. Each row gives
rise to one KV pair per secondary index, whose KV key has fields
mirroring the primary index encoding:

1.  The table ID
2.  The index ID
3.  Data from where the row intersects the indexed columns
4.  If the index is non-unique or the row has a NULL in an indexed
    column, data from where the row intersects the non-indexed primary
    key (implicit) columns
5.  If the index is non-unique or the row has a NULL in an indexed
    column, and the index uses the old format for stored columns, data
    from where the row intersects the stored columns
6.  The column family ID.
7.  When the previous field is nonzero (non-sentinel), its length in bytes.

Unique indexes relegate the data in extra columns to KV values so that
the KV layer detects constraint violations. The special case for an
indexed NULL arises from the fact that NULL does not equal itself, hence
rows with an indexed NULL cannot be involved in a violation. They need a
unique KV key nonetheless, as do rows in non-unique indexes, which is
achieved by including the non-indexed primary key data. For the sake of
simplicity, data in stored columns are also included.

### Value encoding
KV values for secondary indexes are encoded using the following rules:

If the value corresponds to column family 0:

The KV value will have value type bytes, and will consist of
1.  If the index is unique, data from where the row intersects the
    non-indexed primary key (implicit) columns, encoded as in the KV key
2.  If the index is unique, and the index uses the old format for stored
    columns, data from where the row intersects the stored columns,
    encoded as in the KV key
3.  If needed, `TUPLE`-encoded bytes for non-null composite and stored
    column data in family 0 (new format).

Since column family 0 is always included, it contains extra information
that the index stores in the value, such as composite column values and
stored primary key columns. Note that this is different than the encoding of
composite indexed columns values in primary key columns, where the composite
value component of an indexed column is placed in the KV pair corresponding
to the column family of the indexed column. All of these fields are optional,
so the `BYTES` value may be empty. Note that, in a unique index, rows with
a NULL in an indexed column have their implicit column data stored in both the
KV key and the KV value. (Ditto for stored column data in the old format.)

For indexes with more than one column family, the remaining column families'
KV values will have value type `TUPLE` and will consist of all stored
columns in that family in the `TUPLE` encoded format.

### Backwards Compatibility With Indexes Encoded Without Families

Index descriptors hold on to a version bit that denotes what encoding
format the descriptor was written in. The default value of the bit denotes
the original secondary index encoding, and indexes created when all
nodes in a cluster are version 20.1 or greater will have the version representing
secondary indexes with column families.

### Example dump

Example schema and data:

    CREATE TABLE accounts (
      id INT PRIMARY KEY,
      owner STRING,
      balance DECIMAL,
      UNIQUE INDEX i2 (owner) STORING (balance),
      INDEX i3 (owner) STORING (balance)
    );

    INSERT INTO accounts VALUES
      (1, 'Alice', 10000.50),
      (2, 'Bob', 25000.00),
      (3, 'Carol', NULL),
      (4, NULL, 9400.10),
      (5, NULL, NULL);

Index ID 1 is the primary index.

    /Table/51/1/1/0/1489504989.617188491,0 : 0x4AAC12300A2605416C6963651505348D0F4272
    /Table/51/1/2/0/1489504989.617188491,0 : 0x148941AD0A2603426F621505348D2625A0
    /Table/51/1/3/0/1489504989.617188491,0 : 0xB1D0B5390A26054361726F6C
    /Table/51/1/4/0/1489504989.617188491,0 : 0x247286F30A3505348C0E57EA
    /Table/51/1/5/0/1489504989.617188491,0 : 0xCB0644270A

#### Old STORING format

Index ID 2 is the unique secondary index `i2`.

    /Table/51/2/NULL/4/9400.1/0/1489504989.617188491,0 : 0x01CF9BB0038C2BBD011400
                ^--- ^ ^-----                                      ^-^-^---------
      Indexed column | Stored column                           BYTES 4 9400.1
                     Implicit column

    /Table/51/2/NULL/5/NULL/0/1489504989.617188491,0 : 0xE86B1271038D00
                ^--- ^ ^---                                      ^-^-^-
      Indexed column | Stored column                         BYTES 5 NULL
                     Implicit column

    /Table/51/2/"Alice"/0/1489504989.617188491,0 : 0x285AC6F303892C0301016400
                ^------                                      ^-^-^-----------
         Indexed column                                  BYTES 1 10000.5

    /Table/51/2/"Bob"/0/1489504989.617188491,0 : 0x23514F1F038A2C056400
                ^----                                      ^-^-^-------
       Indexed column                                  BYTES 2 2.5E+4

    /Table/51/2/"Carol"/0/1489504989.617188491,0 : 0xE98BFEE6038B00
                ^------                                      ^-^-^-
         Indexed column                                  BYTES 3 NULL

Index ID 3 is the non-unique secondary index `i3`.

    /Table/51/3/NULL/4/9400.1/0/1489504989.617188491,0 : 0xEEFAED0403
                ^--- ^ ^-----                                      ^-
      Indexed column | Stored column                           BYTES
                     Implicit column

    /Table/51/3/NULL/5/NULL/0/1489504989.617188491,0 : 0xBE090D2003
                ^--- ^ ^---                                      ^-
      Indexed column | Stored column                         BYTES
                     Implicit column

    /Table/51/3/"Alice"/1/10000.5/0/1489504989.617188491,0 : 0x7B4964C303
                ^------ ^ ^------                                      ^-
         Indexed column | Stored column                            BYTES
                        Implicit column

    /Table/51/3/"Bob"/2/2.5E+4/0/1489504989.617188491,0 : 0xDF24708303
                ^---- ^ ^-----                                      ^-
       Indexed column | Stored column                           BYTES
                      Implicit column

    /Table/51/3/"Carol"/3/NULL/0/1489504989.617188491,0 : 0x96CA34AD03
                ^------ ^ ^---                                      ^-
         Indexed column | Stored column                         BYTES
                        Implicit column

#### New STORING format

Index ID 2 is the unique secondary index `i2`.

    /Table/51/2/NULL/4/0/1492010940.897101344,0 : 0x7F2009CC038C3505348C0E57EA
                ^--- ^                                      ^-^-^-------------
      Indexed column Implicit column                    BYTES 4 9400.10

    /Table/51/2/NULL/5/0/1492010940.897101344,0 : 0x48047B1A038D
                ^--- ^                                      ^-^-
      Indexed column Implicit column                    BYTES 5

    /Table/51/2/"Alice"/0/1492010940.897101344,0 : 0x24090BCE03893505348D0F4272
                ^------                                      ^-^-^-------------
         Indexed column                                  BYTES 1 10000.50

    /Table/51/2/"Bob"/0/1492010940.897101344,0 : 0x54353EB9038A3505348D2625A0
                ^----                                      ^-^-^-------------
       Indexed column                                  BYTES 2 25000.00

    /Table/51/2/"Carol"/0/1492010940.897101344,0 : 0xE731A320038B
                ^------                                      ^-^-
         Indexed column                                  BYTES 3

Index ID 3 is the non-unique secondary index `i3`.

    /Table/51/3/NULL/4/0/1492010940.897101344,0 : 0x17C357B0033505348C0E57EA
                ^--- ^                                      ^-^-------------
      Indexed column Implicit column                    BYTES 9400.10

    /Table/51/3/NULL/5/0/1492010940.897101344,0 : 0x844708BC03
                ^--- ^                                      ^-
      Indexed column Implicit column                    BYTES

    /Table/51/3/"Alice"/1/0/1492010940.897101344,0 : 0x3AD2E728033505348D0F4272
                ^------ ^                                      ^-^-------------
         Indexed column Implicit column                    BYTES 10000.50

    /Table/51/3/"Bob"/2/0/1492010940.897101344,0 : 0x7F1225A4033505348D2625A0
                ^---- ^                                      ^-^-------------
       Indexed column Implicit column                    BYTES 25000.00

    /Table/51/3/"Carol"/3/0/1492010940.897101344,0 : 0x45C61B8403
                ^------ ^                                      ^-
         Indexed column Implicit column                    BYTES

### Example dump with families
```
CREATE TABLE t (
	a INT, b INT, c INT, d INT, e INT, f INT,
	PRIMARY KEY (a, b),
	UNIQUE INDEX i (d, e) STORING (c, f),
	FAMILY (a, b, c), FAMILY (d, e), FAMILY (f)
);

INSERT INTO t VALUES (1, 2, 3, 4, 5, 6);

/Table/52/2/4/5/0/1572546219.386986000,0 : 0xBDD6D93003898A3306
            ^-- ^                                    ^_^_______      
 Indexed cols   Column family 0                  BYTES Stored PK cols + column c
// Notice that /Table/52/2/4/5/1/1/ is not present, because these values are already indexed
/Table/52/2/4/5/2/1/1572546219.386986000,0 : 0x46CC99AE0A630C
                ^__                                    ^_^___ 
         Column Family 2                            TUPLE  column f
```

### Composite encoding

Secondary indexes use key encoding for all indexed columns, implicit
columns, and stored columns in the old format. Every datum whose key
encoding does not suffice for decoding (collated strings, floating-point
and decimal negative zero, decimals with trailing zeros) is encoded
again, in the same `TUPLE` that contains stored column data in the new
format.

Example schema and data:

    CREATE TABLE owners (
      id INT PRIMARY KEY,
      owner STRING COLLATE en,
      INDEX i2 (owner)
    );

    INSERT INTO owners VALUES
      (1, 'Ted' COLLATE en),
      (2, 'Bob' COLLATE en),
      (3, NULL);

Index ID 1 is the primary index.

    /Table/51/1/1/0/1492008659.730236666,0 : 0x6CA87E2B0A2603546564
    /Table/51/1/2/0/1492008659.730236666,0 : 0xE900EBB50A2603426F62
    /Table/51/1/3/0/1492008659.730236666,0 : 0xCF8B38950A

Index ID 2 is the secondary index `i2`.

    /Table/51/2/NULL/3/0/1492008659.730236666,0 : 0xBDAA5DBE03
                ^---                                        ^-
                Indexed column                          BYTES

    /Table/51/2/"\x16\x05\x17q\x16\x05\x00\x00\x00 \x00 \x00 \x00\x00\b\x02\x02"/2/0/1492008659.730236666,0 : 0x4A8239F6032603426F62
                ^---------------------------------------------------------------                                        ^-^---------
                Indexed column: Collation key for 'Bob'                                                             BYTES 'Bob'

    /Table/51/2/"\x18\x16\x16L\x161\x00\x00\x00 \x00 \x00 \x00\x00\b\x02\x02"/1/0/1492008659.730236666,0 : 0x747DA39A032603546564
                ^------------------------------------------------------------                                        ^-^---------
                Indexed column: Collation key for 'Ted'                                                          BYTES 'Ted'

Interleaving
------------

By default, indexes (in CRDB terminology, so both primary and secondary)
occupy disjoint KV key spans. Users can request that an index be
interleaved with another index, which improves the efficiency of joining
them.

One index, the parent, must have a primary key that, ignoring column
names, is a prefix (not necessarily proper) of the other index, the
child. The parent, which currently must be a primary index, has its
usual encoding. To encode a KV key in the child, encode it as if it were
in the parent but with an interleaving sentinel
(`EncodeNotNullDescending` in [pkg/util/encoding/encoding.go]) where the
column family ID would be. Append the non-interleaved child encoding but
without the parent columns. The sentinel informs the decoder that the
row does not belong to the parent table.

Note that the parent may itself be interleaved. In general, the
interleaving relationships constitute an [arborescence].

Example schema and data:

    CREATE TABLE owners (
      owner_id INT PRIMARY KEY,
      owner STRING
    );

    CREATE TABLE accounts (
      owner_id INT,
      account_id INT,
      balance DECIMAL,
      PRIMARY KEY (owner_id, account_id)
    ) INTERLEAVE IN PARENT owners (owner_id);

    INSERT INTO owners VALUES (19, 'Alice');
    INSERT INTO accounts VALUES (19, 83, 10000.50);

Example dump:

    /Table/51/1/19/0/1489433137.133889094,0 : 0xDBCE04550A2605416C696365
           ^- ^ ^- ^                            ^-------^-^^^-----------
           |  | |  |                            |       | |||
           Table ID (owners)                    Checksum| |||
              | |  |                                    | |||
              Index ID                                  Value type (TUPLE)
                |  |                                      |||
                Primary key (owner_id = 19)               Column ID difference
                   |                                       ||
                   Column family ID                        Datum encoding type (Bytes)
                                                            |
                                                            Datum encoding ('Alice')

    /Table/51/1/19/#/52/1/83/0/1489433137.137447008,0 : 0x691956790A3505348D0F4272
           ^- ^ ^- ^ ^- ^ ^- ^                            ^-------^-^^^-----------
           |  | |  | |  | |  |                            |       | |||
           Table ID (owners) |                            Checksum| |||
              | |  | |  | |  |                                    | |||
              Index ID  | |  |                                    Value type (TUPLE)
                |  | |  | |  |                                      |||
                Primary key (owner_id = 19)                         Column ID difference
                   | |  | |  |                                       ||
                   Interleaving sentinel                             Datum encoding type (Decimal)
                     |  | |  |                                        |
                     Table ID (accounts)                              Datum encoding (10000.50)
                        | |  |
                        Index ID
                          |  |
                          Primary key (account_id = 83)
                             |
                             Column family ID

  [pkg/util/encoding/encoding.go]: https://github.com/cockroachdb/cockroach/blob/master/pkg/util/encoding/encoding.go
  [SQL in CockroachDB: Mapping Table Data to Key-Value Storage]: https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/
  [Implementing Column Families in CockroachDB]: https://www.cockroachlabs.com/blog/sql-cockroachdb-column-families/
  [column families RFC]: https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20151214_sql_column_families.md
  [interleaving RFC]: https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160624_sql_interleaved_tables.md
  [pkg/sql/sqlbase/structured.proto]: https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/sqlbase/structured.proto
  [pkg/sql/rowwriter.go]: https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/sqlbase/rowwriter.go
  [pkg/sql/sqlbase/multirowfetcher.go]: https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/sqlbase/multirowfetcher.go
  [prefix-free]: https://en.wikipedia.org/wiki/Prefix_code
  [new `DECIMAL` encoding]: https://github.com/cockroachdb/cockroach/issues/13384#issuecomment-277120394
  [pkg/sql/sqlbase/table.go]: https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/sqlbase/table.go
  [pkg/sql/sqlbase/encoded\_datum.go]: https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/sqlbase/encoded_datum.go
  [pkg/roachpb/data.proto]: https://github.com/cockroachdb/cockroach/blob/master/pkg/roachpb/data.proto
  [Unicode Collation Algorithm]: http://unicode.org/reports/tr10/
  [an efficient partial inverse]: http://stackoverflow.com/q/23609457/2144669
  [arborescence]: https://en.wikipedia.org/wiki/Arborescence_(graph_theory)
