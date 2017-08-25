- Feature Name: SQL Table Partitioning
- Status: draft
- Start Date: 2017-01-25
- Authors: Daniel Harrison, David Taylor
- RFC PR: (PR # after acceptance of initial draft)

# Summary

Give users of CockroachDB row-level control over where data is stored.

# Motivation

One of the defining characteristics of CockroachDB is its distributed
architecture, with different nodes storing different subsets, or ranges, of
data. In many use cases, an operator may wish to explicitly control where
row-level data is stored -- for example storing the data for users on nearby
servers for latency or regulatory reasons, or storing recent data on a different
hardware class than archival data. The initial motivation is the geographic
partition, though this will be developed as a special case of the more general
feature.

# Detailed Design

NOTE: We concluded that we will use primary key prefix described below to encode
keys in partitioned tables, and will be replacing this document with a full
design doc based on that decision.

A partition is a subset of table data (i.e. rows) determined by the values in
one or more columns of the table (the "partition columns"), the physical storage
of which can be configured. CockroachDB is a monolithic key-value map and uses
ranges of the keyspace as units of replication. Zone configs control where
ranges of keyspace are physically stored. Thus partitioning consists of
organizing rows into ranges of keys against which zone configs can be applied.

The assignment of values to a specific partition is done in one of three ways:
List, Range, or Unique

NB: examples are meant purely to illustrate the partitioning modes, and are not
a concrete syntax suggestion at this time.

- Range partitions are manually specified by split points:
  - e.g. `PARTITION (col) BY RANGE (2001, 2015, 2016)`
- List partitions are manually specified by listing the values in the domain of
  the partition columns, optionally with a default if none match:
  - e.g. `PARTITION (col) BY LIST (“US”,“CA”), (“DE”), (DEFAULT)`
- Unique partitions are automatically created, one for each unique tuple of
  partition columns values:
  - e.g. `PARTITION (col) BY UNIQUE`
  - not included in our initial implementation, but will be added later.
  - Essentially the same as LIST, with the system creating them automatically,
    one per unique value.

Both primary and secondary index data can be partitioned. Each index can only be
partitioned by one set of columns. Secondary indexes on partitioned tables
require being explicitly declared as partitioned or unpartitioned.

While these definitions are in terms of columns, like other systems, we will
allow partitioning by hashes of columns or by other pure functions (e.g.
extracting the year from a date column, as opposed to impure functions, such as
using the current date to make a rolling-window). We will not require a user to
maintain their own explicit denormalizations of a function of a column on which
to partition -- if denormalizations are required, they will be silently
maintained by the system.

# Examples

## Geographic Partitioning

A typical `users` table along with a related auxiliary table (e.g. `orders`)
might looks something like this:

```
CREATE TABLE users (
  user_id SERIAL PRIMARY KEY,
  email STRING UNIQUE,
  region STRING,
  PRIMARY KEY (user_id)
)

CREATE TABLE orders (
  order_id SERIAL PRIMARY KEY,
  user_id INT,
  region STRING,
  PRIMARY KEY (order_id),
  INDEX (user_id),
  FOREIGN KEY (user_id, region) REFERENCES users (user_id),
)
```

For the archetypical example of partitioning users by geographic region,
designating their North American users into a separate partition from the
rest-of-world: `PARTITION BY LIST (region) VALUES ("US", "CA", “MX”), (DEFAULT)`
to the tables and to their secondary indexes.

Note: `users.email` is used to uniquely identify a user at login , and thus
requires global (non-partitioned) uniqueness enforcement. `user_id` and
`order_id` are also unique, even across partitions. The `orders.user_id` index
is partitioned such that the index data for a region is located in that region.
This is important for reduced latency or regulations (example: all order data
for EU users must stay in the EU) or both.

A query to look a user up by `email` for login:

`SELECT region, id FROM users WHERE email = $1`

A query to look a user up by `region` and `user_id` (because the partition is
specified, this should be answerable using data that is geographically nearby):

`SELECT * FROM users WHERE region = $1 AND id = $2`

A query to look a user's orders up by `region` and `user_id` (because the
partition is specified, this should be answerable using data that is
geographically nearby):

`SELECT id FROM orders WHERE region = $1 AND user_id = $3`

## Date Partitioning

```
CREATE TABLE articles (
  article_id SERIAL PRIMARY KEY,
  publish_date DATE,
  view_count INT,
  text BLOB,
)
```
The table is partitioned by year so sufficiently old articles can be moved onto
cheaper, but slower, storage (e.g. spinning disk hard drives vs solid state):
`PARTITION BY RANGE extract(‘year’, publish_date)) `

Note:
`articles.article_id` is unique.

A query to look an article up by `article_id` and `publish_date` (most
publishers already include this in their urls, this is necessary to make it a
point lookup instead of all partitions):

`SELECT * FROM articles WHERE article_id = $1 AND publish_date = $2`

A query to update the publish_date of an article (at publish time):

`UPDATE articles SET publish_date = current_date() WHERE article_id = $1`

# Partitioning Column Eligibility

To provide uniqueness guarantees as well as predictable access performance, we
need to be able to deterministically compute an index key from the columns
included in that index. Because the partition is encoded in the key, we require
that indexes can only be partitioned by a subset of the column(s) indexed or a
pure function of those columns.

Consequently, a table can only be partitioned by columns in its primary key.

This may lead to a different primary key that would be chosen if the table were
not partitioned. As a result, we introduce the following terminology: ‘Logical’
primary key: The values you would normally include in the unpartitioned table’s
primary key, such as user_id in the `users` example above. Table Primary Key
(Table PK): The Logical PK + the partition columns, e.g.  `user_id,region`
above.

In the above `users` table that is partitioned by `region`, the table primary
key must be changed to include `region`. To continue to ensure uniqueness of
`user.id`, a unique and unpartitioned secondary index on `user_id` would be
required.

We could silently add the necessary columns for partitioning when the index is
created, but this would violate user expectations in several ways. Most notably,
queries that specify all columns in an index (as specified at creation and as
returned by table reflection like information_schema) are expected to be point
lookups.

Similarly, automatically adding additional indexes, like the unpartitioned secondary
index mentioned above to preserve uniqueness, again risks violation of
user expectations, both with respect to the cost of writes additional index may
add, as well as concerns over data which must be partitioned.

To aid the user, we can provide guidance in the error returned, but will
initially default to erroring rather than silently changing indexed columns or
adding additional indexes.

# Key Encoding

Key encoding is an open question and the motivation behind this preliminary RFC.
There are two reasonable options and, once that decision is made, a follow up
will fill out the remaining details.

Several components of the SQL Partitions design work similarly with either key
encoding, including secondary index and fk encoding and query planning and index
selection, as discussed further below.

## Proposal: Partition IDs

We assign each partition a unique ID and use it, instead of the TableID, as a
prefix in the encoded keys. As a result, each partition is then naturally
grouped together.

Primary Index for `users`:

`/PartitionID/IndexID/<user_id>/0 -> [email][region]`

Global unique secondary index for `users.email`:

`/TableID/IndexID/<email>/0 -> [user_id][region]`

Partitioned non-unique secondary index for `orders.user_id`:

`/PartitionID/indexID/<user_id>/<order_id> -> null`

Essentially, this is denormalizing a row’s partition into an integer id in the
key.

Reading the table would still involve resolving the table name to a single table
descriptor (with a single table ID), that describes the table’s schema, as well
as describing its partitioning -- only when generating keys for individual rows
and spans (i.e. when calling `MakeTablePrefix`) would the individual partition
IDs be used.

## Proposal: Index Key Prefix

The partition columns are required to be a prefix of the table primary key or
indexed columns. The row key is encoded as the table ID, the index ID, and then
each of the table primary key or index columns, in order, exactly as they are
currently. Because the partition columns come first, the rows in a partition
will be grouped together.

Primary Index for `users`:

`/TableID/IndexID/<region>/<user_id>/0 -> [email]`

Global unique secondary index for `users.email`:

`/TableID/IndexID/<email> -> <region><user_id>`

Partitioned non-unique secondary index for `orders.user_id`:

`/TableID/IndexID/<region>/<user_id>/<order_id> -> null`

When non-adjacent values are defined as mapped to the same partition (e.g. `us`,
`ca` and `mx` are mapped to a North America geo-partition), a copy of or pointer
to the partition zone config would be applied for each disjoint prefix in that
partition.

# Comparing Key Encoding Proposals

As detailed below, the Partition IDs proposal needs [fewer zone config
changes](#zone-configs) and more elegantly allows partitions to be a [function
of columns](#denormalized-vs-evaluated-functions-of-columns),
while making any [repartitionings](#repartitioning-and-initial-partitioning)
quite expensive.

The Index Key Prefix proposal makes select repartitionings inexpensive, but
requires zone config changes.

## `MakeTablePrefix` and Plumbing Table Metadata

In the Partition ID proposal, the first part of the index key can be either a
Partition ID or a Table ID. Because MakeTablePrefix expects only Table IDs,
calls to it would need to be audited.

Some of these calls are below the sql & sqlbase packages and are used for
splitting ranges at table boundaries and zone configs. All other calls will be
replaced by a new function that is partition aware and given a TableDescriptor
and values for the partition columns.

In the Index Key Prefix proposal, the table ID remains unchanged, so we don’t
need this MakeTablePrefix audit. However, both zone configs and table splits
need to become aware of partition boundaries.

## Repartitioning (and Initial Partitioning)

In the Partition ID proposal, the partitioning is denormalized into each index
key, since the computed partition ID is copied into the key itself. Thus, any
partitioning changes, e.g. changing the assignment of "mx" to a different
partition, requires individually rewriting all keys in the index.

In the Index Key Prefix proposal, any repartition of an index is a metadata
change with no key rewriting (unless, of course, the columns in the index need
to change). After repartitioning, our existing replica rebalancing moves the
ranges.

## Denormalized vs. Evaluated Functions of Columns

When partitioning on a function of a column, Index Key Prefix requires that the
evaluation of that function be in a column that can then be used as prefix of
the key (or that we implement first class support for computed indexes). Making
this seamless for users requires maintaining the denormalized evaluation of the
function autoamtically, e.g. “materialized virtual column”.

Partition IDs denormalize the entire computation of row’s assigned partition,
and do not need to persist the evaluation of any intermediate functions, and
thus do not rely on computed index / materialized virtual column support.

## Zone Configs

Zone configs are CockroachDB's mechanism for specifying data locality and
replication requirements. They are stored in the `system.zones` table, keyed by
an integer ID of a database or a table. When looking up a zone config for a row,
the system first looks for a zone config with an ID equal to table ID the key.
If it doesn't find one, it falls back to the database ID, and finally to the
system default config.

### Partition ID Implications

In the Partition ID proposal, zone configs remain essentially unchanged. Because
the partition ID is encoded in the place the table ID normally would be, it is
naturally looked up first. If there was not a zone config for the partition it
falls back to the table, database, and system default as before. Doing so would
likely require storing pointers in the descriptor namespace for each ID (though
these likely would not require leasing machinery, since they can be treated as
immutable, unlike table descriptors, which use leasing to ensure the
consistency of mutable schema information).

### Index Key Prefix Implications

In the Index Key Prefix proposal, zone configs need to be made more granular. We
propose achieving this in one of two ways: embedded subzones or table subzones.

#### Embedded Subzones

The Embedded Subzones proposal adds a field containing repeated pairs of a zone
config and a keyrange. The keyranges do not overlap and only overrides are set
in these subzones. Lookups work as before but with an additional step at the end
to find any subzone overrides.

The table-level zone config is stored as one kv entry and frequently serialized
and deserialized, so a number of space optimizations are used. The encoded id
prefix is omitted, since it's the same for all subzones. If the end of the
range is equal to `startKey.PrefixEnd()` (this will be true
for all initial uses of subzones), it may be omitted.

With these optimizations, each subzone with one key-value constraint is 16 bytes
(for the proto encoding) plus the length of the key and the value (`country=us`
is 9) plus the encoded primary key prefix (`/<IndexID=1>/"US"` is 3), for a
total of ~30 bytes. This puts a practical upper limit on the number of
partitions we can support in a table.

#### Table Subzones

The Table Subzones proposal adds a `system.subzones`. It has the same schema as
`system.zones` but with a start_key and end_key similar to how RangeDescriptors
are stored. This table is used for partition zone configs and could eventually
replace the `system.zones` table.

# Unaffected by Key Encoding

These are some of the questions that will need to be address later, after we
have chosen a specific key encoding.

## Secondary Indexes & FKs

Since table primary keys (and secondary indexes) are required above to include
partition columns, our existing index data for secondary indexes (and thus
foreign keys) already includes the data needed to derive partitions as well, and
is unchanged.

## Query Planning and Index Selection

Query planning and index selection are mostly not directly affected by key
encoding.

They will change in any partitioning scheme in some ways, notably, because the
partitions are bounded and known, some full table scans can be turned into a
series of much smaller scans, one per partition.

Note that many partitioned tables will have secondary indexes to enforce
uniqueness of the columns in the logical primary key. This means that queries
with all the logical primary key columns will use a secondary index, whereas if
the table were unpartitioned they would use the primary.

### Point Lookups vs Full Partition Scans

Searches of an index that do not specify the partition column (e.g. "all-shards"
queries) potentially have very different bounds and thus performance between the
two key encodings, due to the difference in column orderings in their indexes.

Consider a search for `user_id=73` in a users table partitioned by `region`.

In the prefix encoding, since the table primary key is forced to be `[region,userid]`
to maintain the prefix property, the narrowest bound possible is a full table
scan.

In the Partition IDs proposal there is more flexibility in the construction of
the table primary key. If it’s reordered to `[user_id, region]`, then this
SELECT will be one tight-bounded lookup per partition e.g. `/51/1/71/*,/52/1/73/*, ...`.

This might appear to be mitigated by the ability to enumerate possible values
for `region` from the partitioning definition, e.g. to expand it out to
`/51/1/us/71, /51/1/de/73, ...`, however the ability to add a default bucket in
list-based, or to use range-based partitioning, means that it is impossible to
enumerate those possible values to some finite set.

It also might appear to be mitigated by the fact that in the above example, it
is likely that an additional unpartitioned secondary index would exist to ensure
the uniqueness of `user_id`.

However, in other cases, such as partitioned secondary indexes, "add another
index" may not be a viable solution. For example, consider an articles table
partitioned by ranges on `publication_date`, with a secondary index on
`author_id` that also partitioned on date. A query to retrieve all articles by a
given author would again be a full table scan if the index were forced to be
`[publication_date,author]`. Adding a non-partitioned index on `author` likely
isn’t a reasonable solution, because if it was, that’s what the user would have
done in the first place.

## Descriptor Structure and Bookkeeping

Details to be decided after key encoding -- ignore details for now (i.e. not
bikeshedding these yet).

Partitions are stored on TableDescriptors.
```
message PartitionDescriptor {
  message List {
    repeated bytes prefix = 1;
  }
  message Range {
    repeated bytes split = 1;
  }
  message Unique {
  }
  oneof {
    repeated List list = 1;
    optional Range range = 2;
    optional Unique unique = 3;
  }
  optional uint32 prefix_len = 4;
}
```

## Additional Out-of-Scope Items

Again, the purpose of the above is to determine key encoding. Additional
questions that will be addressed in a followup, but are not needed to pick an
encoding, include:
- syntax for declaring partitions / referring to and configuring configs for them.
- how we represent partitions in the UI/CLI.
- how commands for adjusting zone configs or ALTER TABLE change.
- (add more topics here).
