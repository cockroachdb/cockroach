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

A partition is a subset of table data (i.e. rows) determined by the values in
one or more columns of the table (the "partition columns"), the physical storage
of which can be configured. CockroachDB is a monolithic key-value map and uses
ranges of the keyspace as units of replication. Zone configs control where
ranges of keyspace are physically stored. Thus consists of organizing rows into
ranges of keys against which zone configs can be applied.

The assignment of values to a specific partition is done in one of three ways:
List, Range, or Unique
- Range partitions are manually specified by split points:
  - e.g. `PARTITION (col) BY RANGE (4, 6)`
- List partitions are manually specified by listing the values in the domain of
  the partition columns, optionally with a default if none match:
  - e.g. `PARTITION (col) BY LIST (“US”,“CA”), (“DE”), (DEFAULT)`
- Unique partitions are automatically created, one for each unique tuple of
  partition columns values:
  - e.g. `PARTITION (col) BY UNIQUE`

Both primary and secondary index data can be partitioned. Each index can only be
partitioned by one set of columns.

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
`PARTITION BY UNIQUE (extract(‘year’, publish_date))`

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

We could silently add the necessary columns for partitioning when the index is
created, but this would violate user expectations in several ways. Most notably,
queries that specify all columns in an index (as returned by table reflection
like information_schema) are expected to be point lookups. To aid the user, we
can provide guidance in the error returned.

In the above `users` table that is partitioned by `region`, the table primary
key must be changed to include `region`. To continue to ensure uniqueness of
`user.id`, an operator would need to explicitly specify a unique secondary index
on `user_id`.

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
key, so any partitioning changes require a individually rewriting all keys in
the index.

In the Index Key Prefix proposal, any repartition of an index is a metadata
change with no key rewriting (unless, of course, the columns in the index need
to change). After repartitioning, our existing replica rebalancing moves the
ranges.

## Denormalized vs. Evaluated Functions of Columns

When partitioning on a function of a column, Index Key Prefix requires that the
evaluation of that function be in a column that can then be used as prefix of
the key. Maintaining this for the user would amount to implementing a
“materialized virtual column” or denormalized evaluation of the function.

Partition IDs denormalize the whole computation of row’s assigned partition, and
do not need to persist the evaluation of any intermediate functions.

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
falls back to the table, database, and system default as before.

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

Again, the purpose of the above is to determine key encoding, so questions like
how we represent partitions in the UI, how users reconfigure any per-partition
policies, etc are all out-of-scope for now but will be included in a follow-up
full RFC.
