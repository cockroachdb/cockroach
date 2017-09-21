- Feature Name: SQL Table Partitioning
- Status: draft
- Start Date: 2017-01-25
- Authors: Nikhil Benesch, Daniel Harrison, David Taylor
- RFC PR: [#18683]


# Summary

Table partitioning provides row-level control over how and where data is stored.

Specifically, `CREATE TABLE`,  `ALTER TABLE`, and their `INDEX` counterparts
learn to accept a partition specification, which maps ranges of the index to
named partitions. Zone configs learn to target these partitions so that locality
constraints and replication settings can vary per partition instead of per table
or database.


# Motivation

The magic of CockroachDB is its ability to stitch together physically-disjoint
ranges of data into one logical table, no matter whether the servers are
separated by several racks or several continents.

If the cluster operator has any of the following requirements, however,
CockroachDB’s default range allocation policy might be a poor fit:

- **Latency guarantees.** The operator must ensure that accessing particular
  data from a particular part of the world will be fast. Since the default
  allocation policy optimizes average latency across all ranges, it might
  violate the latency requirements on a particular range in the service of
  balancing the cluster.

- **Data sovereignty.*** The operator is subject to regulations that require
  certain data to live on servers in certain geographical regions. The default
  allocation policy will blindly move data from overfull regions to underfull
  regions, potentially violating these regulations.

- **Cost reduction.** The operator needs to store infrequently-accessed data on
  slower hardware (e.g. spinning disks) to save money. The default allocation
  policy will happily shuffle data between nodes with SSDs and nodes with HDDs.

Zone configs can help with the above, provided the constraints applies to an
entire table. To meet latency guarantees, a zone config can constrain an entire
table to a particular data center. To reduce costs, a zone config can constrain
an entire infrequently-accessed table to nodes with slower hardware. Data
sovereignty, however, requires further work.

Often, however, the requirement applies to only a subset of the table. A `users`
table, for example, should store its American users in North America, its
European users in Europe, etc. Maybe an `articles` table should store the most
recent few months of posts on SSDs, but everything else on HDDs. Creating
individual tables (`users_america`, `users_europe`, `articles_recent` ,
`articles_archive` etc.) would allow today’s zone configs to solve the problem,
but would bring back some of the pain of manual sharding that CockroachDB was
designed to avoid. Clients would be responsible for reading data from and
writing data to the right shard and stitching data from multiple shards together
if necessary.

By allowing zone configs to target subsets (i.e., “partitions”) of a table, we
allow operators to meet their latency, regulatory, and cost requirements while
preserving the magic of CockroachDB: that is, a logical table that hides the
complex physical sharding underneath.


# A note on scope

To ensure the design proposed within can be implemented in a reasonable
timeframe, the following items are considered out-of-scope:

- **Improving the zone config interface.** The YAML/CLI interface for specifying
  zone configs is clunky and will get more confusing when partitions are added,
  but improvements are better left to a comprehensive redesign (which is already
  planned for row-level ttls, auditing, etc). Instead, this RFC proposes the
  minimum changes necessary to the existing interface to support partitioning.

- **Data sovereignty.** The design within is necessary but not
  sufficient to meet data sovereignty regulations. In particular, the proposed
  scheme provides little protection against misconfiguration and little
  visibility into, at a given moment, what data is actually stored where.
  Additionally, we'll need a resolution to protected information leaking via
  keys, which make their way into various system ranges (e.g. meta2). The rest
  of the data sovereignty work will be developed in a followup RFC.

- **Engineering work schedule.** Some of this work will make 1.2 and some won’t,
  but determining milestones is intentionally omitted from this RFC.

- **Altering the primary key columns of a table.** The design below relies on
  careful primary key selection, which means tables created without partitioning
  in mind may not have a suitable schema. For now, a workaround is to export and
  reimport the data with a new schema. Eventually, we need to support primary
  key schema changes (see #19141).


# Guide-level explanation

## Example: geographic partitioning

Consider a globally-distributed online storefront, RoachMart, that sells and
ships live cockroaches. RoachMart processes several hundred requests per second,
each of which might generate several queries to RoachMart’s nine-node
CockroachDB cluster. (Live cockroaches are very popular these days.) To keep
these requests speedy, RoachMart’s operators want to automatically locate each
user’s data in datacenters near that user.

Today, RoachMart has two data centers: one in North America and one in
Australia. The company expects to open data centers in Europe, Africa, and Asia
soon, so they want to ensure they can migrate users to the new data centers once
they open.

In CockroachDB, partitions are cheap and easy to redefine at will, with one
caveat: partitions must be defined over columns that are a prefix of the primary
key. A `users` table with just an integer primary key, like

```sql
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  continent STRING,
  ...
);
```

cannot be partitioned by `continent` because `continent` is not part of the
primary key. Instead, the table would need to be specified with a compound
primary key

```sql
CREATE TABLE users (
  id SERIAL,
  continent STRING,
  ...
  PRIMARY KEY (continent, id)
);
```

where the partition column `continent` appears first in the key.

Since it’s not currently possible to change a table’s primary key, the partition
columns included in the primary key when the table is created must be granular
enough to support any partitioning scheme that might be desired in the future.
Even if it were possible, changing the primary key would require rewriting *all*
the data in the table and any tables interleaved beneath, a potentially
intractable operation. See [Partitioning and index columns] for a more detailed
discussion of this drawback.

RoachMart is worried that, someday, they’ll have more than one data center per
continent, so they decide to use `country` instead of `continent` as their
partition column. Using the `PARTITION BY LIST` syntax, they can group
individual countries into larger partitions, one for each of their data centers:

```sql
CREATE TABLE roachmart.users (
  id SERIAL,
  country STRING,
  email STRING,
  ...,
  PRIMARY KEY (country, id)
) PARTITION BY LIST (country) (
  PARTITION australia VALUES ('AU'), ('NZ'),
  PARTITION north_america VALUES ('CA'), ('MX'), ('US'),
  PARTITION default VALUES (DEFAULT),
);
```

As requirements shift (e.g., a new data center opens, an existing data center is
running low on capacity, etc.), a country can be seamlessly migrated to a new
partition with an `ALTER TABLE` command that repartitions the table:

```sql
ALTER TABLE roachmart.users PARTITION BY LIST (country) (
  PARTITION australia VALUES ('AU'), ('NZ'),
  PARTITION north_america VALUES ('CA'), ('MX'), ('US'),
  PARTITION scandinavia VALUES ('DK'), ('NO'), ('SE'),
  PARTITION default VALUES (DEFAULT)
);
```

Each partition is required to have a unique name that logically identifies it
across repartitions. Above, the `default` partition is considered equivalent to
the original `default` partition, though it is missing three countries that are
now in the new `scandinavia` partition.

On their own, partitions are inert. The SQL schema for `roachmart.users` does
nothing to actually locate the `australia` partition in Australia. Applying
functionality to a partition requires zone configs. Just as each database and
table can be targeted by a [zone config] that overrides the number and location
of its replicas, each partition can be targeted by a zone config.

So, RoachMart restarts each of its CockroachDB nodes with a `--locality` flag
that indicates its data center:

```
$ ./cockroach start --locality=datacenter=au1
$ ./cockroach start --locality=datacenter=us1
```

Then they can apply a zone config to each partition that restricts it to the
appropriate locality:

```
$ cat australia.zone.yml
constraints: [+datacenter=au1]
$ cat north_america.zone.yml
constraints: [+datacenter=us1]
$ ./cockroach zone set roachmart.users --partition=australia -f australia.zone.yml
$ ./cockroach zone set roachmart.users --partition=north_america -f north_america.zone.yml
```

The replicate queue on each node will notice the updated zone config and begin
rebalancing the cluster. Users with `country = 'NZ'` will live on a range that
is only replicated within the au1 data center and users with `country = 'US'`
will live on a range that is only replicated within the us1 data center.


### Additional colocated tables

Data in the `users` table is now stored in the correct place, but the typical
RoachMart request accesses more than just the `users` table. In particular,
RoachMart would like to colocate users’ orders. Interleaved tables make this
simple.

```sql
CREATE TABLE roachmart.orders (
  user_country STRING,
  user_id INT,
  id INT,
  part_id INT,
  ...
  PRIMARY KEY (user_country, user_id, id),
  FOREIGN KEY (user_country, user_id) REFERENCES users
) INTERLEAVE IN PARENT users (user_country, user_id)
```

The key encoding of interleaved tables ensures that the zone config of any
top-level partitions applies to the data interleaved within.


### Partitioned secondary indices

RoachMart’s warehouse team wants to know how many orders are processed for each
part they sell. They want a secondary index to make the query efficient. Since
the common operation is to look at one country’s orders at a time, they
partition the index on the same columns as users.

```sql
CREATE INDEX part_idx ON roachmart.orders (user_country, part_id)
PARTITION BY LIST (user_country) (
  ...
);
```

Partitioning an index identically to another is likely to be a common-ish case,
so it has dedicated syntax:

```sql
CREATE INDEX part_idx ON roachmart.orders (user_country, part_id)
PARTITION LIKE roachmart.users@primary (user_country);
```

The `PARTITION LIKE` syntax additionally indicates that any changes to the
`roachmart.users` partitioning should automatically propagate to `part_idx`.

## Example: date partitioning

RoachBlog, a free weblog provider, is worried about the amount of data stored in
their `articles` table. The table is growing at the rate of approximately 100GB
per day. (Cockroach enthusiasts are prolific.)

An investigation of their query traffic has revealed that, save for a few
outliers, articles published more than 30 days ago receive virtually no traffic.
They’d like to move those articles to nodes with HDDs to save money on hardware.
Smartly—as if they had anticipated this eventuality—RoachBlog’s engineers
designed the table with a `TIMESTAMP` primary key:

```sql
CREATE TABLE roachblog.articles (
  id SERIAL,
  published TIMESTAMP,
  author_id INT,
  ...,
  PRIMARY KEY (published, id)
);
```

Listing out every `published` timestamp that should be considered “archived”
would be infeasible, so they use the range partitioning syntax instead:

```sql
ALTER TABLE roachblog.articles PARTITION BY RANGE (published) (
  PARTITION archived VALUES LESS THAN (now() - '30d'),
  PARTITION recent VALUES LESS THAN (MAXVALUE)
);
```

The split point between `archived` and `recent` is computed once when the `ALTER
TABLE` executes. RoachBlog plans to run the query automatically every week to
update the partition split point. Repartitioning was designed to be quite cheap
to support this very use case, and indeed no actual data needs to be rewritten.
NB: Each time this query is run it will produce a new split point in the table,
which could leave a lot of small ranges (until we implement merge). We should
probably caution against running this too often.

As with RoachMart, RoachBlog now need only launch their nodes with appropriate
store attributes

```
$ ./cockroach start --store=path=/mnt/crdb,attrs=ssd
$ ./cockroach start --store=path=/mnt/crdb,attrs=hdd
```

and install the corresponding zone config:

```
$ cat recent.zone.yml
constraints: [+ssd]
$ cat archived.zone.yml
constraints: [+hdd]
$ ./cockroach zone set roachblog.articles --partition=recent -f recent.zone.yml
$ ./cockroach zone set roachblog.articles --partition=archived -f archived.zone.yml
```


### Global secondary indices

RoachBlog has a page which lists all articles written by a given author. There
are no sovereignty issues with this, so they create a global index to keep this
page fast:

```sql
CREATE INDEX author_idx ON roachblog.articles (author_id)
```

## Summary: initial partitioning and repartitioning

- Partitions can be added to a table when it is created, or at any time
  afterward. (However, since the primary key of a table cannot be changed,
  forethought is still required.)

- Partitions are defined over one or more columns. These columns must be a
  prefix of the primary key (or secondary index).

- Indexes can also be partitioned, but are not required to be.

- Each partition is required to have a name that is unique among all partitions
  on that table or index.

- Repartitioning is relatively cheap; data is not rewritten. The table metadata
  is updated and the allocator is left to move the data as necessary. A
  partition with the same name before and after a repartitioning is considered
  the same partition. During a repartitioning, any partition zone config entries
  with no corresponding name in the new partitions are removed.

- Future work to expose zone configs through SQL will likely allow for updating
  partitions and the corresponding zone configs together. In the meantime, users
  can create an empty partition, apply a zone config, then repartition.

- List partitioning without a  `DEFAULT` clause will error on any queries
  producing rows that don’t have a matching partition. This means that
  repartitioning a table may require validating the existing data. There are a
  number of fast paths that don’t require the validation: empty tables, a list
  repartitioning with values that are a superset of the previous partitioning,
  any range partitioning, a list partitioning with a `DEFAULT` clause.

- After a table is partitioned, its partitions can be targeted by zone
  constraints via the existing CLI, e.g.  `./cockroach zone set
  database.table --partition=partition -f zone.yml`.

- As before, CockroachDB uses the most granular zone config available. Zone
  configs that target a partition are considered more granular than those that
  target a table, which in turn are considered more granular than those that
  target a database. Configs do not inherit, but unspecified fields when a zone
  override is first create are copied from the parent, as is currently the case.


## Usage: locality–resilience tradeoff

There exists a tradeoff between making reads/writes fast and surviving failures.
Consider a partition with three replicas of `roachmart.users` for Australian
users. If only one replica is pinned to an Australian datacenter, then reads may
be fast (via [leases follow the sun]) but writes will be slow. If two replicas
are pinned to an Australian datacenter, than reads and writes will be fast (as
long as the cross-ocean link has enough bandwidth that the third replica doesn’t
fall behind and hit the quota pool). If those two replicas are in the same
datacenter, then loss of one datacenter can lead to data unavailability, so some
deployments may want two separate Austrialian datacenters. If all three replicas
are in Australian datacenters, then three Australian datacenters are needed to
be resilient to a datacenter loss.


# Reference-level explanation

## Key encoding

Different key encodings were evaluated at length in a prior version of this RFC.
The approach recommended, entitled [Index Key Prefix], is described here without
discussion of the tradeoffs involved in the decision; refer the original RFC for
that discussion.

The Index Key Prefix approach simply allows part of the existing row key to be
used as the partition key. For a partitioned table, the partition key can be any
prefix of the table primary key. Similarly, for a partitioned index, the
partition key can be any prefix of the indexed columns. Since a row’s key is
encoded as the table ID, followed by the index ID, followed by each of the
indexed columns, in order, the partition key appears immediately after the table
and index IDs, and thus rows with the same partition key will be adjacent.

To further illustrate this, consider the key encodings for the earlier
geographic partitioning example.

Primary index for `roachmart.users`:

```
/TableID/IndexID/<region>/<user_id> -> [email]
```

Global non-unique secondary index for `roachblog.articles.author_id`:

```
/TableID/IndexID/<author_id><article_id> -> null
```

Partitioned non-unique secondary index for `roachmart.orders.part_id`:

```
/TableID/IndexID/<region>/<part_id>/<order_id> -> null
```

Note that values need not be physically adjacent to belong to the same logical
partition. For example, `'US'`, `'CA'` and `'MX'` are interspersed with other
countries, but all map to the same  `north_america` partition in
`roachmart.users`. This comes at a cost, as the `north_america` partition now
requires a minimum of three separate ranges instead of just one. See [Range
splits] below for further discussion.


## SQL syntax

The `PARTITION BY` clause is usable in `CREATE TABLE`, `ALTER TABLE`, `CREATE
INDEX`, `ALTER INDEX` and variants. See [Example: geographic partitioning] for
the `PARTITION BY LIST` syntax and [Example: date partitioning]  for `PARTITION
BY RANGE`.

A somewhat formal version of the syntax is presented below:

```
CREATE TABLE <table-name> ( <elements...> ) [<interleave>] [<partition-scheme>]

Partition scheme:
  PARTITION BY LIST ( <col-names>... ) ( <list-partition> [ , ... ] ) <partition-scheme>
  PARTITION BY RANGE ( <col-names>... ) ( <range-partition> [ , ... ] ) <partition-scheme>

List partition:
  PARTITION <partition-name> VALUES ( <expr>... ) [, ...]

Range partition:
  PARTITION <partition-name> VALUES LESS THAN (  <expr>... ) [, ...]
```

Note that a `NULL` value in a range-partitioned column sorts into the first
range, which is consistent with our key encoding ordering and `ORDER BY`
behavior.

To allow reads to target only selected partitions, we propose to extend table
names (not arbitrary table expressions) with a `PARTITION` clause. For example:

```sql
SELECT * FROM roachmart.users PARTITION (australia, north_america)

SELECT *
  FROM
    roachblog.articles PARTITION (recent) AS a
  JOIN
    roachblog.user_views PARTITION (north_america) AS b
  ON a.id = b.article_id
```

The implementation can simply transform each `PARTITION` clause into an
additional constraint. The join query above, for example, would be rewritten to
include `WHERE user_views.country IN ('CA', 'MX', 'US') AND articles.published >=
'recent cutoff'`;


### Subpartitioning

Subpartitioning allows partitioning along several axes simultaneously. The
`PARTITION BY` syntax presented above is recursive so that partitions can be
themselves partitioned any number of times, using either list or range
partitioning. Note that the subpartition columns must be a prefix of the columns
in the primary key that have not been consumed by parent partitions.

Suppose RoachMart wanted to age out users who haven’t logged in in several
months to slower hardware, while continuing to partition by country for improved
latency. Subpartitioning would neatly solve their use case:

```sql
CREATE TABLE roachmart.users (
  id SERIAL,
  country STRING,
  last_seen DATETIME,
  ...,
  PRIMARY KEY (country, last_seen, id)
) PARTITION BY LIST (country) (
  PARTITION australia VALUES ('AU'), ('NZ') PARTITION BY RANGE (last_seen) (
    PARTITION australia_archived VALUES LESS THAN (now() - '30d'),
    PARTITION australia_recent VALUES LESS THAN (MAXVALUE),
  ),
  PARTITION north_america VALUES ('CA'), ('MX'), ('US') PARTITION BY RANGE (last_seen) (
    PARTITION north_america_archived VALUES LESS THAN (now() - '30d'),
    PARTITION north_america_recent VALUES LESS THAN (MAXVALUE)
  ),
  ...
);
```

Subpartition names must be unique within a table, as each table's partitions and
subpartitions share a namespace.

Other databases also provide less flexible but more convenient syntaxes, like a
`SUBPARTITION TEMPLATE` that prevents repetition of identical subpartition
schemes. Above, a `SUBPARTITION TEMPLATE` could be used to describe the
`last_seen` partitioning scheme exactly once, instead of once for each `country`
partition. We propose to implement only the more general syntax and defer design
of a more convenient syntax until demand exists.

## IndexDescriptor changes

Partitioning information is added to `IndexDescriptor` as below. All tuples are
encoded as `EncDatums` using the value encoding.

```protobuf
message IndexDescriptor {
  ...
  optional PartitioningDescriptor partitioning = 15;
}

message PartitioningDescriptor {
  message List {
    optional string name = 1;
    repeated bytes values = 2;
    optional PartitioningDescriptor subpartition = 3;
  }

  message Range {
    optional string name = 1;
    optional bytes values_less_than = 2;
    optional PartitioningDescriptor subpartition = 3;
  }

  optional uint32 num_columns = 1;
  repeated List list = 2;
  repeated Range range = 3;
}
```


## Zone config changes

Zone configs are currently stored in the `system.zones` table, which maps
database and table IDs to `ZoneConfig` protobufs. We propose to adjust this
`ZoneConfig` protobuf to additionally store a zone config per partition, per
index. This avoids a cluster migration to change the schema of `system.zones`.
Older nodes in a cluster will simply ignore the new `ZoneConfig` fields.

An alternate proposal to instead [replace the system.zones table] was presented
and rejected in a previous version of this RFC.

Specifically, our design adds three fields to the `ZoneConfig` proto:

```protobuf
message ZoneConfig {
  ...
  message PartitionSpans {
    roachpb.Span span = 1;
    uint32 index = 2;
  }
  repeated PartitionSpans partition_spans = 8;
  repeated ZoneConfig partition_configs = 9;
}
```

Any partition with a custom zone config has one or more entries in
`partition_spans` that map the key span of the partition to the index of a
custom zone config in `partition_configs`. This layer of indirection avoids
repeating the custom zone config for each non-adjacent keyspan in a partition,
as caused by a list partition with non-adjacent elements, for example.

Though `partition_spans` could be easily derived from the corresponding
`TableDescriptor`, looking up the zone config for a given key is a frequent
operation and we want to avoid the extra KV lookup for the `TableDescriptor`. As
a result of the denormalization, updating a table's partitioning will require
rewriting the splits in the zone config entry, if it exists, in the same
transaction.

One case requires special care to handle. Suppose the `archived` partition of
`roachblog.articles` has a custom zone config, but the `roachblog.articles`
table itself does not have a custom zone config. The `system.zones` table will
necessarily have an entry for `roachblog.articles` to store the custom zone
config for the `archived` partition, but that entry will have an otherwise empty
`ZoneConfig`. This must not be taken to mean that the other partitions of the
table should use this empty zone config, but that the default zone config
applies. We propose to use the `num_replicas` field for this purpose:
`num_replicas = 0` is invalid and therefore indicates that the table in
question does not have an active `ZoneConfig`.

One downside of this scheme is that the zone configs for all partitions of a
table's indices are stored in one row, which puts an effective limit on the
number of partitions allowed on a table. Additionally, `system.zones` is part of
the unsplittable, gossiped system config span, so all zone configs across all
tables must add up to less than 64MB. Some back of the envelope math suggests 60
bytes per range partition, and 60 bytes + the size of the values in a list
partition.

For `roachmart.users` with every country allocated between 7 partitions, this
results in 1167B. This results in an absolute max of 64MB / 60B = ~1,000,000
partitioned tables or 64MB / 1167B = ~54,000 tables if they were all partitioned
by country. The recommended max number of partitions in a table seems to range
from 100 to 1024 in other partitioning implementations, so this seems
reasonable.

Similar to the way that the zone config for a table, if present, completely
overrides (with no inheritance) the zone config for a database, a zone config
for a partition overrides the zone config for the table, database, or cluster.
The ergonomics of this will likely be sub-optimal; the user will need to
maintain the denormalization of what is naturally an inheritance hierarchy of
configuration. There is a larger upcoming effort to refactor zone configs which
will address these issues, so the following are out of scope of this RFC:

- A SQL interface for reading and writing zone configs

- Auditing of zone config changes

- Inheritance

- Moving zone configs out of gossip

- Raising global and per-table limitations on the number of partitions

- Requesting that a majority (but not all) of a ranges replica's be served out of
  a particular region

- Partition configs outside of the database+table scoping. This could be useful
  for allowing partitioning in a shared CockroachDB offering with static, preset
  zone configs.


## Range splits and schema changes

The CockroachDB unit of replication and rebalancing is the range. So for zone
configs to target a partition, it needs to be on its own range (and potentially
more than that for list partitioning see [Range splits]).

Currently, CockroachDB asynchronously splits each newly created table into its
own range. This happens regardless of whether any zone configs are added to
target that table specifically. Each partition could similarly be asynchronously
split after being defined.

Unfortunately, this may create extra ranges if a table is partitioned,
repartitioned, and only then given zone configs (though this problem goes away
when we support range merges). We could instead lazily split a partition only
when a zone config is added for it. However this both deviates from the table
behavior and would violate a user’s likely expectation of partitions living on
separate ranges, so we'll use `PARTITION BY` as the trigger.

A table’s partitions can have a large influence on sql planning, so any changes
should be broadcast immediately. Further, our schema changes and table leasing
require that at most 2 versions are in use at a time. So it’s a natural fit for
partitionings to run as schema changes. This schema change will be responsible
for creating the splits and can be hooked into `system.jobs` to provide
introspection of progress as well as cancellation.


## Query planning changes

Because of the decision to require partitions be defined over normal,
materialized columns, there are no correctness changes needed to sql query
planning. Performance, however, needs some work.


### Case study

Consider the following queries issued by RoachMart to the `roachmart.users`
table. (Ignore the poor security practices, this is meant to be illustrative.)

When a user visits RoachMart and needs to log in:

```sql
SELECT id FROM roachmart.users WHERE email = $1`
```

Whenever a new page is loaded on the web or a new screen is loaded on the native
app, the `id` is extracted from a cookie or passed via the RoachMart api and
used in the stateless servers to rehydrate the user information:

```sql
SELECT * FROM roachmart.users WHERE id = 12345`
```

Regardless of whether the `roachmart.users` table is partitioned, the first
query is kept speedy by an unpartitioned secondary index on `email`. This may
require a cross-ocean hop to another datacenter but login is an infrequent
operation and so this is okay. (If data sovereignty of emails is a concern, then
a global index is not appropriate and either a global index on `hash(email)` or
a partitioned index on must be used. The details of this is left for a later
sovereignty RFC.)

The second query is much harder. In an unpartitioned table, the primary key
would be only on `(id)` and so this is a point lookup. But RoachMart is a global
company and wants to keep a user’s data in the datacenter nearest them, so
they’ve partitioned `roachmart.users` on `(country, id)`. This means the query
as written above will require a full table scan, which is obviously
unacceptable.

The best solution to this, and the one we will always recommend first, is for
RoachMart to also specify the user’s `country` in the second query. The login
query will be changed to also return the `country`, this will be saved alongside
`id` in the web cookie or native app’s local data, and passed back whenever
retrieving the user. It’s even not as onerous as it first seems since the
RoachMart API returns the user’s `id` as a string containing both pieces:
`US|123`.

In some cases, this will not be possible or will not be desirable, so
CockroachDB has a number of pieces that can be combined to deal with this. NB:
None of these really solve the problem in a satisfactory way, so as mentioned
above we will very strongly urge the user to specify the entire primary key.


1. The developer could create a global index on just `id`, but in the common
   case this requires a cross-datacenter request for the index lookup.

2. If  `LIMIT 1` is added to the query and no sort is requested, the planner is
   free to return the first result it finds and cancel the rest of the work. If
   a uniqueness constraint exists on `id`, the `LIMIT 1` can be assumed. This
   latter is an optimization that may be helpful in general.

3. A list partitioning without a `DEFAULT` is an enum. In this case, before the
   query gets to the planner, a synthetic `AND country IN ('CA', 'MX','US', …)`
   clause could be added. This would turn the query into a point lookup per
   unique value in `country`. If `DEFAULT` is present, the non-default cases
   could optimistically be checked first.

4. Other partitioning implementations allow for the user to explicitly limit a
   query to only fetch from a set of partitions via `SELECT * FROM
   roachmart.users PARTITION (north_america, australia)`.

5. This is not a full table scan in other partitioning implementations because
   they don’t require the `(country, id)` primary key, instead indexing `(id)`
   as normal inside each partition. A query on `id` without the partition
   information then becomes a point lookup per partition. This can be simulated
   in CockroachDB by introducing and partitioning on a derived `partition_id`
   column that is 1:1 with partitions. This may be sufficient justification to
   prioritize building computed columns.

   Concretely, the `roachmart.users` table above could have PRIMARY KEY
   (continent, country, id) and PARTITION BY LIST (continent) to start, so there
   is only one key value per partition. Later, when/if it is needed, it could
   change to PARTITION BY LIST (continent, country)

6. 4+5 could allow a user to issue one query to optimistically try a point
   lookup in the local partition before trying a point lookup in all partitions.


## Other SQL changes

Other implementations surface partitioning information via
`information_schema.partitions`, so we should as well. `SHOW CREATE TABLE` will
also need to learn to display `PARTITION BY` clauses.


## Interleaved tables and partitioning

[Interleaved tables] and partitioning are designed to work together. Geographic
partitioning is used to locate `roachmart.users` records in the nearest
datacenter and interleaved tables are used to locate the data associated with
that user (orders, etc) near it. A geographically partitioned user can be moved
with one `UPDATE` , but this doesn’t move that user’s orders until we support
`ON UPDATE CASCADE`. This is not a new problem, but it will be more obvious when
combined with partitioned tables.


# Drawbacks

Like index selection, column families, interleaved tables, and other
optimization tools, partitioning will require some knowledge of the internals of
the system to use effectively. See, for example, the [locality–resilience
tradeoff] described above.


## Partitioning and index columns

Since a table can only be partitioned by a prefix of its primary key, a table
destined for partitioning often has an “unnatural” primary key. For example, to
support partitioning the `roachmart.users` table by `country`, the table’s
natural primary key, `id`, must be explicitly prefixed with the partition column
to create a composite primary key of `(country, id)`. The composite primary key
has two notable drawbacks: it does not enforce that `id` is globally unique, and
it does not provide fast lookups on `id`. If ensuring uniqueness or fast lookups
are required, the user must explicitly create a unique, unpartitioned secondary
index on `id`.

We could automatically add this secondary index to preserve uniqueness and fast
lookups, but this would violate user expectations. First, a secondary index
might itself store sensitive information. We want to ensure that operators are
aware of the secondary index so they can specify an appropriate zone config or
even create the index on `hash(id)` (though hashes may not provide sufficient
masking of sensitive information, depending on the entropy of the input and the
type of hash). Second, every secondary index increases the cost of writes
(specifically for inserts and for updates that change an indexed or stored
column). For example, an unpartitioned, unique index on `roachmart.users` would
require cross-ocean hops for writes that would otherwise hit just one
continent.*

Similarly, we could silently prefix the specified primary key (i.e., the natural
primary key) with the partition columns, but this too would violate user
expectations. Most notably, queries that specify all columns in an index, as
specified at creation or returned by reflection like `information_schema`, are
expected to be point lookups.

Instead, we can aid the user with detailed guidance in the error message
generated by invalid partitioning schemes.

*In general, ensuring global uniqueness requires cross-datacenter hops on
writes. In limited cases, like `SERIAL` columns, users can achieve both
uniqueness and fast writes without a secondary unique index by assuming
`unique_rowid()` collisions are sufficiently improbable. The risk with such a
scheme, of course, is that someone can manually insert a colliding value.


## Range splits

The unit of replication in CockroachDB is the range, so partitioning necessarily
requires splitting at least one range for each partition. In the worst case,
when partitioning by list, partitions containing non-adjacent values will
generate an extra range for *each* non-adjacent value.

For example, consider the original partitioning specification from
`roachmart.users`, in which `'CA'` and `'MX'` belong to the same partition
`north_america`, but are bisected by a value, `'FJ'`, in another partition
`oceania`. This forces each country on to its own range. As more countries are
added, the effect is amplified.

This is unfortunate but should be fine in larger tables. It can be worked around
by introducing and partitioning on a `partition_id` column, which is derived
from country. (Computed columns are a natural choice for this once we implement
them.) The tradeoff here is that repartitioning will necessitate rewriting rows
instead of just updating range metadata.

Note that repartitioning could make this worse, especially until we support
range merges. As discussed in [Range splits and schema changes], list
partitioning may create more ranges than expected when partition values are not
adjacent. Additionally, repartitioning may result in small or empty ranges that
cannot be cleaned up until we support range merges.


# Future work

## Admin UI

Exposing partitioning information and zone configs in the admin UI is out of
scope for this document. See [#14113], which tracks providing broad insight into
zone constraint violations.


## Bulk load

Other PARTITION BY implementations can be used to quickly bulk load data into a
table (or bulk remove it) and this appears to be a popular use. We currently
allow only bulk load of an entire table at once, so this may be useful for us to
consider as well, but it’s out of scope for this document.


# Alternatives

The separation between partition specification, which happens via SQL, and zone
configuration, which happens via the CLI, is unfortunate and largely historical
happenstance. A table’s partitioning is rarely updated independently of its zone
config, so if and when we move zone configuration to SQL, we should consider
tightly coupling its interface to table definitions/partitioning.


# Appendix: Other PARTITIONING syntaxes

There is unfortunately no SQL standard for partitioning. As a result, separate
partitioning syntaxes have emerged:

- MySQL and Oracle specify partitions inline in `CREATE TABLE`, like we do
  above.

- Microsoft SQL Server requires four steps: allocating physical storage called
  “filegroups” for each partition with `ALTER DATABASE… ADD FILE`, followed by
  `CREATE PARTITION FUNCTION` to define the partition split points, followed by
  `CREATE PARTITION SCHEME` to tie the partition function output to the created
  file groups, followed by `CREATE TABLE... ON partition_scheme`  to tie the
  table to the partitioning scheme.

- PostgreSQL 10 takes a hybrid approach: the partition columns and scheme (i.e,
  `RANGE` or `LIST`) are specified in the `CREATE TABLE` statement, but the
  partition split points are specified by running `CREATE TABLE… PARTITION OF…
  FOR VALUES` once for each partition.

We normally reuse PostgreSQL syntax for compatibility reasons, but we’ve
deliberately rejected it here. Partitioning was not a first class feature before
PostgreSQL 10, which was only released on 2017-10-05, so we shouldn’t have
compatibility issues with existing ORMs and applications. The syntax being
introduced in PostgreSQL 10, shown below, treats a partitioned table roughly as
a collection of tables, which each have their own indexes, constraints, etc. It
does not allow for a global index across all partitions of a table and the
global table namespace is polluted with each partition.

We’ve instead chosen to closely follow the MySQL and Oracle syntax, since it
fits with our model of partitions as mostly-invisible subdivisions of a table.

For reference, we’ve replicated the `roachblog.articles` example in each of the
three syntaxes.

## MySQL and Oracle

```sql
CREATE TABLE articles (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    published DATE,
    ...
) PARTITION BY RANGE(published) (
    PARTITION archived VALUES LESS THAN ('2017-01-25'),
    PARTITION recent VALUES LESS THAN MAXVALUE
);
```

## PostgreSQL

```sql
CREATE TABLE articles (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    published DATE,
    ...
) PARTITION BY RANGE (published);
CREATE TABLE articles_archived PARTITION OF articles
  FOR VALUES FROM (MINVALUE) TO ('2017-01-25');
CREATE TABLE articles_recent PARTITION OF articles
  FOR VALUES FROM ('2017-01-25') TO (MAXVALUE);
```

## Microsoft SQL Server

```sql
CREATE PARTITION FUNCTION fun (int) AS RANGE LEFT FOR VALUES ('2017-01-25');
CREATE PARTITION SCHEME sch AS PARTITION fun TO (filegroups...);
CREATE TABLE articles (id int PRIMARY KEY, published DATE) ON sch (published);
```

[#14113]: https://github.com/cockroachdb/cockroach/issues/14113
[#18683]: https://github.com/cockroachdb/cockroach/pull/18683
[#19141]: https://github.com/cockroachdb/cockroach/issues/19141
[example: date partitioning]: #example-date-partitioning
[example: geographic partitioning]: #example-geographic-partitioning
[index key prefix]: https://github.com/cockroachdb/cockroach/blob/1f3c72f17546f944490e0a4dcd928fd96a375987/docs/RFCS/sql_partitioning.md#key-encoding
[interleaved tables]: https://www.cockroachlabs.com/docs/stable/interleave-in-parent.html
[leases follow the sun]: https://github.com/cockroachdb/cockroach/blob/763d21e6fad69728a523d3cdd8b449c8513094b7/docs/RFCS/20170125_leaseholder_locality.md
[locality–resilience tradeoff]: #usage-localityresilience-tradeoff
[partitioning and index columns]: #partitioning-and-index-columns
[range splits and schema changes]: #range-splits-and-schema-changes
[range splits]: #range-splits
[replace the system.zones table]: https://github.com/cockroachdb/cockroach/blob/1f3c72f17546f944490e0a4dcd928fd96a375987/docs/RFCS/sql_partitioning.md#table-subzones
[zone config]: https://www.cockroachlabs.com/docs/stable/configure-replication-zones.html
