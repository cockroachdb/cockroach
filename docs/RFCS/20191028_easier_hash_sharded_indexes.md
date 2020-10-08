- Feature Name: Easier Hash Sharded Indexes
- Status: draft
- Start Date: 2019-10-28
- Authors: Aayush Shah, Andrew Werner
- RFC PR: PR # after acceptance of initial draft
- Cockroach Issue: [#39340] (https://github.com/cockroachdb/cockroach/issues/39340)

# Summary

This is a proposal to provide better UX for creating hash sharded indexes through easier
syntax. This allows a useful mechanism to alleviate single range hot spots due to
sequential workloads. 

# Motivation

Currently, in CockroachDB, write workloads that are sequential on a particular key will
cause a hotspot on a single range if there's any sort of index with the said key as a
prefix. Note that load-based splitting ([#31413]) doesn't help us here since our reservoir
sampling approach cannot find a valid split point that divides the incoming workload
evenly (since almost all queries are incident on _one_ of the boundaries of the concerned
range).

In 19.2, we added optimizer support to automatically add filters based on check
constraints. This can allow users to alleviate aforementioned single range hotspots by
creating an index on a computed shard column. However, this feature still requires some
relatively unattractive syntax to manually add a computed column which will act as the
shard key. This is illustrated in the following example from issue [#39340]
(https://github.com/cockroachdb/cockroach/issues/39340).

Imagine we have an IOT application where we are tracking a bunch of devices and each
device creates events. Sometimes we want to know which devices published events in some
time period. We might start with the following schema.

```sql
CREATE TABLE events (
    device_id
        UUID,
    event_id
        UUID,
    ts
        TIMESTAMP,
    data
        JSONB,
    PRIMARY KEY (device_id, ts, event_id),
    INDEX (ts)
);
```

This schema would have a hot spot on that `INDEX (ts)` which would be rather unfortunate.
We can alleviate this hot spot by sharding this time ordered index.

```sql
CREATE TABLE events (
    device_id
        UUID,
    shard
        INT8
        AS (fnv32(device_id) % 8) STORED
        CHECK (shard IN (0, 1, 2, 3, 4, 5, 6, 7)),
    event_id
        UUID,
    ts
        TIMESTAMP,
    data
        JSONB,
    PRIMARY KEY (device_id, ts, event_id),
    INDEX (shard, ts)
);
```

This isn't too big of a lift here because the device ID is easy to hash in sql. Imagine
instead we had a primary key based on some other features:

```sql
CREATE TABLE events (
    product_id
        INT8,
    owner
        UUID,
    serial_number
        VARCHAR,
    event_id
        UUID,
    ts
        TIMESTAMP,
    data
        JSONB,
    PRIMARY KEY (product_id, owner, serial_number, ts, event_id),
    INDEX (ts)
);
```

In order to shard this we'll need something like:

```sql
CREATE TABLE events (
    product_id
        INT8,
    owner
        UUID,
    serial_number
        VARCHAR,
    shard
        INT8
        AS (
            fnv32(
                concat(hex(product_id)),
                owner::STRING,
                serial_number
            )
            % 8
        ) STORED
        CHECK (shard IN (0, 1, 2, 3, 4, 5, 6, 7)),
    event_id
        UUID,
    ts
        TIMESTAMP,
    data
        JSONB,
    PRIMARY KEY (
        product_id,
        owner,
        serial_number,
        ts,
        event_id
    ),
    INDEX (shard, ts)
);
```

We can see that this is starting to get heavy. The proposal is that we shoulder the burden
of hashing and installing a check constraint behind a new syntax.

Borrowing from [Postgres](https://www.postgresql.org/docs/9.1/indexes-types.html) and [SQL
Server](https://docs.microsoft.com/en-us/sql/database-engine/determining-the-correct-bucket-count-for-hash-indexes?view=sql-server-2014),
we propose the following syntax:

Primary index:

```sql
CREATE TABLE events (
    product_id
        INT8,
    owner
        UUID,
    serial_number
        VARCHAR,
    event_id
        UUID,
    ts 
        TIMESTAMP,
    data
        JSONB,
    -- Creates a primary index on (shard, product_id, owner, serial_number)
    -- with a check constraint for `shard in (0...7)`
    PRIMARY KEY (product_id, owner, serial_number) USING HASH WITH BUCKET_COUNT=8
)
```

```sql
CREATE TABLE events (
    ts 
        DECIMAL PRIMARY KEY USING HASH WITH BUCKET_COUNT=8,
    product_id
        INT8,
    ...
    ...
)
```

Secondary index:

```sql
CREATE TABLE events (
    product_id
        INT8,
    owner
        UUID,
    serial_number
        VARCHAR,
    event_id
        UUID,
    ts
        TIMESTAMP,
    data
        JSONB,
    PRIMARY KEY (product_id, owner, serial_number, ts, event_id),
    -- Creates a secondary index on (shard, ts)
    -- with a check constraint for `shard in (0...7)`
    INDEX (ts) USING HASH WITH BUCKET_COUNT=8
);
```
```sql
CREATE [UNIQUE] INDEX foo on events (ts) USING HASH WITH BUCKET_COUNT=8
```

Here, the new `USING HASH WITH BUCKET_COUNT...` syntax will create a new computed shard
column based on the set of columns _in the index_.

## Benchmarks 

We consider the following 4 schemas:

 (1) Using hash sharded primary index on `k`.
```sql
CREATE TABLE kv (
    k INT8 NOT NULL,
    v bytes NOT NULL,
    shard 
        INT8 AS (k % 10) STORED
        CHECK (shard IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)), 
    PRIMARY KEY (shard, k)
);
```

(2) Using unsharded primary index on `k`.
```sql
CREATE TABLE kv (
    k INT8 NOT NULL,
    v BYTES NOT NULL,
    PRIMARY KEY (k)
);
```

(3) Hash sharded primary index on `k` and a secondary index on `v`
```sql
CREATE TABLE kv (
    k INT8 NOT NULL,
    v BYTES NOT NULL,
    shard 
        INT4 AS (k % 10) STORED
        CHECK (shard IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)),
    PRIMARY KEY (shard, k),
    INDEX (v)
)
```

(4) Primary (unsharded) index on `k` and a secondary index on `v`.
```sql
CREATE TABLE kv
    k INT8 NOT NULL,
    v BYTES NOT NULL,
    PRIMARY KEY (k),
    INDEX (v)
);
```

### Sequential throughput on 5-node cluster


First, we demonstrate sequential write throughput on
[kv0bench](https://github.com/cockroachdb/cockroach/pull/42203), which is a `tpcc-bench`
style benchmark that searches for the maximum throughput that can be sustained while
maintaining an average latency less than a provided threshold, on a sequential `kv0`
workload. We ran `kv0bench` with the aforementioned threshold value configured to
`10.0ms`. We compare the max sustained throughput under this benchmark on all 4 of the
schemas described above.

The following benchmark was run on a 5 node GCE cluster with `n1-standard-8` machines.

![Write throughput](https://user-images.githubusercontent.com/10788754/70077118-2b2c6f00-15ce-11ea-87c2-7b410cf39a3d.png)

### Throughput scaling
    
Now we demonstrate sequential write throughput comparing schemas (1) and (2) from above on
increasingly larger cluster sizes. Here we see that we essentially unlock linear scaling
on such workloads by preventing a single range hotspot. 

![Scaling](https://user-images.githubusercontent.com/10788754/70077112-2962ab80-15ce-11ea-85f6-464a2f730827.png)

The following metrics from the CockroachDB Admin UI explain the results seen above by
showing an even distribution of queries across nodes in the sharded case, as opposed to
the unsharded case where all queries are being serviced by only one of the nodes.

Sharded:

![Sharded](https://user-images.githubusercontent.com/10788754/70085397-1fe13f80-15de-11ea-9476-16c94580fcbf.png)

Unsharded:

![Unsharded](https://user-images.githubusercontent.com/10788754/70086280-0e009c00-15e0-11ea-8c09-675db555c998.png)

# Guide level explanation

Refer to [motivations](#Motivations) for a quick overview on how to shard your indexes.

# Future work

## Deriving values of "purely" computed columns when all referenced columns are available

The optimizer currently doesn't derive the value of a stored computed column even when all
the columns that it references are available. This means we have to search all the shards.

For example, on schema (1) described in [benchmarks](#Benchmarks):

```
root@localhost:26257/kv> explain select * from kv where k = 10;
  tree |    field    |                                                   description
+------+-------------+-----------------------------------------------------------------------------------------------------------------+
       | distributed | false
       | vectorized  | false
  scan |             |
       | table       | kv@primary
       | spans       | /0/10-/0/10/# /1/10-/1/10/# /2/10-/2/10/# /3/10-/3/10/# /4/10-/4/10/# /5/10-/5/10/# /6/10-/6/10/# /7/10-/7/10/#
       | parallel    |
(6 rows)
```
This means that if the shard value isn't plumbed down from the client side, performance
suffers in sequential workloads that aren't dominated by writes.

We ran `kv --sequential --concurrency=256 --read-percent=90` on both those schemas, with
the following results:

On schema (1)

```
_elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)__total
  300.0s        0        6218614        20728.6     11.9     11.0     19.9     26.2    192.9  read
_elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)__total
  300.0s        0         691363         2304.5      4.0      3.0     10.0     14.7    142.6  write
```

On schema (2)

```
_elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)__total
  300.0s        0       16702732        55675.5      3.7      2.9     10.5     21.0    385.9  read
_elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)__total
  300.0s        0        1855330         6184.4      8.5      6.6     18.9     56.6    121.6  write
```

In our current state, in order to see good performance for reads, the user would have to
plumb the shard value down in their queries. In the context of the prototype ([#42922]),
this is hard since we choose to keep the shard column hidden and the user doesn't really
have a way of knowing _how_ to compute the shard column. However, the idea is that we
won't have this limitation in 20.1.

## Constraining scans to only search "relevant" shards (avoid full table scan when we can)

For example, for the following query:

```sql
SELECT * FROM kv ORDER BY k LIMIT 5;
```

We get the following plan:
```
       tree      | field | description  
+----------------+-------+-------------+
  limit          |       |              
   │             | count | 5            
   └── sort      |       |              
        │        | order | +k           
        └── scan |       |              
                 | table | kv@primary   
                 | spans | ALL      
```

This kind of query could be made faster by pushing the limit down to each partition and
then merging them.

TODO(aayush): add benchmark that demonstrates range scan performance under both of the
plans described above.

# Open Questions

- **Changing the number of shards in primary key**

This is not a problem with respect to secondary indexes since the user could simply create
a new index with the desired number of shards and then remove the old index. However,
there would be no way to change the number of shards in a primary key. How does this tie
in with Solon and Rohan's work with regards to being able to change primary keys?

- **Universal hash function**

The attached prototype ([#42922]) simply casts every data type to be used in the shard
computation to `STRING` in sql and calls `fnv32` on them. We might want to consider a
better approach to hash any arbitrary set of SQL data types that is faster. What are the
considerations when deciding whether to write a new hash function for what we're trying to
do? How would it be better than what we're currently doing (casting to string and then
`fnv32/64`)?

- **SHOW CREATE TABLE**

A `SHOW CREATE...` statement is supposed to produce syntactically valid SQL and it's
output must create the exact same table that it was called on (ie. it must be
_roundtripable_). Given this, how much do we want the user to know about what this new
syntax does? Broadly speaking, we have two options:

1. ***Be very explicit about the existence of a computed shard column*** 

For example:

```sql
CREATE TABLE abc (a INT PRIMARY KEY USING HASH WITH BUCKET_COUNT=4);
```

would simply be an alias for

```sql
CREATE TABLE abc (a INT, a_shard INT AS MOD(hash(a), 4)
    STORED
    CHECK (a_shard IN (0,1,2,3)))
```

This means that the `SHOW CREATE TABLE` syntax doesn't hide any of what's happening from
the user.

2. ***Keep things transparent from the user***

This is the approach that the prototype attached ([#42922]) with this RFC takes. If we
choose to go this route, we keep the computed shard column hidden and `SHOW CREATE TABLE`
output (roughly) returns the syntax that was used to create it. This also means that the
check constraint that is placed on the shard column will also be hidden.

- **Add ability to specify the column set to be used to compute the shard**

The proposed syntax `USING HASH WITH BUCKET_COUNT` simply computes the shard value based
on the set of columns in the index. As proposed, it doesn't allow the user to specify a
different set of columns to compute the shard column with. We could add something like the
following syntax for this:

```sql
CREATE TABLE kv (
    k INT PRIMARY KEY USING HASH (ts, k) WITH BUCKET_COUNT=10,
    v BYTES,
    ts DECIMAL
);
```

This would force the shard column to be computed with `(ts, k)` instead of just `k`. It
is, however, hard to think of cases where this kind of functionality would be useful
enough to justify the bloated syntax. At a high level, the only reason one would need to
specify a different set of columns for computing the shard is if the set of index columns
was not sequential. In this case, the user shouldn't be sharding the index in the first
place, since load-based splitting should take care of finding viable split points in most
other common workload distributions. However, If we can think of a good use case for this
kind of thing, we should easily be able to support it.

[#31413]: https://github.com/cockroachdb/cockroach/pull/31413
[#42922]: https://github.com/cockroachdb/cockroach/pull/42922