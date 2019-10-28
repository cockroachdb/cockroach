- Feature Name: Easier Hash Sharded Indexes
- Status: draft
- Start Date: 2019-10-28
- Authors: Aayush Shah, Andrew Werner
- RFC PR: PR # after acceptance of initial draft
- Cockroach Issue: [#39340] (https://github.com/cockroachdb/cockroach/issues/39340)

# Summary

This is a proposal to provide better UX for creating hash sharded indexes through easier
syntax. This allows a useful mechanism to alleviate hot spots due to sequential write
workloads. 

# Motivation

In 19.2, we added optimizer support to automatically add filters based on check
constraints. This can allow users to alleviate single range hotspots during sequential
workloads by creating an index on a computed shard column. However, this feature still
requires some relatively unattractive syntax to manually add a computed column which will
act as the shard key. This is illustrated in the following example from issue [#39340]
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

This schema would have a hot spot on that INDEX (ts) which would be rather unfortunate. We
can alleviate this hot spot by sharding this time ordered index.

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
of hashing and installing a check constraint behind a new syntax:

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
    INDEX (ts) USING HASH (product_id, owner, serial_number) WITH (BUCKET_COUNT=8)
);
```

# Benchmarks 

 We benchmarked the sequential kv workload against two schemas:

 (1) Using hash sharded primary index on `k`.
```sql
CREATE TABLE kv (
    k INT8 NOT NULL,
    v bytes NOT NULL,
    shard 
        INT8 AS (k % 8) stored 
        CHECK (shard IN (0, 1, 2, 3, 4, 5, 6, 7)), 
    PRIMARY KEY (shard, k)
);
```

(2) Using primary index on `k`.
```sql
CREATE TABLE kv (
    k INT8 NOT NULL,
    v BYTES NOT NULL,
    PRIMARY KEY (k)
);
```

 The following benchmarks were all run on a 7 node GCE cluster with `n1-standard-32`
 machines. We first demonstrate write performance with `kv --sequential --duration=5m` at
 varying levels of concurrency (256, 384, 512). Note that this is a write-only workload.


![Latency](https://user-images.githubusercontent.com/10788754/67722397-63c5a100-f9af-11e9-8316-a173e52fe20b.png)


![Throughput](https://user-images.githubusercontent.com/10788754/67733735-8ec2eb80-f9d5-11e9-889f-38b4466d7234.png)

## Drawbacks / Future work

The optimizer currently doesn't derive the value of a stored computed column even when all
the columns that it references are available. This causes performance to heavily suffer in
sequential workloads that aren't dominated by writes.

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

We ran `kv --sequential --concurrency=256 --read-percent=90` on both those schemas, with the following results:

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