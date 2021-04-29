- Feature Name: SQL Statistics Persistence
- Status: draft
- Start Date: 2021-03-30
- Authors: Archer Zhang
- RFC PR: [#63752](https://github.com/cockroachdb/cockroach/pull/63752)
- Cockroach Issue: [#56219](https://github.com/cockroachdb/cockroach/issues/56219)

# Summary

This RFC describes the motivation and the mechanism for persisting SQL
statistics. By persisting accumulated SQL statistics into a system table, we
can address the issue where currently CockroachDB loses accumulated statistics upon
restart/upgrade. This feature would also enable users of CockroachDB to examine and
compare the historical statistics of statements and transactions over time. As
a result, CockroachDB will gain the ability to help users to easily identify
historical transactions and statements that consume a disproportionate amount
of cluster resources, even after node crashes and restarts.

# Motivation

Currently, CockroachDB stores the statement and transaction metrics in memory.
The retention policy for the in-memory storage is one hour by default. During
this one-hour period, the user can query statistics stored in memory through the
DB Console. However, after the retention period for the collected statistics
expires, users are no longer to be able to access these statistics. There are a
few significant problems with the current setup:

1. Since the amount of statistics data we collected is limited to a one-hour
   period, operators have no way to compare the current statistics to the
   historical statistics in order to understand how the performance of
   queries has changed.
1. Since statement and transaction statistics are stored in memory, to
   aggregate statistics for the entire cluster, the CockroachDB node that is handling
   the RPC request (the gateway node) must fanout RPC calls to every single
   node in the cluster.
   1. Due to this design, if a node becomes unavailable, CockroachDB will be no longer
      able to provide accurate accounting for statement/transaction statistics.
      This can potentially impact the usefulness of having SQL statistics shown
      in the DB Console, as the unavailability of the node can be potentially
      the result of resource-hungry queries that were being executed on that
      particular node.
   1. Due to the reliance on the RPC-fanout, post-processing of the SQL
      statistics (e.g. sorting, filtering) are currently implemented within the DB
      Console. As we move toward storing and displaying historical statistics,
      solely relying on the DB Console to perform slicing-n-dicing of the
      statistics data is not scalable.
1. As CockroachDB is moving toward a multi-tenant architecture, relying on fanout RPC
   implies that tenant statistics aggregation will not only have the drawbacks
   mentioned previously, but also be depending on the progress of pod-to-pod
   communication implementation, which is not ideal.

The persistence of SQL statistics in a CockroachDB system table can address existing
drawbacks. CockroachDB will gain improvement in two areas:
1. **Usability**: currently, users have access to only the node-local SQL
  statistics within the SQL shell. The only way users can access cluster-level
  SQL statistics is through the DB Console. This means users' abilities to
  query SQL statistics is limited to the functionalities implemented by DB
  Console. With persistent SQL statistics, cluster-level SQL statistics are now
  available as system tables. Users will be able to run more complex SQL
  queries on the statistics tables directly through the SQL shell.
1. **Reliability**: with CockroachDB SQL statistics now backed by a persistent
  table, we will ensure the survival of the data across node
  crash/upgrade/restarts.

# Design

## Design Considerations

* Collected SQL statistics need to be available on every node that receives SQL
  queries and the accumulated statistics need to survive node restart/crash.

* Collected statistics should be able to answer users' potential questions for
  their queries over time through both DB Console and SQL shell.

* Statistics persistence should be low overhead, but the collected statistics
  should also have enough resolution to provide meaningful insight into the
  query/transaction performance.

* There is a need for a mechanism to prune old statistics data to reduce the
  burden on storage space. The setting for the pruning mechanism should also be
  accessible to users so that it can be changed to suit different needs.

* Statistics collection and statistics persistence should be decoupled.

## Design Overview

Two new system tables `system.sql_statement_stats` and
`system.sql_transaction_stats` provide storage for storing time series
data for accumulated statistics for statements and transactions.

We will also introduce the following new cluster settings:
* `sql.stats.flush_interval`: this dictates how often each node flushes
  stats to system tables.
* `sql.stats.aggregation_interval`: this setting dictates the size of the
  interval for the aggregation bucket for collecting statistics.
* `sql.stats.memory_limit`: this setting limits the amount of statistics data
  each node stores locally in their memories.
* `sql.stats.query_exec_time_flush_threshold`: this setting defines the
  threshold for query execution time. If the query takes longer than the
  threshold to execute, we immediately flush the collected statistics into
  the system table.

Currently, each CockroachDB node stores in-memory statistics for transactions
and statements for which the node is the gateway for. The in-memory statistics
are flushed into system tables in one of the following scenarios:
1. at the end of a flush fixed interval (determined by a cluster setting).
1. when the amount of statistics data stored in memory exceeds the limit
   defined by the cluster setting.
1. when a query's execution time exceeds the threshold defined by the cluster
   setting.
1. when node shuts down.

During the flush operation, for each statement and transaction fingerprint, the
CockroachDB node will check if there already exists the same fingerprint in the
persisted system tables within the latest aggregation window.
* if such entry exists, the flush operation will aggregate the existing entry.
* if such entry does not exist, the flush operation will insert a new entry.

When DB Console issues fetch requests to CockroachDB node through HTTP endpoint,
the persisted statistics data can be fetched using follower read 
queries in order to minimize read-write contention. However, for the most
up-to-date statistics, we still need to utilize RPC fanout to retrieve the
in-memory statistics from each node. The pros for this option are that this is
already what CockroachDB does today, and we already have a mechanism set up for
this. Consequentially, this means that this option also inherits the
disadvantage of the existing designs, such as data-loss on crashes, inaccurate
stats if nodes become unavailable, etc.

## Design Details

### System table schema

``` SQL
CREATE TABLE system.sql_statement_stats (
    first_run      TIMESTAMPTZ NOT NULL,
    fingerprint_id UUID NOT NULL,
    app_name       STRING NOT NULL,
    plan_hash      INT NOT NULL,
    node_id        INT NOT NULL,

    count        INT NOT NULL,
    agg_interval INTERVAL NOT NULL,

    metadata   JSONB NOT NULL,
    /*
    JSON Schema:
    {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "title": "system.sql_statement_stats.metadata",
      "type": "object",
      "properties": {
        "sql_type":             { "type": "string" },
        "query":                { "type": "string" },
        "database":             { "type": "string" },
        "schema":               { "type": "string" },
        "distsql":              { "type": "boolean" },
        "failed":               { "type": "boolean" },
        "opt":                  { "type": "boolean" },
        "implicit_transaction": { "type": "boolean" },
        "vec":                  { "type": "boolean" },
        "full_scan":            { "type": "boolean" },
        "first_run_at":         { "type": "string" },
        "last_run_at":          { "type": "string" },
      }
    }
    */
    statistics JSONB NOT NULL,
    /*
    JSON Schema
    {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "title": "system.sql_statement_stats.statistics",
      "type": "object",

      "definitions": {
        "numeric_stats": {
          "type": "object",
          "properties": {
            "mean": { "type": "number" },
            "squared_difference": { "type": "number" }
          },
          "required": ["mean", "squared_difference"]
        },
        "statistics": {
          "type": "object",
          "properties": {
            "first_attempt_count": { "type": "number" },
            "max_retries":         { "type": "number" },
            "num_rows":            { "$ref": "#/definitions/numeric_stats" },
            "parse_latency":       { "$ref": "#/definitions/numeric_stats" },
            "plan_latency":        { "$ref": "#/definitions/numeric_stats" },
            "run_latency":         { "$ref": "#/definitions/numeric_stats" },
            "service_latency":     { "$ref": "#/definitions/numeric_stats" },
            "overhead_latency":    { "$ref": "#/definitions/numeric_stats" },
            "bytes_read":          { "$ref": "#/definitions/numeric_stats" },
            "rows_read":           { "$ref": "#/definitions/numeric_stats" }
          },
          "required": [
            "first_attempt_count",
            "max_retries",
            "num_rows",
            "parse_latency",
            "plan_latency",
            "run_latency",
            "service_latency",
            "overhead_latency",
            "bytes_read",
            "rows_read"
          ]
        },
        "execution_statistics": {
          "type": "object",
          "properties": {
            "count":           { "type": "number" },
            "network_bytes":   { "$ref": "#/definitions/numeric_stats" },
            "max_mem_usage":   { "$ref": "#/definitions/numeric_stats" },
            "contention_time": { "$ref": "#/definitions/numeric_stats" },
            "network_message": { "$ref": "#/definitions/numeric_stats" },
            "max_disk_usage":  { "$ref": "#/definitions/numeric_stats" },
          },
          "required": [
            "count",
            "network_bytes",
            "max_mem_usage",
            "contention_time",
            "network_message",
            "max_disk_usage",
          ]
        }
      },

      "properties": {
        "statistics": { "$ref": "#/definitions/statistics" },
        "execution_statistics": {
          "$ref": "#/definitions/execution_statistics"
        }
      }
    }
    */

    plan BYTES NOT NULL,

    PRIMARY KEY (first_run, fingerprint_id, plan_hash, app_name) 
      USING HASH WITH BUCKET_COUNT = 8,
    INDEX (fingerprint_id, first_run, plan_hash, app_name, node_id)
);

CREATE TABLE system.sql_transaction_stats (
    first_run      TIMESTAMPTZ NOT NULL,
    fingerprint_id UUID NOT NULL,
    app_name       STRING NOT NULL,
    node_id        INT NOT NULL,

    count        INT NOT NULL,
    agg_interval INTERVAL NOT NULL,

    metadata   JSONB NOT NULL,
    /*
    JSON Schema:
    {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "title": "system.sql_transaction_stats.metadata",
      "type": "object",
      "properties": {
        "statement_fingerprint_ids": {
          "type": "array",
          "items": {
            "type": "number"
          }
        },
        "first_run_at": { "type": "string" },
        "last_run_at":  { "type": "string" }
      }
    }
    */

    statistics JSONB NOT NULL,
    /*
    JSON Schema
    {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "title": "system.sql_statement_stats.statistics",
      "type": "object",

      "definitions": {
        "numeric_stats": {
          "type": "object",
          "properties": {
            "mean": { "type": "number" },
            "squared_difference": { "type": "number" }
          },
          "required": ["mean", "squared_difference"]
        },
        "statistics": {
          "type": "object",
          "properties": {
            "max_retries":         { "type": "number" },
            "num_rows":            { "$ref": "#/definitions/numeric_stats" },
            "service_latency":     { "$ref": "#/definitions/numeric_stats" },
            "retry_latency":       { "$ref": "#/definitions/numeric_stats" },
            "commit_latency":      { "$ref": "#/definitions/numeric_stats" },
            "bytes_read":          { "$ref": "#/definitions/numeric_stats" },
            "rows_read":           { "$ref": "#/definitions/numeric_stats" }
          },
          "required": [
            "max_retries",
            "num_rows",
            "service_latency",
            "retry_latency",
            "commit_latency",
            "bytes_read",
            "rows_read",
          ]
        },
        "execution_statistics": {
          "type": "object",
          "properties": {
            "count":           { "type": "number" },
            "network_bytes":   { "$ref": "#/definitions/numeric_stats" },
            "max_mem_usage":   { "$ref": "#/definitions/numeric_stats" },
            "contention_time": { "$ref": "#/definitions/numeric_stats" },
            "network_message": { "$ref": "#/definitions/numeric_stats" },
            "max_disk_usage":  { "$ref": "#/definitions/numeric_stats" },
          },
          "required": [
            "count",
            "network_bytes",
            "max_mem_usage",
            "contention_time",
            "network_message",
            "max_disk_usage",
          ]
        }
      },

      "properties": {
        "statistics": { "$ref": "#/definitions/statistics" },
        "execution_statistics": {
          "$ref": "#/definitions/execution_statistics"
        }
      }
    }
    */

    -- protobuf
    stats BYTES NOT NULL,

    PRIMARY KEY (first_run, fingerprint_id, app_name) 
      USING HASH WITH BUCKET_COUNT = 8,
    INDEX (fingerprint_id, first_run, node_id, app_name)
);
```

The first two columns of the primary keys for both tables contain
`first_run` timestamp and `fingerprint_id`. The primary key utilizes hash-sharding
with 8 buckets. There are two reasons for this design:
1. Using hash-sharded primary key avoids writing contentions since `first_run`
   column contains a monotonically increasing sequence of timestamps. This would
   allow us to achieve linear scaling.
1. This speeds up the use case where we want to show aggregated statistics for
   each fingerprint for the past few hours or days.

The last column in the primary key is `app_name`. This stores the name of the
application that issued the SQL statement. This is included because same
statements issued from different applications would have same `fingerprint_id`.
Therefore, having `app_name` as part of the primary key is important to
distinguish same statements from different applications.

We also have an index for `(fingerprint_id, first_run, node_id, app_name)`. This
index aims to improve the efficiency of the use case where we want to inspect
the historical performance of the given query for a given time window. We have
an additional `node_id` column for sharding purposes. This avoids write
contentions in a large cluster.

For the statistics payload, we use multiple `JSONB` columns. The structure of
each `JSONB` column is documented inline using
[JSON schema](https://json-schema.org/). This gives us the flexibility to
continue iterating in the future without worrying about schema migration. Using
`JSONB` over directly storing them as protobuf allows us to query the fields
inside the JSON object, whereas the internal of the protobuf is opaque to the
SQL engine.

Additionally, we store the serialized query plan for each statement in a
separate column to provide the ability to inspect plan changes for a given
fingerprint over a period of time. We also store `count` as a separate column
since we frequently need this value when we need to combine multiple entries
into one.

Lastly, we store 'agg_interval' column, which is the length of the time that
the stats in this entry is collected over. This is particularly useful during
the garbage collection later.

### Example queries that can be used to answer query performance-related questions:

#### Querying attributes over a time period for a statement.

``` SQL
SELECT 
  first_run,
  fingerprint_id,
  count,
  statistics -> 'statistics' -> 'retries',
FROM system.sql_statement_stats
     AS OF SYSTEM TIME follower_read_timestamp()
WHERE fingerprint_id = $1
  AND first_run < $2
  AND first_run > $3
ORDER BY
  first_run;
```

#### Query execplan over a time period for a statement used by an app.

``` SQL
SELECT DISTINCT
  fingerprint_id,
  plan
FROM system.sql_statement_stats
     AS OF SYSTEM TIME follower_read_timestamp()
WHERE fingerprint_id = $1
  AND first_run < $2
  AND first_run > $3
  AND app_name = $4
ORDER BY
  first_run;
```

#### Show top offending statements by attribute for a given period of time.

``` SQL
SELECT
  fingerprint_id,
  SUM(total_service_latency) / SUM(count) as avg_service_latency,
  SUM(total_rows_read) / SUM(count) as avg_total_rows_read
FROM (
  SELECT
    fingerprint_id,
    count,
    count * statistics -> 'service_latency' AS total_service_latency,
    count * statistics -> 'rows_read' AS total_rows_read
  FROM system.sql_statement_stats
       AS OF SYSTEM TIME follower_read_timestamp()
  WHERE first_run < $1
    AND first_run > $2
)
GROUP BY
  fingerprint_id
ORDER BY
  (avg_service_latency, avg_total_rows_read);
```

However, if we are to aggregate both the mean and the squared differences for
each attribute, it would be more difficult and we would have to implement it
using recursive CTE.

``` sql
WITH RECURSIVE map AS (
  SELECT
    LEAD(first_run, 1)
      OVER (ORDER BY (first_run, fingerprint_id)) AS next_first_run,
    LEAD(fingerprint_id, 1)
      OVER (ORDER BY (first_run, fingerprint_id)) AS next_fingerprint_id,
    system.sql_statement_stats.first_run,
    system.sql_statement_stats.fingerprint_id,
    system.sql_statement_stats -> 'statistics' -> 'mean' AS mean,
    system.sql_statement_stats -> 'statistics' -> 'squared_diff' AS squared_diff,
    system.sql_statement_stats.count
  FROM
    system.sql_statement_stats
     AS OF SYSTEM TIME follower_read_timestamp()
  WHERE fingerprint_id = $1
    AND first_run >= $2
    AND first_run < $3
  ORDER BY
    (system.sql_statement_stats.first_run, system.sql_statement_stats.fingerprint_id)
),
reduce AS (
  (
    SELECT
      map.next_first_run,
      map.next_fingerprint_id,
      map.first_run,
      map.fingerprint_id,
      map.mean,
      map.squared_diff,
      map.count
    FROM
      map
    ORDER BY
      (map.first_run, map.fingerprint_id)
    LIMIT 1
  )
UNION ALL
  (
    SELECT
      map.next_first_run,
      map.next_fingerprint_id,
      map.first_run,
      map.fingerprint_id,
      (map.mean * map.count::FLOAT + reduce.mean * reduce.count::FLOAT) / (map.count + reduce.count)::FLOAT
        AS mean,
      (map.squared_diff + reduce.squared_diff) + ((POWER(map.mean - reduce.mean, 2) * (map.count * reduce.count)::FLOAT) / (map.count + reduce.count)::FLOAT)
        AS squared_diff,
      map.count + reduce.count AS count
    FROM
      map
    JOIN
      reduce
    ON
      map.first_run = reduce.next_ts AND
      map.fingerprint_id = reduce.next_fingerprint_id
    WHERE
      map.first_run IS NOT NULL OR
      map.fingerprint_id IS NOT NULL
    ORDER BY
      (map.first_run, map.fingerprint_id)
  )
)

SELECT * FROM reduce ORDER BY (first_run, fingerprint_id) DESC LIMIT 1;

```

### Writing in-memory stats to system tables

When we flush in-memory stats to a system table, the operation is executed in
a single **atomic** transaction, consisting of the following steps:

1. For each statement and transaction fingerprint stored in memory, we check
   if the same fingerprint already exists in the current aggregation window.

   This can be implemented using the following query:

``` sql
SELECT statistics, count
FROM system.sql_statement_stats
WHERE first_run > (current_timestamp() - MOD(EXTRACT(EPOCH FROM current_timestamp())::INT, 5 * 60)::INTERVAL)
  AND fingerprint_id = $1
  AND app_name = $2
  AND plan_hash = $3
  AND node_id = $4
```

1. If such a fingerprint exists, we fetch the fingerprint and combine it with the
   statistics we have in-memory.
1. Delete the existing statistics entry.
1. Insert the newly combined statistics.

Since we combine the persisted statistics with the statistics stored in memory,
we ensured that we would not be creating any more entries than what we already
have. Therefore, we do not have the risk of running out of memory during the
operation.

Also, the primary keys for statistics tables include a field for `node_id`,
this is so that we can avoid having multiple transactions writing to the same
key. This prevents the flush operation from dealing with transaction retries.

### Garbage collection

Garbage collection is an important piece of the puzzle to the persisted SQL stats
as we want to prevent infinite growth of the system table.

To facilitate GC, we introduce the following settings:
* `sql.stats.gc.interval` is the setting for how often the garbage collection
  job will be ran.
* `sql.stats.gc.max_row_limit` is the maximum number of rows we want to retain
  in the system table.

#### MVP Version

In MVP version, the garbage collection process is very simple. It will utilize
the job system to ensure that we only have one GC job in the cluster. During
the execution of the GC job, it will check the number of entries in both the
transaction and statement statistics system tables, and it will remove the
oldest entries that exceed the maximum row limit.

#### Full implementation

In the full implementation, in addition to removing the oldest entries from
the system tables, we want to also aggregate the older entries and downsample
them into a larger aggregation window. This way, we would be able to store
more historical data without incurring more storage overhead.

In the full implementation, we would introduce additional settings:
* `sql.stats.gc.agg_window_amplify_factor`: this setting dictates each time
  when GC job downsamples statistics, how much larger do we want to increase
  the aggregation interval by.
* `sql.stats.gc.max_agg_interval`: this setting dictates maximum interval for
  aggregation window. GC job will not downsample statistics any further after
  the aggregation window has reached this point.

Since we have `node_id` as part of the primary key, the number of entries in the
system tables for each aggregation window are
`num_of_nodes * num_of_unique_fingerprint`. Therefore, by implementing
downsampling in the full implementation of GC, we will be able to remove the
cluster size as a factor of the growth for the number of entries in the system
tables.

## Monitoring and Failure Scenarios

In a resilient system, it is important to timely detect issues and gracefully
handle them as they arise.

### Monitoring

For the flush operation, it will expose two metrics that we can monitor
* Flush count: this metric records the number of times that the flush operation has
  been executed. A high flush count value can potentially indicate frequent
  unusually slow queries, or it could also indicate memory pressure caused by
  the spiking number of queries with distinct fingerprints.
* Flush duration: this metric records how long each flush operation takes. An
  unusually high flush latency could potentially indicate contention in certain
  parts of the system.
* Error count: this metric records number of errors the flush operation
  encounters. Any spike in this metric suggests suboptimal health of the
  system.

Garbage collection:

* GC Duration: this metric records the amount of time it takes to complete
  each garbage collection operation. An usually high garbage duration is a
  good indicator that something might be wrong.
* Error Count: similar to the error count metrics for the flush operation,
  error count can be useful to monitor the overall health of the system.

### Handling Failures

Since the SQL statistics persistence depends on system tables, this means
it is possible for us to experience failures if system tables become
unavailable. When we experience system table failures, we want to gradually
degrade our service quality gracefully.

* Read path: if we are to lose quorum, CockroachDB will reject any future
  write requests while still be able to serve read requests. In this case,
  we should still be able to serve all the read requests from the system tables
  and combine them with in-memory statistics.
  However, in the case where we lose the ability to read from system tables, then we
  will only be serving statistics from the in-memory store at the best-effort
  basis.
* Write path: if we lose ability to write to system table, that means the
  statistics accumulated in-memory in each node will no longer be able to
  be persisted. In this case, we will record all statistics in-memory on a
  best-effort basis. For any fingerprints that are already present in the memory,
  we will record new statistics since it does not incur additional memory
  overhead. However, if we are to record a new fingerprint and we are at
  the maximum memory capacity, we will have no choice but discard the new
  fingerprint.

In the scenario where system table becomes unavailable, we would also want to
disable flush and GC operations via cluster setting to avoid cluster resources
being unnecessarily spent on operations that are doomed to fail.

## Drawbacks

* In order to retrieve the most up-to-date statistics that are yet to be
  flushed to system table, we would be fall back to using RPC fanout to contact
  every single node in the cluster. This might not scale well in a very large
  cluster. This can be potentially addressed via reducing the flush interval.
  However, this comes at the cost of higher IO overhead to the cluster.

## Rationale and Alternatives

* Instead of deleting the oldest stats entries from the system table in the
  stage 5 of the flush operation, we can alternatively delete all stats in the
  oldest aggregation window. This is because for any given transaction
  fingerprint in an aggregation window, all the statement fingerprints that
  such transaction references to, must also be present in the statement table
  within the same aggregation window. (Note: I think this can be formally
  proven)So if we instead delete all the stats belonging to the oldest
  aggregation window, we can ensure that all the statement fingerprints
  referenced by transactions are valid in the statement table.

## Future Work

* We want to account in-memory structure size using a memory monitor. This is
  to avoid OOM when there are a lot of distinct fingerprint stored in memory.
  This also allows us to flush the stats into system table in time before
  the memory limit has reached.

* Instead of aggregating statistics in-memory at the gateway node, or writing
  complex CTE queries, we can create specialized DistSQL operators to perform
  aggregation on `NumericStats` type.

* We want to have the ability to throttle ourselves during the GC job if the
  cluster load in order not to overload the cluster resources.

* We want to have a circuit breaker in place for flush/GC operations. If too
  many errors occur, we want to take a break and degrade our service quality
  gracefully without overwhelming the system.
