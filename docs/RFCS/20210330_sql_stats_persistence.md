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
expires, users are no longer able to access these statistics. There are a
few significant problems with the current setup:

1. Since the amount of statistics data we collected is limited to a one-hour
   period, operators have no way to compare the current statistics to the
   historical statistics in order to understand how the performance of
   queries has changed.
1. Since statement and transaction statistics are stored in memory, to
   aggregate statistics for the entire cluster, the CockroachDB node that is
   handling the RPC request (the gateway node) must fanout RPC calls to every
   single node in the cluster.
   1. Due to the reliance on RPC fanout, post-processing of the SQL statistics
      (e.g. sorting, filtering) are currently implemented within the DB Console.
      As we move to implement displaying and comparing historical statistics,
      solely relying on the DB Console to perform slicing-n-dicing of the
      statistics data is not scalable.
   1. Also because currently we implement post-processing of the SQL statistics
      in the DB Console, users lack the ability to view and analyze SQL
      statistics within the SQL shell. This results in poor UX.

With the persistence of SQL statistics, CockroachDB will gain improvement in
the following areas:
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

Two new system tables `system.statement_statistics` and
`system.transaction_statistics` provide storage for storing time series
data for accumulated statistics for statements and transactions.

We will also introduce the following new cluster settings:
* `sql.stats.flush_interval`: this dictates how often each node flushes
  stats to system tables.
* `sql.stats.memory_limit`: this setting limits the amount of statistics data
  each node stores locally in their memory.

Currently, each CockroachDB node stores in-memory statistics for transactions
and statements for which the node is the gateway for. The in-memory statistics
are flushed into system tables in one of the following scenarios:
1. at the end of a fixed flush interval (determined by a cluster setting).
1. when the amount of statistics data stored in memory exceeds the limit
   defined by the cluster setting.
1. when node shuts down.

During the flush operation, for each statement and transaction fingerprint, the
CockroachDB node will check if there already exists the same fingerprint in the
persisted system tables within the latest aggregation interval.
* if such entry exists, the flush operation will aggregate the existing entry.
* if such entry does not exist, the flush operation will insert a new entry.

However, if we are filling up our memory buffer faster than we can flush them
into system tables, and also since it is not desirable to block query
execution, our only option here is to discard the new incoming statistics.
We would be keeping track of number of statistics we have to discard using a
counter and expose this as a metric. This allows administrators to monitor
the health of the system.

When DB Console issues fetch requests to CockroachDB node through HTTP endpoint,
the code that handles the HTTP request will be updated to fetch the persisted
statistics using follower read to minimize read-write contention. For the most
up-to-date statistics, we would still need to utilize RPC fanout to retrieve the
in-memory statistics from each node. However, the current implementation of our
HTTP handler buffers all statistics it fetched from the cluster in memory
before returning to the client. This can cause potential issue as we extend
this HTTP endpoint to return the persisted statistics from the system table.
The amount of persisted statistics can potentially exceed the available memory
on the gateway node and cause the node to crash because of OOM. Therefore, we
also need to update the existing HTTP endpoint to enable pagination. This way,
we can prevent the HTTP handler from unboundedly buffering statistics in-memory
when it reads from the system table.

It is also worth noting that keeping the RPC fanout in this case will not make
the existing situation worse since the response from RPC fanout will still only
contain the statistics stored in-memory in all other nodes.

Additionally, we can implement a system view to transparently combine persisted
stats from the system tables and in-memory stats fetched using RPC fanout. This
allows users to access both historical and most up-to-date statistics within
the SQL shell.

Lastly, since the new system tables will be accessed frequently, in order to
prevent [bottleneck](https://github.com/cockroachdb/cockroach/pull/63241) in
name resolution, we want to cache the table descriptors for the new system
tables.

## Design Details

### System table schema

``` SQL
CREATE TABLE system.statement_statistics (
    aggregated_ts  TIMESTAMPTZ NOT NULL,
    fingerprint_id BYTES NOT NULL,
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
      "title": "system.statement_statistics.metadata",
      "type": "object",
      "properties": {
        "stmtTyp":              { "type": "string" },
        "query":                { "type": "string" },
        "db":                   { "type": "string" },
        "schema":               { "type": "string" },
        "distsql":              { "type": "boolean" },
        "failed":               { "type": "boolean" },
        "opt":                  { "type": "boolean" },
        "implicitTxn":          { "type": "boolean" },
        "vec":                  { "type": "boolean" },
        "fullScan":             { "type": "boolean" },
        "firstExecAt":          { "type": "string" },
        "lastExecAt":           { "type": "string" },
      }
    }
    */
    statistics JSONB NOT NULL,
    /*
    JSON Schema
    {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "title": "system.statement_statistics.statistics",
      "type": "object",

      "definitions": {
        "numeric_stats": {
          "type": "object",
          "properties": {
            "mean":   { "type": "number" },
            "sqDiff": { "type": "number" }
          },
          "required": ["mean", "sqDiff"]
        },
        "statistics": {
          "type": "object",
          "properties": {
            "firstAttemptCnt":   { "type": "number" },
            "maxRetries":        { "type": "number" },
            "numRows":           { "$ref": "#/definitions/numeric_stats" },
            "parseLat":          { "$ref": "#/definitions/numeric_stats" },
            "planLat":           { "$ref": "#/definitions/numeric_stats" },
            "runLat":            { "$ref": "#/definitions/numeric_stats" },
            "serviceLat":        { "$ref": "#/definitions/numeric_stats" },
            "overheadLat":       { "$ref": "#/definitions/numeric_stats" },
            "bytesRead":         { "$ref": "#/definitions/numeric_stats" },
            "rowsRead":          { "$ref": "#/definitions/numeric_stats" }
          },
          "required": [
            "firstAttemptCnt",
            "maxRetries",
            "numRows",
            "parseLat",
            "planLat",
            "runLat",
            "serviceLat",
            "overheadLat",
            "bytesRead",
            "rowsRead"
          ]
        },
        "execution_statistics": {
          "type": "object",
          "properties": {
            "cnt":             { "type": "number" },
            "networkBytes":    { "$ref": "#/definitions/numeric_stats" },
            "maxMemUsage":     { "$ref": "#/definitions/numeric_stats" },
            "contentionTime":  { "$ref": "#/definitions/numeric_stats" },
            "networkMsg":      { "$ref": "#/definitions/numeric_stats" },
            "maxDiskUsage":    { "$ref": "#/definitions/numeric_stats" },
          },
          "required": [
            "cnt",
            "networkBytes",
            "maxMemUsage",
            "contentionTime",
            "networkMsg",
            "maxDiskUsage",
          ]
        }
      },

      "properties": {
        "stats": { "$ref": "#/definitions/statistics" },
        "execStats": {
          "$ref": "#/definitions/execution_statistics"
        }
      }
    }
    */

    plan BYTES NOT NULL,

    PRIMARY KEY (aggregated_ts, fingerprint_id, plan_hash, app_name, node_id)
      USING HASH WITH BUCKET_COUNT = 8,
    INDEX (fingerprint_id, aggregated_ts, plan_hash, app_name, node_id)
);

CREATE TABLE system.transaction_statistics (
    aggregated_ts  TIMESTAMPTZ NOT NULL,
    fingerprint_id BYTES NOT NULL,
    app_name       STRING NOT NULL,
    node_id        INT NOT NULL,

    count        INT NOT NULL,
    agg_interval INTERVAL NOT NULL,

    metadata   JSONB NOT NULL,
    /*
    JSON Schema:
    {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "title": "system.transaction_statistics.metadata",
      "type": "object",
      "properties": {
        "stmtFingerprintIDs": {
          "type": "array",
          "items": {
            "type": "number"
          }
        },
        "firstExecAt": { "type": "string" },
        "lastExecAt":  { "type": "string" }
      }
    }
    */

    statistics JSONB NOT NULL,
    /*
    JSON Schema
    {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "title": "system.statement_statistics.statistics",
      "type": "object",

      "definitions": {
        "numeric_stats": {
          "type": "object",
          "properties": {
            "mean":   { "type": "number" },
            "sqDiff": { "type": "number" }
          },
          "required": ["mean", "sqDiff"]
        },
        "statistics": {
          "type": "object",
          "properties": {
            "maxRetries": { "type": "number" },
            "numRows":    { "$ref": "#/definitions/numeric_stats" },
            "serviceLat": { "$ref": "#/definitions/numeric_stats" },
            "retryLat":   { "$ref": "#/definitions/numeric_stats" },
            "commitLat":  { "$ref": "#/definitions/numeric_stats" },
            "bytesRead":  { "$ref": "#/definitions/numeric_stats" },
            "rowsRead":   { "$ref": "#/definitions/numeric_stats" }
          },
          "required": [
            "maxRetries",
            "numRows",
            "serviceLat",
            "retryLat",
            "commitLat",
            "bytesRead",
            "rowsRead",
          ]
        },
        "execution_statistics": {
          "type": "object",
          "properties": {
            "cnt":             { "type": "number" },
            "networkBytes":    { "$ref": "#/definitions/numeric_stats" },
            "maxMemUsage":     { "$ref": "#/definitions/numeric_stats" },
            "contentionTime":  { "$ref": "#/definitions/numeric_stats" },
            "networkMsg":      { "$ref": "#/definitions/numeric_stats" },
            "maxDiskUsage":    { "$ref": "#/definitions/numeric_stats" },
          },
          "required": [
            "cnt",
            "networkBytes",
            "maxMemUsage",
            "contentionTime",
            "networkMsg",
            "maxDiskUsage",
          ]
        }
      },

      "properties": {
        "stats": { "$ref": "#/definitions/statistics" },
        "execStats": {
          "$ref": "#/definitions/execution_statistics"
        }
      }
    }
    */

    PRIMARY KEY (aggregated_ts, fingerprint_id, app_name, node_id)
      USING HASH WITH BUCKET_COUNT = 8,
    INDEX (fingerprint_id, aggregated_ts, app_name, node_id)
);
```

The first two columns of the primary keys for both tables contain
`aggregated_ts` column and `fingerprint_id` column. `aggregated_ts` is the
timestamp of the beginning of the aggregation interval. This ensures that for
all entries for every statement and transaction within the same aggregation
window, the will have the same `aggregated_ts` value. This makes cross-query
comparison cleaner and also enables us to use `INSERT ON CONFLICT`-style
statements.

The primary key utilizes hash-sharding with 8 buckets. There are two reasons
for this design:
1. Using hash-sharded primary key avoids writing contentions since `aggregated_ts`
   column contains a monotonically increasing sequence of timestamps. This would
   allow us to achieve linear scaling.
1. This speeds up the use case where we want to show aggregated statistics for
   each fingerprint for the past few hours or days.

The last column in the primary key is `app_name`. This stores the name of the
application that issued the SQL statement. This is included because same
statements issued from different applications would have same `fingerprint_id`.
Therefore, having `app_name` as part of the primary key is important to
distinguish same statements from different applications.

We also have an index for `(fingerprint_id, aggregated_ts, app_name, node_id)`.
This index aims to improve the efficiency of the use case where we want to inspect
the historical performance of a given query for a given time window. We have
an additional `node_id` column for sharding purposes. This avoids write
contentions in a large cluster.

Hash-sharded secondary index is not necessary here. This is because based on
our experience, the customer clusters that are the most performance sensitive
are also those that have tuned their SQL apps to only send a very small number
of different queries, but in large numbers. This means that in such clusters,
since we store statistics per statement fingerprint, we would have a small
number of unique statement fingerprints to begin with. This implies that we are
unlikely to experience high memory pressure (which can lead to frequent flush
operations), and this also means that we will have less statistics to flush to
the system table per flush interval. Conversely, if the cluster does issue large
number of unique fingerprint, then we can assume that fingerprints are
sufficiently numerous to ensure an even distribution.

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
the stats in this entry is collected over. Initially, `agg_interval` equals to
the flush interval defined by the cluster setting.

### Example queries that can be used to answer query performance-related questions:

#### Querying attributes over a time period for a statement.

``` SQL
SELECT 
  aggregated_ts,
  fingerprint_id,
  count,
  statistics -> 'statistics' -> 'retries',
FROM system.statement_statistics
     AS OF SYSTEM TIME follower_read_timestamp()
WHERE fingerprint_id = $1
  AND aggregated_ts < $2
  AND aggregated_ts > $3
ORDER BY
  aggregated_ts;
```

#### Query execplan over a time period for a statement used by an app.

``` SQL
SELECT DISTINCT
  fingerprint_id,
  plan
FROM system.statement_statistics
     AS OF SYSTEM TIME follower_read_timestamp()
WHERE fingerprint_id = $1
  AND aggregated_ts < $2
  AND aggregated_ts > $3
  AND app_name = $4
ORDER BY
  aggregated_ts;
```

#### Show top offending statements by attribute for a given period of time.

``` SQL
SELECT
  fingerprint_id,
  SUM(total_service_lat) / SUM(count) as avg_service_lat,
  SUM(total_rows_read) / SUM(count) as avg_total_rows_read
FROM (
  SELECT
    fingerprint_id,
    count,
    count * statistics -> 'service_lat' AS total_service_lat,
    count * statistics -> 'rows_read' AS total_rows_read
  FROM system.statement_statistics
       AS OF SYSTEM TIME follower_read_timestamp()
  WHERE aggregated_ts < $1
    AND aggregated_ts > $2
)
GROUP BY
  fingerprint_id
ORDER BY
  (avg_service_lat, avg_total_rows_read);
```

However, if we are to aggregate both the mean and the squared differences for
each attribute, it would be more difficult and we would have to implement it
using recursive CTE.

``` sql
WITH RECURSIVE map AS (
  SELECT
    LEAD(aggregated_ts, 1)
      OVER (ORDER BY (aggregated_ts, fingerprint_id)) AS next_aggregated_ts,
    LEAD(fingerprint_id, 1)
      OVER (ORDER BY (aggregated_ts, fingerprint_id)) AS next_fingerprint_id,
    system.statement_statistics.aggregated_ts,
    system.statement_statistics.fingerprint_id,
    system.statement_statistics -> 'statistics' -> 'mean' AS mean,
    system.statement_statistics -> 'statistics' -> 'squared_diff' AS squared_diff,
    system.statement_statistics.count
  FROM
    system.statement_statistics
     AS OF SYSTEM TIME follower_read_timestamp()
  WHERE fingerprint_id = $1
    AND aggregated_ts >= $2
    AND aggregated_ts < $3
  ORDER BY
    (system.statement_statistics.aggregated_ts, system.statement_statistics.fingerprint_id)
),
reduce AS (
  (
    SELECT
      map.next_aggregated_ts,
      map.next_fingerprint_id,
      map.aggregated_ts,
      map.fingerprint_id,
      map.mean,
      map.squared_diff,
      map.count
    FROM
      map
    ORDER BY
      (map.aggregated_ts, map.fingerprint_id)
    LIMIT 1
  )
UNION ALL
  (
    SELECT
      map.next_aggregated_ts,
      map.next_fingerprint_id,
      map.aggregated_ts,
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
      map.aggregated_ts = reduce.next_ts AND
      map.fingerprint_id = reduce.next_fingerprint_id
    WHERE
      map.aggregated_ts IS NOT NULL OR
      map.fingerprint_id IS NOT NULL
    ORDER BY
      (map.aggregated_ts, map.fingerprint_id)
  )
)

SELECT * FROM reduce ORDER BY (aggregated_ts, fingerprint_id) DESC LIMIT 1;

```

### Writing in-memory stats to system tables

When we flush in-memory stats to a system table, we execute everything within
a single **atomic** transaction.

Flush operation is executed by one of the three triggers:
1. Regular flush interval.
1. Memory pressure.
1. Node shutdown.

It is possible that if a CockroachDB node experiences memory pressure, it will
flush in-memory statistics to disk prior to the end of the regular flush
interval. Therefore, there is a possibility where the fingerprint id
the node is trying to insert is already present in the system table within the
current aggregation interval. This, and also because we fix `aggregated_ts` column
of each row to be the beginning of its respective aggregation interval, we would
have to deal with this conflict.

This means that the insertion need to be implemented using
`INSERT ON CONFLICT DO UPDATE` query, where we would combine the persisted
statistics for the given fingerprint with the in-memory statistics.

Upon confirming that all statistics stored in-memory have been successfully
written to disk, the flush operation clears the in-memory stores.

Also, the primary keys for statistics tables include a field for `node_id`,
this is so that we can avoid having multiple transactions writing to the same
key. This prevents the flush operation from dealing with transaction retries.
We will cover the cleanup operations in the next section.

### Cleanup

Cleanup is an important piece of the puzzle to the persisted SQL stats
as we want to prevent infinite growth of the system table.

To facilitate cleanup, we introduce the following settings:
* `sql.stats.cleanup.interval` is the setting for how often the cleanup
  job will be ran.
* `sql.stats.cleanup.max_row_limit` is the maximum number of rows we want to retain
  in the system table.

#### MVP Version

In MVP version, the cleanup process is very simple. It will utilize
the job system to ensure that we only have one cleanup job in the cluster. During
the execution of the cleanup job, it will check the number of entries in both the
transaction and statement statistics system tables, and it will remove the
oldest entries that exceed the maximum row limit.

#### Full implementation

In the full implementation, in addition to removing the oldest entries from
the system tables, we want to also aggregate the older entries and downsample
them into a larger aggregation interval. This way, we would be able to store
more historical data without incurring more storage overhead.

In the full implementation, we would introduce additional settings:
* `sql.stats.cleanup.agg_window_amplify_factor`: this setting dictates each time
  when cleanup job downsamples statistics, how much larger do we want to increase
  the aggregation interval by.
* `sql.stats.cleanup.max_agg_interval`: this setting dictates maximum interval for
  aggregation interval. Cleanup job will not downsample statistics any further after
  the aggregation interval has reached this point.

Since we have `node_id` as part of the primary key, the number of entries in the
system tables for each aggregation interval are
`num_of_nodes * num_of_unique_fingerprint`. Therefore, by implementing
downsampling in the full implementation of cleanup, we will be able to remove the
cluster size as a factor of the growth for the number of entries in the system
tables.

## Monitoring and Failure Scenarios

In a resilient system, it is important to timely detect issues and gracefully
handle them as they arise.

### Monitoring

For the flush operation, it will expose three metrics that we can monitor
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

Cleanup:

* Cleanup Duration: this metric records the amount of time it takes to complete
  each cleanup operation. An usually high cleanup duration is a
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
  However, in the case where we lose the ability to read from system tables,
  then our only option is to serve statistics from the in-memory stores at the
  best-effort basis. This can happen due to various factors that are out of
  our control. In order to guard against this failure scenario, we should also
  include a context timeout (default 20 seconds) in the API endpoint so that we
  can abort an operation if it takes too long. This timeout value is
  configurable through the cluster setting `sql.stats.query_timeout`. The RPC
  response in this case would also set its error field to indicate that it is
  operating at a degraded status and it is only returning partial results from
  the in-memory stores. Since we are still trying to serve statistics to users
  with our best-effort, the RPC response would still include all the statistics
  that the gateway node would be able to fetch and aggregate from all other
  nodes. The unavailability of the other nodes during the RPC fanout can also
  be handled in a similar fashion with a different error message in the error
  field of the RPC response.
* Write path: if we lose ability to write to system table, that means the
  statistics accumulated in-memory in each node will no longer be able to
  be persisted. In this case, we will record all statistics in-memory on a
  best-effort basis. For any fingerprints that are already present in the memory,
  we will record new statistics since it does not incur additional memory
  overhead. However, if we are to record a new fingerprint and we are at
  the maximum memory capacity, we will have to discard the new fingerprint.
  We will also be tracking number of statistics we discard using the same
  counter that was described in the earlier section. Nevertheless, this policy
  is not entirely ideal, because this policy is based on the assumption that
  existing entries in the in-memory store would contain more sample size and
  thus more statistically significant. This assumption may not be entirely
  accurate to reflect the real-life scenarios. This is fine for now to be
  implemented in the MVP. A better approach which we hope to eventually adopt
  is described in the Future Work section.

In the scenario where system table becomes unavailable, we would also want to
disable flush and cleanup operations via cluster setting to avoid cluster resources
being unnecessarily spent on operations that are doomed to fail.

## Drawbacks

* In order to retrieve the most up-to-date statistics that are yet to be
  flushed to system table, we would fall back to using RPC fanout to contact
  every single node in the cluster. This might not scale well in a very large
  cluster. This can be potentially addressed via reducing the flush interval.
  However, this comes at the cost of higher IO overhead to the cluster.

## Rationale and Alternatives

* Instead of deleting the oldest stats entries from the system table in the
  clean up process, we can alternatively delete all stats in the oldest
  aggregation interval. This is because for any given transaction
  fingerprint in an aggregation interval, all the statement fingerprints that
  such transaction references to, must also be present in the statement table
  within the same aggregation interval. (Note: I think this can be formally
  proven)So if we instead delete all the stats belonging to the oldest
  aggregation interval, we can ensure that all the statement fingerprints
  referenced by transactions are valid in the statement table.

## Future Work

* We want to account in-memory structure size using a memory monitor. This is
  to avoid OOM when there are a lot of distinct fingerprint stored in memory.
  This also allows us to flush the stats into system table in time before
  the memory limit has reached.

* Instead of aggregating statistics in-memory at the gateway node, or writing
  complex CTE queries, we can create specialized DistSQL operators to perform
  aggregation on `NumericStats` type.

* We want to have the ability to throttle ourselves during the cleanup job if the
  cluster load in order not to overload the cluster resources.

* We want to have a circuit breaker in place for flush/cleanup operations. If too
  many errors occur, we want to take a break and degrade our service quality
  gracefully without overwhelming the system.

* We want to have a better policy to retain more statistically significant
  entries in our in-memory store when we are forced to discard statistics
  during one of our failure scenarios. The better approach is as follow:
  Maintain a `sampling_factor` which is initially 1. Instead of recording all
  query executions, randomly sample them with probability `1/sampling_factor`
  (so initially we will still be recording everything). When memory fills up,
  find the fingerprint with the smallest count. Multiply the `sampling_factor`
  by some number greater than that smallest count (such as the next power of
  two). Loop over all your in-memory data, dividing all counts by this number.
  Discard any fingerprints whose count is reduced to zero. When we flush the
  data to storage, multiply all counts by the current sampling factor. Once
  flush succeeds, we can discard all data and reset the sampling factor to 1.
  See [link](https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average)
  for more details.
