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
a result, CockroachDB will gain the ability to helps users to easily identify
historical transactions and statements that consume a disproportionate amount
of cluster resources, even after node crashes and restarts.

# Motivation

Currently, CockroachDB stores the statement and transaction metrics in memory. The
retention policy for the in-memory storage is one hour by default. During this
one-hour period, users can query this in-memory data structure through DB
console. However, after the retention period for the collected statistics
expires, users are no longer to be able to access these statistics. There are
few significant problems with the current setup:

1. Users have limited data to investigate and debug issues that happened in the
   past.
1. Since statement and transaction statistics are stored in memory, to
   aggregate statistics for the entire cluster, the CockroachDB node that is handling
   the RPC request (the gateway node) must fanout RPC calls to every single
   node in the cluster.
   1. Due to this design, if a node becomes unavailable, CockroachDB will be no longer
      able to provide accurate accounting for statement/transaction statistics.
      This can potentially impact the usefulness of having SQL statistics shown
      in the DB console, as the unavailability of the node can be potentially
      the result of resource-hungry queries that were being executed on that
      particular node.
   1. Due to the reliance on the RPC-fanout, post-processing of the SQL
      statistics (e.g. sorting, filtering) are currently implemented within DB
      console. As we move toward storing and displaying historical statistics,
      solely relying on DB console to perform slicing-n-dicing of the
      statistics data is not scalable.
1. As CockroachDB moving toward multi-tenant architecture, relying on fanout RPC
   implies that tenant statistics aggregation will not only have the drawbacks
   mentioned previously, but also be depending on the progress of pod-to-pod
   communication implementation, which is not ideal.

The persistence of SQL statistics in a CockroachDB system table can addresses existing
drawbacks. CockroachDB will gain improvement in two areas:
1. **Usability**: with DistSQL we will be able to process more complex queries
  to answer the questions users might have for the performance of their queries
  overtime.
1. **Reliability**: with CockroachDB SQL statistics now backed by a persistent table,
  we will ensure the survival of the data across node crash/upgrade/restarts.

# Design

## Design Considerations

* Collected SQL statistics need to be available on every node that receives SQL
  queries and the accumulated statistics need to survive node restart/crash.

* Collected statistics should be able to answer users' potential questions for
  their queries overtime through both DB Console and SQL shell.

* Statistics persistence should be low overhead, but the collected statistics
  should also have enough resolution to provide meaningful insight into the
  query/txn performance.

* There is a need for mechanism to prune old statistics data to reduce the
  burden on storage space. The setting for pruning mechanism should also be
  accessible to users so that it can be changed to suit different needs.

* Statistics collection and statistics persistence should be decoupled.

## Design Overview

Two new system tables `system.experimetnal_sql_stmt_stats` and
`system.sql_txn_stats` provide storage for storing time series
data for accumulated statistics for statements and transactions.

Currently, each CockroachDB node stores in-memory statistics for transactions and
statements in which the node is the gateway for. The in-memory statistics is
flushed into system tables in one of the following scenarios:
1. at the end of a fixed interval determined by a cluster setting. E.g. every 5
   mins.
1. when user explicitly requests all in-memory statistics to be persisted. E.g.
   through a SQL shell builtin or through DB Console.

During the flush operation, for each statement/transaction fingerprint, the
CockroachDB node will check if there already exists same fingerprint in the persisted
system tables within the latest time bucket.
* if such entry exists, the flush operation will aggregate the existing entry.
* if such entry does not exist, the flush operation will insert a new entry.

The flush operation will also be triggered upon node shutdown.

When DB Console issues fetch requests to CockroachDB node through HTTP endpoint, the
persisted statistics data can be fetched using `AS OF SYSTEM TIME -10s`
queries in order to minimize read-write contention. However, for the most
up-to-date statistics, we still need to utilize RPC fanout to retrieve the
in-memory statistics from each node. The pros for this options is that this is
already what CockroachDB does today, and we already have mechanism setup for
this. Consequentially, this means that this option also inherit the
disadvantage of the existing designs, such as data-loss on crashes, inaccurate
stats if nodes becomes unavailable etc.

## Design Details

### System table schema

``` SQL
CREATE TABLE system.sql_stmt_stats (
    -- primary key
    app_name    STRING NOT NULL,
    fingerprint INT NOT NULL,
    created_at  TIMESTAMP NOT NULL,

    -- metadata
    sql_type     STRING NOT NULL,
    query        STRING NOT NULL,
    distsql      BOOL NOT NULL,
    failed       BOOL NOT NULL,
    opt          BOOL NOT NULL,
    implicit_txn BOOL NOT NULL,
    vec          BOOL NOT NULL,
    full_scan    BOOL NOT NULL,
    first_run_at TIMESTAMP NOT NULL,
    last_run_at  TIMESTAMP NOT NULL,

    -- stats
    count               INT8 NOT NULL,
    first_attempt_count INT8 NOT NULL,
    max_retries         INT8 NOT NULL,
    num_rows            FLOAT8 NOT NULL,
    num_rows_sd         FLOAT8 NOT NULL,
    parse_lat           FLOAT8 NOT NULL,
    parse_lat_sd        FLOAT8 NOT NULL,
    plan_lat            FLOAT8 NOT NULL,
    plan_lat_sd         FLOAT8 NOT NULL,
    run_lat             FLOAT8 NOT NULL,
    run_lat_sd          FLOAT8 NOT NULL,
    service_lat         FLOAT8 NOT NULL,
    service_lat_sd      FLOAT8 NOT NULL,
    overhead_lat        FLOAT8 NOT NULL,
    overhead_lat_sd     FLOAT8 NOT NULL,
    bytes_read          FLOAT8 NOT NULL,
    bytes_read_sd       FLOAT8 NOT NULL,
    rows_read           FLOAT8 NOT NULL,
    rows_read_sd        FLOAT8 NOT NULL,

    -- exec stats
    exec_count               INT8 NOT NULL,
    exec_network_bytes       FLOAT8 NOT NULL,
    exec_network_bytes_sd    FLOAT8 NOT NULL,
    exec_max_mem_usage       FLOAT8 NOT NULL,
    exec_max_mem_usage_sd    FLOAT8 NOT NULL,
    exec_contention_time     FLOAT8 NOT NULL,
    exec_contention_time_sd  FLOAT8 NOT NULL,
    exec_network_messages    FLOAT8 NOT NULL,
    exec_network_messages_sd FLOAT8 NOT NULL,
    exec_max_disk_usage      FLOAT8 NOT NULL,
    exec_max_disk_usage_sd   FLOAT8 NOT NULL,

    -- protobuf
    stats               BYTES NOT NULL,

    PRIMARY KEY (app_name, fingerprint, created_at)
);

CREATE TABLE system.sql_txn_stats (
    -- primary key
    app_name       STRING NOT NULL,
    fingerprint    INT NOT NULL,
    created_at     TIMESTAMP NOT NULL,

    -- metadata
    statement_ids  INT[] NOT NULL,
    count          INT8 NOT NULL,
    max_retries    INT8 NOT NULL,
    num_rows       FLOAT8 NOT NULL,
    num_rows_sd    FLOAT8 NOT NULL,
    service_lat    FLOAT8 NOT NULL,
    service_lat_sd FLOAT8 NOT NULL,
    retry_lat      FLOAT8 NOT NULL,
    retry_lat_sd   FLOAT8 NOT NULL,
    commit_lat     FLOAT8 NOT NULL,
    commit_lat_sd  FLOAT8 NOT NULL,
    bytes_read     FLOAT8 NOT NULL,
    bytes_read_sd  FLOAT8 NOT NULL,
    rows_read      FLOAT8 NOT NULL,
    rows_read_sd   FLOAT8 NOT NULL,
    first_run_at   TIMESTAMP NOT NULL,
    last_run_at    TIMESTAMP NOT NULL,

    -- exec stats
    exec_count               INT8 NOT NULL,
    exec_network_bytes       FLOAT8 NOT NULL,
    exec_network_bytes_sd    FLOAT8 NOT NULL,
    exec_max_mem_usage       FLOAT8 NOT NULL,
    exec_max_mem_usage_sd    FLOAT8 NOT NULL,
    exec_contention_time     FLOAT8 NOT NULL,
    exec_contention_time_sd  FLOAT8 NOT NULL,
    exec_network_messages    FLOAT8 NOT NULL,
    exec_network_messages_sd FLOAT8 NOT NULL,
    exec_max_disk_usage      FLOAT8 NOT NULL,
    exec_max_disk_usage_sd   FLOAT8 NOT NULL,

    -- protobuf
    stats BYTES NOT NULL,

    PRIMARY KEY (app_name, fingerprint, created_at);
);
```

Query plan is stored as part of the raw protobuf bytes.

``` protobuf
message ExplainTreePlanNode {
  option (gogoproto.equal) = true;
  // Name is the type of node this is, e.g. "scan" or "index-join".
  optional string name = 1 [(gogoproto.nullable) = false];

  message Attr {
    option (gogoproto.equal) = true;
    optional string key = 1 [(gogoproto.nullable) = false];
    optional string value = 2 [(gogoproto.nullable) = false];
  }

  // Attrs are attributes of this plan node.
  // Often there are many attributes with the same key, e.g. "render".
  repeated Attr attrs = 2;

  // Children are the nodes that feed into this one, e.g. two scans for a join.
  repeated ExplainTreePlanNode children = 3;
}
```

The table's primary key is composed of `(app_name, fingerprint, created_at)`.
This is to avoid having all nodes writing stats to the same range at the same
time, which would result in write-write contentions and range hotspot.

The metadata fields record high-level information about the queries with given
statement fingerprint.

Statement statistics columns contain statistics for queries with given
statement fingerprint. Each attribute is stored with two columns, one for the
mean of the value of that attribute and the other one for the squared difference.
Similar to statement statistics columns, execution statistics columns are
formatted in a similar fashion.

### Example queries that can be used to answer query performance related questions:

#### Querying attributes over a time period for a statement.

``` SQL
SELECT 
  created_at,
  fingerprint,
  count,
  retries,
FROM system.sql_stmt_stats
     AS OF SYSTEM TIME '-10s'
WHERE fingerprint = $1
  AND created_at < $2
  AND created_at > $3
ORDER BY
  time;
```

#### Query execplan over a time period for a statement.

``` SQL
SELECT
  fingerprint,
  plan
FROM system.sql_stmt_stats
     AS OF SYSTEM TIME '-10s'
WHERE fingerprint = $1
  AND created_at < $2
  AND created_at > $3
ORDER BY
  time;
```

#### Show top offending statements by attribute for a given period of time.

``` SQL
SELECT
  fingerprint,
  avg(service_lat) as avg_service_latency,
  max(service_lat) as max_service_latency,
  avg(overhead_lat) as avg_overhead_latency,
  max(overhead_lat) as max_overhead_latency
FROM system.sql_stmt_stats
     AS OF SYSTEM TIME '-10s'
WHERE  created_at < $1
  AND created_at > $2
GROUP BY fingerprint
ORDER BY
  (avg_service_latency, avg_overhead_latency) DESC;
```

### Writing in-memory stats to system tables
When we flush in-memory stats to a system table, the operation consists of
a single transaction that contains five stages:
1. Fetch statistics from the system table that were newly inserted within the
   current aggregation window.
1. Combining statistics in-memory with statistics fetched from the system
   table. This step is done in-memory because of the custom logic we have
   to combine two `NumericStats`. (Random thoughts: is it worth it to extend
   the SQL Engine to work with NumericStats?)
1. Delete the stats we fetched from the system table in stage 1 from the system
   table.
1. Insert the new stats we created in stage 2 into the system table with the
   current transaction timestamp.
1. Check if number of rows in the persisted table has exceeded maximum limit.
   If the limit has been exceeded, then we would need to delete old stats
   entries.

#### Stage 1: Fetch

To fetch both statement and transaction stats from the system table within the
current aggregation bucket (e.g. 5 minutes), we can use the following query:

``` sql
SELECT fingerprint, stats
FROM system.sql_stmt_stats
WHERE created_at > (current_timestamp() - MOD(EXTRACT(EPOCH FROM current_timestamp())::INT, 5 * 60)::INTERVAL)

SELECT fingerprint, stats
FROM system.sql_txn_stats
WHERE created_at > (current_timestamp() - MOD(EXTRACT(EPOCH FROM current_timestamp())::INT, 5 * 60)::INTERVAL)
```

We can then use the fetched statistics to build
[`sqlStats`](https://github.com/cockroachdb/cockroach/blob/125f6e6be9bf23bc4c69aff6abb02916a3fbd20d/pkg/sql/app_stats.go#L506) in-memory data structure.

#### Stage 2: Combine

Now that we have an in-memory representation of all the statistics from the
current aggregation interval, we can use the `appStats::Add` methods to combine
the fetched `sqlStats` with the `sqlStats` stored in the `sql/Server`.

#### Stage 3: Delete

Before we insert the newly combined statistics back into the system table,
we first need to remove the existing entries to avoid duplication. This can be
done using the following statement:

``` sql
DELETE FROM system.sql_stmt_stats
WHERE created_at > (current_timestamp() - MOD(EXTRACT(EPOCH FROM current_timestamp())::INT, 5 * 60)::INTERVAL
```

#### Stage 4: Insert

Now, we will insert the up-to-date statistics back into the system table
using the current transaction timestamp:

``` sql
INSERT INTO system.sql_stmt_stats (...fields...)
VALUES ($1, current_timestamp(), $2, $3, ...)
```

#### Stage 5: Ensure we don't exceed maximum number of stats limit

Finally, we will check that if we need to prune any of the old statistics
from the table.

We can retrieve the number of the rows in the table using a simple query:

``` sql
SELECT COUNT(*) FROM system.sql_stmt_stats;
```

Then if this value exceeds the maximum limit, we can delete the oldest entries
using the following query:

``` sql
DELETE FROM system.sql_stmt_stats
WHERE (app_name, fingerprint, created_at) IN
  (SELECT app_name, fingerprint, created_at
   FROM system.sql_stmt_stats LIMIT $1
   ORDER BY created_at)
```

where the placeholder is the number of the rows we want to delete.


## Drawbacks

* In order to retrieve the most up-to-date statistics that are yet to be
  flushed to system table, we would be fall back to using RPC fanout to contact
  every single node in the cluster. This might not scale well in a very large
  cluster.

* Currently, this schema does not enforce foreign key constraints between
  the transaction statistics and statement statistics. This means that it is
  possible for an transaction stats entry to reference a statement stats entry
  that has been pruned.

* Currently, we perform aggregation in-memory because of the custom logic in
  combining `NumericStats`. If the aggregation window becomes too big, we are
  risking running out of memory on the gateway node that's performing the
  aggregation.

## Rationale and Alternatives

* Instead of deleting the oldest stats entries from the system table in the
  stage 5 of the flush operation, we can alternatively delete all stats in the
  oldest aggregation window. This is because for any given transaction
  fingerprint in an aggregation window, all the statement fingerprints that
  such transaction references to, must also be present in the statement table
  within the same aggregation window. (Note: I think this can be formally
  proven)So if we instead delete all the stats stats belonging to the oldest
  aggregation window, we can ensure that all the statement fingerprints
  referenced by transactions are valid in the statement table.

* Since stats entries are not mission critical data for the operation of the
  database, we can perhaps tolerate certain degree of inconsistency of data.
  We can handle the inconsistency by showing an error message to user
  explaining that the stats entry they are looking for is too old and has been
  pruned from the storage.

# Unresolved questions

* Should we still store the entire protobuf of the
  `CollectedStatementStatistics` and `CollectedTransactionStatistics` in the
  system table? It makes it easy for the status server and stage 1 of the flush
  operation to quickly fetch data from the table without serialization.
  However, it comes at the cost of storing duplicated data.

* Should we store some sort of query plan hash so we can track how the query
  plan has changed for the same query fingerprint?

* Should we have additional limit other than number of rows for the persisted
  tables? Currently we do not perform memory accounting for the stats stored
  in memory. It is possible for it to cause OOM if the flush interval is
  too long.

## Future Work

* We want to account in-memory structure size using a memory monitor. This is
  to avoid OOM when there are a lot of distinct fingerprint stored in memory.
  This also allows us to flush the stats into system table in time before
  the memory limit has reached.

* Instead of aggregating statistics in-memory at the gateway node, we can
  create specialized DistSQL operators to perform aggregation on `NumericStats`
  type. This will remove the burden on the gateway node to perform all
  aggregation in-memory.
