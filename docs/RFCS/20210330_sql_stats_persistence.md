- Feature Name: SQL Statistics Persistence
- Status: draft
- Start Date: 2021-03-30
- Authors: Archer Zhang
- RFC PR: TODO(@azhng)
- Cockroach Issue: [#56219](https://github.com/cockroachdb/cockroach/issues/56219)

# Summary

This RFC describes the motivation and the mechanism for persisting SQL
statistics. By persisting accumulated SQL statistics into a system table, we
can address the issue where currently CRDB loses accumulated statistics upon
restart/upgrade. This feature would also enable users of CRDB to examine and
compare the historical statistics of statements and transactions overtime. As
the result, CRDB will gain ability that helps users to easily identify
historical transactions and statements that consumes a disproportionate amount
of cluster resource, even after node crashes and restarts.

# Motivation

Currently, CRDB stores the statement and transaction metrics in memory. The
retention policy for the in-memory storage is one hour be default. During this
one-hour period, users can query the this in-memory data structure through DB
console. However, after the retention period for the collected statistics
expires, users will no longer to be able to access these statistics. There are
few significant problems with the current setup:

1. Users have limited data to investigate and debug issues that happened in the
   past.
1. Since statement and transaction statistics are stored in memory, to
   aggregate statistics for the entire cluster, the CRDB node that is handling
   the RPC request (the gateway node) must fanout RPC calls to every single
   node in the cluster.
   1. Due to this design, if a node becomes unavailable, CRDB will be no longer
      able to provide accurate accouting for statement/transaction statistics.
      This can potentially impact the usefulness of having SQL statistics shown
      in the DB console, as the unavailability of the node can be potentially
      the result of resource-hungry queries that were being executed on that
      particular node.
   1. Due to the reliance on the RPC-fanout, post-processings of the SQL
      statistics (e.g. sorting, filtering) are currently implemented within DB
      console. As we move toward storing and displaying historical statistics,
      solely relying on DB console to perform slicing-n-dicing of the
      statistics data is not scalable.
1. As CRDB moving toward multi-tenant architecture, relying on fanout RPC
   implies that tenant statistics aggregation will not only have the drawbacks
   mentioned previously, but also be depending on the progress of pod-to-pod
   communication implementation, which is not ideal.

The persistence of SQL statistics in a CRDB system table can addresses existing
drawbacks. CRDB will gain improvement in two areas:
1. **Usability**: with DistSQL we will be able to process more complex queries
  to answer the questions users might have for the performance of their queries
  overtime.
1. **Reliability**: with CRDB SQL statistics now backed by a persistent table,
  we will ensure the survival of the data across node crash/upgrade/restarts.

# Design

## Design Considerations

* Collected SQL statistics need to be available on every node that receives SQL
  queries and the accumulated statistics need to survive node restart/crash.

* Collected statistics should be able to answer users' potential questions for
  their queries overtime through both DB Console and SQL shell.

* Statistics persistence should be low overhead, but the collected statistics
  should also enough resolution to provide meaningful insight into the
  query/txn performance.

* There is a need for mechanism to prune old statistics data to reduce the
  burden on storage space.

* Statistics collection and statistics persistence should be decoupled.

## Design Overview

Two new system tables `system.experimetnal_sql_stmt_stats` and
`system.experimental_sql_txn_stats` provide storage for storing time series
data for accumulated statistics for statements and transactions.

Currently, each CRDB node stores in-memory statistics for transactions and
statements in which the node is the gateway for. The in-memory statistics is
flushed into system tables in one of the following scenarios:
1. at the end of a fixed interval determined by a cluster setting. E.g. every 5
   mins.
1. when user explicitly requests all in-memory statistics to be persisted. E.g.
   through a SQL shell builtin or through DB Console.

During the flush operation, for each statement/transaction fingerprint, the
CRDB node will check if there already exists same fingerprint in the persisted
system tables within the latest time bucket.
* if such entry exists, the flush operation will aggregate the existing entry.
* if such extry does not exist, the flush operation will insert a new entry.

When DB Console issues fetch requests to CRDB node through HTTP endpoint, the
persisted statistics data can be fetched using `AS OF SYSTEM TIME -10s`
queries in order to minimize read-write contention. However, for the most
up-to-date statistics, there are two possible optiuons:

1. we would still be relying on RPC fanout to retrieve the in-memory statistics
   from each node. The pros for this options is that this is already what CRDB
   does today, and we already have mechanism setup for this. Consequentially,
   this means that this option also inherit the disadvantage of the existing
   designs, such as data-loss on crashes, inaccurate stats if nodes becomes
   available etc.
1. we can give DB Console ability to adjust flush interval dynamically.
   E.g. when user login to DB Console, DB Console issues RPC request to the DB
    to reduce the flush interval to every 5 seconds. This flush interval
    "boost" will automatically expires after `X_number` of  flushes and the
    flush interval will goes back to normal. DB Console can periodically
    sends "boost" request to the CRDB node to maintain the boost. This
    means that when user closes DB Console, the user doesn't have to do
    anything and the stats flush interval will automatically goes back to
    normal shortly.
   This options addresses the disadvantage of the existing design. However, it
   also introduces new challenges. When each nodes are flushing stats to the
   system table, it will be done within the same transaction. Since during the
   insertion, we performs `read`/`aggregate`/`delete`/`write` SQL operations, 
   it is possible we will run into transaction conflicts and force the
   transaction to restart, hence causing contention in the system. This problem
   can become troublesome in a cluster with large number of nodes.

   (Random thoughts: is it possible we can use job system for this?)

## Design Details

### System table schema

``` SQL
CREATE TABLE system.sql_stmt_stats (
    -- primary key
    fingerprint INT NOT NULL,
    timestamp   TIMESTAMP NOT NULL,

    -- metadata
    app_name     STRING NOT NULL,
    sql_type     STRING NOT NULL,
    query        STRING NOT NULL,
    distsql      BOOL NOT NULL,
    failed       BOOL NOT NULL,
    opt          BOOL NOT NULL,
    implicit_txn BOOL NOT NULL,
    vec          BOOL NOT NULL,
    full_scan    BOOL NOT NULL,

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

    PRIMARY KEY (fingerprint, timestamp)
);

CREATE TABLE system.sql_txn_stats (
    -- primary key
    fingerprint    INT NOT NULL,
    timestamp      TIMESTAMP NOT NULL,

    -- metadata
    app_name       STRING NOT NULL,
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

    PRIMARY KEY (fingerprint, timestamp);
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

The table's primary key is composed of `(stmtID, time)`. This is to avoid
having all nodes writing stats to the same range at the same time, which would
result in write-write contentions and range hotspot.

The metadata fields record high-level information about the queries with given
statement fingerprint.

Statement statistics columns contains statistics for queries with given
statement fingerprint. Each attribute is stored with two columns, one for the
mean of the value of that attribute, and the other one for squared difference.
Similar to statement statistics columns, execution statistics columns are
formatted in similar fasion.

### Example queries that can used to answer query performance related questions:

#### Querying attributes over a time period for a statement.

``` SQL
SELECT 
  time,
  stmtID,
  count,
  retries,
FROM system.experimental_sql_stmt_stats
     AS OF SYSTEM TIME '-10s'
WHERE stmtID = $1
  AND time < $2
  AND time > $3
ORDER BY
  time;
```

#### Query execplan over a time period for a statement.

``` SQL
SELECT
  stmtID,
  plan
FROM system.experimental_sql_stmt_stats
     AS OF SYSTEM TIME '-10s'
WHERE stmtID = $1
  AND time < $2
  AND time > $3
ORDER BY
  time;
```

#### Show top offending statements by attribute for a given period of time.

``` SQL
SELECT
  stmtID,
  avg(service_lat) as avg_service_latency,
  max(service_lat) as max_service_latency,
  avg(overhead_lat) as avg_overhead_latency,
  max(overhead_lat) as max_overhead_latency
FROM system.experimental_sql_stmt_stats
     AS OF SYSTEM TIME '-10s'
WHERE  time < $1
  AND time > $2
GROUP BY stmtID
ORDER BY
  (avg_service_latency, avg_overhead_latency) DESC;
```

### Writing in-memory stats to system tables

When we flush in-memory stats to a system table, the operation consists of
a single transaction that contains four stages:
1. Fetch statistics from the system table that were newly inserted within the
   current aggregation window.
1. Combining statistics in-memory with statistics fetched from the system
   table. This step is done in-memory because of the custom logic we have
   to combine two `NumericStats`. (Random thoughts: is it worth it to extend
   the SQL Engine to work with NumericStats?)
1. Delete the stats we fetched from the system table in stage 1 from the system
   table.
1. Insert the new stats we created in stage 2 into the system table with the
   current clock timestamp.

#### Stage 1: Fetch

To fetch both statement and transaction stats from the system table within the
current aggregation bucket (e.g. 5 minutes), we can use the folloing query:

``` sql
SELECT fingerprint, stats
FROM system.experimental_sql_stmt_stats
WHERE timestamp > (current_timestamp() - MOD(EXTRACT(EPOCH FROM current_timestamp())::INT, 5 * 60)::INTERVAL)

SELECT fingerprint, stats
FROM system.experimental_sql_txn_stats
WHERE timestamp > (current_timestamp() - MOD(EXTRACT(EPOCH FROM current_timestamp())::INT, 5 * 60)::INTERVAL)
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
DELETE FROM system.experimental_sql_stmt_stats
WHERE timestamp > (current_timestamp() - MOD(EXTRACT(EPOCH FROM current_timestamp())::INT, 5 * 60)::INTERVAL
```

#### Stage 4: Insert

Finally, we will insert the up-to-date statistics back into the system table
using the current clock timestamp:

``` sql
INSERT INTO system.experimental_sql_stmt_stats (...fields...)
VALUES ($1, clock_timestamp(), $2, $3, ...)
```

### Clean up old/stale statistics data

We can impose an upper limit on the number of rows that we store in the
system table. Then at the end of the flush operation, we can simply check if
the total number of rows in the system table exceeds the upper limit. If so,
the operation can remove the oldest entries from the table.

## Drawbacks

...

## Rationale and Alternatives

...

# Unresolved questions

* Should we still store the entire protobuf of the
  `CollectedStatementStatistics` and `CollectedTransactionStatistics` in the
  system table? It makes easy for the status server and stage 1 of the flush
  operatoon to quickly fetch data from the table without serialization. However
  it comes at the cost of storing duplicated data.

* How should we address the issue of getting the near-up-to-date data for
  db console. We can choose from the two alternatives presented in the earlier
  section of this RFC, or maybe perhaps some other options.

* Should we store some sort of query plan hash so we can track how the query
  plan has changed for the same query fingerprint?
