- Feature Name: Monitoring Queries and Sessions
- Status: completed
- Start Date: 2017-05-04
- Authors: Bilal Akhtar
- RFC PR: https://github.com/cockroachdb/cockroach/pull/15761
- Cockroach Issue: [#7003](https://github.com/cockroachdb/cockroach/issues/7003)

# Summary

This feature would add a mechanism to list currently active sessions and currently executing queries
in the admin UI and sql shell, along with their start timestamps / durations. In this document,
queries are defined as every executable statement; not just `SELECT`s. Everything that is converted
into a `planNode` is a query.

This RFC answers these use-cases:

- Getting a list of node-local and cluster-wide active sessions through the CLI and the admin UI
- Getting a list of node-local and cluster-wide executing queries through the CLI and the admin UI

The following concerns are out of scope for this RFC:

- Assigning unique identifiers to sessions and queries
- Identifying and listing SQL transactions
- Outlining a mechanism for cancelling sessions or queries 
- The return values/errors returned by canceled queries/sessions/closed connections.
- Whether cancellation should be supported at SQL transaction level, or query/statement level.

# Motivation

Currently, there's no visibility into what queries/sessions are running on the cluster at any
given point of time. Adding visibility into this has been a common customer request for
a while.

Albeit cancellation is out of scope for this RFC, being able to view sessions and queries
along with information like application names, duration, etc, would be important information
for DBAs to have, in order to make an appropriate cancellation decision (when that is implemented).

Since some DB deployments can be shared between multiple client applications, some level of
functionality to filter sessions and queries by client application names, both in the admin
UI and in the CLI, would also be useful. And since not all DBAs are familiar with SQL,
a non-SQL interface to monitor queries and sessions would be an asset.

# Detailed design

The inspiration behind some of the design comes from
[this PR from Raphael](https://github.com/cockroachdb/cockroach/pull/10317) which implemented
node-local session registries exposed via a virtual table.

## Thread-safe session struct

The session struct is currently not thread-safe; since all operations on it
currently happen in one thread. Making it thread-safe involves making a nested struct
inside it (called `mu`), similar to how the `Txn` struct is structured. There
would be an `RWMutex` on this nested `mu` struct.

These fields would be moved from the main `Session` struct, to the `mu` sub-struct:
- Application Name
- Eventually: any contexts/object references useful for cancellation

These two new fields will be added to `mu`:
- Active queries (reference to QueryMeta objects defined below, can be nil)

This active query field will be a map of `QueryMeta` pointers (type `map[*queryMeta]struct{}`).
We already have `parallelizeQueue` of a map type with `planNode` as a key, but because of the way
parallelization code is currently written, adding non-parallel queries to `parallelizeQueue`
would be non-trivial. So, a new map containing all queries in flight and their metadata would be
a conceptually simpler solution.

Write locks on this struct will be acquired when:
- The application name is reset (in `resetApplicationName()`)
- A query begins or ends execution (to updated the active query reference / parallelize queue)

Furthermore, the `txnState` struct within `Session` will be updated to have a `mu`
locked struct within it. `txnState.txn` will be moved into it, to become `txnState.mu.txn`,
and updates to that variable in `Executor.execRequest` and reads from `SHOW SESSIONS` will require
acquiring that mutex.

## Node-local session registry

The session registry will be an in-memory set (type `map[*session]struct{}`) that store
references to all session objects on that node. Addition and deletion of new
sessions to this set would require acquiring a mutex lock.

This addition/deletion into the set can be made in `NewSession(...)` and `Session.Finish(...)`
in `pkg/sql/session.go`.

## New QueryMeta struct

A new struct, `QueryMeta`, would be made that contains the below fields:

- Query string (exact query string provided by the user)
- Start timestamp
- A boolean denoting whether the query is distributed.
- A two-state `phase` denoting whether the query is being prepared for execution, or is executing.
- Eventually: any contexts/object references useful for cancellation 

QueryMetas would be stored in a map in `Session.mu`, with the map having type `map[*queryMeta]struct{}`.
The planner struct will also have a reference to the corresponding queryMeta.

## RPC endpoint in status server to return local queries and sessions

A new RPC endpoint would be made: `LocalSessions`. This would return the node's local
list of sessions and any nested lists of queries as necessary. If a username
is provided in the request, only sessions belonging to that user must be returned.

This endpoint would be added to the status RPC server.

## SQL statements to retrieve cluster-wide queries and sessions

For sessions, a new SQL statement will be added to the grammar: `SHOW [LOCAL|CLUSTER] SESSIONS`.
`SHOW LOCAL SESSIONS` will return the gateway node's local list of sessions only, while
`SHOW CLUSTER SESSIONS` will return the entire cluster's list of sessions. Default is CLUSTER
if unspecified.
 
The `CLUSTER` call will involve looping through all nodes and issue RPC calls to `LocalSessions`
with timeouts. The results of all the calls that do succeed, will be returned as a table.
`RaftDebug` in `pkg/server/status.go` is an example of a procedure that does a similar aggregation
of results across all known nodes in the cluster.

Example usage:

```

root@:26257/> SHOW SESSIONS

+---------+----------+------------------+------------------+------------------------------------------------------+---------------+--------------------+-----------+
| node_id | username | client_address   | application_name | active_queries                                       | session_start | oldest_query_start | kv_txn_id |
+---------+----------+------------------+------------------+------------------------------------------------------+---------------+--------------------+-----------+
| 1       |  root    | 192.168.1.5:8080 | test             | SELECT * FROM users; INSERT INTO test VALUES (...);  | 1234567890    | 1234567891         | 1e5f6bac  |
| 2       |  bilal   | 192.168.1.7:8080 | pg.js            |                                                      | 1494524202    |                    |           |
+---------+----------+------------------+------------------+------------------------------------------------------+---------------+--------------------+-----------+
(2 rows)

```

Note that the `active_queries` field is a delimeter-separated list of all queries currently
executing under that session, with SQL parallelization taken into account.

Additions to the SQL grammer will be made to support filtering using SELECT statements and
nesting of SHOWs as tables:

```

root@:26257/> SELECT * FROM (SHOW SESSIONS) AS a WHERE a.user_name = 'bilal';

+---------+-----------+------------------+------------------+------------------------------------------------------+---------------+--------------------+-----------+
| node_id | user_name | client_address   | application_name | active_queries                                       | session_start | oldest_query_start | kv_txn_id |
+---------+-----------+------------------+------------------+------------------------------------------------------+---------------+--------------------+-----------+
| 2       |  bilal    | 192.168.1.7:8080 | pg.js            |                                                      | 1494524202    |                    |           |
+---------+-----------+------------------+------------------+------------------------------------------------------+---------------+--------------------+-----------+
(1 row)

```

Similarly, `SHOW [CLUSTER|LOCAL] QUERIES` will return currently executing queries.

Example usage:

```

root@:26257/> SHOW QUERIES

+---------+----------+------------+--------------------------------+------------------+------------------+------------------+------------+
| node_id | username | start      | query                          | client_address   | application_name | distributed      | phase      |
+---------+----------+------------+--------------------------------+------------------+------------------+------------------+------------+
| 1       |  root    | 1234567894 | SELECT * FROM users;           | 192.168.1.5:8080 | test             | NULL             | preparing  |
| 1       |  root    | 1234567891 | INSERT INTO test VALUES (...); | 192.168.1.5:8080 | test             | true             | executing  |
+---------+----------+------------+--------------------------------+------------------+------------------+------------------+------------+
(1 row)

```

## A note on permissions

Currently, we only let the root user run `SET CLUSTER SETTING`. Following that convention,
`SHOW SESSIONS/QUERIES` will only show the current user's sessions/queries if the user
is not root, or all users if the user is root.

# Drawbacks

## Locking/synchronization

The RWMutex lock inside the Session object could see contention if a `SHOW SESSIONS` or `SHOW QUERIES`
is running in parallel to an operation within the session that updates any of the fields inside
`session.mu`. However there should be little to no performance penalty in the common use-cases of
queries and sessions running without being inspected by a SHOW command.

To ensure that we don't unintentionally cause a performance regression, the included benchmarks
in `pkg/sql/bench_test.go` will be run before and after the change, and the results posted in the PR
for this feature.

## Memory usage on gateway node

In a production-scale cluster, running `SHOW SESSIONS`  will return a large number of rows - which
will all be aggregated on the gateway node in memory before any filtering is done. This can be reduced
by issuing RPCs to other nodes in batches (so the gateway node doesn't get too many results all at
once) , and then sending off the results to the client connection or parent planNode just as a
`LocalSessions` call returns. This way, only a subset of all sessions would be in memory
on the gateway node at once; however, the gateway node won't be able to enforce a default
sort order. This solution will also not make a significant difference if there's a parent
filter planNode (eg. `SELECT * FROM (SHOW SESSIONS) WHERE ...`) - the gateway node
will have to buffer all results in memory anyway.

The memory cost of these SHOW commands will be assessed in a large cluster with generated load
before this feature lands, to gauge the value of implementing streaming.

# Alternatives

## Virtual tables in crdb_internal instead of a SHOW statement

We considered using virtual tables in the `crdb_internal` database to expose sessions and queries,
however that idea was dropped in favour of a SHOW statement for a couple reasons.

The generator functions for virtual tables (which are run every time a query to that table is made),
are not aware of filtering expressions in the query being made; so even a query with a `WHERE nodeid=self`
clause would necessitate aggregating info from the entire cluster. A full refactor of the vtable code was
deemed to be out of scope for this project.

In addition, virtual tables give the user the intuition that the data is persistent and cached/stored
locally, which it isn't. If a node in the cluster dies, the virtual table would have partial info.
A SHOW statement sets a more accurate expectation; that the sessions/queries are fetched upon request.

# Unresolved questions

See the list of unanswered questions that are out of scope for this RFC, in the Summary section above.

Any unanswered questions within this RFC's scope that pop up, will be added here.
