- Feature Name: Query cancellation
- Status: draft
- Start Date: 2017-06-05
- Authors: Bilal Akhtar
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [#15593](https://github.com/cockroachdb/cockroach/issues/15593)

# Summary

This feature adds new query identifiers to the output of `SHOW QUERIES`, and also
adds a new statement `CANCEL QUERY <query_id>` which cancels that running query in the
cluster if it exists.

# Motivation

Being able to cancel long-running or resource-intensive queries would be a useful
ability to free up server resources. If a user mistakenly runs a long-running query
currently that they do not wish to run, they can close and re-open the connection
and resume running more queries; however, the original query will keep running and using
up resources till termination.

Currently, a DBA can see what queries are running and for how long, using `SHOW QUERIES`:

```sql
root@:26257/> SHOW CLUSTER QUERIES;
+---------+----------+----------------------------------+----------------------------------------------------------------------------------------------------------+-----------------+------------------+-------------+-----------+
| node_id | username |              start               |                                                  query                                                   | client_address  | application_name | distributed |   phase   |
+---------+----------+----------------------------------+----------------------------------------------------------------------------------------------------------+-----------------+------------------+-------------+-----------+
|       1 | root     | 2017-06-07 16:14:37.38177+00:00  | SHOW CLUSTER QUERIES                                                                                     | [::1]:54421     |                  | NULL        | preparing |
|       2 | root     | 2017-06-07 16:14:37.380942+00:00 | UPDATE accounts SET balance = CASE id WHEN $1 THEN $3::INT WHEN $2 THEN $4::INT END WHERE id IN ($1, $2) | 127.0.0.1:54887 |                  | false       | executing |
|       2 | root     | 2017-06-07 16:14:37.382055+00:00 | UPDATE accounts SET balance = CASE id WHEN $1 THEN $3::INT WHEN $2 THEN $4::INT END WHERE id IN ($1, $2) | 127.0.0.1:54936 |                  | false       | executing |
|       2 | root     | 2017-06-07 16:14:37.380492+00:00 | UPDATE accounts SET balance = CASE id WHEN $1 THEN $3::INT WHEN $2 THEN $4::INT END WHERE id IN ($1, $2) | 127.0.0.1:54920 |                  | false       | executing |
|       2 | root     | 2017-06-07 16:14:37.381263+00:00 | UPDATE accounts SET balance = CASE id WHEN $1 THEN $3::INT WHEN $2 THEN $4::INT END WHERE id IN ($1, $2) | 127.0.0.1:54912 |                  | false       | executing |
|       2 | root     | 2017-06-07 16:14:37.3805+00:00   | UPDATE accounts SET balance = CASE id WHEN $1 THEN $3::INT WHEN $2 THEN $4::INT END WHERE id IN ($1, $2) | 127.0.0.1:54928 |                  | false       | executing |
+---------+----------+----------------------------------+----------------------------------------------------------------------------------------------------------+-----------------+------------------+-------------+-----------+
(6 rows)
```

# Detailed design

## Query identifiers

Query identifiers are intended to provide a unique way to identify a query across
the cluster, preferably with an easy way to recover the node ID from the query ID without issuing
requests to other nodes. They will be used primarily in:

- `SHOW QUERIES`, presented under the `id` column.
- `CANCEL QUERY <query-id>` statements as the only argument.

Query identifiers could also be added to other parts of the code, such as to log tags, however
those changes will be out of scope for this project.

Query identifiers will be of the form `<node_id>:<unique_rowid>` where the `<node_id>` portion
is the ID of the gateway node, and the `<unique_rowid>` portion is the output of `unique_rowid()`.
Although the output of `unique_rowid()` has the `node_id` encoded in the lowest 15 bits, having it
repeated as a separate `int64` addresses the case where the node ID exceeds 32k. 

Query IDs will be presented in the output of `SHOW QUERIES` as a STRING field.

Example output of `SHOW QUERIES` with query ID (see the `id` column):

```sql
root@:26257/> SELECT node_id, id, query FROM [SHOW CLUSTER QUERIES];
+---------+----------------------+--------------------------------------------------------------------+
| node_id | id                   | query                                                              | 
+---------+----------------------+--------------------------------------------------------------------+
| 1       | 1:253065114725613569 | SHOW CLUSTER QUERIES                                               |
| 2       | 2:253065241735528449 | INSERT INTO Students VALUES ('Robert'); DROP TABLE Students; --')  |
+---------+----------------------+--------------------------------------------------------------------+
(2 rows)
```

## Context forking / cancellation scope

The `context.Context` will be forked in the SQL executor for each statement, and the cancellation handle
will be stored inside the `queryMeta` struct for that query. There will also be an integer flag that,
when set to 1 (using an `atomic.StoreInt32()`), will signal cancellation. PlanNodes that do in-memory
processing such as insertNode's insert batching, and sortNode's sorting, will periodically check
for this flag (such as once every some tens of thousands of rows).

Any `CANCEL QUERY` statements directed at that query will close that context's `Done` channel, which would
error out any RPCs being made for that query. The cancellation flag would also be atomically set to 1,
which would cause any planNodes that check it periodically to return a cancellation error. The error would
propagate to the SQL executor which would then rollback the current KV transaction, and mark the SQL
transaction as aborted. The client's connection will _not_ be closed.

In short, cancellation will be implemented at the statement level, with cancelled statements also rolling back
the associated transaction. The executor would re-throw a more user-friendly "Execution cancelled upon user
request" error instead of the "Context cancelled" one.

Both context cancellation and a shared flag is being used, because:

- Context cancellation is already built into gRPC and works well for cleaning up work on other nodes.
- Checking a shared integer atomically is significantly faster than checking a context's Done channel,
especially when that context is the result of several derivations.

For minimal impact on performance, the integer is better for frequent cancellation checks, and a select
on `<- ctx.Done()` is better for parts of the code that already wait on it (or wait for other channels).

## New CANCEL QUERY statement

The syntax of the actual `CANCEL` statement has already been approved in
[this RFC](https://github.com/cockroachdb/cockroach/pull/16273), and will be of the form
`CANCEL [QUERY|JOB] <query_id>`.

A non-root database user can only issue `CANCEL QUERY` statements to queries run by that same
user. The root user can cancel any queries on the cluster.

When this statement is executed, the query ID will be parsed to determine the node ID, and an RPC call
will be made to that node if it is not the current node. That node would look up and cancel the
context related to that query.

Example use case: Assume there are two concurrently running sessions, session 1 and 2.

Session 1 (long running query):

```sql
root@:26257/> SELECT * FROM students WHERE long_bio LIKE '%ips%or';
```

Session 2 (DB admin wishing to cancel the query above):

```sql
root@:26257/> SELECT node_id, id, query FROM [SHOW CLUSTER QUERIES];
+---------+-----------------------+-----------------------------------------------------------+
| node_id | id                    | query                                                     | 
+---------+-----------------------+-----------------------------------------------------------+
| 1       | 1:253065114725613569  | SHOW CLUSTER QUERIES                                      |
| 2       | 2:253065114725614235  | SELECT * FROM students WHERE long_bio LIKE '%ips%or';     |
+---------+-----------------------+-----------------------------------------------------------+
(2 rows)

root@:26257/> CANCEL QUERY '2:253065114725614235';
CANCEL QUERY

root@:26257/> SELECT node_id, id, query FROM [SHOW CLUSTER QUERIES];
+---------+-----------------------+-----------------------------------------------------------+
| node_id | id                    | query                                                     |
+---------+-----------------------+-----------------------------------------------------------+
| 1       | 1:253065114725613569  | SHOW CLUSTER QUERIES                                      |
+---------+-----------------------+-----------------------------------------------------------+
(1 row)
```

Going back to session 1:

```sql
root@:26257/> SELECT * FROM students WHERE long_bio LIKE '%ips%or';
pq: query cancelled by user 'root'

root%:26257/>
```

## Uncancellable / commit phase

There would be an uncancellable phase of queries starting from the point when the `EndTransactionRequest`
for the associated transaction (either an explicit or implicit one) is sent. At this point, a flag in
`queryMeta` will be set to indicate that the query can no longer be cancelled; any new `CANCEL QUERY`
statements directed at it will error out.

For instance, for an `insertNode`, the `EndTransactionRequest` is sent when `tableWriter.finalize()`
is called after all rows to write have been staged. At that point, the `INSERT` is uncancellable.

## DistSQL cancellation

RPC requests made to other nodes as part of distSQL nodes will be cancelled from the perspective
of the node making the calls, when the associated context is cancelled. The associated context on
the server node will also be cancelled; however, distSQL code rarely checks for the closure
of its context's `.Done()` channel. A cancelled distSQL query can leak goroutines on other
nodes until more context closure checks are added and/or another RPC endpoint to flip another cancelled
boolean flag on that node (similar to how the gateway node will signal cancellation to its planNodes).

The first implementation of this project will not involve adding any distSQL-specific cancellation
checks. This RFC will be updated after the first implementation with a more detailed plan on how
distSQL processors will check for, and respond to, cancellation, in the second iteration.

# Drawbacks

# Alternatives

## Session cancellation

Cancellation could also be supported at the session level. A `CANCEL SESSION` call would
cancel any queries in flight under that session (if any), and close the client connection
from the server side. The use-case of session cancellation would differ from that of
query cancellation; for instance, it could be used to immediately stop a rogue user or
attacker from accessing the database, while query cancellation is more useful to free up a
connection to do more things and/or stop an unnecessary query from hogging all server resources.

For now, query cancellation will be prioritized because of its more common use-case.

# Unresolved questions

Any unanswered questions will be added here.
