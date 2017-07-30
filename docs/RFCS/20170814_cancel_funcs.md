- Feature Name: Query/Transaction/Session Cancellation
- Status: draft
- Start Date: 2017-08-14
- Author: Julian Gilyadov, Bilal Akhtar
- RFC PR: [#17252](https://github.com/cockroachdb/cockroach/pull/17252) [#16417](https://github.com/cockroachdb/cockroach/pull/16417)
- Cockroach Issue: None

# Summary

This RFC proposes a set of features which adds the ability to cancel an in-progress query, transaction, or session(QTS) using the new `CANCEL` statements.  
The reason for those proposed features is that although it is possible to monitor QTS. There is no way to cancel them.  
Which can be useful in many use cases - for example: cancelling long-running queries hogging up cluster resources or cancelling a session to revoke an unauthorized access to the DB.  
Implementation of those proposed features will be described in-depth below. Generally, new `CANCEL [QUERY|TRANSACTION|SESSION] <id>` statements will be added and new QTS identifiers will be implemented to include the node ID in them. As a result, CockroachDB will know where to route the request internally without issuing additional requests to other nodes.

# Motivation

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

Similarly, `SHOW SESSIONS` lists active sessions along with start timestamps:

```sql
root@:26257/> SHOW SESSIONS;
+---------+----------+----------------+------------------+-------------------------+----------------------------------+----------------------------------+--------+
| node_id | username | client_address | application_name |     active_queries      |          session_start           |        oldest_query_start        | kv_txn |
+---------+----------+----------------+------------------+-------------------------+----------------------------------+----------------------------------+--------+
|       1 | root     | [::1]:57557    |                  | SHOW CLUSTER SESSIONS;  | 2017-06-14 19:09:50.609823+00:00 | 2017-06-14 19:09:54.480242+00:00 | NULL   |
+---------+----------+----------------+------------------+-------------------------+----------------------------------+----------------------------------+--------+
(1 row)
```

Extending this functionality with cancellation would be a logical next step.

QTS cancellation all have different motivations and use cases:

#### Query cancellation

Query cancellation is particularly useful for cancelling long-running queries hogging up cluster resources.

_Both MySQL and Postgres support query cancellation._  
_A prerequisite for transaction cancellation._

#### Transaction cancellation

Transaction cancellation would involve cancelling any active queries under the transaction,
in addition to rolling it back and cleaning up intents held by the transaction.  
Cancelling an idle transaction is a useful use case for example.

_A prerequisite for session cancellation._

#### Session cancellation

Session cancellation would involve cancelling any active transactions, followed by termination
of the connection with the client. A particular useful use case would be revoking unauthorized access to the DB.

_Both MySQL and Postgres support session cancellation._

# Guide-level explanation

New `CANCEL [QUERY|TRANSACTION|SESSION] <id>` statements will be added to cockroach's SQL dialect. These statements can be used to cancel running QTS.  

IDs for queries can be received through `SHOW QUERIES` and IDs for transactions and sessions can be received through `SHOW SESSIONS` as 32-character hex strings.

Example use case: Assume there are two concurrently running sessions, session 1 and 2.

Session 1 (long running query):

```sql
root@:26257/> SELECT * FROM students WHERE long_bio LIKE '%ips%or';
```

Session 2 (DB admin wishing to cancel the query above):

```sql
root@:26257/> SELECT node_id, id, query FROM [SHOW CLUSTER QUERIES];
+---------+----------------------------------+-----------------------------------------------------------+
| node_id | id                               | query                                                     | 
+---------+----------------------------------+-----------------------------------------------------------+
| 1       | 2834b4f8a230ce2394e4f12a8c9236c1 | SHOW CLUSTER QUERIES                                      |
| 2       | 827f91ab89c3e72d10154fbec8293de1 | SELECT * FROM students WHERE long_bio LIKE '%ips%or';     |
+---------+----------------------------------+-----------------------------------------------------------+
(2 rows)

root@:26257/> CANCEL QUERY '827f91ab89c3e72d10154fbec8293de1';
CANCEL QUERY

root@:26257/> SELECT node_id, id, query FROM [SHOW CLUSTER QUERIES];
+---------+----------------------------------+-----------------------------------------------------------+
| node_id | id                               | query                                                     |
+---------+----------------------------------+-----------------------------------------------------------+
| 1       | 2834b4f8a230ce2394e4f12a8c9236c1 | SHOW CLUSTER QUERIES                                      |
+---------+----------------------------------+-----------------------------------------------------------+
(1 row)
```

Going back to session 1:

```sql
root@:26257/> SELECT * FROM students WHERE long_bio LIKE '%ips%or';
pq: query cancelled by user 'root'

root%:26257/>
```

Common errors include:
1. Invalid ID.
2. ID valid but not found.
3. Failure to dial to remote node in the cluster.

Note that if the `CANCEL QUERY` statement succeeds, there is no guarantee that the query in question was actually cancelled - and did not commit any changes (if applicable).

# Reference-level explanation

_Will be updated as development progresses._

# Detailed design

>Due to the interdependent nature of these different levels of cancellation, and the significant
technical overlap between each feature, query cancellation will be the first feature to be implemented.  
Query cancellation by itself solves the fewest number of use cases compared to transaction
or session cancellation, but once it has been implemented well, its mechanism can be easily
leveraged for transaction and then for session cancellation.

### Identifiers

Queries, transactions and sessions will use IDs which will encode the node ID into them, so when there's a request to cancel, cockroach will know where to route the request without issuing additional requests to other nodes.  
The node ID will be decoded from the `CANCEL [QUERY|TRANSACTION|SESSION]` request, and an RPC call would be made to that node if it is not the local node to issue the cancel.

Identifiers will be stored as 128-bit unsigned integers (two `uint64`s) - composed of the
96-bit HLC timestamp plus 32 bits of node ID. [There already is a uint128 type in the code
base](https://github.com/cockroachdb/cockroach/tree/master/pkg/util/uint128) that can be levereged.
Using the full HLC timestamp along with the node ID makes this ID unique across the cluster.

The identifiers will be primarily presented in:
- `SHOW QUERIES`:  queries IDs presented under the `id` column as 32-character hex strings.
- `SHOW SESSIONS`: session and transaction IDs will be presented in this table as 32-character hex strings.
- in `CANCEL [QUERY/TRANSACTION/SESSION] <id>` statements as the only argument.

Those identifiers could also be added to other parts of the code, such as to log tags. However, those changes will be out of scope for this project.

Example output of `SHOW QUERIES` with query ID (see the `id` column):

```sql
root@:26257/> SELECT node_id, id, query FROM [SHOW CLUSTER QUERIES];
+---------+----------------------------------+--------------------------------------------------------------------+
| node_id | id                               | query                                                              | 
+---------+----------------------------------+--------------------------------------------------------------------+
| 1       | 2834b4f8a230ce2394e4f12a8c9236c1 | SHOW CLUSTER QUERIES                                               |
| 2       | 827f91ab89c3e72d10154fbec8293de1 | INSERT INTO Students VALUES ('Robert'); DROP TABLE Students; --')  |
+---------+----------------------------------+--------------------------------------------------------------------+
(2 rows)
```

### Syntax
New `CANCEL [QUERY|TRANSACTION|SESSION] <id>` statements will be added to CockroachDB's SQL dialect. The syntax of the actual `CANCEL` statement has already been approved in
[this RFC](https://github.com/cockroachdb/cockroach/pull/16273) and implemented.

A non-root database user can only issue a cancel request to QTS run by that same user. The root user can cancel any on the cluster.

When this statement is executed, the ID will be parsed to determine the node ID, and an RPC call will be made to that node if it is not the current node. That node would look up the QTS and cancel it.

For a more efficient look up, there will be a map for local queries, transactions and sessions. The node would use the ID of the QTS to retrieve it from the map and cancel it.

The request would return an error if no matching QTS was found for that ID, or if the ID was invalid.

## Query cancellation

We will reuse the transaction's context at the query level; all query cancellations will close the entire transaction's
context. Forking a query-specific context would be technically challenging due to [context scoping assumptions made in
the TxnCoordSender](https://github.com/cockroachdb/cockroach/blob/8ff5ff97df139fa5958e15a2fd5ffa65e09b49ff/pkg/kv/txn_coord_sender.go#L619). The
`queryMeta` will store a reference to the txn context. There will also be an integer flag in `queryMeta`
that, when set to 1 (using an `atomic.StoreInt32()`), will signal cancellation. PlanNodes that do in-memory
processing such as insertNode's insert batching, and sortNode's sorting, will periodically check
for this flag (such as once every some tens of thousands of rows).

Any `CANCEL QUERY` statements directed at that query will close the txn context's `Done` channel, which would
error out any RPCs being made for that query. The cancellation flag would also be atomically set to 1,
which would cause any planNodes that check it periodically to return a cancellation error. The error would
propagate to the SQL executor which would then mark the SQL transaction as aborted. The client would
be expected to issue a `ROLLBACK` upon seeing the errored-out query. The client's connection will _not_ be closed.

The executor would re-throw a more user-friendly "Execution cancelled upon user request" error instead of
the "Context cancelled" one.

Both context cancellation and a shared flag is being used, because:

- Context cancellation is already built into gRPC and works well for cleaning up work on other nodes.
- Checking a shared integer atomically is significantly faster than checking a context's Done channel,
especially when that context is the result of several derivations.

For minimal impact on performance, the integer is better for frequent cancellation checks, and a select
on `<- ctx.Done()` is better for parts of the code that already wait on it (or wait for other channels).

#### DistSQL cancellation

After some initial RPCs from the gateway node out to other node, RPCs in distSQL generally
go in the reverse direction (i.e. from producers to consumers). We can't just rely on gRPC
to propagate context cancellation down to all nodes. However distSQL does have drain signals
that go from consumers to producers, and can prevent more rows from being sent back. These
drain signals can potentially be levereged for cancelling distSQL queries.

The first implementation of this project will not involve adding any distSQL-specific mechanisms;
so non-gateway nodes will continue to work on cancelled queries until completion.
This RFC will be updated after the first implementation with a more detailed plan on distsql
query cancellation.

## Transaction cancellation

A new boolean flag will be added into the transaction state struct(`txnState`) to indicate whether the transaction should transition to an aborted state.

The boolean flag in the `txnState` object will be set during transaction cancellation. This flag will be checked at key points by the executor who will be responsible for transitioning the txn into an aborted state if the flag is set.  

#### Tests

Tests for transaction cancellation should be written as logic and cluster tests in the `run_control` test suite.  
Fundamentally, it should be similar to the query cancellation tests.  
Multiple types of queries need to be tested:
*   When the txn is currently running a distributed query. (I understand this may not succeed currently because cancellation of distributed queries is not complete yet, but then your test should expect and validate that the cancellation fails.)
*   With a query running `crdb_internal.force_retry('10m')` so that it is busy with a retry loop.
*   With a query running `show trace for ...` with a long-running statement.

## Session cancellation

All active transactions under the session will be cancelled followed by termination of the connection with the client.

# Drawbacks

_Will be updated as development progresses._

# Rationale and Alternatives

### Why separate transaction cancellation from query cancellation?

Currently, canceling a query aborts the transaction since the transaction model is very simple but this will not be the case. In the future, cockroach will expose error handling capabilities in its SQL dialect which will allow one to recover from errors without toasting a transaction. Thus the semantics of the query cancellation should not be tied too much to those of transactions.  
Moreover, transactions are not necessarily constantly running queries and it should be possible to cancel a transaction at a time when there's no query running under it.  
And even if there is a query in progress, cancelling it races with the query finishes, so you can't rely on cancelling that query to necessarily do anything such as cancelling the transaction.

### Why have separate syntax for query/transaction/session cancellation?

Both MySQL and Postgres support query and sessions cancellation separately, so it makes sense to separate them in CockroachDB as well.

Needless to say, if we accept that cancelling a query or session without disconnecting a client is useful, we should also accept that cancelling a transaction without disconnecting a client is useful.

Transaction and session cancellation are closely related and the implementation is quite similar, but it makes sense to have syntax to cancel them separately.

Although, there isn't a very clear use case for either query or transaction separate from session cancellation, the syntax should be separate.

### Not to include node ID in the identifiers

One alternative that was considered for IDs was an in-memory counter combined with the
node ID in one string. This would have led to issues with duplicate IDs if the counter
resets to 0 upon node restart, so it wasn't chosen. `unique_rowid()`s were considered but
were also rejected due to a lack of a strong uniqueness guarantee with it.

Not combining the node ID into the ID was once suggested, but that would have required
the user to supply both the node and appropriate IDs to the `CANCEL` statement. Since the ID
would make little sense out of the context of a node ID, and both IDs will be presented together
in `SHOW QUERIES/SESSIONS`, it makes more sense from a UI perspective to combine them.

Needless to say, there's no global list of sessions. Although, it is possible to do `SHOW CLUSTER SESSIONS` to get all sessions and linearly search them for a matching session but that internally dials to each node and asks it for its list which is too expensive.

### Support postgres' cancellation syntax

Postgres has a built-in function that has cancellation as a side-effect:

```
SELECT pg_cancel_backend(<query-id>);
```

Supporting this syntax was considered for compatibility reasons but decided against. Since
functions should ideally not have side-effects, and also because postgres' query IDs are `int32`s
which would be hard to reconcile with our larger query IDs.

# Unresolved questions

_Will be updated as development progresses._
