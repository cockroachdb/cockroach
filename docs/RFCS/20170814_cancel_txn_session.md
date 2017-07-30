- Feature Name: Transaction and Session cancellation
- Status: draft
- Start Date: 2017-08-14
- Author: Julian Gilyadov
- RFC PR: [#17252](https://github.com/cockroachdb/cockroach/pull/17252)
- Cockroach Issue: None

# Summary

This RFC proposes a set of features which add the ability to cancel an in-progress transaction and session using `CANCEL` statements.  
Cancellation can be useful in many use cases such as cancelling a session to revoke an unauthorized access to the DB.  
The implementation of those proposed features is similar to the implementation of the query cancellation feature as described in this [RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20170608_query_cancellation.md) and [PR #17003](https://github.com/cockroachdb/cockroach/pull/17003).  
Generally, new `CANCEL [TRANSACTION|SESSION] <id>` statements will be added and new transaction and session identifiers will be implemented to include the node ID in them. As a result, CockroachDB will know where to route the request internally without issuing additional requests to other nodes.

# Motivation

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

Extending this functionality with transaction and session cancellation would be a logical next step.

Both transaction and session cancellation have different motivations and use cases:

#### Transaction cancellation

Transaction cancellation would involve cancelling any active queries(if any) under the transaction,
in addition to rolling it back and cleaning up intents held by the transaction.  
Note that cancelling an idle transaction is impossible using the query cancellation mechanism. And even if there is a query in progress, cancelling it races with the query finishes, so one can't rely on cancelling that query to necessarily do anything such as cancelling the transaction.  

#### Session cancellation
Session cancellation would involve cancelling the current transaction (if any), followed by closing the client connection.  
A particular useful use case would be revoking unauthorized access to the DB.

_Both MySQL and Postgres support session cancellation._

# Guide-level explanation
New `CANCEL [TRANSACTION|SESSION] <id>` statements will be added to cockroach's SQL dialect. These statements can be used to cancel running transactions and sessions.  

IDs for transactions and sessions can be received through `SHOW SESSIONS` as 32-character hex strings.

Example use case: Assume there are two concurrently running sessions, session 1 and 2.

Session 1 (running transaction):

```sql
root@:26257/> SHOW TRACE FOR SELECT * FROM generate_series(1,20000000);
```

Session 2 (DB admin wishing to cancel the txn above):

```sql
root@:26257/> SELECT node_id, kv_txn, active_queries FROM [SHOW CLUSTER SESSIONS];
+---------+--------------------------------------+-------------------------------------------------------+
| node_id | kv_txn                               | active_queries                                        | 
+---------+--------------------------------------+-------------------------------------------------------+
| 1       | 14e58aab-9046-1f2d-0000-000000000001 | SHOW TRACE FOR SELECT * FROM generate_series(1,20000) |
| 2       | 14e58aac-50cf-3cfc-0000-000000000002 | SHOW CLUSTER SESSIONS                                 |
+---------+--------------------------------------+-------------------------------------------------------+
(2 rows)

root@:26257/> CANCEL TRANSACTION '14e58aab-9046-1f2d-0000-000000000001';
CANCEL TRANSACTION

root@:26257/> SELECT node_id, kv_txn, active_queries FROM [SHOW CLUSTER SESSIONS];
+---------+--------------------------------------+-------------------------------------------------------+
| node_id | kv_txn                               | active_queries                                        | 
+---------+--------------------------------------+-------------------------------------------------------+
| 1       | NULL                                 |                                                       |
| 2       | 14e58ffc-23cf-3f3c-0000-000000000002 | SHOW CLUSTER SESSIONS                                 |
+---------+--------------------------------------+-------------------------------------------------------+
(2 rows)
```

Going back to session 1:

```sql
root@:26257/> SHOW TRACE FOR SELECT * FROM generate_series(1,20000000);
pq: transaction cancelled by user 'root'

root%:26257/>
```

Session 2 (DB admin wishing to cancel session 1)

```sql
root@:26257/> SELECT node_id, session_id FROM [SHOW CLUSTER SESSIONS];
+---------+--------------------------------------+
| node_id | session_id                           | 
+---------+--------------------------------------+
| 1       | 14e58aac-50cf-3cfc-0000-000000000001 |
| 2       | 14e22aab-9046-1f2d-0000-000000000002 |
+---------+--------------------------------------+
(2 rows)

root@:26257/> CANCEL SESSION '14e58aac-50cf-3cfc-0000-000000000001';
CANCEL SESSION

root@:26257/> SELECT node_id, session_id FROM [SHOW CLUSTER SESSIONS];
+---------+--------------------------------------+
| node_id | session_id                           | 
+---------+--------------------------------------+
| 2       | 14e22aab-9046-1f2d-0000-000000000002 |
+---------+--------------------------------------+
(1 rows)
```

Going back to session 1:

```sql
pq: session cancelled by user 'root'

user:~$: 
```

Common errors include:
1. Invalid ID.
2. Transaction/session not found.
3. Failure to dial to remote node in the cluster.

# Reference-level explanation

_Will be updated as development progresses._

# Detailed design

### Identifiers

Transactions and sessions will use IDs which will encode the node ID into them, so when there's a request to cancel, CockroachDB will know where to route the request without issuing additional requests to other nodes.  
The node ID will be decoded from the `CANCEL [TRANSACTION|SESSION]` request, and an RPC call would be made to that node if it is not the local node to issue the cancel.

Identifiers will be stored as [uint128](https://github.com/cockroachdb/cockroach/tree/master/pkg/util/uint128) composed of the 96-bit HLC timestamp plus 32 bits of node ID.  
Using the full HLC timestamp along with the node ID makes this ID unique across the cluster.

The identifiers will be primarily presented in:
- `SHOW SESSIONS`: session and transaction IDs will be presented in this table as 32-character hex strings.
- in `CANCEL [TRANSACTION/SESSION] <id>` statements as the only argument.

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
New `CANCEL [TRANSACTION|SESSION] <id>` statements will be added to CockroachDB's SQL dialect.  

A non-root database user can only issue a cancel request run by that same user. The root user can cancel any transaction or session on the cluster.

When the statement is executed, the ID will be parsed to determine the node ID, and an RPC call will be made to that node if it is not the current node. That node would look up the transaction or session and cancel it.

Each node will maintain mapping of current (local) transactions and sessions, indexed by their respective IDs.

The request would return an error if no matching transaction or session was found for that ID, or if the ID was invalid(InvalidArgument).

## Transaction cancellation
The transaction will transition immediately if there are no active queries.
If there are active queries, then:
1. No more queries can be started on the session until the transaction is in the Aborted state.
2. Cancel all running queries.
3. Wait for the cancelled queries to finish and then transition the transaction into an aborted state if needed. Note that usually, no explicit transition is needed because the act of cancelling all the active queries will have transitioned the transaction to the desired state.

#### Tests

Tests for transaction cancellation should be written as logic and cluster tests in the `run_control` test suite.  
Fundamentally, it should be similar to the query cancellation tests.  
Multiple types of queries need to be tested:
*   When the txn is currently running a distributed query. (I understand this may not succeed currently because cancellation of distributed queries is not complete yet, but then your test should expect and validate that the cancellation fails.)
*   With a query running `crdb_internal.force_retry('10m')` so that it is busy with a retry loop.
*   With a query running `show trace for ...` with a long-running statement.

## Session cancellation
The connection with the client is terminated followed by cancellation of the current transaction (if any).

# Drawbacks
None.

# Rationale and Alternatives

### Why separate transaction cancellation from query cancellation?

Currently, canceling a query aborts the transaction since the transaction model is very simple but this will not be the case. In the future, CockroachDB will expose error handling capabilities in its SQL dialect which will allow one to recover from errors without toasting a transaction. Thus the semantics of the query cancellation should not be tied to those of transactions.  
Moreover, transactions are not necessarily constantly running queries and it should be possible to cancel a transaction at a time when there's no query running under it.  
And even if there is a query in progress, cancelling it races with the query finishes, so one can't rely on cancelling that query to necessarily do anything such as cancelling the transaction.

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

# Unresolved questions

### Should we support postgres wire protocol query cancellation
_Should we remove this section as the RFC is no longer about query cancellation?_  
Postgres has a built-in function for query cancellation:

```
SELECT pg_cancel_backend(<query-id>);
```

Supporting this syntax was considered for compatibility reasons but decided against. Since
functions should ideally not have side-effects, and also because postgres' query IDs are `int32`s
which would be hard to reconcile with our larger query IDs.

However, it has its benefits and further discussion is needed.
