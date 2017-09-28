- Feature Name: Transaction and Session cancellation
- Status: draft
- Start Date: 2017-08-14
- Author: Julian Gilyadov
- RFC PR: [#17252](https://github.com/cockroachdb/cockroach/pull/17252)
- Cockroach Issue: None

# Summary

This RFC proposes a set of features which add the ability to cancel
an in-progress transaction and session using the `CANCEL` statements.  
Cancellation can be useful in many use cases such as canceling a session
to revoke an unauthorized access to the DB.  

The implementation of those proposed features is similar to the implementation
of the query cancellation feature as described in this [RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20170608_query_cancellation.md)
and [PR #17003](https://github.com/cockroachdb/cockroach/pull/17003).  
In summary, query cancellation cancels the query by ID; 
it uses the query ID to locate the coordinating node;
and that node looks up and cancels the context related to that query.

Generally, new `CANCEL [TRANSACTION|SESSION] <id>` statements will be added
and new transaction and session identifiers will be implemented to include
the node ID in them. As a result, CockroachDB will know where to route the
request internally without issuing additional requests to other nodes.

# Motivation

Currently, `SHOW SESSIONS` lists active sessions:

```sql
root@:26257/> SHOW SESSIONS;
+---------+----------+----------------+------------------+-------------------------+----------------------------------+----------------------------------+--------+
| node_id | username | client_address | application_name |     active_queries      |          session_start           |        oldest_query_start        | kv_txn |
+---------+----------+----------------+------------------+-------------------------+----------------------------------+----------------------------------+--------+
|       1 | root     | [::1]:57557    |                  | SHOW CLUSTER SESSIONS;  | 2017-06-14 19:09:50.609823+00:00 | 2017-06-14 19:09:54.480242+00:00 | NULL   |
+---------+----------+----------------+------------------+-------------------------+----------------------------------+----------------------------------+--------+
(1 row)
```

Transaction and session cancellation is essential for DBMS administration and 
currently it is only possible to view them, so extending this functionality 
with cancellation would be a logical next step.

Virtually all current popular DBMS include transaction and/or session cancellation.

Both transaction and session cancellation have different motivations and use cases:

#### Transaction cancellation

Transaction cancellation would involve canceling any active queries(if any) under the transaction,
in addition to rolling it back and cleaning up intents held by the transaction.  
A common use case is to cancel a transaction which uses too much of cluster resources.
Note that currently, query cancellation cancels the underlying transaction too.  
However, canceling an idle transaction is impossible using the query cancellation mechanism; 
and even if there is a query in progress, canceling it races with the query finishing; so one 
can't rely on canceling a query to necessarily do anything such
as canceling the transaction.  

#### Session cancellation
Session cancellation would involve canceling the current transaction (if any),
followed by closing the client connection.  
A common use case is to cancel a session which uses too much of cluster resources.
A particular useful use case would be revoking unauthorized access to the DB.  
Also, the existence of cluster settings that don't take effect until all sessions 
have been restarted is a good argument for session cancellation.  
Cancelling all active sessions would be less disruptive than restarting all client processes.

_Both MySQL and Postgres support session cancellation._

# Guide-level explanation
New `CANCEL [TRANSACTION|SESSION] <id>` statements will be added to cockroach's SQL dialect.
These statements can be used to cancel running transactions and sessions.  

IDs for transactions and sessions can be received through `SHOW SESSIONS`
as 32-character hex strings.

Example use case: Assume there are two concurrently running sessions, session 1 and 2.

Session 1 (running transaction):

```sql
root@:26257/> SHOW TRACE FOR SELECT * FROM generate_series(1,20000000);
```

Session 2 (DB admin wishing to cancel the txn above):

```sql
root@:26257/> SELECT node_id, kv_txn, active_queries FROM [SHOW CLUSTER SESSIONS];
+---------+----------------------------------+-------------------------------------------------------+
| node_id | kv_txn                           | active_queries                                        | 
+---------+----------------------------------+-------------------------------------------------------+
| 1       | 14e58aab90461f2d0000000000000001 | SHOW TRACE FOR SELECT * FROM generate_series(1,20000) |
| 2       | 14e58aac50cf3cfc0000000000000002 | SHOW CLUSTER SESSIONS                                 |
+---------+----------------------------------+-------------------------------------------------------+
(2 rows)

root@:26257/> CANCEL TRANSACTION '14e58aab-9046-1f2d-0000-000000000001';
CANCEL TRANSACTION

root@:26257/> SELECT node_id, kv_txn, active_queries FROM [SHOW CLUSTER SESSIONS];
+---------+----------------------------------+-------------------------------------------------------+
| node_id | kv_txn                           | active_queries                                        | 
+---------+----------------------------------+-------------------------------------------------------+
| 1       | NULL                             |                                                       |
| 2       | 14e58ffc23cf3f3c0000000000000002 | SHOW CLUSTER SESSIONS                                 |
+---------+----------------------------------+-------------------------------------------------------+
(2 rows)
```

Going back to session 1:

```sql
root@:26257/> SHOW TRACE FOR SELECT * FROM generate_series(1,20000000);
pq: transaction cancelled by user 'root'

root%:26257 ERROR>
```

After a transaction was canceled the session would be in an `Aborted` 
state(and thus we will have an `ERROR` status in our prompt). To reset 
the session state the client has to send a `ROLLBACK` statement.

Another example use case is session cancellation:

Session 2 (DB admin wishing to cancel session 1)

```sql
root@:26257/> SELECT node_id, session_id FROM [SHOW CLUSTER SESSIONS];
+---------+----------------------------------+
| node_id | session_id                       | 
+---------+----------------------------------+
| 1       | 14e58aac50cf3cfc0000000000000001 |
| 2       | 14e22aab90461f2d0000000000000002 |
+---------+----------------------------------+
(2 rows)

root@:26257/> CANCEL SESSION '14e58aac-50cf-3cfc-0000-000000000001';
CANCEL SESSION

root@:26257/> SELECT node_id, session_id FROM [SHOW CLUSTER SESSIONS];
+---------+----------------------------------+
| node_id | session_id                       | 
+---------+----------------------------------+
| 2       | 14e22aab90461f2d0000000000000002 |
+---------+----------------------------------+
(1 rows)
```

Going back to session 1:

```sql
pq: session canceled by user 'root'

user:~$: 
```

Common errors include:
1. Invalid ID.
2. Transaction/session not found.
3. Failure to dial to the remote node in the cluster.

# Reference-level explanation

_Will be updated as development progresses._

# Detailed design

### Identifiers

Transactions and sessions will use IDs which will encode the node ID into them,
so when there's a request to cancel, CockroachDB will know where to route the request
without issuing additional requests to other nodes.  
The node ID will be decoded from the `CANCEL [TRANSACTION|SESSION]` request, and
an RPC call would be made to that node if it is not the local node to issue the cancel.

Identifiers will be stored as [uint128](https://github.com/cockroachdb/cockroach/tree/master/pkg/util/uint128) 
composed of the 96-bit HLC timestamp plus 32 bits of node ID.  
Using the full HLC timestamp along with the node ID makes this ID unique across the cluster.

The identifiers will be primarily presented in:
- `SHOW SESSIONS`: session and transaction IDs will be presented
in this table as32-character hex strings.
- in `CANCEL [TRANSACTION/SESSION] <id>` statements as the only argument.

Those identifiers could also be added to other parts of the code, such as to log tags.
However, those changes will be out of scope for this project.

Example output of `SHOW SESSIONS` with transaction IDs (see the `kv_txn` column):

```sql
root@:26257/> SELECT node_id, kv_txn, active_queries FROM [SHOW CLUSTER SESSIONS];
+---------+----------------------------------+-------------------------------------------------------+
| node_id | kv_txn                           | active_queries                                        | 
+---------+----------------------------------+-------------------------------------------------------+
| 1       | 14e58aab90461f2d0000000000000001 | SHOW TRACE FOR SELECT * FROM generate_series(1,20000) |
| 2       | 14e58aac50cf3cfc0000000000000002 | SHOW CLUSTER SESSIONS                                 |
+---------+----------------------------------+-------------------------------------------------------+
(2 rows)
```

### Syntax
New `CANCEL [TRANSACTION|SESSION] <id>` statements will be added to CockroachDB's SQL dialect.  

A non-root database user can only issue a cancel request run by that same user.
The root user can cancel any transaction or session on the cluster.

When the statement is executed, the ID will be parsed to determine the node ID,
and an RPC call will be made to that node if it is not the current node.
That node would look up the transaction or session and cancel it.

Each node will maintain a mapping of current (local) transactions and sessions,
indexed by their respective IDs.

The request would return an error if no matching transaction or session was found
for that ID, or if the ID was invalid(InvalidArgument).

## Transaction cancellation
If there are no active queries, the KV transaction will be rolled back and the session will 
transition to the `Aborted` state.
If there are active queries, then:
1. No more queries can be started on the session until the transaction is in the Aborted state. 
For that, we introduce an `isCancelled` flag in the `txnState`, which is checked in the executor 
before a query is about to start executing.
2. Cancel all running queries by iterating all active queries under the 
transaction and canceling them, this will also cancel the transaction's context.
3. Wait for the canceled queries to finish and then transition the transaction
into the `Aborted` state if needed. Note that usually, no explicit transition is
needed because the act of canceling all the active queries will have
transitioned the transaction into the desired state already.

#### Tests

Tests for transaction cancellation should be written in `logic_test/run_contorl.go` 
and `run_control_test.go`.  
Fundamentally, it should be similar to the query cancellation tests.  
Multiple types of queries need to be tested:
*   When the txn is currently running a distributed query.
*   With a query running `crdb_internal.force_retry('10m')` so that it is busy with a retry loop.
*   With a query running `show trace for ...` with a long-running statement.

## Session cancellation
The connection with the client is terminated followed by the
cancellation of the current transaction (if any).  
This will be achieved by canceling the session's context 
and use `newReadTimeoutConn()` to evaluate that Context when 
we're blocked on a network socket read.

# Drawbacks
None.

# Rationale and Alternatives

### Why separate transaction cancellation from query cancellation?

Currently, canceling a query aborts the transaction since the
transaction model is very simple but this will not be the case.
In the future, CockroachDB will expose error handling capabilities
in its SQL dialect which will allow one to recover from errors without
toasting a transaction. Thus the semantics of the query cancellation should
not be tied to those of transactions.  
Moreover, transactions are not necessarily constantly running queries and
it should be possible to cancel a transaction at a time when there's no query running under it.  
And even if there is a query in progress, canceling it races with the query
finishes, so one can't rely on canceling that query to necessarily
do anything such as canceling the transaction.

### Why have separate syntax for query/transaction/session cancellation?

Both MySQL and Postgres support query and sessions cancellation separately,
so it makes sense to separate them in CockroachDB as well.

Needless to say, if we accept that canceling a query or session without disconnecting
a client is useful, we should also accept that canceling a transaction
without disconnecting a client is useful.

Transaction and session cancellation are closely related and the implementation
is quite similar, but it makes sense to have syntax to cancel them separately.

Although there isn't a very clear use case for either query or transaction
separate from session cancellation, the syntax should be separate.

### Not to include node ID in the identifiers

One alternative that was considered for IDs was an in-memory counter combined with the
node ID in one string. This would have led to issues with duplicate IDs if the counter
resets to 0 upon node restart, so it wasn't chosen. `unique_rowid()`s were considered but
were also rejected due to a lack of a strong uniqueness guarantee with it.

Not combining the node ID into the ID was once suggested, but that would have required
the user to supply both the node and appropriate IDs to the `CANCEL` statement. Since the ID
would make little sense out of the context of a node ID, and both IDs will be presented together
in `SHOW QUERIES/SESSIONS`, it makes more sense from a UI perspective to combine them.

Needless to say, there's no global list of sessions. Although, it is possible
to do `SHOW CLUSTER SESSIONS` to get all sessions and linearly search them
for a matching session but it internally dials to each node and asks it for
its list which is too expensive.

# Unresolved questions

### Should we support Postgres wire protocol query cancellation
_This was also discussed in the query cancellation RFC and PR._

Postgres has a built-in function for session cancellation 
which has query cancellation as a side-effect.

`pg_cancel_backend()` - takes a session ID as an argument, but it only cancels the current 
query in that session (if any).  
`pg_terminate_backend()` - is for session cancellation.  
Both of these functions take a 32-bit session ID as an argument.  
The protocol-level cancellation feature is more or less equivalent 
to `pg_cancel_backend()` but it takes two 32-bit arguments:
1. The session id.
2. 32-bit session-level "secret key" (used as a 
lightweight authentication mechanism so this type of cancellation can be used without 
establishing another fully-authenticated connection to issue the cancellation call).

```
SELECT pg_cancel_backend(<session-id>, <session-secret-key>);
```

Supporting this syntax was considered for compatibility reasons but decided against.  
Since functions should ideally not have side-effects, and also because Postgres' IDs are `int32`s
which would be hard to reconcile with our larger IDs.

The small key sizes make these functions difficult 
to support (especially when load balancers are involved). 
We may want to support these interfaces one day (since they're presumably built into Postgres 
client drivers), but for now, we should just support our own syntax with larger IDs and see 
how much demand there is for Postgres-compatible cancellation.  
Implementing some mapping of 32 or 64-bit IDs to queries 
would get its own RFC when/if we decide to do it.
