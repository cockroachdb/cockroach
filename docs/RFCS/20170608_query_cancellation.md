- Feature Name: Query and session cancellation
- Status: in-progress
- Start Date: 2017-06-05
- Authors: Bilal Akhtar
- RFC PR: [#16417](https://github.com/cockroachdb/cockroach/pull/16417)
- Cockroach Issue: [#15593](https://github.com/cockroachdb/cockroach/issues/15593)

# Summary

This feature adds the ability to cancel an in-progress query, transaction, or session
using new `CANCEL` statements. Initially, this project starts off with a focus solely
on query cancellation, and adds a new query identifier and a new `CANCEL QUERY` statement
for this purpose.

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

Extending that functionality with cancellation would be a logical next step.

Query, transaction and session cancellation all have different motivations and use-cases:

- Query cancellation is particularly useful for long-running queries hogging up cluster resources.
Upon cancellation, a query would immediately return an error and leave the transaction in a state
where it can be rolled back.
- Transaction cancellation would involve cancelling any active queries under that transaction,
in addition to rolling it back and cleaning up intents and leases held by that transaction. Cancelling
idle transactions would also be useful for this reason.
- Session cancellation would involve cancelling any active transactions, followed by termination
of the connection with the client. Would be useful for revoking unauthorized access to db.

Due to the interdependent nature of these different levels of cancellation, and the significant
technical overlap between each feature, query cancellation will be the first feature to be implemented.
Query cancellation by itself solves the fewest number of use-cases compared to transaction
or session cancellation, but once it has been implemented well, its mechanism can be easily
levereged for transaction and session cancellation.

This RFC currently only describes query cancellation in detail; it will be expanded with details
about transaction and session cancellation once query cancellation has been implemented.

# Detailed design

## Query identifiers

Query identifiers are intended to provide a unique way to identify a query across
the cluster, preferably with an easy way to recover the node ID from the query ID without issuing
requests to other nodes. They will be used primarily in:

- `SHOW QUERIES`, presented under the `id` column.
- `CANCEL QUERY <query-id>` statements as the only argument.

Query identifiers could also be added to other parts of the code, such as to log tags, however
those changes will be out of scope for this project.

Query identifiers will be stored as 128-bit unsigned integers (two `uint64`s) - composed of the
96-bit HLC timestamp plus 32 bits of node ID. [There already is a uint128 type in the code
base](https://github.com/cockroachdb/cockroach/tree/master/pkg/util/uint128) that can be levereged.
Using the full HLC timestamp along with the node ID makes this ID unique across the cluster.

Query IDs will be presented in the output of `SHOW QUERIES` as 32-character hex strings.

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

## Context forking / query cancellation mechanism

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

## New CANCEL QUERY statement

The syntax of the actual `CANCEL` statement has already been approved in
[this RFC](https://github.com/cockroachdb/cockroach/pull/16273), and will be of the form
`CANCEL [QUERY|JOB] <query_id>`.

A non-root database user can only issue `CANCEL QUERY` statements to queries run by that same
user. The root user can cancel any queries on the cluster.

When this statement is executed, the query ID will be parsed to determine the node ID, and an RPC call
will be made to that node if it is not the current node. That node would look up and cancel the
context related to that query.

The `CANCEL QUERY` would return an error if no query was found for that ID. However, if the
`CANCEL QUERY` statement succeeds, there is no guarantee that the query in question was actually
cancelled - and did not commit any changes (if applicable).

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

## DistSQL cancellation

After some initial RPCs from the gateway node out to other node, RPCs in distSQL generally
go in the reverse direction (i.e. from producers to consumers). We can't just rely on gRPC
to propagate context cancellation down to all nodes. However distSQL does have drain signals
that go from consumers to producers, and can prevent more rows from being sent back. These
drain signals can potentially be levereged for cancelling distSQL queries.

The first implementation of this project will not involve adding any distSQL-specific mechanisms;
so non-gateway nodes will continue to work on cancelled queries until completion.
This RFC will be updated after the first implementation with a more detailed plan on distsql
query cancellation.

# Drawbacks

# Alternatives

## Query ID alternatives

One alternative that was considered for query IDs was an in-memory counter combined with the
node ID in one string. This would have led to issues with duplicate query IDs if the counter
resets to 0 upon node restart, so it wasn't chosen. `unique_rowid()`s were considered but
were also rejected due to a lack of a strong uniqueness guarantee with it.

Not combining the node ID into the query ID was once suggested, but that would have required
the user to supply both the node and query IDs to the `CANCEL` statement. Since the query ID
would make little sense out of the context of a node ID, and both IDs will be presented together
in `SHOW QUERIES`, it makes more sense from a UI perspective to combine them.

## Supporting postgres' cancellation syntax

Postgres has a built-in function that has cancellation as a side-effect:

```
SELECT pg_cancel_backend(<query-id>);
```

Supporting this syntax was considered for compatibility reasons but decided against - since
functions should ideally not have side-effects, and also because postgres' query IDs are `int32`s
which would be hard to reconcile with our larger query IDs. 

# Unresolved questions

