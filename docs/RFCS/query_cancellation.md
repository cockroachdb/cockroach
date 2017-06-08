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

Adding a unique query identifier to each query and exposing that in this table will be necessary
for the DBA to be able to issue the correct CANCEL command.

# Detailed design

## Query identifiers

Query identifiers will be of the format `<node_id>:<counter>`, where `<node_id>` is the ID
of the gateway node (an integer), and `<counter>` is an `int64` counter on that gateway node
that gets incremented for every query run on that node.

Bringing the node ID into the query identifier removes the need to either:

- Contact every node to figure out where that query lives, or
- Have the user pass in two arguments to the `CANCEL QUERY` statement; both the node ID and
the query ID.

Using a colon as the delimiter instead of a comma or period makes it clear that the ID field
is a string field, and not a float value.

Query IDs will be presented in the output of `SHOW QUERIES`:

```sql
root@:26257/> SELECT node_id, id, query FROM [SHOW CLUSTER QUERIES];
+---------+----------+--------------------------------------------------------------------+
| node_id | id       | query                                                              | 
+---------+----------+--------------------------------------------------------------------+
| 1       | 1:43872  | SHOW CLUSTER QUERIES                                               |
| 2       | 2:11     | INSERT INTO Students VALUES ('Robert'); DROP TABLE Students; --')  |
+---------+----------+--------------------------------------------------------------------+
(2 rows)
```

Having the counter part be numeric also allows us to eventually support the Postgres cancellation
syntax:

```sql
SELECT pg_cancel_backend(<query-id>);
```

Since postgresql has integer query IDs, for compatibility reasons this function could support taking
just the counter part of the query ID as input instead of the full ID. The db could assume it's referring
to a query on that same gateway node.

However, while supporting `pg_cancel_backend` is part of the motivation to go with integer query IDs,
actual implementation of `pg_cancel_backend` is not a key part of this project and can be deferred
until deemed necessary.

## Context forking / cancellation scope

The `context.Context` will be forked in the SQL executor for each statement, and the cancellation handle
will be stored inside the `queryMeta` struct for that query. Any `CANCEL QUERY` statements directed at that
query will close that context's `Done` channel, which would error out any RPCs being made for that query.
The error would propagate to the SQL executor which would then rollback the current KV transaction, and
mark the SQL transaction as aborted. The client's connection will _not_ be closed.

In short, cancellation will be implemented at the statement level, with cancelled statements also rolling back
the associated transaction.

The executor would re-throw a more user-friendly "Execution cancelled upon user request" error instead of
the "Context cancelled" one.

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
+---------+----------+-----------------------------------------------------------+
| node_id | id       | query                                                     | 
+---------+----------+-----------------------------------------------------------+
| 1       | 1:43872  | SHOW CLUSTER QUERIES                                      |
| 2       | 2:11     | SELECT * FROM students WHERE long_bio LIKE '%ips%or';     |
+---------+----------+-----------------------------------------------------------+
(2 rows)

root@:26257/> CANCEL QUERY '2:11';
CANCEL QUERY

root@:26257/> SELECT node_id, id, query FROM [SHOW CLUSTER QUERIES];
+---------+----------+-----------------------------------------------------------+
| node_id | id       | query                                                     | 
+---------+----------+-----------------------------------------------------------+
| 1       | 1:43872  | SHOW CLUSTER QUERIES                                      |
+---------+----------+-----------------------------------------------------------+
(1 row)
```

Going back to session 1:

```sql
root@:26257/> SELECT * FROM students WHERE long_bio LIKE '%ips%or';
pq: query cancelled by user 'root'

root%:26257/>
```

## Phase 2: DistSQL cancellation

For now, any cancels done to queries that have `distributed=true` in the `SHOW QUERIES` output
would not be supported, and any `CANCEL QUERY` statements directed at such a query would error out
without cancelling anything. Before distSQL cancellation is implemented, this RFC will be updated to
describe any distSQL-specific implementation details.

# Drawbacks

## Ambiguity around late cancellations

We would not have an uncancellable phase for queries; so a cancellation received
after a non-read-only query's `EndTransaction` would cause the query to error out (from a client's
perspective) but still have otherwise persisted any writes.

The cancellation code could potentially check if the transaction has been marked as committed,
and if it has, then the `CANCEL QUERY` statement itself could error out - effectively
making that phase of the query non-cancellable.

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
