- Feature Name: Syntax for pausing, resuming, and canceling operations
- Status: completed
- Start Date: 2017-06-01
- Authors: Nikhil Benesch, Jordan Lewis, Bilal Akhtar
- RFC PR: [#16273]
- Cockroach Issue: None

# Motivation

For the 1.1 release, we're adding support for canceling SQL queries and for
pausing, resuming, and canceling long-running jobs (i.e., backups, restores, and
schema changes).

We draw a distinction between queries and jobs, both in internal implementations
and in user-facing interfaces, because queries and jobs have vastly different
expected durations. Queries are expected to run for milliseconds or seconds, or
minutes in the worst case. Jobs are expected to run for minutes or hours, or
days in the worst case.

This difference is more significant than in might seem on a first pass. Queries
are short-lived enough that they benefit from an RPC-based cancel mechanism.
Jobs can spare the latency of waiting for gossip or polling a system table.
CockroachDB already takes advantage of this difference: the list of running
queries exists only in memory on each node, while the list of running jobs is
stored and replicated in a proper SQL table, `system.jobs`.

Still, we want the syntax to cancel a query to be similar to the syntax to
cancel a job because, conceptually, the operations are identical.

# Detailed design

We propose the following syntax:

```sql
PAUSE JOB <job-id>;
RESUME JOB <job-id>;
CANCEL [QUERY|JOB] <job-or-query-id>;
```

This syntax is symmetric with the soon-to-be-merged syntax for listing running
queries, `SHOW QUERIES`, and a soon-to-be-proposed syntax for listing running
jobs, `SHOW JOBS`.

These statements will all complete as soon as the pause, resume, or cancel
request has been submitted. For example, when the `CANCEL` statement completes,
the operation in question may still be in progress. Users can detect when the
operation has fully canceled by polling `SHOW QUERIES` or `SHOW JOBS`.

# Drawbacks

This is a lot of new SQL syntax with no direct basis in another SQL dialect.

# Alternatives

## M{Y,S}SQL syntax

MySQL and MSSQL use a similar syntax, but with `KILL` in place of `CANCEL`:

```sql
KILL <id>;
```

MySQL additionally supports modifiers (`KILL CONNECTION <connection-id>` and
`KILL QUERY <query-id>`), much like we do with `QUERY` and `JOB`.

We prefer `CANCEL` to `KILL`, as "kill" implies the query or job will be
terminated immediately. This will not be true of our cancel implementations. In
particular, large schema changes may take several hours to cancel as the cluster
rolls back all backfill progress.

## Postgres syntax

Postgres (ab)uses a built-in function:

```sql
SELECT pg_cancel_backend(<query-id>);
SELECT pg_cancel_backend(generate_series) FROM generate_series(1, 100000); -- Yes, this is valid.
```

Postgres's approach avoids introducing new syntax... by introducing the world's
largest side effect. Pausing, resuming, and canceling are novel operations that
seem like they *should* have new syntax.

Alas, we may eventually end up supporting this syntax for compatibility's sake.

## Oracle syntax

Oracle uses a hybrid approach and nests new syntax under an `ALTER SYSTEM`
command:

```sql
ALTER SYSTEM KILL SESSION <session-id>;
```

We have no precedent for `ALTER SYSTEM`, and it doesn't seem worth introducing
`ALTER SYSTEM` just to avoid three new top-level constructions (`PAUSE...`,
`RESUME...`, `CANCEL...`). We already have precedent for new top-level syntax
for new operations, like `BACKUP...` and `RESTORE...`.

[Peter Mattis suggested][peter-suggestion] a slight variation on
the Oracle syntax, `ALTER JOB...`/`ALTER QUERY...`, but the RFC authors feel the
proposed top-level `PAUSE`/`RESUME`/`CANCEL` verbs read more naturally.

# Unresolved questions

None. How these operations will actually be implemented is out of scope.

[#16273]: https://github.com/cockroachdb/cockroach/pull/16273
[peter-suggestion]: https://github.com/cockroachdb/cockroach/pull/16273#issuecomment-305590661
