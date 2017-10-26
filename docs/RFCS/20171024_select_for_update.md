- Feature Name: Support `SELECT FOR UPDATE`
- Status: draft
- Start Date: 2017-10-17
- Authors: Rebecca Taft
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [#6583](https://github.com/cockroachdb/cockroach/issues/6583)

# Summary

Support the `SELECT ... FOR UPDATE` SQL syntax, which locks rows returned by the `SELECT`
statement.  Several potential customers have asked for this feature, and it would 
also get us closer to feature parity with Postgres.  The easiest way to implement this 
in CockroachDB would be by setting row-level "dummy" intents, but another option
would be to set span-level locks.

# Motivation

As described in [this issue](https://github.com/cockroachdb/cockroach/issues/6583),
`SELECT ... FOR UPDATE` is not standard SQL, but many databases now support it, including
Postgres.  Several third party products such as the [Quartz Scheduler](http://www.quartz-scheduler.org),
[OpenJPA](http://openjpa.apache.org) and [Liquibase](http://www.liquibase.org)
also rely on this feature, preventing some potential customers from switching to CockroachDB.

In some cases, `SELECT ... FOR UPDATE` is required to maintain correctness when running CockroachDB
in `SNAPSHOT` mode.  `SELECT ... FOR UPDATE` is essentially a no-op when running in `SERIALIZABLE` mode, 
but it may still be useful for controlling lock ordering and avoiding deadlocks.

Many implementations of this feature also include options to control whether or not to wait on locks.
`SELECT ... FOR UPDATE NOWAIT` is one option, which causes the query to return an error if it is
unable to immediately lock all target rows. This is useful for latency-critical situations,
and could also be useful for auto-retrying transactions in CockroachDB. `SELECT ... FOR UPDATE SKIP LOCKED`
is another option, which returns only the rows that could be locked immediately, and skips over the others.
This option returns an inconsistent view of the data, but may be useful for cases when multiple
workers are trying to process data in the same table as if it were a queue of tasks.  
The default behavior of `SELECT ... FOR UPDATE` is for the transaction to block if some of the
target rows are already locked by another transaction.  Note that it is not possible to use the
`NOWAIT` and `SKIP LOCKED` modifiers without `FOR { UPDATE | SHARE | ... }`.

# Guide-level explanation

The [Postgres Documentation](https://www.postgresql.org/docs/current/static/sql-select.html#sql-for-update-share)
describes this feature as it is supported by Postgres.  As shown, the syntax of the locking clause has the form

```
FOR lock_strength [ OF table_name [, ...] ] [ NOWAIT | SKIP LOCKED ]
```

where `lock_strength` can be one of

```
UPDATE
NO KEY UPDATE
SHARE
KEY SHARE
```

For our initial implementation in CockroachDB, we will likely simplify this syntax to

```
FOR UPDATE
```

i.e., no variation in locking strength, no specified tables, and no options for avoiding
waiting on locks.  Using `FOR UPDATE` will result in locking the rows returned by the `SELECT` query
with exclusive locks.  As described above, this feature alone is useful because it helps
maintain correctness when running CockroachDB in `SNAPSHOT` mode (avoiding write skew), and serves
as a tool for optimization when running in `SERIALIZABLE` mode.

For example, consider the following transaction:

```
BEGIN;

SELECT * FROM employees WHERE name = 'John Smith' FOR UPDATE;

...

UPDATE employees SET salary = 50000 WHERE name = 'John Smith';

COMMIT;
```

will lock the rows of all employees named John Smith at the beginning of the transaction.
As a result, the `UPDATE employees ...` statement at the end of the transaction will not need
to acquire any additional locks.  Note that `FOR UPDATE` will have no effect if it is used in a
stand-alone query that is not part of any transaction.

# Reference-level explanation

This section provides more detail about how and why the CockroachDB implementation of
the locking clause will differ from Postgres.

With the current model of CockroachDB, it is not possible to support the locking strengths
`NO KEY UPDATE` or `KEY SHARE` because
these options require locking at a sub-row granularity.  It is also not clear that CockroachDB can support
`SHARE`, because there is currently no such thing as a "read intent".  `UPDATE` can be supported by
marking the affected rows with dummy write intents.

By default, if `FOR UPDATE` is used in Postgres without specifying tables
(without the `OF table_name [, ...]` clause),
Postgres will lock all rows returned by the `SELECT` query.  The `OF table_name [, ...]` clause
enables locking only the rows in the specified tables.  To lock different tables with different
strengths or different options, Postgres users can string multiple locking clauses together.  
For example,

```
SELECT * from employees e, departments d, companies c
WHERE e.did = d.id AND d.cid = c.id
AND c.name = `Cockroach Labs`
FOR UPDATE OF employees SKIP LOCKED
FOR SHARE OF departments NOWAIT
```
locks rows in the `employees` table that satisfy the join condition with an exclusive lock,
and skips over rows that are already locked by another transaction.
It also locks rows in the `departments` table that satisfy the join condition with a shared lock,
and returns an error if it cannot lock all of the rows immediately. It does not lock
the `companies` table.

Implementing this flexibility in CockroachDB for use of different tables and different options
may be excessively complicated, and it's not clear that our customers actually need
it.  To avoid spending too much time on this, as mentioned above, we will probably just implement the most
basic functionality in which clients use `FOR UPDATE` to lock the rows returned by the query.
Initially we won't include the `SKIP LOCKED` or `NOWAIT` options, but it may be worth implementing
these at some point.

At the moment it is not possible to use `FOR UPDATE` in views (there will not be an error, but it will
be ignored).  This is similar to the way `ORDER BY` and `LIMIT`  are handled in views. See comment from
@a-robinson in [data_source.go:getViewPlan()](https://github.com/cockroachdb/cockroach/blob/5a6b4312a972b74b0af5de53dfdfb204dc0fd6d7/pkg/sql/data_source.go#L680). If `ORDER BY` and `LIMIT` are supported later, `FOR UPDATE` would come for free.

Another potential difference between the Postgres and CockroachDB implementations
relates to the precision of locking.  In general, Postgres locks exactly the rows returned
by the query, and no more.  There are a few examples given in the
[documentation](https://www.postgresql.org/docs/current/static/sql-select.html#sql-for-update-share)
where that's not the case.  For example, `SELECT ... LIMIT 5 OFFSET 5 FOR UPDATE` may
lock up to 10 rows even though only 5 rows are returned.  It may be more difficult to
be precise with locking in CockroachDB, since locking happens at the KV layer, and
predicates may be applied later.  This should not affect correctness, but could affect
performance if there is high contention.

## Detailed design

There are a number of changes that will need to be implemented in order to
support `FOR UPDATE`.

- Update the parser to support the syntax in `SELECT` statements.
- Update the KV API to include new messages ScanForUpdate and ReverseScanForUpdate
- Update the KV and storage layers to mimic processing of Scan and ReverseScan
  and set dummy write intents on every row touched.

As described by Ben in [issue #6583](https://github.com/cockroachdb/cockroach/issues/6583), 
`git grep -i reversescan` provides an idea of the scope of the change. The bulk of the changes
would consist of implementing new ScanForUpdate and ReverseScanForUpdate calls in the KV API. 
These would work similarly to regular scans, but A) would be flagged as read/write commands
instead of read-only and B) after performing the scan, they'd use MVCCPut to write back the
values that were just read (that's not the most efficient way to do things, but Ben thinks it's
the right way to start since it will have the right semantics without complicating the
backwards-compatibility story). Then the SQL layer would use these instead of plan
Scan/ReverseScan when FOR UPDATE has been requested.

There was some discussion about whether we really needed new API calls, but Ben pointed out
that making it possible to write on `Scan` requests would make debugging a nightmare.

## Drawbacks

It seems that there is sufficient demand for <i>some</i> form of the `FOR UPDATE` syntax.  However,
there are pros and cons to implementing or not implementing certain features.

- It will be a lot of work to implement all of the features supported by Postgres.  
  This is probably not worth our time since it's not clear these features will actually get used.
  We can easily add them later if needed.
- If we use the approach of setting intents on every row returned by the `SELECT`, it
  could hurt performance if we are selecting a large range.  But setting intents on every
  row will be easier to implement than the alternatives discussed below.

## Rationale and Alternatives

The proposed solution is to lock rows by writing dummy write intents on each row.
However, another alternative is to lock an entire Range if the `SELECT` statement
would return the majority of rows in the range.  This is similar to the approach
suggested by the [Revert Command RFC](https://github.com/cockroachdb/cockroach/pull/16294).

The advantage of locking entire ranges is that it would significantly improve the performance
for large scans compared to setting intents on individual rows.  The downside is that
this feature is not yet implemented, so it would be significantly more effort than using 
simple row-level intents.  It's also not clear that customers would use `FOR UPDATE` with 
large ranges, so this may be an unneeded performance optimization.  Furthermore, 
locking the entire range based on the predicate could result in locking rows that should
not be observable by the `SELECT`. For instance, if the transaction performing the
`SELECT FOR UPDATE` query over some range is at a lower timestamp than a later `INSERT`
within that range, the `FOR UPDATE` lock should not apply to the newly written row.
This issue is probably not any worse than the other problems with locking precision described above,
though.

One advantage of implementing range-level locking is that we could reuse this feature
for other applications such as point-in-time recovery.  The details of the proposed implementation
as well as other possible applications are described in the [Revert Command RFC](https://github.com/cockroachdb/cockroach/pull/16294).
However, in the interest of getting something working sooner rather than later,
I believe row-level intents make more sense at this time.

## Unresolved questions

- Should we move forward with implementing only `FOR UPDATE` at first,
  or are some of the other features essential as well?
- Should we stick with row-level intents or implement range-level locking?
- Should we be concerned about locking precision (i.e., lock exactly the 
  rows returned by query and no more)?
