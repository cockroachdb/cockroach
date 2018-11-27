- Feature Name: Support `SELECT FOR UPDATE`
- Status: postponed
- Start Date: 2017-10-17
- Authors: Rebecca Taft
- RFC PR: [#19577](https://github.com/cockroachdb/cockroach/pull/19577)
- Cockroach Issue: [#6583](https://github.com/cockroachdb/cockroach/issues/6583)

# Summary

This RFC is postponed because it seems that, given CockroachDB's model of concurrency control, 
it is not possible to implement the functionality that users would expect for `SELECT ... FOR UPDATE`.
None of the implementation alternatives we have examined would fully replicate the semantics that Postgres
provides, and there is a risk that customers would try to use the feature without fully
understanding the pitfalls. The details are described below in the [Drawbacks](#drawbacks) and
[Alternatives](#alternatives) sections. We may revisit this feature if there is sufficient demand from
customers, or if we can prove that there is a significant benefit to using `SELECT ... FOR UPDATE`
for certain applications.

Original Summary: Support the `SELECT ... FOR UPDATE` SQL syntax, which locks rows returned by the `SELECT`
statement. This pessimistic locking feature prevents concurrent transactions from updating
any of the locked rows until the locking transaction commits or aborts. This is useful for 
enforcing consistency when running in `SNAPSHOT` mode, and may be useful for avoiding deadlocks
when running in `SERIALIZABLE` mode. Several potential customers have asked for this feature,
and it would also get us closer to feature parity with Postgres. The proposed implementation is
to set row-level "dummy" intents by transforming the `SELECT ... FOR UPDATE` query tree to
include an `updateNode`.

# Motivation

As described in [Issue #6583](https://github.com/cockroachdb/cockroach/issues/6583),
`SELECT ... FOR UPDATE` is not standard SQL, but many databases now support it, including
Postgres. Thus the primary motivation for this feature is compatibility with existing code. 
Several third party products such as the [Quartz Scheduler](http://www.quartz-scheduler.org),
[OpenJPA](http://openjpa.apache.org) and [Liquibase](http://www.liquibase.org)
also rely on this feature, preventing some potential customers from switching to CockroachDB.

In some cases, `SELECT ... FOR UPDATE` is required to maintain correctness when running CockroachDB
in `SNAPSHOT` mode. In particular, `SELECT ... FOR UPDATE` can be used to prevent write skew anomalies.
Write skew anomalies occur when two concurrent transactions read an overlapping set of
rows but update disjoint sets of rows. Since the transactions each operate on private snapshots of
the database, neither one will see the updates from the other.

The [Wikipedia entry on Snapshot Isolation](https://en.wikipedia.org/wiki/Snapshot_isolation) has a
useful concrete example:

> ... imagine V1 and V2 are two balances held by a single person, Phil. The bank will allow either
V1 or V2 to run a deficit, provided the total held in both is never negative (i.e. V1 + V2 ≥ 0).
Both balances are currently $100. Phil initiates two transactions concurrently, T1 withdrawing $200
from V1, and T2 withdrawing $200 from V2. .... T1 and T2 operate on private snapshots of the database:
each deducts $200 from an account, and then verifies that the new total is zero, using the other account
value that held when the snapshot was taken. Since neither update conflicts, both commit successfully,
leaving V1 = V2 = -$100, and V1 + V2 = -$200.

It is possible to prevent this scenario from happening in `SNAPSHOT` mode by using `SELECT ... FOR UPDATE`.
For example, if each transaction calls something like:
`SELECT * FROM accounts WHERE acct_name = 'V1' OR acct_name = 'V2' FOR UPDATE` at the start of the transaction,
one of the transactions would be blocked until the "winning" transaction commits and releases the locks.
At that point, the "losing" transaction would be able to see the update from the winner, so it would not deduct
$200. Therefore, using `SELECT ... FOR UPDATE` lets users obtain some of the performance benefits of using `SNAPSHOT`
isolation instead of `SERIALIZABLE`, without paying the price of write skew anomalies.

`SELECT ... FOR UPDATE` is not needed for correctness when running in `SERIALIZABLE` mode, 
but it may still be useful for controlling lock ordering and avoiding deadlocks. For example,
consider the following schedule:

```
T1: Starts transaction
T2: Starts transaction
T1: Updates row A
T2: Updates row B
T1: Wants to update row B (blocks)
T2: Wants to update row A (deadlock)
```

This sort of scenario can happen in any database that tries to maintain some level of correctness. 
It is especially common in databases that use pessimistic two-phased locking (2PL) since transactions
must acquire shared locks for reads in addition to exclusive locks for writes. But deadlocks like the
one shown above also happen in databases that use MVCC like PostgreSQL and CockroachDB, since writes must
acquire locks on all rows that will be updated. Postgres, CockroachDB, and many other systems detect deadlocks by
identifying cycles in a "waits-for" graph, where nodes represent transactions, and directed edges represent
transactions waiting on each other to release locks (or write intents). In Postgres, if a cycle (deadlock) is detected,
transactions will be selectively aborted until the cycle(s) are removed. CockroachDB forces one of the transactions
in the cycle to be "pushed", which generally has the effect of aborting at least one transaction. CockroachDB
can perform this detection and push almost instantaneously after the conflict happens, so "deadlocks" in CockroachDB
are less disruptive than in Postgres, where deadlock detection can take up to a second. Some other systems
use a timeout mechanism, where transactions will abort after waiting a certain amount of time to acquire a lock.
In all cases, the deadlock causes delays and/or aborted transactions.

`SELECT ... FOR UPDATE` will help avoid deadlocks by allowing transactions to acquire all of their locks
(lay down intents in CockroachDB) up front. For example, the above schedule would change to the following:

```
T1: Starts transaction
T2: Starts transaction
T1: Locks rows A and B
T1: Updates row A
T2: Wants to lock rows A and B (blocks)
T1: Updates row B
T1: Commits
T2: Locks rows A and B
T2: Updates row B
T2: Updates row A
T2: Commits
```

Since both transactions attempted to lock rows A and B at the start of the transaction, the deadlock was prevented. 
Acquiring all locks (or laying down write intents) up front allows the database to lock rows in a consistent order
(even if they are updated in a different order), thus preventing deadlocks. 

Many implementations of this feature also include options to control whether or not to wait on locks.
`SELECT ... FOR UPDATE NOWAIT` is one option, which causes the query to return an error if it is
unable to immediately lock all target rows. This is useful for latency-critical situations,
and could also be useful for auto-retrying transactions in CockroachDB. `SELECT ... FOR UPDATE SKIP LOCKED`
is another option, which returns only the rows that could be locked immediately, and skips over the others.
This option returns an inconsistent view of the data, but may be useful for cases when multiple
workers are trying to process data in the same table as if it were a queue of tasks. 
The default behavior of `SELECT ... FOR UPDATE` is for the transaction to block if some of the
target rows are already locked by another transaction. Note that it is not possible to use the
`NOWAIT` and `SKIP LOCKED` modifiers without `FOR { UPDATE | SHARE | ... }`.

The first implementation of `FOR UPDATE` in CockroachDB will not include `NOWAIT` or `SKIP LOCKED` options.
It seems that some users want these features, but many would be satisfied with `FOR UPDATE` alone.
As of this writing we are not aware of any commonly used third-party products that use these options.

# Guide-level explanation

The [Postgres Documentation](https://www.postgresql.org/docs/current/static/sql-select.html#sql-for-update-share)
describes this feature as it is supported by Postgres. As shown, the syntax of the locking clause has the form

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
waiting on intents. Using `FOR UPDATE` will result in laying intents on the rows returned by the `SELECT`
query. Note that it only lays intents for rows that already exist; preventing inserts matching
the `SELECT` query is not desired. As described above, this feature alone is useful because it helps
maintain correctness when running CockroachDB in `SNAPSHOT` mode (avoiding write skew), and serves
as a tool for optimization (avoiding deadlocks) when running in `SERIALIZABLE` mode.

For example, consider the following transaction:

<a name="employees_transaction"></a>
```
BEGIN;

SELECT * FROM employees WHERE name = 'John Smith' FOR UPDATE;

...

UPDATE employees SET salary = 50000 WHERE name = 'John Smith';

COMMIT;
```

This code will lay intents on the rows of all employees named John Smith at the beginning of the transaction,
preventing other concurrent transactions from simultaneously updating those rows.
As a result, the `UPDATE employees ...` statement at the end of the transaction will not need
to lay any additional intents. Note that `FOR UPDATE` will have no effect if it is used in a
stand-alone query that is not part of any transaction.

One important difference between CockroachDB and Postgres relates to transaction priorities. In CockroachDB,
if there is a conflict and the second transaction has a higher priority, the first transaction will be
pushed out of the way -- even if it has laid down intents already. This would apply to transactions with
`SELECT ... FOR UPDATE`, just as it would for any other transaction.

# Reference-level explanation

This section provides more detail about how and why the CockroachDB implementation of
the locking clause will differ from Postgres.

With the current model of CockroachDB, it is not possible to support the locking strengths
`NO KEY UPDATE` or `KEY SHARE` because
these options require locking at a sub-row granularity. It is also not clear that CockroachDB can support
`SHARE`, because there is currently no such thing as a "read intent". `UPDATE` can be supported by
marking the affected rows with dummy write intents.

By default, if `FOR UPDATE` is used in Postgres without specifying tables
(without the `OF table_name [, ...]` clause),
Postgres will lock all rows returned by the `SELECT` query. The `OF table_name [, ...]` clause
enables locking only the rows in the specified tables. To lock different tables with different
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
it. To avoid spending too much time on this, as mentioned above, we will probably just implement the most
basic functionality in which clients use `FOR UPDATE` to lay intents on the rows returned by the query.
Initially we won't include the `SKIP LOCKED` or `NOWAIT` options, but it may be worth implementing
these at some point.

At the moment `FOR UPDATE` is disabled for use in views (there will not be an error, but it will
be ignored). This is similar to the way `ORDER BY` and `LIMIT` are handled in views. See comment from @a-robinson in
[data_source.go:getViewPlan()](https://github.com/cockroachdb/cockroach/blob/5a6b4312a972b74b0af5de53dfdfb204dc0fd6d7/pkg/sql/data_source.go#L680).
As described in the comment, the outer `Select` AST node is currently being stripped out of the view plan.
If `ORDER BY` and `LIMIT` are enabled later by including the entire `Select`, `FOR UPDATE` would come for free.
Postgres supports all of these options in views, since it supports any `SELECT` query, and re-runs
the query each time the view is used. CockroachDB should do the same.

With the (temporary) exception of views, `FOR UPDATE` should propagate throughout the query plan
as expected. If `FOR UPDATE` only occurs in a subquery, the rows locked are those returned by the subquery
to the outer query (unless the optimizer reduces the amount of data scanned in the subquery). For example,
`SELECT * FROM employees e, (SELECT * FROM departments FOR UPDATE) d WHERE e.did = d.id`
would only lock rows in the departments table. If the `FOR UPDATE` occurs on the outer `SELECT`, however,
all rows returned by the query will be locked.

If the number of intents exceeds `maxIntents` as defined in `txn_coord_sender.go`
(default 100,000), the transaction will be rejected. This is similar to the way updates work,
and will prevent users from using `SELECT FOR UPDATE` with queries that would result in a full table
scan. If we need to reduce this number later we can add a new setting just for `SELECT FOR UPDATE`.

One issue that is not discussed in detail in the Postgres documentation is the order in which
locks are acquired. Acquiring locks in a consistent order is an important tool to prevent deadlocks
from occuring. For the CockroachDB implementation of `FOR UPDATE`, we will not implement the feature
in DistSQL at this time (DistSQL does not currently support writes), so our implementation
will most likely produce a consistent ordering of write intents (we currently have no evidence
to the contrary). It is difficult to guarantee a particular ordering, however, since the implementation
of the local execution engine may change in the future to take advantage of parallel processing. Likewise,
if `FOR UPDATE` is supported later in DistSQL, ordering will be more difficult to guarantee.

## Detailed design

There are a number of changes that will need to be implemented in the SQL layer in order to
support `FOR UPDATE`.

- Update the parser to support the syntax in `SELECT` statements.
- Add checks to ensure that `FOR UPDATE` cannot be used with `GROUP BY`, `HAVING`, `WINDOW`, `DISTINCT`,
  `UNION`, `INTERSECT`, `EXCEPT`, or in contexts where returned rows cannot be clearly identified with
  individual table rows; for example it cannot be used with aggregation. Postgres also disallows
  `FOR UPDATE` with all of these query types.
- Possibly add additional checks for the first implementation to limit `SELECT FOR UPDATE` queries to
  "simple" queries where the primary key must be used in the `WHERE` clause, and the predicate must be either `==` or `IN`.
- Modify the query tree so that the top level `renderNode` is transformed to an `updateNode` which sets each selected
  column value to itself and returns the selected rows (e.g., `UPDATE table SET a = a RETURNING a`). If needed,
  a `sortNode` and/or `limitNode` can be applied above the `updateNode` in the query tree.

Note that none of this design involves DistSQL. At the moment DistSQL does not support writes,
so it would not make sense to support `FOR UPDATE`.

## <a name="drawbacks"></a>Drawbacks

We have decided to postpone this feature because it is not clear that the proposed solution
(or any of the alternatives discussed below) would meet customer needs.

Some potential customers (such as those using the Quartz Scheduler) use `SELECT FOR UPDATE`
as if it were an advisory lock; they never actually update the rows selected by the `SELECT FOR UPDATE`
statement, so the only purpose of the lock is to prevent other concurrent transactions from accessing some 
shared resource during the transaction. Although the proposed solution of laying intents
will achieve this isolation, it will result in many aborted transactions. For example, consider a
transaction `T1` with a lower timestamp than another transaction `T2`. If `T1` tries to access rows
already marked with an intent by `T2`, `T1` will block until `T2` commits or aborts, and then `T1` will
abort with a retryable error. Most existing codebases using `SELECT FOR UPDATE` are probably not equipped
to handle these retryable errors because other databases would not cause transactions to abort in this scenario.

The key motivation described above for using `SELECT FOR UPDATE` in `SERIALIZABLE` mode is to control
lock ordering so as to minimize transaction aborts. Due to the way concurrency control works in
CockroachDB, however, `SELECT FOR UPDATE` is still likely to cause many aborts (as described in the previous
paragraph). Postgres also recommends against using this feature in `SERIALIZABLE` mode, since it is not necessary
for correctness, and can degrade performance by causing disk accesses to set locks. Additionally, although `FOR UPDATE`
can prevent deadlocks if used judiciously, it can also cause deadlocks if not every transaction
sets intents in the same order. Since `FOR UPDATE` results in transactions setting more intents
for a longer period of time, the chance of collision is higher. There may be some benefit to using `FOR UPDATE`
with high-contention workloads since it would cause conflicting transactions to fail earlier and do less work
before aborting, but we would need to run some tests to validate this (perhaps by running benchmarks
with high-contention workloads). If we do eventually implement this feature, we should probably discourage customers
from using `FOR UPDATE` in `SERIALIZABLE` mode unless they have good reasons to use it.

It's also not clear that this feature is worth implementing for use in `SNAPSHOT` mode.
As mentioned above, the primary motivation for this feature is compatibility with existing code.
Simply running in `SERIALIZABLE` mode is (probably) a better way of avoiding write skew than `SNAPSHOT`
+`FOR UPDATE`.

Given the above drawbacks, we have decided it is not worth the effort to implement `SELECT FOR UPDATE` at
this time. But if we do implement it in the future, one way to make sure customers avoid the pitfalls
is to create an "opt-in" setting for using this feature. By default, using `FOR UPDATE` would throw an
error and direct users to view the documentation. Only users who explicitly opt in by updating their cluster
or session settings would be able to use the feature. We could also add an option to either use the feature
with intents or as a no-op (see the [Isolation Upgrade](#no-op) alternative below). Adding options adds
complexity, however.

If we implement this feature, we should probably start with only `FOR UPDATE`, and not
include other features (e.g., `NOWAIT`, `SKIP LOCKED`, etc). It will be a lot of work to implement
all of the features supported by Postgres, and it is probably not worth our time since it's not clear
these features will actually get used. We can easily add them later if needed.

In the mean time, customers who really need explicit locking functionality can emulate the
feature by executing something like `UPDATE x SET y=y WHERE y=y`. Executing this command at
the start of a transaction would effectively set intents on all of the rows in table `x`.

## <a name="alternatives"></a>Rationale and Alternatives

The proposed solution is to "lock" rows by writing dummy write intents on each row as part
of an update operation. However, there are a couple of alternative implementations worth
considering, specifically row-level intents set as part of a scan operation,
range-level intents, and isolation ugrade.

### Row-level intents set during scan

Laying intents as part of an `UPDATE` operation could be expensive for simple `SELECT FOR UPDATE`
queries since it requires multiple requests to the KV layer. The first KV request performs the scan
operation, and subsequent requests update each row to lay an intent. For simple queries, a less
expensive approach would be to lay intents directly during the scan operation.

This approach has a few downsides, however. First, it would be more work to implement since 
it would require updates to the KV API to include new messages (e.g., ScanForUpdate and ReverseScanForUpdate).
These new messages would require updates to the KV and storage layers to mimic processing of Scan and ReverseScan
and set dummy write intents on every row touched.

As described in [issue #6583](https://github.com/cockroachdb/cockroach/issues/6583), 
> Implementing this would touch a lot of different parts of the code. No part is individually too
tricky, but there are a lot of them (`git grep -i reversescan` will give you an idea of the scope).
The bulk of the changes would consist of implementing new ScanForUpdate and ReverseScanForUpdate
calls in the KV API. These would work similarly to regular scans, but A) would be flagged as read/write
commands instead of read-only and B) after performing the scan, they'd use MVCCPut to write back the
values that were just read (that's not the most efficient way to do things, but I think it's the right
way to start since it will have the right semantics without complicating the backwards-compatibility
story). Then the SQL layer would use these instead of plan Scan/ReverseScan when `FOR UPDATE` has been
requested.

There was some discussion in the issue about whether we really needed new API calls, but
the consensus was that making it possible to write on `Scan` requests would make debugging a nightmare.

This approach would also require a change in the SQL layer to handle the case when a `SELECT FOR UPDATE`
query would only scan a secondary index and not touch the primary key (PK) index. In this case,
we would need to implicitly modify the query plan to add a join with the PK index so that intents
would always be laid on the PK index. This would ensure that the `SELECT FOR UPDATE` query would prevent
concurrent transactions from updating the corresponding rows, since updates must always lay
intents on the PK index.

Another downside is that in many cases this approach would set intents on more rows than returned by the
`SELECT`, since most predicates are not applied until after the scan is completed. This should not affect
correctness in terms of consistency or isolation, but could affect performance if there is high contention.
For example, if the first `SELECT` statement in the [employees transaction shown above](#employees_transaction)
were `SELECT * FROM employees WHERE name like '%Smith' FOR UPDATE;`, CockroachDB would set intents on
all of the rows in the `employees` table because it's not possible to determine from the predicate
which key spans are affected. This lack of precision would be an issue for any predicate that
does not directly translate to particular key spans. Furthermore, since this translation may not be obvious
to users, they could easily write queries that would result in full-table scans by accident.

In contrast, Postgres (generally) locks exactly the rows returned by the query, and no more. There are a few
examples given in the [documentation](https://www.postgresql.org/docs/current/static/sql-select.html#sql-for-update-share)
where that's not the case. For example, `SELECT ... LIMIT 5 OFFSET 5 FOR UPDATE` may
lock up to 10 rows even though only 5 rows are returned. 

### Range-level intents

One alternative to row-level intents is to set an intent on an entire Range if the `SELECT` statement
would return the majority of rows in the range. This is similar to the approach
suggested by the [Revert Command RFC](https://github.com/cockroachdb/cockroach/pull/16294).

The advantage of setting intents on entire ranges is that it would significantly improve the performance
for large scans compared to setting intents on individual rows. The downside is that
this feature is not yet implemented, so it would be significantly more effort than using 
simple row-level intents. (It was actually deemed to be too complex and all-interfering in the
[Revert Command RFC](https://github.com/cockroachdb/cockroach/pull/16294), which is why that
RFC is now closed.) It's also not clear that customers would use `FOR UPDATE` with 
large ranges, so this may be an unneeded performance optimization. Furthermore, 
setting an intent on the entire range based on the predicate could result in "locking" rows that should
not be observable by the `SELECT`. For instance, if the transaction performing the
`SELECT FOR UPDATE` query over some range is at a lower timestamp than a later `INSERT`
within that range, the `FOR UPDATE` should not apply to the newly written row.
This issue is probably not any worse than the other problems with locking precision described above,
though.

One advantage of implementing range-level intents is that we could reuse this feature
for other applications such as point-in-time recovery. The details of the proposed implementation
as well as other possible applications are described in the [Revert Command RFC](https://github.com/cockroachdb/cockroach/pull/16294).
However, in the interest of getting something working sooner rather than later,
I believe row-level intents make more sense at this time.

### <a name="no-op"></a>Isolation upgrade

Another alternative approach is to avoid setting any intents whatsoever. Instead, the `SELECT FOR UPDATE`
would be a no-op for `SERIALIZABLE` transactions, and the database would automatically upgrade the isolation
of a `SNAPSHOT` transaction to `SERIALIZABLE` when a `SELECT FOR UPDATE` is used. The advantage of this
approach is its simplicity: the work required to support this feature would be minimal. Additionally,
it would avoid the issues of lock precision and poor performance that exist in the other proposed solutions.
Furthermore, it would successfully prevent write skew in
`SNAPSHOT` transactions, which is a key reason many customers might want to use `SELECT FOR UPDATE`
in the first place.

The downside is that it may not be what our customers expect. It would not allow customers to control
lock ordering, which as described above is one feature of `SELECT FOR UPDATE` that savvy users can employ to
prevent deadlocks and other conflicts. Some users may also have reason to lock rows that they don't intend to
update, and they will have no way to do that with this solution.

### Alternatives to FOR UPDATE

So far this RFC has assumed that we want to implement some form of the `FOR UPDATE`
SQL syntax. However, there are many other types of locks provided by Postgres, and
it's possible that one of these other options would be a better choice for our customers.

1. <b>Table Level Locks:</b>
   Postgres provides eight different types of table level locks: `ACCESS SHARE, ROW SHARE,`
   `ROW EXCLUSIVE, SHARE UPDATE EXCLUSIVE, SHARE, SHARE ROW EXCLUSIVE, EXCLUSIVE,` and `ACCESS EXCLUSIVE`.
   These locks can be acquired explicity using the `LOCK` command, but they are also implicitly
   acquired by different SQL statements. For example, a `SELECT` command (without `FOR UPDATE`)
   implicitly acquires the `ACCESS SHARE` lock on every table it accesses. This prevents concurrent
   calls to `DROP TABLE, TRUNCATE,` etc. on the same table, since those commands must acquire
   an `ACCESS EXCLUSIVE` lock, wchich conflicts with `ACCESS SHARE`. See the
   [PG docs](https://www.postgresql.org/docs/current/static/explicit-locking.html#locking-tables) 
   for the full list of conflicts.
2. <b>Row Level Locks:</b>
   There are four different types of row level locks: `UPDATE, NO KEY UPDATE, SHARE, KEY SHARE`.
   As described above, these can be acquired explicitly with the `SELECT ... FOR locking_strength`
   command. This RFC has been focused on `SELECT ... FOR UPDATE`, but the other three options
   are available in Postgres to allow more concurrent accesses. The `UPDATE` (or under certain
   circumstances `NO KEY UPDATE`) locks are also acquired implicitly by `UPDATE` and `DELETE` commands.
   See the [PG docs](https://www.postgresql.org/docs/current/static/explicit-locking.html#locking-rows)
   for more details.
3. <b>Advisory Locks:</b>
   Advisory locks are used to lock application-level resources, identified either by a single 64-bit key
   value or two 32-bit key values. It is up to the application programmer to ensure that locks are
   acquired and released at the correct points in time to ensure application-level resources are
   properly protected. Postgres provides numerous options for advisory locks. They can be:
   - Session-level (must be explicitly unlocked) or transaction-level (will be unlocked automatically at commit/abort)
   - Shared or exclusive
   - Blocking or non-blocking (e.g. `pg_try_advisory_lock` is non-blocking)

   All of the different advisory lock functions are listed in the [PG docs](https://www.postgresql.org/docs/current/static/functions-admin.html#functions-advisory-locks)

There are valid arguments for using each of these different lock types in different applications.
However, I do not think that either table-level locks or advisory locks will be a good
substitue for our customers that require explicit row-level locks.
Table-level locks are not a good substitue for `FOR UPDATE` and the other row-level locks
because they are too coarse-grained and will cause unnecessary performance degradation.
Advisory locks place too much responsibility on the application developer(s) to ensure that the
appropriate lock is always acquired before accessing a given row. This doesn't mean we
shouldn't support advisory locks, but I don't think we should force developers to use them
in place of explicit row-level locks.
