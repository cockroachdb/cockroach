- Feature Name: SQL step-wise execution
- Status: in-progress
- Start Date: 2019-10-14
- Authors: andrei knz nathan
- RFC PR: [#42864](https://github.com/cockroachdb/cockroach/pull/42864)
- Cockroach Issue:
  [#28842](https://github.com/cockroachdb/cockroach/issues/28842)
  [#33473](https://github.com/cockroachdb/cockroach/issues/33473)
  [#33475](https://github.com/cockroachdb/cockroach/issues/33475)

# Summary

This RFC proposes to introduce *sequence points* around and inside the
execution of SQL statements, so that all KV reads performed after a
sequence point observe the data at the time the sequence point was
established.

This change intends to solve issue
[#28842](https://github.com/cockroachdb/cockroach/issues/28842) which
is a serious—and classical—semantic error that can cause business
problems in customer applications, see
https://en.wikipedia.org/wiki/Halloween_Problem for details.

It's also an area where CockroachDB diverges from PostgreSQL, so
fixing this would provide a modicum of additional compatibility.

The proposed solution capitalizes on the *KV sequence numbers*
previously introduced at the KV and storage levels for an unrelated
reason (txn performance optimizations) and can be summarized as
"annotate every read request with the current read seqnum" on the
common path, with the new `Step()` API to copy the current write
seqnum as new current read seqnum.

# Motivation

See above.

# Guide-level explanation

After this change, the "read" portion of SQL mutations operate using a
snapshot of the database as per the moment the statment started. In
particular it cannot see its own writes.

This guarantees e.g. that a statement like `INSERT INTO t SELECT *
FROM t` always terminates, or that `UPDATE t SET x = x+2` never
operates on the same row two times.

# Reference-level explanation

The following API is added to `TxnCoordSender` and `*client.Txn`:

```go
   // Step creates a sequencing point in the current transaction. A
   // sequencing point establishes a snapshot baseline for subsequent
   // read operations: until the next sequencing point, read operations
   // observe the data at the time the snapshot was established and
   // ignore writes performed since.
   //
   // Before the first step is taken, the transaction operates as if
   // there was a step after every write: each read to a key is able to
   // see the latest write before it. This makes the step behavior
   // opt-in and backward-compatible with existing code which does not
   // need it.
   // The method is idempotent.
   Step() error

   // DisableStepping disables the sequencing point behavior and
   // ensures that every read can read the latest write. The
   // effect remains disabled until the next call to Step().
   // The method is idempotent.
   DisableStepping() error
```

- `Step()` is called every time a SQL execution step is reached:
  - before the execution of each regular statement;
  - at the end of the mutations, before and in-between the FK and cascading action phases, if any.
- the implementation of `Step()` forwards the call to the RootTxn's
  TxnCoordSender `Step()` method (new), which in turn saves the current
  write seqnum as reference seqnum for future reads.
- `DisableStepping()` is merely a convenience, but is provided
  for use with the execution part of all the DDL statements.
  These currently contain many internal steps where each
  internal step observes the results of steps prior. Without
  `DisableStepping()`, we would need to add calls to `Step()`
  throughout, which would be error-prone.

## Detailed design

The design is prototyped here:

https://github.com/cockroachdb/cockroach/pull/42854

https://github.com/cockroachdb/cockroach/pull/42862

Additional topics of interest:

- [Uniqueness violations](#Uniqueness-violations)
- [ON CONFLICT processing](#ON-CONFLICT-processing)
- [FK existence checks under a single mutation](#FK-existence-checks-under-a-single-mutation)
- [FK cascading actions under a single mutation](#FK-cascading-actions-under-a-single-mutation)
- [Multiple mutations with CTEs](#Multiple-mutations-with-CTEs)
- [Schema changes](#Schema-changes)

### Uniqueness violations

There are really two cases:

- we insert/modify a single row, and doing so creating a duplicate of
  a row that was modified in a previous statement (or sequencing
  step). This case is simple and transparently handled by "read at
  seqnum of previous step".

- we insert/modify the same row two times inside the same mutation
  statement, or two rows such that they are duplicate according to
  some unique index.
  Here the problem is seemingly that the 2nd row update will not
  see the first.

However, when looking more closely there is no new problem here.

All writes to a unique index go through a KV `CPut` on the uniqueness key.
By ensuring that `CPuts` read their _condition_ at the current write
seqnum, we can always pick up the latest write and detect duplicates.

(CPut will still skip over ignored / rolled back seqnums like other KV
ops. It's only the target read seqnum that's ratcheted up to the
present for CPut, in contrast to other mvcc ops that will be blocked by
the configured target read seqnum.)

This opens a question of whether we need a variant of CPut which does
not do this. TBD. (Initial analysis says no.)

### ON CONFLICT processing

Question arises of what to do when the result of ON CONFLICT
processing changes a row in a read-modify-write fashion. For example:

```sql
INSERT INTO t(x) VALUES (1), (1) ON CONFLICT(x) DO UPDATE SET x = t.x + excluded.x
--                      ^^^^^^^^ notice the dup row
```

Here conceptually the INSERT suggests that the 2nd ON CONFLICT resolution
will observe the row as it was left over by the 1st. This would not work
with "read at seqnum of previous statement".

The answer here is from a previous discussion around mutations that
observed the following:

- postgres does not support updating the same row two times in an ON
  CONFLICT clause.

- it is desirable to batch / pre-compute the ON CONFLICT result values
  concurrently with the mutation for performance, and we've already
  established back in 2018 that the lack of support for updating the
  same row twice in pg makes this optimization possible.

- the implementation was updated when bringing this logic under the CBO

From here, it follows that we don't care about "read at seqnum"
inconsistencies as the current CBO logic already assumes that it's
fine to miss earlier conflict resolutions.

### FK existence checks under a single mutation

FK existence checks must observe the data values post-mutation. For
this we need to introduce a sequence point (call to `Step()`) between
the end of the "run" phase (where results were produced for the
client) and the FK existence checks.

This way the reads for FK existence checks can see all the writes by
the mutation.

Note that today FK existence checks are "interleaved" with mutations
on the common path, which is a useful optimization but incorrect in
some cases. This will need to be adjusted. See the following issue for details:
https://github.com/cockroachdb/cockroach/issues/33475

In 19.2/20.1 there is a new notion of "post-queries" which the CBO is
increasingly using to perform FK checks and cascading actions. These
benefit simply by adding a sequence point before the execution of each
post-query.

### FK cascading actions under a single mutation

Postgres uses post-statement triggers to process FK cascading actions
and existence checks. Cascading actions that result in mutations to
other tables themselves append more triggers to run.

Each subsequent step in this cascade of effects is able to read its
own writes (for futher FK checks).

We emulate this in CockroachDB by introducing a step boundary between
iterations of the cascading algorithm.

### Multiple mutations with CTEs

It's possible for a single statement to define multiple mutations for example:

```sql
WITH
  a AS (INSERT ... RETURNING ...),
  b AS (INSERT ... RETURNING ...)
  SELECT ...
```

PostgreSQL does not guarantee that the effect of one mutation is
visible to another, or even to the later read-only parts of the
statement. In fact it requires that all mutations operate
on the same data at the beginning of the statement:

More specifically: https://www.postgresql.org/docs/12/queries-with.html

> The sub-statements in WITH are executed concurrently with each other
> and with the main query. Therefore, when using data-modifying
> statements in WITH, the order in which the specified updates
> actually happen is unpredictable. **All the statements are executed
> with the same snapshot (see Chapter 13), so they cannot “see” one
> another's effects on the target tables.** This alleviates the effects
> of the unpredictability of the actual order of row updates, and
> means that RETURNING data is the only way to communicate changes
> between different WITH sub-statements and the main query.

So with the logic proposed so far, all the mutations inside the same
statement execute from the same read seqnum.

If there is FK work to be done, the first sequencing step necessary
for FK checks (to advance the read seqnum) will only occur after all
mutations have completed.

(The observations from [Uniqueness violations](#Uniqueness-violations) above apply here as well.)

### Schema changes

Schema changers that operate synchronously operate "under the sequence
point" and need no further adjustment.

Schema changers that operate asynchronously already operate under
independent `*client.Txn` instances and are thus unaffected.


## Drawbacks

None known.

## Rationale and Alternatives

No alternative was evaluated.

## Unresolved questions

None known.
