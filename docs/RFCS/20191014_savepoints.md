- Feature Name: SQL savepoints
- Status: draft
- Start Date: 2019-10-14
- Authors: andrei knz, with technical input from nathan tbg lucy radu
- RFC PR: [#41569](https://github.com/cockroachdb/cockroach/pull/41569)
- Supporting tech note: [#42116](https://github.com/cockroachdb/cockroach/pull/42116) or [here](../tech-notes/txn_coord_sender.md) after this PR merges.
- Cockroach Issue: [#10735](https://github.com/cockroachdb/cockroach/issues/10735) [#28842](https://github.com/cockroachdb/cockroach/issues/28842)

Table of contents:

- [Summary](#Summary)
- [Motivation](#Motivation)
- [Guide-level explanation](#Guide-level-explanation)
- [Reference-level explanation](#Reference-level-explanation)
- [Drawbacks](#Drawbacks)
- [Rationale and Alternatives](#Rationale-and-Alternatives)
- [Unresolved questions](#Unresolved-questions)

# Summary

This RFC proposes to implement SQL savepoints as supported by PostgreSQL.

Savepoints enable a client to partially roll back a transaction.

This is a feature that is often requested by users, and used
prominently in the test suites of 3rd party tools, in particular
ORMs. We want to enable these test suites so as to increase our
compatibility coverage.

The addition of SQL savepoints is enabled by recent changes to the
KV layer, whereby write intents now preserve the history of sequence
numbers that wrote to a key throughout a transaction.

# Motivation

SQL savepoints are prominently used by 3rd party tools and frameworks.
They are key to implement nested transactions, which is a common
programming idiom from Java and other object-oriented languages.

# Guide-level explanation

A savepoint is a special mark inside a transaction that allows all
commands that are executed after it was established to be rolled back,
restoring the transaction state to what it was at the time of the
savepoint.

See also: https://www.postgresql.org/docs/current/sql-savepoint.html

- [Syntax and introduction](#Syntax-and-introductory-example)
- [Nesting](#Nesting-and-example)
- [Savepoint name scoping](#Savepoint-name-scoping)
- [Multiple-level commit/rollback](#Multi-level-commit-rollback)
- [Behavior in case of errors](#Behavior-in-case-of-errors)
- [Relationship with client-side retries](#Relationship-with-client-side-retries)

## Syntax and introductory example

- to establish a savepoint inside a transaction: `SAVEPOINT savepoint_name`

  The usual PostgreSQL identifier rules apply: `SAVEPOINT foo` and
  `SAVEPOINT Foo` define the same savepoint, whereas `SAVEPOINT "Foo"`
  defines another.

- to roll back a transaction partially to a previously established
  savepoint: `ROLLBACK TO SAVEPOINT savepoint_name`

- to forget a savepoint, and keep the effects of statements executed
  after the savepoint was established: `RELEASE
  SAVEPOINT savepoint_name`

For example:

```sql
BEGIN;
    INSERT INTO table1 VALUES (1);
    SAVEPOINT my_savepoint;
    INSERT INTO table1 VALUES (2);
    ROLLBACK TO SAVEPOINT my_savepoint;
    INSERT INTO table1 VALUES (3);
COMMIT;
```

The above transaction will insert the values 1 and 3, but not 2.

## Schema changes under savepoints

Schema changes and other DDL are supported "under" savepoints and can
be partially rolled back without rolling back the entire transaction.

For example:

```sql
BEGIN;
  CREATE TABLE u(x INT);

  SAVEPOINT foo;
  CREATE TABLE t(x INT);
  INSERT INTO t(x) VALUES (1);
  ROLLBACK TO SAVEPOINT foo;

  INSERT INTO u(x) VALUES(1);

  SAVEPOINT bar;
  CREATE TABLE t(x TEXT);
  RELEASE SAVEPOINT foo;
  INSERT INTO t(x) VALUES ('a');
COMMIT;
```

This inserts 1 into u and 'a' into t. The table `t` with an INT column
does not exist after the transaction commits.

Note that the ordering of DDL vs DML statements remain restricted as
per previously, none of the [known
limitations](https://www.cockroachlabs.com/docs/v19.1/online-schema-changes.html#no-schema-changes-within-transactions)
are lifted in this work.

## Nesting

Savepoints can be nested.

For example:

```sql
BEGIN;
    INSERT INTO table1 VALUES (1);
    SAVEPOINT my_savepoint;
    INSERT INTO table1 VALUES (2);
    SAVEPOINT my_savepoint2;
    INSERT INTO table1 VALUES (3);
    ROLLBACK TO SAVEPOINT my_savepoint2;
    INSERT INTO table1 VALUES (4);
    RELEASE my_savepoint;
COMMIT;
```

This inserts values 1, 2 and 4 but not 3.

Changes partially committed by a savepoint release can be rolled back by an outer savepoint.

For example:

```sql
BEGIN;
    INSERT INTO table1 VALUES (1);
    SAVEPOINT my_savepoint;
    INSERT INTO table1 VALUES (2);
    SAVEPOINT my_savepoint2;
    INSERT INTO table1 VALUES (3);
    RELEASE SAVEPOINT my_savepoint2;
    ROLLBACK TO SAVEPOINT my_savepoint;
COMMIT;
```

This inserts only value 1. The value 3 is rolled back alongside 2.

## Savepoint name scoping

As an extension to the SQL standard, PostgreSQL allows a
`SAVEPOINT` statement to shadow an earlier savepoint with the same
name. The name refers to the new savepoint until released/rolled back,
after which the name reverts to referring to the previous savepoint.

For example:

```sql
BEGIN;
    INSERT INTO table1 VALUES (1);
    SAVEPOINT my_savepoint;
    INSERT INTO table1 VALUES (2);
    SAVEPOINT my_savepoint;
    INSERT INTO table1 VALUES (3);
    ROLLBACK TO SAVEPOINT my_savepoint;
    INSERT INTO table1 VALUES (4);
    RELEASE SAVEPOINT my_savepoint;
COMMIT;
```

This inserts values 1, 2 and 4, but not 3.

## Multi-level commit/rollback

`RELEASE SAVEPOINT` and `ROLLBACK TO SAVEPOINT` can refer to
a savepoint "higher" in the nesting hierarchy. When this occurs, all
the savepoints "under" the nesting are automatically released/rolled
back too.

For example:


```sql
BEGIN;
    SAVEPOINT foo;
    INSERT INTO table1 VALUES (1);
    SAVEPOINT bar;
    INSERT INTO table1 VALUES (2);
    RELEASE SAVEPOINT foo;
COMMIT;
```

This inserts both 1 and 2.

```sql
BEGIN;
    SAVEPOINT foo;
    INSERT INTO table1 VALUES (1);
    SAVEPOINT bar;
    INSERT INTO table1 VALUES (2);
    ROLLBACK TO SAVEPOINT foo;
COMMIT;
```

This inserts nothing: both inserts are rolled back.

```sql
BEGIN;
    SAVEPOINT foo;
    SAVEPOINT bar;
    ROLLBACK TO SAVEPOINT foo;
    RELEASE SAVEPOINT bar; -- error: savepoint "bar" does not exist
COMMIT;
```

This demonstrates that the name "bar" is not visible after it was rolled back over.

## Savepoints and prepared statements

Prepared statements (`PREPARE`/`EXECUTE`) are not transactional and
thus prepared statements are not invalidated upon savepoint
rollbacks:

```
BEGIN;
SAVEPOINT foo;
PREPARE a AS SELECT 1;
ROLLBACK TO SAVEPOINT foo;
EXECUTE a; -- this is valid
```

## Behavior in case of errors

If a SQL error occurs "under" a savepoint, it is possible to recover
an open, "healthy" txn by rolling back the savepoint without rolling back the txn.
(An exception to this is discussed below.)

For example:

```
kena=> create table u(x int unique);
CREATE TABLE

kena=> insert into u(x) values(1);
INSERT 0 1

kena=> begin; \
  savepoint foo; \
  insert into u(x) values (1); \
  rollback to savepoint foo; \
  insert into u(x) values (2); \
  commit;

BEGIN
SAVEPOINT
ERROR:  duplicate key value violates unique constraint "u_x_key"
DETAIL:  Key (x)=(1) already exists.
ROLLBACK
INSERT 0 1
COMMIT

kena=> select * from u;
 x
---
 1
 2
(2 rows)
```

(i.e. the second insert succeeds even though the first insert encountered an error)

In the first implementation, an exception to this will be the handling
of retry errors. These will not be cancellable with a savepoint rollback.
Instead, a client-side retry loop must be implemented using the
regular mechanism (see next section).


## Relationship with client-side retries

The following remains from previous versions:

- Generally, a transaction must be aborted and started from scratch if
  a **retry error** is encountered.
- Some amount of extra performance can be gained in this process
  by using crdb's bespoke "client-side retry protocol":

  - the first statement after `BEGIN` must be `SAVEPOINT cockroach_restart`.
  - if a retry error is encountered, the client can run `ROLLBACK TO SAVEPOINT cockroach_restart` and perform the work again.
  - at the end of a txn, the client must run `RELEASE SAVEPOINT cockroach_restart` before they issue `COMMIT`,
    so as to enable a retry attempt in case there is an end-of-txn retry error.

This behavior remains and thus creates a restriction on the use of
non-retry savepoints:

- only savepoints with a name starting with the special prefix
  `cockroach_restart` (including `cockroach_restart` itself but also
  `cockroach_restart123134` etc) are considered a marker for
  client-side retries.
- restart savepoints can only be used at the outmost level of nesting,
  i.e. `begin; savepoint cockroach_restart; savepoint foo` is OK, but
  `begin; savepoint foo; savepoint cockroach_restart` is not.

There is a glimmer of hope for an implementation that is able to
handle retries at the level of a single savepoint (the innermost one) but
this would be more complex and is left out of scope for this implementation.

(more restrictions TBD as informed by implementation)

# Reference-level explanation

- [Savepoint and rollback semantics](#Savepoint-and-rollback-semantics)
- [Layering of work](#Layering-of-work)
- [Technical milestones](#Technical-milestones)
- [Design overview](#Design-overview)
- [Bill of work (preliminary)](#Bill-of-work-preliminary)
- [SQL executor changes](#SQL-executor-changes)
- [Savepoints and schema changes](#Savepoints-and-schema-changes)
- [Savepoint rollbacks, MVCC and storage](#Savepoint-rollbacks-MVCC-and-storage)
- [TxnCoordSender changes](#TxnCoordSender-changes)

## Savepoint and rollback semantics

We propose to:

- manage the stack of valid savepoints in the SQL executor,
- extend the KV interface with the ability to checkpoint KV-level transactions
  and rewind a KV txn to a previous checkpoint. The SQL executor
  would map SQL savepoints internally to KV checkpoints.

Savepoint releases, expected to be the most common operations, are
treated as no-op as far as KV is concerned (we don't want I/O churn on
the common path). The SQL executor merely checks that the release is
semantically valid at that point and discards the savepoint state.

Savepoint rollbacks need to consider all the types of side effects performed
by a client during a txn:

1. KV writes. These must be rewound.
2. The txn's persistent "aborted" state. This must be set back to "open". An obstacle to this state transition must block the savepoint rollback.
3. The txn's in-flight "retryable" state upon retry errors. If set, this must block the savepoint rollback unless the savepoint is a retry savepoint (`cockroach_restart` established at the start of the txn).
4. Postgres portals. This must be precisely rewound.
5. Schema changes. This must be partially rewound, or, failing a correct implementation, block rolling back a savepoint.
6. Jobs creation for transactional DDL. This must be partially rewound, or, failing a correct implementation, block rolling back a savepoint.
7. Namespace cache. This can be either partially or fully evicted.
8. Descriptor cache. This can be either partially or fully evicted.
9. Query plan cache. This can be either partially or fully evicted.

Additionally:

- Prepared statements are not transactionals in pg's dialect and thus
  not affected by savepoint rollbacks.
- Session variables, as currently implemented by CockroachDB, are
  equivalent to pg's "global scope" where they are non-transactional
  and thus unaffected by savepoint rollbacks. CockroachDB does not yet
  support `SET LOCAL` which makes a session var transactional. If/when
  this is supported, it must be brought "under" the savepoint
  mechanism.
- non-DDL Bulk I/O statements (`BACKUP` etc) are non transactional
  so their jobs don't need to be cancelled upon savepoint rollbacks.


## Layering of work

We propose 6 levels of savepoint compatibility, to be
implemented/introduced gradually into CockroachDB. They differ
in how flexible they make `ROLLBACK TO SAVEPOINT`:

- L1: rewind KV writes without resuming over errors
- L2: rewind over SQL logical errors only
- L3: rewind over simple KV errors including CPut errors (duplicate checks)
- L4: rewind over DDL statements that don't create jobs.
- L5: rewind over DDL statements that creates jobs.
- L6: rewind over retry errors that are not aborts.

We are aiming for L1-L3 in CockroachDB 20.1, with L4 as stretch goal.

The following table indicates when `ROLLBACK TO SAVEPOINT` is accepted/rejected:

| Situation                                                                       | L1                   | L2                             | L3                                           | L4                                           | L5                         | L6                                |
|---------------------------------------------------------------------------------|----------------------|--------------------------------|----------------------------------------------|----------------------------------------------|----------------------------|-----------------------------------|
| Txn state "open", read-only so far                                              | ok, rewind KV writes | ditto L1                       | ditto L1                                     | ditto L1                                     | ditto L1                   | ditto L1                          |
| after `SELECT nonexistent` or other SQL-only logical error                      | unsupported          | same as above + back to "open" | ditto L2                                     | ditto L2                                     | ditto L2                   | ditto L2                          |
| after `INSERT INTO v(x) VALUES (1)` causes dup error                            | unsupported          | unsupported                    | same as above + reset client.Txn error state | ditto L3                                     | ditto L3                   | ditto L3                          |
| after `ALTER DATABASE x RENAME TO y` or `CREATE TABLE` (no index etc)           | unsupported          | unsupported                    | unsupported                                  | same as above + rewind sc.chg + flush caches | ditto L4                   | ditto L4                          |
| after `CREATE INDEX`, `ALTER ADD COLUMN`                     etc.               | unsupported          | unsupported                    | unsupported                                  | unsupported                                  | same as above + cancel job | ditto L5                          |
| after `SELECT crdb_internal.force_retry('1s')` or txn pushes or "write too old" | unsupported          | unsupported                    | unsupported                                  | unsupported                                  | unsupported                | same as above + rewind client.Txn |

## Technical milestones

L1: implement rewinding KV writes: extend the KV/SQL interface with
savepoints and seqnum ignore lists. Reject rollbacks in SQL if txn
currently in (logical) aborted state, or if there are pending schema
changes, or new jobs. Ensure that pgwire portals are properly rolled
back.

L2: change the SQL executor state machine to enable a new transition
from "aborted" to "open". Only allow rollbacks if the client.Txn
reports that it is not in error (i.e. the error is SQL-only).

L3: whitelist certain KV errors and call a (new) method on client.Txn
to reset the error state.

L4: relax the condition introduced in L1 to allow DDL "under" a
savepoint being rolled back. Ensure that the various caches
(namespace, descriptors, query plans) are flushed properly.

L5: relax the condition introduced in L1 to allow DDL jobs "under" a
savepoint being rolled back. Cancel+rewind the jobs. If/when a
job cannot be rewound, refuse the savepoint rollback.

L6: re-architecture the error handling in client.Txn and the
TxnCoordSender. Re-architecture the executor state machine to allow
retry loops in any nested txn, not just at the top level.

## Design overview

The overall design can be understood as follows:

- at the SQL/KV interface, KV operations are associated with *sequence numbers* (seqnums):
  - write operations generate new seqnums, which are stored inside write intents
  - the txn object also contains a list of sequence numbers (or ranges
    thereof) that are rolled back and are to be ignored during reads.
  - read operations operate "at" a particular seqnum- a MVCC read that
    encounters an intent ignores the values written at later seqnums
    and returns the most recent value at that seqnum instead, also
    substracting any past value at seqnums marked to be ignored due to
    rollbacks.
  - intent resolution also throws away any values inside the rolled
    back seqnum ranges.

- outside of SQL, reads operate at the seqnum of the latest write as
  is done today; this preserves read-your-own-writes even inside
  individual KV batches

- inside SQL, the executor configures the sql/kv interface so that all
  reads performed on behalf of an "execution step" (usually, a single
  statement) are performed at the seqnum of the last write in the
  previous step.
  - most statements consist of just one step, whereby the seqnum
    boundary is in-between statements. However, with CTE mutations, FK
    checks and cascading updates, there may be multiple steps inside a
    single complex statement.
  - for CPuts specifically, the condition part ignores the configured
    read seqnum and checks the most recent (non rolled back) write
    instead. [This is needed in mutations.](#Savepoints-and-SQL-mutations)

- a savepoint is defined by:
  - when adding a savepoint, by saving the current write seqnum,
  - to roll back a savepoint, by marking the seqnums in-between as
    rolled back inside the txn object, to be ignored during reads (see
    above).
  - savepoint release is a no-op in kv, and simply updates a name-seqnum mapping inside SQL.

- conceptually, savepoints define a stack-like structure. Any
  savepoint has at most one savepoint open "under" it. When releasing
  or rolling back to a savepoint higher in the stack, all the
  savepoints in-between are also released or rolled back.
  In the SQL executor, this is managed using a stack-like data structure.

- to introduce savepoint semantics in schema updates / DDL, the
  descriptor caches are extended to store the seqnum alongside the
  cached descs. Cache entries are not considered/evicted if the
  seqnum has been rolled back.

## SQL executor changes

To support nesting and shadowing, the SQL executor maintains a naming
environment: a list of mappings from names to savepoints.

In Go, using  `[]struct{name,sp}` or equivalent.
(We do not use a simple `map` because savepoints form a stack.)

The most recent, innermost savepoint information is at the end of the
list.

When defining a savepoint, a new entry is appended. When releasing or
rolling back a savepoint, the latest mapping for that sp name is
removed:

- Trying to release or rollback a savepoint using a name that does not
  exist in that list results in an error 3B001 "savepoint does not
  exist".
- When the target of a RELEASE is an entry that's earlier than the
  latest entry, all the entries from that entry to the last are also
  popped out and released.
- Similarly, if the target of a ROLLBACK is an earlier entry,
  all the entries in-between are also removed from the environment
  and (conceptually) rolled back.
- (TBD:) uses of savepoint syntax with `cockroach_restart` special names
  first verify that the stack of savepoints is empty. If it is not,
  a usage/syntax/unimplemented error is produced.

The executor also provides a method `Step()` on the execution context
(likely `planner` or whatever is most relevant at the time of
implementation):

- this is called every time a SQL execution step is reached:
  - at the end of execution of each regular statement
  - after each top-level mutation CTE in a multi-mutation statement
  - in-between the FK and cascading action phases, if any
- the implementation of `Step()` forwards the call to the RootTxn's
  TxnCoordSender `Step()` method (new), which in turn saves the current
  write seqnum as reference seqnum for future reads.

## Savepoints and SQL mutations

At a [high level](#Design-overview), mutations read at the seqnum of
the previous statement and write at new seqnums. In particular, they
don't read their own writes and this solves issue
[#28842](https://github.com/cockroachdb/cockroach/issues/28842).

This raises a couple "interesting" topics:

- [Uniqueness violations](#Uniqueness-violations)
- [ON CONFLICT processing](#ON-CONFLICT-processing)
- [FK existence checks under a single mutation](#FK-existence-checks-under-a-single-mutation)
- [FK cascading actions under a single mutation](#FK-cascading-actions-under-a-single-mutation)
- [Multiple mutations with CTEs](#Multiple-mutations-with-CTEs)

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
this we introduce a [step boundary](#SQL-executor-changes) between the
end of the "run" phase (where results were produced for the client)
and the FK existence checks.

This way the reads for FK existence checks can see all the writes by
the mutation.

### FK cascading actions under a single mutation

(To be checked)

Postgres uses post-statement triggers to process FK cascading actions
and existence checks.  Cascading actions that result in mutations to
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

## Savepoints and schema changes

Background:

- descriptors used in a txn are loaded in a desc cache (TableCollection + db desc cache)
  that's (currently) invalidated at the end of the txn
- DDL statements work as follows:
  - they modify the descriptor by adding "pending mutation" records and persist
    the descs embedding these records using the current KV txn context
  - additional DDL may add more mutations and perform further KV updates
  - at the end of a SQL txn, _after_ the KV txn commits, a "schema changer" job/process
    is kicked off to resolve the mutation entries on descriptors. The
	mutation records are read using AS OF SYSTEM TIME with the KV commit timestamp
	and the final desc post-mutations is written back to kv (possibly at a later timestamp).

To implement SQL savepoints:

- the descriptor caches must be invalidated when a savepoint is rolled
  back. To achieve this:
  - we extend the caches to store the savepoint object
    (write seqnum) under which a descriptor was loaded (= the most recent
    savepoint that's still open).
  - when releasing a savepoint, we re-annotate all the cached descriptors
    cached "under" the savepoint(s) being released, to become cached
	"under" the savepoint that's still open. (Alternatively, we could
	flush the desc caches entirely. This is simpler but perf impact unknown. TBD.)
  - when rolling back a savepoint, we evict entries from the cache
    that correspond to a seqnum being rolled back.

- we let the further processing of DDL statements proceed using KV
  operations as usual. We expect that the rest of the KV/SQL semantics
  will make schema changes "just work":

  - pending mutation records are cancelled/rolled back as any other KV write
  - when the KV txn commits, only the KV writes corresponding to non-rolledback
    savepoints have been persisted, so any DDL changes "under" savepoints
	become invisible.
  - the schema changer task that starts after that will thus only
    see the remaining (committed/released) mutation records and the
	DDL semantics should "just work".

  (This will be validated by experimentation in a prototype).

## Savepoint rollbacks, MVCC and storage

Background:

- for a given KV pair, any write by a txn is done via a write intent
- if there is another write inside the txn, we _append_ the new write
  to the intent already laid. After two writes, we'd have separately:
  - two seqnums
  - two values
  - two mvcc timestamps

We need to keep both values around even without savepoints because we
may reorder KV reads and writes. For example, in the sequence
write1-read1-write2, if the read1 operation happens to be processed
after write2, we still want it to see only write1 that is logically in
its past.

The way this works today is that each KV operation also has a metadata
field which tells it "at which seqnum" it should read.  The MVCC read
logic, when it encounters an intent, scans the entries in the intent
from last to first, and ignores all writes performed at a later
seqnum.

For savepoints, this needs to be extended as follows:

- the txn metadata, which is available in the context of every MVCC
  read, is extended by a new field "list of ignored seqnum ranges".

- when a MVCC reads finds an intent and scans to find the first
  value that's no later than the current read seqnum, it
  should also skip over any value written at a seqnum
  also in the ignore list.

Savepoint rollbacks are implemented by populating the ignore list
with the range of seqnums generated from the point the savepoint
was last established, to the point of the rollback.

This storage-specific part of the work is described in this issue:
https://github.com/cockroachdb/cockroach/issues/41612

## TxnCoordSender changes

- Background: [Txn interface between SQL and KV](#Background-Txn-interface-between-SQL-and-KV)
- [Overview of TxnCoordSender changes](#Overview-of-TxnCoordSender-changes)
  - [Seqnums and savepoints](#Seqnums-and-savepoints)
  - [Error handling changes](#Error-handling-changes)
- [SQL / KV API definition](#SQL--KV-API-definition)


### Background: Txn interface between SQL and KV

This RFC assumes understanding from the TxnCoordSender tech note at
[#42116](https://github.com/cockroachdb/cockroach/pull/42116) or
[here](../tech-notes/txn_coord_sender.md) after this PR merges.

### Overview of TxnCoordSender changes

- [Seqnums and savepoints](#Seqnums-and-savepoints)
- [Error handling changes](#Error-handling-changes)

#### Seqnums and savepoints

On the "all-is-well" path (no errors) we want to associate SQL
savepoints with seqnums, and clean up the read-your-own-writes
semantics of SQL mutations.

The code changes will introduce the following principles *for SQL
transactions* (non-SQL txns are unaffected):

1) *reads will always be performed at the seqnum
  of the latest sequencing point* (and not just the latest KV write
  performed). Sequencing points are introduced by a new TCS `Step()`
  method called by SQL on the RootTxn where relevant:
  - in-between statements
  - in between the mutation and FK check / cascading actions

   This method takes the latest write seqnum generated before it was
   called and copies it to become the read seqnum for every read
   performed thereafter.

2) there cannot be in-flight LeafTxns active when the `Step()` method
   is called, so that it's always clear which last invocation
   of `Step()` any given LeafTxn refers to.

3) a *SQL savepoint token* is a copy of the seqnum captured by the
   last call to `Step()`, together with the minimal additional state
   sufficient to partially roll back the txn (this is discussed
   further below).

4) a SQL savepoint release checks and reports any currently deferred
   error (see tech note for definition, e.g. txn push or WriteTooOld).

5) a SQL savepoint rollback is implemented as a `SavepointRollback()` method
   in the TCS, which takes as argument the SQL savepoint token
   where to rollback, computes the range of seqnums from the target savepoint to the
   current last generated seqnum, and then populates the current txn
   object to mark this entire range as rolled back.  (This
   information will live in the transaction record and be available
   through `TxnMeta` to every KV op in MVCC, which need it.)

6) there cannot be any in-flight LeafTxn active when a savepoint is
   rolled back, so that the list of ignored seqnum ranges can never
   change "under" a LeafTxn concurrently.

The SQL executor is responsible for organizing SQL execution so as to
prevent LeafTxns existing concurrently between the special `Step()`
and `SavepointRollback()` operations.

#### Error handling changes

[See the section of the tech note for background information](../../tech-notes/txn_coord_sender.md#Error-handling-in-TxnCoordSender).

| Error kind                                | Prior situation                                                             | New (proposed) situation                                    |
|-------------------------------------------|-----------------------------------------------------------------------------|-------------------------------------------------------------|
| recoverable errors with in-place recovery | Auto-retry/adjust internal to TCS, txn object remains live, no client error | (unchanged)                                                 |
| recoverable errors with txn restart       | txn object re-init with epoch bump, retry error                             | unchanged, but see (2) below                                |
| deferred retry errors (eg WriteTooOld)    | error stored, temporarily hidden from client, re-reported during commit     | unchanged, but see (3) below                                |
| transient processing errors               | TCS trashed + txn aborted                                                   | TCS + txn remain active, no txn state change, see (1) below |
| transaction aborts                        | TCS trashed + txn aborted                                                   | (unchanged)                                                 |
| unhandled errors                          | TCS trashed + txn aborted                                                   | (unchanged)                                                 |

1) avoid trashing the TCS when innocuous KV errors happen

   The first and main change is to relax the notion of "unrecoverable
   error" (see tech note for a definition).

   Today, transient processing errors like reading from a historical
   ts that's been GCed, an invalid CPut condition, etc all cause the
   TCS to move to the "txnError" state after which no operation is
   ever possible any more. The RFC proposes to change this so that
   only internal assertion errors cause a TCS to become fully
   invalidated.

   Instead, KV errors like CPut condition errors will simply generate an
   error object (as they already do) and nothing else, and this error
   will flow back to SQL where it can be dealt with as usual.

   *The new behavior is that it will be possible to continue issuing
   KV requests via the TCS after such an error occurs.*

2) The main change from the [Seqnums and savepoints
   section](#Seqnums-and-savepoints) above decouples the seqnum
   increments from epoch increments. This means that the "internal"
   error handling performed by TCS on retry errors will not invalidate
   seqnum ranges (and rolled back seqnum ranges after SQL savepoint
   rollbacks).

   *The new behavior is that it becomes possible to recover from a
   retry error (other than WriteTooOld, see point 3 below) using a SQL
   savepoint rollback.*

3) any deferred error (currently, just `WriteTooOld`) is deferred merely
   to the first next savepoint RELEASE. If present it is reported then.
   If a client opens a new savepoint while there is a pending WriteTooOld
   error, that error state is preserved in the savepoint token and
   restored when the savepoint is rolled back.

   (TBD: whether this is correct/desirable. There is discussion about
   whether WriteTooOld is still handled this way at all.)

From the perspective of SQL clients:

| Error type                                | Prior situation                                         | New (proposed) situation                  |
|-------------------------------------------|---------------------------------------------------------|-------------------------------------------|
| transaction aborts                        | no recovery possible                                    | (unchanged)                               |
| transient processing errors               | no recovery possible                                    | can be recovered using savepoint rollback |
| unhandled errors                          | no recovery possible                                    | (unchanged)                               |
| recoverable errors with in-place recovery | automatic recovery, invisible to client                 | (unchanged)                               |
| recoverable errors with txn restart       | retry error, must start from beginning of txn           | can be recovered using savepoint rollback |
| deferred retry errors (eg WriteTooOld)    | error reported during commit, entire txn can be retried | can be recovered using savepoint rollback |

### SQL / KV API definition

- `GetSavepoint() (SavepointToken, error)` method on TCS and
  `*client.Txn`, returns a `SavepointToken`, to be used exclusively on
  RootTxns without LeafTxns active.

  1) new `GetSavepoint()` method on TCS.

     Initially:

     ```
     type SavepointToken struct {
        SeqNum                   enginepb.TxnSeq
        Timestamp                hlc.Timestamp
        RefreshedTimestamp       hlc.Timestamp
        OrigTimestampWasObserved bool
        InFlightWriteIndex       int   // see explanation below
        Epoch                    int32 // (may be removed entirely)
    }
    ```

  2) conceptually asks all interceptors to "build a savepoint" although
     first implementation will be as simple as getting the current
     counter value in the `txnSeqNumAllocator`.

  3) new `GetSavepoint()` on `TxnSender` interface

  4) new `GetSavepoint()` on `client.Txn`, forwards to the `TxnSender`.

- `RollbackToSavepoint(SavepointToken) error` method on TCS
  and `*client.Txn`, to be used exclusively on RootTxns
  without LeafTxns active.

  1) new `RollbackToSavepoint()` method on TCS.

  2) performs the seqnum invalidation in the txn record, as described
     above. Restores the additional state. In particular, the in
     flight write slice (`InFlightWrites` in the `roachpb.Transaction`
     record) is truncated to the position indicated by
     `InFlightWriteIndex`).

  3) new `RollbackToSavepoint()` method on `TxnSender` interface

  4) new `RollbackToSavepoint()` method on `client.Txn`, forwards to `TxnSender`.

# Drawbacks

This feature introduces more complexity in the SQL executor.

# Rationale and Alternatives

(to be populated)

## General implementation direction

There are two general design directions:

1. mark certain ranges of sequence numbers as "rolled back" in the
   transaction record itself, to be ignored during MVCC reads.

   Pros: rollbacks are cheaper

   Cons: reads are more expensive

   Cons: after a rollback the intents are "still there" and create
   contention with concurrent txns that touch the rolled back intents.

2. proactively iterate through all intents generated for the current
   txn and remove the rolled back sequence numbers from the generated
   intents, including removing the intent if the sequence numbers
   being rolled back are the only ones remaining.

   Pros: reads are cheaper

   Pros: rolled back intents also roll back the contention

   Pros: may simplify the row locking story (although Tobias found out
   that PostgreSQL is pretty bad on this so we don't need this pro to
   be at least as good as pg)

   Cons: rollbacks are more expensive

Nathan recommends approach 1, sensing that it requires less work.

# Unresolved questions

(none at this time)
