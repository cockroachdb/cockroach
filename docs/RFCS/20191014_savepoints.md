- Feature Name: SQL savepoints
- Status: in-progress
- Start Date: 2019-10-14
- Authors: andrei knz, with technical input from nathan tbg lucy radu
- RFC PR: [#41569](https://github.com/cockroachdb/cockroach/pull/41569)
- Supporting tech note: [#42116](https://github.com/cockroachdb/cockroach/pull/42116) or [here](../tech-notes/txn_coord_sender.md) after this PR merges.
- Cockroach Issue: [#10735](https://github.com/cockroachdb/cockroach/issues/10735)

**Remember, you can submit a PR with your RFC before the text is
complete. Refer to the [README](README.md#rfc-process) for details.**

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

## Relationship with row locks

CockroachDB supports exclusive row locks. (They come in two variants -
distributed as write intents and, since v20.1, also non-distributed
from SELECT FOR UPDATE. However this distinction is not material
here).

- In PostgreSQL, row locks are released/cancelled upon ROLLBACK TO
  SAVEPOINT.
- In CockroachDB, row locks are *preserved* upon ROLLBACK TO SAVEPOINT.

This is an architectural difference in v20.1 that may or may
not be lifted in a later CockroachDB version.

Client code that rely on row locks in client apps must be reviewed
and possibly modified to account for this difference. In particular,
if an application is *relying* on ROLLBACK TO SAVEPOINT to release
row locks and allow a concurrent txn touching the same rows to proceed,
this behavior will not work with CockroachDB.

## Relationship with client-side retries

The introduction of regular SQL savepoints reveals a difference
between two situations that were previously somewhat
indistinguishable:

- retry errors (txn conflicts) that are detected / occur in the middle
  of a transaction, as a result for some SQL statement.

  Some (most) of these can now be recovered using ROLLBACK TO
  SAVEPOINT and a regular savepoint name.

- retry errors (txn conflicts) that are detected at the end
  of a transaction, during COMMIT.

  These remain detected during COMMIT, or during `RELEASE
  cockroach_restart` (more below).  These can only be recovered using
  `ROLLBACK TO SAVEPOINT cockroach_restart` and a full txn replay.

From a user perspective, this translates as follows.

The newly introduced support for SQL savepoints still considers
the name `cockroach_restart` special. A savepoint defined with the
name `cockroach_restart` is a "restart savepoint" and has separate
semantics:

1. it must be opened immediately when the transaction starts.
   It is not supported/allowed to open a restart savepoint
   after some other statements have been executed.

   In contrast, regular savepoints can be opened
   after some other statements have been executed already.

2. after a successful RELEASE, a restart savepoint does not
   allow further use of the transaction. COMMIT must immediately
   succeed the RELEASE.

   In contrast, other statements can be executed after a RELEASE
   of a restart savepoint.

3. restart savepoints cannot be nested.
   Issuing `SAVEPOINT cockroach_restart` two times in a row only
   creates a single savepoint marker (this can be seen with
   `SHOW SAVEPOINT STATUS`). Issuing `SAVEPOINT cockroach_restart`
   after `ROLLBACK TO SAVEPOINT cockroach_restart` reuses the marker
   instead of creating a new one.

   In contrast, two `SAVEPOINT` statements with a regular savepoint
   name, or `SAVEPOINT` immediately after `ROLLBACK`, create two
   distinct savepoint markers.

Note: the session setting `force_savepoint_restart` still works
and causes every savepoint name to become equivalent to
`cockroach_restart` with the special semantics described above.

## Schema changes under savepoints

**Note: this section is not yet implemented in 20.1 and is planned for a
later version.**

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


# Reference-level explanation

- [Design overview](#Design-overview)
- [Bill of work (preliminary)](#Bill-of-work-preliminary)
- [SQL executor changes](#SQL-executor-changes)
- [Savepoints and schema changes](#Savepoints-and-schema-changes)
- [Savepoint rollbacks, MVCC and storage](#Savepoint-rollbacks-MVCC-and-storage)
- [TxnCoordSender changes](#TxnCoordSender-changes)

## Design overview

The overall design can be understood as follows:

- at the SQL/KV interface, KV operations are associated with *sequence numbers* (seqnums):
  - write operations generate new seqnums, which are stored inside write intents
  - read operations operate "at" a particular seqnum- a MVCC read that
    encounters an intent ignores the values written at later seqnums
    and returns the most recent value at that seqnum instead, also
    substracting any past value at seqnums marked to be ignored due to
    rollbacks.
  - intent resolution also throws away any values inside the rolled
    back seqnum ranges.

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

## Bill of work (preliminary)

Initial analysis, pre-impl, suggests the following Work to be performed:

- [SQL execution changes](#SQL-executor-changes):
  - define the new executor `Step()` method and call it where relevant.
  - [integrate with mutation and FK semantics](#Savepoints-and-SQL-mutations)
  - connect the current savepoint/rollback logic to the new txncoordsender interface
- [SQL metadata](#Savepoints-and-schema-changes):
  - evict entries from desc caches upon savepoint rollback
- [Storage changes to support rollbacks](#Savepoint-rollbacks-MVCC-and-storage):
  - https://github.com/cockroachdb/cockroach/issues/41612
  - extend the txn proto with a list of ignored seqnum ranges
  - extend the MVCC read logic to skip over ignored seqnum ranges
  - extend the intent resolution logic to skip over ignored seqnum ranges
- [TxnCoordSender changes](#TxnCoordSender-changes):
  - extend the TxnCoordSender interface with methods to
    create a savepoint and rewind (= mark seqnum ranges as ignored)
  - rework the error handling logic in txncoordsender (and perhaps other
    layers) to avoid marking the txn object as in-error for
    non-permanent errors
  - When rolling back to a savepoint, we'll also want to stop
    considering writes with those sequence numbers as in-flight from
    the perspective of a parallel commit. This logic will live in the
    `txnPipeliner`.
- [Savepoints and row locks](#Savepoints-and-row-locks)

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

1) *seqnums will not reset to 0 when a txn epoch is increemented* so
   that seqnums can continue to increase monotonically across txn epochs
   and there is no seqnum reuse across epochs.

2) a *SQL savepoint token* is a copy of the current write seqnum,
   together with the minimal additional state sufficient to partially
   roll back the txn (this is discussed further below).

3) a SQL savepoint release checks and reports any currently deferred
   error (see tech note for definition, e.g. txn push or WriteTooOld).

4) a SQL savepoint rollback is implemented as a `SavepointRollback()` method
   in the TCS, which takes as argument the SQL savepoint token
   where to rollback, computes the range of seqnums from the target savepoint to the
   current last generated seqnum, and then populates the current txn
   object to mark this entire range as rolled back.  (This
   information will live in the transaction record and be available
   through `TxnMeta` to every KV op in MVCC, which need it.)

5) there cannot be any in-flight LeafTxn active when a savepoint is
   rolled back, so that the list of ignored seqnum ranges can never
   change "under" a LeafTxn concurrently.

The SQL executor is responsible for organizing SQL execution so as to
prevent LeafTxns existing concurrently between
`SavepointRollback()` operations.

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

## Savepoints and row locks

The current code in CockroachDB preserves row locks during savepoint
rollbacks. This was already the case with the "restart savepoint"
mechanism introduced in v1.0, and continues to be the case with
the more general savepoints proposed here.

This behavior is a divergence from PostgreSQL which releases locks
upon rollbacks. This may have user-visible impact (see [guide-level
section](#Relationship-with-row-locks)).

Releasing locks on a savepoint rollback is not trivial because we'd
have to track what sequence number each lock was acquired at, and we'd
have to maintain full fidelity for this tracking (preventing us from
collapsing different spans in the footprint to save memory).

One option is to not eagerly release locks, but to update the
transaction record upon a rollback with a list of ignored seq
nums. Then concurrent pusher txns could be allowed to push locks
corresponding to ignored seq nums. This would mean that the kvserver
would have to maintain full fidelity on the locks, however.

There's also a case where we benefit from holding on to locks - when
rolling back to an initial savepoint after a retriable error. In that
case, chances are the same locks will be requested again, so holding
on to them ensures the transaction can acquire them on the retry.

All in all we need some experience with savepoints being used in the
wild to fully understand the trade-offs and make better informed
decisions. Until then, the previous/current behavior and pg divergence
will remain and needs to be documented.

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
