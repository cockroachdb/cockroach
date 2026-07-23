- Feature Name: Read Committed Isolation
- Status: completed
- Start Date: 2023-01-22
- Authors: Nathan VanBenschoten, Michael Erickson, Drew Kimball
- RFC PR: #100608
- Cockroach Issue: None

# Summary

**Concurrency control** comprises the mechanisms that a database system employs
to guarantee the "correct" execution of concurrent operations. **Isolation
levels** provide concurrency control with the requirements for correctness —
defining how and when the changes made by one transaction become visible to
other transactions.

Strong isolation levels provide a high degree of isolation between concurrent
transactions. They limit or eliminate the forms of concurrency effects that
transactions may observe.

Weak isolation levels are more permissive. They trade off isolation guarantees
for improved performance. Transactions run under weaker isolation levels block
less and encounter fewer aborts (retry errors). In some systems, they also
perform less work.

This RFC proposes the implementation of the **Read Committed** isolation level
in CockroachDB.

```
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED
```

Read Committed is a common, relatively weak isolation level found in most legacy
database systems, including PostgreSQL. In fact, it is the default isolation
level in PostgreSQL and most other legacy database systems.

Critically for this proposal, it is the strongest isolation level present in
PostgreSQL that does not experience [serialization failure
errors](https://www.postgresql.org/docs/current/mvcc-serialization-failure-handling.html)
and require applications to handle these errors using application-side retry
logic.

By providing users with the option to run transactions under Read Committed, we
provide them with the option to avoid transaction retry logic in their
applications and make migrations to CockroachDB easier.

# Motivation

As alluded to above, the main impetus behind the introduction of Read Committed
is that the isolation level is expected to **ease application migrations**.

Existing applications that were built for Read Committed (the default isolation
level in many legacy databases, including PostgreSQL) often struggle to move to
CockroachDB's Serializable isolation level. Doing so requires the introduction
of transaction retry logic in the application to handle isolation-related
"serialization" errors. These errors are a consequence of strong isolation
guarantees. While adding retry logic is merely inconvenient for new
applications, it is often infeasible for existing applications. Implementing
Read Committed in CockroachDB will allow these applications to migrate to
CockroachDB without also migrating to a stronger isolation level at the same
time.

For much the same reason, applications also struggle to move to CockroachDB
because they must now tolerate consistency-related "clock uncertainty" errors.
While not a direct consequence of strong isolation, these errors cannot commonly
be handled transparently by CockroachDB because of strong isolation guarantees.
Read Committed's isolation guarantees are weak enough that CockroachDB can
commonly handle consistency-related retry errors internally without involvement
from the application, eliminating the other reason for application-side retry
logic.

Performance-sensitive applications may also prefer Read Committed over
Serializable. The isolation guarantees made by Read Committed are weak enough
that implementations can prevent readers from blocking writers (or causing them
to retry) and writers from blocking readers (or causing them to retry). This
limits the forms of transaction contention that can cause performance
degradation to just write-write contention.

Transaction contention is notoriously difficult to understand, predict, and
mitigate. This is because contention is a global property of an entire workload,
not a local property of single transactions. It emerges from the conflicts
between two transactions, their relative timing, and the degree to which the
transactions are impacted by contention with other transactions. As a result, it
is often associated with meta-stable failures, where a system behaves well until
contention reaches a tipping point, beyond which throughput collapses. Limiting
contention concerns to write-write contention is therefore a major win for
**performance predictability**. In exchange, Read Committed is permissive of
concurrency anomalies, but this may be the right trade-off for some
applications.

In addition, while the primary beneficiaries of the new isolation level are
users that run their apps entirely under the weaker isolation level, users
running their apps under Serializable isolation may still benefit from this
work. Read Committed will provide these users with a tool to run **bulk
read-write transactions** alongside their applications without risk of these
bulk transactions being starved due to serialization errors. This has been a
common struggle for users of CockroachDB.

Arguably, these bulk transactions would be just as well served (or in some cases
better served) by Snapshot isolation, an isolation level between Read Committed
and Serializable. However, CockroachDB does not support this isolation level
either. This reveals the fourth benefit of this work — that it **paves the road
to and then past Snapshot isolation** (Repeatable Read, in PostgreSQL terms).
Almost all engineering work performed in service of Read Committed will also
benefit a potential future Snapshot isolation implementation. At that point, the
work to support Snapshot isolation will be predominantly an exercise in testing,
as many of the pieces inside the database will already be in place.

# Background

The following two subsections present the high-level landscape of isolation
levels and then focus on PostgreSQL's location in this landscape. This
background helps to contextualize the design choices made later on in this
proposal. However, readers that are comfortable with the topic can feel free to
jump to the [technical design](#technical-design).

## Isolation Level Theory Primer

[ANSI SQL](https://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt) (1992)
defined four isolation levels: READ UNCOMMITTED, READ COMMITTED, REPEATABLE
READ, and SERIALIZABLE. The levels were defined in terms of three _phenomena_:
Dirty Reads, Non-Repeatable Reads, and Phantom Reads. Stronger isolation levels
allow fewer phenomena to occur. As a result, they permit less anomalous behavior
(permit fewer anomalies). Weaker isolation levels allow more phenomena to occur.

[A Critique of ANSI SQL Isolation
Levels](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf)
(1995) demonstrated that the ANSI SQL standard definitions of isolation levels
were insufficient. Some phenomena were ambiguous, while others were missing
entirely. The work provided a new characterization of isolation levels, defining
the levels using a set of eight different phenomena. The expanded
characterization also made room for a new isolation level: SNAPSHOT.

While more complete, these definitions were still based on preventing
conflicting operations that could lead to anomalies from executing concurrently.
Adya's dissertation ["Weak Consistency: A Generalized Theory and Optimistic
Implementations for Distributed
Transactions"](https://pmg.csail.mit.edu/papers/adya-phd.pdf) (1999) argued that
this _preventative_ approach is overly restrictive. The definitions were
"disguised versions of locking" and therefore disallow optimistic and
multi-versioning schemes. Adya's work generalizes existing isolation levels in
terms of conflicts, serialization graphs, and the forms of phenomena allowed in
the serialization graphs of different isolation levels.

Because Adya's classification of isolation levels and phenomenon is general
enough to allow both locking and optimistic transaction implementations (of
which CockroachDB is a hybrid), we use its formalization where appropriate in
this proposal. Readers are encouraged to familiarize themselves with the thesis.
Once they notice the page count, they are encouraged to familiarize themselves
with the [summary paper](https://pmg.csail.mit.edu/papers/icde00.pdf) instead.

Conveniently, [Elle](https://github.com/jepsen-io/elle), a transactional
consistency checker that we intend to use as part of the [testing for this
work](#testing), also talks in the language of Adya-style transaction histories
and anomalies.

For a different take on transaction isolation, readers can familiarize
themselves with the work of Crooks et al. in [Seeing is Believing: A
Client-Centric Specification of Database
Isolation](http://www.cs.cornell.edu/lorenzo/papers/Crooks17Seeing.pdf). The
work presents a formalization of isolation guarantees from the perspective of
the users of a database system, instead of from the perspective of the system
itself. Crook's handling of different variants of Snapshot isolation is
particularly novel.

## Isolation Levels in PostgreSQL

Transaction isolation in PostgreSQL is an interesting and relevant topic that is
well documented [here](https://www.postgresql.org/docs/current/transaction-iso.html).

PostgreSQL provides all four standard transaction isolation levels: READ
UNCOMMITTED, READ COMMITTED, REPEATABLE READ, and SERIALIZABLE. However, READ
UNCOMMITTED is not implemented and maps to READ COMMITTED, so internally the
system only supports three distinct isolation levels. Furthermore, for a variety
of reasons and in a variety of ways, the three isolation levels that are present
deviate from their ANSI SQL definitions. Let's explore those briefly, from
strongest to weakest.

`SERIALIZABLE` is the strongest isolation level, and PostgreSQL has supported
true Serializable isolation since its 9.1 release (2011). Being the only
unambiguously defined ANSI SQL isolation level, PostgreSQL's implementation of
Serializable is true Serializable (PL-3 in Adya).

However, PostgreSQL deviates from ANSI SQL's assumed use of [two-phase
locking](https://en.wikipedia.org/wiki/Two-phase_locking) (2PL) to implement
Serializable isolation. Instead, it uses an optimistic concurrency control
protocol called [Serializable Snapshot
Isolation](https://courses.cs.washington.edu/courses/cse444/08au/544M/READING-LIST/fekete-sigmod2008.pdf)
(SSI). SSI extends the multi-versioning present in Snapshot isolation with
additional runtime conflict detection to provide Serializable isolation. The
implementation of SSI in PostgreSQL is recounted by Ports and Grittner in
[Serializable Snapshot Isolation in
PostgreSQL](https://drkp.net/papers/ssi-vldb12.pdf).

Notably, CockroachDB also implements Serializable isolation using SSI.

`REPEATABLE READ` is the next strongest standard isolation level. Formally, the
isolation level (PL-2.99 in Adya) permits Phantom Reads but does not permit
Write Skew. In its place and under the same name, PostgreSQL implements Snapshot
isolation (PL-SI in Adya). Snapshot isolation does not permit Phantom Reads but
does permit Write Skew. This deviation is not strictly wrong from a standards
conformance perspective due to ANSI SQL's ambiguous definitions (which is what
prompted Berenson, Bernstein et al.'s
[critique](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf)),
but it is confusing.

Architecturally, the deviation is a consequence of PostgreSQL's multi-version
concurrency control scheme and general avoidance of read locks. PostgreSQL's
docs explain the [benefit of this
approach](https://www.postgresql.org/docs/current/mvcc-intro.html):

> The main advantage of using the MVCC model of concurrency control rather than
> locking is that in MVCC locks acquired for querying (reading) data do not
> conflict with locks acquired for writing data, and so reading never blocks
> writing and writing never blocks reading.

When viewed as traditional Snapshot isolation, PostgreSQL's REPEATABLE READ
isolation level is the most straightforward of the bunch. Transactions establish
a consistent read snapshot of previously committed data when they begin. All
reads across statements in the transaction are served out of this snapshot
(ignoring read-your-writes). Mutation statements evaluate their row search using
the read snapshot. They then lock and modify qualifying rows, aborting if any
locked row has been changed since the transaction began ("first committer
wins"). These locks then block other writers, but not readers, until the locking
transaction commits.

`READ COMMITTED` is the second weakest standard isolation level, stronger only
than `READ UNCOMMITTED` (which PG does not implement). It is also the focus of
this RFC, so we will give its implementation in PostgreSQL extra attention.

Formally, the isolation level (PL-2 in Adya) permits Non-Repeatable Reads,
Phantom Reads, Lost Updates, and Write Skew, but does not permit Dirty Reads or
Dirty Writes. PostgreSQL's implementation of Read Committed adheres to the
requirements of the isolation level. It also includes additional monotonicity
and recency properties beyond what is required by Read Committed. As a result,
it may be more accurately characterized as an implementation of [Monotonic
Atomic View](https://www.vldb.org/pvldb/vol7/p181-bailis.pdf).

Each statement in a Read Committed transaction establishes a consistent read
snapshot of previously committed data when that statement began. For SELECT
statements, all reads are served out of this per-statement consistent snapshot
(ignoring read-your-writes). As a result, the statement never sees either
uncommitted data or changes committed by concurrent transactions during
statement execution. However, successive SELECT statements in the same
transaction receive different read snapshots and can see different data. Later
statements always receive newer read snapshots.

Mutation statements (e.g. `UPDATE` and `DELETE`) evaluate their row search using
the per-statement read snapshot. They then lock qualifying rows, receiving the
latest version of the row. Once locked, rows that were unchanged since the
statement's read snapshot are mutated. Rows that were changed since the
statement's read snapshot are passed back through the mutation's search
predicate to determine whether they still qualify for mutation. This process of
re-evaluating the query predicate after locking is referred to as
`EvalPlanQual`, and is described
[here](https://github.com/postgres/postgres/blob/f03bd5717eaf31569ca797a2f7d65608f88ac2a2/src/backend/executor/README#L350).

Re-evaluating query predicates when skew is detected between a mutation's read
snapshot and its locked snapshot avoids the need for serialization errors on
write-write conflicts. However, it also permits concurrency anomalies like Lost
Updates. One way to understand this is that anomalies can occur because the
mutation's row search is performed at a different MVCC snapshot than its
mutations, so interleaving mutations from other transactions can be lost.

An example of this is presented in [PG's
docs](https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED:~:text=Because%20of%20the,with%20transactions%20like%3A),
near the text "it is possible for an updating command to see an inconsistent
snapshot".

Explicit row-level locking statements (e.g. `SELECT FOR UPDATE`) behave
similarly to mutations in Read Committed transactions. The initial row search of
the statement is performed using a non-locking read. Qualifying rows are then
locked and their latest version is retrieved. Rows that were changed since the
statement's read snapshot are re-qualified using the same EvalPlanQual approach
and rows that still match the query qualifications are returned.

An added complexity of `SELECT FOR UPDATE` compared to other mutations is that
multiple tables can be locked at once when the query contains a join. In these
cases, base rows that contribute to the returned join rows in each table are
locked. If re-qualification is needed, individual join rows are passed back
through the join condition to determine whether they satisfy the condition with
the latest versions of the contributing rows. If so, the join row is still
returned. Otherwise, it is discarded.

For readers that want to learn more about isolation levels in PostgreSQL,
[Jepsen's 2020 analysis of PostgreSQL
12.3](https://jepsen.io/analyses/postgresql-12.3) is an enjoyable read.

### Data Consistency Checks at the Application Level

PostgreSQL [documents](https://www.postgresql.org/docs/current/applevel-consistency.html)
approaches that application developers can use to enforce application-level data
integrity rules at the different isolation levels.

As expected, the rules are trivial for Serializable isolation. By emulating
serial transaction execution, Serializable isolation has the unique property
that any interleaving of correct transactions will preserve application-level
data constraints, even if the database system is not aware of the constraints.
This property is a major simplification for application developers, and it is
why some are willing to pay a performance penalty.

Repeatable Read and Read Committed are both susceptible to concurrency
anomalies, so the rules and strategies to ensure correctness are more complex.
PostgreSQL recommends the use of explicit row-level (`SELECT FOR UPDATE`,
`SELECT FOR SHARE`) and table-level (`LOCK TABLE`) locks to protect against
concurrent operations that could violate data integrity. 

The use of explicit locking to enforce correctness [has been
demonstrated](http://www.bailis.org/papers/acidrain-sigmod2017.pdf) to be subtle
and error-prone, and yet, it is a task bestowed upon the application developer
by these weaker isolation levels. You get what you pay for.

Of note is that explicit locking is never necessary to preserve data integrity
under Serializable isolation. However, it is necessary in some cases for weaker
isolation levels. This elevates the importance of a correct implementation of
explicit locking; data integrity depends on it. This will be discussed later in
the RFC.

### Data Consistency Checks at the System Level

While not externally documented, PostgreSQL enforces system-level data integrity
rules (foreign key constraints, etc.) under weak isolation levels in the same
manner. Explicit row-level locks are acquired when performing constraint checks
and these locks are held for the remainder of the transaction.

Foreign key existence checks are an example of such a system-level consistency
check. Under the hood, PostgreSQL runs a `SELECT FOR SHARE` query that [looks
like this](https://github.com/postgres/postgres/blob/75f49221c22286104f032827359783aa5f4e6646/src/backend/utils/adt/ri_triggers.c#L363)
(specifically, the query uses `SELECT FOR KEY SHARE`).

### Serialization Failure Handling

PostgreSQL also [documents](https://www.postgresql.org/docs/current/mvcc-serialization-failure-handling.html)
the approaches that application developers can use to handle serialization
failures. As discussed earlier, both Repeatable Read and Serializable isolation
can produce serializable errors (`code 40001 - serialization_failure`).
Repeatable Read will raise them on write-write conflicts. Serializable will also
raise them on certain conflict cycles involving only read-write conflicts.

The documentation recommends that application developers using either of these
isolation levels add retry loops in their application around transactions, and
that these loops should unconditionally retry on `serialization_failure` errors.

Read Committed transactions are not susceptible to these errors, so such retry
loops are not needed. This again is the [primary motivation](#motivation) for
the introduction of Read Committed into CockroachDB.

The documentation does mention that all three isolation levels are susceptible
to locking deadlock errors (`code 40P01 - deadlock_detected`). However, it makes
a weaker recommendation about how application developers should handle this
class of error. Retry loops "may be advisable", but in practice, these deadlocks
are typically handled by structuring transaction logic to use a consistent
locking order.

# Technical Design

The design of the Read Committed isolation level is sprawling. However, the bulk
of the design is split across three major areas of focus:
- [Transaction Model](#transaction-model)
- [Row-Level Locks](#row-level-locks)
- [Query Planning and Execution](#query-planning-and-execution)

## Transaction Model

This RFC proposes a Read Committed implementation in CockroachDB based on a
"Per-Statement Snapshot Isolation" transaction model. To understand this model,
it is helpful to start at CockroachDB's current Serializability model,
resuscitate the corresponding form of Snapshot isolation[^1], and then
generalize this model with the ability to operate over multiple read snapshots
to arrive at Read Committed.

[^1]: CockroachDB originally supported Snapshot isolation. However, the
    isolation level was removed in v2.1 (October, 2018) by
    [#26475](https://github.com/cockroachdb/cockroach/issues/26475) due to its
    multiple bugs. Many of these bugs were a result of CockroachDB's minimal
    support for row-level locking at the time.

To do so, we start by defining two properties:

**Write skew tolerance**: Does the isolation level permit write skew? In
CockroachDB, this property can be expressed as whether the isolation level
allows transactions to write and commit at an MVCC timestamp above the MVCC
timestamp of its read snapshot(s).

**Read snapshot scope**: Does the isolation level allow transactions to operate
across multiple read snapshots? If not, a single read snapshot is used for the
entire transaction. If so, what is the scope of each read snapshot?

With these two properties, we can then construct a unifying framework for the
three isolation levels:

| Isolation Level     | Write Skew Tolerance | Read Snapshot Scope |
|---------------------|----------------------|---------------------|
| Serializable (SSI)  | No                   | Per-Transaction     |
| Snapshot (SI)       | Yes                  | Per-Transaction     |
| Read Committed (RC) | Yes                  | Per-Statement       |

Interested readers can find a proof of correctness of this transaction model [in
the appendix](#appendix-proof-of-correctness).

### Write Skew Tolerance

The primary difference between a Serializable implementation and a hypothetical
Snapshot implementation is Snapshot's tolerance of write skew.

Like in other MVCC systems that support Snapshot isolation, Snapshot isolation
in CockroachDB would permit a transaction's read timestamp and its write
timestamp to diverge and still allow that transaction to commit. Reads performed
at the transaction's read timestamp would never be validated through a refresh
at the transaction's eventual commit timestamp.

Consequently, it would be possible for a key-value that was read at a
transaction's read timestamp to be updated by a second, concurrent transaction
at an MVCC timestamp greater than the first transaction's read timestamp but
less than the first transaction's commit timestamp. The initial read would "no
longer be valid" at the first transaction's commit timestamp. This setup forms
the basis for the write skew anomaly.

Because skew is permitted between (unlocked) reads T a transaction's read
timestamp and its final commit timestamp, reads do not need to be tracked by a
transaction's coordinator, and read refreshes are not needed. The transaction
coordinator can forgo the use of a `txnSpanRefresher` entirely, which is the
component responsible for read span tracking and refreshing.

This tolerance to write skew applies to Read Committed transactions as well.

### Per-Statement Read Snapshots

Write skew tolerance is the major difference between a Serializable
implementation and a hypothetical Snapshot implementation. The major difference
between a hypothetical Snapshot implementation and a Read Committed
implementation is per-statement read snapshots.

Like in PostgreSQL, each statement in a Read Committed transaction will
establish a new read snapshot and will use that snapshot (i.e. MVCC timestamp)
for all of its reads. This will be accomplished by forwarding a `kv.Txn`'s
`ReadTimestamp` to `hlc.Now()` on each statement boundary (i.e. each call to
`kv.Txn.Step`).

By establishing a new read snapshot for each statement, `SELECT` statements and
unlocked portions of mutation statements will observe only data committed before
the statement began. They will not observe uncommitted data or changes committed
concurrently with statement execution.

From this description, it is evident that Read Committed does not provide
repeatable reads. `SELECT` statements in the same transaction can return
different versions of the same data.

Statements will see the effects of writes performed by previous statements
within the same transaction. However, they won't see the effects of writes
performed by the same statement in the same transaction. This avoids the
"Halloween Problem"[^2]. There are no differences between the Serializable and
Read Committed isolation levels as they relate to intra-transaction visibility,
so no changes are needed here.

[^2]: Note that there are currently known bugs in the area of intra-transaction
    visibility (e.g. [#70731](https://github.com/cockroachdb/cockroach/issues/70731))
    which this work does not intend to fix.

#### Monotonicity and Atomicity Guarantees

The use of a globally consistent per-statement read snapshot that advances
between statements is a stronger consistency model than is strictly required by
Read Committed. Notably, it prevents a transaction from observing some of a
committed transaction's effects and then later failing to observe other effects
from the same committed transaction.

With these guarantees, the isolation level could be considered an implementation
of [Monotonic Atomic View](https://www.vldb.org/pvldb/vol7/p181-bailis.pdf).
This closely matches the behavior of PostgreSQL, which has also been
[characterized similarly](https://github.com/ept/hermitage).

#### Recency Guarantees

The use of a per-statement read snapshot that includes all data committed before
(in real-time) the statement began is also a stronger consistency model than is
required by Read Committed. However, this again would match the behavior of
PostgreSQL and other legacy database systems.

### Read Uncertainty Intervals

Providing strong recency guarantees in a distributed system requires more effort
than it does in a single-machine system.

CockroachDB already provides such real-time constraints across transactions.
Formally, the system provides a single-object consistency model of
[linearizability](https://jepsen.io/consistency/models/linearizable). Translated
to a multi-object, multi-operation consistency model, CockroachDB has been
characterized informally as providing [a guarantee of "no stale
reads"](https://www.cockroachlabs.com/blog/consistency-model/). A transaction
observes all data committed before the transaction began.

To provide this guarantee in a distributed system without perfectly synchronized
clocks but with the ability to place _some_ bound on clock skew between nodes,
the system employs "read uncertainty intervals". Readers unfamiliar with the
concept are encouraged to read [this blog
post](https://www.cockroachlabs.com/blog/living-without-atomic-clocks/) and then
[this tech
note](https://github.com/cockroachdb/cockroach/blob/8e24570fa366ed038c6ae65f50db5d8e22826db0/pkg/kv/kvserver/uncertainty/doc.go).

Read Committed transactions have the option to provide the same "no stale reads"
guarantee at the level of each individual statement. Doing so would require
transactions to reset their `GlobalUncertaintyLimit` and `ObservedTimestamps` on
each statement boundary, setting their `GlobalUncertaintyLimit` to `hlc.Now() +
hlc.MaxOffset()` and clearing all `ObservedTimestamps`.

We propose that Read Committed transactions do not do this and instead use the
same uncertainty interval across all statements. The cost of resetting a
transaction's uncertainty interval on each statement boundary is likely greater
than the benefit. Doing so increases the chance that individual statements retry
due to `ReadWithinUncertaintyInterval` errors. In the worst case, each statement
will need to traverse (through retries) an entire uncertainty interval before
converging to a "certain" read snapshot. While these retries will be scoped to a
single statement and [should not escape to the client](#transaction-retries),
they do still have a latency cost.

We make this decision because we do not expect that applications rely on strong
consistency guarantees between the commit of one transaction and the start of an
individual statement within another in-progress transaction. To rely on such
guarantees would require complex and surprising application-side
synchronization.

This design decision can be revisited if it proves to be problematic for
applications. We've been surprised before.

For a discussion of how uncertainty errors are handled, see
[below](#consistency-related-retries).

### Write-Write Conflict Handling (or Lost Update Intolerance)

While write skew is permitted under Snapshot isolation, lost updates are not.
Like under Serializable isolation, transactions throw errors on write-write
conflicts. This is commonly referred to as a "first writer wins" or "first
committer wins" conflict resolution policy.

Protection against lost updates is weaker under Read Committed isolation, where
a limited form of the anomaly is permitted. Updates cannot be lost between a
single statement's read and write timestamp, but can be lost across one
statement's read timestamp and a subsequent statement's write timestamp. This is
a consequence of the use of [per-statement read
snapshots](#per-statement-read-snapshots) in Read Committed.

To understand this, we first decompose the definition of a write-write conflict
as follows:

**Write-Write Locking Conflict**: any case where a transaction attempts to lock
or write to a key that is locked with an exclusive lock by a different
transaction, where intents are considered to be a combination of an exclusive
lock and a provisional value.

**Write-Write Version Conflict**: any case where a transaction attempts to lock
or write to a key that has a _committed version_ with an MVCC timestamp greater
than the locking/writing transaction's current read snapshot.

We define _write-write version conflict_ in terms of a transaction's "current
read snapshot" (i.e. `txn.ReadTimestamp`) to afford flexibility in the
definition to transactions that change their read snapshot across their
execution. For example, Read Committed transactions advance their read snapshot
on each statement boundary, so a committed version that would cause a
write-write version conflict for one statement may not cause a write-write
version conflict for a later statement in the same transaction.

Snapshot and Read Committed transactions handle _write-write locking conflicts_
identically to Serializable transactions. The prospective locker
[waits](#blocking-write-write-conflicts) for the existing exclusive lock to be
released before acquiring it. In cases where multiple transactions wait for
exclusive access to the same key, they form an orderly queue through the
`lockWaitQueue` mechanism.

Once a transaction (regardless of isolation level) has navigated any potential
_write-write locking conflict_, it may experience a _write-write version
conflict_. This occurs when the holder of the lock performs a write on the
locked row before committing. In such cases, a `WriteTooOld` error is thrown.

For Snapshot and Serializable transactions, this requires the entire transaction
to retry. We will see below that this is not the case for Read Committed
transactions. A write-write version conflict will raise a `WriteTooOld` error
that will cause the Read Committed transaction to retry the individual statement
that experienced the write-write version conflict at a new read snapshot, but
will not require the entire Read Committed transaction to retry. This
adaptability is possible because Read Committed transactions use per-statement
read snapshots.

This behavior deviates from PostgreSQL, which does not require statements in
Read Committed to restart on write-write conflicts. We [explore an alternative
approach](#appendix-postgres-compatible-intra-mutation-consistency) that behaves
more similarly to PostgreSQL in the appendix, where we also explain why that
approach was rejected in favor of the stronger model presented here.

### Blocking Behavior

A transaction's _blocking behavior_ defines the cases in which contention with
other transactions does or does not block.

Isolation levels do not directly mandate a specific blocking behavior — the same
isolation level can be implemented differently and lead to different blocking
behavior while providing the same isolation guarantees. However, weaker
isolation levels often make it easier to avoid blocking with fewer consequences
(e.g. potential aborts).

In the following subsections, we outline the blocking behavior of Read Committed
transactions for the four different classes of conflicts composed of read and
write operations performed by two different transactions on the same row(s).

In all cases, exclusive locking reads (`SELECT FOR UPDATE`) are treated as a
type of write. Shared locking reads complicate the discussion for little
immediate benefit, so [their discussion is deferred](#shared-locks).

The name of each conflict refers to the order in which operations take place.
For example, a "write-read conflict" refers to a write to a row, followed by a
read of the same row by a different transaction. The later operation is said to
encounter the conflict, so we focus on the blocking behavior of that operation.
The earlier operation never blocks because there was no conflict at the time of
its execution.

#### Non-Blocking Read-Read Conflicts

Two reads to the same row from concurrent transactions never block. Furthermore,
in an MVCC system like CockroachDB, the two reads do not interact at all. The
later read is not influenced by the earlier read.

#### Blocking Write-Write Conflicts

Two writes to the same row cause blocking. The later writer will wait for the
lock acquired by the earlier writer to be released. This will occur when the
earlier writer's transaction is committed or rolled back.

#### Non-Blocking Read-Write Conflicts

A read of a row followed by a write to that row by a different transaction does
not block. The writer is allowed to proceed with its write, unencumbered. This
is an [oft-cited benefit of MVCC
systems](https://docs.oracle.com/en/database/oracle/oracle-database/19/cncpt/data-concurrency-and-consistency.html#GUID-1D60EFCC-03F4-4A04-B099-1B4DE5D02C47).

However, while there is no blocking, the two operations do still interact. The
writer is barred from committing at an MVCC timestamp at or below the timestamp
of the reader. Failure to do so would violate the consistency of the reader's
snapshot. In CockroachDB, this coordination is facilitated through the
[TimestampCache](https://github.com/cockroachdb/cockroach/blob/8e24570fa366ed038c6ae65f50db5d8e22826db0/pkg/kv/kvserver/tscache/cache.go#L31).

#### Non-Blocking Write-Read Conflicts

Finally, a write of a row followed by a read of that row by a different
transaction does not block. The reader is allowed to proceed with its read after
determining whether it should observe or ignore the conflicting write. Either
way, it does not block and wait for the writer to commit. This is the other
[oft-cited benefit of MVCC
systems](https://docs.oracle.com/en/database/oracle/oracle-database/19/cncpt/data-concurrency-and-consistency.html#GUID-1D60EFCC-03F4-4A04-B099-1B4DE5D02C47).

Non-blocking write-read conflicts are the **only blocking behavior interaction
that will change with the Read Committed work**. As such, they deserve focused
attention.

Currently, a subset of write-read conflicts are blocking in CockroachDB. This
has been a repeated source of confusion and frustration for users of the system.
This is especially true for migrations from PostgreSQL, which provides
non-blocking write-read conflicts in all isolation levels. Unexpected blocking
can cause performance problems, unpredictability, and application deadlocks.

Preliminary discussions with users of CockroachDB revealed that a desire for
this property is one of the underlying reasons why they ask for Read Committed,
alongside the desire for fewer transaction aborts. 

And yet, the property is not strictly an artifact of isolation. As PostgreSQL
demonstrates, it can be provided at all isolation levels. CockroachDB's
engineering team has [previously
explored](https://docs.google.com/document/d/1ji6C0aDI6n61sVKPjf5-YUucbtgBlwfpsrNdcidW5a0/edit?usp=sharing)
what it would take to support non-blocking write-read conflicts under
Serializable isolation.

That earlier proposal outlined a long-term vision for changes that would
primarily benefit transaction contention, but would also benefit multi-region
transactions, one-phase commits, foreign key lookups, and more. It was ambitious
but difficult to decompose. It also struggled with increasing the risk of
transaction starvation without sufficient recourse for users, at least until
either SHARED locks or weaker isolation levels are introduced.

This RFC proposes two deviations from that original proposal. First, we will
start by supporting the (non-)blocking behavior **only at the Read Committed
isolation level**, where there are fewer trade-offs. Read Committed transactions
are not subject to the increased risk of starvation in order to provide this
property, so there are fewer risks to the work. Second, we will implement the
(non-)blocking behavior using **a simpler approach** that has fewer auxiliary
benefits and shifts work from writers to readers but still achieves the
sought-after non-blocking property.

After the mechanisms to support non-blocking write-read conflicts are built as
part of this work, we can then revisit their use for Serializable transactions.
At that point, engineering efforts can focus solely on the problem of read
predictability vs. fairness. If the property is eventually extended to
Serializable transactions, negatively impacted users will be able to combat
starvation by using SHARED locks during select reads or by running select
transactions under Read Committed isolation.

##### Status Check RPC on Intent Conflicts

To implement non-blocking write-read conflicts, the conflict resolution rules
for non-locking readers that encounter conflicting intents will be adjusted.
Note here that "conflicting" means that the read timestamp of the reader is
equal to or greater than the write timestamp of the intent and that the reader
is attempting to read the intent's key. The reader must determine whether the
intent should be included in its read snapshot.

In such cases, the presence of the intent alone cannot be used to determine
whether the intent's writer is still running or whether it has already been
committed. Distinguishing between these two cases as a reader is necessary to
ensure both atomicity and (CAP) consistency.

Instead of waiting on these conflicting intents until the intent's transaction
completes, the non-locking readers will perform a "status check" RPC in the form
of a `PushTxn(PUSH_TIMESTAMP)` to the transaction record of encountered intent
to determine its visibility. If the intent's transaction has already been
committed or aborted, the reader must initiate and wait for intent resolution
and then observe the post-resolution state[^3]. However, if the intent's
transaction is in progress, the reader can [push the minimum commit
timestamp](https://github.com/cockroachdb/cockroach/pull/95911) of the intent
holder to prevent it from committing at or before the reader's read timestamp.
The reader can then proceed to ignore the intent and continue with its scan.

[^3]: there are other projects like
    https://github.com/cockroachdb/cockroach/issues/91848 that aim to reduce
    blocking between intent resolution and conflicting KV operations. These are
    out of the scope of this RFC.

To make the performance of this approach reasonable, two changes will be needed.

First, requests must cache status check results for transactions that have been
pushed, were seen to be `PENDING` after the request began, and are uncommittable
below the request's read timestamp. This cache can be used to prevent a request
from needing to perform a status check for the same transaction multiple times.

The cache must be request-scoped and not scoped more broadly (e.g.
range-scoped). This is because: Requests have different read timestamps. The
cache can only be used to avoid a PushTxn request if a prior PushTxn advanced
the intent holder's minimum commit timestamp above a request's read timestamp.
Requests originate at different times. A PushTxn that previously observed a
PENDING intent holder _before_ a request arrived is no proof that the intent
holder has not committed before the request's transaction began. Ignoring such
intents because they _used to be_ PENDING (without additional uncertainty
interval logic) could lead to violations of real-time ordering.

For ease of implementation, a new request-scoped cache of non-visible
transactions will be added to `concurrency.Guard`, making it per-request and
per-range scoped. This is convenient because it avoids wire-protocol changes and
because `concurrency.Guard` objects are scoped below transaction read_timestamp
adjustments, so cache eviction logic on transaction refresh will be implicit. As
a result of this scoping, a given read request may query the status of a given
pending transaction at most O(ranges) times, which is deemed to be a reasonable
cost.

Second, readers must be taught to ignore conflicting intents that are known to
be PENDING without traversing consensus to resolve (rewrite) them. This can be
achieved through either an augmentation of the `intentInterleavingIter` or by
providing the MVCC scanner with another view into the `concurrency.Guard`. The
approach is outlined in
[#94730](https://github.com/cockroachdb/cockroach/issues/94730) and has been
prototyped in
[nvanbenschoten/nonBlockingReads](https://github.com/nvanbenschoten/cockroach/commits/nvanbenschoten/nonBlockingReads).

##### Ignored Exclusive Lock Conflicts

The conflict resolution rules for non-locking readers that encounter exclusive
locks will also be adjusted. This adjustment is simpler. Exclusive locks that do
not protect provisional values (e.g. those acquired by `SELECT FOR UPDATE`) will
no longer block non-locking readers. Further, because they have no associated
provisional value that may require visibility, readers need not concern
themselves with determining the status of the lock holder's transaction.
Instead, readers can simply ignore these locks.

This matches the behavior of PostgreSQL, even under Serializable isolation.
However, as with the other half of Non-Blocking Write-Read Conflicts, this
behavior change will only initially apply to Read Committed readers until we can
be sure it does not regress performance for Serializable transactions.

### Transaction Retries

CockroachDB's Serializable implementation combines elements of a pessimistic and
an optimistic transaction model. Write-write conflicts are eagerly detected
during statement execution and lead to immediate transaction aborts. However,
"dangerous structures" (see [Serializable Snapshot
Isolation](https://courses.cs.washington.edu/courses/cse444/08au/544M/READING-LIST/fekete-sigmod2008.pdf))
that could otherwise lead to non-serializable histories are detected at commit
time.

This optimistic validation is accomplished through the combination of a
commit-time condition that a transaction's `ReadTimestamp` equals its
`WriteTimestamp`
(https://github.com/cockroachdb/cockroach/blob/8e24570fa366ed038c6ae65f50db5d8e22826db0/pkg/kv/kvserver/batcheval/cmd_end_transaction.go#L504)
and a [read
refresh](https://github.com/cockroachdb/cockroach/blob/8e24570fa366ed038c6ae65f50db5d8e22826db0/pkg/kv/kvclient/kvcoord/txn_interceptor_span_refresher.go#L45)
mechanism that advances the transaction's `ReadTimestamp`. The first of these
checks fails if the transaction has established an inbound read-write
anti-dependency (e.g. its `WriteTimestamp` is pushed by timestamp cache). The
second of these checks fails if the transaction has established an outbound
read-write anti-dependency (e.g. its read refresh finds a newer version). If
both fail, the transaction aborts.

These commit-time isolation checks are not needed for Read Committed
transactions. This is because the "dangerous structures" that can create
non-serializable histories are permitted under the Read Committed isolation
level. In practice, this means that a Read Committed transaction can commit even
if its `ReadTimestamp` is skewed from its `WriteTimestamp`.[^4]

[^4]: This would be true of a Snapshot isolation implementation as well.

#### Isolation-Related Retries

We [alluded earlier](#write-write-conflict-handling-or-lost-update-intolerance)
that write-write version conflicts do not cause Read Committed transactions to
abort. This is despite the fact that write-write version conflicts raise
`WriteTooOld` errors, as they do in other isolation levels.

When Serializable transactions encounter such situations, they restart from the
beginning to establish a new read snapshot that includes the committed version
that caused the write-write version conflict. Read Committed transactions have
the flexibility to do better, thanks to the use of [per-statement read
snapshots](#per-statement-read-snapshots). Because each statement in a Read
Committed transaction can observe a different read snapshot, `WriteTooOld`
errors can be handled at the statement level and not at the transaction level.
Critically, this allows the statement to retry at a new read snapshot without
involving the client.

To facilitate these statement-level retries of write-write conflict errors, each
SQL statement will be run inside of a retry loop. `WriteTooOld` errors will be
caught before they escape to the client and the statement will be retried. All
prior effects of the statement will be rolled back on retry through the use of
[transaction
savepoints](https://www.cockroachlabs.com/docs/stable/savepoint.html). A
savepoint will be created at the beginning of each statement, released on
success, and rolled back on uncertainty error.

However, there is a caveat here. If the statement has already begun streaming a
partial result set back to the client, it cannot retry transparently. By
default, the result set will be buffered up to 16KiB before overflowing and
being streamed to the client. However, this result buffer size can be configured
using the `sql.defaults.results_buffer.size` cluster setting or the
`results_buffer_size` session variable. This condition is analogous to the
[automatic retry
behavior](https://www.cockroachlabs.com/docs/stable/transactions.html#automatic-retries)
of implicit transactions.

The only remaining source of isolation-related transaction aborts is locking
deadlocks. Unlike write-write conflicts, which can be scoped to a single
statement and retried without client involvement, locking deadlocks will result
in a transaction abort and an error returned to the client, as [is also the case
in PostgreSQL](#serialization-failure-handling). This is because locks acquired
earlier in the transaction may need to be dropped to break the deadlock.

#### Consistency-Related Retries

While Read Committed is a weak enough isolation level to avoid most
isolation-related causes of retries, its implementation in a distributed system
leads to a uniquely distributed concern: consistency-related retries.

As [previously discussed](#read-uncertainty-intervals), Read Committed
transactions will use uncertainty intervals to provide real-time ordering
guarantees between transactions. The use of uncertainty intervals implies the
possibility of `ReadWithinUncertaintyInterval` errors. While reading, a Read
Committed transaction may observe an MVCC version above its read snapshot but
within its uncertainty interval. Ignoring such values could violate
linearizability, so the value must be returned. However, merging the uncertain
value into the existing read snapshot would create an inconsistent view.

When Serializable transactions encounter such situations, they restart from the
beginning to establish a new read snapshot that includes the uncertain value.
Like with `WriteTooOld` errors, Read Committed transactions have the flexibility
to do better, thanks to the use of [per-statement read
snapshots](#per-statement-read-snapshots). Because each statement in a Read
Committed transaction can observe a different read snapshot,
`ReadWithinUncertaintyInterval` error can be handled at the statement level and
not at the transaction level. Like with write-write conflict errors, this allows
the statement to retry at a new read snapshot without involving the client.

The same per-statement retry loop used to retry write-write conflicts will also
be used to facilitate statement-level retries of uncertainty errors.

As with transaction-level retries of Serializable transactions, statement-level
retries of Read Committed transactions will not reset the transaction's
uncertainty interval. Even as the read snapshot advances across retries, the
upper bound of the uncertainty interval will remain fixed. This eliminates the
possibility of starvation and bounds the retry duration to the configured
maximum clock offset (i.e. the size of the uncertainty interval). In other
words, even in the worst case, this statement-level retry loop will converge.

The same caveat about result set streaming that was mentioned in the previous
section applies to uncertainty errors as well.

#### Retry Avoidance Through Read Refreshes

While a per-statement retry loop limits the cases where isolation and
consistency-related retry errors escape from the SQL executor to the client, it
is better if these errors never emerge from the key-value layer in the first
place. The avoidance of such retry errors is possible in limited but important
cases through a familiar mechanism: read refreshes.

This proposal previously mentioned that read refreshes are not needed for
isolation levels that tolerate write skew. While read refreshes are less
important for weak isolation levels, these levels can still benefit from the
mechanism in the limited cases where transactions need to adjust their read
snapshot. Specifically, even under weak isolation levels, read refreshes can be
used to handle write-write version conflicts (`WriteTooOld` errors) and read
uncertainty conflicts (`ReadWithinUncertaintyInterval` errors). Read refreshes
do so by proving that a transaction's current read snapshot is equivalent to a
later read snapshot for the set of key spans previously read by the transaction,
allowing the transaction to dynamically adjust its read snapshot without a full
restart.

Read refreshes come in two flavors: client-side and server-side. Both forms of
refreshes will be supported for Read Committed through the use of the
`txnSpanRefresher` and the `CanForwardReadTimestamp` protocol.

The only difference between Read Committed transactions and
Snapshot/Serializable transactions in this area is that Read Committed
transactions will clear their refresh spans when establishing new per-statement
read snapshots. Doing so will increase the success rate of read refreshes Read
Committed transactions. Additionally, it will increase the number of cases where
server-side refreshes are possible — each statement will begin with the
opportunity to perform server-side refreshes. Consequently, many simple
statement will never perform per-statement retries, even if they experience
contention.

#### Structural Commit Conditions

The remaining commit conditions that Read Committed transactions must check are
structural.

First, Read Committed transactions must ensure that their intent writes have all
succeeded. If any pipelined intent writes have failed, a
`RETRY_ASYNC_WRITE_FAILURE` error will be returned.

Second, Read Committed transactions must commit before their schema-imposed
deadline. If long-running Read Committed transactions fail to update schema
leases before attempting a commit, a `RETRY_COMMIT_DEADLINE_EXCEEDED` error will
be returned.

None of these structural errors are user-controllable. They should not be
produced outside of extraordinary cluster conditions like node failures. Users
are not expected to handle them gracefully.

#### Parallel Commits Protocol

The [parallel
commits](https://github.com/cockroachdb/cockroach/blob/8e24570fa366ed038c6ae65f50db5d8e22826db0/pkg/kv/kvclient/kvcoord/txn_interceptor_committer.go#L51)
atomic commit protocol also deserves discussion.

The protocol extends traditional two-phase commit with a distributed commit
condition called the "implicit commit" state. This state is a function of the
staging timestamp of a transaction record and the version timestamp of each of
the intents listed in the transaction record's in-flight write set.

The condition does not change for Read Committed transactions, and the atomic
commit protocol remains equally applicable to the weaker isolation level.

## Row-Level Locks

Serializable transactions in CockroachDB use read refreshes as a form of
optimistic validation to ensure the integrity of reads on commit. As discussed
above, this will not be the case for Read Committed transactions.

This is a deliberate design decision. By permitting skew between the one or more
read snapshots observed during a transaction and the MVCC snapshot into which
the transaction installs its writes, Read Committed transactions can avoid
retries and blocking. In some sense, this flexibility is what enables the Read
Committed isolation level to accomplish its goals. In exchange, these reads
provide weak isolation guarantees.

However, there are cases where stronger guarantees are needed for the reads in a
Read Committed transaction to enforce data integrity rules. These situations
arise at both the application level (outside the DB) and the system level
(inside the DB). In these cases, row-level locks serve as an important tool to
"upgrade the isolation" of select read operations in an otherwise weakly
isolated transaction.

A useful mental model for those familiar with MVCC is that standard reads
performed during a Read Committed transaction are not guaranteed to remain valid
up to the point when the transaction commits. However, by acquiring locks on
specific rows when reading, writes by other transactions that would invalidate
those reads are blocked from completing until the reader commits[^5]. As a
result, stronger guarantees can be made about the relationship between certain
reads in a transaction and that transaction's writes. These guarantees then
serve as the foundation for building higher-level data integrity constraints.

[^5]: Ignoring phantoms inserted between rows or assuming the use of gap locks.

While CockroachDB already has limited support for row-level locks (e.g. `SELECT
FOR UPDATE`), the support lacks the expressivity and reliability that users of
Read Committed will demand. New forms of row-level locks must be introduced.
These locks must then be improved to provide strong correctness guarantees.

### Shared Locks

CockroachDB added support for `SELECT FOR UPDATE` in v20.1. `SELECT FOR UPDATE`
acquires an exclusive lock on each row returned from a `SELECT` statement. At
the time, the tool was primarily meant to provide fine-grained control over lock
ordering. Users could avoid transaction retries in certain cases by acquiring
locks for rows that they intended to update earlier in their transaction. This
motivation was explored in [this introductory blog
post](https://www.cockroachlabs.com/blog/when-and-why-to-use-select-for-update-in-cockroachdb/).

`FOR UPDATE` refers to the strength of the lock acquired on rows returned from
the `SELECT` statement. If a transaction intends to later `UPDATE` a row, it
benefits from acquiring an
[Exclusive](https://github.com/cockroachdb/cockroach/blob/e67e3961a8f4314dd7d92a0bcbe0986f3ce8cee9/pkg/kv/kvserver/concurrency/lock/locking.proto#L91)
lock on the row when it initially reads the row's value.

Under Read Committed, transactions may want to lock a row to prevent concurrent
writes even if they don't intend to `UPDATE` the row themselves. In these cases,
blocking concurrent readers is unnecessary and undesirable, so
[Shared](https://github.com/cockroachdb/cockroach/blob/e67e3961a8f4314dd7d92a0bcbe0986f3ce8cee9/pkg/kv/kvserver/concurrency/lock/locking.proto#L53)
locks are a better alternative.

SQL provides a tool for these situations in the form of `SELECT FOR SHARE`.
`SELECT FOR SHARE` behaves identically to `SELECT FOR UPDATE`, except that it
acquires a
[weaker](​​https://github.com/cockroachdb/cockroach/blob/e67e3961a8f4314dd7d92a0bcbe0986f3ce8cee9/pkg/kv/kvserver/concurrency/lock/locking.proto#L142)
Shared lock on each of the returned rows.

The design of Shared locks was explored in: [#101799](https://github.com/cockroachdb/cockroach/pull/101799).

Read Committed will depend on the implementation of the Shared locking strength
for two reasons. First, applications that use `SELECT FOR SHARE` will behave
incorrectly if we continue to treat the locking strength as a no-op. Second,
CockroachDB's SQL layer will begin to use `SELECT FOR SHARE` internally to
enforce referential integrity constraints in Read Committed transactions, as we
will see later on in this proposal.

### Reliability and Enforcement

Row-level locks acquired by `SELECT FOR UPDATE` are currently best-effort.

The locks are maintained only on the leaseholder (they are "unreplicated") and
are discarded in a handful of circumstances, including lease transfers, range
splits, range merges, node failures, and memory limits. This has caused user
confusion, but has not been a correctness concern to date because Serializable
transactions never rely on these locks to ensure isolation guarantees.

With the introduction of Read Committed, best-effort row-level locks are no
longer sufficient for `SELECT FOR UPDATE`. We will need these locks to provide
stronger guarantees.

#### Properties of Reliability

We can split the meaning of "reliable" row-level locks into two properties:

**Isolation**: If a lock is held on a row by a transaction, that row's value
must not be changed by any other transaction before the lock holder commits.
Here, "before" is defined in the MVCC timestamp domain.

**Mutual Exclusion**: If a lock is held on a row by a transaction, no other
transaction may acquire a
[conflicting](https://github.com/cockroachdb/cockroach/blob/e67e3961a8f4314dd7d92a0bcbe0986f3ce8cee9/pkg/kv/kvserver/concurrency/lock/locking.proto#L142)
lock on that same row before the original lock holder commits. Here, "before" is
defined in the wall clock domain, as perceived by a client of the database.

Without additional constraints or assumptions, neither property implies the
other. This is inconsequential today because CockroachDB's row-level locks
currently provide neither property.

Isolation is enforced in Serializable transactions using read refreshes,
allowing the locks to be best-effort. If a lock is lost and a conflicting
version is installed on a previously locked row before the lock holder is
committed, the lock holder's refresh will fail and the transaction will abort.

Mutual Exclusion is not provided by any other mechanism. Instead, it is a use
case for row-level locks that CockroachDB has resisted. At a theoretical level,
fault-tolerant mutual exclusion in a distributed system [is
impossible](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html).
The best a distributed system like CockroachDB can do is strive to minimize the
cases where mutual exclusion is violated and introduce commit-time validation
that mutual exclusion was not violated (e.g. preventing a transaction from
committing if its lock was removed because the system incorrectly deemed its
coordinator to have failed).

To support Read Committed, only the Isolation property is necessary. However,
some solutions move us closer to achieving the Mutual Exclusion property as
well.

#### Reliability Improvement Alternatives

There are a collection of approaches that we could take to strengthen row-level
locking to a degree that the row-level locks could be used for Read Committed.
To compare these alternatives, we first define a set of tests to evaluate each
approach.

- **(T1) Provides Isolation**: Does the approach provide the **Isolation**
  property?

- **(T2) Provides Mutual Exclusion**: Does the approach provide the **Mutual
  Exclusion** property?

- **(T3) Lock Persistence**: Does the approach require a disk write per
  row-level lock?

- **(T4) Lock Replication**: Does the approach require replication per row-level
  lock?

- **(T5) Commit-Time Validation**: Does the approach require commit-time
  validation of lock validity?

- **(T6) Commit Starvation**: Is the approach susceptible to commit starvation
  if a committer is pushed during a commit-time validation phase?

- **(T7) Completeness**: Will the approach support all transactions?

- **(T8) Abort Conditions**: What conditions will cause a transaction to abort
  and return an error?

Using these tests, we can then compare the base alternatives and their
specialized variants.

At a high level, there are three alternative approaches:
- **Refresh Under Locks**
- **Validate Locks**
- **Replicate Locks**

##### Alternative: Refresh Under Locks

The **Refresh Under Locks** approach extends the existing Read Refresh
mechanism. While Read Committed transactions no longer refresh all of their
reads, the keys on which they acquired locks will be tracked in their refresh
span set and will be refreshed on commit.

This approach provides isolation (**T1**) for the set of keys locked by a
transaction in the same way that Serializable transactions provide isolation for
all keys read, using a commit-time refresh (**T5**). However, it makes no
attempt to ensure that locks are still held at commit time, so it does not
provide mutual exclusion (**T2**).

Because locks can be lost without violating isolation, the most basic version of
this approach does not need lock persistence (**T3**) or lock replication
(**T4**). However, a lost lock can lead to a refresh failure and a transaction
abort (**T8**) if the loss of the lock allows for a conflicting write to
succeed.

Thanks to refresh span condensing, the approach is close to complete (**T7**),
assuming no contention. Even transactions that acquire millions of locks like
`SELECT * FROM billion_rows FOR UPDATE` should be able to commit, though span
condensing does create the opportunity for false positive refresh validation
failures. However, the approach is not complete with contention. A transaction
that acquires millions of locks will quickly exceed the maximum in-memory lock
limit and the locks will be lost. This can allow conflicting writes on
should-be-locked keys, causing the refresh to fail.

Finally, assuming non-blocking write-read conflicts, the approach is subject to
starvation (**T6**). When a transaction begins to commit, it will refresh to its
current provisional commit timestamp. If its minimum commit timestamp is pushed
by a conflicting reader during this time, the transaction will need to refresh
again. This can starve, as is described in
[#95227](https://github.com/cockroachdb/cockroach/issues/95227).

While the approach builds upon the existing Read Refresh mechanism, that
mechanism will need generalization to work with Read Committed. This is because
Read Committed transactions operate across many read snapshots. The refresh span
tracking must be augmented with a timestamp-per-span, which complicates the data
structure and the span condensing algorithm. The Read Refresh API already
supports per-span `RefreshFrom` timestamps, so this will be strictly a
client-side bookkeeping change.

##### Alternative: Validate Locks

The **Validate Locks** approach is similar in spirit to the **Refresh Under
Locks** approach. Both use a commit-time validation step to verify isolation.
However, the two approaches differ in what they verify. While **Refresh Under
Locks** looks for violations of isolation in the MVCC history of keys that were
locked, **Validate Locks** simply verifies that the locks that were acquired are
still held at commit time.

The approach then provides isolation (**T1**) by construction — if locks protect
against conflicting writes that could violate isolation, then a commit-time
proof (**T5**) that all acquired locks still exist acts as a proof that
isolation has not (yet) been violated. Additionally, this serves as a proof that
mutual exclusion (**T2**) has not been violated, because no conflicting lock
could have been acquired since the committing transaction acquired its locks.

The assumption with this approach is that neither lock persistence (**T3**) nor
lock replication (**T4**) would be used. As a result, lease transfers, node
crashes, memory limits (**T7**), or other events could lead to lost locks, which
would cause validation to fail and the lock acquirer to abort (**T8**). Unlike
**Refresh Under Locks**, this abort would occur even if there was no real
transaction contention.

This approach also has a similar downside to **Refresh Under Locks**, which is
that the commit-time validation only provides a proof of isolation up to some
MVCC timestamp. The lock could be lost immediately after validation and the best
the validating transaction could do is bump the timestamp cache below the lock
during validation to its current provisional commit timestamp (in fact, it must
do this). However, if the committing transaction is then pushed, it would need
to re-validate its locks. This means that the approach is also subject to
starvation (**T6**) with non-blocking write-read conflicts.

##### Alternative: Replicate Locks

The **Replicate Locks** approach is similar to the **Validate Locks** approach
in that it also uses locks themselves to provide isolation (**T1**) and mutual
exclusion (**T2**) over the keys that it protects. However, it does not use a
commit-time validation step (**T5**) to determine whether locks are still held.
Instead, it replicates locks eagerly during transaction execution to eliminate
cases where they could be lost.

There are a handful of variants of this approach, all of which include some form
of lock persistence (**T3**) and lock replication (**T4**). The simplest
approach would be to synchronously replicate locks during the execution of
`SELECT FOR {UPDATE/SHARE}` statements. A more sophisticated variant would be to
pipeline this lock acquisition like we do with [intent
pipelining](https://www.cockroachlabs.com/blog/transaction-pipelining/), at the
risk of pipelining errors. A third variant would be to eagerly acquire
unreplicated locks during the execution of `SELECT FOR {UPDATE/SHARE}`
statements and then promote these locks to replicated locks at commit-time, at
the risk of the unreplicated locks having been lost by this time. The variants
differ in performance but are otherwise similar.

Replicating locks and storing them in persistent storage (using the replicated
lock table keyspace which was designed for such storage) prevents the locks from
being lost during lease transfers, node crashes, or memory limits (**T8**). As a
result, this approach is complete (**T7**). It can use client-side span
coalescing to scale to an arbitrary number of locks in roughly the same way that
a transaction can write to an arbitrary number of keys.

Replicating locks also avoids the risk of commit starvation (**T6**) because it
forgoes commit-time validation entirely. Once a transaction has persisted all
locks, it is free to commit at any MVCC timestamp in the future. It can continue
to be pushed by contending readers without consequence. The only requirement is
that when locks are released after a transaction has committed and the locks are
not replaced by a committed version, the timestamp cache must be bumped to the
commit timestamp of the transaction to prevent conflicting writers from
re-writing portions of the "locked MVCC history".

##### Variants and Optimizations

As implied above, variants of the approaches exist and the approaches are not,
for lack of a better word, mutually exclusive. Aspects of some approaches could
be used to augment other approaches to arrive at strong alternatives. For
example, the commit-time lock replication variant discussed above can be viewed
as a hybrid of Lock Validation and Lock Replication. Lock Validation schemes
could also employ Lock Replication as a mechanism to spill locks on memory
limits or ship locks during lease transfers.

One additional hybrid approach that deserves mention is **Lock Validation with
Validity Windows**. This approach exploits the fact that a stable leaseholder
can provide _some_ guarantees on lock validity, even if it can't guarantee lock
validity indefinitely. For instance, a leaseholder could make a guarantee that a
lock will only be lost if the leaseholder crashes. In doing so, it could
guarantee isolation on locked keys up to its lease expiration. As a result,
unreplicated lock acquisition would provide a validity window for each lock, and
a transaction would only need to validate its locks if it ran for long enough to
exceed this validity window. This hybrid approach provides many of the benefits
of each alternative — namely, it avoids synchronous lock replication but it also
avoids commit starvation. However, for a leaseholder to make such a guarantee,
it would likely employ lock replication in certain cases (e.g. memory limits),
so it makes sense to consider this hybrid approach as a future optimization.

#### Reliability for Preview Release

For the initial version of Read Committed, we propose a limited form of
synchronous **Lock Replication** as a mechanism to ensure lock reliability. This
is primarily due to the completeness **(T7)** and starvation **(T6)** properties
provided by the alternative. Performance is a secondary consideration, and the
performance of replicated locks can be optimized in future releases using the
suggestions outlined above.

However, one performance optimization will be incorporated into the initial
design. While `SELECT FOR UPDATE` and `SELECT FOR SHARE` will synchronously
replicate locks, "implicit select for update" performed during the search phase
of certain mutation statements will continue to acquire unreplicated and
unvalidated locks. These locking reads are immediately followed by a write, so
they need not provide isolation on their own. Meanwhile, replicating these locks
would incur a severe latency cost. This is true even if the replication is
pipelined, because the subsequent write would immediately hit a pipeline stall
while waiting for the replication of the lock to complete.

The decision of whether to replicate locks or not will be expressed from SQL
through the KV API using the existing `lock.Durability` flag. Only replicated
locks will provide isolation guarantees.

## Query Planning and Execution

To support the more stringent locking requirements of Read Committed, several
changes must be made to query planning and execution around lock acquisition.

### Read-Only Queries (`SELECT`)

Query planning and execution for read-only queries (i.e. `SELECT` statements)
will not change for Read Committed transactions, despite the use of
[per-statement read snapshots](#per-statement-read-snapshots). These queries
will continue to operate on a consistent snapshot of the system, so all planning
optimizations derived from SQL constraints remain valid.

### Explicit Locking Queries (`SELECT FOR UPDATE`)

Under our Serializable isolation, locking is not needed for correctness. Because
of this, our current implementation of `SELECT FOR UPDATE` takes some liberties
for better performance.

- Locks are currently [only placed on the indexes scanned by the
  query](https://github.com/cockroachdb/cockroach/issues/57031). If the `SELECT
  FOR UPDATE` query never reads from the primary index of the table, it will not
  place a lock there. This could prevent a `SELECT FOR UPDATE` query from
  correctly blocking an `UPDATE` if the `SELECT FOR UPDATE` and the `UPDATE`
  touch disjoint indexes.
- Locks are acquired [during the initial index scans of the
  query](https://github.com/cockroachdb/cockroach/issues/75457), even if some
  rows are later eliminated (e.g. by a filter or a join). This could cause us to
  acquire unnecessary locks, potentially causing artificial contention.
- To avoid this artificial contention, locks are sometimes [not
acquired](https://github.com/cockroachdb/cockroach/blob/48ef0d89e6179c0d348a5236ad308d81fa392f7c/pkg/sql/opt/exec/execbuilder/mutation.go#L987-L1009)
  at all. This could prevent `SELECT FOR UPDATE` from working in some cases.
- As described in [Reliability and Enforcement](#reliability-and-enforcement),
  locks are best-effort, and may not persist until commit for various reasons.

Under Read Committed these shortcuts could cause incorrect query execution. To
fix them, when necessary we will add an extra locking join to the top of the
query plan instead of locking during the initial row fetch. This will typically
be an index join or a lookup join to the primary index of the table to lock (or
multiple joins in the case of multiple tables to lock).

This locking join will acquire fully-replicated locks to ensure the locks
persist until commit, as described in [Reliability for Preview
Release](#reliability-for-preview-release). The locking join will return a
`WriteTooOld` error if there have been any new versions committed to locked rows
after the statement read snapshot.

#### Optimizer Locking Alternatives

There are several alternative methods the optimizer could use to produce this
extra locking join for `SELECT FOR UPDATE`:
1. Add a new `Lock` operator.
2. Add a locking property to the exsting `Select` operator.
3. Add a new `LockedSelect` operator.
4. Use a physical property enforcer.

##### Alternative: Lock Operator

TODO(michae2): describe lock operator alternative

##### Alternative: Locking Property in Select

TODO(michae2): describe locking property in select alternative

##### Alternative: LockedSelect Operator

TODO(michae2): describe lockedselect operator alternative

##### Alternative: Physical Property Enforcer

TODO(michae2): describe physical property enforcer alternative

#### Optimizer Change for Preview Release

For the initial version of Read Committed 

#### Locking Individual Column families

Narrowing lock scope to an individual column family of a row can help ensure
that the performance benefits of multiple column families are realized in
workloads with contention. For very simple `SELECT FOR UPDATE` queries, our
current implementation is able to lock an individual column family of a row,
rather than every column family, depending on how the initial row fetch of the
`SELECT FOR UPDATE` is constrained.

With the changes for Read Committed, we expect that `SELECT FOR UPDATE` will be
able to lock only the necessary individual column families in more cases. This
is because the extra locking join will be able to use column-family-tight spans
in cases where the initial row fetch cannot.

#### Write-Write Version Conflicts

As discussed in [Write-Write Conflict
Handling](#write-write-conflict-handling-or-lost-update-intolerance), `SELECT
FOR UPDATE` statements can experience write-write version conflicts if new
versions of rows are discovered after acquiring locks. PostgreSQL uses a special
`EvalPlanQual` mode to handle these write-write version conflicts, which
re-evaluates some of the query logic on the new version of each locked row. We
will not implement an EPQ mode. Instead, on discovering new committed versions,
the locking join will fail with a `WriteTooOld` error which will cause the
statement to retry.

### Reading Mutation Statements (`INSERT ON CONFLICT`, `UPDATE`, `DELETE`, etc.)

"Reading mutation statements" are DML statements that both read from and write
to the database, such as most `UPDATE` statements. Under Serializable isolation
the read sets of these statements are validated at transaction commit time, to
avoid write skew. In our current implementation, reading mutation statements
sometimes acquire implicit row locks to try and avoid retries, but as mentioned
previously these locks are not needed for correctness.

Surprisingly, query planning and execution for reading mutation statements do
not need to change for Read Committed, despite their potential to incur
write-write conflicts. This is because write-write version conflicts will be
detected when these statements write new versions of each row (lay down
intents). Any committed version newer than the mutation statement's read
snapshot will generate a `WriteTooOld` error, causing at least one of the
conflicting statements to retry, so mutation statement execution can remain
unaware of write-write conflict handling.

This means that, as for `SELECT FOR UPDATE`, we will not implement EPQ mode for
mutation statements. Instead we will rely on statement retries to handle
write-write conflicts.

And as described in [Reliability for Preview
Release](#reliability-for-preview-release), mutation statements can continue to
use unreplicated locks during their initial row fetch, because the initial
unreplicated locks do not need to persist until commit time for
correctness.

FK checks performed at the end of mutation statements, however, will have to use
replicated locks to ensure we maintain FK constraints. (See [system-level SQL
constraints](#system-level-sql-constraints) below.)

### Blind Mutation Statements (`INSERT`, `UPSERT`, `DELETE`, etc.)

"Blind mutation statements" are DML statements that write to the database
without reading, such as most `UPSERT` statements. These statements cannot incur
lost updates, so it should almost never be necessary to retry these statements
at the conn_executor level. Fortunately, these statements can benefit from use
of server-side read refreshes, [as outlined
previously](#retry-avoidance-through-read-refreshes).

### Common Table Expressions and User-Defined Functions

Planning and execution for CTEs and UDFs does not need to change for Read
Committed isolation.

CTEs, and both `STABLE` and `IMMUTABLE` UDFs will perform their reads at the
same read snapshot as the main statement. Any locking or mutation statements
they contain will perform in the manner described above. If a CTE or a UDF
encounters a `WriteTooOld` error due to a write-write conflict, the entire main
statement will retry.

`VOLATILE` UDFs will perform their reads at the same read snapshot as the main
statement, but with [a later sequence
number](https://github.com/cockroachdb/cockroach/blob/08ac8fde23e42cf26677a3dfd1c3a0fb60e40f65/pkg/sql/routine.go#L44-L59),
allowing them to read writes performed by the main statement, but not writes
committed from later transactions. If a `VOLATILE` UDF encounters a
`WriteTooOld` error it will also cause the main statement to retry.

### System-level SQL Constraints

System-level constraints must be enforced correctly regardless of isolation
level. Under Read Committed isolation, this will require holding replicated
locks for all constraint checks and cascades (whether executed inline or as
post-queries).

[Foreign key checks](https://github.com/cockroachdb/cockroach/issues/80683) will
change to use `SELECT FOR SHARE` locks.

Foreign key cascades will not change. The intents they write will function as
replicated locks for the duration of the transaction.

`UNIQUE` checks will not change. These checks are always handled through a
combination of `InitPut` or `CPut` commands and careful key encoding when
mutating a row. The intents written by the row mutation will function as
replicated locks.

`UNIQUE WITHOUT INDEX` checks cannot easily be implemented correctly under Read
Committed using only row locking, because they depend on the non-existence of a
span of rows. Initially we will disallow the enforcement of `UNIQUE WITHOUT
INDEX` checks in transactions run under Read Committed, so these transactions
will be unable to insert into tables with this form of constraint. Consequently,
`REGIONAL BY ROW` tables will be inaccessible to Read Committed transactions in
the initial preview. Eventually we will allow `UNIQUE WITHOUT INDEX` checks if
the check can be built using single-row spans (i.e. if there is an index which
only has enum columns before the `UNIQUE WITHOUT INDEX` columns). This will
require taking `SELECT FOR SHARE` locks on non-existent rows.

`CHECK` constraint checks will have to acquire `SELECT FOR SHARE` locks on any
unmodified column families if the constraint references multiple column
families. This may negate the benefit of column families in some cases.

### Transaction Orchestration

TODO(nvanbenschoten): Work with Rafi to flesh this section out. Mention
connExecutor and changes for:
- Updating txn read snapshot on each statement. Done through existing calls to Txn.Step
- Retrying statements on retry errors. Needs to error handling logic.

## Configuration

Configuration of the Read Committed isolation level will be handled through the
standard session variable infrastructure.

Individual transactions can be configured to run at the Read Committed isolation
level using any of the following configuration methods:
```sql
-- set isolation in BEGIN statement
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- set isolation of current transaction (statement style)
BEGIN; SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- set isolation of current transaction (variable style)
BEGIN; SET transaction_isolation = 'read committed';
```

The default isolation level for all future transactions can be set to Read
Committed using any of the following configuration methods:
```sql
-- set default isolation level (statement style)
SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- set default isolation level (variable style)
SET default_transaction_isolation = 'read committed';

-- set default isolation level (connection parameter style)
cockroach sql -–url='postgres://root@hostname:26257/db?options=-c default_transaction_isolation=read%20committed'
```

Like in PostgreSQL, READ UNCOMMITTED will also be accepted by all of these
configurations and will map to READ COMMITTED.

## Configuration Introspection

Observability into the current transaction's isolation level and the session's
default isolation level will both also be handled through the standard session
variable infrastructure:
```sql
SHOW transaction_isolation;

SHOW default_transaction_isolation;
```

### Configuration Migration

Currently, CockroachDB accepts many of these configurations. However, it ignores
them and provides applications with Serializable isolation instead. This poses a
risk that applications which use CockroachDB today are unwittingly asking for
Read Committed, either directly or indirectly (e.g. through an ORM). These same
applications may not be prepared to suddenly jump to Read Committed when
upgrading CockroachDB to v23.2.

To hedge against this risk and provide users that fall into this situation with
quick remediation, a hidden cluster setting will be introduced to disable the
use of Read Committed and retain the existing mappings in all configurations
from "read committed" isolation to "serializable" isolation. The cluster setting
can be removed in a future release.

## Observability

Details about transaction isolation will be added to the following three system
tables:
```
crdb_internal.node_transactions
crdb_internal.cluster_transactions
crdb_internal.cluster_locks
```

Surfacing transaction isolation and its impact on transaction contention in the
DB console is important, but it is beyond the scope of this RFC.

## Miscellaneous Interactions

### With Serializable Isolation

Serializable and Read Committed transactions can coexist in relative harmony. As
described earlier, the isolation levels differ primarily in their tolerance to
write skew and in their selection of read snapshot(s). These differences are
internal concerns that do not affect other concurrent transactions, so mixing
transaction isolation levels will not cause problems. Regardless of isolation
level, transactions will continue to read from consistent snapshots of the
system and the commit of any transaction will remain atomic to all other
transactions.

A serializable history will still be possible to construct from all transactions
running at Serializable isolation. However, it will not always be possible to
place Read Committed transactions into that history.

The one interaction between the two isolation levels that deserves discussion is
write-read conflicts where the writer is a Serializable transaction and the
reader is a Read Committed transaction. We discussed earlier that we intend to
make write-read conflicts non-blocking for all isolation levels. However, we
will only initially make that change for Read Committed transactions because
there are fewer consequences to doing so. During this intermediate period, we
could either block the Read Committed transaction on the Serializable
transaction to avoid pushing the Serializable transaction and forcing it to
refresh, or we could make the conflict non-blocking to avoid unexpectedly
blocking the Read Committed transaction. We chose the latter option to ensure
that reads in Read Committed transactions never block, potentially at the cost
of Serializable transaction retries.

### With Implicit Transactions

Implicit transactions are single-statement transactions without `BEGIN` or
`COMMIT` statements. The transactions benefit from [automatic
retries](https://www.cockroachlabs.com/docs/stable/transactions.html#automatic-retries)
thanks to the CockroachDB SQL gateway hearing about the entire transaction at
once.

Read Committed's "per-statement Snapshot isolation" transaction model implies
that these implicit transactions will behave identically to a potential Snapshot
isolation level. Namely, they will operate at a single read snapshot and will
permit skew between their commit timestamp and this read snapshot's timestamp.

### With One-Phase Commit

One-Phase Commit is a fast-path for mutations that perform writes to a single
range and issue these writes in a batch with their EndTxn request. These
transactions can avoid two-phase commit, bypassing the use of intents or a
transaction record and writing committed values directly.

Read Committed transactions will have access to this fast-path. The only
relevant difference between Read Committed transactions and Serializable
transactions is that Read Committed transactions have a more lenient commit
condition (skew between their read and write timestamps are permitted) and the
one-phase commit fast-path must be made aware of this.

### With AS OF SYSTEM TIME

The [`AS OF SYSTEM
TIME`](https://www.cockroachlabs.com/docs/stable/as-of-system-time.html) clause
can be added to individual statements or to entire read-only transactions,
instructing CockroachDB to read at a historical MVCC snapshot. This is an
important feature that underpins follower reads.

In some regard, such a feature is incompatible with the Read Committed isolation
level, which captures a new MVCC snapshot for each statement in a transaction.
For this reason, PostgreSQL disallows the use of `SET TRANSACTION SNAPSHOT`
(analogous to `AS OF SYSTEM TIME`) in Read Committed transactions. When issued,
an error is returned:
```
ERROR:  a snapshot-importing transaction must have isolation level SERIALIZABLE or REPEATABLE READ
```

Proscribing `AS OF SYSTEM TIME` in Read Committed transactions would cause
confusion and inconvenience for users of CockroachDB, especially when attempting
to use follower reads. Instead of banning it, the syntax will be accepted in
`BEGIN` and `SET` statements and the transaction will be promoted to a
read-only, Serializable transaction[^6] with the specified fixed read timestamp
across all statements, which matches the user's intent. Such transactions are
not subject to retry errors.

[^6]: read-only, Serializable isolation transactions are equivalent to
    read-only, Snapshot isolation transactions.

As an extension, the syntax can also be accepted on individual `SELECT`
statements in a Read Committed transaction. This will instruct the transaction
to run the individual statement at the specified fixed read timestamp. The
utility of this may be limited, but the behavior matches expectations of Read
Committed transactions.

### With Non-Standard SELECT FOR UPDATE Wait Policies

TODO(michae2): write this after the Explicit Row-Level Locking (SELECT FOR
UPDATE) above.

#### NOWAIT

#### SKIP LOCKED

### With Schema Changes

The schema change protocol runs partially within user transactions, so special
care must be taken before permitting Read Committed transactions to perform
schema changes. The protocol depends on a serialization of versions for each
individual descriptor and on cross-descriptor consistency. Consequently, lost
updates on a single descriptor and write skew across descriptors must both be
prohibited.

The use of explicit row-level locking (`SELECT FOR SHARE`) during descriptor
lookups may be sufficient to eliminate these hazards. However, for the preview
release of Read Committed, schema changes in Read Committed transactions will be
disabled.

### With Column Families

TODO(nvanbenschoten): prove that writes to disjoint column families cannot cause
index corruption.

### With CDC

Read Committed has no meaningful interaction with CDC. Committed values from
Read Committed transactions will be published on rangefeed subscriptions during
Raft log application like in any other transaction.

### With Multi-Region

Read Committed has only limited interactions with multi-region. At a high level,
this is because Read Committed weakens isolation guarantees but not consistency
guarantees. Consistency is where much of the cost of multi-region comes from.

With that said, Read Committed transactions run in a multi-region deployment
will still see some benefit from their weaker isolation guarantees. For example,
Read Committed transactions avoid refreshing reads on commit, which can be
costly in a read-write transaction that reads from a remote leaseholder.

On the other hand, the high network latencies in a multi-region cluster may also
expand the timing window for isolation-related anomalies to occur in Read
Committed transactions. Anomalies that may be rare in single-region clusters
(because transactions commit with low latency) may become more common in
multi-region clusters.

TODO(michae2): mention `UNIQUE WITHOUT INDEX` constraints

### With Multi-Tenancy

Read Committed has no meaningful interaction with multi-tenancy. Tenants can run
transactions at any isolation level without concern.

However, the introduction of non-blocking write-read conflicts will simplify the
resolution of [#71946](https://github.com/cockroachdb/cockroach/issues/71946).

# Testing

To gain confidence in an implementation of Read Committed, testing will be
needed at multiple levels. As always, individual changes and components will be
validated using targeted unit tests. However, ensuring transaction isolation
guarantees is a cross-cutting concern, so integration testing across the
database stack will be paramount.

To verify the integrity of changes made for Read Committed to the key-value
layer, [kvnemesis](https://github.com/cockroachdb/cockroach/blob/fadd137a98540a317a379e938a7545fa70590cb4/pkg/kv/kvnemesis/doc.go)
will be enhanced in four ways:
1. The framework will be updated to run transactions at weak isolation levels.
   The framework's validator will be taught to permit write skew for
   transactions run below Serializable isolation.
2. The framework will be updated to step read committed transactions through
   multiple read snapshots, mimicking the style of use expected from SQL when
   encountering statement boundaries.
3. The framework's generator will be updated to issue locking reads with a
   SHARED lock strength.
4. The framework's generator will be updated to issue locking reads with a
   REPLICATED lock durability. The framework's validator will then be taught to
   expect stronger (serializable-like) isolation guarantees from keys locked by
   a transaction with replicated locks.

Integration testing of Read Committed that exercises SQL will be also
introduced.

CockroachDB's existing suite of logictests will be extended to run transactions
under Read Committed isolation. Very few logictests exercise concurrency, so
they should behave no different than if run under Serializable isolation. The
few logictests that do manipulate multiple session concurrency will need to be
updated with isolation-specific expectations.

[Elle](https://github.com/jepsen-io/elle) is a transactional consistency checker
for black-box databases, which supports weak isolation levels has been
integrated into [Jepsen](https://github.com/jepsen-io/jepsen). CockroachDB
already integrates Jepsen into its nightly test suite. This testing will be
expanded to exercise Read Committed and validate correctness.

[Hermitage](https://github.com/ept/hermitage) is a test suite that consists of
transaction histories that simulates various concurrency issues. The test suite
will be manually run against CockroachDB's Read Committed isolation level to
ensure that it is subject to expected concurrency anomalies and not subject to
unexpected anomalies. It will also be updated to reflect the addition of new
isolation levels to CockroachDB.

PostgreSQL's [isolation test
suite](https://github.com/postgres/postgres/blob/36f40ce2dc66f1a36d6a12f7a0352e1c5bf1063e/src/test/isolation/README)
contains a set of tests for concurrent transaction behaviors running at
different isolation levels in PostgreSQL. This test suite will be hooked up to
CockroachDB. While the behavior of transactions differs between CockroachDB and
PostgreSQL in multiple ways, it will be useful to see how much of the test suite
passes against CockroachDB and to understand precisely why the tests that fail
do so.

Nightly testing of TPC-C will be adapted to run at the Read Committed isolation
level. When doing so, transaction retry loops will be removed from the workload
to validate that retry errors are rare or non-existent under weak isolation.
TPC-C provides a useful sandbox to test weaker isolation levels because it
contains three moderately complex read-write transactions, two read-only
transactions, a diverse schema with referential integrity constraints, and
twelve post-workload consistency checks.

Finally, existing workloads that run against Read Committed in other DBMS
systems will be solicited from customers. Where possible, these will be run
against CockroachDB's implementation of Read Committed to validate correctness,
sufficient completeness of the Read Committed implementation, and expected
properties like no retry errors and non-blocking reads.

# Performance

TODO(nvanbenschoten): from @bdarnell:
> I'd like to see a section on performance. There are a number of ways that this
> proposal affects performance, both good (less wasted work doing retries, no
> pre-commit span refreshes), and bad (lock replication, savepoint overhead,
> explicit locks for FK checks). Aside from the correctness concerns, how can we
> characterize the net expected performance of the two isolation levels?
>
> There are also more subtle performance-related risks: Many applications have
> isolation-related bugs that go undetected because the app simply doesn't get
> enough traffic to hit the necessary race conditions. If your sub-millisecond
> operations suddenly start to take longer because foreign key checks now
> involve multiple RPCs, these latent bugs may be exposed.

# Variation from PostgreSQL

The form of the Read Committed isolation level presented here is strictly
stronger than what is found in PostgreSQL. The difference between these two
implementations is in how they handle write-write conflicts during mutation
statements.

The "Per-Statement Snapshot Isolation" model presented here retries individual
statements on write-write conflicts, ensuring that within a single statement, no
lost updates are permitted. This stronger model avoids certain anomalies that
could allow a mutation statement to perceive non-atomic commits of other
transactions. In exchange, this stronger model is subject to internal
per-statement retries.

The "Postgres-Compatible Intra-Mutation Consistency" model breaks mutation
statements into a search phase, a locking phase, and a predicate re-evaluation
phase. This decomposition avoids any per-statement retries. In exchange, it can
permit intra-statement lost updates and other anomalous behavior when
write-write conflicts are experienced.

A more complete comparison between the two models is presented in the
[appendix](#appendix-postgres-compatible-intra-mutation-consistency). This
comparison is accompanied by a discussion of how Postgres-Compatible
Intra-Mutation Consistency might be implemented in CockroachDB.

# Drawbacks

The primary drawback of introducing the Read Committed isolation level is that
it provides users with a tool to weaken the correctness guarantees of
CockroachDB. Unwitting users may employ Read Committed for performance reasons
without understanding the trade-offs, leading to unexpected correctness bugs or
data corruption.

This is a real risk. Yet, the risk of not providing users with this
configuration and failing to support a large class of applications is greater.
Instead, we will combat this concern with ample documentation to help users make
well-informed decisions about the performance/correctness trade-off.

# Unresolved questions

## Should we implement Snapshot isolation at the same time?

As alluded to in this proposal, the changes needed to implement Snapshot
isolation are contained within the changes needed to implement Read Committed.
As a result, it will be a small lift for us to expose Snapshot isolation after
making these changes. Conceptually, Snapshot isolation is identical to Read
Committed except that it chooses _not_ to advance a transaction's read snapshot
at each statement boundary.

We propose not to expose this isolation level immediately to keep engineering
efforts focused and to reduce the scope of testing work needed to gain
confidence in these changes. Still, we note that doing so will be a small lift
if/when we decide that such an isolation level is needed.

## If we implement Snapshot isolation, should we call it Repeatable Read?

Strictly speaking, the two isolation levels are not the same. Repeatable Read
(PL-2.99 in Adya) permits Phantom Reads but does not permit Write Skew. Snapshot
isolation (PL-SI in Adya) does not permit Phantom Reads but does permit Write
Skew.

However, there is ample precedent for conflating the two with minimal concern.
Chiefly, PostgreSQL itself implements a form of Snapshot isolation and calls it
Repeatable Read to remain ANSI SQL compliant. Therefore, if we decide to
implement Snapshot isolation, we propose that we also call it Repeatable Read.

## Should Read Committed become the new default isolation level in CockroachDB?

We do not plan to immediately change the default isolation level in CockroachDB.
If such a decision is made at some later point, it will be separate from the
initial design and implementation effort.

# Appendix: Examples

In the following examples, consider the schema:
```sql
create table kv (k int primary key, v int);
```

### SELECT behavior (without FOR UPDATE)

```sql
truncate table kv;
insert into kv values (1, 5);
```

| Client 1 | Client 2 |
| -------- | -------- |
| `begin transaction isolation level read committed;` | |
| | `begin transaction isolation level read committed;` |
| `select * from kv;`<br>`k \| v`<br>`--+---`<br>`1 \| 5` | |
| | `insert into kv values (2, 6);` |
| `select * from kv;`<br>`k \| v`<br>`--+---`<br>`1 \| 5` | |
| `insert into kv values (3, 7);` | |
| `select * from kv;`<br>`k \| v`<br>`--+---`<br>`1 \| 5`<br>`3 \| 7` | |
| | `commit;` |
| `select * from kv;`<br>`k \| v`<br>`--+---`<br>`1 \| 5`<br>`2 \| 6`<br>`3 \| 7` | |
| `commit;` | |

### SELECT FOR UPDATE behavior

```sql
truncate table kv;
insert into kv values (0, 5), (1, 5), (2, 5), (3, 5), (4, 1);
```

| Client 1 | Client 2 |
| -------- | -------- |
| `begin transaction isolation level read committed;` | |
| | `begin transaction isolation level read committed;` |
| | `insert into kv values (5, 5);` |
| | `update kv set v = 10 where k = 4;` |
| | `delete from kv where k = 3;` |
| | `update kv set v = 10 where k = 2;` |
| | `update kv set v = 1 where k = 1;` |
| | `update kv set k = 10 where k = 0;` |
| `select * from kv where v >= 5 for update;`<br>`... waits ...` | |
| | `commit;` |
| `... waiting completes`<br>`k \| v`<br>`--+---`<br>`2 \| 10`<br>`4 \| 10`<br>`5 \| 5`<br>`10 \| 5` | |
| `commit;` | |

### UPDATE and DELETE behavior

```sql
truncate table kv;
insert into kv values (0, 5), (1, 5), (2, 5), (3, 5), (4, 1);
```

| Client 1 | Client 2 |
| -------- | -------- |
| `begin transaction isolation level read committed;` | |
| | `begin transaction isolation level read committed;` |
| | `insert into kv values (5, 5);` |
| | `update kv set v = 10 where k = 4;` |
| | `delete from kv where k = 3;` |
| | `update kv set v = 10 where k = 2;` |
| | `update kv set v = 1 where k = 1;` |
| | `update kv set k = 10 where k = 0;` |
| `update kv set v = 100 where v >= 5;`<br>`... waits ...` | |
| | `commit;` |
| `... waiting completes` | |
| `select * from kv`<br>`k \| v`<br>`--+---`<br>`1 \| 1`<br>`2 \| 100`<br>`4 \| 100`<br>`5 \| 100`<br>`10 \| 100` | |
| `commit;` | |

### INSERT behavior

Insert a new key that has just been changed by another transaction:

```sql
truncate table kv;
insert into kv values (1, 1);
```

| Client 1 | Client 2 |
| -------- | -------- |
| `begin transaction isolation level read committed;` | |
| | `begin transaction isolation level read committed;` |
| | `update kv set k = 2 where k = 1;` |
| `insert into kv values (2, 1);`<br>`... waits ...` | |
| | `commit;` |
| `... waiting completes`<br>`ERROR: duplicate key value violates unique constraint "kv_pkey"` | |
| `rollback;` | |

Insert a new key that has just been changed by another transaction, with `ON CONFLICT`:

```sql
truncate table kv;
insert into kv values (1, 1);
```

| Client 1 | Client 2 |
| -------- | -------- |
| `begin transaction isolation level read committed;` | |
| | `begin transaction isolation level read committed;` |
| | `update kv set k = 2 where k = 1;` |
| `insert into kv values (2, 1) on conflict (k) do update set v = 100;`<br>`... waits ...` | |
| | `commit;` |
| `... waiting completes` | |
| `select * from kv`<br>`k \| v`<br>`--+---`<br>`2 \| 100` | |
| `commit;` | |

Insert an old key that has been removed by another transaction:

```sql
truncate table kv;
insert into kv values (1, 1);
```

| Client 1 | Client 2 |
| -------- | -------- |
| `begin transaction isolation level read committed;` | |
| | `begin transaction isolation level read committed;` |
| | `update kv set k = 2 where k = 1;` |
| `insert into kv values (1, 1);`<br>`... waits ...` | |
| | `commit;` |
| `... waiting completes` | |
| `select * from kv`<br>`k \| v`<br>`--+---`<br>`1 \| 1`<br>`2 \| 1` | |
| `commit;` | |

Insert an old key that has been removed by another transaction, with `ON CONFLICT`:

```sql
truncate table kv;
insert into kv values (1, 1);
```

| Client 1 | Client 2 |
| -------- | -------- |
| `begin transaction isolation level read committed;` | |
| | `begin transaction isolation level read committed;` |
| | `update kv set k = 2 where k = 1;` |
| `insert into kv values (1, 1) on conflict (k) do update set v = 100;`<br>`... waits ...` | |
| | `commit;` |
| `... waiting completes` | |
| `select * from kv`<br>`k \| v`<br>`--+---`<br>`1 \| 1`<br>`2 \| 1` | |
| `commit;` | |

# Appendix: Proof of Correctness

We demonstrate that this model for Read Committed is stronger than[^7]
Berenson's characterization of Read Committed and Adya's characterization of
Read Committed (PL-2). We also demonstrate that it is equivalent to[^8] ANSI
SQL's characterization of Read Committed (Degree 2).

[^7]: Using [Berenson et al.'s
    definition](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf)
    of stronger than, denoted `L1 » L2`.
[^8]: Using [Berenson et al.'s
    definition](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf)
    of equivalent to, denoted `L1 == L2`.

Berenson defines Read Committed as proscribing Dirty Write (P0) and Dirty Read
(P1) and permitting Non-repeatable Read (P2), Phantom Read (P3), Lost Update
(P4), and Read/Write Skew (A5). Our transaction model does not permit P0 because
transactions are not permitted to overwrite uncommitted writes from other
transactions. Our transaction model also does not permit P1 because all reads
are served out of the per-statement reads snapshots established at the beginning
of each statement. However, P3 and P4 are both allowed because subsequent
statements can use different read snapshots, and so different statements in the
same transaction can see different data. A5 is also permitted, because read
locks are not acquired during reads, allowing both forms of skew to occur.

|              | P0<br>Dirty Write | P1<br>Dirty Read | P2<br>Fuzzy Read | P3<br>Phantom | P4C<br>Cursor Lost Update | P4<br>Lost Update      | A5A<br>Read Skew | A5B<br>Write Skew |
| ------------ | ----------------- | ---------------- | ---------------- | ------------- | ------------------------- | ---------------------- | ---------------- | ----------------- |
| Berenson RC  | Not Possible      | Not Possible     | Possible         | Possible      | Possible                  | Possible               | Possible         | Possible          |
| CRDB RC      | Not Possible      | Not Possible     | Possible*        | Possible*     | Possible*                 | Possible*              | Possible*        | Possible          |

While permitted by this formalization, our transaction model only allows some
forms of P2, P3, P4, P4C, and A5A. Read anomalies are not permitted within a
statement because each statement operates using a consistent snapshot.
Similarly. lost updates between the reads and writes in a statement are not
permitted because a first writer wins conflict resolution policy is applied to
such conflicts. However, all of these anomalies are permitted across statements.

Adya defines Read Committed (PL-2) as proscribing G0 (Write Cycles), G1a
(Aborted Reads), G1b (Intermediate Reads), and G1c (Circular Information Flow)
and permitting G-single (Single Anti-dependency Cycles), G2-item (Item
Anti-dependency Cycles), and G2 (Anti-dependency Cycles). Our transaction model
does not permit G0 because transactions are not permitted to overwrite
uncommitted writes from other transactions. Our transaction model also does not
permit G1a, G1b, or G1c because all reads are served out of the per-statement
reads snapshots established at the beginning of each statement. These snapshots
contain writes from committed transactions, and only the final version of those
writes on any given row. However, G-single, G2-item, and G2 are all permitted
because our model allows a Read Committed transaction's read timestamp(s) to
skew from its commit timestamp ("Write Skew Tolerance").

|           | G0<br>Write Cycles | G1a<br>Aborted Reads | G1b<br>Intermediate Reads | G1c<br>Circular Information Flow | G-single<br>Single Anti-dependency Cycles | G2-item<br>Item Anti-dependency Cycles | G2<br>Anti-dependency Cycles |
| --------- | ------------------ | ---------------- | ---------------- | ------------- | ------------- | ---------- | ----------- |
| Adya PL-2 | Not Possible       | Not Possible     | Not Possible     | Not Possible  | Possible      | Possible   | Possible    |
| CRDB RC   | Not Possible       | Not Possible     | Not Possible     | Not Possible  | Possible*     | Possible   | Possible    |

As with the previous analysis, our transaction model is stronger than required
by Adya because it does not permit G-single within a single statement.

Finally, ANSI SQL defines Read Committed as proscribing Dirty Read (P1) and
permitting Non-repeatable Read (P2) and Phantom Read (P3). As demonstrated
above, out transaction model does not permit P1 but does permit P2 and P3.
Therefore, it is _equivalent to_ ANSI SQL's definition of Read Committed.

|         | P1<br>Dirty Read | P2<br>Non-repeatable Read | P3<br>Phantom Read |
| ------- | ---------------- | ------------------------- | ------------------ |
| ANSI RC | Not Possible     | Possible                  | Possible           |
| CRDB RC | Not Possible     | Possible                  | Possible           |

# Appendix: Postgres-Compatible Intra-Mutation Consistency

This RFC proposed a "Per-Statement Snapshot Isolation" transaction model for
Read Committed. An alternative approach considered was a "Postgres-Compatible
Intra-Mutation Consistency" transaction model.

## Comparison

The difference between these two models is how they handle write-write conflicts
during mutation statements.

The "Per-Statement Snapshot Isolation" model presented here retries individual
statements on write-write conflicts, ensuring that within a single statement, no
lost updates are permitted. This stronger model avoids certain anomalies that
could allow a mutation statement to perceive non-atomic commits of other
transactions. In exchange, this stronger model is subject to internal
per-statement retries.

The "Postgres-Compatible Intra-Mutation Consistency" model breaks mutation
statements into a search phase, a locking phase, and a predicate re-evaluation
phase. This decomposition avoids any per-statement retries. In exchange, it can
permit intra-statement lost updates and other anomalous behavior when
write-write conflicts are experienced.

## Write-Write Conflict Handling

Per-statement read snapshots are one major difference between a Read Committed
implementation and a Serializable (or hypothetical Snapshot) implementation. The
other major difference is in the handling of write-write conflicts. Where a
Serializable transaction would throw a serialization error on a write-write
conflict, Read Committed transactions wait for the conflict to resolve (e.g. the
conflicting transaction to commit or abort and release locks) and then continue
running.

To understand this, we first decompose the definition of a write-write conflict
as follows:

**Write-Write Locking Conflict**: any case where a transaction attempts to lock
or write to a key that is locked with an exclusive lock by a different
transaction, where intents are considered to be a combination of an exclusive
lock and a provisional value.

**Write-Write Version Conflict**: any case where a transaction attempts to lock
or write to a key that has a _committed version_ with an MVCC timestamp greater
than the locking/writing transaction's current read snapshot.

We define _write-write version conflict_ in terms of a transaction's "current
read snapshot" (i.e. `txn.ReadTimestamp`) to afford flexibility in the
definition to transactions that change their read snapshot across their
execution. For example, Read Committed transactions advance their read snapshot
on each statement boundary, so a committed version that would cause a
write-write version conflict for one statement may not cause a write-write
version conflict for a later statement in the same transaction.

Read Committed transactions handle _write-write locking conflicts_ identically
to Serializable transactions. The prospective locker
[waits](#blocking-write-write-conflicts) for the existing exclusive lock to be
released before acquiring it. In cases where multiple transactions wait for
exclusive access to the same key, they form an orderly queue through the
`lockWaitQueue` mechanism.

Once a Read Committed transaction has navigated any potential _write-write
locking conflict_, it may experience a _write-write version conflict_. In such
cases, locking read operations (e.g. `Get(key, Exclusive)`) return the latest
committed version of the key, regardless of the reader's read snapshot. Writing
operations (e.g. `Put(key, value)`) place an intent on the key with a version
timestamp above the latest committed version.

In both cases, the operations advance the locker/writer's `WriteTimestamp` above
the committed version's timestamp. Recall that while a transaction is running,
the `WriteTimestamp` serves as its provisional commit timestamp, forming a lower
bound on the MVCC timestamp that the transaction can commit at.

#### Impact of Write-Write Conflict Handling on KV API

This necessitates a change in the KV API's handling of write-write version
conflicts between transaction isolation levels. This difference is summarized
below:

| Operation type   | SI Read Version   | SI WW Version Conflict     | RC Read Version   | RC WW Version Conflict     |
| ---------------- | ----------------- | -------------------------- | ----------------- | -------------------------- |
| Non-locking Read | [txn.ReadTimestamp](https://github.com/cockroachdb/cockroach/blob/8e24570fa366ed038c6ae65f50db5d8e22826db0/pkg/storage/pebble_mvcc_scanner.go#L791) | N/A                        | txn.ReadTimestamp | N/A                        |
| Locking Read     | [txn.ReadTimestamp](https://github.com/cockroachdb/cockroach/blob/8e24570fa366ed038c6ae65f50db5d8e22826db0/pkg/storage/pebble_mvcc_scanner.go#L791) | [WriteTooOldError](https://github.com/cockroachdb/cockroach/blob/8e24570fa366ed038c6ae65f50db5d8e22826db0/pkg/storage/pebble_mvcc_scanner.go#L832)           | latest version    | txn.WriteTimestamp.Forward |
| Write-Only       | N/A               | [txn.WriteTimestamp.Forward](https://github.com/cockroachdb/cockroach/blob/8e24570fa366ed038c6ae65f50db5d8e22826db0/pkg/kv/kvserver/replica_evaluate.go#L371), then [WriteTooOldError](https://github.com/cockroachdb/cockroach/blob/8e24570fa366ed038c6ae65f50db5d8e22826db0/pkg/kv/kvclient/kvcoord/txn_interceptor_span_refresher.go#L259) | N/A               | txn.WriteTimestamp.Forward | 
| Read-Write       | [txn.ReadTimestamp](https://github.com/cockroachdb/cockroach/blob/8e24570fa366ed038c6ae65f50db5d8e22826db0/pkg/storage/mvcc.go#L2050) | [WriteTooOldError](https://github.com/cockroachdb/cockroach/blob/8e24570fa366ed038c6ae65f50db5d8e22826db0/pkg/kv/kvserver/replica_evaluate.go#L365)           | latest version    | txn.WriteTimestamp.Forward |

Users of the KV API must be aware of this change in write-write version conflict
handling or they will risk lost updates. Specifically, a non-locking read of a
key followed by a write to that key in the same transaction could ignore a
committed version between the transaction's read and write timestamp.

Users of the KV API must also be aware that this change in write-write version
conflict handling can expose locking read and read-write operations to an
inconsistent snapshot of the system.

We will later see how [SQL handles these
situations](#mutations-update-delete-etc), exploiting the weakened consistency
guarantees to avoid retries while still providing reasonably sound semantics.
However, in general, we recommend that most users of the raw KV API continue to
use Serializable transactions.

## Post-Locking Predicate Re-evaluation

A "Postgres-Compatible Intra-Mutation Consistency" transaction model requires
additional SQL planning and execution logic beyond the "Per-Statement Snapshot
Isolation" model in order to handle the predicate re-evaluation step. The
following proposal was considered as an approach to closely (but not exactly)
match the postgres predicate re-evaluation behavior. The complexity necessary to
fully implement this behavior was a factor in the decision to instead implement
the "Per-Statement Snapshot Isolation" model.

The proposed design hoped to achieve a few desirable properties:
1. There should not be significant overhead for the happy path (no conflicts).
2. CRDB should not exhibit anomalies that are prevented in Postgres.
3. The solution should be general, and shouldn’t require special execution-time
   handling for individual operators (e.g. the Postgres
   [EvalPlanQual](https://github.com/postgres/postgres/blob/0bc726d95a305ca923b1f284159f40f0d5cf5725/src/backend/executor/README#L350-L351)
   step for scans).

### Motivation

Under the "Postgres-Compatible Intra-Mutation Consistency" model, predicate
re-evaluation is necessary becuase a row may be updated between when it is read
and filtered, and when it is locked. Note that this is not a concern when all
predicates can be inlined into a locking scan, because locking and filtering
happen at the same time in this case. Re-evaluation becomes necessary when the
query scans, filters, and then locks rows in separate steps.

### Requirements

Postgres places the following restrictions on the syntax of a query that
performs locking:

1. Locking is [not permitted](https://www.postgresql.org/docs/current/sql-select.html) for
   tables within GROUP BY, HAVING, WINDOW, DISTINCT, UNION, INTERSECT, or EXCEPT
   clauses.
2. In addition, postgres [doesn't allow](https://github.com/postgres/postgres/blob/eae0e20deffb0a73f7cb0e94746f94a1347e71b1/src/backend/optimizer/plan/initsplan.c#L1415-L1442)
   locking to include tables from the null-extended side of an outer join.

Note that this applies both to explicit locking specified using `SELECT FOR
UPDATE` syntax, as well as the implicit locking added for mutations (applied
only to the target table). Because of these restrictions, only mutations, WHERE
clauses, and joins have to be considered for a re-evaluation approach.

### The Approach

Under the re-evaluation approach, a query with locking follows these steps:
1. Execute the main query, as normal.
2. Lock the qualifying rows of any locked tables.
3. Retrieve the most up-to-date values for the locked rows.
4. Logically re-evaluate the query for the rows that changed.

The following sub-sections will specify how these steps might be implemented.

Note that the examples shown will have some details omitted for clarity. Some of
the SQL syntax is simplified for the same reason.

#### Lock Operator

Given a query that scans and then filters rows, locking can be handled by adding
a lookup join onto the primary key of each locking table, which locks each row
and returns its most recent version. This takes advantage of the aforementioned
KV api changes. Re-evaluation is then performed using these updated values.

Consider the following example query:
```
SELECT * FROM customers c 
WHERE NOT EXISTS (SELECT * FROM orders o WHERE o.id = c.order_id)
FOR UPDATE OF c;
```
The rewrite to use a locking lookup join would look approximately like this:
```
SELECT locked.* FROM
(
  SELECT * FROM customers c 
  WHERE NOT EXISTS (SELECT * FROM orders o WHERE o.id = c.order_id)
) INNER LOOKUP JOIN customers locked ON c.id = locked.id
FOR UPDATE OF locked;
```
The remainder of the query (including predicate re-evaluation) would proceed
with the updated values returned by the lookup join. This covers steps (1) and
(2) of the approach.

For convenience, assume that the locking lookup join also projects a boolean
column that indicates whether a given row was updated.

#### WHERE Clause

WHERE clause handling is straightforward; given the updated and locked rows
returned by the locking lookup join, we re-evaluate the WHERE clause predicates
using the updated values.

Consider again the example query:
```
SELECT * FROM customers c 
WHERE NOT EXISTS (SELECT * FROM orders o WHERE c.id = o.cust_id)
FOR UPDATE OF c;
```
The predicate from the WHERE clause would be duplicated, and made to reference
the updated values returned by the locking lookup join from the previous step:
```
SELECT locked.* FROM (...) locked
WHERE NOT EXISTS (SELECT * FROM orders o WHERE o.id = locked.order_id)
```
This would remove any new values of `order_id` that don't pass the predicate.

#### Joins

A locking query can perform INNER or LEFT joins between the locked table and
arbitrary subqueries. Note that the FROM and USING clauses of UPDATE and DELETE
statements perform implicit INNER joins. For now, ignore the possibility of
locking multiple tables.

The difficulty is that the non-locking subqueries can be arbitrarily complex,
and their results can depend on rows from the locking relation for correlated
subqueries. Since the locking relation's rows can observe concurrent updates
during re-evaluation, the result of a correlated subquery can change during
re-evaluation. It may be possible to simplify and decorrelate specific examples,
but (at least currently) we cannot guarantee this in the general case.

Postgres actually re-executes the correlated subqueries with scan nodes rigged
to return the same (single) row that contributed to the original output row (see
[here](https://github.com/postgres/postgres/blob/eae0e20deffb0a73f7cb0e94746f94a1347e71b1/src/backend/executor/execScan.c#L27-L38)).
This differs subtly from fully re-executing the correlated subqueries with
updated values. The behavior does not fit well with CRDB’s vectorized and
distributed execution model, and an implementation would likely impose a
significant maintenance cost. The proposal departs from Postgres here by fully
re-executing correlated subqueries during re-evaluation. Note that non-locked
tables would still read at the old timestamp, and therefore would not observe
concurrent updates.

This is implemented by duplicating the subquery, and joining it back with the
locked query. Take the following example:
```
SELECT * FROM customers c
INNER JOIN orders o
ON c.id = o.cust_id
FOR UPDATE OF c;
```
The duplicated join would look like this:
```
SELECT * FROM (...) locked INNER JOIN (SELECT * FROM orders) o ON locked.id = o.cust_id;
```

##### Left Joins

Handling a LEFT join in the original query only requires changing the duplicated
join to a LEFT join as well. For the example query:
```
SELECT * FROM customers c
LEFT JOIN orders o
ON c.id = o.cust_id
FOR UPDATE OF c;
```
The resulting plan would look like this:
```
SELECT * FROM (...) locked LEFT JOIN (SELECT * FROM orders) o ON locked.id = o.cust_id;
```

##### Multiple Locked Relations

It is possible to lock multiple tables in a `SELECT FOR UPDATE` query. If this
is the case, the locked relations will be connected by INNER joins, since it is
not possible to lock rows in the right input of a LEFT join. This is implemented
by duplicating the INNER join conditions into a combined WHERE clause that
references the updated values. As always, we can skip evaluating the filters for
non-updated rows using a CASE statement. This WHERE clause will then ensure that
the updated join row passes all the join conditions.

Example:
```
SELECT * FROM customers c
INNER JOIN orders o 
ON c.id = o.cust_id
INNER JOIN items i
ON i.id = o.item_id
FOR UPDATE;
```
Note that the `FOR UPDATE` clause here applies to all tables. For this query,
re-evaluation for the join conditions would look like this:
```
SELECT * FROM (...) locked
WHERE c_id = cust_id AND i_id = item_id;
```

Note that this implementation will reproduce Postgres’ behavior where an updated
row will only match with rows that matched before and after the update, only for
the case when multiple relations are locked and joined together. For non-locking
sub-selects/relations, the behavior will still be total re-evaluation instead.

##### Non-Unique Join Keys

So far, this discussion has ignored the case when a join is performed on a
non-unique key. In this case, the re-evaluation algorithm will duplicate
results, since there may be more than one match for a given locked row during
re-evaluation. This problem can be resolved using a DISTINCT operator,
de-duplicating on a key for each locked table.

Example:
```
CREATE TABLE xy (x INT PRIMARY KEY, y INT);
CREATE TABLE ab (a INT PRIMARY KEY, b INT);

SELECT * FROM xy INNER JOIN ab ON y = b FOR UPDATE OF xy;
```
In the above example, the DISTINCT would de-duplicate on column `x`, since it
forms a key for the locked relation `xy`:
```
SELECT DISTINCT ON (x) * FROM (...) locked;
```

#### Mutations

Mutations add implicit locking on updated rows in the target table. This is
logically equivalent to first running a `SELECT FOR UPDATE` in the transaction
before executing the mutation. Therefore, handling mutations can be reduced to
handling the implicit `SELECT FOR UPDATE` that performs the locking.

#### Optimization

It is desirable to avoid re-evaluation for rows that weren't updated. For the
filters used in the re-evaluation step, this is simple: wrap each filter in a
CASE statement that checks whether the row was updated. Example:
```
CASE WHEN updated THEN <evaluate filter> ELSE True END;
```
CASE statements guarantee that a branch is not evaluated unless it is taken, so
this method avoids overhead for filter re-evaluation in the happy case. This
optimization applies to the filters for both WHERE clauses and join ON
conditions.

The other case that must be optimized is re-evaluation of correlated subqueries.
CASE statements cannot be used here, because it is possible for the subquery to
return multiple rows. However, strict UDFs have the required behavior - a
set-returning strict UDF short-circuits and returns no rows when given a NULL
argument.

We can build a UDF that re-evaluates the correlated subquery, and join its
result back to the main query. The UDF parameters will correspond to
outer-column references, and the SQL body to the subquery. We can skip
re-evaluation by adding an extra parameter that is non-NULL when the locked row
changed, and NULL when it did not change. Example:
```
CREATE FUNCTION re_eval_subquery(val1, val2, ..., null_if_not_updated) RETURNS RECORD AS $$
  <correlated subquery, remapped to refer to function parameters>
$$ STRICT LANGUAGE SQL;
```
Note that while the example shows a `CREATE` statement, only the UDF's execution
machinery would be built here; it would not have a descriptor or become visible
to users.

A few corrections are necessary to make the UDF solution work:
1. The old and new values for the subquery must be combined. This can be done
   via another CASE statement.
2. The join must preserve locked (left) rows where the UDF returned no rows,
   since this is the case when there were no updates. This is handled by always
   using a LEFT join. If the original join was an INNER join, we add an extra
   filter to remove rows that were updated and did not pass the join filters.

Example for correlated subquery re-evaluation:
```
SELECT * FROM (...) locked
LEFT JOIN LATERAL (SELECT * FROM re_eval_subquery(locked.val, null_if_not_updated)) sub
ON locked.id = sub.cust_id
[WHERE (NOT updated) OR passed_join_condition]; -- Only for INNER joins.
```

It's unclear how to avoid the overhead of de-duplication for non-unique joins.

#### Summary

Ignoring optimizations, the proposed implementation of the "Postgres-Compatible
Intra-Mutation Consistency" model follows these steps for a query with locking:
1. Execute the original query, taking care to keep the primary key of each
   target table.
2. Perform a lookup join into the primary index for each target table, locking
   each row and returning the most recent values.
3. For each non-locking subquery joined to the target table(s), duplicate the
   subquery, and join it back to the main query.
4. Duplicate the query's WHERE clause, remapping it to refer to updated values,
   and add it to the main query.
5. Duplicate each join condition that joins target tables to one another, and
   add it to the main query's WHERE clause.
6. De-duplicate the result on the key of each target table.

### Limitations

* Complexity - many different steps are required in the algorithm in order to
  handle the various edge cases around handling joins. The model is also more
  complex to reason about and test than "Per-Statement Snapshot Isolation".
* Overhead - in the fast path, the initial scan/filtering must preserve primary
  keys for the locking tables in order to perform the locking lookup join. This
  will add some execution-time overhead and possibly restrict the possible query
  plans. In addition, the slow path is handled via correlated subqueries, which
  can have very poor performance. This may not be unique to the predicate
  re-evaluation design, however.
* Compatibility - Postgres and CRDB handle row identifiers differently - in PG,
  deleting and inserting a row with the same key and a different is not the same
  as updating it. In CRDB, the two scenarios are indistinguishable. This means
  it would be possible for re-evaluation to read a newly inserted row in CRDB
  when it wouldn't be possible in PG. In addition, PG would "follow" an update
  that changes the row's key during re-evaluation, while CRDB would not. In
  addition, the proposal does not exactly replicate Postgres behavior for
  correlated joins with non-locking subqueries.
