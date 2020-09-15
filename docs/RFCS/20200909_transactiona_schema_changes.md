- Feature Name: Transactional Schema Changes
- Status: draft
- Start Date: 2020-09-09
- Authors: Andrew Werner, Lucy Zhang
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [#42061], [#43057]

# Summary

CockroachDB's online schema change architecture necessitates step through 
multiple descriptor states. All of these state changes are asynchronous after
rather than before the transaction commits, preventing that writing
transaction from interacting with schema elements undergoing changes and opening
the door to non-atomicity in the face of failure. This proposal seeks to pull
the steps of an online schema change to, by default, occur during the execution
of the transaction in order to remedy the above shortcomings and provide schema
changes which cohere with the transactional model. New syntax is proposed to
retain the benefits of the existing asynchronous operations.

# Motivation

The lack of transactional schema changes boils down to two fundamental problems:

1. When a user transaction commits, its DMLs all commit but its DDLs are 
merely queued. It's possible that the async parts of its DDLs will fail and
result in a partial commit. The behavior to roll back these
failures is, at best, surprising.

2. Some changes to the schema performed in a transaction are not visible for
subsequent writes inside of that transaction.

## Non-atomic DMLs ([#42061])

Many schema changes require `O(rows)` work. Generally we've wanted to avoid
performing this work directly on the user's transaction (perhaps partly out of
fear that a long-running transaction may not commit, see [#52768], [#44645]).
Many of these operations may fail (see the table in [#42061]). In today's world,
when one of these operations fails, we attempt to roll back all of the DDLs that
had been a part of that transaction (this also includes cases where there are
multiple clauses in an `ALTER TABLE`). There are some particularly pernicious
interactions when mixing the dropping of columns with other operations which
can fail (see [#46541]). 

## Non-visible DMLs ([#43057])

This is easiest to motivate with examples. Here we'll see about adding and
removing columns though there are other important cases like adding enum
values and constraints. Constraints and constraint enforcement are particularly
interesting case that will get more attention later.

### Dropping a column

When dropping a column, the column is logically dropped 
immediately when the transaction which is performing the drop commits; the 
column goes from `PUBLIC`->`WRITE_ONLY`->`DELETE_ONLY`->`DROPPED`. The important
thing to note is that the writing transaction uses its provisional view of the
table descriptor. The following is fine:

```sql
root@127.179.54.55:40591/movr> CREATE TABLE foo (
  k   STRING PRIMARY KEY,
  c1  INT, 
  c2, INT
);
CREATE TABLE

root@127.179.54.55:40591/movr> BEGIN;
root@127.179.54.55:40591/movr  OPEN> ALTER TABLE foo DROP COLUMN c2;
ALTER TABLE

root@127.179.54.55:40591/movr  OPEN> SELECT * FROM foo;
  k | c1
+---+----+
(0 rows)

root@127.179.54.55:40591/movr  OPEN> ALTER TABLE foo DROP COLUMN c1;
ALTER TABLE

root@127.179.54.55:40591/movr  OPEN> SELECT * FROM foo;
  k  
+---+
(0 rows)

root@127.179.54.55:40591/movr  OPEN> COMMIT;
```

When the above transaction commits, `foo` is in version 2 and has pending 
mutations to drop the columns c1 and c2. One thing to note is that transactions 
which happen after the above transaction may still observe those columns due to 
schema leasing. Another thing to note is that under versions 1 and 2 of foo,
the physical layout has not changed but the logical layout has. When version 3 
(`DELETE_ONLY`) comes into effect, the physical layout may differ from
version 1. It's critical that by the time the physical layout begins to change
we know that no transactions are in version 1.

### Add Column

Now let's look at the ADD COLUMN case.

```sql
root@127.179.54.55:40591/movr> CREATE TABLE foo (k STRING PRIMARY KEY);
root@127.179.54.55:40591/movr> BEGIN;
root@127.179.54.55:40591/movr  OPEN> ALTER TABLE foo ADD COLUMN c1 INT;
ALTER TABLE

Time: 2.285371ms

root@127.179.54.55:40591/movr  OPEN> SELECT * FROM foo;
  k
+---+
(0 rows)

Time: 526.967µs

root@127.179.54.55:40591/movr  OPEN> ALTER TABLE foo ADD COLUMN c2 INT;                                                                                                                                                                                                                                                     
ALTER TABLE

Time: 3.795652ms

root@127.179.54.55:40591/movr  OPEN> SELECT * FROM foo;
  k
+---+
(0 rows)

Time: 516.892µs

root@127.179.54.55:40591/movr  OPEN> COMMIT;

root@127.179.54.55:40591/movr> SELECT * FROM foo;

   k   |  c1  |  c2
+------+------+------+
(0 rows)
```

How bizarre is that! In the DROP COLUMN case we observe the effect immediately,
within the transaction. In the ADD COLUMN case we don't observe the effect
during the transaction. The reason for this is relatively straightforward: in 
order to observe and act on the effect of the `ADD COLUMN` we would need the
physical layout of the table at the timestamp at which this transaction commits 
to differ from the physical layout of the table when the transaction began. In
order for the schema change protocol to maintain correctness, it must be the 
case that for any given point in time (MVCC time) that a table has exactly one
physical layout. We achieve this through the use of the invariant that at most
there are two versions of a table leased at a time: the latest version and the
preceding version. Between two versions of a table descriptor either the
logical layout or the physical layout may change but never both. If both the
logical and physical layout changed in one version step, lessees of the older
version may not know how to interpret table data. Put differently, by the time
a physical layout changes, all lessees of the table are guaranteed to know how
to interpret the new physical layout.

# Guide-level explanation

The upshot of this design is that we can achieve compatibility with postgres
and meet user expectations for what is possible within a SQL transaction. This
proposal is not, however, without its risks. In order to regain
transactionality, in some cases, we'll need to actually perform the work of
a schema change underneath the user's transaction, prior to it committing. This
exposes the system to potentially a lot of wasted work in the face of rollbacks.
Additionally, as we'll see, coordinating with concurrent transactions through
the key-value store will require some delicate dances to allow the transaction
to both achieve serializable isolation as its implementation literally observes
state changes.

## Concrete examples

Let's walk through the idealization of above example as experienced in postgres
to try to fill in some of the details of this proposal. The ideas proposed in
this discussion are intended to be seen as both a sketch and a straw-man. More
concrete and precise language will arrive in the reference-level explanation.

```sql
postgres=> BEGIN;
BEGIN
postgres=> ALTER TABLE foo ADD COLUMN c1 INT NOT NULL DEFAULT 1;
ALTER TABLE
```

Ideally, at this point, the ongoing transaction could treat c1 as a `PUBLIC`
column and could perform reads and writes against it. Let's say that at the
outset `foo` is at version 1 (`foo@v1`). In this proposal, during the execution
of the above statement we'd first perform a single-step schema change in a child
transaction. This single-step schema change would add the new column in the
`DELETE_ONLY` state. In the `DELETE_ONLY` state nobody could write to that
column but other transactions would know how to interpret the column if they
were to come across it. The child transaction commits it will then wait for
all leases on the old version to be dropped.

Once the side schema changes and has evidence that all leases are up-to-date,
the statement execution can continue. At this point a new child transaction can
write and commit a descriptor of the table descriptor which has the column in
`WRITE_ONLY` with a pending mutation to perform a backfill that it
intends to write upon `COMMIT`. So long as the column does not become `PUBLIC`
before the user transaction commits, other transactions won't be observing its
side effects (if there were `UNIQUE` or `CHECK` constraints that might not be
the case but that comes later). It will also have an in-memory version of the
table descriptor which has the new column as `PUBLIC` which it will use for the
execution of the rest of the transaction.

This simplification doesn't deal with rollbacks, concurrent DDLs operations, or
really anything going wrong.

Okay, now let's proceed with the example:

```sql
postgres=> INSERT INTO foo VALUES ('a', 1);
INSERT 0 1
postgres=> SELECT * FROM foo;
 k | c1
---+----
 a |  1
(1 row)
```
Here we see the ongoing transaction using the PUBLIC view of the new column.
```sql
postgres=> ALTER TABLE foo DROP COLUMN c1;
ALTER TABLE
```
To execute the above statement we'll need to:
* write a new provisional table descriptor in a side txn which has no reference
  to the new column (functionally equivalent to foo@v1) and wait for it to be leased.
* perform the "backfill" on rows written in this transaction which might have 
  used a different physical layout.
```sql
postgres=> ALTER TABLE foo ADD COLUMN c2 INT NOT NULL DEFAULT 1;
ALTER TABLE
```
When we do another mutation we need to go through the same exact dance. All
rows written during this ongoing transaction to the table being modified will
need to be backfilled such that they have the physical layout of the table as 
described by the descriptor which will be committed. The backfill process
determines which rows to rewrite based on their MVCC timestamp. Specifically 
it will only backfill rows which have an MVCC timestamp earlier than the
ModificationTime (i.e. commit timestamp as of [#40581]) which moved the newly
added column to WRITE_ONLY.

```sql
postgres=> COMMIT;
COMMIT
```

At this point we'll need to write the table descriptor to a new version which
has the newly added. This is safe because we'll have very carefully avoided
interacting with the descriptor which has changed over the course of the
transaction. Well, this glosses over a bunch of details but maybe gives a
glimpse of the idea.

# Reference-level explanation

Up to this point we've talked in broad strokes about the basic ideas; adjacent
descriptor versions are carefully crafted to be compatible and the leasing
protocol ensures at most two adjacent versions are concurrently active. The
problem, so-to-speak, is that many user-level operations require more than one
descriptor version step. Now we need to get into the nuts and bolts of how to
coordinate committing these multiple steps through descriptor states while
avoiding any user-visible side-effects prior to the commit of the user
transaction.

## Key Terms

This RFC utilizes the langauge of
[20151014_online_schema_change.md](./20151014_online_schema_change.md). 

 * Child Transaction - a new concept in this RFC. A Child transaction is a 
 transaction which is associated with a parent transaction. This is handy for
 the purposes of deadlock detection.
 * Descriptor coordination key - a key out of `system.descriptor` which SQL
 transactions attempting to perform a DDL will write an intent to prior to
 commencing a transactional schema change.
 * Synthetic descriptors - a descriptor utilized by a transaction that does not
 correspond to a committed or written descriptor state. This is not a
 particularly new or novel concept; we do something akin to this already in the
 process of validating constraints. This proposal will make rather judicious use
 of this technique and ideally we can formalize it better in code.

## Key Concepts


### Index Backfill: the one true backfill

Today, there are two types of "backfills" as we call them, the "column" backfill
and the "index" backfill. The column backfiller is largely a vestige of a time
when tables when primary indexes could not be changed. This proposal relies on
moving column changes to creating new indexes as outlined in [#47989] in order
to make rollbacks reasonable and subsequent backfills possible. 

The column backfill is used, as you might have guessed, to replace columns.
The way that the column backfiller works is that it updates the physical layout
of an index in-place. Each row is overwritten transactionally during the
backfill to include the new `WRITE_AND_DELETE_ONLY` column. It must do this as
the index is concurrently receiving transactional reads and writes.

This should be juxtaposed with the index backfiller which is used to backfill
indexes which are not live for read traffic (though notably are live for
writes). The index backfiller reads from a source index from a fixed snapshot
and populates its target by synthesizing SSTs using the `bulkAdder`. All rows
written by the index backfill have a timestamp which precedes any write due to
transactional traffic, thus preserving subsequent writes as they are.

What's important to note is that the process of changing the primary key of a
table operates by performing an index backfill into a new index and then
swapping over to that new index. Similarly now we've changed truncate from
creating a new table to swapping over to new copies of indexes in the same
table. There is no reason why all column changes couldn't be done this way and
doing so opens the door for other benefits (including fixing a very irritating
CDC behavior in the face of column changes [#35738]).

For reasons that will hopefully become clear, utilizing index backfills for
all column change operations is an important stake in this design.

### Schema Changer: an explicit state machine to be wielded

#### Some historical context

Today's schema changer has evolved semi-organically though a number of
iterations. Where to draw the circle around the schema changer is a bit
amorphous. We have created new job types for type changes but for practical
purposes, that too is the schema changer. Furthermore, we've got logic in the
schema changer for applying schema change steps for descriptors created in the
current transaction. That importantly is also a part of the schema changer.

In "olden times" (19.2 and prior), the schema changer would watch the set of
table descriptors and respond to changes to them. At some point long ago when
the jobs infrastructure was added, the schema changer began correlating
mutations with jobs as a means to expose some UI. This proved horribly
problematic, both because the jobs API was, well, bad (it still is but not
nearly as bad as it was) and because of general mishandling of transactions.
In 20.1, this story got better as job life-cycles matured and the schemachanger
was re-imagined as a job. In that re-imaging we created the notion of the
schema change job which is roughly, for the most part, the steps of a descriptor
mutation with a one-to-one relationship to a job, executed by the jobs
subsystem. This scheme is **way** better than the old one, but still problematic
in its ways. For one, we can only talk about modifying a single descriptor in a
job (err, we can also talk about `DROP DATABASE CASCADE` and
`DROP SCHEMA CASCADE` but that's only further evidence that it's a bad
abstraction). In the 20.1 cycle we added those primary key changes which, for
reasons, rely on creating multiple jobs that are poorly coordinated and lead
to a less than ideal UX.

Another important observation is that the business logic is tried to these
things we call descriptor mutations. A mutation is both an implied, almost
declarative member of a descriptor as well as a specification for changes to
be made. This overloading is confusing and very poorly represented in code.
What's perhaps worse is that the business logic of handling these mutations
proceeds entirely procedurally, walking through the same implied state machine
as a mess of switches and if statements which hide various operations like
backfills, descriptor reference updates, and the like. 

#### A better way

In order to move forward we need:

1) More flexibility to re-arrange and control the logic of a schema change
2) More observability, what really are the states and transitions?
3) Decoupled from jobs, still usable by jobs
4) Capable of expressing changes over multiple descriptors

This and its coordination in the connExecutor represent the largest code
changes required to realize this vision.

### Child Transactions

The primary new KV concept required by this proposal is the child transaction.
A child transaction is related to a snapshot of a parent transaction. A child
transaction is able to observe intents written by the parent (at visible
sequence numbers). Critically a child transaction is associated with its parent
for the purposes of deadlock detection.

## Examples with more details and precision

### Column Addition

In order to add the new column, we’ll assume that the user transaction needs to ensure that the new indexes have been built before the transaction can commit.

```sql
CREATE TABLE foo (i INT PRIMARY KEY);


BEGIN;
SELECT COUNT(*) FROM foo;
INSERT INTO foo VALUES (2); -- these contending writes are interesting        . 
ALTER TABLE foo ADD COLUMN j INT AS (i+1) STORED;
INSERT INTO foo VALUES (1, 3);
-- what if you did other things like add constraints or change primary key or drop columns ...
COMMIT;
```

During the transaction, during the execution of the ALTER statement, we need to
construct the new indexes. Let’s assume that we’re in a world where
table alterations happen as a form of index creations and swaps as described
already.

* Lock the descriptor in question using a side key.
   * We can’t actually write directly to the descriptor because then we’ll
    contend with the child transaction that will work to make the new indexes,
    instead we use a separate key to coordinate changes
* Switch to pessimistic mode so that refreshes succeed (optional, see later).
* Start a child transaction to add the column
   * What if we run into a failed, previous schema change?
      * Roll it back before doing anything.
      * GC job creations should be synchronous.
      * GC jobs are async and don’t need to wait for TTL.
   * This will write a new version of the table descriptor with the column in
     `DELETE_ONLY` as a member of a new index.
   * This transaction will create a job which then builds the relevant indexes
     but does not make them public.
   * If the job fails, the statement returns an error 
      * Rollback should happen under the job too. We’ll wait for terminal.
   * If it succeeds the user transaction may proceed.
   * The schema change statement in depth:
      * Ensure that the user transaction is able to drop the leases to the old
        table descriptor so that we don’t prevent version bumps.
      * Created in child txn which writes column as delete only.
      * First step is make column write-only and wait for that before beginning
        backfill. This also adds the indexes which will be backfilled by the job.
      * Backfill all new relevant indexes.
      * Mark job as completed.
      * At this point we have fully populated indexes which concurrent and
        future transactions will keep up-to-date.
      * Aside: if a future transaction is able to grab the descriptor lock and finds the table descriptor in this state, we know the transaction trying to add this column has failed and the work should be rolled back before other schema changes can commence.
      * Why is this backfill different from all other backfills?
          * It needs to behave differently in the face of intents due to the user transaction.
          * What should it do when it encounters an intent from the user transaction?
            * It should read them as though they were committed.
            * This is aided by the fact that we’re not re-writing rows.
            * The pessimistic mode interactions here are important
                * The backfiller should be able to read ignoring the parent’s locks.
            * What does it mean to re-issue previous writes to tables with new indexes?
            * It's okay that the backfills "commits" in the new index writes which
              are intents in the old index because they will only ever become visible
              if the transaction commits.
  * Backfill concludes, user transaction needs to get bumped above the timestamp of the conclusion of the backfills.
  * Before proceeding to the next statement, the user transaction needs to populate its table collection with a synthetic (unwritten) table descriptor of the “next” version which will have the column being added as public and the new indexes also as public and the old indexes as dropping. 
  * At this point the table descriptor just needs to be updated with the final step of the schema change - making the new column public in the next version. 
  * Subsequent schema changes need some thought.
    * You could imagine dropping the intermediate indexes but you need to be careful of savepoints.
  * At commit, before issuing EndTxn, we need to write the table descriptors that we have synthesized into the table collection.
  * Then commit and wait for the version to get adopted.



### Constraint verification

The challenge is going to be that all transactions with a commit timestamp
before the transaction which adds the constraint should not observe it and all
that commit after should.

#### Adding a `CHECK` constraint transactionally.

```sql
CREATE TABLE foo (i INT PRIMARY KEY, j INT);
INSERT INTO foo VALUES (2, 2);
BEGIN;
ALTER TABLE foo ADD CONSTRAINT CHECK (i >= j); -- should succeed
INSERT INTO foo VALUES (1, 2);  -- should fail
```

* High level we need to ensure that concurrent transactions know that the
  constraint may exist but doesn’t definitely exist. This means the constraint
  needs to be evaluated but upon failure, special logic needs to run to either
  check if the constraint has been added or prevent it from being added.
* Do the usual transactional schema change dance of writing to the “side key”
  using the user transaction and then use a child transaction to ensure that
  the table descriptor is in a mutable state (no mutations/changes from previous
  rolled back transactional schema changes need to be rolled back).
* Use the side transaction to write to the table descriptor that the constraint
  is in the ADDING state. Publish that version, wait for the leases to drop.
* Perform validation of existing rows.
   * This can happen in a side transaction or a job though the job will need to
     be a child of the user transaction so that it observes its writes.
   * If this fails, adding the constraint fails, roll back the transaction.
* Move the user transaction timestamp above the lease drop time.
* For future statements in the user transaction, enforce the new table
  descriptor with the constraint as `PUBLIC` using a synthetic descriptor
  (but don't write descriptors until the end).
* For concurrent transactions which see the constraint in `ADDING`
   * Evaluate the constraint. If failed:
      * Read the table descriptor in the transaction to determine whether the
        constraint has been committed or not (this can be cached).
         * There’s some room for optimization here.
         * Furthermore, once a constraint has been failed by a transaction, you
           can also cache that and move on.
      * If constraint has been made `PUBLIC`, return the constraint violation
        error
         * The same transaction used to observe whether a new version has been
           committed needs to write the failure key if it has not been
           committed. 
         * Can just be done with the user transaction (probably should be).
      * If the constraint has not been made `PUBLIC`, prevent it from every
        being made public.
         * Straw man: write to the table descriptor marking the constraint as
           having failed.
            * One problem I see is that the side-key gave us some assumption that there were not concurrent modifications from user transactions to the table descriptor
         * Straw man 2: write to some other key that the user transaction making the constraint     public will need to read prior to committing. 
* Upon commit, user transaction finally writes to the table descriptor with the constraint in public and reads the failure key.
   * The user error here is likely to be a serializability violation at commit
     time rather than a constraint addition failure but maybe we could make
     the error message a bit clearer. Sort of bad because you might try to add
     the constraint again which might be expensive but this is a rare case when
     you had a constraint that would have been validated except in the face of a
     concurrent violating write.
   * Maybe doesn’t matter at all because most check constraint additions
     probably succeed.


#### Adding a `UNIQUE` constraint

```
CREATE TABLE foo (i INT PRIMARY KEY, j INT);
INSERT INTO foo VALUES (2, 2);
BEGIN;
ALTER TABLE foo ADD CONSTRAINT UNIQUE (j);
INSERT INTO foo VALUES (1, 2);  -- should fail
```

* Adding a constraint means adding a unique index, and validating it with all
  of the added fun of the above check constraint
* Major differences
   * Before index backfill can begin, you need to wait two versions, one for the
     index in delete only to be adopted and then another for it to be in write-only.
   * At that point you perform index backfill on the new unique index. 
   * Then you have to validate the row counts between the primary and new index
     are the same.
    
At the time when the constraint-adding transaction commits (some ambiguity) all
future transactions must see the constraint. Writes which precede the timestamp
should not experience any side-effects of the constraint.

To implement this, we must set the table descriptor to a state where writes
which would violate the constraint transactionally ensure that the verification
will fail (somehow, probably by reading some key which the schema change is
 going to write to and writing a key which is has already read).

#### Foreign Keys

Should be an almost identical paradigm, just involves more descriptors.

### Regaining Async Online Schema Changes

As we've mentioned before and will mention again, transactional schema changes
as proposed here are not a panacea. For one, if the user connection dies while
a long-running operation occurs, that work will be wholly wasted. 

The proposal is that by default we won’t ever do the asynchronous work for
schema changes in a transactional. We instead propose that we add an `ASYNC`
keyword which can apply to individual statements that will have better semantics
on failure. 

Why would you want this? Why wouldn’t you just not use a transaction? This seems
to be allowing a user to opt in to non-transactional behavior.
    
If you want transactionality then you’d put them in a transaction and it would
work. The ASYNC would help here for ergonomic reasons because you don't have to
wait to issue. 

The reason for the ASYNC keyword is to provide a mechanism for queuing up
schema changes without needing to wait potentially hours to tell the system
about it.
    Maybe provides an opportunity for optimization with regards to work
    scheduling. Semantics here are up in the air.

Still very unclear what it means for an ASYNC DDL to fail. 
ASYNC means that we should return to the user immediately without performing
the change. 

Should you be able to set up dependent mutations?
    
Sequencing ASYNC DDLS after SYNC DDLS seems more-or-less well defined.
Maybe we say that you can’t interact with schema changes which are queued. 

We don’t today have a sense of dependencies
    They could even run in parallel

If any ASYNC step fails, then all “subsequent” async DDLs fail. This is the
Spanner semantics and it seems much better than what we do today.

How do we expect users to use this ASYNC behavior?

  * This isn’t for the tools, this is for today’s users of schema changes who
  want their schema changes to happen if their connection disconnects.
  * Assuming some people are expecting this async behavior, are we going to
  trash existing user’s schema change workflows?
      *  Everything should look roughly the same.
  * The main differences are in behavior during failure.
  * The reason for ASYNC is to provide customers with a mechanism to issue
    long-running schema changes and not have to fear their connection dropping
    leading to the change not happening.

Summary of questions:
* What are the semantics of ASYNC in transactions?
* What are the semantics of DDLs, specifically interactions with previously
  issued ASYNC DDLs for subsequent sync or async DDLs. 
    * For sync DDLs, makes sense to wait for all previously issues ASYNC DDLs to clear before 
      attempting to run
    * For async, assume success and queue, any failure and the tail of the queue
      all fails. System is free to flatten queue into fewer steps but not to
      reorder? 
    
## Drawbacks

## Long-running transactions

Today schema changes are rather lightweight operations. We write out a job and
a small change to the descriptor and then wait for the asynchronous set of
operations to complete.

## Rationale and Alternatives

Some discussion of making execution and storage aware of physical layout.

TODO(ajwerner): say more

## Additional Considerations

### Pessimistic Mode

See [#52768]. Transactional schema changes will **require** pushing their
timestamp which will require read refreshes. In normal times such refreshes
can be catastrophic when considering large transactions. One good thing is that
transactional schema changes are not necessarily going to transactionally (as
is refresh spans) interact with all that much data. That being said, the cost
of a rollback will be potentially immense.

   * In the first pass, we could imagine using in-memory, best-effort locks.
      * Are these exclusive? All that exists today are exclusive locks but 
        best-effort read locks are a small task. Distributed, durable upgrade
        locks are a much bigger task.
      * There is some talk about making these much higher reliability.
         * Right now in-memory on leaseholder.
         * Don’t get transferred with lease.
         * Work item to transfer them with cooperative lease transfers.
         * Most lease transfers are cooperative.
   * For all tracked read spans, acquire locks.
   * This will require refreshes but those refreshes are likely to succeed.
   * This is purely an optimization to prevent restarts.
      * Certain workloads with concurrent access to the table will likely always invalidate reads making this a liveness problem.
      * Not a safety concern.

## Unresolved questions

* What should the semantics be when adding a column with a default value and using the `ASYNC` keywords?
    * Seems like ASYNC operations should not take effect at all.
* How do we deal with DMLs and DDLs interleaved within a transaction to a table.
* How big of a problem is it for transactions to observe side-effects of non-committed DML transactions?
    * In particular, the observation would an error when success might be possible if the concurrent DML fails to commit.
   * E.g. enforce a constraint before it’s completely added.
   * Where do we want to draw the line of accommodating clients vs. being principled
* We should support postgres’s behavior as much as possible.
    * For things which are expensive, we should provide more async, weaker semantics for those who know what they are doing.
* More of a TODO than an open question: What do alter columns look like for interleaves?
  * There's sort of two options with regards to the backfill of children and both
    aren't great.
      * Create a new index as an interleaved sibling under the same parent. This
        will lead to a fragmented LSM and rollbacks will be expensive because
        you can't rely on clear range operations.
      * Rewrite the entire interleave. This is in some ways simpler but might
        capture way more data and is code that we don't currently have.
* How exactly does this interact with `SAVEPOINT`?
    * Need to keep track of transactional schema change state with each savepoint. Upon rollback, will need to use a side-transaction to move the table descriptor back to the logical point it existed as of that savepoint, then update the TableCollection. Should “just work”.
* Can we defer validations and coordination with concurrent transactions to commit time?
    * This would be a sweet optimization for transactions with large numbers of statements which require backfills or validations.
    * Seems like it adds some complexity to the backfills and their expectations of table state. 
    * What about FK relationships? If you can’t take advantage of indexes, will validation be too slow? 
    * In 21.1 we can, in theory, do a full table scan.
    * How much can be deferred?
* Is it okay to not deal with the `SetSystemConfigTrigger`?
  * One oddity in the cockroach transactional schema change landscape is that
    if you want to perform a DDL in a transaction, it must be the first writing
    statement. This is due to implementation details of zone configurations but
    is thematically aligned with regards to limitations of schema changes in
    transactions.
* What really are the semantics of AddSSTable in the face of concurrent operations?
* What protections do we need to not end up wasting resources due to uncommitted
  schema changes.
* Should jobs be used in any way for dealing with backfills and validations
  underneath a user transaction?
    * What are the pros? Lots of cons.

[#35738]: https://github.com/cockroachdb/cockroach/issues/35738
[#42061]: https://github.com/cockroachdb/cockroach/issues/42061
[#43057]: https://github.com/cockroachdb/cockroach/issues/43057
[#44645]: https://github.com/cockroachdb/cockroach/issues/44645
[#46541]: https://github.com/cockroachdb/cockroach/issues/46541
[#47989]: https://github.com/cockroachdb/cockroach/issues/47989
[#52768]: https://github.com/cockroachdb/cockroach/issues/52768
