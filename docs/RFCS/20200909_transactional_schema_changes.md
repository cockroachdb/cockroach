- Feature Name: Transactional Schema Changes
- Status: draft
- Start Date: 2020-09-09
- Authors: Andrew Werner, Lucy Zhang
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [#42061], [#43057]

# Summary

CockroachDB's online schema change architecture necessitates steps through
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

This proposal relies on always creating a new primary index when changing table
column sets ([#47989], [Schema Change Backfills](#schema-change-backfills)).
That architectural change will lead to a solution to [#35738], a
severe usability issue with `CHANGEFEED` in the face of schema changes.

## Non-atomic DDLs ([#42061])

Many schema changes require `O(rows)` work. Generally we've wanted to avoid
performing this work directly on the user's transaction (perhaps partly out of
fear that a long-running transaction may not commit, see [#52768], [#44645]).
Many of these operations may fail (see the table in [#42061]). In today's world,
when one of these operations fails, we attempt to roll#35738] back all of the
DDLs that had been a part of that transaction (this also includes cases where
there are multiple clauses in an `ALTER TABLE`). There are some particularly
pernicious interactions when mixing the dropping of columns with other
operations which can fail (see [#46541]).

## Non-visible DDLs ([#43057])

This is easiest to motivate with examples. Here we'll see about adding and
removing columns though there are other important cases like adding enum
values and constraints. Constraints and constraint enforcement are particularly
interesting cases that will get more attention later.

### Dropping a column

When dropping a column, the column is logically dropped
immediately when the transaction which is performing the drop commits; the
column goes from `PUBLIC`->`WRITE_ONLY`->`DELETE_ONLY`->`DROPPED`. The important
thing to note is that the writing transaction uses its provisional view of the
table descriptor. The following is fine:

```sql
root@127.179.54.55:40591/movr> CREATE TABLE foo (
  k  STRING PRIMARY KEY,
  c1 INT NOT NULL CHECK (c1 > 5),
  c2 INT NOT NULL,
  c3 INT
);
CREATE TABLE

root@127.179.54.55:40591/movr> BEGIN;
root@127.179.54.55:40591/movr  OPEN> ALTER TABLE foo DROP COLUMN c2, DROP COLUMN c3;
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

```

When the above transaction commits, `foo` is in version 2 and has pending
mutations to drop the columns c1 and c2. One thing to note is that transactions
which happen after the above transaction may still observe those columns due to
schema leasing. Another thing to note is that under versions 1 and 2 of `foo`,
the physical layout has not changed but the logical layout has. When version 3
(`DELETE_ONLY`) comes into effect, the physical layout may differ from
version 1. It's critical that by the time the physical layout begins to change
we know that no transactions are in version 1.

While the above scenario on the surface seems desirable, it actually papers over
very real complications related to the dropping of columns with constraints
([#47719]). The problem this posses is that later statements in the transaction
do not provide a value for the dropped column, yet, in order to preserve our
two version correctness properties, must write a value into that column that
does not violate any constraints assumed by concurrent transactions using the
previous version.

Let's explore the implications of the above issue as it pertains both to the
executing transaction and to concurrently executing transactions. Imagine that
the following statements were to be inserted to the above session immediately
after dropping `c2` and `c3`.

```sql
root@127.179.54.55:40591/movr  OPEN> INSERT INTO foo VALUES ('foo', 42);
INSERT 1
```

Now imagine a concurrent connection attempts to read from foo. It will issue
a select which will block until the `root` transaction concludes. At that point
it will read using the schema version which existed prior to the root
transaction.

```sql
other_user@127.179.54.55:37812/movr> SELECT * FROM foo;
   k  | c1 | c2 |  c3
------+----+----+-------
  foo | 42 |  0 | NULL

```

Notice that this concurrent transaction will observe a zero value for
`c2 NOT NULL`. This is to deal with the fact that the execution engine
really cannot deal with `NULL` values in `NOT NULL` columns. What's perhaps
worse is that the same zero value would be written into `c1` if an insert
were to be performed after it had been dropped in the transaction. This would
lead the concurrent transaction to observe the check constraint being violated.
There may be cases where this could transiently result in a correctness
violation due to a plan which assumed the check constraint.

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

Let's walk through the idealization of the above example as experienced in
postgres to try to fill in some of the details of this proposal. The ideas
proposed in this discussion are intended to be seen as both a sketch and a
straw-man. More concrete and precise language will arrive in the reference-level
explanation.

```sql
postgres=> BEGIN;
BEGIN
postgres=> ALTER TABLE foo ADD COLUMN c1 INT NOT NULL DEFAULT 1;
ALTER TABLE
```

Ideally, at this point, the ongoing transaction could treat c1 as a `PUBLIC`
column and could perform reads and writes against it. Let's say that at the
outset `foo` is at version 1 (`foo@v1`). Recall from the [online schema change
RFC] and the [F1 schema change paper] that the steps to adding a column are
`DELETE_ONLY`->`WRITE_ONLY`->(maybe backfill)->`PUBLIC`.

In this proposal, during the execution
of the above statement we'd first perform a single-step schema change in a
[child transaction](#child-transactions) (new concept) rather than on the user
transaction. This single-step schema change would add the new column in the
`DELETE_ONLY` state. Recall from the  In the `DELETE_ONLY` state nobody could
write to that column but other transactions would know how to interpret the
column if they were to come across it. The child transaction that commits it
will then wait for all leases on the old version to be dropped.

Once the child transaction commits and has evidence that all leases on all nodes
are up-to-date, the statement execution can continue. At this point, a new child
transaction can write and commit a descriptor of the table descriptor which has
the column in `WRITE_ONLY` with a pending mutation to perform a backfill that it
intends to write upon `COMMIT`. So long as the column does not become `PUBLIC`
before the user transaction commits, other transactions won't be observing its
side effects (if there were `UNIQUE` or `CHECK` constraints that might not be
the case, but that comes later). The user transaction, following execution of
the `ALTER` statement will hold an in-memory version of the table descriptor
which has the new column as `PUBLIC` which it will use for the execution of the
rest of the transaction.

This simplification doesn't deal with rollbacks, concurrent DDL operations, or
really anything going wrong. More details on how those scenarios are handled
will be introduced in the reference-level explanation. let's proceed with the
example:

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
* Write a new provisional table descriptor in a child txn which has no reference
  to the new column (functionally equivalent to `foo@v1`) and wait for it to be
  leased.
* Perform the "backfill" on rows written in this transaction which might have
  used a different physical layout. In the current implementation this means
  rewriting every row in the primary index to no longer contain the column in
  question. In the proposal, we'll construct a new primary index which does not
  include this column.
```sql
postgres=> ALTER TABLE foo ADD COLUMN c2 INT NOT NULL DEFAULT 1;
ALTER TABLE
```
When we do another mutation we need to go through the same exact dance. All
rows written during the current transaction to the table will need to be
re-written such that they have the physical layout of the table as
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
has the newly added column c2. This is safe because we'll have very carefully
avoided interacting with the descriptor which has changed over the course of the
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

This RFC utilizes the language of the [online schema change RFC].

## New Concepts

 * Child Transaction - a new concept in this RFC. A Child transaction is a
   transaction which is associated with a parent transaction. This is handy for
   the purposes of deadlock detection and critical to implement backfills of
   data and read from indexes which have writes from the parent.
 * Descriptor coordination key - a key out of `system.descriptor` which SQL
   transactions attempting to perform a DDL will write an intent to prior to
   commencing a transactional schema change.
 * Synthetic descriptors - a descriptor utilized by a transaction that does not
   correspond to a committed or written descriptor state. This is not a
   particularly new or novel concept; we do something akin to this already in
   the process of validating constraints. This proposal will make rather
   judicious use of this technique and ideally we can formalize it better in
   code.
 * Constraint invalidation table - a table which can be utilized by transactions
   to invalidate the addition of a constraint which might have otherwise
   succeeded.
 * Transaction self-pushing - a KV level mechanism to push a transaction
   manually to a timestamp. Today transactions are only pushed in response to
   kv-level interactions.
 * Condition observation - After a new version of a descriptor is published by
   a child transaction, the execution must wait for that version to be adopted
   before proceeding to the next version. We need a tool to observe that all
   leases have been dropped that interacts appropriately with deadlock
   detection. This mechanism will retry a closure with a child transaction until
   that closure indicates that the condition has been satisfied.

## Key Concepts

### Schema Change Backfills - Background

During the course of certain schema changes, data stored in the KV needs to
change. This may be due to the construction of new indexes or to changes in the
layout of existing index. We'll first discuss the two existing backfill
mechanisms, column and index, and then talk about the plan to move all schema
changes to utilize the index backfiller.

Today, there are two types of "backfills" as we call them, the "column" backfill
and the "index" backfill. The column backfiller is largely a vestige of a time
when tables' primary indexes could not be changed. This proposal relies on
moving column changes to creating new indexes as outlined in [#47989] in order
to make rollbacks reasonable and subsequent backfills possible.

The column backfill is used, as you might have guessed, to replace columns.
The way that the column backfiller works is that it updates the physical layout
of an index in-place. Each row is overwritten transactionally during the
backfill to include the new `WRITE_AND_DELETE_ONLY` column. It must do this as
the index is concurrently receiving transactional reads and writes.

This should be juxtaposed with the index backfiller which is used to backfill
indexes that are not live for read traffic (though notably are live for
writes). The index backfiller reads from a source index from a fixed snapshot
and populates its target by synthesizing SSTs using the `bulkAdder`. All rows
written by the index backfill have a timestamp that precedes any write due to
transactional traffic, thus preserving subsequent writes as they are. This is
needed because AddSSTable does not interact with the usual transaction
mechanisms, it just injects the keys into the keyspace.

What's important to note is that the process of changing the primary key of a
table operates by performing an index backfill into a new index and then
swapping over to that new index. Similarly now we've changed truncate from
creating a new table to swapping over to new copies of indexes in the same
table. There is no reason why all column changes couldn't be done this way and
doing so opens the door for other benefits (including fixing a very irritating
CDC behavior in the face of column changes [#35738]).

### Eliminating the column backfill

For reasons that will hopefully become clear, utilizing index backfills for
all column change operations is an important stake in this design. This is
crucial so that the process of rolling back a failed schema change can be
as lightweight as scheduling an asynchronous GC job. If we, somehow, continued
with the column backfill during a transactional schema change, rolling back a
failed change would require synchronously rewriting the index to remove any
detritus rows before being able to proceed.

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
schema change job that is roughly, for the most part, the steps of a descriptor
mutation with a one-to-one relationship to a job, executed by the jobs
subsystem.

This scheme is **way** better than the old one, but still problematic
in its ways. For one, we can only talk about modifying a single descriptor in a
job (err, we can also talk about `DROP DATABASE CASCADE` and
`DROP SCHEMA CASCADE` but that's only further evidence that it's a bad
abstraction). In the 20.1 cycle we added those primary key changes that, for
reasons, rely on creating multiple jobs that are poorly coordinated and lead
to a less than ideal UX.

Another important observation is that the business logic is tried to these
things we call descriptor mutations. A mutation is both an implied, almost
declarative member of a descriptor as well as a specification for changes to
be made. This overloading is confusing and very poorly represented in code.
What's perhaps worse is that the business logic of handling these mutations
proceeds entirely procedurally, walking through the same implied state machine
as a mess of switches and if statements that hide various operations like
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
for the purposes of deadlock detection. A child transaction differs from a
nested transaction as implemented today primarily because it can be committed
independently of the parent transaction and that its side effects will become
visible to other transactions when it is committed.

The work required to realize child transactions is discussed in [#54633].

Child transactions will be used to read all descriptors in the transaction
and to perform (and commit) all writes to descriptors except the final write
which makes the schema change visible. This is critical to ensure that the user
transaction does not violated serializable isolation. Fundamentally, if the user
transaction were to observe the descriptor and then that descriptor were to
change during the transaction (which this proposal mandates), then the
transaction could not both write to the descriptor and commit.

The process of managing descriptor resolution and ensuring that it occurs inside
of child transactions properly will be delegated to the `descs.Collection` and
name resolution infrastructure. One added bit of complexity is ensuring that
the user transaction can read from the various virtual tables which read
descriptors and namespace.

#### Sequence numbers of visibility of parent txn writes

It is also important for child transactions to be able to read provisional
values written by their parents. This is important so that index backfills
to new indexes will include previous writes from the transaction. Designs in
which the parent buffers writes to be re-executed were explored but rejected.
Backfilling intents into new indexes as committed values is safe because the
index will only become public if the parent transaction commits. In this case,
the child transaction will be constructed with the sequence number of the parent
transaction.

In other uses of child transactions, it is not desirable to observe writes of
the parent transaction. In these cases (like [#condition-observation]), we'll
want to construct the child transaction as though it is operating at the zero
sequence of the parent transaction. This implies some need to control the
sequence number of the parent transaction for use by the child transaction at
construction time.

### Descriptor coordination keys

In order to prevent concurrent transactional schema changes on the same table,
we want to achieve exclusive locking semantics on a given descriptor. However,
given that the execution of a schema change will require writing multiple
versions of the descriptor and waiting for those versions to be observed,
the user transaction cannot interact with the descriptor key directly. Doing so
would ensure that serializability was violated. Instead, we introduce a new
table `system.descriptor_coordination` which the user transaction can write to
prior to initiating any transactional schema changes.

If a DDL executes and, after writing the descriptor coordination key, encounters
a descriptor is in a state that indicates that it has a partially applied
transactional schema change, then it knows that those changes can be rolled
back.

### Constraint coordination spans

In order to transactionally add constraints, we need to coordinate with
concurrent writes to the table. The process of adding the constraint will
validate that all existing data meets that constraint. Concurrent writes,
however, may violate that constraint. In order to deal with that, we'll
combine serializability with explicit execution action to ensure that
constraints being added are not concurrently invalidated.

When a constraint is being added, it is added in a `VALIDATING` state. In this
state executing transactions detect when they would violate this constraint as
though it were public, but, upon detecting the violation, determine whether
the constraint has been committed. If the transaction determines that the
constraint has not been committed, it must ensure that it will not be committed.

The way that the transaction ensures that the constraint will not be committed
is by writing an entry into `system.constraint_coordination`, a new table. When
a schema change adds a new constraint in the validating state, it reads the
entire relevant span where other transactions might write. Upon commit, the
user transaction will issue a delete range command over this span. If any
rows have been written, the transaction will fail.

There are many other protocols which could be imagined for this sort of
coordination but this one seems reasonably simple and should work. If we were
to implement the [Pessimistic Mode](#pessimistic_mode) extension, things may
not work as intended.

### Condition Observation

When a child transaction in a transactional schema change commits a new version
of a descriptor, it must wait for that version to be adopted before proceeding.
The process of observing this condition is fundamentally problematic in that it
exposes the system to deadlock.

We'll call this topic "condition observation". The sketch of the proposal here
is re-iterated in [#55981]. The idea is that the KV layer will expose
functionality to run a closure in a retry loop with backoff until that closure
indicates that the condition has been satisfied. The condition will utilize a
child transaction at each iteration to check the condition. The reason we give
this concept a name is that it interacts directly with deadlock detection.

The hazard we need to deal with is the fact that a transaction with an
outstanding lease we're waiting to drop may be blocked on an intent of the user
transaction (or of another transaction transitively blocked on the user
transaction). The blocking transaction is either writing, in which case it will
(eventually) be in the txn wait queue for the user transaction with a
`PUSH_ABORT` request or reading in which case it will have issued a
`PUSH_TIMESTAMP` request. We prefer to abort blocked writers rather than let
them abort the user transaction. The way we'll deal with this is to self-push
the user transaction above all blocked readers and we'll issue `PUSH_ABORT`
requests against all of the writing transactions. We'll need to extend
`QueryTxn` to return the type and timestamp of pushers.

This requirement is somewhat unfortunate in that the isolation of the deadlock
detection layer from the KV api was nice, but it seems important as at the end
of the day, this condition is a resource on which a transaction is blocked.

Upon fulfilling the condition, the user transaction will push itself above the
timestamp at which the condition was observed to be true.

## Examples with more details

### Column Addition

In order to add the new column, we’ll assume that the user transaction needs to
ensure that the new indexes have been built before the transaction can commit.

```sql
CREATE TABLE foo (i INT PRIMARY KEY);


BEGIN;
INSERT INTO foo VALUES (1); -- these contending writes are interesting.
ALTER TABLE foo ADD COLUMN j NOT NULL DEFAULT 42;
INSERT INTO foo VALUES (2, 2);
SELECT * FROM foo; -- (1, 42), (2, 2)
COMMIT;
```

During the transaction, during the execution of the `ALTER` statement, we need
to construct the new indexes. Let’s assume that we’re in a world where
table alterations happen as a form of index creations and swaps as described
already.


![
@startuml
:CREATE TABLE foo (i INT PRIMARY KEY);
:BEGIN;
:INSERT INTO foo VALUES (1);
:ALTER TABLE foo ADD COLUMN j INT NOT NULL DEFAULT 42;
partition "ALTER TABLE Implementation"  {
   partition "Prepare for schema change on user transaction" {
       :Lock the descriptor coordination key;
       note right
          We can’t actually write directly to the descriptor because then we’ll
          contend with the child transaction that will work to make the new indexes,
          instead we use a separate key to coordinate changes
       end note
       :(Optional) Switch to pessimistic mode ;
   }
   partition "Add new column in DELETE_ONLY" {
       #LightBlue:repeat :Read descriptor using new child transaction;
       backward :Clean up incomplete mutations;
       note right
              Create GC jobs to clean up
              indexes if necessary.
       end note
       repeat while (Descriptor has pending mutations?) is (Yes) not (No)
       :Drop leases in user transaction for descriptor;
       #LightBlue:Write new descriptor version with DELETE_ONLY column
             as member of new DELETE_ONLY index;
       #LightBlue:Commit child transaction;
       #LightBlue:Wait for one version using new child transaction;
   }
   partition "Move column to WRITE_ONLY" {
       #LightBlue:Read descriptor using new child transaction;
       note right
          The version really should not have changed unless the
          user transaction was aborted because of the descriptor
          synchronization key. If the descriptor does not meet
          expectations, abort. Note, however, that it doesn't
          need to be exactly the version we expect but rather
          a "compatible" version. This complexity exists deal
          with the desire to support `GRANT` and `REVOKE`
          concurrent with transactional schema changes.
       end note
       #LightBlue:Write new descriptor version with WRITE_ONLY column and index;
       #LightBlue:Wait for one version using new child transaction;
   }
   partition "Perform backfill" {
       #LightBlue:Run the backfill of the new index;
       note right
           This backfill is different from normal index backfills in
           that it needs to read the intents of the parent transaction
           as though they were committed values so that they end up
           in the new index. This is safe because the new index will
           only become public if the transaction commits.
       end note
   }
   partition "Prepare user transaction for next statement" {
       :Push the user transaction to a timestamp above the read timestamp
        at which all nodes were on a version where the new index and column
        are in `WRITE_ONLY`;
       :Acquire new lease on current version of descriptor;
       :Synthesize a descriptor which has the old primary index in
        `WRITE_ONLY` and the newly backfilled index as the primary
        index in `PUBLIC`;
       note right
          This new descriptor with a drop index mutation will reference
          an in-memory job record which will be written when this
          descriptor is written, at `COMMIT` time.
       end note
   }
}
:INSERT INTO foo VALUES (2, 2);
:SELECT * FROM foo;
note right
   |= i |= j |
   | 1  | 42 |
   | 2  | 2  |
end note
:COMMIT;
partition "COMMIT Implementation" {
   :Write the synthesized descriptor which has the old primary index in
    `WRITE_ONLY` and write mutation jobs to drop the old primary index;
   :Commit user SQL transaction;
   :Wait for job which will move the old primary index to `DROPPED`,
    create GC job, and wait for one version;
}
@enduml
](http://www.plantuml.com/plantuml/png/jLVTRjku4hxFKmnoBavou1EWGQySBBQEhHP67TlhE2rojQQfSSI68hKa5SVT5jZNsDVR9zdHs9BfdsI10ciW2YJDVFncvfiXzSu7uKAPwvDsTv9qfWbCEzU311RMmfc2_d0Auqd_fZEvXu_9_VdLIViw-T0Vql_-y3QPJ2kB4Lj_wWpkabiuUrCPTGRJP191TNezw8u6TpT3-CAWmn7ztGq6q4lUT-u6KtXxUNLI41KLb3Lm6WFqyq9ZZYQ8wjqfmEyd010PZntI7T9A3hpCC1SWCs4U4EXjwT51SC9u8TcVtVdN7bZv2253ID5Bfuf00D9QboeZ6FeHLrUrjR41mQc7B9n0y_kC88Nvwuy_0n1yARHUmTAfG93AeGnq6-p-4dEKWcXLZmqiaRorZZ2bDG5D2aiLCdQLcT9f70Cz5O7UQmrBwoW42xbuP40mk0HbKdn6tuf0bV411O4YL4iBy5NEH42AaJasSUCcURxs9YuSV9Esir5HiH3w76w9fCmg_mAzLxdoGKd8ROh0YVk-LwfEcZ93QNMP6s950XWaq-JNqN1m7vNcFuCgqTUwn3OL5odQzeJenrailJ8F37QGdwPcSo4VbyAbqEvg52I6WfQKjX9KGCZBjQJygpNcNzTXbPaFNVXYvvxpjG7SDQnJ3sf1r2HwBzpgugLCRa9R4d-4izusjanuACYQ8joo_FaSb8UpU_Jd5GgS3UrvKvEUimKG9L-jVQXwxerjygwEvFapovRo6IVv2PqdTzPYNAfD0NU39z8vvdDqO1SC53jmOewjsxLvhi8hHOmv2XKu56kmuVQw40urU6EVi1PWiF1vqdzDVp-akwCwccPRpWvvLFZCbferGIL_gZilXT9ezBxgvyZ_eAPBuK7ChGkODZF5BlQcJGJWLqPcpXhrjPbk5z3VTu3KecTAEMBC7vyBb1ijjjOBNy2G8cr1PfT8eRNMWqa5nZ3_ZRqD4ijWYIW1YVLaZ1Aon0q-pCi0Jj2xcBg0qwfjQV6vnjFQwu9ogZoi6_fPXHNGrGTFqOXeeh9wwoYLmug6Buk20e3PXqbdE9s1C2dC9icdqSTajZkBPUaSci0WSVg5tjri_46R_r2JiHpZ7cDgt3t_MaUCqH54pkDoeRH-eH5AmqcihLXhyOxpklovLbjdvI5LYmLoRXVEvc28Wj1hgCREqtq4KWkCvSHpsA58cOOoWQ1ypQeGZ1o57iC8ygNUUs1Pq9PDD43o8AgQwKbGl1wyvUNOX8kwE-sLsGr_eqP5ZcA1qIuVsV2c7ODOerULgSs9SZdNIeBY06Bc6sh75VRz-6de-1PWy3c03oBmWIe-2urBpyauz0iM10IL8_dbHTNuJnmLftxxeebglQF938JMn9A4liulGOcjrZDq-xaXeU_lArKWoi1isnMpguPoH_vMAiS8lFNn4dMxrWlPnR5zhtsxChIuLryHH6ImOPwnFWXFfr0ubGktMdECrHYJOkwRQFIgKI-cTMYCLsCr43KcpCPtruD-T_PFEuZo-yE3XmQ5u6onWQiF3AmrSCWz9Z724OPC_vzZRYakEj2GaRGktGJFRdFaSsn0BbMbTEKZX8Y0yhLbgohzh3kwkUbFPooC5mJx_UNFXyiMN5PV4BTqTEXEuN_mVZAwgIokJdPJykqdKDNb2tpZMtXJNTvUrhUNzUMaMRkzfhRpeR5-TF2DmNsndjPSED_89Vq1jHmeXH8MdJ_hCoKNyJWIww8-Ad67tluoE9Zhszs04EDgvjooHodIihFUP3GU9xrPYzdA-ApRMlCziijSKHdVKMxBNFyD)

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

From a high level, we need to ensure that concurrent transactions know that the
constraint may exist but doesn’t definitely exist. This means the constraint
needs to be evaluated, but upon failure, special logic needs to run to either
check if the constraint has been added, or prevent it from being added.

![
@startuml
:CREATE TABLE foo (i INT PRIMARY KEY, j INT);
:INSERT INTO foo VALUES (2, 2);
:BEGIN;
:ALTER TABLE foo ADD CONSTRAINT CHECK (i >= j);
partition "ALTER TABLE Implementation"  {
   partition "Prepare for schema change on user transaction" {
       :Lock the descriptor coordination key;
       :(Optional) Switch to pessimistic mode;
   }
   partition "Add the constraint in VALIDATING state" {
       #LightBlue:repeat :Read descriptor using new child transaction;
       backward :Clean up incomplete mutations;
       repeat while (Descriptor has pending mutations?) is (Yes) not (No)
       :Drop lease in user transaction for descriptor;
       #LightBlue:Write new descriptor version with VALIDATING constraint;
       #LightBlue:Commit child transaction;
       #LightBlue:Wait for one version using new child transaction;
       note right
          This means that the user transaction cannot proceed until all
          transactions which have a lease on the old descriptor version
          complete. The observation of this condition will need to coordinate
          with deadlock detection.
       end note
   }
   partition "Validate the constraint" {
       :Use user transaction to scan constraint coordination span;
       note right
           Fail if it is non-empty.
       end note
       #LightBlue:Use child transactions to run validation;
   }
   partition "Prepare user transaction for next statement" {
       :Push the user transaction to a timestamp above the read timestamp
        at which all nodes were on a version where the constraint was `VALIDATING`;
       :Acquire new lease on current version of descriptor;
       :Synthesize a descriptor which has the constraint `PUBLIC`;
   }
}
split
partition "Transaction 2" {
   #LightGreen:INSERT INTO foo VALUES (2, 3);
   note right
       The descriptor indicates that there is a CHECK constraint
       in the VALIDATING state. If, during execution, the statement
       might violate the constraint, it needs to determine whether
       the constraint has been committed. It does this by reading
       the descriptor within the transaction. If the constraint is
       not committed, then the transaction needs to write a row to
       the relevant constraint coordination span. Once a transaction
       has determined the fate of a constraint one way or another, it
       can cache that outcome for the rest of execution.
   end note
   detach
}
split again
:INSERT INTO foo VALUES (1, 2);
note right
   ERROR!
end note
detach
split again
:COMMIT;
partition "COMMIT Implementation" {
   :Issue a DeleteRange over the descriptor coordination span;
   :Write the synthesized descriptor which has the constraint in `PUBLIC`;
   :Commit user SQL transaction;
   :Wait for one version;
}
detach
end split
@enduml
](http://www.plantuml.com/plantuml/png/ZLR1Sjis4BthAxRfHPfnFTFqfapRo9AQQY9BhaIduvlNmCf4J08ii9JYTlplzO8M1Pbg6bqy9e67jm_lxV9zOFHSbqLlE5vDHzaKij75V0eRvw1lOBR8u6erknojRk3ZzEOCFikZmRlUSBPOJrUP_BUCYp-DvjVJDVJVdi5RUNyn_J1RFFyTpRFfAW4TJIOmNYxMsMeauEFVf-EFSj8lFyFdvtqLUZPid8KtwSvPMHLKacMKTsy0_ka1GBBuobE5dc3ZF0ILKucWShJt1Cv27SW3UxG1LTmlsvlVSExK0t1Ee2aeRofs7fHpNXkB4Vg17j-rg_lBIXvYCO3rph3AWHrK588fJM2Ze7IQulAdLmH7MiTZbBE1FHhBOAoeDfkCijdY0mH6feJPztDpd_D5KTFGKqN8C5mHwfHd7Ooz1qixKBafT5fYI_aErSCElORXk22qK5TWh7AY9nEKTQDeQDU_dBLx1YJeJmw7vHYW8glbo7RRhmCm0Vet50PW7KD_uGQjM1FlAYW80u7fNe9SLLBCknDr_-aDazIN5hqb7sJxpd2Uw9S8UmfgxChIy6cTEiUYuSZEMMgF-nQjhMC2BpWzQ7zPRWAKX3O0vyXYWguM2gt8LtcdY3JKbaq1M1G9Jh8yoFMe77BS4k2BnCv6Q5Ve4sebEFkxFuTCbjy5yjl6wMu3B6ILizfmet5HW1L6x0wfe0Gjte8cr8NaI1DJP7Y-Nq9MHrbEHU8J5aOZqwjSfEcy3bsnX4jGQ9CjnuaD5VxFdS1lQ0em6p0i_hNE_a1bnOzTsbrt2AME1OAGyhM5RQofTSNJwJPrEWoMld3J1aewbk6g3ZbmVbeB13Ob1SQo0hnpsqPFJwWFBzhQaL-yWtApJbE07Vde7Zn4AoV_-bPWXm5k3tcxFJJ5aVghDhu9Qkj5LNjFbbjCjpaLzk7wqN9EmVnDWCc2rk3XDOtRg-kB-Mny-oBlKozKXU5qRcI9F6z5nVR-FdWY-xN9zTCWmdOzanqF26ErKSXqYBGdCH8sy-p0jmKmCPsTVdyEiyqPwDfBSw4lf6gXVIPhMoUq6AKmWgrnHJSqPs0u9ZKwKNBeIsD9hbBOxI6EDqLzxuWigDWUcVGp8GRjA3IjuEvHZ2GzFqL8b93yDxMbjfIoEjCk994y71XhxG0SIjd56O3WtGxOfImy5RH5or_j0-Umj8e0K_0zY5JVAgKZvWQPnAgOWZhH4H_1UK3hH4vHUuyIEn6gd1elk9gLAoa6kc4PM13Roptl7VSNeO0gtpiPy1wD_U_lgn-RxwjZYqvNg-NgkrwBsa0U0uwNbvUpxEWBgtdK-RYAYHdEGgX5k0d9j5Wrtr9RygyTSBhrDhExyN4RSVrD2JVsEEJxsHrxt_gFUMVqTWTsVFEqbq5aQRh4UxAwBej_0G00)


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
will fail by using the constraint coordination span.

#### Foreign Keys

Should be an almost identical paradigm, just involves more descriptors.

### Rollback

When a transactional schema change fails to commit (due either to an explicit
rollback or due to a session closing), the provisional descriptor changes will
need to be reverted and any partially created indexes will be removed. The
process is quite like intent resolution for aborted transactions. In the
case of an explicit rollback, the connection will eagerly perform this cleanup.
However, we cannot rely on this eager cleanup. In cases where future
transactions attempt to perform schema changes, the existing "pending" mutations
will need to be cleaned up first. This can be done using the child transaction
in the same way as any other change that will be made in the course of a
transactional schema change.

The process of cleaning up partially created indexes will fall to the GC Job as
it does today. The GC Job will not need to respect a GC TTL as the indexes will
never have been made public. The cost of these cleanups in the case of frequent
rollbacks is a point of concern in this design. Avoiding spurious rollbacks and
restarts of expensive transactional schema changes will be important for this
proposal to be successful (see Pessimistic mode discussion).

Note that the above paradigm should generalize to savepoints. The fact that we
write intents to new indexes as committed values is not a problem because if a
transaction rolls back to a `SAVEPOINT` prior to a write it must also drop any
subsequently added indexes. In that way, there should not be any hazards related
to exposing values that arise due to this proposal. This work is deferred and
discussed in more detail in the [`SAVEPOINT`s and "Nested Transactions"](#savepoints-and-nested-transactions) future work section.

### Regaining Async Online Schema Changes

As we've mentioned before and will mention again, transactional schema changes
as proposed here are not a panacea. For one, if the user connection dies while
a long-running operation occurs, that work will be wholly wasted.

The proposal is that by default we won’t ever do the asynchronous work for
schema changes in an explicit transactional. We instead propose that we add an
`CONCURRENTLY` keyword which can apply to individual statements that will have
better semantics on failure.

#### The `CONCURRENTLY` keyword and Postgres

We choose the `CONCURRENTLY` keyword because it is the keyword utilized by
postgresql for this sort of asynchronous schema change. Postgres's
`CONCURRENTLY` only applies to index addition. The usability of this keyword
is severely restricted. See the [Postgres `Concurrently` Documentation](
  https://www.postgresql.org/docs/13/sql-createindex.html#SQL-CREATEINDEX-CONCURRENTLY)
for documentation on the limitations. We'll summarize them below:

> Regular index builds permit other regular index builds on the same table to
occur in parallel, but only one concurrent index build can occur on a table at
a time. In both cases, no other types of schema modification on the table are
allowed meanwhile. Another difference is that a regular CREATE INDEX command can
be performed within a transaction block, but CREATE INDEX CONCURRENTLY cannot.
>
> Concurrent builds for indexes on partitioned tables are currently not
supported. However, you may concurrently build the index on each partition
individually and then finally create the partitioned index non-concurrently in
order to reduce the time where writes to the partitioned table will be locked
out. In this case, building the partitioned index is a metadata only operation.

#### Considerations

Why would you want this? Why wouldn’t you just not use a transaction? This seems
to be allowing a user to opt in to non-transactional behavior.

If you want transactionality then you’d put them in a transaction and it would
work. The `CONCURRENTLY` would help here for ergonomic reasons because you don't
have to wait to issue.

A reason for the `CONCURRENTLY` keyword is to provide a mechanism for queuing up
schema changes without needing to wait potentially hours to tell the system
about it.
    Maybe provides an opportunity for optimization with regards to work
    scheduling. Semantics here are up in the air.

Still very unclear what it means for an `CONCURRENTLY` DDL to fail.
`CONCURRENTLY` means that we should return to the user immediately without
performing the change.

Should you be able to set up dependent mutations?

Sequencing `CONCURRENTLY` DDLs after synchronous DDLs seems more-or-less well
defined. Maybe we say that you can’t interact with schema changes which are
queued.

We don’t today have a sense of dependencies. Some schema changes on the same
table could even, in theory, run in parallel.

If any `CONCURRENTLY` step fails, then all “subsequent” async DDLs fail. This
is similar to the Spanner semantics and it seems much better than what we do
today from a simplicity perspective. Spanner has a notion of batches of DDL
statements. It appears that different batches do not observe changes which
have been queued but not yet applied.

> Cloud Spanner applies statements from the same batch in order, stopping at
> the first error. If applying a statement results in an error, that statement
> is rolled back. The results of any previously applied statements in the batch
> are not rolled back.

The language is confusing when it comes to the interaction between batches:

Lastly, Spanner is not set up for frequent schema changes. This is not
an acceptable limitation for cockroach.

> Avoid more than 30 DDL statements that require validation or index backfill
> in a given 7-day period, because each statement creates multiple versions of
> the schema internally.

How do we expect users to use this `CONCURRENTLY` behavior?

  * This isn’t for the tools, this is for today’s users of schema changes who
  want their schema changes to happen if their connection disconnects.
  * Assuming some people are expecting this async behavior, are we going to
  trash existing user’s schema change workflows?
      *  Everything should look roughly the same.
  * The main differences are in behavior during failure.
  * The reason for `CONCURRENTLY` is to provide customers with a mechanism to
    issue long-running schema changes and not have to fear their connection
    dropping leading to the change not happening.

Summary of questions:
* What are the semantics of `CONCURRENTLY` in transactions?
* What are the semantics of DDLs, specifically interactions with previously
  issued `CONCURRENTLY` DDLs for subsequent sync or async DDLs.
    * For sync DDLs, makes sense to wait for all previously issues
      `CONCURRENTLY` DDLs to clear before attempting to run
    * For `CONCURRENTLY`, assume success and queue, any failure and the tail of
      the queue all fails. System is free to flatten queue into fewer steps but
      not to reorder?

#### Concrete proposal

Add support for `CONCURRENTLY` schema changes to a table which cannot be used
inside of a transaction. `CONCURRENTLY` schema changes must be single
statements. A `CONCURRENTLY` schema change operates on the table as though all
previously enqueued `CONCURRENTLY` schema changes have been completed.
`CONCURRENTLY` schema changes result in a table that has a job ID corresponding
to the schema change. For multi-clause `ALTER` statements, multiple jobs may be
returned. If any async schema change fails, all subsequent queued schema changes
will fail. Non-`CONCURRENTLY` schema changes may not proceed on a table until
all `CONCURRENTLY` schema changes finish.

A problem with this proposal is that it means that there is no efficient way to
add multiple columns. This seems likely to be unacceptable. Perhaps we need to
make an attempt to combine adjacent concurrent operations where easy. This would
complicate the failure semantics.

##### Example

```sql
> CREATE TABLE foo();
CREATE TABLE

> ALTER TABLE CONCURRENTLY foo ADD COLUMN i INT NOT NULL DEFAULT 42;
        job_id
----------------------
  594409267822002177

> CREATE INDEX CONCURRENTLY foo_idx_i ON foo(i) ASYNC;
        job_id
----------------------
  594409269143273473

> ALTER TABLE CONCURRENTLY foo ADD CONSTRAINT foo_unique_i UNIQUE (i), ADD COLUMN j INT;
        job_id
----------------------
  594409267848740865
  594409270501670913
```

## Drawbacks

### Long-running transactions

Today schema changes are rather lightweight operations. We write out a job and
a small change to the descriptor and then wait for the asynchronous set of
operations to complete. In this proposal, index creation and
validation will occur underneath the user transaction. This
means that any reads (not due to backfills or validations) will
need to remain valid throughout the course of the transaction
in order to maintain serializability (see pessimistic mode
discussion below).

### Client connection breakages and gateway node failures

One arguably nice property of Cockroach's schema changes today is that they
will proceed even if the client connection breaks before the operation finishes.
Determining when a schema change has reached the point at which it will proceed
is not an easy thing to programatically determine, but, in practice, most schema
changes enter this phase quite soon after statement execution (or `COMMIT` in
the case of explicit transactions). The hope of this RFC is to regain this
behavior more explicitly and usefully with the `ASYNC` form of schema changes,
though, admittedly, that remains the most vague and open-ended portion of the
proposal.

## Rationale and Alternatives

Some discussion of making execution and storage aware of physical layout. It
seems possible to encode the structure of the table into the row encoding
directly. In that case, the execution engine might be able to detect that the
row encoding does not match its expectation and it could upgrade its view of
the table metadata during execution. This style of approach seemed extremely
cross-cutting and complex. It also does not deal with constraint addition.

## Additional Considerations

### Mixed version state

Almost certainly we'll need to retain the existing behavior in the mixed version
states. We'll need to carefully sequence the rollout of this functionality to
ensure that the upgrade process is smooth. The details of the mixed version
state remain an unresolved issue.

### Pessimistic Mode

See [#52768]. Transactional schema changes will **require** pushing their
timestamp which will require read refreshes. In normal times such refreshes
can be catastrophic when considering large transactions. One good thing is that
transactional schema changes are not necessarily going to transactionally (as
is refresh spans) interact with all that much data. That being said, the cost
of a rollback will be potentially immense.

   * In the first pass, we could imagine using in-memory, best-effort locks.
      * Are these exclusive? All that exists today are exclusive locks but
        best-effort shared locks are a small task. Distributed, durable upgrade
        locks are a much bigger task.
      * There is some talk about making these much higher reliability.
         * Right now in-memory on leaseholder.
         * Don’t get transferred with lease.
         * Work item to transfer them with cooperative lease transfers.
         * Most lease transfers are cooperative.
   * For all tracked read spans, acquire locks.
      * This implies the existence of ranged shared locks which are currently
        not supported.
   * This will require refreshes but those refreshes are likely to succeed.
   * This is purely an optimization to prevent restarts.
      * Certain workloads with concurrent access to the table will likely always
        invalidate reads making this a liveness problem.
      * Not a safety concern.

A likely acceptable first step would be to implement pessimistic mode on top of
best-effort, non-replicated, upgrade, ranged read locks. These may be a goal of
the 21.1 release.

## Future work

### `SAVEPOINT`s and Nested Transactions

Currently attempts to rollback to a savepoint in a transaction which would
discard a schema change is not allowed ([#10735]). This proposal does not intend
to remedy this limitation in its first pass. However, it would be problematic if
the proposal precluded a solution.


Any solution would need to keep track of transactional schema change state with
each savepoint. Upon rollback, will need to use a child transaction to move the
table descriptor back to the logical point it existed as of that savepoint, then update the TableCollection. Should “just work”.


## Unresolved questions

* What should the semantics be when adding a column with a default value and using
  the `CONCURRENTLY` keywords?
  * Seems like ASYNC operations should not take effect at all.
* Should we use the `CONCURRENTLY` keyword of postgres?
  * It's the conceptually related keyword but our semantics are likely to
    differ dramatically.
  * Perhaps we should use a different keyword like `ASYNC`.
* How big of a problem is it for transactions to observe side-effects of
  non-committed DML transactions?
  * In particular, the observation would an error when success might be
    possible if the concurrent DML fails to commit.
  * E.g. enforce a constraint before it’s completely added.
  * Long-running schema changes which perform writes seem uncommon.
* Where do we want to draw the line of accommodating clients vs. being
  principled.
  * Eventually we should support postgres’s behavior as much as possible.
    * For things which are expensive, we should provide more async, weaker
      semantics for those who know what they are doing.
  * Should the default be something other than transactional if there are
    risks of restarts and regressions?
    * Seems like no, but we need to get our async story in order and document it
      well.
* More of a TODO than an open question: What do alter columns look like for interleaves?
  * There's sort of two options with regards to the backfill of children and both
    aren't great.
      * Create a new index as an interleaved sibling under the same parent. This
        will lead to a fragmented LSM and rollbacks will be expensive because
        you can't rely on clear range operations.
      * Rewrite the entire interleave. This is in some ways simpler but might
        capture way more data and is code that we don't currently have.
* Can we defer validations and coordination with concurrent transactions to
  commit time?
    * This would be a sweet optimization for transactions with large numbers of
      statements which require backfills or validations.
    * Seems like it adds some complexity to the backfills and their expectations
      of table state.
    * What about FK relationships? If you can’t take advantage of indexes, will
      validation be too slow?
    * In 21.1 we can, in theory, do a full table scan.
    * How much can be deferred?
* Is it okay to not deal with the `SetSystemConfigTrigger`?
  * One oddity in the cockroach transactional schema change landscape is that
    if you want to perform a DDL in a transaction, it must be the first writing
    statement. This is due to implementation details of zone configurations but
    is thematically aligned with regards to limitations of schema changes in
    transactions.
  * [#54477]
* Should jobs be used in any way for dealing with backfills and validations
  underneath a user transaction?
    * What are the pros? Lots of cons.
* How exactly will the mixed version state work and how will this proposal be
  sequenced across releases.
    * The current thinking is that the new schema change infrastructure and
      child transactions will be delivered in 21.1. Other surrounding
      infrastructure, including, perhaps, `CONCURRENTLY` schema changes may also be
      delivered. Nevertheless, it is likely that in the mixed 21.1-21.2 state,
      schema changes will behave as in 21.1.
    * Does this change of behavior upon upgrade imply that the change should be
      gated by some setting? How do we ensure that this setting is flipped on
      prior to 22.1?
* Privileges: roles and role membership are stored in a system table which has
  its version incremented on every write in order to ensure properly
  synchronized dissemination of information. How will that code be affected by
  this work?
* Dropping schema elements: How should drops interact with ongoing schema
  changes? Should they cancel them? How would we implement this?

[#10735]: https://github.com/cockroachdb/cockroach/issues/10735
[#35738]: https://github.com/cockroachdb/cockroach/issues/35738
[#42061]: https://github.com/cockroachdb/cockroach/issues/42061
[#43057]: https://github.com/cockroachdb/cockroach/issues/43057
[#44645]: https://github.com/cockroachdb/cockroach/issues/44645
[#46541]: https://github.com/cockroachdb/cockroach/issues/46541
[#47719]: https://github.com/cockroachdb/cockroach/issues/47719
[#47989]: https://github.com/cockroachdb/cockroach/issues/47989
[#52768]: https://github.com/cockroachdb/cockroach/issues/52768
[#54477]: https://github.com/cockroachdb/cockroach/issues/54477
[#54633]: https://github.com/cockroachdb/cockroach/issues/54633
[#55981]: https://github.com/cockroachdb/cockroach/issues/55981
[online schema change RFC]: ./20151014_online_schema_change.md
[F1 schema change paper]: https://pdfs.semanticscholar.org/5086/e7566a6b706c03f83a1638752b1a1c8209b7.pdf
