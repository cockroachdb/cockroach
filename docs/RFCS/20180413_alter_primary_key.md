- Feature Name: Alter Primary Key
- Status: Draft
- Start Date: 2018-04-11
- Authors: David Taylor, Rohan Yadav, Solon Gordon
- RFC PR: [25208](https://github.com/cockroachdb/cockroach/pull/25208)
- Cockroach Issues: #19141

# Table of Contents
- [Summary](#summary)
- [Motivation](#motivation)
- [High-level summary](#high-level-summary)
- [Implementation plan](#implementation-plan)
- [Follow-up, Out-of-Scope Work](#follow-up-out-of-scope-work)

# Summary

Users should be able to change the primary key of an existing table.

# Motivation

Schemas change over time, and the primary key is particularly special in
Cockroach: it determines where the actual row data is stored.

The choice of primary key -- the choice of columns and their order -- determines
how a table's rows are physically stored in the database, which can be critical
in determining performance of various workloads and, with the introduction of
user-controlled partitioning based on primary key prefixes, can have significant
operational considerations as well.

As applications, workloads and operational requirements evolve over time, the
choice of primary key for a given table may evolve as well, making the ability
to change it an important feature of a maintainable database. In particular,
the inability to alter primary keys is a painful roadblock for customers who
start with a single-region Cockroach cluster and later want to expand into a
partitioned, global deployment. The ability to change primary keys also reduces
friction during prototyping when users are still figuring out their schemas.

# High-level Summary

Note: This document assumes familiarity with the content of
[`Structured data encoding in CockroachDB SQL`](`docs/tech-notes/encoding.md`).

Our approach for changing primary keys is simple at a high level. The user will
specify which columns they want their new primary key to contain, and then
CockroachDB will start a long-running schema change operation. It will compute
which secondary indexes need to be rewritten, start a job where these new
indexes and the new primary key are created, and finally swap out the old
indexes for the new ones.

There are two reasons that a secondary index may need to be rewritten. The
first is if it does not index or store all the columns from the new primary
key. This property is required to support index joins, where we first look up
into a secondary index and then look up the corresponding value in the primary
index.

The second is that a secondary index may depend on the primary index for
"uniqueness." In particular, inverted indexes, non-unique indexes, and unique
indexes with nullable columns all encode primary key columns in their keys to
guarantee that each key is unique. This means that if the primary key changes
and the old primary key is deleted, that uniqueness guarantee no longer holds.

In order to prevent unexpected performance degradation to existing queries, we
will also rewrite the old primary index as a secondary index with the same key,
unless otherwise specified by the user. (This step can be omitted if the old
primary key columns form a prefix of the new ones.)

# Detailed Explanation

## Syntax

We propose to use the syntax `ALTER TABLE t ALTER PRIMARY KEY (col1, col2,
...)`. This will initiate the long-running schema change described above.

This differs from Postgres syntax, where the old primary key constraint is
first dropped via `ALTER TABLE t DROP CONSTRAINT...` and then the new index is
added via  `ALTER TABLE t ADD PRIMARY KEY...`. This is viable in Postgres
because the primary key does not affect the underlying storage; it is just a
special index which is unique and non-null. This is not the case in Cockroach,
where truly dropping the primary key would mean rewriting the entire table
before a new key is even added.

### Example
Say that we have the following `users` table:
```sql
CREATE TABLE users (
  id UUID PRIMARY KEY,
  email STRING,
  name STRING,
  INDEX users_name_idx (name)
);
```

Now we are moving to a global deployment, so we would like to add a new
`region` column which is part of the primary key. First we add the column,
which must not be nullable:
```sql
ALTER TABLE users ADD COLUMN region STRING NOT NULL;
```

Next, we change the primary key.
```sql
ALTER TABLE users ALTER PRIMARY KEY (region, id);
```

The old primary index has been rewritten as a secondary index named
`user_id_idx`. It can be dropped it if no longer needed.
```sql
DROP INDEX users@users_id_idx;
```

## Rewriting indexes

While simple at a high level, there are several key technical details involved
in applying the steps above. Consider again the example of prepending a
`region` column to the `users` table's primary key. The steps we want to
perform behind the scenes are:
1. Build a new primary index on `(region, id)`
2. Build a new secondary index `users_name_idx_rewritten` that uses the new
   primary index's columns for key uniqueness.
3. Build a new secondary index `users_id_idx` which indexes the same columns as
   the old primary key.
4. Swap the old primary index with the new primary index and `users_name_idx`
   with `users_name_idx_rewritten`
5. Delete the old `users_name_idx` and the old primary key.

There are several reasons we cannot accomplish these steps using existing SQL
syntax.
1. The new primary index needs to be *built and encoded as a primary index*. We
   cannot build it as a secondary index and suddenly switch to using it as a
   primary index.
2. The new index `users_name_idx_rewritten` needs to be built using the new
   primary key columns as the extra columns that it stores or indexes. This can
   be done by manually changing the `ExtraColumnIDs` field on the
   `IndexDescriptor` for `users_name_idx_rewritten` as the `IndexDescriptor` is
   created.
3. `users_name_idx_rewritten` *cannot* be visible for reading until the primary
   key of the table is actually changed, because depending on the new primary
   key columns, `users_name_idx_rewritten` might not contain enough information
   to lookup back into the old primary key.
4. The old index `users_name_idx` *must not be visible for reads* as soon as
   the primary key swap occurs, as it will not have enough information to
   lookup into the new primary key.
5. The new indexes being promoted into read, the primary key swap, and the old
   indexes demoted from read must all occur in the *same atomic write*.

To address these issues, we have prototyped two new constructs.

### Covering Indexes

In order to treat some secondary indexes like primary keys at the encoding
level, we introduce a notion of a `covering index`. These indexes are denoted
by a new bit on the `IndexDescriptor`. If an index is covering, it is
implicitly assumed to store all columns and be encoded as a primary key, even
if its not marked as a primary key on `TableDescriptor`. This bit is handled
in `EncodeSecondaryIndex`, where a covering index is encoded using the primary
key encoding. Nothing else needs to change to accommodate this addition, as it
is meant to be used only during primary key changes.

### Primary Key Swap Mutation

The `TableDescriptor` struct has a field `Mutations` that is a list of
`DescriptorMutation` objects. These correspond to pending mutations that need
to be applied to the `TableDescriptor`. This list includes operations such as
adding and dropping indexes. In order to solve the index visibility and index
swap problems from above, we introduce a new `DescriptorMutation` type that
represents the work that needs to get done when processing a primary key swap
operation.

The new mutation type specifies what index ID is the new primary index and
which secondary indexes need to be swapped when the primary key is changed.
When the `SchemaChangeManager` processes this mutation, it performs all the
specified swaps and places index deletion mutations for the old indexes onto
the `TableDescriptor`'s `Mutations` queue. This all-at-once operation changes
the visibility of all the relevant indexes at once so that no one read
operations are performed operations on invalid indexes.

## Interleaving

To support interleaving a new primary key, the ALTER PRIMARY KEY statement will
support an INTERLEAVE IN PARENT clause which behaves the same as it does on a
CREATE TABLE statement. When the new primary index is written, it will be
interleaved into the specified parent exactly as if it were a new table.

However, the scenario where a user wants to run ALTER PRIMARY KEY on an
interleaved _parent_ is trickier, because we do not support dropping an
interleaved parent without CASCADE, i.e. dropping its children as well. Due to
this restriction, we will initially disallow ALTER PRIMARY KEY on interleaved
parents. If a user wants to alter an interleaved parent, they will need to
first rewrite the children so that they are no longer interleaved into that
table. If the child is a primary index, this means altering the primary key. If
it is a secondary index, this means writing a new index and then dropping the
old one. Once the parent index is altered, the user may want to alter the
children once more to interleave them into the new parent.

Admittedly this is a somewhat cumbersome process but it represents the minimum
work necessary to support the interleaved use case. See the Follow-up Work
section for potential improvements.

## Column Families

We currently enforce that primary key columns can only be in the first column
family (ID 0). However, this invariant may be violated when the primary key
changes. We propose removing this restriction so that primary key columns may
appear in any column family. Note that in practice this only affects secondary
indexes, since the primary key columns are encoded in every column family's keys
for the primary index.

## Foreign Keys

Foreign keys currently have a hard dependency on an index of the table they
reference. We intend to remove that dependency and instead have foreign
key checks use whatever indexes are available. However, this work will not
necessarily be completed for 20.1.

Because of this, whenever we rewrite a secondary index while altering a primary
key, we must update any foreign key references to point to the new index.
Furthermore, if any foreign keys depend on the old primary index, we must update
them to instead depend on the new secondary index which represents that index.
Specifically, this means updating the LegacyReferencedIndex field for each
relevant ForeignKeyConstraint.

## Rollbacks
Viewing the primary key change process as 3 distinct stages greatly complicates
the procedure for rolling back from an in-progress primary key change. However,
we can view the asynchronous index cleanup as not part of the job, and consider
the primary key change finished at the point of the atomic index swap. Coincidentally,
that is how it appears to users as well! In this case, the only place where we have
to worry about a cancellation/failure occurring is during the index building phase.
If the primary key change process is requested to rollback during the index building
phase, each new index can be rolled back, and the primary key swap mutation can be
removed.

## Test Plan

We plan to test primary key changes at several levels:

### Unit Tests
Implement a series of test hooks within the process of primary key changes.
These test hooks can be swapped out with waiting/notification functions
during unit tests to verify properties like index visibility at different
points of the primary index change process.

### Roachtests
Run some long-running workloads that interact with a large table before,
during, and after a primary key change.

### Logictests
Implement a logictest mutation that when encountering a `CREATE TABLE`
statement, alters the primary key to a new primary key with the same columns as
the old primary key. This test will give us a large amount of coverage of SQL
operations on the transformed table.

### Schema Changer Specific Tests
Set up a variety of schema change style tests, including cancellations,
rollbacks, node failures, etc.

# Follow-up, Out-of-Scope Work

These are intended as a starting point for future work, capturing any discussion
that was had in the process of forming the above plan, but is *not* in scope at
this time.

## Supporting Postgres Syntax

Mainly for ORM compatibility, we should try to support the Postgres syntax for
changing a primary key, where the old constraint is dropped and then the new
one is added. We might accomplish this by detecting when these statements
appear together in the same transaction and then proceeding with ALTER PRIMARY
KEY as usual. In this case, we would not create a corresponding secondary index
for the old primary key, since the user explicitly specified that it should be
dropped.

## Preserving the Old Primary Index

Rather than rewriting the old primary index as an equivalent secondary index,
we could keep it around as a secondary index with the Covering bit set. This
has two potential advantages. One is that we would rewrite less data. The other
is that we could allow ALTER PRIMARY KEY on interleaved parents, because the
children could remain interleaved into the old primary key. One challenge here
would be keeping the old primary index up-to-date when columns are added or
dropped. There would also be new complexity from allowing tables to be
interleaved into non-primary indexes, which is currently not permitted.

## Supporting DROP on Interleaved Parents

We could also start allowing ALTER PRIMARY KEY on interleaved parents if
https://github.com/cockroachdb/cockroach/issues/8036 were completed. This is
perhaps less ideal than preserving the old primary index, because the child
tables would be interleaved into nothing until their primary keys were also
altered.

## Prepending Columns to Indexes

We anticipate that a common use case for index changes is when a customer is
trying to scale their database to multiple regions. In this case, it is common
for users to need to prepend something like a `region` column so that they can
easily apply geo-partitioning strategies. This command syntactically could be
something like `ALTER TABLE t PREFIX INDEXES BY (region)` and would involve
rewriting each index in the table. This is not directly applicable to the
general use case of changing the primary key, but is a step that could help the
"path to global" user story.
