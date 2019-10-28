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
partitioned, global deployment.

# High-level Summary

Note: This document assumes familiarity with the content of
[`Structured data encoding in CockroachDB SQL`](`docs/tech-notes/encoding.md`).

At a _high level_, a "primary key" in CockroachDB is just a unique index on the
primary key columns that also "stores" all the other table columns. Thus
"changing the primary key" can be as simple as just changing which (compatible)
index we call "primary".

Of course, the new index must be compatible with the defined semantics of a
`PRIMARY KEY` and existing _secondary_ indexes also have some defined behavior
with respect to "the primary key" as far as columns they index and store.

Specifically, secondary indexes "point" from indexed values to the row
containing them, which, as established above, is an entry in the (old) primary
index. Thus it would _seem_ that changing which index is primary would require
re-writing those indexes to "point" to the new primary key.

Fortunately however, that "pointing" is implemented as a requirement that the
primary index key be _derivable_ from the secondary index key-value _pair_.
Specifically, that any primary key columns that are not already indexed
themselves, or explicitly stored, are implicitly stored.

This means that a given secondary index is potentially compatible as-is with
more than one set of columns being the primary key so long as it indexes or
stores a superset of that set of columns.

Thus, it is theoretically possible to simply flip between compatible unique
indexes being called "primary" if all the secondary indexes are compatible with
both.

Such a flip is the easiest and fastest possible primary key change.

For this reason, validation of compatibility followed by simple swap of which
index is "primary" will be the **initial implementation** of primary key changes.
However this is only possible when the user already has a compatible schema.

Fortunately, the problem of altering a primary key that is incompatible
secondary indexes can be reduced to the above case by first replacing the
incompatible indexes indexes with compatible ones (i.e. versions that store
additional columns).

Similarly, if an existing compatible unique index does not exist on the new
primary key columns, one could be created before proceeding as above.

For user-friendliness, follow-up work would do these pre-requisite steps
automatically, as part of an `ALTER ... PRIMARY KEY` operation.

# Detailed Explanation

The initial implementation of changing the primary key is via changing which of
the existing indexes are designated primary, validating that the schema is
compatible with the new index being primary.

Later improvements may include:
  1) add the ability to implicitly add the new index that will be marked primary
     if a matching one does not exist
  2) alter the existing schema, replacing secondary indexes with versions that
     store the new PK columns, to make it compatible with the new primary key.

## Swapping the Primary Index

### Syntax

Postgres uses the syntax `ALTER TABLE ADD PRIMARY KEY... USING INDEX foo` to use
an existing index `foo` as the primary key. The explicit specification of the
replacement index allows predictable performance: if the index exists and it and
the schema are compatible, it just switches. If the index does not exist or is
incompatible, it just fails. This matches the proposed semantics of our initial
version of primary key changes, so it is likely we should re-use the same
syntax.

Note however that this is for _adding_ a primary key. In fact, altering one in
place is not supported, and requires first dropping the old primary key. However
Postgres differs from Cockroach in that it allows tables to have no primary key,
since it uses its own internal storage for the rows themselves. Cockroach's
workaround for tables that do not specify a primary key is to add and index an
extra column, making such a `DROP` a relatively expensive operation that we are
unlikely to implement.

Thus instead of `ADD` we will use `ALTER` in to edit the existing key, e.g.:
`ALTER TABLE <tablename> ALTER PRIMARY KEY USING INDEX idx_foo`

In the interest of out-of-the-box compatibility, we *could* silently interpret
`ADD PRIMARY KEY` as `ALTER PRIMARY KEY` but this seems like a risky/error prone
blurring of semantics. Instead, we may be able to recognize when a `DROP` and
`ADD` are paired in the same `ALTER TABLE` clause, replacing the pair with our
supported in-place alteration.

### Compatible Primary Indexes

The definition of primary key -- `UNIQUE` and `NOT NULL` adds some constraints
on which indexes are eligible to become the primary key:
  1) The index must be `UNIQUE`, indexing _exactly_ the columns intended to be
     the new primary key (as that set is what is constrained to be unique).
  2) All indexed columns must be `NOT NULL`
  3) Stores all non-indexed table columns.
  4) The index is not inverted.

### Secondary Index Compatibility

For a new proposed primary key to be valid, all the columns in it must be
encoded by each of the existing secondary and primary indexes, meaning that they
are either indexed by or stored (implicitly or explicitly), by those indexes.

```
For each index i in table,
  For each column c in proposed key,
    If i does not index or store c, fail.
```

Non-unique secondary indexes and inverted indexes have an additional
complication: in order for their KV keys to be unique, they implicitly encode
all primary key columns into their keys. This means that when we swap the
primary key, we need to rewrite any non-unique secondary indexes which do not
contain all the new primary key columns in their own keys. This can be
performed as a schema change job which adds a new index and then removes the
previous one. (An alternate approach which does not require rewriting is
described in the Follow-up Work section.)

### Example
Say that we have the following existing table:
```sql
CREATE TABLE users (
  id UUID PRIMARY KEY,
  email STRING,
  name STRING,
  INDEX users_name_idx (name)
);
```
Now we are moving to a global deployment, so we would like to add a new `region`
column which is part of the primary key. First we add the column, which must
not be nullable:
```sql
ALTER TABLE users ADD COLUMN region STRING NOT NULL;
```
Next we add a new index, which will later become the primary key. The index
must be unique and store all columns.
```sql
CREATE UNIQUE INDEX primary_global ON users (region, id) STORING (email, name);
```
The existing index `users_name_idx` is not compatible with our new primary key
because it does not store the `region` column. And aside from the compatibility
issue, we want to prepend this index with the `region` column anyway so that we
can properly partition the index. We'll create a new index and drop the old one.
```sql
CREATE INDEX users_region_name_idx ON users (region, name);
DROP INDEX users@users_name_idx;
```
Now we are ready to make the primary key change.
```sql
ALTER TABLE users ALTER PRIMARY KEY USING INDEX primary_global;
```
Optionally we can now drop the previous primary index, assuming that no foreign
key constraints depend on it.
```
DROP INDEX users@primary CASCADE;
```

# Implementation Plan

Although the encoding of primary and secondary indexes is largely the same,
there are a number of places where we currently treat the primary index as
special. In order to allow switching a primary index to a secondary one, we
need to ensure that secondary indexes support all the features that primary
indexes do, such as column families and interleaving. We expect this to be the
bulk of the implementation work for this feature. Once all indexes are
fungible, performing a primary key change is a matter of verifying that the new
primary index is "valid" as described above and then updating the the table
descriptor to swap the `PrimaryIndex` with the appropriate `Indexes` entry.

The major work items for making primary indexes less special are described below.

## Column Families

As it stands, only primary indexes can have more than one column family. We
will extend column family support to secondary indexes as well and introduce
new SQL syntax to specify them:

```sql
CREATE INDEX b(x) STORING (y, z, FAMILY (y), FAMILY (z))
```

This includes introducing a Families field on IndexDescriptor and deprecating
the existing one on TableDescriptor. Note that the key encoding format for
secondary indexes already specifies a column family but it is hard-coded to 0,
so this will be a natural extension.

TODO: An open question raised by Radu is whether we should keep column
family definitions at the table level and just have secondary indexes start
respecting them for stored columns. This would certainly reduce the complexity
we expose to the user. However it raises some other questions, like how (or if)
we would support altering the column family definitions.

## Interleaving

The parent of an interleaved relationship must currently be a primary index,
although the child can be either primary or secondary. We will relax this
restriction so that any index can be an interleaved parent.

## Stored Columns

In various places, the descriptor handling of the primary index is
special-cased with respect to handling stored or extra columns. With the
ability to swap the index which is "primary", such special-handling will need
to be revised. Index descriptors should be updated to make these columns
explicit rather than being inferred. Specifically, the ExtraColumnIDs and
StoreColumnIDs fields should include all columns which are not indexed but are
still stored in the key or value. In many cases removing special handling
should simplify existing code which needs to expand these implicit cases.

## IndexIDs

Various code assumes that the primary index ID is 0. This will need to be
updated to consult the table descriptor instead.

# Follow-up, Out-of-Scope Work

These are intended as a starting point for future work, capturing any discussion
that was had in the process of forming the above plan, but is *not* in scope at
this time.

The goal of this RFC is to describe the minimum changes necessary to enable
online primary key changes. This does put a burden on the user to manually
prepare their indexes so that the change is possible. The main follow-up work
will be to try and reduce this burden for the most common use cases.

## Automatic Secondary Index Rewriting

The process above only works with compatible secondary indexes. Initially it
will be up to the user to prepare their schema, replacing their existing indexes
with versions that store the correct columns. In the future, this process could
be automatically handled by the `ALTER ... PRIMARY KEY` statement. This would
make the statement into a long-running job, and thus imply job control,
including determining potential interaction with jobs for individual indexes.
This is out of scope for the initial implementation.

## Altering Indexes In-Place

Preparing for a primary key change requires ensuring all secondary indexes are
compatible with both the old and new primary key. This likely involves altering
the existing indexes -- which is only currently possible via the creation of new
replacements before dropping the existing ones. It might be nice to have an some
form of in-place alteration, whereby columns could be added to the set stored by
an existing index. This would require a schema-change process and backfill to
rewrite that index. This could be useful in general, but in particular could
streamline changing a primary key where several such alterations are likely.

## Automatic Dropping of the Previous Index

In many cases, queries may still be using the previous primary index so it
may or may not be disruptive to drop it automatically. It may even be
impossible to drop it, for instance if foreign key constraints depend on it.
However, it still might make sense in some cases -- e.g. if or when a syntax is
added that does automatic creation or modification of secondary indexes.

For tables created without an explicit primary key, the extra rowid column and
the index on it are very likely no longer serving a useful purpose if the index
considered primary changes. These are likely candidates for automatic dropping.

## Prepending Columns to Indexes

We anticipate that a common use case for index changes is when a customer is
trying to scale their database to multiple regions. In this case, it is common
for users to need to prepend something like a `region` column so that they can
easily apply geo-partitioning strategies. This command syntactically could be
something like `ALTER TABLE t PREFIX INDEXES BY (region)` and would involve
rewriting each index in the table. This is not directly applicable to the
general use case of changing the primary key, but is a step that could help the
"path to global" user story.

## Letting Secondary Indexes "Depend" on a Secondary Unique Index
A pain point in the existing plan is that non-unique secondary indexes must be
rewritten when the primary key is swapped, because their keys implicitly encode
primary key columns to make them unique. However, note that as long as the old
primary index isn't dropped, its columns could still be used to "uniquify" these
keys. If we introduced and persisted a new "depends" relationship, we could
allow non-unique indexes to continue depending on the old primary index rather
than rewriting them. We would also prevent the user from dropping the old
primary index without first dropping and recreating these indexes. This would
streamline primary key changes at the expense of introducing more user-facing
complexity.
