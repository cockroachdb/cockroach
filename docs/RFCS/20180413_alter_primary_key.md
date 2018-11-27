- Feature Name: Alter Primary Key
- Status: Draft
- Start Date: 2018-04-11
- Authors: David Taylor
- RFC PR: [25208](https://github.com/cockroachdb/cockroach/pull/25208)
- Cockroach Issues: #19141

[TOC levels=1-3 markdown]: #

# Table of Contents
- [Summary](#summary)
- [Motivation](#motivation)
- [Guide-level explanation](#guide-level-explanation)
- [Reference-level explanation](#reference-level-explanation)

# Draft Status Note

This feature was de-prioritized and the design process paused after the initial
draft of this RFC circulated.

As noted above, this document is still in draft status and requires further
review and expansion before acceptance. It is merged as-is to provide a starting
point when that work is revisited. Some of the areas needing further attention
are explicitly highlighted inline below. The [original
PR](https://github.com/cockroachdb/cockroach/pull/25208) has additional context.

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
to change it an important feature of a maintainable database.

# High-level Summary

Note: This document assumes familiarity with the content of
[`Structured data encoding in CockroachDB SQL`](`docs/tech-notes/encoding.md`).

At a _high level_, a "primary key" in CockroachDB is just an unique index on the
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

## Syntax

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
`ALTER PRIMARY KEY (col1, col2) USING INDEX idx_foo`

In the interest of out-of-the-box compatibility, we *could* silently interpret
`ADD PRIMARY KEY` as `ALTER PRIMARY KEY` but this seems like a risky/error prone
blurring of semantics. Instead, we may be able to recognize when a `DROP` and
`ADD` are clause paired in the same `ALTER TABLE`, replacing the pair with our
supported in-place alteration.

### Compatible Primary Indexes

The definition of primary key -- `UNIQUE` and `NOT NULL` adds some constraints
on which indexes are eligible to become the primary key:
  1) The index must be `UNIQUE`, indexing _exactly_ the columns intended to be
     the new primary key (as that set is what is constrained to be unique).
  2) All indexed columns must be `NOT NULL`
  3) Stores all non-indexed table columns.

### Secondary Index Compatibility

For a new proposed primary key to be valid, all the columns in it must be
encoded by each of the existing secondary and primary indexes, meaning that they
are either indexed by or stored (implicitly or explicitly), by those indexes.

```
For each index i in table,
  For each column c in proposed key,
    If i does not index or store c, fail.
```

TODO: Address unique-ifying of non-unique secondary index keys. Currently this
is done by adding PK cols, as that ensures uniqueness. That stays OK as long as
the old PK index stays around as a unique index. Expand this to cover what
follows.

### Special Handling of Primary Indexes

In various places, the descriptor handling of the primary index is special-cased
with respect to handling stored or extra columns. With the ability to swap the
index which is "primary", such special-handling will need to be revised. Ideally
it can be simply removed, e.g. all columns just appear in the extra or stored
lists, rather than being implicitly inferred to be there if primary.  In many
cases removing special handling may simplify some code that currently needs to
expand these implicit cases.

Beyond the mechanical metadata computation and bookkeeping, there are some cases
where the primary index currently has special-case behaviors, chiefly support
for column families, which will need to be expanded other indexes as well.

### TODO: Examples (descriptors before and after)

### Implementation Plan

Implementing the above likely begins by eliminating as much special-handling of
the primary index as possible: adding family support to all indexes, adding
explicit fields that track columns stored, implicitly indexed, etc and/or always
populating them and using them for all indexes, primary or otherwise. Once the
primary index is no longer given special treatment, the swap above, when its
conditions are met, should be possible.

### Follow-up, Out-of-Scope Work

These are intended as a starting point for future work, capturing any discussion
that was had in the process of forming the above plan, but is *not* in scope at
this time.

#### Automatic Secondary Index Rewriting

The process above only works with compatible secondary indexes. Initially it
will be up to the user to prepare their schema, replacing their existing indexes
with versions that store the correct columns. In the future, this process could
be automatically handled by the `ALTER ... PRIMARY KEY` statement. This would
make the statement into a long-running job, and thus imply job control,
including determining potential interaction with jobs for individual indexes.
This is out of scope for the initial implementation.

#### Altering Indexes In-Place

Preparing for a primary key change requires ensuring all secondary indexes are
compatible with both the old and new primary key. This likely involves altering
the existing indexes -- which is only currently possible via the creation of new
replacements before dropping the existing ones. It might be nice to have an some
form of in-place alteration, whereby columns could be added to the set stored by
an existing index. This would require a schema-change process and backfill to
rewrite that index. This could be useful in general, but in particular could
streamline changing a primary key where several such alterations are likely.

#### Automatic Dropping of the Previous Index

In many cases, queries may still be using the previously primary index so it
may or may not be disruptive to drop it automatically. It might make sense in
some cases -- e.g. if or when a syntax is added that does automatic creation or
modification of secondary indexes.

For tables created without an explicit primary key, the extra rowid column and
the index on it are very likely no longer serving a useful purpose if the index
considered primary changes. These are likely candidates for automatic dropping.
