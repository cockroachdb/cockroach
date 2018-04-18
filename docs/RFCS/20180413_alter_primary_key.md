- Feature Name: Alter Primary Key
- Status: Draft
- Start Date: 2018-04-11
- Authors: David Taylor
- RFC PR: []()
- Cockroach Issues:

[TOC levels=1-3 markdown]: #

# Table of Contents
- [Summary](#summary)
- [Motivation](#motivation)
- [Guide-level explanation](#guide-level-explanation)
- [Reference-level explanation](#reference-level-explanation)


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

At a high level, a "primary key" in CockroachDB is just an unique index on the
primary key columns that also "stores" all the other table columns. Thus
"changing the primary key" can be as simple as just changing which (compatible)
index we call "primary".

In practice it is not quite as simple: various assumptions in the code handling
the table special-case the primary index (e.g. it does not need to declare that
it stores all the columns), the indexes must be compatible with the defined
semantics of a `PRIMARY KEY` and, in particular, existing secondary indexes need
to be considered.

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

Postgres uses the syntax `ADD PRIMARY KEY... USING INDEX foo` to use an existing
index `foo` as the primary key. The explicit specification of the replacement
index allows predictable performance: if the index exists and it and the schema
are compatible, it just switches. If the index does not exist or is
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
     the new primary key (as that set is what is constrained to be unique). must
     be unique and its columns must be `NOT NULL`.
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

### Special Handling of Primary Indexes

In various places, the descriptor handling of the primary index is special-cased
with respect to handling stored or extra columns. With the ability to swap the
index which is "primary", such special-handling will need to be revised. Ideally
it can be simply removed, e.g. all columns just appear in the extra or stored 
lists, rather than being implicitly inferred to be there if primary.

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