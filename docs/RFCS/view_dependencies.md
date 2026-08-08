- Feature Name: Cluster upgrades after view descriptor enhancements
- Status: draft
- Start Date: 2017-08-08
- authors: knz
- RFC PR:
- Cockroach Issue: #10028 #10083 #17501

# Summary

This RFC first outlines how to solve several shortcomings of views, as
a mere introduction to the main topic: how to safely perform an
incremental ugprade of a cluster when some nodes support said solution
and other (older) nodes do not.

As of this writing (to start the discussion) there seem to be three
possible solutions:

- simplest: mandating a stop-the-world upgrade;
- simpler: tolerating broken views for clients connected to non-upgraded nodes;
- complex: create two "versions" of view descriptors, and prevents
  solving the root problems (#10028 #10083) for descriptors with
  "old versions" while enabling the solution for descriptors using
  the "new version". The question then becomes what to do with old view
  descriptors over time to avoid bloat both in the DB and in the code.

# Motivation

The [implementation of views](views.nd) in CockroachDB uses a purely
syntactic representation for the view query in the view
descriptor.

For example a view defined with `CREATE VIEW v AS SELECT v FROM kv` is
encoded in a table descriptor for `v` containing the literal string
"`SELECT v FROM kv`" in its `ViewQuery` field.

This implementation has the following two shortcomings:

- it cannot easily support table, column or index renames in view
  queries. More details on this below.
- it cannot safely support star expansions in view queries. For
  otherwise, a view defined with `CREATE VIEW s AS SELECT * FROM kv`
  would break when adding a new column to `kv` after `s` was created.

# Two solutions to enable star expansion in views and renames in dependencies

Recall, this RFC is not about which solution will be implemented. This
section is not strictly necessary to understand the rest of the RFC,
and is just provided here for some context.

## Syntactic encoding vs. object renames

Regarding why it is difficult to rename tables, column or indexes depended
on by views. For example, consider:
  
  `CREATE VIEW r AS SELECT foo.v, kv.v FROM (kv AS foo, (VALUES (1),(2)) AS kv)`
  
To recognize that the first occurrence of `kv` (in "`kv.v`") should
*not* be renamed when the table `kv` is renamed, we would need a
semantic encoding. However currently view descriptors use a
syntactic encoding. To perform semantic substution on the syntactic
form in the current code base, we'd need to perform he entire
logical planning for the view query and also equip logical planning
with renaming facilities it doesn't support.

This would be a lot of work!

## Solution 1: semantic encoding

That's more or less what pg does and what we probably would like to do
in CockroachDB in the long term.

In this solution:

- the SQL AST in CockroachDB is extended to carry the results of name resolution, in particular:
  - AST nodes for table names are expanded to also be able to refer to descriptors (e.g. by ID);
  - AST nodes for index references are expanded to be able to link to the descriptor they refer to;
  - AST nodes for column references are expanded to be able to link to the descriptors that define the column they refer to;

  (Note: these extensions are purely semantic because there exists no SQL syntax to represent them syntactically!)

- the SQL AST is modified in-place when stars are expanded.
- the view descriptor is changed to encode the full semantic SQL AST, not merely the SQL syntax;
- SHOW CREATE VIEW is changed to pretty-print the part of the SQL that does have a syntactic representation;
  - the pretty-printing code is changed so that if an AST node contains a reference to a descriptor,
    the *current* name of the referenced entity is used instead of the one that was known when the view was created;
- when a view descriptor is used (in FROM in a query), the semantic AST is loaded and name resolution is skipped - 
  instead the descriptor references stored in the semantic ASTs are used directly.

In this solution, because stars are pre-expanded during view creation,
star-containing views becomes immune to problems when new columns are
added afterwards.

Also, after this solution is implemented, there is no more dependency
from the view descriptor onto the current names of the things that are
depended on. From then on, they can be renamed safely without needing
to alter view descriptors.

## Solution 2: namespace overloads

This solution piggy-backs on a mechanism that CockroachDB is gaining
anyway in order to support Common Table Expressions (CTEs): *name
scopes*. CTEs bring name scoping from SQL and when/if CockroachDB
supports CTEs the planning engine will support scopes too.

A scope, like in other programming languages, is a context in which
names can be made to refer to other things than they would otherwise.

Consider the two following queries:

```sql
                                   SELECT * FROM kv;
WITH kv(x) AS (VALUES (1),(2),(3)) SELECT * FROM kv;
```

The two queries are "the same": `SELECT * FROM kv`
What changes is the meaning of the name "`kv`": the first query columns selects from
the table `kv` in the database, whereas the other refers to the VALUES clause.

Scoping in SQL, like in other programming languages, can be nested, so we
have a stack of scopes. For example:

```sql
WITH kv AS TABLE foo
  SELECT * FROM (
       WITH kv AS TABLE bar
	      SELECT * FROM kv
	    ),
		kv;
```

The inner SELECT will have a first operand that's really `bar` in
disguise; whereas the outer SELECT will have a second operand that's
really `foo` in disguise: the mapping between the name `kv` and a
table is restored when the inner WITH scope ends.

So to implement CTEs we need a stack of naming environments. This is a
general mechanism, for which we already have an implementation ready
(#17501).

Once this mechanism is available, we can reuse that for views too! We
do this by storing in the view descriptor the name-to-ID mappings
*that were in effect at the time the view was loaded*, and loading
these mappings in the name environment when the view descriptor is
accessed.

This is incidentally also what #17501 is doing, and it works fine.

## Common aspects between the solutions

Regardless of the solution taken, in order to solve the two root
problems (#10028 and #10083) we are finding a way to liberate the
database schema from the hard requirement to forbid schema changes on
view dependencies.

# Main topic of this RFC - what to do during cluster upgrades?

Suppose we have CockroachDB 1.1 where views cause a "lock down" on the
schema because none of the solutions described above are yet implemented.

The are (at least) two problematic scenarios.

## Scenario A: renames post-upgrade.

1. in 1.1, some view is created:

    `CREATE TABLE kv(k INT, v INT); CREATE VIEW v AS SELECT v, k FROM kv`

2. one node is upgraded to 1.2.
3. on the new node, the statement `ALTER TABLE kv RENAME TO foo` is issued.
  "It's possible" , because 1.2 knows how to deal with views properly now.
4. on another node not yet upgraded, `SELECT * FROM v` is issued. The query breaks!
   because the non-upgraded node doesn't know how to "use" the new information
   in the descriptor / the solution.

## Scenario B: views with stars post-upgrade

1. in 1.1, some table is created:

    `CREATE TABLE kv(k INT, v INT);`
	
2. one node is upgraded to 1.2.
3. on the new node, a view is created: `CREATE VIEW v AS SELECT * FROM kv`.
4. on another non-upgraded node, a new column is added on the table. This is valid
   because even in 1.1 (currently), we can already add columns to tables
   depended-on by views: `ALTER TABLE kv ADD COLUMN w INT`
5. on the non-upgraded node, the query `SELECT * FROM v` is run. This breaks!
   That's because the non-upgraded node is then surprised to see more columns
   in the view's logical plan (k,v,w) than it expects (k,v).

# Possible approaches to cluster upgrades

## Stop-the-world upgrade

1. stop all nodes.
2. start one node with 1.2; this runs a migration which rewrites all view descriptors.
3. start other nodes with 1.2. This knows how to use the new view descriptors.

## Break views for non-upgraded nodes.

1. start one node with 1.2; this runs a migration which rewrites all view descriptors, *and bumps their FormatVersion* field.
2. on every node still running 1.1, any query using the upgraded view descriptors fail with
   "unknown format version"

## Maintain old view descriptors alongside new view descriptors.

In this solution, only views created with the new CockroachDB version (1.2) will
use the new fields/semantics. So in the same database we can have both "old views" and "new views".

In this solution:

- the path for renames must inspect *if there is any view depending on
  the table/column/index being renamed that is still using the old
  format*, and if so reject the rename.
- either the `ViewQuery` field for new views must be populated by an invalid string,
  or new views must be created with a new `FormatVersion`, in either case ensuring that
  old nodes cannot use them at all.
- the new code in 1.2 must be careful to still properly populate the
  `DependedOnBy` and `DependsOn` fields even on new view descriptors, so
  that old nodes can still handle DROP and BACKUP/RESTORE properly.

# Remaining questions

- Which solution to adopt?
- if we keep two sets of view descriptors side by side, when will we be able to get rid
  of the old code path?
  (This question incidentally extends to our current compatibility code for FamilyVersion vs InterleavedVersion
  w.r.t regular table descriptors. To my knowledge, we never really properly addressed the process of
  retiring and purging the FamilyVersion code either.)

- **if we keep two sets of view descriptors, what feature can we offer to users to upgrade an "old descriptor"
  to the "new format" once the user knows it's safe?** The reasons why this is necessary is that
  there can be a complex tree of view dependencies and simply requiring dropping all the views and creating
  them anew is going to cause very poor UX.
