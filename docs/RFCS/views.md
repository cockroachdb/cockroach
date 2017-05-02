- Feature Name: Non-Materialized Views
- Status: completed
- Start Date: 2016-09-01
- Authors: Alex Robinson
- RFC PR: [#9045](https://github.com/cockroachdb/cockroach/pull/9045)
- Cockroach Issue: [#2971](https://github.com/cockroachdb/cockroach/issues/2971)

# Summary

Add support for non-materialized views to our SQL dialect.
Materialized views are explicitly out of scope.

# Motivation

[Views](https://en.wikipedia.org/wiki/View_(SQL)) are a widely-supported
feature across
[all](https://www.postgresql.org/docs/9.1/static/sql-createview.html)
[major](http://dev.mysql.com/doc/refman/5.7/en/views.html)
[SQL](https://msdn.microsoft.com/en-us/library/ms187956.aspx)
[databases](http://www.ibm.com/support/knowledgecenter/SSEPEK_10.0.0/intro/src/tpc/db2z_views.html).
In a sense, they're table stakes. Views are used for a number of reasons,
including aliasing complex queries, limiting access to underlying data, or
maintaining compatibility with legacy code as changes are made to the underlying
database schema.

# Scope

As a bare minimum, we need to support creating views, referencing views
in queries, and dropping views. We should probably also support altering
views, although it would be possible to get good use out of views
without that.

Beyond the basics, though, different major SQL databases offer differing
features around views. Some allow writing to underlying tables through views
and checking the integrity of such updates. Some support a
`CREATE OR REPLACE` statement to change a view's definition in a single
command or idempotently create a view. Some have special restrictions on
the `CREATE OR REPLACE` command. Some allow additional options on views,
such as whether they're only temporary for the current session.

Given our PostgreSQL compatibility, it makes sense to support what they
support unless we have reason not to.

* Even though it isn't part of the SQL standard,
  [Postgres supports](https://www.postgresql.org/docs/9.1/static/sql-createview.html)
  the `CREATE OR REPLACE` statement so long as the replacement query
  outputs the same columns as the original query in the same order
  (i.e. it can only add new columns to the end).
* We should also support the applicable limited `ALTER VIEW` options that
  [Postgres offers](https://www.postgresql.org/docs/9.1/static/sql-alterview.html).
* Posgres supports views over
  [VALUES statements](https://www.postgresql.org/docs/9.1/static/sql-values.html),
  while MySQL does not. We have taken the approach of supporting VALUES
  statements elsewhere, and will do so here as well.
* Postgres supports specifying fewer column names than are returned by a
  view's query, while MySQL does not. Neither allows specifying more column
  names than the query returns. We will match Postgres's behavior here.
* Even though it is intentionally undefined by the SQL standard (and despite
  [SQL Server's lack of support](https://msdn.microsoft.com/en-us/library/ms187956.aspx#Anchor_1),
  both Postgres and MySQL support `ORDER BY` clauses on the select
  statements used in view definitions, and respect them in later queries.
  For compatibility reasons, we should try to do so as well.
* We should support the `RESTRICT` option as the default on
  [`DROP VIEW`](https://www.postgresql.org/docs/9.1/static/sql-dropview.html).
  While supporting `CASCADE` as well would be nice, it can implemented
  separately at a later time, as we have so far
  [chosen to do for DELETE/TRUNCATE on foreign keys](fk.md#cascade-and-other-behaviors).
* Now that there will be references to underlying tables and indexes, we
  will have to support the `RESTRICT` option (and eventually `CASCADE`)
  on various other `DROP` commands as well.
* [Postgres started supporting inserts/updates/deletes through what they call "simple" views](https://www.postgresql.org/docs/9.3/static/sql-createview.html)
  as of version 9.3 (from late 2013). I propose that we put off supporting them
  for now.
* [For historical reasons](https://www.postgresql.org/docs/9.4/static/sql-alterview.html),
  Postgres allows for `ALTER TABLE` to be used on views, but I propose that
  we avoid supporting that for as long as possible.

# Detailed design

The major problems we have to solve to support the in-scope features are
validating new views, storing view definitions somewhere, tracking
their dependencies to enable `RESTRICT` behavior when schemas are
changed, and rewriting queries that refer to views.

## Validating new views

Without having dug very far into the code yet, I'd expect to be able to
reuse existing query validation functionality pretty directly for this.
There may be some differences (e.g. not allowing `ORDER BY`), but hopefully
not too many.

## Storing view descriptors

We can reuse the
[`TableDescriptor`](https://github.com/cockroachdb/cockroach/blob/develop/sql/sqlbase/structured.proto#L244)
protocol buffer type to represent views. Only a small amount of
modification will be needed to support the needs of views, and reusing
the same descriptor will remove the need to duplicate most of the fields
in the proto and much of the code that processes the proto. Tables and
views are typically used in the same ways, so it isn't much of a stretch
to share the underlying descriptor, which is also what we do to support
the information_schema tables.

## Storing view queries

The more interesting question around storage is around how we will store
the underlying query that defines a view. We could choose to do so either
*syntactically* or *semantically*. We're going to start with a syntactic
encoding for the reasons outlined below.

### Syntactically

Syntactically would mean that we simply store the SQL string that defines
the intended query. At query time, the string would get re-parsed and
re-analyzed every time and inserted in place of where the view name was
used. This is what we currently do for default expressions and check
expressions in table descriptors, and is fairly simple to implement.

There will be some trickiness in handling compatibility in the face of
schema changes. For adding/removing columns or removing tables, we can
do proper verification of schema changes against all dependent views as
they're made.

The trickier thing to handle will be renames of tables and columns. It
is quite difficult to rewrite the SQL string of an arbitrary view
definition whenever a referenced table or column is renamed. To get
around this, we could reject attempts to rename anything in a table that
is depended on by a view.

Star expansions (e.g. `SELECT * FROM foo.bar`) also cause problems for a
syntactic encoding - to prevent a star expansion from changing meaning over
time, we'd have to block users from adding columns to the table that the
star applies to, which would be far too strong of a restriction. We could
potentially expand out all the stars in a query and replace them with
resolved column names, but due to the amount of work involved in doing that,
we will instead prevent users from including star expansions in their view
queries to begin with.

### Semantically

Semantically would mean that we define an encoding for our abstract
semantic query trees and store them rather than the query string.
The query trees would presumably use IDs rather than names to refer
to columns and tables, obviating the renaming problem of a syntactic
representation.

The difficulty of this approach is primarily the work involved in
defining and maintaining a consistent encoding for our abstract trees.
While we think this is something we're likely to do in the future as
it would help with many features (e.g. default expressions, check
expressions, prepared statements, stored procedures, etc.), it's a
very large project, and now might not be the right time to take it on.

Update: Work on an intermediate representation that could be used for
semantically encoding views has begun in
[#10055](https://github.com/cockroachdb/cockroach/pull/10055). Once it
is eventually in place, switching over to it will allow us to remove the
non-standard restrictions required by our initial syntactic encoding.

## Tracking view dependencies

In order to maintain consistency within a database, we need to prevent
a table (or view) that a view relies on in its query from being deleted out
from underneath the view, or from being modified in a way that makes it
incompatible with the view. Thus, upon a request to delete or update a
table/view, we have to know whether or not some view depends on its
existence.

While some other databases (e.g.
[PostgreSQL](https://www.postgresql.org/docs/8.4/static/catalog-pg-depend.html)
and [SQL Server](https://msdn.microsoft.com/en-us/library/bb677315.aspx))
use dedicated system tables for tracking dependencies between database
entities, CockroachDB has so far taken the approach of maintaining
dependency information denormalized in the underlying descriptor tables.
For example, foreign key and interleaved table relationships are tracked
by storing `ForeignKeyReference` protocol buffers in index descriptors
that refer back to the relevant tables and columns in both direcitons..

We can take a similar approach for view relationships, meaning that a
`ViewDescriptor` will reference the tables/views it depends on, and each
of the tables/views that it depends on will maintain state referring back
to it. As with foreign key constraints, the overhead of maintaining state
in both places should be negligible due to the infrequency of schema updates.

## Handling schema updates

I expect that schema changes to views  will mostly mirror how we handle
schema updates to tables today, but with the added need to verify the
validity of changes (to tables, indexes, and views) against referenced
or dependent descriptors. As mentioned in the discussion of a syntactic
vs semantic representation above, this will mean restricting renames and
and removeals of columns and tables depended on by views.

## Query rewriting

Similar to validating new views, this should mostly be manageable just
by reusing existing code. For example, it's easy to imagine adding
to the logic for looking up a table descriptor to also handle view
descriptors, then inserting (and processing) the subquery from the view
in its place.

# Unresolved questions

None.
