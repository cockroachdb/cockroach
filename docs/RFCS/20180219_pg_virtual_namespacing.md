- Feature Name: Virtual pg-like schemas
- Status: completed
- Start Date: 2018-01-15
- Authors: knz, Jordan, Peter
- RFC PR: #21456
- Cockroach Issue: #22371, #22753

# Summary

## Short summary

Question: "What are some example clients that are currently broken
specifically because of our incomplete catalog/schema semantics?"

Answer (from Jordan): *"it's all of the GUI tools. like, all of them."*

This RFC aims to address this specifically.

## Longer summary

This RFC proposes to introduce the notion of “schema” in the *namespace*
rules used by CockroachDB so that tools that use virtual tables to
introspect the schema find a similar layout as in PostgreSQL, and
enables them to use the same name structure to construct queries as
they could otherwise with pg.

This makes it possible to:

1. make database and table names appear in the right positions of the
   introspection tables in `pg_catalog` and `information_schema`.

2. support queries like `select * from mydb.public.tbl`, i.e. support
   the standard pg notation for fully qualified tables, needed for
   clients that construct SQL queries by using introspection.

The change does *not* include changing the hierarchical structure of
stored database/table descriptors in CockroachDB. In particular it
does not enable the use of multiple distinct physical schemas
side-by-side inside a single database, i.e. the ability to have two
stored (physical) tables with the same name in the same database (in
different schemas): having both `mydb.foo.tbl1` and `mydb.bar.tbl1`
side-by-side will still not be supported.

(Although it is still possible, like previously, to store a physical
table in the `public` schema that has the same name as a virtual table
in one of the virtual schemas.)

To achieve this, the RFC proposes to tweak the name resolution rules
and how the database introspection virtual tables (`pg_catalog.*`,
`information_schema.*`) are generated.

# Motivation

- The changes proposed here will unblock proper user experience of
  CockroachDB for users of (graphical or non-graphical) DB inspection
  tools.

- The changes proposed here will enable the alignment of the
  terminology used in CockroachDB with that used with PostgreSQL's
  documentation, so that pg's documentation becomes more readily
  applicable to CockroachDB.

We aim for both goals with the general purpose to further drive
developer adoption, especially first-time developers who are not yet
sufficiently savvy to understand the current subtle distinctions
between CockroachDB and pg.

# Guide-level explanation

## Concepts and vocabulary

With this change we must be careful of the terminology. This needs
adjustments in docs, explanations, etc., to better align with Postgres
concepts.

| Word, before   | What is being designated                                   | Word, after            | Visible to users? |
|----------------|------------------------------------------------------------|------------------------|-------------------|
| Database       | The name for a stored database descriptor                  | DB descriptor name     | Mostly not        |
| Database       | Namespace container from the perspective of SQL clients    | Catalog or Database    | Yes               |
| Schema         | The conceptual set of all db/table/view/seq descriptors    | Physical schema        | Mostly not        |
| Schema         | A namespace container for virtual tables                   | Logical schema         | Yes               |
| (didn't exist) | Namespace container for all tables in a catalog            | Logical schema         | Yes               |
| Table          | The name for a stored table/view/sequence descriptor       | Object descriptor name | Mostly not        |
| Table          | The name for a table where a SQL client can store stuff    | Table or Relation      | Yes               |

## How do we teach this?

### Teaching to new roachers

- a CockroachDB cluster contains multiple *catalogs*, or
  "databases". Every cluster starts with at least the `system`
  catalog. More catalogs can be created with `CREATE DATABASE`.

- each catalog contains one *physical schema* called `public`,
  and some additional *virtual schemas*, currently including `pg_catalog`, `information_schema` and `crdb_internal`.
  - a future version of CockroachDB may support multiple logical schemas per catalog besides `public`.

- each schema contains zero or more tables, views, sequences, etc.
  - the `public` schema of different catalogs can contain the same table name, but they will designate different tables.
    For example, two applications can use separate catalogs `myapp1` and `myapp2` and define their own `customers` table,
    and the same name "`customers`" will refer to different tables.
  - the virtual schemas exist in every catalog. They contain the same
    tables in every catalog, but their (automatically generated)
    contents will differ across catalogs.
	- for example `db1.pg_catalog.pg_tables` only contains tables for `db1`,
	  `db2.pg_catalog.pg_tables` only tables for `db2`, etc.

- the session variable `database` designates the current
  catalog, which is used in queries to resolve
  (table/view/sequence/schema) names when no catalog is further
  specified.

  - the `USE` statement, provided as convenience for developers and inspired from MySQL,
    adjusts the `database` session variable.

- the session variable `search_path` contains a list of schema names
  inside the current catalog where to search for functions and tables named in
  queries.

  For example, with a `search_path` set to `public, pg_catalog`, a
  `database` set to `myapp2` and given a query `select * from
  kv`, CockroachDB will search for table `kv` first in the `public`
  schema of catalog `myapp2`, then in the `pg_catalog` schema for
  catalog `myapp2`.

- As a specific CockroachDB extension, a SQL client can specify
  a table name as `dbname.tblname` in some conditions to
  provide compatibility with previous CockroachDB versions.

### Teaching to existing roachers

- We'll adopt the word "catalog" as a synonym for "database" to
  designate the visible portion of the storage container for tables.  The
  word "schema" should be used more sparingly, as it has a specific
  meaning in PostgreSQL which CockroachDB does not yet support.

  - Except for what was called "virtual schema" in CockroachDB; these
    were already properly named after the equivalent PostgreSQL
    concept and do not change.

- The virtual tables in `information_schema`, `pg_catalog` now list
  the catalog in the "Catalog" column, instead of the "Schema" column
  as previously. The previous filler string "`def`" disappears. The
  string "`public`" is now used as filler for the "Schema" column for
  rows that point to actual table data.

  - The virtual schemas are still listed as previously in the "Schema"
    column. They appear (are repeated) for every catalog.

- When talking to users, be mindful that "every catalog has multiple
  schemas, including one physical schema called `public` that contains
  that catalog's physical tables", instead of saying "catalogs contain
  tables".

- `search_path` now refers to schemas, not catalogs, resolved relative
  to the current value of `database`.

# Reference-level explanation

There are 4 relevant separate algorithms for name resolution, depending
on where in the SQL syntax the name resolution occurs:

- Algorithm A1: resolving the name of an *existing* persistent object
  (table/view/sequence or function, later types)
   - `SELECT ... FROM <here>`
   - `INSERT INTO <here> (...) ...`
   - `ALTER TABLE <here> ...`
   - `DROP TABLE <here>`
   - `SELECT <here>(x,y,z)` (function application)
   - `SELECT lastval('<here>')` (sequence name in string)
   - `SELECT '<here>'::REGPROC` (function name to OID conversion)
   - `SELECT '<here>'::REGCLASS` (table name to to OID conversion)
   - **NOT:** `CREATE TABLE ...` (see below)
   - **NOT:** `SELECT ... FROM ...@<here>` (see below)

- Algorithm A2: resolving the name for a *new* persistent object
  (table/view/sequence, we don't support custom functions or types yet
  but if we did they would be included here)
   - `CREATE TABLE <here>` (ditto view, sequence)
   - `ALTER TABLE ... RENAME TO <here>` (ditto view, sequence)
   - **NOT:** `CREATE DATABASE ...` (see below)

- Algorithm B: resolving the name for a column name
   - `SELECT <here> FROM ...` (i.e. names in scalar expressions that don't fall into the patterns above)

- Algorithm C: resolving a *pattern* for persistent object(s)
   - `GRANT ... TO ... ON <here>`

The name resolution for database and index names uses separate
algorithms and that remains unchanged in this RFC.

## Outline of the implementation

The generic, reusable algorithms are implemented in
`pkg/sql/sem/tree/name_resolution.go`.

- `(*TableName).ResolveExisting()`: algorithm A1
- `(*TableName).ResolveTarget()`: algorithm A2
- `(*ColumnItem).Resolve()`: algorithm B
- `(*TableNamePrefix).Resolve()`: algorithm C

## Changes to algorithm A1

Common case: accessing an existing object.

Input: some (potentially partially qualified) name N.
Output: fully qualified name FQN + optionally, object descriptor

Currently:

```
1. if the name already has two parts (D.T), then go to step 4 directly.
2. otherwise (name only has one part T), if `database` is non-empty and the object T exists in the
   current database, then set D := current value of `database` and go to step 4 directly.
3. otherwise (name only has one part T), try for every value D in `search_path`:
   3.1 if the object D.T exists, then keep D and go to step 4
   3.2 if no value in `search_path` makes D.T exist, fail with a name resolution error.
4. FQN := D.T; resolve the descriptor using db D and object name T.
```

After this change:

```
1. if the name already has 3 parts (C.S.T) then go to step 4 directly.

2. otherwise, if the name already has 2 parts (S.T) then:
   2.1. if the object S.T already exists in the current database (including if the current database is the empty string,
        see below for details), then set C := current value of `database` and go to step 4 directly.
   2.2. if the object S.public.T already exists, then set C := S, set S := 'public' and go to step 4.
   2.3. otherwise, fail with a name resolution error.

3. otherwise (name only has one part T), try for every value N in `search_path`:
   3.1. make C := current value of `database`, S := N
   3.2. if the object C.S.T exists, then keep C and S and go to step 4.
   3.3. if no value N in `search_path` makes N.T / C.N.T exist as per the rule above, then fail with a name resolution error.

   (note: search_path cannot be empty, see "other changes" below)

4. FQN := C.S.T; resolve the descriptor using db C and object name T.
```

The rule 2.2 is a CockroachDB extension (not present in PostgreSQL)
which provides compatibility with previous CockroachDB versions.

For example, given a table `kv` in database `foo`, and `search_path` set to its default `public, pg_catalog`:

- `SELECT x FROM kv` with `database = foo`
  - rule 1 fails
  - rule 2 fails
  - rule 3 applies
  - rule 3.1 applies with C=foo, N=public
  - rule 3.2 applies (`foo.public.kv` exists), FQN becomes `foo.public.kv`

- `SELECT x FROM blah` with `database = foo`
  - rule 1 fails
  - rule 2 fails
  - rule 3 applies
  - rule 3.1 applies with C=foo, N=public
  - rule 3.2 fails (`foo.public.blah` doesn't exist)
  - rule 3.1 applies with C=foo, N=pg_catalog
  - rule 3.2 fails (`foo.pg_catalog.blah` doesn't exist)
  - name resolution error

- `SELECT x FROM pg_tables` with `database = foo`
  - rule 1 fails
  - rule 2 fails
  - rule 3 applies
  - rule 3.1 applies with C=foo, N=public
  - rule 3.2 fails (`foo.public.pg_tables` doesn't exist)
  - rule 3.1 applies with C=foo,N=pg_catalog
  - rule 3.2 applies (`foo.pg_catalog.pg_tables` is valid), FQN becomes `foo.pg_catalog.pg_tables`

- `SELECT x FROM kv` with empty `database`
  - rule 1 fails
  - rule 2 fails
  - rule 3 applies
  - rule 3.1 applies with C="", N=public
  - rule 3.2 fails (`"".public.kv` doesn't exist)
  - rule 3.1 applies with C="", N=pg_catalog
  - rule 3.2 fails (`"".pg_catalog.kv` doesn't exist)
  - name resolution error

- `SELECT x FROM pg_tables` with empty `database` (CockroachDB extension)
  - rule 1 fails
  - rule 2 fails
  - rule 3 applies
  - rule 3.1 applies with C="", N=public
  - rule 3.2 fails (`"".public.pg_tables` doesn't exist)
  - rule 3.1 applies with C="",N=pg_catalog
  - rule 3.2 applies (`"".pg_catalog.pg_tables` is valid), FQN becomes `"".pg_catalog.pg_tables`


CockroachDB extensions for compatibility with previous CockroachDB versions:

- `SELECT x FROM foo.kv` with `database = foo`
  - rule 1 fails
  - rule 2 applies
  - rule 2.1 fails (`foo.foo.kv` doesn't exist)
  - rule 2.2 applies (`foo.public.kv` exists), FQN becomes `foo.public.kv`

- `SELECT x FROM blah.kv` with `database = foo`
  - rule 1 fails
  - rule 2 applies
  - rule 2.1 fails (`foo.blah.kv` doesn't exist)
  - rule 2.2 fails (`blah.public.kv` doesn't exist)
  - name resolution error

- `SELECT x FROM foo.kv` with empty `database`
  - rule 1 fails
  - rule 2 applies
  - rule 2.1 fails (`"".foo.kv` doesn't exist)
  - rule 2.2 applies (`foo.public.kv` exists), FQN becomes `foo.public.kv`

- `SELECT x FROM blah.kv` with empty `database`
  - rule 1 fails
  - rule 2 applies
  - rule 2.1 fails (`"".blah.kv` doesn't exist)
  - rule 2.2 fails (`blah.public.kv` doesn't exists)
  - name resolution error

- `SELECT x FROM pg_catalog.pg_tables` with `database = foo`
  - rule 2 applies
  - rule 2.1 applies (`foo.pg_catalog.pg_tables` exists), FQN becomes `foo.pg_catalog.pg_tables`

- `SELECT x FROM pg_catalog.pg_tables` with empty `database` (CockroachDB extension)
  - rule 2 applies
  - rule 2.1 applies (`"".pg_catalog.pg_tables` exists), FQN becomes `"".pg_catalog.pg_tables`

## Changes to algorithm A2

Case: creating a new object or renaming an object to a new name.

Input: some (potentially partially qualified) name N.
Output: fully qualified name FQN (valid to create a new object / rename target)

Currently:

```
1. if the name already has two parts (D.T), then go to step 4 directly.
2. otherwise (name only has one part T) if `database` is set then set D := current value of `database` and go to step 4 directly.
3. otherwise (name only has one part T, `database` not set), fail with an "invalid name" error
4. FQN := D.T. Check D is a valid database; if it is not fail with an "invalid target database" error
```

After this change:

```
1. if the name already has 3 parts (C.S.T) then go to step 4 directly.

2. otherwise, if the name already has 2 parts (S.T) then:
   2.1. set C := current value of `database`; then
        if C.S is a valid target schema, go to step 4 directly.
   2.2. otherwise (<current database>.S is not a valid target schema):
        set C := S, S := 'public' and go to step 4 directly.

3. otherwise (name only has one part T):
   3.1. C := current value of `database`, S := first value specified in search_path
   3.2. if the target schema C.S exists, then keep C and S and go to step 4
   3.3. otherwise, fail with "no schema has been selected"

4. FQN := C.S.T. Check C.S is a valid target schema name; if it is not fail with an "invalid target schema" error
```

The rule 2.2 is a CockroachDB extension (not present in PostgreSQL)
which provides compatibility with previous CockroachDB versions.

For example, given a database `foo` and `search_path` set to its default `public, pg_catalog`

- `CREATE TABLE kv` with `database = foo`
  - rule 1 fails
  - rule 2 fails
  - rule 3 applies
  - rule 3.1 applies
  - rule 3.1.1 applies, FQN := `foo.public.kv`
  - rule 4 checks: `foo.public` is a valid target schema.

- `CREATE TABLE kv` with `database = blah`
  - rule 1 fails
  - rule 2 fails
  - rule 3 applies
  - rule 3.1 applies
  - rule 3.1.1 applies, FQN := `blah.public.kv`
  - rule 4 checks: `blah.public` is a valid target schema, error "invalid target schema"

- `CREATE TABLE kv` with empty `database`
  - rule 1 fails
  - rule 2 fails
  - rule 3 applies
  - rule 3.1 applies
  - rule 3.1.1. applies, FQN := `"".public.kv`
  - rule 4 checks `"".public` is not a valid target schema, error "invalid target schema"

- `CREATE TABLE foo.kv` with `database = foo`
  - rule 1 fails
  - rule 2 applies
  - rule 2.1 fails (C.S = `foo.foo`, not a valid target schema)
  - rule 2.2 applies, FQN := `foo.public.kv`
  - rule 4 checks `foo.public` is valid

- `CREATE TABLE foo.kv` with empty `database`
  - rule 1 fails
  - rule 2 applies
  - rule 2.1 fails (`database` not set)
  - rule 2.2 applies, FQN := `foo.public.kv`
  - rule 4 checks `foo.public` is valid

## Changes to algorithm B

(Used for column names)

Input: some (potentially partially qualified) name N
Output: fully qualified column name FQN + column ID

Currently:

```
1. if the name already has 3 parts (D.T.X), then
   1.2. if there's a data source with name D.T already, then go to step 4 directly
   1.2. otherwise, fail with "unknown column X"

2. if the name already has 2 parts (T.X), then
   2.1. try to find a data source with name T in the current context.
   2.2. if none is found, fail with "unknown table T"
   2.2. if more than one is found, fail with "ambiguous table name T"
   2.3. otherwise (exactly one found), extract the db name D from the data source metadata, then go to step 4.

3. otherwise (name only has one part X), try for every data source in the current context:
   3.1. try to find an anonymous data source that provides column X in the current context.
   3.2. if more than one is found, fail with "ambiguous column name"
   3.3. if exactly one is found, extract the name D.T from the data source metadata, then go to step 4.
   3.4. otherwise, try to find a named data source that provides column X in the current context.
   3.5. if more than one is found, fail with "ambiguous column name"
   3.6. if none is found, fail with "no data source matches prefix"
   3.7. otherwise (exactly one found), extract the name D.T from the data source metadata, then go to step 4

4. FQN := D.T.X, column ID looked up from data source descriptor
```

After this change:

```
1. if the name already has 4 parts (C.S.T.X), then
   1.1. if there's a data source with name C.S.T already, then go to step 5 directly
   1.2. otherwise, fail with "unknown column X"

2. if the name already has 3 parts (S.T.X), then
   2.1. try to find a data source with suffix S.T in the current context.
   2.2. if more than one is found, fail with "ambiguous column name"
   2.3. if exactly one is found, extract the db name C from the data source metadata, then go to step 5.
   2.4. if none is found, then

        2.4.1. if there's a data source with name S.public.T already, then use C:=S, S:='public' and go to step 5 directly
        2.4.2. otherwise, fail with "unknown column X"

3. same rule as rule 2 above
4. same rule as rule 3 above
5. FQN := C.S.T.X, column ID looked up from data source descriptor
```

The rule 2.4.1 is a new CockroachDB extension (not present in PostgreSQL)
which provides compatibility with previous CockroachDB versions.

For example, given a table `kv` in database `foo`

- `SELECT x FROM foo.public.kv`
  - rule 1, 2, 3 don't apply
  - rule 4 applies, FQN := `foo.public.kv.x`

- `SELECT kv.x FROM foo.public.kv`
  - rule 1, 2 don't apply
  - rule 3 applies, FQN := `foo.public.kv.x`

- `SELECT foo.public.kv.x FROM foo.public.kv`
  - rule 1 applies, FQN = given name

- `SELECT foo.kv.x FROM foo.public.kv`
  - rule 1 doesn't apply
  - rule 2 applies
  - rule 2.1 determines no source with suffix `foo.kv` in current context
  - rules 2.2, 2.3 fail
  - rule 2.4 applies
  - rule 2.4.1 applies, FQN := `foo.public.kv.x`

- `SELECT bar.kv.x FROM foo.public.kv`
  - rule 1 doesn't apply
  - rule 2 applies
  - rule 2.1 determines no source with suffix `foo.kv` in current context
  - rules 2.2, 2.3 fail
  - rule 2.4 applies
  - rule 2.4.1 fails
  - rule 2.4.2 applies: unknown column `bar.kv.x`

## Changes to algorithm C

Case: GRANT ON TABLE (table patterns)

Input: some table pattern
Output: fully qualified table pattern FQP

Currently:

```
1. if the name already has two parts with no star or a table star (D.T, D.*), then use that as FQP
   (note: we don't support the syntax *.T in table patterns)
2. if the name only has one part and is not a star (T), then
   2.1 if `database` is set, set D := current value of `database` and use D.T as FQP
   2.2 otherwise, fail with "invalid name"
```

After this change:

```
1. if the name already has 3 parts with no star or a table star (D.S.T, D.S.*), then use that as FQP
2. if the name already has 2 parts with no star or a table star (S.T, S.*), then
   2.1. if `database` is set, set C:= current value of `database`; if C.S is a valid schema, use that as FQP
   2.2. otherwise (`database` not set or C.S not a valid schema), set C := S, S := `public`, use that as FQP
3. if the pattern is an unqualified star for tables, then search for all tables
   in the first schema specified in `search_path`.
```

The rule 2.2 is a new CockroachDB extension.

## Other changes

- same rules / compatibility for zone specifiers

- the vtable generator functions in
  `sql/pg_catalog.go`. `sql/information_schema.go` and
  `sql/crdb_internal.go` are modified to list the database descriptor
  name in the "Catalog" column instead of "Schema". The virtual
  schemas remain in the "Schema" column but are repeated for every
  database descriptor (logical catalog).

  - These generator functions already accept a "db prefix" parameter
    to constraint the visibility they have over the physical
    schema. This is to be filled with the current value of `database`.

Note: already stored views need no special handling due to the compatibility rules.

# Detailed design

This section has two parts: a [background section](#background)
reminds the reader of what is expected.

A ["problems with CockroachDB"
section](#current-problems-with-cockroachdb) spells out what are the
current shortcomings.

A last [detailed solution section](#detailed-solution) maps the
proposed solution, outlined in the reference-level guide above, to the
detailed problem statement. Two alternatives are proposed.

## Background

This section provides an introduction to standard naming rules in SQL
and what are the differences between the Postgres and MySQL
dialects.

If you are already intimately knowledgeable with these rules, the
following high-level summary should be a sufficient refresher:

- we must pay attention to the 3 separate features "name resolution",
  "database introspection" and "meta-introspection". A common pitfall
  when reasoning about SQL naming is to only think about the
  first. The latter two features, once all is said and done, more or
  less mandate a 3-level logical namespace with the components
  catalog, schema, relation, and restricts the spectrum of what can be
  done about the first feature.

- there are three separate rules (algorithms) for name resolution: one
  for persistent objects (including tables and functions), one for
  column references, and one for sub-parts of complex values.

Feel free to skip to the next section (["problems with
CockroachDB"](#current-problems-with-cockroachdb)) if you already know
these details. However, that will refer to some details presented here.

### Terminology and high-level features

The terminology for object names in standard SQL, and pg's dialect in
particular, uses the words "catalog", "schema" and "relation".  These
define a *namespacing scheme*: relation names are scoped to a schema
namespace; schema names are scoped to a catalog namespace.

"Scoping" means the same as it does in e.g. C or Go: it makes it
possible to reuse the same name for different things. For example,
this standard naming structure allows the same name `tbl1` to
designate two different tables, e.g. `mydb.schema1.tbl1` and
`mydb.schema2.tbl1`.

Within this context, any SQL engine must provide the following 3 features:

1. name resolution for database objects.
2. introspection of database objects via `information_schema` (and, for pg compatibility, `pg_catalog` too).
3. introspection of `information_schema` via `information_schema`.

Each of these three items deserves attention because it provides
boundary restrictions on the work being done here.

### Name resolution

Any SQL engine must provide a translation from language-level,
catalog/schema/relation *semantic* names to physical,
in-memory/on-disk data structures. The question that needs to be
mechanically answered is:

*Which table ID / descriptor does this particular name refer to?*

With a variant when accessing individual columns in a table/view:

*Which table ID / descriptor and which column ID inside that does this particular name refer to?*

In CockroachDB, the mechanical transformation of a name to a table ID
/ descriptor is done as follows:

- the *schema* part of the name is used to look up a database ID
  (`select id from system.namespace where name = <schemaname> and "parendID" = 0`)
- the *relation* part of the name is used to look up a table ID,
  within all IDs that have the database ID as `ParentID`:
  (`select id from system.namespace where name = <relname> and "parentID" = <dbID>`)
- then the descriptor for that table ID is loaded if needed; if column ID
  resolution is also needed, that will use just that table/view descriptor.

### Introspection of database objects

SQL engines also provide introspection tables in `information_schema`
(also `pg_catalog` for pg). These must answer the question:

*For each object in the database, what is the canonical name to address it in SQL queries?*

For example, `information_schema.tables` has 3 columns
`table_catalog`, `table_schema`, `table_name` that contain the
canonical name decomposition for tables.

It is possible for a SQL engine to not support the catalog part of
logical names. For example, this seems to be true of MySQL.  In this
case, the `catalog` column is irrelevant; then the following rules
hold:

- if `information_schema.tables` contains a row with values `unused`, `a`,
  `b` for the aforementioned columns, then a query of the form
  `select * from a.b` must work.

- if `information_schema.schematas` contains a row with values
  `unused`, `a` for the catalog and schema name columns, then a
  statement of the form `create table a.b (...)` must work.

However, if the engine claims to support the catalog part, *which is
necessary for compatibility with pg's SQL dialect*, then the following
assertions must hold for `information_schema` to be properly
constructed:

- if `information_schema.tables` contains a row with values `a`, `b`,
  `c` for the aforementioned columns, then a query of the form
  `select * from a.b.c` must work.

- if `information_schema.schematas` contains a row with values `a`,
  `b` for the catalog and schema name columns, then a statement of the
  form `create table a.b.c (...)` must work.

Regardless of which of the two variants is supported, these
observations teach us the following: the structure of
`information_schema` does not give us freedom to design fancy naming
schemes where the path to access a table can be too short or
arbitrarily long.

Really, the SQL community has settled on the catalog/schema/relation
structure for names, crystallized in the structure of the
`information_schema` tables: there's a catalog part, there's a schema
part, there's a table name part. This does not leave us the freedom to
make up our own naming scheme while hoping that existing tools using
db introspection will cope.

### Introspection of `information_schema` (meta-introspection)

`information_schema` (and, for pg, `pg_catalog` too) are very
specifically defined to be *schema names*.  Also they are very much
defined to designate *virtual schemas* that *must exist in every
catalog*.

The working intution is that the virtual tables in the virtual schemas
only contain rows pertaining to the catalog in which they are
(virtually) contained:

- `db1.information_schema.tables` only contains information about tables in `db1`.
- `db2.information_schema.tables` only contains information about tables in `db2`.
- etc.

Meanwhile, they are schemas, so they must appear in the introspection
tables in the right position.  For example, the word
"`information_schema`" must occur in the column `schema_name` of
`information_schema.tables`, with a repeated row for every database
(because `information_schema` exists virtually in every catalog/database):

| Catalog    | Schema             | Table     |
|------------|--------------------|-----------|
| test       | information_schema | tables    |
| test       | information_schema | columns   |
| test       | information_schema | ...       |
| myapp      | information_schema | tables    |
| myapp      | information_schema | columns   |
| myapp      | information_schema | ...       |

### Separate resolution rules for persistent objects, columns and sub-parts of complex values

Four separate rules (set of algoritms) apply to the different
syntactic constructs for names in SQL:

1. **resolution of persistent objects:** (algorithms A1 & A2 in the [reference-level explanation](#reference-level-explanation))
   - the naming of tables / views in FROM clauses.
   - the naming of *functions* in both scalar contexts and FROM clauses.
   - the expansion of table patterns in GRANT.
   - the naming of in-db objects in CREATE, DROP, ALTER RENAME, etc.

2. **resolution of column references:** (algorithm B in the reference-level explanation)
   - a column reference is always composed of an optional persisent object name as prefix, followed by
     a mandatory column identifier.

3. **resolution of sub-parts inside a complex value**, when the engine supports
   sub-parts (e.g. arrays and/or compound types):
   - the naming of columns in the INSERT/UPSERT target column list, or the LHS of UPDATE SET statements.
   - scalar expressions of the form `<expr>[123][456]` or `(<expr>).path[1].to.field[2][3]`

4. **resolution of patterns** in e.g. GRANT (algorithm C in the reference-level explanation)

For example, in the following queries:

     INSERT INTO a.b.c (d.e.f) VALUES (1)
                 ^^^^    ^^^^- this uses the resolution rule 3 of sub-parts inside column 'd'
                    |
                    \--------- this uses the resolution rule 1 of a persistent object (alg A1 & B)

     SELECT (a.b.c.d).e.f FROM a.b.c
             ^^^^^^^ ^^^^      ^^^^^ this uses the resolution rule 1 of a persistent object (alg A1)
               |       |
               |       \------------ this uses the resolution rule 3 of sub-parts inside column 'd'
               |
               \-------------------- this uses the resolution rule 2 for a column (alg B).

    SELECT a.b.c(123)  -- this is a SQL function application
           ^^^^^- this uses the resolution rule 1 of a persistent object (alg A1).

    CREATE TABLE a.b.c ( ... )
                 ^^^^^- this uses the resolution rule 1 of a persistent object (alg A2).

    GRANT SELECT TO admin ON TABLE a.b.c
                                   ^^^^^- this uses resolution rule 4 for  patterns

The choice of which of the two rules 1 or 2 to apply in a scalar
context is made unambiguously by the presence (rule 1) or absence
(rule 2) of a function call argument list starting with '(' after the
name.

The resolution rules are very well specified across all SQL engines:

1. when resolving a name for a persistent object, the last part of the name is always the name of the object.

   The part before that, if present, is the logical schema name. The
   part before that, if present, is the logical catalog name.

   This implies that a fully qualified persistent object name has at
   most 3 components.

2. when resolving a name for a column, the last part of the name is
   always the name of a *column*.

   The part before that, if present, is the name of the relation (one
   defined from a FROM clause). The part before that, if present, is
   the logical schema name. The part before that, if present, is the
   logical catalog name.

   This implies that a column reference has at most 4 components.

3. when resolving a name for a sub-part of a complex value:

   - array subscripts are used by appending `[offset]` to some scalar
     expression.
   - to access a field in a compound type (if those are supported),
     grouping parentheses *must* be used if the thing containing the
     field is a column reference.

    For example, `SELECT (a.b.c.d).e.f` in the query above.

    In constrast, `SELECT a.b.c.d.e.f` would not be allowed, because
     it is ambiguous: it could refer either to `.b.c.d.e.f` in column
     `a` of some implicit table, or `.c.d.e.f` in column `b` of table
     `a`, or `.d.e.f` in column `c` of table `b` in schema `a`, etc.

    In contrast to the resolution of persistent objects above, the path to a
    sub-part of a compound value can be arbitrarily long.

Currently CockroachDB does not support compound types, so the logic
for rule 3 is not yet fully implemented -- we only support arrays. The
support for compound types and field access is not in-scope for this
RFC and not considered further. The code in PR #21753 has ensured that
there is adequate space in the grammar to add this support later, with
concrete suggestions on how to achieve this compatibility.

### Partially qualified names

In all contexts where a fully qualified name (FQN) is accepted, a
*partially qualified* name is also accepted. A partially qualified
name is recognizable because it has fewer components than the number
of components expected for a FQN in that position. (As described
above, the number of components expected for a FQN is unambiguously
defined for each syntactic position.)

A partially qualified name is transformed into a FQN *before* name
resolution, as defined in the previous sections, occurs.

The rules are defined separately for each SQL engine.

- In MySQL, for example, the logical catalog part is always inferred
  to be "`def`", and the logical schema part, if absent, is taken from
  the latest USE statement.

- In PostgreSQL, for example:
  - the logical catalog part, if absent, is inferred from the database
    name specified in the connection string, incidentally also
    available via the built-in function `current_catalog`.
  - the logical schema part, if absent, is inferred by searching each
    schema named in the `search_path` session variable:
    - taking the first item in `search_path`, also designated by
	  the built-in function `current_schema()`, for operations that create a new object;
    - iterating through `search_path` to find a schema that contains
      the persistent object named by the last component, for
      operations that require the object to exist already.

    This search across schemas is made using schemas of the current
    catalog only.

The PostgreSQL rules are not to be taken lightly, because they interact
very specifically with the data `information_schema` and `pg_catalog`.
If, say, `information_schema.tables` mentions two schemas `a` and `b`,
and two separate tables, both called `tbl`, in each of these two schemas,
then a client will expect to be able to set either

    search_path = ['a']

or

    search_path = ['b']

and expect queries of the form

    SELECT * FROM tbl

to resolve `tbl` in one or the other of the two schemas.

This is of particular interest when a client needs to overload a name
or a table that otherwise already exists in `pg_catalog`:

     -- client connects to `curdb`,
     -- client sets search_path = ['public', 'pg_catalog']

     SELECT * FROM pg_tables; -- initially resolves curdb.pg_catalog.pg_tables

     CREATE TABLE pg_tables (x int); -- creates curdb.public.pg_tables

     SELECT x FROM pg_tables; -- now resolves curdb.public.pg_tables

It would be an error to let the client access `pg_catalog.pg_tables`
in the latter query (and give it an error because `x` doesn't exist
there) after they have been able to run `CREATE TABLE pg_tables`
successfully.

## Current problems with CockroachDB

CockroachDB currently has several problems that this RFC aims to address:

- the name resolution algorithms are different than pg's. This means
  that some queries valid in pg's SQL dialect are not valid in
  CockroachDB, and vice-versa.

  The specific phrasing of the problem is the following:

  - CockroachDB currently uses the logical schema part of a qualified
    name as a key to look up a database ID.
  - CockroachDB fails to recognize FQN relation, column and function names.

  Example failing queries, that should really work:

  - `select * from mydb.public.foo`        (invalid use of schema part)
  - `select mydb.public.kv.v from kv`      (insufficient FQN support)
  - `select mydb.pg_catalog.pg_typeof(1)`  (insufficient FQN support)

- the introspection tables are insufficiently populated for admin users.

  A client that connects (via pg connection string) to a database
  `curdb` but as admin user expects all the databases to be listed
  alongside each other as separate "catalog" entries in
  `information_schema` tables. Currently, CockroachDB will
  only show them the tables for `curdb`, not other databases.

- the introspection tables plainly violate their contract.

  1. A client will see a row (`def`, `curdb`, `tbl`) in there but
     the query `select * from def.curdb.tbl` is invalid.

  2. A client knowing that they are connected to database `curdb`
     (from their connection URL) cannot find the string
     "`curdb`" in the catalog column of the `information_schema` tables.

- meta-introspection (introspection of `information_schema` itself) is
  wrong when connected as "root": the virtual schemas must exist for
  every database, and currently they are only listed once.

These various problems compound and cause CockroachDB to confuse
most DB inspection tools.

## Detailed solution

The proposed change addresses the problem above as follows:

- the deviation in name resolution algorithms is resolved by changing
  the name resolution algorithms to match pg's.

  Backward compatibility with previous CockroachDB versions is ensured by a "catch"
  rule that uses the logical schema name as database name if the pg rules would otherwise
  determine the name was invalid.

- the limitations with introspection tables are addressed by
  populating the database descriptor name in the "catalog" column. The
  names of the virtual schemas are repeated for each database
  descriptor.

### Handling of view queries

(needs some convincing argument that the proposed algorithm addressed previously stored views adequately)

# Drawbacks

Why should we *not* do this? Will need some adjustment by existing
CockroachDB users.

Mitigating factors: the name resolution rules may be able to recognize
invalid schema names as catalog names for compatibility.

Consequences on other areas of CockroachDB: internal queries
ran by CockroachDB against itself should use the new naming rules.

# Rationale and Alternatives

- Why is this design the best in the space of possible designs?

  See the PG compatibility doc by Andy Woods.

- What other designs have been considered and what is the rationale for not choosing them?

  - See my previous RFC from last year, which proposes to introduce
    fully-fledged schemas (to support a 3-level hierarchy for table
    descriptors). This would provide even more PG compatibility but is
    left out of scope in this RFC to make the change more incremental.

  - A wild idea by Peter: make FQNs variable length. ("The SQL to KV
    mapping could be extended without too much difficulty to support
    an arbitrary number of levels") - this does not fit the
    restriction on name length forced on us by `information_schema`.

- What is the impact of not doing this?

  Broken compatibility with GUI database inspection tools.

# Unresolved questions

Handling of view queries (currently under investigation).
