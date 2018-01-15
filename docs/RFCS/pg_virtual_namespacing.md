- Feature Name: Virtual pg-like schemas
- Status: draft
- Start Date: 2018-01-15
- Authors: knz, Jordan
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

**Remember, you can submit a PR with your RFC before the text is
complete. Refer to the [README](README.md#rfc-process) for details.**

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
does not enable the use of multiple distinct schemas side-by-side
inside a single database, i.e. the ability to have two tables with the
same name in the same database (in different schemas): having both
`mydb.foo.tbl1` and `mydb.bar.tbl1` side-by-side will still not be
supported.

To achieve this, the RFC proposes to tweak the name resolution rules
and how the database introspection virtual tables (`pg_catalog.*`,
`information_schema.*`) are generated.

Code experiment ongoing here:
https://github.com/knz/cockroach/tree/20180125-schema-cat2

As of this writing (2018-01-27) the experiment is not concluded yet.

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
adjustments in docs, explanations etc to better align with Postgres
concepts.

| Word, before   | What is being designated                                   | Word, after            | Visible to users? |
|----------------|------------------------------------------------------------|------------------------|-------------------|
| Database       | The name for a database descriptor in KV                   | DB descriptor name     | Mostly not        |
| Database       | Namespace container from the perspective of SQL clients    | Catalog or Database    | Yes               |
| Schema         | The conceptual set of all db/table/view/seq descriptors    | Physical schema        | Mostly not        |
| Schema         | A namespace container for virtual tables                   | Logical schema         | Yes               |
| (didn't exist) | Namespace container for all tables in a catalog            | Logical schema         | Yes               |
| Table          | The name for a table/view/sequence descriptor in KV        | Object descriptor name | Mostly not        |
| Table          | The name for a table where a SQL client can store stuff    | Table or Relation      | Yes               |

## How do we teach this?

### Teaching to new roachers

- a CockroachDB cluster contains multiple *catalogs*, or
  "databases". Every cluster starts with at least the `system`
  catalog. More catalogs can be created with `CREATE DATABASE` (we may
  want to support `CREATE CATALOG` for compatibility/clarity).

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

- the session variable `database` designates the current
  catalog, which is used in queries to resolve
  (table/view/sequence/schema) names when no catalog is further
  specified.

  - the `USE` statement, provided for compatibility with MySQL,
    adjusts the `database` session variable.

- the session variable `search_path` contains a list of schema names
  inside the current catalog where to search for functions and tables named in
  queries.

  For example, with a `search_path` set to `public, pg_catalog`, a
  `database` set to `myapp2` and given a query `select * from
  kv`, CockroachDB will search for table `kv` first in the `public`
  schema of catalog `myapp2`, then in the `pg_catalog` schema for
  catalog `myapp2`.

  - As a specific CockroachDB extension, schemas in `search_path` can
    be prefixed with a catalog name.

    For example, with `search_path = myapp2.public, myapp1.public,
    pg_catalog`, given a query `select * from kv`, CockroachDB will
    search for table `kv` first in the `public` schema of catalog
    `myapp2`, then in the `public` schema of catalog `myapp1`, then in
    the `pg_catalog` schema for the catalog currently set in
    `database`.

### Teaching to existing roachers

- We'll adopt the word "catalog" as a synonym for "database" to
  designate the visible portion of the KV container for tables.  The
  word "schema" should be used more sparingly, as it has a specific
  meaning in PostgreSQL which CockroachDB does not yet support.

  - Except for what was called "virtual schema" in CockroachDB; these
    were already properly named after the equivalent PostgreSQL
    concept and do not change.

- various statements with a `DATABASE` keyword are changed to also
  support the keyword `CATALOG`.

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
  to the current value of `database`. It is possible to cover multiple
  catalogs by specifying a catalog prefix in some `search_path`
  entries.

Note:

- we also support the syntax `CREATE CATALOG`, `DROP CATALOG`, `RENAME
  CATALOG` as (non-documented) aliases for `CREATE DATABASE`, `DROP
  DATABASE`, `RENAME DATABASE` statements for compatibility with
  Postgres.

# Reference-level explanation

- the name normalization logic in `sql/sem/tree` is modified to
  support up to 4 parts, not just 3. The following forms are to be
  recognized:

  - for tables: `table`, `schema.table`, `catalog.schema.table`
  - for functions: `fun`, `schema.fun`, `catalog.schema.fun`
  - for columns: `col`, `table.col`, `schema.table.col`, `catalog.schema.table.col`

  For compatibility with CockroachDB v1.x. a name of the form `foo.x`
  when the current database is `foo` will be understood as
  `foo.public.x`.

  This is done as follows:

  - the `database` variable is to be used to transform
    partially-qualified names into fully qualified names (FQNs) as
    follows: given a partially qualified object name N of the form
    `S.T`
  
    - if S is equal to the current value of `database`, the FQN becomes: `S || '.public.' || T`
    - otherwise, the FQN is formed with: `database || '.' || N`
  
  - the `database` variable is to be used together with `search_path` to
    transform unqualified names into FQNs as follows:
  
    - given an unqualified base object name "T", for each name X in
      `search_path`, the FQN is formed as follows:
    
      - if X is a simple name, the FQN is: `database || '.' || X || '.' || T`.
      - if X is prefixed already, the FQN is: `X || '.' || T`.
  
    - which of the possible qualifications is retained is defined as follows:
    
      - if an existing object is wanted, the FQN that is retained is
        the first one that yields an valid descriptor when looked up.
      - if a new object is being created (or the name is used as new
        name during a rename), the FQN that is retained is the
        first one that uses a valid schema.

- same rules / compatibility for zone specifiers and table patterns
  for GRANT.

- the vtable generator functions in
  `sql/pg_catalog.go`. `sql/information_schema.go` and (perhaps)
  `sql/crdb_internal.go` are modified to list the database descriptor
  name in the "Catalog" column instead of "Schema". The virtual
  schemas remain in the "Schema" column but are repeated for every
  database descriptor (logical catalog).

  - These generator functions already accept a "db prefix" parameter
    to constraint the visibility they have over the physical
    schema. This is to be filled with the current value of `database`.

- already stored views need special handling, see [the specific
  section](#handling-of-view-queries)

- to keep the normalization rules simple while retaining compatibility
  with previous CockroachDB versions, we change the semantics of SQL
  sessions **to always have a current database set**.

  - for sessions by remote clients, current database default to "`def`"
    - the database `def` is automatically created by a migration.

  - if a client issues `CREATE DATABASE X` and the current value of
    `database` is `def`, then the `database` variable is modified
    automatically to become X. This ensures that clients can issue
    subsequent CREATE statements without any name resolution errors.

  - for internal sessions, the `database` var defaults to "`def`" too
    -- however further experimentation is needed, perhaps a default of
    empty string is preferable here.

- what of cross-db queries?

  Example:
  - `select v from foo.kv`  when current db = `foo` -> (most common) no problem, compat
  - `select v from foo.kv`  when current db = `bar` -> error message, forces
    to use `select v from foo.public.kv`

  I suggest that this behavior is acceptable, as cross-db queries should be uncommon anyway.

- the `CREATE SCHEMA` statement is supported syntactically and reports
  an error that suggests that stored logical schemas are not (yet?)
  supported.

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

Three separate name resolution algorithms apply to the different
syntactic constructs for names in SQL:

1. **resolution of persistent objects:**
   - the naming of tables / views in FROM clauses.
   - the naming of *functions* in both scalar contexts and FROM clauses.
   - the expansion of table patterns in GRANT.
   - the naming of in-db objects in CREATE, DROP, ALTER RENAME, etc.

2. **resolution of column references:**
   - a column reference is always composed of an optional persisent object name as prefix, followed by
     a mandatory column identifier.

3. **resolution of sub-parts inside a complex value**, when the engine supports
   sub-parts (e.g. arrays and/or compound types):
   - the naming of columns in the INSERT/UPSERT target column list, or the LHS of UPDATE SET statements.
   - scalar expressions of the form `<expr>[123][456]` or `(<expr>).path[1].to.field[2][3]`

For example, in the following queries:

     INSERT INTO a.b.c (d.e.f) VALUES (1)
                 ^^^^    ^^^^- this uses the resolution rule 3 of sub-parts inside column 'd'
                    |
                    \--------- this uses the resolution rule 1 of a persistent object

     SELECT (a.b.c.d).e.f FROM a.b.c
             ^^^^^^^ ^^^^      ^^^^^ this uses the resolution rule 1 of a persistent object
               |       |
               |       \------------ this uses the resolution rule 3 of sub-parts inside column 'd'
               |
               \-------------------- this uses the resolution rule 2 for a column, and thus
                                     the rule 1 for a persisent object for the prefix `a.b.c`.

    SELECT a.b.c(123)  -- this is a SQL function application
           ^^^^^- this uses the resolution rule 1 of a persistent object.

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
    schema named in the `search_path` session variable, in turn, until
    one is found that either:
    - exists, for operations that create a new object with that name;
    - contains the persistent object named by the last
      component, for operations that require the object to exist already.
    
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

  Backward compatibility with previous CockroachDB versions is ensured by:

  - a special rule when the logical schema name matches the current value of `database`
  - no surprise with undefined/empty `database` by forcing it to always be defined
    - creating a default db on-the-fly to achieve this, if needed
  - a cluster migration that rewrites view queries already stored

  (Note: an alternative design proposed by Peter is mentioned below -
  not yet researched though.)

- the limitations with introspection tables are addressed by
  populating the database descriptor name in the "catalog" column. The
  names of the virtual schemas are repeated for each database
  descriptor.

### Handling of view queries

XXX - this needs to be researched further

Views are rewritten by a migration:

- 1-item table names could not occur in view queries anyways
- 2-item table names change: `select v from foo.kv` -> `select v from [123 as kv]`
- 3-item column names change: `select foo.kv.v from ...` -> `select foo.public.kv.v from ..`
- 2-item column names *unchanged*: `select kv.v from ...`
- function names in view queries are left unchanged.

### Alternative design - proposed by Peter

> I'm wondering if we can allow tables to exist (for name lookup
> purposes) both directly within a catalog/database and also within a
> schema.

> I think this can be accomplished with a tweak to the name lookup
> rules.

> Similar to Postgres, `CREATE TABLE foo` would be shorthand for `CREATE
> TABLE public.foo`.

- knz: this doesn't need special treatment, it's
  already implied by [the rules for search_path](#partially-qualified-names)

> `SHOW TABLES FROM database` would be shorthand for `SHOW TABLES FROM
> database.public`.

- knz: this doesn't need special treatment, it's
  already implied by [the rules for search_path](#partially-qualified-names)

> When we encounter a name `x.y`, we would have to check to see if `x`
> is a schema name within the current database

- knz: or one of the virtual schema names

> and thus translate that to `<current
> database>.x.y` or, if that fails, `x` is a database name in which case
> we would translate it to `x.public.y`.

> A table name like `x.y.z` would be interpreted as
> `<database>.<schema>.<table>`.

> The downside is additional name lookup complexity, but I think that
> is fairly contained.

> Another downside is mild confusion with tables named
> `public`.

- knz: or `information_schema`, `pg_catalog` or `crdb_internal` (or, in general,
  any table that has the same name as one of the virtual schemas)

> Consider a table named `public` in the database `foo`,
> schema `public` (i.e. `foo.public.public`). If the user specifies
> `foo.public`, we'll translate that to `foo.public.public` which
> might not be what they intended (perhaps the query was actually an
> error). I don't think this is a significant problem, but perhaps
> there are similar ones lurking around.

> The upside to this approach is no need to migrate existing views, no
> need to force sessions to always have a database, and we're backward
> compatible with existing apps.

# Drawbacks

Why should we *not* do this? Will need some adjustment by existing
CockroachDB users.

Mitigating factors: the name resolution rules may be able to recognize
invalid schema names as catalog names for compatibility.

Consequences on other areas of CockroachDB: internal queries
ran by CockroachDB against itself should use the new naming rules.

Consequences on performance: name resolution actually becomes slightly
simpler, so there might be a (very slight) boost on logical planning.

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
