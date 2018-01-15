- Feature Name: Virtual pg-like schemas
- Status: draft
- Start Date: 2018-01-15
- Authors: knz, Jordan
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

**Remember, you can submit a PR with your RFC before the text is
complete. Refer to the [README](README.md#rfc-process) for details.**

# Summary

This RFC proposes to introduce the notion of “schema” in the namespace
rules used by CockroachDB so that tools that use virtual tables to
introspect the schema find a similar layout as in PostgreSQL.

The change does *not* include structuring the data in KV in a new way
and in particular does not enable the use of multiple distinct schemas
side-by-side inside a single database.

This is required for compatibility with database inspection GUI tools.

To achieve this the RFC proposes to tweak the name resolution rules
and how the database introspection vtables (`pg_catalog.*`,
`information_schema.*`) are generated.

Experiment ongoing here:
https://github.com/knz/cockroach/tree/20180125-schema-cat2

# Motivation

In the world of PostgreSQL the data namespace has 4 levels: cluster,
catalog/database, schema, table. A single cluster can contain multiple
catalogs, one catalog can contain multiple schemas, the same table
name can designate separate tables in two schemas inside the same
database.

This corresponds more or less as follows in CockroachDB:

| Postgres concept | CockroachDB concept |
|------------------|---------------------|
| cluster          | cluster             |
| database/catalog | database            |
| schema           | (does not exist)    |
| table            | table               |

The distinction between cluster  and catalog is as follows:

- users are global to a cluster  and shared across all catalogs/databases in
  that cluster.  Different clusters  have different user information.
- Tables can cross-refer (via FKs) other tables from any catalog
  inside the same cluster, but not across clusters.

Clusters in pg are mostly invisible to database
inspection/introspection tools. They do not appear in `pg_catalog` /
`information_schema`.

The distinction between catalog and schema in pg is as follows:

- the catalog appears in the "catalog" column of `information_schema`
  vtables, the schema appears in the "schema" column.
- each new catalog has at least one physical schema called `public`
  (this name is not modifiable).
- the `pg_catalog` namespace is a *virtual schema* (not virtual
  catalog, not virtual database) in pg, and *it appears to exist in
  every catalog*. Ditto for `information_schema`.

A database inspection tool thus expects the following output when looking
at `information_schema.tables`, for example:

| Catalog    | Schema             | Table     |
|------------|--------------------|-----------|
| test       | public             | kv        |
| test       | public             | foo       |
| test       | pg_catalog         | pg_tables |
| test       | pg_catalog         | pg_types  |
| test       | pg_catalog         | ...       |
| test       | information_schema | tables    |
| test       | information_schema | columns   |
| test       | information_schema | ...       |
| myapp      | public             | orders    |
| myapp      | public             | customers |
| myapp      | public             | lineitems |
| myapp      | pg_catalog         | pg_tables |
| myapp      | pg_catalog         | pg_types  |
| myapp      | pg_catalog         | ...       |
| myapp      | information_schema | tables    |
| myapp      | information_schema | columns   |
| myapp      | information_schema | ...       |

In CockroachDB, in comparison, a tool would *currently* find the following data:

| Catalog    | Schema             | Table     |
|------------|--------------------|-----------|
| def        | test               | kv        |
| def        | test               | foo       |
| def        | myapp              | orders    |
| def        | myapp              | customers |
| def        | myapp              | lineitems |
| def        | pg_catalog         | pg_tables |
| def        | pg_catalog         | pg_types  |
| def        | pg_catalog         | ...       |
| def        | information_schema | tables    |
| def        | information_schema | columns   |
| def        | information_schema | ...       |

This is a problem because tools attempt to filter "everything
pertaining to an app" at the catalog level and cannot separate
concerns properly.

(Note for historical reference: `def` stands for "default" and is what
MySQL lists in the `information_schema` for any reference to
"Catalog". This is because in MySQL "Catalogs" and "Schemas" are both
equivalent and given the name "Database". The design of our object
hierarchy was heavily influenced by MySQL, which is why we made
similar choices along the way. -- Nathan)

# Guide-level explanation

## Concepts and vocabulary

With this change we must be careful of the terminology. This needs
adjustments in docs, explanations etc to better align with Postgres
concepts.

| Word, before   | What is being designated                                   | Word, after          | Visible to users? |
|----------------|------------------------------------------------------------|----------------------|-------------------|
| Database       | A KV prefix for multiple tables                            | KV Database          | No                |
| Database       | Namespace container from the perspective of SQL clients    | Catalog or Database  | Yes               |
| Schema         | The set of KV database and table/view/sequence descriptors | KV Schema            | No                |
| Schema         | A namespace container for virtual tables                   | (Virtual) Schema     | Yes               |
| (didn't exist) | A virtual namespace container for all tables in a catalog  | Schema               | Yes               |
| Table          | a KV prefix for row data                  | (KV) Table, still unambiguous for now | Yes               |

## How do we teach this?

### Teaching to new roachers

- a CockroachDB cluster contains multiple *catalogs*, or "databases". Every cluster
  starts with at least the `system` catalog. More catalogs can be
  created with `CREATE DATABASE` (we may want to support `CREATE CATALOG` for compatibility/clarity).

- each catalog contains one *physical schema* called `public`,
  and some additional *virtual schemas*, currently including `pg_catalog`, `information_schema` and `crdb_internal`.
  - a future version of CockroachDB may support multiple physical schemas per catalog besides `public`.
  - we should introduce support for the `CREATE SCHEMA` statement and have it report a meaningful error.

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

What does not change:

- The SQL to KV mapping does not change: there is still only a 2-level hierarchy for SQL data.
  - The SQL catalog name resolves to a KV database descriptor.
  - The SQL catalog logical prefix maps to a KV database prefix.
  - SQL tables map to KV as usual.

- The new `public` schema has currently no KV encoding. That's why
  there can only be one physical schema (and its name is forced to be
  "`public`") in every catalog.

To support the change the following are adapted:

- the name normalization logic in `sql/sem/tree` is modified to
  support up to 4 parts, not just 3. The following forms are to be
  recognized:

  - for tables: `table`, `schema.table`, `catalog.schema.table`
  - for columns: `col`, `table.col`, `schema.table.col`, `catalog.schema.table.col`
  - for functions: `fun`, `schema.fun`, `catalog.schema.fun`

  For compatibility with CockroachDB v1.x. a name of the form `foo.x`
  when the current database is `foo` will be understood as
  `foo.public.x`.

- same rules / compatibility for zone specifiers

- to keep the normalization rules simple, we change the semantics of SQL
  sessions **to always have a current database set**.

  - for sessions by remote clients, current database default to "`def`"
    - the database `def` is automatically created by a migration.

  - for internal sessions, current database defaults to "`default`"
    too -- however further experimentation is needed, perhaps a
    default of `system` is preferable here.

- what of cross-db queries?

  Example:
  - `select v from foo.kv`  when current db = `foo` -> (most common) no problem, compat
  - `select v from foo.kv`  when current db = `bar` -> error message, forces
    to use `select v from foo.public.kv`

  I suggest that this behavior is acceptable, as cross-db queries should be uncommon anyway.

- already stored views are rewritten by a migration:

    - 1-item table names could not occur in view queries anyways
    - 2-item table names change: `select v from foo.kv` -> `select v from foo.public.kv`
    - 3-item column names change: `select foo.kv.v from ...` -> `select foo.public.kv.v from ..`
    - 2-item column names *unchanged*: `select kv.v from ...`

- the `database` variable is to be used with `search_path` as follows:

  - for each name X in `search_path`, a fully qualified name is formed as follows:
    - if X is a simple name, the FQ name is `database || '.' || X`
    - if X is prefixed already, the FQ name is X.

  - tables are looked up in the schemas listed by `search_path`.

- the vtable generator functions in
  `sql/pg_catalog.go`. `sql/information_schema.go` and (perhaps)
  `sql/crdb_internal.go` are modified to list the KV database name in
  the "Catalog" column instead of "Schema". The virtual schemas remain
  in the "Schema" column but are repeated for every KV database
  (logical catalog).

  - These generator functions already accept a "db prefix" parameter
    to constraint the visibility they have over the KV schema. This is
    to be filled with `current_catalog`.

## Detailed design

See above.

## Drawbacks

Why should we *not* do this? Will need some adjustment by existing CockroachDB users.

Mitigating factors: the name resolution rules may be able to recognize
invalid schema names as catalog names for compatibility (only if
stricly needed)

Consequences on other areas of CockroachDB: None

Consequences on performance: name resolution actually becomes slightly
simpler, so there might be a (very slight) boost on logical planning.

## Rationale and Alternatives

- Why is this design the best in the space of possible designs?

  See the PG compatibility doc by Andy Woods.

- What other designs have been considered and what is the rationale for not choosing them?

  See my previous RFC from last year, which proposes to introduce
  fully-fledged schemas (to support a 3-level KV hierarchy). This
  would provide even more PG compatibility but is left out of scope in
  this RFC to make the change more incremental.

- What is the impact of not doing this?

  Broken compatibility with GUI database inspection tools.

## Unresolved questions

None known.
