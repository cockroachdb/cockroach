- Feature Name: Information Schema
- Status: completed
- Start Date: 2016-07-19
- Authors: Nathan VanBenschoten
- RFC PR: [#7965](https://github.com/cockroachdb/cockroach/pull/7965)
- Cockroach Issue: [#5194](https://github.com/cockroachdb/cockroach/issues/5194),
                   [#5583](https://github.com/cockroachdb/cockroach/issues/5583)


# Summary

`information_schema` allows users to perform database introspection, providing
information about all tables, views, columns, procedures, and access privileges
in a database. This proposal details initial plans to support the `information_schema`
specification in CockroachDB.

# Motivation

Up until this point, CockroachDB has only supported limited schema reflection through
`SHOW` statements. This is restricting in that it is inconsistent with other SQL
statements, it forces users to learn new syntax, and it is inextensible. Unlike `SHOW`
statements, `information_schema` exposes a table-like interface which feels natural in
relational database and addresses all of these concerns. For other advantages of
querying `information_schema` over using `SHOW` statements, see MySQL's
[documentation on the topic.](http://dev.mysql.com/doc/refman/5.7/en/information-schema.html#idm140648692050944)

More importantly, in order to perform schema reflection, a large majority of ORMs
and other database tools query metadata tables within the `information_schema`. Without
proper support for these meta table, many tools simply cannot be used with CockroachDB.
This is an issue, as support for these tools is critical for user accessibility
of CockroachDB. Because of this widespread usage of `information_schema`, adding support
for the standard will be a major step towards supporting a group of ORMs and other tools,
and by extension, expanding the ecosystem around CockroachDB.

# Detailed design

### Relation to VIEWs

`information_schema` tables are commonly referred to as VIEWs. This is because they are
not base tables, so there are no files associated with them. On top of this, they are read-only
tables, so one can only read their contents, and cannot perform `INSERT`, `UPDATE`, or `DELETE`
operations on them.

However, while `information_schema` tables are conceptually similar to read-only views, they
are handled differently than standard user-level VIEWs in most databases. For instance, in MySQL
`information_schema` tables are **temporary tables that are created and filled on demand**.
The reason for this is that a `VIEW` is a mapping from an ordinary base table to a virtual table.
However, the data stored in the `information_schema` is not stored in any underlying table,
so the "tables" need to supply data themselves. All this goes to say that even though future
implementation efforts towards adding support for VIEWs to CockroachDB may touch
`information_schema` code, the features can and should be developed independently from each other.

### General Implementation Concerns

There are two general implementation concerns that need to be considered for the
introduction of `information_schema` into CockroachDB. The first concern is that the
`information_schema` database and the tables it contains should **behave** exactly
like all other databases and tables, respectively. Regardless of how they are implemented,
they should be indistinguishable from persisted relations. The second concern is that the
tables need to **produce** the correct information when queried by performing a form
of database introspection.

### Virtual Database and Table Descriptors

To address the first implementation concern, this RFC proposes that we hardcode a set of
static database and table descriptors for the new database and all new tables needed
for the `information_schema`. This will be similar to the approach taken for `SystemDatabase`,
except there is no need to actually store the descriptors in an underlying Cockroach KV store
during cluster bootstrap, because they will not need to be modified. These descriptors
will all be given **read-only privileges**, just like the majority of system tables.

Interactions with these "virtual" database and table descriptors will be handled by
`sql.planner`'s implementation of
[`DatabaseAccessor`](https://github.com/cockroachdb/cockroach/blob/4d45696ea776a5b912262e5eef9889eacb6abe41/sql/database.go#L90)
and
[`SchemaAccessor`](https://github.com/cockroachdb/cockroach/blob/4d45696ea776a5b912262e5eef9889eacb6abe41/sql/table.go#L89),
respectively. By handling these descriptors at this level, we can avoid having to add
special-cases for `information_schema` constructs at abstraction levels above this.
Desired behavior which this would naturally provide is:

- Preventing the creation of databases called "information_schema"
- Preventing the creation of tables in the "information_schema" (read-only database privileges)
- Preventing the mutation of tables in the "information_schema" (read-only table privileges)

### Virtual Data Sources

To address the second implementation concern, the RFC proposes that we catch queries to
these virtual descriptors in
[`planner.getDataSource`](https://github.com/cockroachdb/cockroach/blob/4d45696ea776a5b912262e5eef9889eacb6abe41/sql/data_source.go#L191),
and return a `valuesNode` instead of a `scanNode` with desired information populated. Using
a `valueNode` in this way draws direct parallels to our current implementation of `SHOW`
statements. It allows us to mock out the table scan, and populate the provided values using
arbitrary code.

### pg_catalog

`pg_catalog` is a PostgreSQL extension database that predates the introduction of
`information_schema` to the SQL standard. PostgreSQL maps all `information_schema` "tables" to
`pg_catalog` through the use of VIEWS. While it would be ideal to only support
`information_schema`, as it is part of the SQL standard and present in most databases (as opposed
to just PostgreSQL), there does seem to be a demand for an implementation of `pg_catalog` as well.
This demand comes from:

- PostgreSQL-specific tools and ORMs, which do not make an effort to be cross platform compatible
- PostgreSQL-specific drivers, which do not make an effort to be cross platform compatible
- Legacy cross-platform tools and ORMs, which special-case the use of `pg_catalog` for
  any database exposing the PostgreSQL wire protocol

The RFC proposes to map all `pg_catalog` tables to queries on the `information_schema`, in
a similar way that PostgreSQL does (but in the opposite direction). Ideally this would be
completed with VIEWs, but there are a few downsides to this. These include that the eventual
implementation of VIEWs will require `ViewDescriptors` to be distributed, which is unnecessary
for a static VIEW. Instead, it would be simpler to map `pg_catalog` queries directly to
`information_schema` queries in code. However, this RFC also proposes that we push off a more
detailed discussion on `pg_catalog` until the implementation of `information_schema` is complete.

### Standards Considerations

It is advised that we take a similar approach to MySQL in its representation of
`information_schema`. The `information_schema` table structure will follow the ANSI/ISO
SQL:2003 standard Part 11 Schemata. The intent is approximate compliance with SQL:2003 core
feature F021 Basic information schema.

Additionally, MySQL provides a number of extensions to their implementation of the
`information_schema`. An example of this is the inclusion of an `ENGINE` column in their
`INFORMATION_SCHEMA.TABLES` table. The database also excludes columns that do not
make sense to include given their SQL implementation. Similarly, we will omit columns that
are not relevant for our implementation, and add CockroachDB-specific extension columns
if any specific need for them arises.

CockroachDB-specific information which makes sense to add to our `information_schema`
implementation includes:
- column families
- interleaved tables
- index stored columns

### Privileges

All users will have access to the `information_schema` tables, but they will only be able
to see rows which they have access to. The same privileges will apply to selecting information
from `information_schema` and viewing the same information through `SHOW` statements. This will
be a change from our current implementation of `SHOW` statements, where we generally return all
instances of a given object, even those that a user does not have privileges for.

This approach to access control for the `information_schema` is identical to MySQL's.

### Other Concerns

#### SHOW Statements

Our inclusion of `SHOW` statements was inspired by MySQL. In MySQL, `SHOW` statements
were added as the primary means of schema reflection before `information_schema` was
introduced to the SQL standard. Shortly after it was introduced, MySQL added support for the
meta tables, and began mapping `SHOW` statements directly onto `SELECT` statements from
these virtual tables.

This RFC proposes that sometime after support for `information_schema` is added, we
follow MySQL's lead and turn `SHOW` statements into a wrapper around `information_schema`
queries. This will help eliminate duplicate implementations and simplify SHOW statement
handling significantly.

#### Schema Search Path Extension (only needed for `pg_catalog` support)

PostgreSQL has a notion of a "Schema Search Path". In much the same way that we support the
`SET database = xyz`, which will search for unqualified table names in the `xyz` database, the
schema search path allows unqualified table names to be associated with a specific
database/table pair. However, unlike our session database setting, PostgreSQL allows multiple
databases to be in the search path. This is important because a large percent of the the time
that `pg_catalog` tables are referenced in ORMs and other external tools, they are
referenced using unqualified table names. For instance, a number of the ORMs use
`pg_catalog` like:

```sql
SELECT * from pg_class
```

instead of

```sql
SELECT * from pg_catalog.pg_class
```

The reason this is allowed is because PostgreSQL includes `pg_catalog` in its
database search path by default. In order to support this use case, we should extend our
notion of a single database search path to a set of search paths. Like support for
`pg_catalog`, this does not need to be completed during the initial implementation of
this RFC.

# Drawbacks

- Complicates some SQL descriptor logic
- Complicates database introspection on itself (ie. `SHOW TABLES FROM information_schema`)
- Adds a few special cases to the query engine, but this is much less intrusive than
  if we tried to mock out the database and table existence at a higher level than
  descriptors

# Alternatives

- Create and maintain real tables just like we do with SystemDB tables
- Mock out database and table existence at a higher level than [virtual
  descriptors](#virtual-database-and-table-descriptors)
- Begin building out VIEW infrastructure, and treat `information_schema` like
  a read-only view. As noted in [Relation to VIEWs](#relation-to-views), this
  wouldn't actually change very much, and would still require a lot of
  this work. For instance, we would still need to somehow mock out the existence
  of the `information_schema` database itself, and would still need to have the
  VIEWs source their own introspective data.

# Unresolved questions

### Information_Schema Across Different Versions of CockroachDB

Because this proposal suggests the use of static virtual descriptors instead of real
descriptors persisted to an underlying CockroachDB store, the descriptors used for a
given `information_schema` query will depend on the version of the CockroachDB instance
fielding a SQL request. In a mixed-version cluster, this could result in two nodes returning
`information_schema` tables with different schemas if the static schema was altered between
the nodes' versions. Still, both nodes will issue replicated transactions internally to
populate these tables, so both will return "correct" information.

Inconsistencies where different nodes return different results for a given query can occur
today if two nodes are running different versions of our SQL query engine. The only difference
is that this change will introduce the possibility of schema inconsistencies because
`information_schema` descriptors may not be consistent between nodes. This shouldn't be a
serious issue, but it is something to note.
