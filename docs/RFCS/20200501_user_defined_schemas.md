- Feature Name: User-defined schemas
- Status: draft
- Start Date: 2020-05-01
- Authors: Raphael Poss, Andrew Werner, Rohan Yadav, Lucy Zhang
- RFC PR: #48276, previously #30916
- Cockroach Issue: #26443

# Summary

Schemas form a level of namespacing between tables and databases and currently
have limited support within CockroachDB. This RFC proposes support for
user-defined schemas: users will be able to create, alter, and drop arbitrary
schemas within a database.

This makes it possible to have two tables with the same name, say `orders`, in
the same database, by making them live in separate schemas: for example,
`mydb.testschema.orders` and `mydb.prodschema.orders`. This is useful for
compatibility and to ease hosting of multiple users/apps on a single cluster.

# Motivation

There are two motivations:

A. compatibility with PostgreSQL -- ORMs and client apps expect this to work.
B. simplify the management of multi-user or multi-app clusters.

The scenario for the point B goes as follows:

1. suppose hosting company HostedInc, with customer TheUser.
2. TheUser staff wants to deploy multiple apps over time but does not want to
   inform HostedInc continuously about this.
3. Meanwhile HostedInc doesn't want to provide root access to TheUser, because
   that would cause risks to the stability of the hosted cluster.
4. The question thus arises: how to enable TheUser to deploy multiple apps
   without providing root access? *Currently root access is required to create
   new databases.*

The proposed solution, which incidentally is both industry-standard
and already expected by users, goes as follows:

1. HostedInc creates a single database for TheUser.
2. HostedInc grants the "create schema" privilege to TheUser.
3. TheUser creates 1 schema per app in their database, and grants finer-tuned
   permissions on each schema for each app.
4. The app developers (or the apps themselves) create the tables/objects they
   need in their respective schemas.

# Guide-level explanation

CockroachDB already provides partial support for schemas. Schemas have entries
in the namespace table, all tables have a schema parent ID, and the name
resolution rules take schemas into account. All this is sufficient to support
the virtual schemas such as `pg_catalog`, the per-session `pg_temp` schemas for
temporary tables, and the only other schema, `public`.

What's new about user-defined schemas is supporting additional schemas that can
be created and modified. Syntax-wise, we'll start supporting `CREATE SCHEMA`,
`ALTER SCHEMA`, and `DROP SCHEMA`. The main problem involved in user-defined
schemas is how to perform efficient name resolution for arbitrary, changing
schema namespace entries.

As a starting point, this requires the ability to lease databases and schemas
the way tables are currently leased, which is also necessary for the ongoing
work on supporting enums and other user-defined types (see #47070). Leasing
enables transactionally renaming and dropping objects such that names are
correctly resolved for the duration of the change. With the introduction of
generalized leasing, we'll introduce a `SchemaDescriptor` to the
`sqlbase.Descriptor` union to store schema metadata. These schema descriptors
undergo schema changes similarly to online schema changes for tables.

A problem of name resolution specific to CockroachDB's behavior is that 2-part
names of the form `x.y`, are ambiguous, because `x` can refer to either a
database (which is non-standard) or a schema. We must first attempt to resolve
`x` as a schema name, and then as a database name. It's necessary to cache
enough information to minimize lookups both in the schema case and in the
database case, and caching a subset of the schemas in the database is
insufficient in general: if `x` were actually a database, resulting in a cache
miss, an attempted lookup for a schema named `x` would be required before
proceeding to trying to look up a database. Our solution is to store a snapshot
of the name-to-ID mapping for all schemas in the database on every database
descriptor, and use the cached version of this for schema name resolution. This
ensures that `x` is a schema if and only if it's present on the database
descriptor.

Many of the remaining open questions are around backward compatibility, and
about cross-database references in particular, which are allowed in Cockroach
but not in Postgres. The problem is basically how to transition users from a
world in which databases were like schemas to a world in which schemas are
fully supported.

# Reference-level explanation

## Detailed design

Technically the changes impact:

* `system.descriptor`: we need to start writing schema metadata to the
  descriptor table.
* Table leasing and caching: this needs to be generalized to cover databases
  and schemas (as well as enums, other user-defined types, and so on).
* The SQL/KV name resolution code (sqlbase/resolver.go and its dependencies):
  this needs to resolve schemas beyond `public`, the virtual schemas, and the
  `pg_temp` temporary schemas.
* Privileges:
    * Logic for GRANT/REVOKE: this must support privileges on schemas in
      addition to databases/tables.
    * Permission checking for CREATE (and perhaps SHOW): this must test the
      create privilege on the target schema, not database.
* Bulk I/O and related:
    * `cockroach dump`: this must be extended to support dumping a single
      schema inside a database. It must also be extended to properly qualify
      schema names inside the dump data.
    * BACKUP/RESTORE: this must be extended to support backing up / restoring a
      single schema, and naming schemas when listing target objects.
    * IMPORT CSV: this must be extended to recognize a schema name in the
      import target.
    * IMPORT PGDUMP: this must be extended to disable the "flattening" of the
      schema information contained in the dump to "public", and instead
      preserve/use the schema information in the dump.
    * EXPORT: probably no change needed, but QA must verify that users can use
      EXPORT for single schemas, or tables across different schemas.
* The web UI which presents the database objects in a cluster and the admin RPC
  endpoints that support these features in the web UI. This must reveal schemas
  between databases and tables. (TODO: what's the current support for virtual
  schemas and temp table schemas in the admin UI?)
* Zone configuration: propagation/inheritance from database to schema, then
  from schema to table.

### Schema descriptors and leasing

Physical schemas (i.e., `public` and the `pg_temp` schemas) have an ID in the
namespace table and can be resolved by name, like tables and databases. Unlike
tables and databases, schemas have no associated metadata stored in the
descriptor table. To enable lease-based caching and transactional updates
(create, rename, drop) with correct name resolution, we'll need to start
writing descriptors for schemas.

Currently, table descriptors have a table-specific leasing mechanism. Database
name-to-ID mappings have a per-node incoherent cache, and schema IDs have a
cache that is never cleared because the name-to-ID mapping is immutable for the
`public` and `pg_temp` schemas, which clearly won't be true in general. The
rest of the RFC assumes that we'll implement #48250 to extract leasing-related
metadata from the table descriptor to somewhere common to all
`sqlbase.Descriptor`s and generalize the existing table leasing system,
enabling every type of `Descriptor` to be leased.

First, we'll need to add a `SchemaDescriptor` to the `Descriptor` union:
```
message SchemaDescriptor {
  optional string name;
  optional uint32 id;
  optional uint32 parent_id;
  optional PrivilegeDescriptor privileges;
  optional []??? draining_names; // For renaming/dropping schemas
  optional ??? state; // Enum with PUBLIC and DROP values
}
```
This is similar to a `DatabaseDescriptor`, with some fields for schema changes
that will eventually be common across all objects that need to be
transactionally renamed and dropped. It's possible that we'll extract those
fields to the `Descriptor` itself or somewhere else.

Next, we need to generalize our leasing and caching capabilities (this list is
mostly not schema-specific, and overlaps heavily with #48250):

* Generalize the `LeaseManager` to handle leasing all descriptor types.
* Remove the old database and schema caches on the `TableCollection` and
  establish a consistent interface for getting leased and uncommitted objects.
* Update the `CachedSchemaAccessor` to start using the updated `LeaseManager`
  and `TableCollection`, and clean up and expand the `SchemaAccessor` interface
  to support returning schema descriptors and not just IDs.

Note that the schema-related groundwork that's been completed for temporary
tables, and the proposed extension of the work in this RFC, is Alternative B in
the original user-defined schemas RFC: #30916. Alternative C, which involves
storing all schema metadata on database descriptors, would also be possible in
our current state, and is discussed later in the context of caching
and name resolution.

### Schema changes on schemas

There are 4 categories of schema changes on schemas we have to consider:

* Creating a schema (`CREATE SCHEMA`)
* Renaming a schema (`ALTER SCHEMA ... RENAME TO ...`)
* Changing the owner of a schema (`ALTER SCHEMA ... OWNER TO ...`)
* Dropping a schema (`DROP SCHEMA ... [CASCADE]`, or as a result of `DROP
  DATABASE CASCADE`)

Changing the schema owner simply requires a write to the schema descriptor, as
for tables. The other changes (creating, renaming, dropping) additionally
require updates to the namespace table. When renaming or dropping a schema,
we'll need to use the same two-transaction protocol as we do for tables, where
we wait for old leases to drain before proceeding to the second transaction to
clear the old namespace entry. (Enums and databases will need the same
treatment.) To review:

* Create schema `a` with ID 51:
    * Insert new descriptor, insert namespace entry with `(name, id) = (a, 51)`
* Rename schema 51 from `a` to `b`:
    * 1st transaction: Update schema descriptor `name` to `b`, insert namespace
      entry for `(b, 51)`, add `a` to draining names list
    * 2nd transaction: Delete namespace entry for `(a, 51)`, delete `a` from
      draining names list
* Drop schema `a` with ID 51:
    * 1st transaction: Update schema state to `DROP`
    * 2nd transaction: Delete namespace entry for `(a, 51)`, delete descriptor

`DROP SCHEMA ... CASCADE` may drop tables or user-defined types, and `DROP
DATABASE ...  CASCADE` may drop schemas. The implementation of these statements
will be similar to how `DROP DATABASE ... CASCADE` currently acts on tables.

The actual schema changes will be implemented as jobs, as table descriptor
schema changes are. There's a question of how much of the existing schema
changer implementation we should extract and generalize, which we'll also want
to consider in the context of enums/user-defined types.

### Name resolution and leasing schema names

As described above, when resolving names of the form `x.y`, we face the problem
of caching enough information to minimize lookups both when `x` is a schema and
when it is a database. At the time of writing, to solve this problem for a
restricted set of possible schemas, we have a cache for schema name-to-ID
lookups on the `TableCollection`, and a workaround for the negative case where
we avoid attempting a schema lookup if the potential schema name is not
`public` and does not begin with `pg_temp`.

Our solution is to store a complete schema name-to-ID map on every database
descriptor along with each schema's `PUBLIC`/`DROP` state, updated whenever a
schema in the database is added, renamed, or dropped. This snapshot must be
updated in the same transaction whenever a namespace entry is updated or a
schema transitions to the `DROP` state, as detailed above. (Note that each
update corresponds to a new database descriptor version.)

Then when a schema is leased, we will always acquire a lease on the database if
we don't have one already, and we'll always use the namespace mapping on the
leased database descriptor for cached name resolution. (TODO: Would we do this
even when resolving a 3-part name, or would we just look at the cached schemas
directly? The only reason why this might matter is that it's possible for our a
leased database and one of its schemas to be offset by one "version" in the
cache when both the database and schema versions were bumped in the same
transaction. I think this is fine for correctness, but maybe for the sake of a
consistent experience it's always better to use the database namespace
mapping.)

This means that, at this point, the only data stored on a schema descriptor
that isn't duplicated on the database descriptor would be the privileges. An
alternative approach considered was to store all schema metadata on database
descriptors, without giving them individually leasable descriptors. A privilege
descriptor, however, is comparable in size to all the other fields combined on
the schema descriptor, so storing the privileges only on the individual schema
descriptors will enable support for significantly more schemas in a single
database. Furthermore, the ability to lease individual schemas doesn't
represent much of an increase in complexity because the new leasing system is
meant to be general.

One downside of this approach (as well as of hypothetically storing all
metadata on the database descriptor) is that the rate at which schema changes
can be performed on schemas within the same database would be limited by how
quickly schema changes can happen in succession for a single database
descriptor. This would probably mainly affect users' migrations that run schema
changes in quick succession when starting up, as well as ORM tests, especially
if multiple changes to different schemas in the same database happen
concurrently, leading to retries when publishing new database versions. This is
an inherent limitation of using the leasing system for cache invalidation for a
single database-wide cache.

### The `public` schema, cross-database references, and backward compatibility

Our current implementation of schemas has areas of incompatibility with
Postgres and requires special handling for backward compatibility in this
proposal:

* The `public` schema has an "ID" of 29 and is the `parentSchemaID` for all new
  tables, but this shared ID can never correspond to an actual schema with a
  descriptor for any specific database. In Postgres, the `public` schema is a
  schema like any other.
* We allow cross-database references for foreign keys, views, etc., whereas
  Postgres only allows cross-schema references within the same database.
* More broadly speaking, our databases work similarly to how schemas work in
  Postgres: They're the immediate "parent" of tables, and we allow
  cross-database references.

An open question: When user-defined schemas are created, what should their
behavior be with respect to cross-database references? If we disallow them, how
do we handle the existing `public` schema (and its ID), which contains tables
that may have cross-database references? Some options:

* Support cross-database references. This would be more compatible with our
  current behavior, at least superficially, but it would also be more complex
  to implement (e.g., in name resolution), there's much less of a benefit to
  allowing cross-database references from a feature point of view once we also
  fully support schemas, and users may misuse the "feature" since it violates
  Postgres-based expectations of what a database is.
* Disallow cross-database references, but keep `public` as the one schema in
  each database that does allow them. This would have the benefit of preserving
  existing behavior while avoiding the complexity of dealing with all schemas
  in all databases.
* Disallow cross-database references and eventually deprecate support for them.

Somewhat independently of the above options, there's also the question of what
to do about the existing implementation of the `public` schema. Some options:

* Keep the `public` schema as it is, with a pseudo-ID and no actual descriptor,
  and basically identify it with the database itself. New tables created in the
  `public` schema would behave the same way as tables created before the
  introduction of user-defined schemas. Renaming or dropping the schema would
  be impossible. It's not clear what setting zone configs or privileges should
  do.
* Migrate the `public` schema to an actual schema with a per-database ID and a
  descriptor. In the world where we deprecated cross-database references,
  `public` would become a schema like any other. In the world where we allowed
  cross-database references only in `public`, it would have special rules.

Regardless of which options we choose, we will want to expose a way for users
to convert their databases into schemas. A database would have to have no
user-defined schemas to be eligible to be converted. (TODO: Can we do this
without rewriting the tables?)

The `pg_temp` schemas also exist without schema descriptors. Since they cannot
be renamed or dropped or have their privileges changed, it would not be useful
to make them leasable, and they can remain as descriptor-less schemas
indefinitely.

## Drawbacks

* Storing (and updating) the name-to-ID mapping of all child schemas in every
  database descriptor becomes memory-intensive when there are many schemas.
  There's also the problem described above of concurrent schema changes on
  different schemas in the same database producing contention on the database
  descriptor. The second problem is unavoidable if we use the database
  descriptor to store a consistent view of all the schemas. We'll need to do
  some testing to see how we handle lots of schemas and lots of concurrent
  schema changes.
* Adding more types of descriptors to `system.descriptor` will make full-table
  scans worse on that table. This was brought up in the context of enums, and
  the decision was that (somehow) building an index would be better than trying
  to split enums off in their own table; the same thing applies here.

## Rationale and Alternatives

(TODO, coming soon)

## Unresolved questions

* What to do about backward compatibility for cross-database references and
  about the existing `public` schema? These issues are detailed above.
* How much of the existing schema change infrastructure for tables should we
  try to extract and make more general, at least from the outset? (e.g.,
  `MutableTableDescriptor`, the `SchemaChanger` or specific parts of it, etc.)
  It seems tempting to do this for renaming and dropping objects, since the
  interactions with `system.namespace` across all object types are very
  similar. This is a question for enums as well as schemas and databases.
