- Feature Name: User-defined schemas
- Status: in progress
- Start Date: 2020-05-01
- Authors: Raphael Poss, Andrew Werner, Rohan Yadav, Lucy Zhang
- RFC PR: [#48276](https://github.com/cockroachdb/cockroach/pull/48276),
  previously [#30916](https://github.com/cockroachdb/cockroach/pull/30916)
- Cockroach Issue:
  [#26443](https://github.com/cockroachdb/cockroach/pull/26443)

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

1. Compatibility with PostgreSQL -- ORMs and client apps expect this to work.
2. Simplify the management of multi-user or multi-app clusters.

The scenario for the second point goes as follows:

* Suppose hosting company HostedInc, with customer TheUser.
* TheUser staff wants to deploy multiple apps over time but does not want to
  inform HostedInc continuously about this.
* Meanwhile HostedInc doesn't want to provide root access to TheUser, because
  that would cause risks to the stability of the hosted cluster.
* The question thus arises: how to enable TheUser to deploy multiple apps
   without providing root access? Currently root access is required to create
   new databases.

The proposed solution, which incidentally is both industry-standard
and already expected by users, goes as follows:

* HostedInc creates a single database for TheUser.
* HostedInc grants the "create schema" privilege to TheUser.
* TheUser creates 1 schema per app in their database, and grants finer-tuned
  permissions on each schema for each app.
* The app developers (or the apps themselves) create the tables/objects they
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
work on supporting enums and other user-defined types (see
[#47040](https://github.com/cockroachdb/cockroach/pull/47070)). Leasing
enables transactionally renaming and dropping objects such that names are
correctly resolved for the duration of the change. With the introduction of
generalized leasing (see
[#48250](https://github.com/cockroachdb/cockroach/pull/48250)), we'll
introduce a `SchemaDescriptor` to the `sqlbase.Descriptor` union to store
schema metadata. These schema descriptors undergo schema changes similarly to
online schema changes for tables.

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
  and schemas (as well as user-defined types).
* The SQL/KV name resolution code (`sqlbase/resolver.go` and its dependencies):
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

Additionally, we need to verify that our existing schema-related functionality,
including `search_path` and the metadata virtual tables `pg_catalog.namespace`
and `information_schema.schemata`, continue to work.

### Schema descriptors and leasing

Physical schemas (i.e., `public` and the `pg_temp` schemas) have an ID in the
namespace table and can be resolved by name, like tables and databases. Unlike
tables and databases, schemas currently have no associated metadata stored in
the descriptor table. To enable lease-based caching and transactional updates
(create, rename, drop) with correct name resolution, we'll need to start
writing descriptors for schemas.

Currently, table descriptors have a table-specific leasing mechanism. Database
name-to-ID mappings have a per-node incoherent cache, and schema IDs have a
cache that is never cleared because the name-to-ID mapping is immutable for the
`public` and `pg_temp` schemas, which clearly won't be true in general. The
rest of the RFC assumes that we'll implement
[#48250](https://github.com/cockroachdb/cockroach/pull/48250) to extract
leasing-related metadata from the table descriptor to somewhere common to all
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
mostly not schema-specific, and overlaps heavily with
[#48250](https://github.com/cockroachdb/cockroach/pull/48250)):

* Generalize the `LeaseManager` to handle leasing all descriptor types.
* Remove the old database and schema caches on the `TableCollection` and
  establish a consistent interface for getting leased and uncommitted objects.
* Update the `CachedSchemaAccessor` to start using the updated `LeaseManager`
  and `TableCollection`, and clean up and expand the `SchemaAccessor` interface
  to support returning schema descriptors and not just IDs.

Note that the schema-related groundwork that's been completed for temporary
tables, and the proposed extension of the work in this RFC, is Alternative B in
the original user-defined schemas RFC
([#30916](https://github.com/cockroachdb/cockroach/pull/30916)). Alternative
C, which involves storing all schema metadata on database descriptors, would
also be possible in our current state, and is discussed later in the context of
caching and name resolution.

Additionally, even though schema IDs existed prior to this RFC, we'll want to
update `pg_catalog.pg_namespace` to use the schema descriptor's ID as the OID,
instead of using a hash of the database ID and the schema name. This is
consistent with the use of table IDs as OIDs in `pg_class`, and makes the OID
stable across renames, which is required for Postgres compatibility.

### Schema changes on schemas

These are the categories of schema changes on schemas to consider:

1. Schema changes requiring updates to both the schema descriptor and namespace
   table:
    * Creating a schema (`CREATE SCHEMA`)
    * Renaming a schema (`ALTER SCHEMA ... RENAME TO ...`)
    * Dropping a schema (`DROP SCHEMA ... [CASCADE]`, or as a result of `DROP
      DATABASE CASCADE`)
2. Schema changes requiring updates only to the schema descriptor:
    * Updating privileges (`ALTER SCHEMA ... OWNER TO ...`, `GRANT`/`REVOKE`)
3. Schema changes that do not involve descriptor updates:
    * Zone config updates (`ALTER SCHEMA ... CONFIGURE ZONE USING ...`)
    * Updating comments (`COMMENT ON SCHEMA ...`)

Schema changes in categories (2) and (3) are relatively straightforward and
will be implemented like their equivalents for tables. Schema changes in
category (1) are more complex, and will be revisited in the later section about
caching and name resolution, but their implementations will also be essentially
the same as for tables. In particular, when renaming or dropping a schema,
we'll need to use the same two-transaction protocol where we wait for old
leases to drain before proceeding to the second transaction to clear the old
namespace entry. (User-defined types and databases will need the same
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
to consider in the context of user-defined types.

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
descriptors, without giving them individually leasable descriptors. But we
expect memory savings from keeping the privileges off the database descriptor:
Assuming we impose a length limit on identifier names of 63 characters
([#48443](https://github.com/cockroachdb/cockroach/issues/48443)), each
additional ID-name pair will require at most 68 bytes (though usually less in
practice), whereas privileges (consisting of a list of name-to-permissions
mappings) are unbounded in size. Storing privileges separately also reduces
potential contention on the database descriptor.

To bound the memory footprint of caching schema names, we can additionally
impose a limit on the number of schemas per database.

One other consequence of maintaining all schema names on the database
descriptor is that the rate at which schema changes be performed on schemas
within the same database is limited by how quickly schema changes can happen in
succession for a single database descriptor. This would probably mainly affect
users' migrations that run schema changes in quick succession when starting up,
as well as ORM tests. (Frameworks and tests generally don't do multiple schema
updates concurrently, so we don't necessarily expect contention in the general
case, just waiting for leases to drain.)

In general, maintaining a coherent cache of all schema names inherently
requires coordination across all nodes, and contention when updating multiple
schemas is unavoidable. When publishing new database descriptor versions in the
presence of contention, if it turns out that we spend too much time waiting in
the retry loop and lowering the backoff isn't viable, we could build a more
exact waiting mechanism for retries.

### The `public` schema, cross-database references, and backward compatibility

Our current implementation of schemas has areas of incompatibility with
Postgres, and requires special handling for backward compatibility in this
proposal:

* The `public` schema has an "ID" of 29, which is the `parentSchemaID` for all
  new tables, but this is essentially a placeholder. If all schemas are to have
  their own descriptors, it's impossible for each database's `public` schema
  to have the same ID. In Postgres, the `public` schema is a schema like any
  other.
* We allow cross-database references for foreign keys, views, etc., whereas
  Postgres only allows cross-schema references within the same database.
* More broadly speaking, our databases work similarly to how schemas work in
  Postgres: Conceptually, they're the immediate "parent" of tables, and we
  allow cross-database references.

Currently, all (permanent) tables involved in cross-database references are
necessarily in the `public` schema of their respective databases. We propose
disallowing cross-database references in the general case, with a temporary
exception for tables in `public` for the sake of backward compatibility.
Allowing cross-database references introduces complexity while providing no
real advantages if schemas are fully supported, and users may misuse the
"feature" since it violates Postgres-based expectations of databases being
self-contained.

The proposed deprecation cycle:
* In the release where user-defined schemas are introduced, all existing tables
  remain in the `public` pseudo-schema with a `parentSchemaID` of 29, and all
  existing cross-database references are preserved.
    * We will provide tools for users to convert a database into a schema in a
      specified database (described later).
    * New tables and user-defined types created in `public` should probably go
      into the unupgraded `public` schema, to avoid complications from creating
      a new `public` schema and having two `public` schemas existing
      simultaneously.
    * New databases can get a proper `public` schema from the beginning.
    * Attempting to rename an unupgraded `public` schema should either induce a
      migration to a new schema descriptor or be disallowed. Dropping an
      unupgraded `public` schema should involve dropping the relevant tables.
    * We may want a setting to disallow new cross-database references even in
      the old `public` schemas, turned off by default, to make this transition
      easier.
* In a future release `v` when long-running migrations become available (see
  [#39182](https://github.com/cockroachdb/cockroach/issues/39182)), we'll want
  to upgrade any remaining databases with an old-style `public` schema to have
  a proper `public` schema with a unique ID and a descriptor. We'll need this
  migration to only start running when it's guaranteed that no more nodes at
  version `v-1` can join the cluster, and we'll need to prevent nodes at
  version `v+1` from joining until the migration is complete.
    * We may want to expose a way for users to manually "upgrade" a `public`
      schema in a specific database before the cluster-wide migration occurs.
* At version `v+1`, all schemas will have a proper ID and descriptor, and
  cross-database references will be disallowed in general.

When converting a database to a schema or upgrading the `public` schema to give
it a new ID and descriptor, what's required is changing the `parentID` or
`parentSchemaID` of each table in the database/schema. This process is the same
as the process for renaming a table; in fact, we already support changing the
database of a table with `ALTER TABLE RENAME`.

Some open questions:
* During the interval of time when unupgraded `public` schemas can coexist with
  proper schemas (including upgraded `public` schemas), there will be
  differences in behavior between the two types of schemas. How do we make this
  clear in the UI and avoid surprises?
    * knz:
        > I think the solution is to explain that:
        > * the public schema cannot get zone configs or permissions at all
        > * however, just as any other schema which wasn't assigned zone
            configs or permissions explicitly, it also inherits the settings
            from the database

* Since unupgraded `public` schemas have no separate descriptor for metadata
  and no unique ID, how should we handle updating zone configs and privileges
  for the `public` schema? We could identify those configuration settings with
  the settings for the database itself, but that could lead to confusion.
* Should we allow users to "upgrade" a `public` schema manually (thus possibly
  unlocking, e.g., the ability to set schema-specific zone configs and
  privileges)? What would the UI for this look like?
    * Simply allowing `CREATE SCHEMA public` is an option. (This is slightly
      inconsistent with us already pretending to have a `public` schema in the
      database.)

Note that the `pg_temp` schemas also exist without schema descriptors. Since
they cannot be renamed or dropped or have their privileges changed, it would
not be useful to make them leasable, and they can remain as descriptor-less
schemas indefinitely.

### Converting databases into schemas

To preserve references betwen tables in different databases, we will need to
expose a way for users to convert their existing databases into schemas. Only
databases with no user-defined schemas would be eligible to be converted.

The conversion consists of:
* creating a new schema;
* updating the tables' descriptors and namespace entries, with a draining
  interval, which basically amounts to renaming the table; and
* (possibly) dropping the old database.

To provide an all-in-one migration, we can just do these steps sequentially.
For example, suppose database `old_db` (ID 52) contains a single table `tbl`
(ID 51), and we would like to convert `old_db` into a schema in database
`new_db` (ID 53). Both `tbl` and `new_db` are on version 1. Before the schema
change, `system.namespace` contains the following entries:

  parentID | parentSchemaID |  name  | id
-----------|----------------|--------|-----
         0 |              0 | new_db | 53
         0 |              0 | old_db | 52
        52 |             29 | tbl    | 51

At time `t1`, create a new schema in `new_db` with name `old_db` (ID 54), which
bumps `new_db` to version 2, and wait for leases on `new_db` to drain.

At time `t2`, insert a new namespace entry for `tbl` with the new `parentID`
and `parentSchemaID`, and update the descriptor with the new IDs. `tbl` is now
on version 2. Wait for leases on `tbl` to drain. At this point, the namespace
table contains the following entries:

  parentID | parentSchemaID |  name  | id
-----------|----------------|--------|-----
         0 |              0 | new_db | 53
         0 |              0 | old_db | 52
        52 |             29 | tbl    | 51
        53 |              0 | old_db | 54
        53 |             54 | tbl    | 53

At time `t3`, delete the old namespace entry for `tbl`, and start dropping the
database `old_db`. The dropped database needs to go through another draining
interval before being removed from the namespace table at some time `t4`,
assuming that transactional database dropping is implemented. After that, the
namespace table contains the following entries:

  parentID | parentSchemaID |  name  | id
-----------|----------------|--------|-----
         0 |              0 | new_db | 53
        53 |              0 | old_db | 54
        53 |             54 | tbl    | 53

In the interval between `t2` and `t3`, both the `old_db.public.tbl` and
`new_db.old_db.tbl` names can be used, and it is forbidden for any other table
named `old_db.public.tbl` to exist. Regardless of the cached version of the
table, using the name `old_db.tbl` always causes `old_db` to be resolved as a
schema.

Syntax-wise, we could treat this as part of `ALTER SCHEMA RENAME` with a
special-case implementation, so the above would be a result of `ALTER SCHEMA
old_db.public RENAME TO new_db.old_db`.

To turn a set of databases with cross-database references into a set of schemas
with cross-schema references, we'd need a way to migrate multiple databases in
the same transaction, and defer the check that no cross-database references
persist until when the transaction is ready to commit.

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
  scans worse on that table. This was brought up in the context of user-defined
  types, and the decision was that (somehow) building an index would be better
  than trying to split types off in their own table; the same thing applies
  here. ([#47534](https://github.com/cockroachdb/cockroach/issues/47534) tracks
  support for a native protobuf column type, which is a first step.)

## Rationale and Alternatives

On schema metadata:
* Storing all schema metadata on the database descriptor instead of storing
  separate leasable descriptors was considered, and rejected for reasons of
  reducing memory usage and database descriptor contention, as described above.

On backward compatibility and cross-database references:
* We considered allowing cross-database references from `public` in perpetuity
  (or even allowing them in general), but decided this wasn't worth the
  complexity. From a feature point of view, there's no good reason to allow
  cross-database references in the first place, since they violate user
  expectations and solve a problem that should have been solved by schemas.
  Backward-compatibility concerns are alleviated by giving users ways to
  rename/move their existing tables and schemas to avoid cross-database
  references. 
* If we wanted to keep `public` as a special case for cross-database
  references, there was a possibility of keeping it as a special descriptorless
  schema for tables whose parent (conceptually speaking) was the database
  itself. We decided against this along similar lines.

## Unresolved questions

* There are some details to be worked out about how exactly the migration to
  upgrade all existing `public` schemas will work, and how those unupgraded
  `public` schemas will coexist with other schemas, new tables, etc. The first
  question should be answered once more progress is made on long-running
  migrations.
* How much of the existing schema change infrastructure for tables should we
  try to extract and make more general, at least from the outset? (e.g.,
  `MutableTableDescriptor`, the `SchemaChanger` or specific parts of it, etc.)
  It seems tempting to do this for renaming and dropping objects, since the
  interactions with `system.namespace` across all object types are very
  similar. This is a question for user-defined types as well as schemas and
  databases.
