- Feature Name: Temporary Tables
- Status: draft
- Start Date: 2019-10-02
- Authors: Arul Ajmani, knz
- RFC PR: [#30916](https://github.com/cockroachdb/cockroach/pull/30916)
- Cockroach Issue: [#5807](https://github.com/cockroachdb/cockroach/issues/5807)

# Summary

This RFC proposes to introduce support for session-scoped temporary tables. Such Temporary Tables 
can only be accessed from the session they were created in and persist across transactions in the same
session. Temporary tables are also automatically dropped at the end of the session.

As this RFC proposes to make changes to the PK of `system.namespace` table, it will only be available
on cluster version 20.1 and later.

Eventually we want to support transaction scoped temporary tables as well, but that is out of scope for this RFC.


# Motivation

A. Compatibility with PostgreSQL -- ORMs and client apps expect this to work. 

B. It exposes an explicit way for clients to write intermediate data to disk.

# Guide-level explanation

The semantics described below are the same as those offered by Postgres.

Temporary tables (TTs) are data tables that only exist within the session they are defined. 
This means that two different sessions can use the same TT name without conflict, and the data 
from a TT gets automatically deleted when the session terminates.

A temporary table is defined using `CREATE TEMP TABLE` or `CREATE TEMPORARY TABLE`. 
The remainder of the `CREATE TABLE` statement supports all the regular table features.

The differences between TTs and non-temporary (persistent) tables (PTs) are:
- A TT gets dropped automatically at the end of the session, a PT does not.
- A PT created by one session can be used from a different session, whereas a TT is only usable from
 the session it was created in.
- TTs can depend on other TTs using foreign keys, and PTs can depend on other PTs, but it's not
 possible to refer to a PT from a TT or vice-versa.
- The name of a newly created PT can specify the `public` schema. 
If/when CRDB supports user-defined schemas ([#26443](https://github.com/cockroachdb/cockroach/issues/26443)), a PT can specify any user-defined physical schema as well; 
in comparison CREATE TEMP TABLE must always specify a temporary schema as target and TTs always get 
created in a special session-specific temporary schema.
- A TT/PT can not be converted to a PT/TT after creation.

Additionally, TTs are exposed in `information_schema` and `pg_catalog` like regular tables, with their 
temporary schema as parent namespace. TTs can also use persistent sequences in the same ways that 
persistent tables can.

### Temporary schemas
TTs exist in a session-scoped temporary schema that gets automatically created the first time a TT 
is created, and also gets dropped when the session terminates. 

There is just one temporary schema defined per database and per session. Its name is auto-generated 
based on the session ID. For example, session with ID 1231231312 will have 
`pg_temp_1231231312` as its temporary schema name.

Once the temporary schema exists, it is possible to refer to it explicitly when creating or using 
tables:
- `CREATE TEMP TABLE t(x INT)` is equivalent to `CREATE TEMP TABLE pg_temp_1231231312.t(x INT)` 
and also `CREATE TABLE pg_temp_1231231312.t(x INT)` and also `CREATE TABLE pg_temp.t(x INT)`
    - Note that the last two equivalences are a reminder that the TEMP keyword is merely syntactic sugar 
    for injecting the `pg_temp_<session_id>` namespace into name resolution instead of `public` when the name is unqualified;
    conversely, the same mechanism is always used when the CREATE statement targets a temporary schema, 
    regardless of whether the TEMP keyword is specified or not.
- `SELECT * FROM t` is equivalent to `SELECT * FROM pg_temp_1231231312.t`
    - (Although see section below about `search_path`)

The temporary schema, when needed the first time, gets auto-created in the current database as 
defined by the `database` session variable (and the head of `search_path`). If a client session 
changes its current database and creates a temporary table, a new temporary schema with the 
same name gets created in the new database. The temporary schema is thus defined per-database 
and it is thus possible to have identically named temporary tables in different databases 
in the same session.

Sessions that do not use temporary tables do not see a temporary schema. 
This provides a stronger guarantee of compatibility with extant CRDB clients that do 
not know about temporary tables yet.

### Name resolution lookup order
CockroachDB already supports the name resolution rules defined by PostgreSQL.
Generally:
- Qualified object names get looked up in the namespace they specify
- Non-qualified names get looked up in the order specified by `search_path`, with the same special 
cases as PostgreSQL.
- It's possible to list the temp schema name at an arbitrary position in `search_path` using the special 
string "pg_temp" (even though the temp schema actually has a longer name).
- If "pg_temp" is not listed in `search_path`, it is assumed to be in first position. This is why, 
unless `search_path` is overridden, a TT takes priority over a PT with the same name.

More details are given below in the "Reference level" section.

### Metadata queries
Metadata query semantics for Postgres and MySQL are very confusing. 
For MySQL, 
- `information_schema.tables` does not contain any temporary tables.
- `information_schema.schemata` does not have an entry for a temporary schema.
- `SHOW SCHEMAS` does not show any additional entry for a temporary schema either.
- As there is no temporary schema, there is no way to supply a schema to `SHOW TABLES` and view all 
temporary tables created by a session. 


For Postgres, 
- `pg_catalog.pg_class` shows entries for other sessions' temporary tables. 
- `pg_catalog.pg_namespace` shows entries for other sessions' temporary schemas.
- `information_schema.tables` does NOT show entries for other sessions' temporary tables.
- `information_schema.schemata` shows entries for other sessions' temporary schemas. 

The Postgres semantics are slightly inconsistent because `information_schema.tables` and `pg_catalog.pg_class`
treat TTs differently. CRDB will slightly tweak the Postgres behavior to provide a consistent, global overview of
all temporary tables/schemas that exist in the database.

CRDB will provide the following semantics:
- `pg_catalog` and `information_schema` will expose all PTs and all TTs.
- `pg_catalog` and `information_schema` will expose all temporary schemas that were created by active sessions.
This is in addition to all other schemas (virtual + `public`) that are shown today. 
- The behavior of `SHOW TABLES/SHOW SCHEMAS` as views over `information_schema` remains unchanged. The
semantics described above extend to them by construction.
- Simply querying for `SHOW TABLES` will continue to return all PTs (under the `public` schema of the current db). 
This is because CRDB implicitly assumes `<current_db>.<public>` if no db/schema is provided explicitly.  
To view all temporary tables, the user would have to explicitly query `SHOW TABLES FROM pg_temp`. This 
would only show the temporary tables created by the session, as `pg_temp` in the context of the 
session is an alias for the session's temporary schema. 
- `SHOW SCHEMAS` will show all the temporary schemas that have been created by different sessions. This
is in addition to all other schemas that are shown today (virtual + `public`)
 
### Compatibility with the SQL standard and PostgreSQL
CockroachDB supports the PostgreSQL dialect and thus the PostgreSQL notion of what a TT should be. 
The differences between PostgreSQL and standard SQL are detailed 
[here](https://www.postgresql.org/docs/12/sql-createtable.html#SQL-CREATETABLE-COMPATIBILITY).

At this point, CockroachDB will not support PostgreSQL's ON COMMIT clause to CREATE TEMP TABLE, 
which defines transaction-scoped temp tables.


# Reference-level explanation
The major challenges involve name resolution of temporary tables and how deletion would occur. 
The high level approach for these bits is as follows:
1. There needs to be a way to distinguish temporary table descriptors from persistent table descriptors 
during name resolution -- In Postgres, every session is assigned a unique schema that scopes 
temporary tables. Tables under this schema can only be accessed from the session that created the schema. 
Currently, CockroachDB only supports the `public` physical schema. As part of this task, CRDB should 
be extended to support other physical schemas (`pg_temp_<session id>`) under which temporary tables can live.
There will be one `pg_temp_<session_id>` schema per (session, database), for every (session, database)
that has created a TT (and the session still exists).
2. Dropping temporary tables at the end of the session -- We can not rely on a node to clean up the 
temporary tables’ data and table descriptors when a session exits. This is because there can be 
failures that prevent the cleanup process to complete. We must run a background process that ensures 
that cleanup happens by periodically checking for sessions that have already exited and had created 
temporary tables.

- Ensuring no foreign key cross referencing is allowed between temporary/persistent tables should not be
  that hard to solve -- a boolean check in ResolveFK to ensure the two table descriptors have the same
  persistence status should suffice. 
  This requires adding an additional boolean flag to TableDescriptor that is set to true if 
  the table is temporary. Even though we create a MutableTableDescriptor in create table, only the
  TableDescriptor part is persisted. Without this field, we can not ascertain that correct FK semantics
  are being followed. 
- DistSQL should “just work” with temporary tables as we pass table descriptors down to remote nodes.


### Workflow

Every (session, database) starts with 4 schemas (`public`, `crdb_internal`, `information_schema`, `pg_catalog`). 
Users mainly interact with the `public` schema. Users only have `SELECT` privileges on the other three
schemas.

Envision the scenario where the user is interacting with the `movr` database and is connected
to it on a session with sessionID 1231231312. At the start, the system.namespaces table will look
like: 

| parentID | parentSchemaID |  name  | ID |
|----------|----------------|--------|----|
|        0 | 0              | movr   |  1 |
|        1 | 0              | public | 29 |
|        1 | vehicles       | 29     | 51 |

Note that pg_temp_1231231312 does not exist yet, as no temporary tables have been created.

When the user issues a command like `CREATE TEMP TABLE rides(x INT)` or `CREATE TABLE pg_temp.rides(x INT)`
for the first time, we generate two new unique IDs that correspond to the schemaID and tableID. If 
the generated IDs are 52 and 53 respectively, the following two entries will be added to system.namespace:

| parentID | parentSchemaID |        name        | ID |
|----------|----------------|--------------------|----|
|        1 |              0 | pg_temp_1231231312 | 52 |
|        1 |             52 | rides              | 53 |

Additionally, (1, pg_temp_1231231312, 0) -> 52 will be cached, so that subsequent lookups for 
interaction with temporary tables do not require hitting the KV layer during resolution.
This mapping can never change during the course of a session because the schema can not be renamed
or dropped. Even if all temporary tables for a session are manually dropped, the schema is not. Thus,
this cache is always consistent for a particular session and does not need an invalidation protocol.

All subsequent TT commands have the following behavior. If the user runs  
`CREATE TEMP TABLE users(x INT)`, we generate a new unique ID that corresponds to the tableID. Say
this generated ID is 54, the following is added to the system.namespaces table:

| parentID | parentSchemaID |        name | ID |
|----------|----------------|-------------|----|
|        1 |             52 | users       | 54 |

When the session ends, the system.namespace table returns to its initial state and the last three
entries are removed. The data in the `users` and `rides` table is also deleted.

### Metadata queries
#### information_schema and pg_catalog
To reflect the semantics described in the Guide level explanation, the TableDescriptor must encode
the `parentSchemaID` as well. 

Currently, when we iterate over all TableDescriptors, physical descriptors get a hardcoded `public` schema.
To make `pg_catalog.pg_class` and `information_schema.tables` behave correctly,
we must change the algorithm to:
- If the TableDescriptor belongs to a PT, it will continue to reflect `public` as its schema.
- If the TableDescriptor belongs to a TT, the `parentSchemaID` will be used to ascertain the 
temporary schema name associated with the TT.

Currently, iterating over all schemas implies going over all virtual schemas and `public`. To correctly
reflect the semantics of `pg_class.namespace` and `information_schema.schemata`, we must scan (and cache)
all schemas from the `system.namespace` table.
   
#### SHOW TABLES
`SHOW TABLES` is a view that uses `pg_namespace` and `pg_class`/`information_schema` to show tables under the supplied schema.
The choice underlying metadata table(s) is dependent on the presence of the `WITH COMMENT` clause in the query.
Ensuring this works correctly is equivalent to ensuring the underlying `pg_catalog` and `information_schema` tables are
correctly populated.

#### SHOW SCHEMAS
`SHOW SCHEMAS` is a view over `information_schema.schemata`. Ensuring this works correctly is equivalent 
to ensuring the underlying table is correctly populated. 

### Name resolution rules (reference guide)
CockroachDB already supports name resolution like PostgreSQL, as outlined in the name resolution 
[RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20180219_pg_virtual_namespacing.md):
- Qualified object names get looked up in the namespace they specify
- Non-qualified names get looked up in the order specified by `search_path`, with the same special 
case as PostgreSQL:
    - If `search_path` mentions pg_catalog explicitly, `search_path` is used as-is
    - If `search_path` does not mention pg_catalog explicitly, then pg_catalog is assumed to be listed as the first entry in search_path.

With temporary tables, another exception is introduced in the handling of `search_path`, which is detailed in depth in 
[src/backend/catalog/namespace.c](https://github.com/postgres/postgres/blob/master/src/backend/catalog/namespace.c): 

- If `search_path` mentions pg_temp explicitly, the `search_path` is used as-is.
- If `search_path` does not mention pg_temp explicitly, then pg_temp is searched before pg_catalog and the explicit list.



## Detailed design

### Session Scoped Namespace

Currently CockroachDB does name resolution by mapping (ParentID, ObjectName) -> ObjectID for all 
objects in the database. This limits our ability to create temporary tables with the same name as 
a persistent table or two temporary tables from different sessions that have the same name. 

To remedy this:
- 29 (the next available unreserved ID) is now reserved for `PublicSchemaID`.
- System.namespace mapping is changed to (ParentDatabaseID, ParentSchemaID, ObjectName) -> ObjectID
- Name resolution for databases changes to (0, 0, Database Name) -> DatabaseID.
- Name resolution for schemas is introduced, as (ParentDatabaseID, 0, Schema Name) -> SchemaID.
- Name resolution for tables changes to (ParentDatabaseID, ParentSchemaID, TableName) -> TableDescriptorID.
- All temporary tables are placed under `pg_temp_<session_id>` namespace. As “pg_” prefixed names are 
reserved in Postgres, it will be impossible for this schema name to conflict with a user defined 
schema once CRDB has that support.
- If a session tries to access a temporary table owned by another session, this can be caught during  
name resolution as the schema name is constructed using the session. A session is only allowed to 
access `pg_temp_<session_id>` and`public` physical schemas.
 

As all `public` schemas have 29 as their ID, this does not require an extra KV lookup for PTs. 
Moreover, having a special ID for public schemas means we do not have to allocate a new ID during
migration/new database creation.
We also create a cache for `pg_temp_<session_id>` schemaID, where the ID is cached after the first
lookup. As schemas can not be dropped or renamed during the session, this cache will always be 
consistent.

#### Migration:
Temporary tables require ParentSchemaIDs, which are in-turn dependent on the new `system.namespace` table. 
The feature must be gated on cluster version 20.1, which is the point where we can safely switch to the new 
`system.namespace` table.

The migration process from the old `system.namespace` to the new one involves the following steps:
- For every DatabaseID that exists in the deprecated `system.namespace` table, a `public` schema is added by 
adding an entry (DatabaseID, 0, public) -> 29.
- For all existing databases, the parentSchemaID field is prefilled  with 0 to match the key encoding
described above.
- For all existing tables, the parentSchemaID field is prefilled with 29 to scope them under `public`.

To ensure correctness of 20.1 nodes in a 19.2/20.1 mixed version cluster, `system.namespace`
access semantics change as well. Before describing the new semantics, here are some
motivating scenarios and key observations:

1. New tables/databases can't be written to just the new `system.namespace` table,
because 19.2 nodes do not have access to it. 
2. New tables/databases can't be written to both the new and old `system.namespace` table
either. Consider the case where a 20.1 node creates a table `t` and writes to both `system.namespace` tables. Then,
a 19.2 node deletes this table `t`. As a 19.2 node only knows about the old `system.namespace`  table, it
leaves a hanging entry for `t` in the new system.namespace table. Now, a 20.1 node can no longer create
a table `t`.  
3. The migration described above runs when a node in the cluster is upgraded to 20.1. There is a 
window between this migration running and the cluster version being bumped to 20.1, where if a new object
is created, it will be present in the old `system.namespace` table instead of the new one. Such
objects may not be copied over even after the cluster version is bumped.
4. Entries in `system.namespace` are never modified. Either new entries  are added,
or existing entries are deleted.

The new `system.namespace` access semantics are:
##### Adding new entries:
- If the cluster version is < 20.1, entries are added only to the old `system.namespace` table. 
- If the cluster version  is >= 20.1, entries are added only to the new `system.namespace` table.

##### Lookups and Deletion:
Namespace table entries may be present in either of the two `system.namespace` tables, or
both of them. 
- If an entry was created after the cluster version bump, it will only exist in the new `system.namespace` table.
- If an entry was created before the cluster version bump, it will be copied over to the new `system.namespace`
as part of the migration.  Such an entry would be present in both versions of `system.namespace`.
- If the cluster  version is still 19.2, an entry will only be present in  the  old 
`system.namespace`. Also any objects created in the window described above will only
be present in the old `system.namespace`, even after the cluster version is bumped.

##### Lookup:
- If the entry exists in the new `system.namespace`, it is considered found and returned.
As entries are never modified, only ever added/deleted, the value returned is correct even if the entry
exists in the old `system.namespace` table as well.
- If the entry is not found, we fallback to the old `system.namespace`. If the entry exists, it is considered
found and returned.
- If the entry is not found in either of the two `system.namespace` tables, the entry is considered to not exist.

##### Deletion:
- The entry is deleted from both the old and new `system.namespace`. If it does not exist in
one of those tables, the delete will have no effect, which is safe.
- We can not stop deleting from the old `system.namespace` table after the cluster version has been bumped,
 because of the fallback lookup logic above.
 
 An implementation of these semantics can be found [here.](https://github.com/cockroachdb/cockroach/blob/5be25e2ece88f9f6f56f42169a29da85cc410363/pkg/sql/sqlbase/namespace.go)

##### Namespace entries that may not be migrated over
As described above, there is a window where objects may be added to the old `system.namespace` after
the migration has been run. Among other cases, this can happen if an object is created after
the last migration is run, but before the cluster version is bumped. Or, this can happen if a node
is creating an entry while the cluster version is being bumped, and the node observes the old (19.2)
cluster version. 

Regardless of why, an entry that *missed* the migration from the old `system.namespace` to the new one
for a 20.1 cluster will continue to be accessible/behave properly because of the lookup and deletion
semantics described above.

The deletion semantics also ensure that an entry present only in the old `system.namespace` for a 20.1
cluster is valid and correct. Such entries should be copied over to the new `system.namespace` table though. The most
straightforward way would be to do this as part of a 20.2 migration, essentially performing a second copy over.
At this point, the old `system.namespace` will be redundant (and can be completely removed).

##### Benchmarks:

Microbenchmarks for system.namespace when a temporary schema exists/ when it doesn't.

|                 name                | master time/op | new approach time/op | delta |
| ----------------------------------- | -------------- | -------------------- | ----- |
| NameResolution/Cockroach-8          | 163µs ± 0%     | 252µs ± 0%           | ~     |
| NameResolution/MultinodeCockroach-8 | 419µs ± 0%     | 797µs ± 0%           | ~     |

|                        name                        | master time/op | new approach time/op | delta |
| -------------------------------------------------- | -------------- | -------------------- | ----- |
| NameResolutionTempTablesExist/Cockroach-8          | 175µs ± 0%     | 337µs ± 0%           | ~     |
| NameResolutionTempTablesExist/MultinodeCockroach-8 | 1.06ms ± 0%    | 1.07ms ± 0%          | ~     |

TPC-C on a 3 node cluster, with 16 CPUS: 

MAX WAREHOUSES = 1565    

### Session Scoped Deletion
There could be cases where a session terminates and is unable to perform clean up, for example when 
a node goes down. We can not rely on a session to ensure that hanging data/table descriptors are 
removed. Instead, we use the jobs framework to perform cleanup. 

Every time a session creates temporary schema, it also queues up a job that is responsible
for its deletion. This job knows the sessionID and databaseID under which the temporary
schema was created. The jobs framework ensures that cleanup occurs even if  the node fails.

The job finds all active sessions and checks if the session associated
with the job is alive. If it is alive, the job sleeps for a specified amount of time. If
it dead, then the job  performs cleanup. 

The temporary table descriptors/table data are cleaned up by setting their TTL
to 0 when they go through the regular drop table process. The namespace table entries
are also deleted.

## Rationale and Alternatives

### Alternative A: Encode the SessionID in the metadataNameKey for Temporary Tables

We can map temporary tables as (ParentID, TableName, SessionID) -> TableDescriptorID. 
The mapping for persistent tables remains unchanged. 

Temporary tables continue to live under the `public` physical schema, but to the user they appear 
under a conceptual `pg_temp_<session_id>` schema. 

When looking up tables, the physical schema accessor must try to do name resolution using both forms
of keys (with and without SessionID), depending on the order specified in the `search_path`. If the 
(conceptual) temporary schema is not present in the `search_path`, the first access must include the 
 sessionID in the key. This ensures the expected name resolution semantics.

The conceptual schema name must be generated on the fly for pg_catalog queries, by replacing `public`
with `pg_temp_<session_id>` for table descriptors that describe temporary tables.

As users are still allowed to reference tables using FQNs, this case needs to be specially checked 
during name resolution -- a user should not be returned a temporary table if they specify 
db.public.table_name. This needs special handling because the temporary schema is only conceptual 
-- everything still lives under the `public` namespace. 

#### Rationale

- No need for an (easy) migration, but this approach offers a higher maintainability cost. 

### Alternative B: In Memory Table Descriptors
#### Some Key Observations:
1. Temporary tables will never be accessed concurrently. 
2. We do not need to pay the replication + deletion overhead for temporary table descriptors for no 
added benefit. 
> Note that this approach still involves persisting the actual data -- the only thing kept in memory
> is the table descriptor.

Instead of persisting table descriptors, we could simply store temporary table descriptors in the 
TableCollection cache by adding a new field. All cached data in TableCollection is transaction scoped
but temporary table descriptors must not be reset after transactions. As all name resolution hits 
the cache before going to the KV layer, name resolution for temporary tables can be easily intercepted. 

To provide session scoped deletion we must keep track of the tableIDs a particular session has 
allocated. The current schema change code relies on actual Table Descriptors being passed to it to 
do deletion, but we can bypass this and implement the bare bones required to delete the data ourselves.
This would only require knowledge of the table IDs, which will have to be persisted.
#### Rationale

1. Temporary tables’ schemas can not be changed after creation, because schema changes 
require physical table descriptors.
2. Dependencies between temporary tables and a persistent sequence can not be allowed. There is no 
way to reliably unlink these dependencies when the table is deleted without a table descriptor. 
3. Debugging when a session dies unexpectedly will not be possible if we do not have access to the
table descriptor. 

### Alternative C: special purpose, limited compatibility, node local TTs 
- In this approach,  TTs are stored in a specialized in-memory rocksdb store, without registered range descriptors in the meta ranges. 
Their table descriptors live in a new session-scoped Go map (not persisted) and forgotten when the session dies. 
Some maximum store size as a % of available physical RAM constrain the total TT capacity per node.
- Operations on such TTs would require a custom alternate TxnCoordSender logic (b/c txn snapshot/commit/rollback semantics have to work over TTs just like PTs,
 but the KV ops need to be routed directly to the new storage bypassing DistSender), custom table reader/writer processors, 
 custom distsql planning, custom CBO rules, custom pg_catalog/info_schema code, custom DDL execution 
 and schema change code, a refactor/extension of the "schema accessor" interface in `sql` and a 
 separate path in name resolution. 
 
#### Rationale
Pros:

- Cleanup is trivial, as when the node goes down, so does the data. 
- Skips the replication overhead for TTs
- No range management "noise" as TTs get created and dropped
- Always stores the data on the gateway node, so provides locality. 
- Would not be susceptible issues around the descriptor table being gossiped. 

Cons: 
- Requires an alternate code path in many places across all the layers of CockroachDB:
    1. Storage :
        - introduce yet another local store type
    2. KV:
        - introduce routing of transactional KV ops to this store type
        - Introduce alternate handling in TxnCoordSender 
    3. SQL schema changes:
        - Alternate DDL logic without real descriptors/leasing
        - Alternate introspection logic for pg_catalog and information_schema code 
    4. Bulk I/O:
        - Alternate specialized `CREATE TABLE AS` code
        - Alternate column and index backfillers 
    5. CBO:
        - Alternate name resolution logic
        - Alternate index lookup and selection logic
        - Requires to indicate in scan nodes and index join nodes which key-prefix to use, which will be different for TTs/PTs.
        - Specialized dist sql planning because TTs would not have ranges/leaseholders. 
    6. SQL execution:
        - Alternate table readers/writers
        - Alternate index joiner
- It also has the same restrictions of alternative B described above.

### Alternative D: persistent TTs but with better locality and less overhead
- Each newly created TT get associated with a zone config upon creation, which assigns its ranges to 
the gateway node where it was created with replication factor 1.
This will need a way to share a single zone config proto between multiple tables, to keep the size 
of system.zones under control (i.e. just 1 entry per node, instead of 1 per TT)

- (optional) Each node starts with an in-memory store tagged with some attribute derived from the node ID
 (to make it unique and recognizable in the web UI), and the zone config uses that to even skip 
 persistence entirely.
     - This solution is a bit risky because we don't yet support removing a store from a cluster, so we'd need to add that logic so that restarting a node works properly
 

#### Rationale

Pros: 
- Same compat benefits as base case
- Better performance / locality in the common case
- Easy for users to customize the behavior and "move" the TT data to other nodes / replication zones 
depending on application needs 

Cons: 
- More work

Note: Alternative D is probably desirable, and can be reached in a v2.


## Unresolved questions
#### Q1. What frequency should the temporary tables be garbage collected at?

 
#### Q2. Do we need to efficiently allocate temporary table IDs? 
Currently, we do not keep track of which table IDs are in use and which ones have been deleted. 
A table ID that has been deleted creates a “hole” in the ID range. As temporary tables are created 
and deleted significantly more than regular tables, this problem will be exacerbated. Does this need
to be solved? Are there any obvious downsides to having large numbers for tableIDs?

This might be part of a larger discussion about ID allocation independent of temporary tables though.

Radu: I don't see why this would be a problem (except maybe convenience during debugging), but if it
is we could generate IDs for temp tables using a separate counter, and always set the high bit for 
these - this way "small" IDs will be regular tables and "large" IDs will be temp tables.
