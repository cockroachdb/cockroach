- Feature Name: Temporary Tables
- Status: draft
- Start Date: 2019-10-02
- Authors: Arul, knz
- RFC PR: #30916
- Cockroach Issue: #5807

# Summary

This RFC proposes to introduce support for session-scoped temporary tables. Such Temporary Tables 
can only be accessed from the session they were created in and persist across transactions in the same
session. Temporary tables are also automatically dropped at the end of the session.

Eventually we want to support transaction scoped temporary tables as well, but that is out of scope for this RFC.


# Motivation

A. Compatibility with PostgreSQL -- ORMs and client apps expect this to work. 

B. It exposes an explicit way for clients to write intermediate data to disk.

# Guide-level explanation

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
- The name of a newly created PT can specify the `public` schema, 
and if/when CRDB supports user-defined schemas, can specify any user-defined physical schema; 
in comparison CREATE TEMP TABLE must always specify a temporary schema as target and TTs always get 
created in a special session-specific temporary schema.

Additionally, TTs are exposed in information_schema and pg_catalog like regular tables, with their 
temporary schema as parent namespace. TTs can also use persistent sequences in the same ways that 
persistent tables can.

### Temporary schemas
TTs exist in a session-scoped temporary schema that gets automatically created the first time a TT 
is created, and also gets dropped when the session terminates. 

There is just one temporary schema defined per database and per session. Its name is auto-generated 
based on the session ID. For example, session with ID 1231231312 will have 
"pg_temp_1231231312" as its temporary schema name.

Once the temporary schema exists, it is possible to refer to it explicitly when creating or using 
tables:
- `CREATE TEMP TABLE t(x INT)` is equivalent to `CREATE TEMP TABLE pg_temp_1231231312.t(x INT)` 
and also `CREATE TABLE pg_temp_1231231312.t(x INT)` and also `CREATE TABLE pg_temp.t(x INT)`
>Note that the last two equivalences are a reminder that the TEMP keyword is merely syntactic sugar 
>for injecting the `pg_temp_<session_id>` namespace into name resolution instead of `public` when the name is unqualified;
> conversely, the same mechanism is always used when the CREATE statement targets a temporary schema, 
> regardless of whether the TEMP keyword is specified or not.
- SELECT * FROM t is equivalent to SELECT * FROM pg_temp_1231231312.t
(Although see section below about search_path)

The temporary schema, when needed the first time, gets auto-created in the current database as 
defined by the `database` session variable (and the head of search_path). If a client session 
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
- Non-qualified names get looked up in the order specified by search_path, with the same special 
cases as PostgreSQL.
- It's possible to list the temp schema name at an arbitrary position in search_path using the special 
string "pg_temp" (even though the temp schema actually has a longer name).
- If "pg_temp" is not listed in search_path, it is assumed to be in first position. This is why, 
unless search_path is overridden, a TT takes priority over a PT with the same name.

More details are given below in the "Reference level" section.

### Compatibility with the SQL standard and PostgreSQL
CockroachDB supports the PostgreSQL dialect and thus the PostgreSQL notion of what a TT should be. 
The differences between PostgreSQL and standard SQL are detailed 
[here](https://www.postgresql.org/docs/12/sql-createtable.html#SQL-CREATETABLE-COMPATIBILITY).

At this point, CockroachDB will not support PostgreSQL's ON COMMIT clause to CREATE TEMP TABLE, 
which defines transaction-scoped temp tables.


# Reference-level explanation

Ensuring no foreign key cross referencing is allowed between temporary/persistent tables should not be
that hard to solve -- a boolean check at the point of establishment should suffice. 
This requires adding an additional boolean flag to TableDescriptors that is set to true if 
the table is temporary.

DistSQL should “just work” with temporary tables as we pass table descriptors down to remote nodes.
 
The major challenges involve name resolution of temporary tables and how deletion would occur. The high level approach for these bits is as follows:
1. There needs to be a way to distinguish temporary table descriptors from persistent table descriptors 
during name resolution -- In Postgres, every session is assigned a unique schema that scopes 
temporary tables. Tables under this schema can only be accessed from the session that created the schema. 
Currently, CockroachDB only supports the `public` physical schema. As part of this task, CRDB should 
be extended to support other physical schemas (`pg_temp_<session id>`) under which temporary tables can live.
2. Dropping temporary tables at the end of the session -- We can not rely on a node to clean up the 
temporary tables’ data and table descriptors when a session exits. This is because there can be 
failures that prevent the cleanup process to complete. We must run a background process that ensures 
that cleanup happens by periodically checking for sessions that have already exited and had created 
temporary tables.

### Workflow

Every session starts with 4 schemas (`public`, `crdb_internal`, `information_schema`, `pg_catalog`). 
Users mainly interact with the `public` schema. They only have `SELECT` privileges on the other three.

Envision the scenario where the user is interacting with the `movr` database and is connected
to it on a session with sessionID 1231231312. At the start, the system.namespaces table will look
like: 

| parentID | name     | Id | parentSchemaID |
|----------|----------|----|----------------|
| 0        | movr     | 1  | 0              |
| 1        | public   | 0  | 0              |
| 1        | vehicles | 51 | 0              |

Note that pg_temp_1231231312 does not exist yet, as no temporary tables have been created.

When the user issues a command like `CREATE TEMP TABLE rides(x INT)` or `CREATE TABLE pg_temp.rides(x INT)`
for the first time, we generate two new unique IDs that correspond to the schemaID and tableID. If 
the generated IDs are 52 and 53 respectively, the following two entries will be added to system.namespace:

| parentID | name               | Id | parentSchemaID |
|----------|--------------------|----|----------------|
| 1        | pg_temp_1231231312 | 52 | 0              |
| 1        | rides              | 53 | 52             |

Additionally, (1, pg_temp_1231231312, 0) -> 52 will be cached, so that subsequent lookups for 
interaction with temporary tables do not require hitting the KV layer during resolution.
This mapping can never change during the course of a session because the schema can not be renamed
or dropped. Even if all temporary tables for a session are manually dropped, the schema is not. Thus,
this cache is always consistent for a particular session.

All subsequent TT commands have the following behavior. If the user runs  
`CREATE TEMP TABLE users(x INT)`, we generate a new unique ID that corresponds to the tableID. Say
this generated ID is 54, the following is added to the system.namespaces table:

| parentID | name               | Id | parentSchemaID |
|----------|--------------------|----|----------------|
| 1        | users              | 54 | 52             |

When the session ends, the system.namespace table returns to its initial state and the last three
entries are removed. The data in the `users` and `rides` table is also deleted.

### Name resolution rules (reference guide)
CockroachDB already supports name resolution like PostgreSQL, as outlined in the name resolution 
[RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20180219_pg_virtual_namespacing.md):
- Qualified object names get looked up in the namespace they specify
- Non-qualified names get looked up in the order specified by search_path, with the same special 
case as PostgreSQL:
    - If search_path mentions pg_catalog explicitly, search_path is used as-is
    - If search_path does not mention pg_catalog explicitly, then pg_catalog is assumed to be listed as the first entry in search_path.

With temporary tables, another exception is introduced in the handling of search_path, which is detailed in depth in 
[src/backend/catalog/namespace.c](https://github.com/postgres/postgres/blob/master/src/backend/catalog/namespace.c): 

- If search_path mentions pg_temp explicitly, the search_path is used as-is.
- If search_path does not mention pg_temp explicitly, then pg_temp is searched before pg_catalog and the explicit list.



## Detailed design

### Session Scoped Namespace

Currently CockroachDB does name resolution by mapping (ParentID, ObjectName) -> ObjectID for all 
objects in the database. This limits our ability to create temporary tables with the same name as 
a persistent table or two temporary tables from different sessions that have the same name. 

To remedy this:
- System.namespace mapping is changed to (ParentID, ParentSchemaID, ObjectName) -> ObjectID
- Name resolution for databases changes to (0, 0, Database name) -> DatabaseID.
- Name resolution for schemas is introduced, as (Parent ID, 0, Schema Name) -> SchemaID.
- Name resolution for tables changes to (ParentID, SchemaID, Table Name) -> TableDescriptorID
- All temporary tables are placed under `pg_temp_<session_id>` namespace. As “pg_” prefixed names are 
reserved in Postgres, it will be impossible for this schema name to conflict with a user defined 
schema once CRDB has that support.
- If a session tries to access a temporary table owned by another session, this can be caught during  
name resolution as the schema name is constructed using the session. A session is only allowed to 
access `pg_temp_<session_id>` and`public `physical schemas.
 
To reduce the extra lookup for `public` and `pg_temp_<session_id>` schemaIDs, we cache the result after the first 
lookup. As schemas can not be dropped or renamed during the session, this cache will always be 
consistent.

#### Migration:
- For every DatabaseID that exists in the system.namespace table, a `public` schema is added by 
adding an entry (DatabaseID, 0, public) -> 0.
- For all existing tables, the schemaID field is prefilled with 0 to scope them under `public`.

### Session Scoped Namespace
There could be cases where a session terminates and is unable to perform clean up, for example when 
a node goes down. We can not rely on a session to ensure that hanging data/table descriptors are 
removed. Instead, we use a daemon process to perform cleanup.

A background process finds all active sessions and filters through the system.namespace table to find 
namespaces associated with sessions that have exited. The temporary table descriptors/table data are 
then cleaned up by setting their TTL to 0 and going through the regular drop table process.

As the namespace resolution only relies on the sessionID, we do not need to maintain any additional 
data structure that keeps track of temporary table descriptors created by a session. For example, 
say sessionID 123 goes offline. When the background process scans all the temporary schemas in 
systems.namespace, it will realize that pg_temp_123 exists, but the session does not. 
All temporary tables scoped under pg_temp_123 can then be safely deleted.

## Rationale and Alternatives

### Alternative A: Encode the SessionID in the metadataNameKey for Temporary Tables

We can map temporary tables as (ParentID, TableName, SessionID) -> TableDescriptorID. 
The mapping for persistent tables remains unchanged. 

Temporary tables continue to live under the `public` physical schema, but to the user they appear 
under a conceptual `pg_temp_<session_id>` schema. 

When looking up tables, the physical schema accessor must try to do name resolution using both forms
of keys (with and without SessionID), depending on the order specified in the search_path. If the 
(conceptual) temporary schema is not present in the search_path, the first access must include the 
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

Instead of persisting table descriptors, we could simply store temporary tabled descriptors in the 
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



## Unresolved questions

#### Q1. Do we need to efficiently allocate temporary table IDs? 
Currently, we do not keep track of which table IDs are in use and which ones have been deleted. 
A table ID that has been deleted creates a “hole” in the ID range. As temporary tables are created 
and deleted significantly more than regular tables, this problem will be exacerbated. Does this need
to be solved? Are there any obvious downsides to having large numbers for tableIDs?

This might be part of a larger discussion about ID allocation independent of temporary tables though.
