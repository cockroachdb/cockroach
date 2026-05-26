- Feature Name: RBAC Extensions and System Level Privileges
- Status: draft
- Start Date: 2022-04-26
- Authors: Richard Cai
- RFC PR: [#80580](https://github.com/cockroachdb/cockroach/pull/80580)

# Summary
CockroachDB’s privilege system today is split between grantable privileges and role options requiring users to maintain two mental models when it comes to RBAC. Furthermore, there are use cases that role options based privilege model do not cover. Notably, role options are scoped per role and cannot be inherited.
The proposal here is to extend our current privilege system to support more objects that are not necessarily backed by a descriptor such that privilege inheritance can be well defined.

# Motivation
In CockroachDB today, the privilege system is somewhat bifurcated. We have database object level grants such as SELECT for tables or CREATE for databases. These grants exist only on database objects (tables, schemas, databases, types). We have also leveraged the role option system to define some coarse system level privileges. For example, we have role options such as CONTROLJOB which lets a user pause/cancel jobs. Jobs are not a database level object meaning they do not fall anywhere in the database/schema/table hierarchy and they are also not backed by a descriptor. As such we cannot define privileges on them currently, we only allow defining privileges on database level objects that are backed by a descriptor. In the meanwhile, CONTROLJOB gives us some control over who can interact with jobs. 

The problem with role options are that they are not inheritable, this can be considered an anti-pattern with regards to RBAC. Generally in the RBAC model, when a role is granted a privilege, granting that role to another role will also give the role that privilege. With role options we currently do not support inheritance due to ambiguous inheritance semantics.

This has led to customer issues such as [#79571](https://github.com/cockroachdb/cockroach/issues/79571).

Another relevant use case is that for Cockroach Cloud we want to be able to define an “operator” user which is a step below admin, (granting admin generally violates the least privilege principle). As of right now, the operator role is given `CREATEROLE CREATELOGIN CREATEDB CONTROLJOB CANCELQUERY VIEWACTIVITY CONTROLCHANGEFEED` role options, the problem is, granting the operator role to a user does not actually allow the user to perform operator abilities due to role options not being inherited. The user must login as the operator role itself.

This will also allow us to support grants on virtual tables which has been a pain point in the past.

# Technical Design
The goal here is to extend our current privilege system to allow more granular grants on objects / entities that are not necessarily backed by a descriptor such as jobs and changefeed objects.
Certain role options should be replaced with grants. At a high level, we want to introduce “system” or “global” level grants that follow general inheritance rules. This is a concept that exists in many popular non-Postgres databases. (MySQL, Oracle, SQL Server, Db2). We also want to support more granular grants on jobs and bulkio operations.

## Replacing Relevant Role Options
Note - to preface this, role options will likely not be deprecated, we can keep them side by side with their equivalent form of regular grants due to the overhead not being high enough to warrant a potentially tricky migration. We can emphasize in docs to use system privileges instead.

In short, role options should be reserved for user administration permissions, anything that reads/writes from the database should be a grant.

Role Option | Inheritable
---- | ----
CREATEROLE / NOCREATEROLE | Unchanged
PASSWORD | Unchanged
LOGIN / NOLOGIN | Unchanged
VALIDUNTIL | Unchanged
CONTROLJOB / NOCONTROLJOB | Replace with per job grants Example: GRANT PAUSE ON JOB TYPE BACKUP
CONTROLCHANGEFEED / NOCONTROLCHANGEFEED | Changefeeds are created per table and write to a sink. We can support CHANGEFEED privilege on tables or on the database and also introduce grants on sinks
CREATEDB / NOCREATEDB | Unchanged (Is a Postgres roleoption)
CREATELOGIN / NOCREATELOGIN | Unchanged (?)
VIEWACTIVITY / NOVIEWACTIVITY | Replace with system level grant
CANCELQUERY / NOCANCELQUERY | Replace with system level grant
MODIFYCLUSTERSETTING / NOMODIFYCLUSTERSETTING | Replace with system level grant
VIEWACTIVITYREDACTED / NOVIEWACTIVITYREDACTED | Replace with system level grant
SQLLOGIN / NOSQLLOGIN | Unchanged
VIEWCLUSTERSETTING / NOVIEWCLUSTERSETTING | Replace with system level grant 

##  New System Table

The system.privileges table will store privileges for non-descriptor backed objects.
We will require either a path-style string or the combination of object_type + id to uniquely identify them. The privileges will be represented as a bit mask. We can serialize and deserialize synthetic privilege descriptors using rows in this table to leverage our existing code that manipulates PrivilegeDescriptors for grants. We can also consider making the privileges and role_options columns have type `[]STRING`, the downside here is that it takes slightly more space (probably not a concern) and that we have to add some logic to serialize the string array into a bit array which is how we currently manipulate privileges.

### system.privileges schema option 1

```
CREATE TABLE system.privileges (
username STRING NOT NULL,
object string NOT NULL,
privileges VARBIT NOT NULL,
with_grant VARBIT NOT NULL,
CONSTRAINT "primary" PRIMARY KEY (username, object)
);
```

The `object` is a string style path that represents an object.
The general format of the string is 
`/namespace/id.../`

For example we can identify jobs per user using the string
`/jobs/user/$USERID/backup/$JOBID`

To identify a virtual system table for example the `system.descriptor` table with id 3, we can do:
`/virtual-table/0/29/3`
`virtual-table` identifies the namespace which is virtual tables.
The 0 is the system database id, the 29 is the schema id and the 3 is the table id.

We could identify system level or system cluster setting privileges with:
`/system/` or `/system/cluster-settings/`

### system.privileges schema option 2

Have a object_type (string) and (object_id) INT column.

We will create a new “system.privileges” table.
```
CREATE TABLE system.privileges (
username STRING NOT NULL,
object_id INT NOT NULL,
object_type string NOT NULL,
privileges VARBIT NOT NULL,
with_grant VARBIT NOT NULL,
CONSTRAINT "primary" PRIMARY KEY (username, object, object_type)
);
```


We have also considered making the `object_type` an enum, however this would lead to headaches in mixed version cluster settings. We would have to worry about if the enum value exists or not and this makes adding object types fairly annoying as we'd have to update the system enum.

`system.object_type` will be an enum that defines the various object types that can support privileges in CockroachDB. This tentatively looks like 
```
CREATE TYPE system.object_type AS ENUM('system', 'jobs', 'changefeed_sink')
```

Note that this will be the first example of a system enum.

### Option 1 vs Option 2 Pros and Cons
The path-style string allows for more flexibility, we can for example define jobs per user whereas in option 2 where we identify objects based on the type and id, this is less straightforward to do.
The path-style string also opens up the possibility of easily support wild card grants. The code complexity of option 1 seems marginally higher than option 2.
Option 1 overall seems more appealing due to the flexibility and allowing for expressive grants in the future.

### Alternate considerations
The system level grants could be stored on a PrivilegeDescriptor on the system database, however this would require us to support mutations on the system database descriptor which we currently do not do and this allows us to not perform a round trip whenever we look for a system table.

We could create descriptors to represent every object we want to grant on. This is not an attractive option at first glance as these descriptors will likely only be used to store privileges and nothing more.

## Syntax for System Level Privileges
The current proposal is to support `GRANT SYSTEM [privileges...] TO [users...]`.
We cannot do `GRANT [privileges…] ON SYSTEM TO [users…]` due to the fact that tables can be called system, this would be ambiguous. Using two words, such as SYSTEM ADMIN (can’t think of a good phrase here) would work.

Proposed new system level privileges:
VIEWACTIVITY (this can be broken down into more privileges)
VIEWACTIVITYREDACTED
CANCELQUERY
MODIFYCLUSTERSETTING
VIEWCLSUTERSETTING
VIEWSYSTEMTABLES (can be broken down further if necessary)


The syntax is up in the air however, the grant would create or modify the entry in the system.privileges table.

Example: 
`GRANT SYSTEM MODIFYCLUSTERSETTING TO foo` 

Results in the row
(foo, 1, “system”, 16384, 0)

The bitmask value for MODIFYCLUSTERSETTING is assumed to be 1 << 14 = 16384 in the above example.

Followed by 

`GRANT SYSTEM VIEWCLUSTERSETTING TO foo` 

Results in the row
(foo, 1, “system”, 49152, 0)

The bitmask value for VIEWCLUSTERSETTING is assumed to be 1 << 15 = 32768 in the above example. 1<<14 | 1<<15 = 49152.

## Support for grants on virtual tables
One thing worth explicitly calling out is that users have asked for the ability to grant on virtual tables (crdb_internal, pg_catalog). This proposal allows us to support grants on virtual tables and unify the grant experience for tables. Virtual tables have OIDs starting at `MaxUint32 = 1<<32 - 1` counting backwards, an INT (64 bit) column for object id supports this case.

## Adding new privileges
This proposal allows for CockroachDB engineering teams to add privileges on arbitrarily defined objects as long as those objects can be identified by an object type + object id.
Once the SQL Experience team completes the work for this framework, it is the responsibility of individual engineering teams to add privileges.

## New Bulkio related privileges
Right now we have an issue that we do not have granular controls to enable backup / restore and export / import. For example, right now, admin is required to do a full cluster backup. We could add a system level privilege (ie FULLCLUSTERBACKUP) to enable full cluster backups without admin privileges.

For backing up individual database objects right now, we check for CONNECT on databases, SELECT on tables and USAGE on schemas and types. We can potentially augment these in the future? 

## New Jobs related privileges
The ability to interact with jobs hinges on the coarse grained CONTROLJOB role option. It makes sense to define privileges such as CONTROL on individual job types.

Another ask has been to have VIEW only privilege on jobs.

For example: 
`GRANT CONTROL ON JOB TYPE BACKUP TO foo`.

## New Changefeed related privileges
Changefeed privileges can be defined on the table level, database level. Furthermore, we want to consider defining privileges on sinks.

## Backwards Compatibility Considerations
We’ll aim to not deprecate role options and continue to support those albeit we may want to place emphasis on docs on using regular grants. 

New grants will not affect backwards compatibility.

## Observability
Do we include privileges from system.privileges in pg_catalog or only a crdb_internal table?
Likely confine them to a crdb_internal table as this is crdb specific.

## Multi-region Latency Considerations
In a multi-region cluster, the latency to query a system table may be high. To mitigate this, we can use a cache to reduce the number of hits to the system table. We have caches in similar places such as for authentication and role membership.
A straightforward cache invalidation policy is to clear the cache if any change to the system.privileges table occurs, however we may want to be a little smarter and invalidate entries only if the object identifier is changed in the system table.
We could potentially also use a GLOBAL table.

## Backup / Restore Considerations
Currently we only restore privileges during a full cluster restore. There is no additional work we'd have to do in this case as system tables are handled in backup / restore.
However, we do have plans to support restoring privileges. [#79144](https://github.com/cockroachdb/cockroach/pull/79144). We'll have to update this PR to also special case restoring rows to the system table.
For restore, we have to consider that ID's can change and track ID changes before writing the privilege row to the `system.privileges` table.
The work for restoring privileges in the non full cluster restore case will not be handled in the initial implementation.

## Data Sovereignty Considerations
If an object is located in a specific location, does the privilege metadata also have to belong to that region?
The metadata stored in the `system.privileges` data is not sensitive but it is an open question about whether we also need to locate the metadata in the same region as an object given data domiciling requirements.


