- Feature Name: Default Privileges
- Status: in-progress
- Start Date: 2021-05-20
- Authors: Richard Cai
- RFC PR: 
- Cockroach Issue: #64613, #65604

# Summary

Currently CockroachDB handles privilege “inheritance” on objects in an ad-hoc way.
When a table-like or schema object is created, it copies it’s privilege descriptor from the parent database and removes any invalid privileges.

To illustrate how our current privilege inheritance works, when we create a table’s privilege descriptor, we’ll create a copy of the parent database’s privilege descriptor and remove all “invalid” privileges. An invalid privilege in this case is a privilege that is valid for databases but not for tables. For example, if “CONNECT” privilege is specified for a user on a database, we have to flip the CONNECT bit off because there is no CONNECT privilege for tables.

We’ve defined certain privileges to be valid on databases to support this method of inheritance. This has caused our privilege model to further diverge from Postgres'.

Our goal here is to do-away with our ad-hoc privilege inheritance system and support default privileges like Postgres.

# Motivation

The main motivation for this change is to continue making our privilege system more compatible with Postgres. We want to reduce the burden on users to learn a new privilege system or have to learn the differences between the CockroachDB and Postgres privilege systems. We want as little friction as possible for users to use CockroachDB and thus privileges should be compatible with Postgres.

Overall our current privilege system can be confusing, our current privilege inheritance system follows neither the Postgres or MySQL semantics but is somewhat a mix of both.

# Technical design

### Default Privileges
The main technical change is to support default privileges. Ideally we want to support default privileges as closely as possible to Postgres’ default privileges.

Default privileges exist on schemas and databases. Note, Postgres defines default privileges defined for a database as “global” default privileges. Global makes more sense in the Postgres world as you cannot switch databases during a session like you can in CockroachDB.  You can define default privileges for objects that are created by yourself or roles that you are a member of.

Default privileges define the set of privileges on newly created objects in a database or schema. Default privileges are able to be set for specific users. Changing default privileges on a database or schema does not change the privileges of existing objects in them.

Postgres defines which privileges are default on which objects. See https://www.postgresql.org/docs/current/ddl-priv.html

In summary about default privileges in Postgres:
No privileges are granted to PUBLIC by default on tables, table columns, sequences, foreign data wrappers, foreign servers, large objects, schemas, or tablespaces.
CONNECT and TEMPORARY are granted to PUBLIC by default for databases.
EXECUTE is granted to public by default for functions and procedures.
USAGE is granted to public by default for languages / data types (including domains)
Since CockroachDB does not support all the objects listed, the matrix below does a comparison on which default privileges to public CockroachDB will have.
Default Privileges for PUBLIC role

|                               | Postgres           | CockroachDB                                    |
| ----------------------------- | ------------------ | ---------------------------------------------- |
| Tables                        | None               | None                                           |
| Table Columns                 | None               | X                                              |
| Sequences                     | None               | None                                           |
| Foreign Data Wrappers         | None               | X                                              |
| Foreign Servers               | None               | X                                              |
| Large Objects                 | None               | X                                              |
| Schemas                       | None               | None                                           |
| Tablespaces                   | None               | X                                              |
| Databases                     | Connect, Temporary | Connect, Temporary (not yet supported in CRDB) |
| Functions / Stored Procedures | Execute            | X                                              |
| Languages                     | USAGE              | X                                              |
| Data types                    | USAGE              | USAGE                                          |

### Storing DEFAULT PRIVILEGES in CockroachDB
Currently, we store privileges in PrivilegeDescriptors. They exist on all “object” descriptors ie (TableDescriptor, DatabaseDescriptor). PrivilegeDescriptors carry UserPrivileges which is a list of users and their privileges as a bit field.

To store default privileges, we can continue to use PrivilegeDescriptors (either create a new protobuf field called “DefaultPrivileges” and add them to the object descriptors or we can directly add them to PrivilegeDescriptors. Or we can do what Postgres does and populate the pg_default_acl table which dictates initial privileges for newly created objects. We currently do something similar to this with users, role membership and role options in system.users, system.role_members, and system.role_options.

#### Option 1: Store default privileges on the object descriptor (protobuf)

**Pros**:
- How we currently handle our object privileges.
    - Can likely re-use existing code, ie. We can store the default privileges in a PrivilegeDescriptor and copy the default PrivilegeDescriptor to create the new object’s PrivilegeDescriptor.
    - Can re-use bit manipulation / validation code easily.
- Do not have to query system table to look for privileges
    - System table queries have been a source of slowness for multi region setups
    - Can grab PrivilegeDescriptor in O(1) time when creating a new object from the parent object.
    - Descriptors are currently effectively cached whereas system tables are not.

**Cons**:
- Must loop through all object descriptors to create pg_default_acl table

Note: the public schema does not have a SchemaDescriptor, we’ll have to store the public schema’s default privileges on the database descriptor.

#### Option 2: Store default privileges in a table (like pg_default_acl)

**Pros**:
- This is what Postgres does (?) not really a pro

**Cons**:
- Have to query to grab privileges when creating table, at least O(logn) in number of objects

Note that this is fairly similar to how we store users / role options in system.users / system.role_options.

In terms of performance, option 1 optimizes for object creation since we can grab and copy the default privileges in O(1) time.
Intuitively it makes more sense to optimize for performance for object creation since ALTERING DEFAULT PRIVILEGE is likely to happen less frequently than object creation. Also option 1 is more inline with what we currently do with privileges. Hence, overall option 1 is preferable.

### New Syntax
We’ll have to add new syntax for ALTER DEFAULT PRIVILEGES
Copied from Postgres:
```
ALTER DEFAULT PRIVILEGES
[ FOR { ROLE | USER } target_role [, ...] ]
[ IN SCHEMA schema_name [, ...] ]
Abbreviated_grant_or_revoke
```

Note that there's no syntax where you specify a database name for altering default privileges in Postgres. This makes more sense in Postgres where databases are more isolated. We may want to explore having syntax for altering default privileges for a database.
Alter default privileges without specifying a schema always refers to the current database.

### Migration / Upgrade Path

Another key point to take into consideration is how will we handle existing objects that will not have DEFAULT PRIVILEGES defined.

We largely have two options for a migration. Either we do an explicit migration - a long running migration to populate the default privileges PrivilegeDescriptors on all the database objects.
Or two, we can handle it similarly to how we handled owners, if the object descriptor has no PrivilegeDescriptor field (we can also use a bool or a version field), we’ll interpret it as some set of default privileges* and create the PrivilegeDescriptor as needed. If an ALTER DEFAULT PRIVILEGES is run for that object, we can then create and populate the descriptor (or flip the bool / update the version).

* With an explicit long running migration, we can version gate using a cluster setting to ensure that all the PrivilegeDescriptors have been upgraded such that we can use the default privilege syntax and features.

We can also first use option two to add logic to create default privileges on the fly if the object explicitly does not have default privileges and follow up with a long running migration in a future version.

Overall, I would start with option two as it is less intrusive before performing a long running migration to fully upgrade the descriptors.

We also have to take into consideration what set of default privileges we want to use if the default privileges are not set for a descriptor. We can either use the unchanged default set of privileges for that object type or we may want to generate the default privileges based on the privileges on the object. For example if we have SELECT on a database, the set of default privileges we create should include SELECT for backwards compatibility reasons, prior to this change, a table created in a database where a user has SELECT privilege would have SELECT privilege on the table.


### Deprecating Postgres incompatible privileges

In relation to default privileges, should we ensure all our privileges match 1:1 with Postgres? After supporting default privileges, we should be able to remove the incompatible privileges from being granted to objects. (See table below for incompatibilities).
Example: currently we support SELECT on Databases for Cockroach, Postgres does not allow granting SELECT on databases. As mentioned before, one part of allowing SELECT on databases was to make our ad-hoc inheritance system work.

Once we have our DEFAULT PRIVILEGEs system implemented and ironed out, should we make an effort to fully deprecate SELECT on databases and other similar cases. We currently have not deprecated any privileges to maintain backwards compatibility.

One possible migration path is to deprecate GRANTing of incompatible privileges while allowing them to be REVOKED (from old descriptors) and technically “valid” the have on the descriptor.

In conjunction with deprecating the GRANT syntax, we can also leverage the version field on PrivilegeDescriptors to ensure PrivilegeDescriptors created on the version with default privileges do not have the incompatible privileges.

If necessary / not risky - we can create a long-running migration to fully remove all invalid privileges and upgrade the versions of the Privilege descriptors.

Assuming default privileges are added in 21.2. The steps for migrating would be
1. In 21.2, deprecate granting incompatible privileges. PrivilegeDescriptors created in 21.2 will have version “DefaultPrivileges” and will be validated to ensure no incompatible privileges are on them. Revoke is still allowed
2. In 22.1, add a long running migration to upgrade PrivilegeDescriptors and remove incompatible privileges.
3. In 22.2, remove long running migration. Deprecate syntax for revoking invalid privileges.

Privilege Incompatibilities (CockroachDB vs Postgres)

| Privilege  | Applicable Object Types (Postgres)                                             | Applicable Object Types (CRDB)                      |
| ---------- | ------------------------------------------------------------------------------ | --------------------------------------------------- |
| SELECT     | LARGE OBJECT, SEQUENCE, TABLE (and table-like objects), table column           | SEQUENCE, TABLE (and table-like objects), DATABASES |
| INSERT     | TABLE, table column                                                            | TABLE (and table-like objects), DATABASES           |
| UPDATE     | LARGE OBJECT, SEQUENCE, TABLE, table column                                    | TABLE (and table-like objects), DATABASES           |
| DELETE     | TABLE                                                                          | TABLE (and table-like objects), DATABASES           |
| TRUNCATE   | TABLE                                                                          | N/A                                                 |
| REFERENCES | TABLE, table column                                                            | N/A                                                 |
| TRIGGER    | TABLE                                                                          | N/A                                                 |
| CREATE     | DATABASE, SCHEMA, TABLESPACE                                                   | DATABASE, SCHEMA, TABLE (and table-like objects)    |
| CONNECT    | DATABASE                                                                       | DATABASE                                            |
| TEMPORARY  | DATABASE                                                                       | N/A                                                 |
| EXECUTE    | FUNCTION, PROCEDURE                                                            | N/A                                                 |
| USAGE      | DOMAIN, FOREIGN DATA WRAPPER, FOREIGN SERVER, LANGUAGE, SCHEMA, SEQUENCE, TYPE | SCHEMA, TYPE |

- **Table-like objects includes sequences, views**
- **Sequences have the same privilege set as tables in CRDB, this is not true in Postgres, see USAGE**


## Explain it to folk outside of your team
In an effort to make our privilege system more compatible with Postgres, we would like to introduce default privileges to CockroachDB. CockroachDB currently does not have the concept of default privileges and thus privileges for new objects have their privileges somewhat arbitrarily copied from the schema or database they’re created in. Default privileges allow a user to specify which privileges new objects in a schema or database will have upon creation. This will reduce the burden of requiring CockroachDB users to learn a new type of privilege system. Specifically users who come from Postgres will not have to learn about specific rules in Cockroach’s privilege system reducing the friction of using CockroachDB.

