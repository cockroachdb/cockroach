- Feature Name: SQL Role-based access control
- Status: in-progress
- Start Date: 2017-11-18
- Authors: Marc Berhault
- RFC PR: [#20149](https://github.com/cockroachdb/cockroach/pull/20149)
- Cockroach Issue: [#20371](https://github.com/cockroachdb/cockroach/issues/20371)

Table of Contents
=================

   * [Summary](#summary)
   * [Motivation](#motivation)
   * [Related resources](#related-resources)
   * [Out of scope](#out-of-scope)
   * [Guide-level explanation](#guide-level-explanation)
      * [Terminology](#terminology)
      * [User-level explanation](#user-level-explanation)
         * [Role basics](#role-basics)
         * [Creating a role](#creating-a-role)
         * [Granting and revoking privileges for a role](#granting-and-revoking-privileges-for-a-role)
         * [Manipulating role membership](#manipulating-role-membership)
         * [Dropping a role](#dropping-a-role)
         * [Listing roles](#listing-roles)
         * [Listing role memberships](#listing-role-memberships)
         * [Administrators](#administrators)
         * [Non-enterprise users](#non-enterprise-users)
      * [Contributor impact](#contributor-impact)
   * [Reference-level explanation](#reference-level-explanation)
      * [Detailed design](#detailed-design)
         * [Roles system table](#roles-system-table)
         * [New SQL statements](#new-sql-statements)
         * [Modifying role memberships](#modifying-role-memberships)
         * [Expanding role memberships](#expanding-role-memberships)
         * [Checking permissions](#checking-permissions)
         * [Admin role](#admin-role)
         * [Migrations and backwards compatibility](#migrations-and-backwards-compatibility)
         * [Non-enterprise functionality](#non-enterprise-functionality)
         * [Virtual tables](#virtual-tables)
      * [Drawbacks](#drawbacks)
         * [Use of leases to signal role changes](#use-of-leases-to-signal-role-changes)
         * [Code location](#code-location)
      * [Rationale and Alternatives](#rationale-and-alternatives)
         * [Internal representation of memberships](#internal-representation-of-memberships)
      * [Future improvements](#future-improvements)

# Summary

Role-based access control is an enterprise feature.

We propose the addition of SQL roles to facilitate user management.

A role can be seen as a group containing any number of "entities" as members. An entity can be a user or
another role.

Roles can have privileges on SQL objects. Any member of the role (directly a member,
or indirectly by being a member-of-a-member) inherits all privileges of the role.

A new `admin` role will replace the current `root` user, with `root` being a member of `admin`.
Non-enterprise users will not be able to manipulate roles (create/delete/change membership), but
will retain all roles created using an enterprise license.

This will be implemented through the addition of a new `role_members` system table containing role-role and
user-role relationships. Role memberships are synchronized through descriptor versions, a new versions indicating
that a node should refresh role information.

# Motivation

Roles simplify the task of user management on a large database.

By granting database/table privileges to a role, users of a team can be more
easily added to the system and granted all necessary privileges by simply adding them as members
of the corresponding roles. The alternative would be granting privileges for all necessary objects,
making it likely to forget some.

Managing privileges on a given object becomes easier: by changing the privileges granted to the role,
all members of the role are updated implicitly.

# Related resources

We follow the postgresql use of roles:
* [PostgreSQL user management](https://www.postgresql.org/docs/current/static/user-manag.html)
* [PostgreSQL CREATE ROLE](https://www.postgresql.org/docs/10/static/sql-createrole.html)
* [PostgreSQL DROP ROLE](https://www.postgresql.org/docs/10/static/sql-droprole.html)
* [PostgreSQL GRANT on ROLES](https://www.postgresql.org/docs/current/static/sql-grant.html#SQL-GRANT-DESCRIPTION-ROLES)
* [PostgreSQL REVOKE](https://www.postgresql.org/docs/current/static/sql-revoke.html)

# Out of scope

The following are not in scope but should not be hindered by implementation of this RFC:
* role attributes (see [PostgreSQL CREATE ROLE](https://www.postgresql.org/docs/10/static/sql-createrole.html) for full list)
* `ALTER` command (modify role attributes, rename role)
* role auditing
* multiple "admin" (or superuser) roles
* admin UI manipulation of roles
* new privileges (eg: admin UI related)

See [Future improvements](#future-improvements) for discussion of some of these.

# Guide-level explanation

## Terminology

Some terminology used in this RFC:
* **role**: the focus point of this RFC. Please re-read the [summary](#summary)
* **role admin**: a member of the role allowed to change role membership
* **admin**: the special role allowed superuser operations, always exists
* **root**: the special user existing by default as a member of the `admin` role
* **inherit**: the behavior that grants a role's privileges to its members
* **`A ∈ B`**: user or role `A` is a member of role `B`
* **direct member**: A is a direct member of B if `A ∈ B`
* **indirect member**: A is an indirect member of B if `A ∈ C ... ∈ B` where `...` is an arbitrary number of memberships

## User-level explanation

### Role basics

A role can be thought of as a group with any number of members belonging to the group.
Roles have privileges on objects in the same way that users do. All members of the role
inherit the privileges held by the role. This is done recursively.

For example:
* role `employees` has `ALL` privileges on table `mydb.employee_data`
* user `marc` is a member of `employees` (also written: `marc ∈ employees`)
* without any extra privileges, `marc` can now perform any operations on table `mydb.employee_data`

Roles and users follow a small set of rules:
* roles cannot login (they do not have a password and cannot use certs)
* roles and users use the same namespace (ie: if a user `marc` exists, we cannot create a role `marc`)
* users and roles can be members of roles
* membership loops are not allowed (direct: `A ∈ B ∈ A` or indirect: `A ∈ B ∈ C ... ∈ A`)
* all privileges of a role are inherited by all its members
* privileges on a table/database can be assigned to roles and users
* only the admin role can create/drop roles
* role admins (members with the `ADMIN OPTION`) can modify role memberships
* a role cannot be dropped if it still has privileges
* there is no limit (minimum or maximum) to the number of members of a role (except for the `admin` role)

### Creating a role

Cockroach administrators can create a new role called `myrole` using:
```
CREATE ROLE myrole
```

This fails if `myrole` exists either as a role or a user.

### Granting and revoking privileges for a role

Granting privileges to a role is the same a granting privileges to a user. This syntax has not changed.

For example, granting `SELECT` privileges to the role `myrole` on a table consists of:
```
GRANT SELECT ON TABLE mydb.mytable TO myrole
```

Revoking privileges is also the same as before:
```
REVOKE SELECT ON TABLE mydb.mytable FROM myrole
```

### Manipulating role membership

Modification of role membership requires one of the following:
* cockroach administrator (`admin` role)
* role admin (added to the role using `WITH ADMIN OPTION`)

A user `marc` can be added to the role `myrole` without role admin permissions using:
```
GRANT myrole TO marc
```

`marc` is now a "regular" member of the role, inheriting all role privileges. However, `marc`
is not allowed to modify role membership.

To allow a user to modify role membership, we can use `WITH ADMIN OPTION`:
```
GRANT myrole TO marc WITH ADMIN OPTION
```

`marc` is now a member of the role **and** is allowed to modify role membership.

### Dropping a role

Cockroach administrators can delete a role called `myrole` using:
```
DROP ROLE myrole
```

This fails if `myrole` does not exist. We can instead use:
```
DROP ROLE IF EXISTS myrole
```

Dropping a role will fail if it still has direct privileges on database objects (privileges through membership
in other roles does not trigger an error).

Any role relationships involving this role will be removed automatically.

This applies to dropping users as well: dropping user `A` when `A ∈ myrole` will automatically remove
the membership.

### Listing roles

We can get a simple list of all roles in the cluster by running:
```
SHOW ROLES

+--------------+
| Role         |
+--------------+
| admin        |
| myrole       |
| myotherrole  |
+--------------+
```

### Listing role memberships

A list of members can be found using:
```
SHOW GRANTS ON ROLE myrole

+--------+----------- +-------+
| Role   | User/Role  | Admin |
+--------+------------+-------+
| myrole | marc       | YES   |
| myrole | other      | NO    |
+--------+------------+-------+
```

We will also allow the following variants:
* lookup all role memberships: `SHOW GRANTS ON ROLE`
* show all memberships for a user: `SHOW GRANTS ON ROLE FOR marc`
* show all members of a role: `SHOW GRANTS ON ROLE myrole`
* show user membership in a role: `SHOW GRANTS ON ROLE myrole FOR marc`

### Administrators

Before this RFC, there is a single administrative user in a cockroach cluster: the user `root`.

We propose to add a default role: `admin` with a single user: `root`.
`root` will have admin privileges on the `admin` role, allowing addition/removal of other users.

The `admin` role cannot be deleted, and cannot be empty. The special `root` user cannot be dropped or removed
from the `admin` role.

All operations that previously checked for the `root` user will now check for membership in the `admin` role.

Clusters created before the existence of roles will have a new `admin` role created automatically and privileges
for it added where `root` currently has privileges. `root` will keep its individual privileges to allow downgrades
to older versions.

### Non-enterprise users

Manipulation of roles is an enterprise feature.

Without a valid enterprise license, the following are **not** allowed:
* creating a role (`CREATE ROLE ...`)
* dropping a role (`DROP ROLE ...`)
* adding a member of a role (`GRANT myrole TO ...`)
* removing a member from a role (`REVOKE myrole TO ...`)

The following will function without an enterprise license:
* listing roles
* listing role memberships
* granting privileges to a role
* revoking privileges from a role

This restriction leaves non-enterprise users with a fully-functioning product but without the
ease of management provided by roles.

## Contributor impact

Any modifications to the SQL code must keep in mind:
* privileges can be applied to roles and users
* roles and users share the same namespace

# Reference-level explanation

A number of concepts are detailed in the [Guide-level explanation](#guide-level-explanation) and not
repeated in this section.

## Detailed design

### Roles system table

We propose the modification of the users table to store roles, and the addition of a new table to store
role memberships.

#### Users table

Since roles and users share the same namespace, it is simplest to alter the `system.users` table to
include roles.

The schema for the users table becomes:
```
CREATE TABLE system.users (
  username         STRING PRIMARY KEY,
  "hashedPassword" BYTES,
  isRole           BOOL,
  (INDEX isRole)
);`
```

Depending on the complexity of migration, we may be able to rename the `username` field and even the table
to `rolename` and `system.roles` to reflect the fact that users are really just roles.

`isRole` denotes whether the corresponding name is a user or a role. Roles cannot log in or have passwords.

#### Role members table

The `role_members` stores the list of all role memberships. This only stores direct relationships:

```
CREATE TABLE system.role_members (
  role          STRING,
  member        STRING,
  isAdmin       BOOL,
  PRIMARY KEY (role, member),
  INDEX (member)
);
```

For alternate representations [Internal representation of memberships](#internal-representation-of-memberships).

### New SQL statements

We propose some new and some modified SQL statements, all mimicking the corresponding statements
in PostgreSQL (see [Related resources](#related-resources)).

The new statements all operate on the `system.users` and `system.role_members` tables.

#### CREATE ROLE

```
CREATE ROLE [ IF NOT EXISTS ] rolename
```

* **Behavior**: creates a new role named `rolename`. Fails if a role or user by that name exists.
* **Permissions**: caller must be an administrator.
* **Enterprise requirement**: valid license required.

#### DROP ROLE

```
DROP ROLE [ IF EXISTS ] rolename
```

* **Behavior**: drops the role named `rolename`. Fails if it does not exist, unless `IF EXISTS` is specified.
* **Permissions**: caller must be an administrator.
* **Enterprise requirement**: valid license required.

#### Grant/revoke privileges to/from a role

The `GRANT` and `REVOKE` statements used to grant privileges to a user will accept a role. No syntax
changes are necessary.

An enterprise license is not required.

#### GRANT role

```
GRANT rolename TO name [ WITH ADMIN OPTION ]
```

* **Behavior**: adds `name` as a member of `rolename`. Member is a role admin if `WITH ADMIN OPTION` is specified.
  - Fails if either `rolename` or `name` does not exist, or if this creates a membership loop.
  - Existing memberships can have their `WITH ADMIN` option enabled, but never disabled.
  - The `ADMIN OPTION` is inherited. eg: if `A ∈ B ∈ C` and `B` has `ADMIN OPTION` on `C`, then `A` has `ADMIN OPTION` on `C` (but not necessarily on `B`).
* **Permissions**: must be an administrator or role admin.
* **Enterprise requirement**: valid license required.

#### REVOKE role

```
REVOKE [ ADMIN OPTION FOR ] rolename FROM name
```

* **Behavior**: removes `name` as a member of `rolename`. Member admin setting is removed if `ADMIN OPTION FOR` is specified, but the membership remains.
Fails if either `rolename` or `name` does not exist. Succeeds if the membership does not exist.
* **Permissions**: must be an administrator or role admin.
* **Enterprise requirement**: valid license required.

#### SHOW ROLES

```
SHOW ROLES
```

* **Behavior**: lists all existing roles (role names only).
* **Permissions**: none required.
* **Enterprise requirement**: none required.

#### SHOW GRANTS ON ROLE

```
SHOW GRANTS ON ROLE [rolename] [ FOR name ]
```

Where `rolename` can be one or more role and `name` can be one of more user or role. An unspecified `rolename`
or `name` returns all roles or members.

* **Behavior**: lists role memberships for matching roles/names.
* **Permissions**: none required.
* **Enterprise requirement**: none required.

**TODO(mberhault)**: should we have an option to pick whether to show direct/indirect/all memberships?

### Modifying role memberships

Modifying role memberships is done through normal operations on the system tables.

However, we set `UpVersion` on the `role_members` table to trigger a descriptor version upgrade,
notifying lease holders that a refresh is needed.

### Expanding role memberships

Expanding role memberships is necessary to perform permission checks. Given how frequently these need to be
done, we propose a per-node cache of role memberships with refreshes triggered by increases of the
`role_members` table descriptor version.

The process for a permission check is:
* if the user's expanded roles is not cached, expand roles using the `role_members` table.
* reads of the `role_members` table is performed at the table descriptor's `ModificationTime`
* the cache holds a lease on the `role_members` table
* when the `role_members` table descriptor version changes (indicating a change to role memberships), refresh the roles from the table

This insures that we can keep role membership information reasonably current with refreshes being forced
only when changes occur.

The role cache can be a simple LRU cache with a maximum size of a few MB, but this limit should be configurable.
Given the likely small size of the cached entries, we can expire them after some amount of inactivity rather than
constantly refreshing them.

Role membership expansion can be pre-computed (see [Internal representation of memberships](#internal-representation-of-memberships)) to allow for cheaper lookup at the time of permission checks.

### Checking permissions

Permission checks currently consist of calls to one of:
```
func (p *planner) CheckPrivilege(descriptor sqlbase.DescriptorProto, privilege privilege.Kind) error
func (p *planner) CheckAnyPrivilege(descriptor sqlbase.DescriptorProto) error
func (p *planner) RequireAdminRole(action string) error
func (p *planner) HasAdminRole() (bool, error)
func (p *planner) MemberOfWithAdminOption(member string) (map[string]bool, error)
```

All methods operate on `p.session.User`.

These functions check a number of things for the specified user:
* has the required privileges in the descriptor
* is the `root` or `node` user
* has "any" privileges on the descriptor

In a world with roles, permission checks are performed against the set of names consisting of:
* the username
* the expanded list of roles the user is a member of

Checking permissions becomes checking that any of the names in the set have the required permissions.
For `root` checks, this is hard-coded to checking that the expanded list includes the `admin` role.

### Admin role

The `admin` role is a special role corresponding to the current `root` user.
It is checked everywhere we currently check `if user == security.RootUser`.

It is created at cluster-creation time (or migration) and initialized with the `root`
user as a member.

The `admin` role cannot be dropped and cannot be empty (member removal of the last member
will fail).

### Migrations and backwards compatibility

Migrations from older versions will perform the following tasks:
* add the `isRole` column to the `system.users` table
* add the `admin` role to the `system.users` table
* create the `system.role_members` table
* create the `root` user
* add `root` as a member of `admin` with `isAdmin = true`
* give admin the same privileges as the `root` user

Notes:
* we intentionally do not remove `root` privileges to allow for binary downgrade.
* is a user named `admin` exists, `log.Fatal` the migration and ask the user to downgrade/drop the user/try again.

### Non-enterprise functionality

See [Non-enterprise users](#non-enterprise-users) for actions requiring an enterprise license.

All prohibited actions are in the "administrative" category of operations (role management), and not
in the path of data-access SQL queries.

This makes the enterprise license check relatively painless.

It is currently possible to manipulate the system tables directly, bypassing role manipulation statements.
Closing this loop-hole is out of scope for this document. See [tracking issue #19277](https://github.com/cockroachdb/cockroach/issues/19277).

### Virtual tables

We must populate some virtual tables with role information.

#### pg_catalog.pg_roles

See [PostgresSQL doc](https://www.postgresql.org/docs/current/static/view-pg-roles.html)

The existing `pg_roles` table is currently populated for all users:
```
h.UserOid(username),         // oid
tree.NewDName(username),     // rolname
tree.MakeDBool(isRoot),      // rolsuper
tree.MakeDBool(false),       // rolinherit
tree.MakeDBool(isRoot),      // rolcreaterole
tree.MakeDBool(isRoot),      // rolcreatedb
tree.MakeDBool(false),       // rolcatupdate
tree.MakeDBool(true),        // rolcanlogin
negOneVal,                   // rolconnlimit
tree.NewDString("********"), // rolpassword
tree.DNull,                  // rolvaliduntil
tree.NewDString("{}"),       // rolconfig
```

A similar entry must be created for each existing role with the following changes:
* `isRoot` is replaced with the condition `rolename == admin`
* `rolcanlogin` is always false

**TODO(mberhault)**: determine if members that inherit superuser roles are listed here with the inherited attributes
(eg: if `marc ∈ admin`, does `marc` have `rolsuper` set to true?).

#### pg_catalog.pg_auth_members

See [PostgresSQL doc](https://www.postgresql.org/docs/current/static/catalog-pg-auth-members.html)

The new virtual table `pg_auth_members` must be added. It describes role memberships:
```
CREATE TABLE pg_catalog.pg_auth_members (
  roleid           OID,   ; ID of the role that has a member
  member           OID,   ; ID of the member
  grantor          OID,   ; ID of the role that granted this membership
  admin_options    BOOL   ; true if member can grant membership in roleid to others
)
```

The `grantor` may or may not be a bogus value.

**TODO(mberhault)**: determine PostgreSQL logic for grantor (especially if original OID is dropped).

#### pg_catalog.pg_authid

See [PostgreSQL doc](https://www.postgresql.org/docs/current/static/catalog-pg-authid.html).

The `pg_authid` virtual table is not currently implemented in Cockroach.
It contains sensitive information (passwords) and PostgreSQL clearly states:
```
Since this catalog contains passwords, it must not be publicly readable. pg_roles is a publicly readable view on pg_authid that blanks out the password field.
```

We propose to keep in unimplemented, users should query `pg_roles` and `pg_auth_members` instead.

#### information_schema

There are multiple relevant tables in `information_schema` that must be changed or added.
* `administrable_role_authorizations`: roles the current user has the `ADMIN OPTION` on.
* `applicable_roles`: roles whose privileges the current user can use.
* `enabled_roles`: roles the current use is a member of (direct or indirect).
* `role_table_grants`: similar to the existing `table_privileges`.

**TODO(mberhault)**: determine exactly which are needed and flesh out details.

## Drawbacks

Roles themselves follow the PostgreSQL behavior and any drawbacks there are intended for compatibility.

There are some minor drawbacks to this implementation.

### Use of leases to signal role changes

While allowing for cheaper role expansions, the use of leases to signal role membership changes
subverts the original purpose.

If a more appropriate solution is found, it can replace the current mechanism at a later date.

### Code location

The role manipulation code is very similar to user manipulation (it's just a field inside `system.users`), making
it preferable to reuse most of the code. We also need the role logic (expand roles, check privileges, etc..) to
keep working in non-enterprise mode.

These make it tricky to place all the role-related code in `pkg/ccl`. We need to figure out exactly which code
can be kept separate.

## Rationale and Alternatives

### Internal representation of memberships

The current proposal requires an iterative lookup to discover all memberships for a user.
This is the most naive way of representing memberships and maps well to the conceptual representation of roles.

This makes role manipulation cheap, with role lookups bearing the brunt of the cost.

Some alternatives include:

Expanding role membership into the roles table (each role contains the list of all roles its a member of):
* **Pros**: fast lookup of a user's memberships
* **Cons**: more complex manipulation of roles, needs a concept of "direct" vs "indirect" membership

Expanding role membership into database/table descriptors (descriptor contains all users with privileges):
* **Pros**: regular SQL operations only require the user and descriptor
* **Cons**: more complex manipulation of roles, increased cost for very large roles

Dual tables: one for direct role membership information, one for expanded membership information:
* the direct membership table would include the `ADMIN OPTION` field and would be queried for role manipulation
* permission checking only requires lookups in the expanded table
* the expanded table can use an index on the user or even an array type for the list of roles
* **Pros**: fast lookup of a users's memberships
* **Cons**: slightly more complex role manipulation

## Future improvements

### Move privileges to their own table

Privileges are currently stored in database/table descriptors. To properly integrate with the
role change control, they need to be moved to their own table.

[Existing issue](https://github.com/cockroachdb/cockroach/issues/2939).

### Role attributes

We do not currently have attributes on users or roles.

While we do not propose to implement all of them, some of them would be useful. For instance:
* `SUPERUSER`: database superuser
* `CREATEDB`: can create databases
* `CREATEROLE`: can manipulate all roles (except the superuser roles)
* `INHERIT|NOINHERIT`: roles privileges are inherited (or not) 

Implementing additional attributes is out of scope for this RFC. If added, this can be done for
both users and roles by adding the corresponding columns to the `system.users` table.

### Convenience statements

Some SQL statements can be enhanced to improve usability. For example:

Adding members `firstmember` and `secondmember` when creating a new role `myrole`:
```
CREATE ROLE myrole ROLE firstmember, secondmember
```

Adding the new role `myrole` as a member of existing roles `existingrole1` `existingrole2:
```
CREATE ROLE myrole IN ROLE existingrole1, existingrole2
```

Adding members `firstadmin` and `secondadmin` to the new role `myrole` with the `ADMIN OPTION` set:
```
CREATE ROLE myrole ADMIN firstadmin, secondadmin
```

### UI visibility and management

At the very least, the admin UI should show role information (privileges on objects, memberships, etc...).
Manipulation of roles may be a UI event.

Changing roles through the admin UI should be possible, but is currently gated on admin UI authentication.

### Backup/restore of privileges/users/roles

Backup/restore currently does not restore privileges or users.
Should it start doing so, roles must be restored as well.
