- Feature Name: Finer-grained Role Privileges
- Status: draft
- Start Date: 2020-07-21
- Authors: Solon Gordon, Joel Kenny

# Summary

CockroachDB offers a built-in "admin" role which can be granted in order to
give users a wide range of privileges. For one thing, it grants full
permissions on all objects in the database. But there are also a number of
operations which only admins are permitted to perform, like creating a new
database or changing cluster settings. Since these operations can only be
performed by admins, it's currently impossible to grant a user the ability to
perform any one of these operations without giving them full admin access. This
document proposes adding new role-level privileges so that admin-like abilities
can be granted in a more fine-grained manner.

# Motivation

One major motivator for these changes is CockroachCloud. When we create the
initial user for a CockroachCloud cluster, we currently grant the admin role
to that user so they can perform basic operations like creating new databases.
However, this also gives that user the ability to perform actions which could
damage their cluster, like deleting the user which is used for automatic
backup. Rather than granting the admin role, the CockroachCloud team would like
the ability to create an "operator" role which only grants whatever abilities
are necessary for the user to administer their cluster.

These new options will also be useful for customers who want to grant users
administrative abilities without managing the security of their cluster by
limiting the number of people with true admin access.

# Guide-level explanation

As of v20.1, CockroachDB supports a few role-level options which can be granted
via the `CREATE ROLE` or `ALTER ROLE` statement. One example is `CREATEROLE`,
which grants users the ability to create, alter, and drop other roles. A
complete list is available at
https://www.cockroachlabs.com/docs/v20.1/alter-role.html#parameters.

In order to support granting admin-like abilities, we add several new role
options:

### CREATEDB
Allows a user to create new databases. The user who issues the command is the
owner of the database in question. This confers the ability to rename the
database via `ALTER DATABASE ...  RENAME`.

### CREATELOGIN
Allows a user to manage authentication. This grants access to:
* the `WITH PASSWORD` clause for `CREATE/ALTER USER/ROLE`
* the `VALID UNTIL` clause for `CREATE/ALTER USER/ROLE`
* `ALTER USER/ROLE CREATELOGIN/NOCREATELOGIN`
* `ALTER USER/ROLE LOGIN/NOLOGIN`

Note that these abilities were previously available to users with the
CREATEROLE option. For backward compatibility, a 20.2 migration grants
CREATELOGIN to all users who previously had CREATEROLE.

### CREATEROLE
As noted above, we already support this option. However, it currently does not
prevent a non-admin user from dropping an admin. We will add this restriction
to protect against dropping essential admin users.

### CONTROLJOB
Allows a user to pause, resume, and cancel jobs. Non-admin users cannot control
jobs created by admins.

### CANCELQUERY
Allows users to cancel queries and sessions of other users. Without this
privilege, users can only cancel their own queries and sessions. Even with this
privilege, non-admins cannot cancel admin queries or sessions. This option
should usually be combined with VIEWACTIVITY so that the user can view other
user's query and session information.

### VIEWACTIVITY
Allows a user to see other users' queries and sessions via the following means:
- SHOW QUERIES
- SHOW SESSIONS
- the WebUI Statements and Transactions pages
Without this privilege, the SHOW commands only show the user's own data, and the
WebUI pages are unavailable.

### CONTROLCHANGEFEED
Allows users to run `CREATE CHANGEFEED` on tables they have `SELECT` privileges
on. In the future this should also control whether a user can pause, resume, or
cancel a changefeed, but that is currently controlled via job control so will
be determined by the `CONTROLJOB` option.

### MODIFYCLUSTERSETTING
Allows a user to modify certain cluster settings. At present this allows
modifying any setting with the `sql.defaults` prefix, but this list may be
expanded in the future.

## BACKUP, RESTORE, and IMPORT INTO
We also add the ability for non-admin users to perform certain BACKUP, RESTORE,
and IMPORT INTO operations. Rather than enabling this via a new role option, we
do so based on existing privileges. Database and table backups require SELECT
privileges on the target object. User-defined type and schema backups require
the USAGE privilege.

RESTOREs require CREATE privileges on all restored objects and CREATEDB in the
case of database restores.

IMPORT INTO requires INSERT and DROP on the target table. (DROP is required
because the IMPORT implementation makes the table unavailable for the duration
of the operation.)

Full cluster and tenant BACKUP and RESTORE continue to require admin.

We also restrict what source URLs non-admins can use for these operations.
nodelocal, HTTP, and AWS/GCS/Azure sources which rely on implicit credentials
will continue to require the admin role. This is acceptable for the Cockroach
Cloud use case since that system uses explicit, temporary credentials for
RESTORE.

# Reference-level explanation
The backend implementation of these new privileges should be straightforward.
Role-level options are already stored in the `system.role_options` table and no
migration is necessary to add new options. We will add the new privileges to
our list of supported role options and assert that a user has the appropriate
role option (or is an admin) when performing the associated operation.

# Limitations

## Automatic grants
The new privileges proposed here do not include a way to guarantee that a role
has privileges on all objects in the cluster. This remains a unique property of
the admin role.

This may present a problem for CockroachCloud, since the customer will not have
access to any admin users, so there will be no user who has reliable access to
all objects. However, adding a mechanism for granting non-admins cluster-wide
access is also undesirable, because there are some databases and tables which
we don't want them to have access to. One example of this is tables which we
use to manage automatic backups.

However, the CockroachCloud team could mitigate this by enhancing their
management console to allow operators to modify privileges. For example, the
console could allow altering the owner of database objects. That way a user
could switch the owner to themself or the operator role if they needed access
to a given object. Behind the scenes, these privileges changes would be
performed by an admin user which is not exposed to the customer.

# Alternatives

### CONTROLCHANGEFEED
Rather than making CONTROLCHANGEFEED a role option, we could consider making it
a privilege at the database/schema/table level. This would provide more granular
control but seems less desirable in a few ways:
* Since the owner of an object receives all privileges on it, creating a table
  would give a user the ability to create a changefeed, which might not be
  desired.
* It's not clear if there is a realistic scenario where a user should only have
  the ability to create changefeeds on only certain tables they have read
  access to.
