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
Allows a user to create new databases. The user who issues the command is
granted all permissions on the database in question, emulating Postgres’
behaviour of granting ownership of a database to the user who issues the
`CREATE` call. This also confers the ability to rename a database via
`ALTER DATABASE ...  RENAME`.

### CREATELOGIN
Allows a user to manage authentication. This grants access to:
* the `WITH PASSWORD` clause for `CREATE/ALTER USER/ROLE`
* the `VALID UNTIL` clause for `CREATE/ALTER USER/ROLE`
* `ALTER USER/ROLE CREATELOGIN/NOCREATELOGIN`
* `ALTER USER/ROLE LOGIN/NOLOGIN`

Note that Raphael already has a PR for this:
https://github.com/cockroachdb/cockroach/pull/50601

### CREATEROLE
As noted above, we already support this option. However, it currently does not
prevent a non-admin user from dropping an admin. We will add this restriction
to protect against dropping essential admin users.

### CONTROLJOB
Allows a user to pause, resume, and cancel jobs. Non-admin users cannot control
jobs created by admins.

### CANCELQUERY
Allows users to cancel queries of other users. Without this privilege, users
can only cancel their own queries. Even with this privilege, non-admins cannot
cancel admin queries.

### CANCELSESSION
Allows users to cancel sessions of other users. Without this privilege, users
can only cancel their own session. Even with this privilege, non-admins cannot
cancel admin sessions.

### CHANGEFEED
Allows users to run `CREATE CHANGEFEED` on tables they have `SELECT` privileges
on. In the future this should also control whether a user can pause, resume, or
cancel a changefeed, but that is currently controlled via job control so will
be determined by the `CONTROLJOB` option.

## RESTORE and IMPORT INTO
We also add the ability for non-admin users to perform RESTORE and IMPORT INTO
operations. Rather than enabling this via a new role option, we do so based on
existing privileges. For a database restore, the user must have the CREATEDB
privilege. For a table restore, they must have the CREATE privilege on the
parent database. For IMPORT INTO, the user must have INSERT and DROP on the
target table. (DROP is required because the IMPORT implementation makes the
table unavailable for the duration of the operation.)

We do restrict what source URLs non-admins can use for these operations.
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
has privileges on all objects in the cluster. This remains a unique property
of the admin role.

In scenarios like CockroachCloud where a customer will not have access to an
admin user, this could be undesirable since there will be no user who has
reliable access to all objects. This could be mitigated by having a cron job
which uses an admin user to grant the "operator" role access to all database
objects, excepting those which are used for internal CockroachCloud processes.

Note that in 20.2 we are adding the concept of ownership to our privileges
model, which will somewhat mitigate this issue. If an operator creates a
database, they will automatically become the owner of that database and
therefore have full permissions on all objects within. However, they will not
necessarily have any privileges on databases created by other operators.

Rather than the cron job workaround, CockroachCloud could also consider
enhancing their management UI to allow operators to alter database owners. That
way a user could switch the owner to themself or the operator role if they
needed access to a database.

## Cluster settings
Admins will still be the only users who can modify cluster-level settings. In
the CockroachCloud use case, if there are cluster settings we want to allow
users to change, we can make them available via the UI and rely on an internal
admin user to perform the change.

# Alternatives

### CHANGEFEED
Rather than making CHANGEFEED a role option, we could consider making it a
privilege at the database/schema/table level. This would provide more granular
control but seems less desirable in a few ways:
* Since the owner of an object receives all privileges on it, creating a table
  would give a user the ability to create a changefeed, which might not be
  desired.
* It's not clear if there is a realistic scenario where a user should only have
  the ability to create changefeeds on only certain tables they have read
  access to.
