- Feature Name: Grant Option
- Status: draft
- Start Date: 2021-11-3
- Authors: Jack Wu
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [67410](https://github.com/cockroachdb/cockroach/issues/67410)

# Summary

Currently, a user in CockroachDB is able to grant any privilege they possess to another user if they have the "GRANT" privilege. This
is an issue because we do not have enough granularity over controlling which privileges a user can grant.

Adding a "GRANT OPTION" flag when [granting privileges](https://www.postgresql.org/docs/9.0/sql-grant.html) to or [revoking privileges](https://www.postgresql.org/docs/14/sql-revoke.html)
from a user will allow for more control over the granting ability of the user. If this flag is present in a grant statement, then the user being
granted the privileges is able to grant those same privileges to subsequent users; otherwise, they cannot. Including the flag on revoke statements
will enable removing the ability of a user to grant privileges without taking away the user's privileges. The aforementioned behavior can similarly be extended to grant and revoke
statements when altering default privileges.

Broadly, this change will be implemented by keeping track of which privileges any user can grant, similar to how a user's privileges are
currently stored. The new release will be accompanied by a long-running migration to give all previous users
with the "GRANT" privilege granting ability on their privileges to promote backwards compatibility. The "GRANT" privilege will effectively
be replaced by this change and become deprecated.


# Motivation

Adding functionality for grant options when granting or revoking privileges will support use cases where users of a
database can have certain privileges but cannot give them to others. Implementing this in CockroachDB
will enhance compatibility with the privilege system of Postgres, of which this is already a feature.

# Technical design
### New Syntax

This change will incorporate an optional flag to indicate when to apply the grant option logic on grant ("WITH GRANT OPTION")
and revoke ("GRANT OPTION FOR") statements. Postgres has extensive documentation on [granting](https://www.postgresql.org/docs/9.0/sql-grant.html) 
and [revoking](https://www.postgresql.org/docs/14/sql-revoke.html) on various object types.

For example, grant on tables:
```
GRANT { { SELECT | INSERT | UPDATE | DELETE | TRUNCATE | REFERENCES | TRIGGER }
    [, ...] | ALL [ PRIVILEGES ] }
    ON { [ TABLE ] table_name [, ...]
         | ALL TABLES IN SCHEMA schema_name [, ...] }
    TO { [ GROUP ] role_name | PUBLIC } [, ...] [ WITH GRANT OPTION ]
```

For example, revoke on tables:
```
REVOKE [ GRANT OPTION FOR ]
    { { SELECT | INSERT | UPDATE | DELETE | TRUNCATE | REFERENCES | TRIGGER }
    [, ...] | ALL [ PRIVILEGES ] }
    ON { [ TABLE ] table_name [, ...]
         | ALL TABLES IN SCHEMA schema_name [, ...] }
    FROM { [ GROUP ] role_name | PUBLIC } [, ...]
    [ CASCADE | RESTRICT ]
```

### Core Logic
Similar to how privileges are currently tracked for a user, the grant options will be stored as a uint32, with each bit representing
a different privilege. Each user will have its grant option bits tracked in parallel to its privilege bits.

The grant and revoke functions will be changed to accept a boolean that will be true if the flag is added to the SQL
statement and false otherwise. If true, then the same actions applied to the privilege bits will be applied to the
grant option bits as well (bitwise OR for grant, bitwise AND NOT for revoke).

A validation check will be run whenever a grant statement is executed; it will iterate through the privileges in the statement
and check if the granter possesses that privilege in its grant option bits. If it does not, then an error will be 
returned since this is not a valid grant statement.

An example of an error when the current user tries to grant SELECT to another user but does not possess grant options for it would be:
```
ERROR: missing WITH GRANT OPTION privilege type SELECT
SQLSTATE: 01007
```

An example of an error when the current user tries to revoke SELECT from another user but does not possess grant options for it would be:
```
ERROR: missing WITH GRANT OPTION privilege type SELECT
SQLSTATE: 01006
```


###Special Cases
####Superusers
Root and admin will always have ALL privileges and grant options. Instead of initializing superusers with ALL grant options, the validation check
will simply check if the current user is a superuser and not run if so. This is to save the time needed to look them up in the descriptor.

####Owners
Owners are users who create objects. Owners will always have the ability to grant and revoke from objects as long as they exist. This means that 
even if an owner gets its privileges or grant options revoked, it will still have the ability to grant and revoke privileges to itself or other users.
This is the singular exception to when a user is able to grant/revoke without possessing the privilege itself

###Unimplemented Behavior
According to the documentation, a user can only [revoke](https://www.postgresql.org/docs/14/sql-revoke.html) privileges from another if they were
the one who originally granted it to them. We do not currently have this behavior since we do not track who granted what. This is out of scope for
this addition but will be needed in the future.

The reason this is important to note is because of the new behavior that will be present in the meantime for this feature. A user can revoke privileges
as long as they possess the privilege as well as the grant options for that privilege (the only exception to this are the owners mentioned above).

###Impacted Commands
This change will result in several differences in other commands. Primarily, these changes will revolve around displaying grant options in output.

####`Show GRANTS`
Currently, running `Show Grants` on a database with a single table will display something like this:
```
database_name   | schema_name | table_name | grantee   | privilege_type
----------------+-------------+------------+-----------+-----------------
  defaultdb     | public      | t1         | admin     | ALL
  defaultdb     | public      | t1         | root      | ALL
  defaultdb     | public      | t1         | testuser  | ALL
  defaultdb     | public      | t1         | testuser2 | CREATE
  defaultdb     | public      | t1         | testuser2 | SELECT
```

Integrating grant options will require another column:
```
database_name   | schema_name | table_name | grantee   | privilege_type  | grant_options
----------------+-------------+------------+-----------+-----------------+-------------------------
  defaultdb     | public      | t1         | admin     | ALL             | ALL
  defaultdb     | public      | t1         | root      | ALL             | ALL
  defaultdb     | public      | t1         | testuser  | ALL             | SELECT, INSERT, DELETE 
  defaultdb     | public      | t1         | testuser2 | CREATE          | 
  defaultdb     | public      | t1         | testuser2 | SELECT          | SELECT
```

If the privilege_type is an  individual privilege, then grant_option will either be that same privilege (if present) or blank (if grant option is not held on that).
The one special case here is that a user can hold ALL privileges but only certain grant options, so in this case the grant options it holds would be listed out.

####`has_database_privilege`, `has_table_privilege`, `pg_has_role`
The addition of grant options can simplify the implementation of these functions as well. The code currently gets around this by "representing a single priv with a fake "grant option",
which is later computed as a conjunction between that priv's kind and the GRANT privilege, while also computing a disjunction across all comma-separated privilege strings."

With this current change, this logic can be simplified to simply tap into the grant option information that is now available. See [this](https://github.com/cockroachdb/cockroach/blob/bea70261e51bf25b09098ad4856a8ff4b1b1c176/pkg/sql/sem/builtins/pg_builtins.go#L488) for more information


### Mixed Version Deployments
In mixed-version deployments containing any node with version 21.2, the validation check for whether a user granting
privileges holds those privileges in its grant bits will not be run. This is because the node(s) containing the old
version will have all of its users with empty grant option bits. It is only until all nodes are upgraded from 21.2 to 22.1 
that the check is run, meaning this validation logic will be contained with a version gate.

Differences:

| 21.2 & Mixed Cluster  | 22.1                                           | 
| ------------------------------------------------------------------| ----------------------------------------------------------------- | 
| Any user can grant privileges that they possess given that they have the “GRANT” privilege (ignore grant option privileges). | When granting privileges, run a validation check to make sure the granter has the grant option on those privileges.               |                                                | 

### Long Running Migration (21.2 -> 22.1)
A long running migration will be needed to give grant option access to all previous users before the update.

This is to promote backwards compatibility and avoid unexpected behavior, since customers will be able to continue granting
privileges like they did previously - otherwise, they would have to go back in and re-grant to all users, which could take a
very long time if there are a lot of users.

The migration will need to go in and set every user’s grant option privileges field equal to its privileges field if the
user possesses the "GRANT" privilege since that means it was intended to have granting privileges in 21.2; otherwise, it could
not have granted anyways, so we do nothing.


![](images/grant-option-migration-mixed.png?raw=true)

After the migration is performed, we will deprecate GRANT by alerting users who try to use it in 22.1 and ultimately remove it in 22.2 by
taking it off the list of privileges in addition to removing the validation check that it must be present for a grant to succeed. The validation
check in question is the following in `grant_revoke.go`:

```sql
if err := p.CheckPrivilege(ctx, descriptor, privilege.GRANT); err != nil {
  return err
}
```

## Drawbacks

The drawbacks primarily revolve around added complexity in regards to backwards compatibility. Essentially the only problems 
that can arise are due to developer error, such as invalid grants occurring in mixed version clusters because of an 
improper use of logic gates.


## Rationale and Alternatives

This design is a good choice because it is intuitive and makes use of existing components/abstractions. Not performing this change
means the compatibility gap in privileges between CockroachDB and Postgres would continue.

There was some discussion of the behavior of the long-running migration. Another option that was briefly considered was to not migrate anything,
leaving it up to the users to redistribute grant options. We ultimately decided against this because of how
irritating it could be to have to give grant privileges to potentially many users as well as the confusion this would cause.


# Explain it to folk outside of your team

If I wanted to grant privileges to Alice, I could specify the "WITH GRANT OPTION" flag in the grant statement. If I do so,
then Alice possesses and can grant the privileges it just received from me to other users; if I do not add that flag, then Alice 
still obtains those privileges but cannot grant them to other users.

For example, the snippet below demonstrates how granting privileges works with grant options. Alice obtains all privileges on the
table t1 but since "WITH GRANT OPTION" is not specified, she is unable to grant these to user "Bob". It is only
when Alice is granted all privileges "WITH GRANT OPTION" that she has the ability to grant any privilege on table t1
to another user, which she does to Bob.
```sql
--login as root
USER root;

CREATE USER Alice;
CREATE USER Bob;
CREATE TABLE t1();

GRANT ALL PRIVILEGES ON TABLE t1 TO Alice;

--login as Alice
USER Alice;

--will not work since Alice has all privileges but not the ability to grant them
GRANT SELECT, INSERT ON TABLE t1 to Bob;

--login as root
USER root;

GRANT ALL PRIVILEGES ON TABLE t1 TO Alice WITH GRANT OPTION;

--login as Alice
USER Alice;

--works now since Alice has GRANT OPTION on all privileges
GRANT SELECT, INSERT ON TABLE t1 to Bob;
```

If I wanted to revoke privileges from Alice, I could specify the "GRANT OPTION FOR" flag in the revoke statement. If I do so,
then Alice loses the ability to grant the privileges in that statement to other users but her own privileges remain unchanged; if
I do not add that flag, then Alice loses both those privileges and the ability to grant those privileges.

The snippet below demonstrates how revoking privileges works with grant options. Alice initially receives all privileges
and the ability to grant any of them on table t1. When the grant option for privileges SELECT and DELETE are revoked,
Alice can still use them herself, but she cannot grant them to Bob. Alternatively, when the "GRANT OPTION FOR" flag
is omitted in a revoke statement, both privileges and the ability to grant them are revoked, as shown how
Alice can no longer select from t1.
```sql
--login as root
USER root;

CREATE USER Alice;
CREATE USER Bob;
CREATE TABLE t1();

GRANT ALL PRIVILEGES ON TABLE t1 TO Alice WITH GRANT OPTION;

REVOKE GRANT OPTION FOR SELECT, DELETE ON TABLE t1 FROM Alice;

--login as Alice
USER Alice;

--will not work since GRANT OPTION was revoked for SELECT
GRANT SELECT ON TABLE t1 to Bob;

--works since INSERT privileges were not revoked
GRANT INSERT ON TABLE t1 to Bob;

--works since only the GRANT OPTION was revoked, not the privilege itself
SELECT * FROM TABLE t1;

--login as root
USER root;

REVOKE SELECT, DELETE ON TABLE FROM Alice;

--login as Alice
USER Alice;

--does not work since the SELECT privilege was revoked
SELECT * FROM TABLE t1;
```

The code will perform a check whenever a user tries to grant privileges to other users to see if it holds the grant options for
those privileges - if it does not, then the command fails and returns an error message.

Altering default privileges will also support the "GRANT OPTION" flags above in the grant/revoke statements.

# Unresolved questions

- What should the system output when a user attempts to grant a privilege it does possess the grant option for? Postgres stops the grant if everything
in the grant statement is invalid. However, if at least one privilege in the grant is valid, the command goes through and applies what the granter has, ignoring
everything else and giving a warning that "not all privileges were successfully granted". This can be a bit confusing since it does not specify what was applied.
Should CockroachDB return an error or should it mimic the behavior of Postgres?
  - Resolved to error out and print the privilege that it lacks grant options for like CockroachDB already does for granting privileges that it does not possess.
