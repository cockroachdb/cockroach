- Feature Name: SQL privileges
- Status: completed
- Start Date: 2015-08-10
- RFC PR: [#2054](https://github.com/cockroachdb/cockroach/pull/2054)
- Cockroach Issue: [#2005](https://github.com/cockroachdb/cockroach/issues/2005)

# Summary

Introduce SQL privileges for databases and tables. The aim is to provide a privilege
framework familiar to users of popular SQL servers while embracing simplicity.

We intentionally do not address privileges for other granularities (columns or rows),
or unsupported features (views, temporary tables, procedures, etc...).

# Motivation

The SQL interface needs permissioning on database and tables.
For ease of migration and understanding, we wish to use common SQL privileges
albeit adapted for our use, or simplified.

We use the [postgres](http://www.postgresql.org/docs/9.4/static/sql-grant.html) and
[mysql](https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html) privileges
as references.

# Detailed design

## Privilege logic

### Postgres

* all objects have owners. Owners default to all privileges on their objects.
* only the granter of a privilege can remove it. (eg: user A grants `SELECT` to B, only
  A can remove `SELECT` from B. If another role granted `SELECT` as well, it stays).
* databases share no data between them except for user tables. As such they are closer
  to our databases then the postgres schema.
* `SUPERUSER` is a user attribute. It allows everything.
* postgres has a `PUBLIC` schema granting liberal privileges to all users.
* privileges can be displayed using the `\du` command in psql.
* `ALL` is translated to the list of all privileges.

### Mysql

* privileges are stored in a global table. It is thus possible to grant privileges
  on objects that do not yet exist (eg: `GRANT ALL on db.* TO user1`).
* privileges can be displayed using `SHOW GRANTS [FOR x]`.
* `ALL` is a shortcut for setting and display.

### Cockroach

* the `root` user has default `ALL` privileges on all new databases. It is also the only
  user allowed to create databases.
* `root` user privileges cannot be removed. (This may change and be implicit `ALL` for `root`).
* table privileges are inherited from database privileges at creation time. After that,
  they are allowed to diverge.
* `GRANT` is specified as a privilege. It may make more sense to use `WITH GRANT OPTION`
  on the `GRANT` statement (similar to postgres and mysql).
* privileges can be displayed using `SHOW GRANTS [FOR x]`.
* `ALL` is a shortcut for setting and display.

## Supported privileges

| Privilege | Level     |
|-----------|-----------|
| ALL       | DB, Table |
| CREATE    | DB        |
| DROP      | DB, Table |
| GRANT     | DB, Table |
| SELECT    | Table     |
| INSERT    | Table     |
| DELETE    | Table     |
| UPDATE    | Table     |

## Statement comparison

*Important note*: there is a lot of simplification in the following summaries.
For more details, it is recommended to follow the links.

`CREATE DATABASE`:
* [mysql](https://dev.mysql.com/doc/refman/5.7/en/create-database.html):
	requires mysqladmin command or `CREATE` privilege on the database.
* [postgres](http://www.postgresql.org/docs/9.4/static/sql-createdatabase.html):
	requires `SUPERUSER` or `CREATEDB` user option.
* cockroach: requires `root` user.

`CREATE TABLE`:
* [mysql](https://dev.mysql.com/doc/refman/5.7/en/create-table.html):
	`CREATE` privilege on table.
* [postgres](http://www.postgresql.org/docs/9.4/static/sql-createtable.html):
	`CREATE` privilege in schema.
* cockroach: `CREATE` privilege on database.

`DROP DATABASE`:
* [mysql](https://dev.mysql.com/doc/refman/5.7/en/drop-database.html):
	`DROP` on database.
* [postgres](http://www.postgresql.org/docs/9.4/static/sql-dropdatabase.html):
	database owner only.
* cockroach: `DROP` on database, or `root` user.

`DROP TABLE`:
* [mysql](https://dev.mysql.com/doc/refman/5.7/en/drop-table.html):
	`DROP` on table.
* [postgres](http://www.postgresql.org/docs/9.4/static/sql-droptable.html):
	table or schema owner. or `SUPERUSER`.
* cockroach: `DROP` on table.

`GRANT`:
* [mysql](https://dev.mysql.com/doc/refman/5.7/en/grant.html):
	`GRANT OPTION` and the granted privileges.
* [postgres](http://www.postgresql.org/docs/9.4/static/sql-grant.html):
	`GRANT OPTION` and the granted privileges.
* cockroach: `GRANT` on affected object. Does not require the granted privileges.

`REVOKE`:
* [mysql](https://dev.mysql.com/doc/refman/5.7/en/revoke.html):
	`GRANT OPTION` and the revoked privileges (another invocation has stricter requirements).
* [postgres](http://www.postgresql.org/docs/9.4/static/sql-revoke.html):
	owner, or `GRANT OPTION` on revoked privileges. Complex rules for grantees.
* cockroach: `GRANT` on affected object.

`SELECT`:
* [mysql](https://dev.mysql.com/doc/refman/5.7/en/select.html):
  `SELECT` (doc seems incomplete).
* [postgres](http://www.postgresql.org/docs/9.4/static/sql-select.html):
  `SELECT` if tables are read. Also requires `UPDATE` when using `FOR UPDATE`.
* cockroach: `SELECT` if tables are read.

`INSERT`:
* [mysql](https://dev.mysql.com/doc/refman/5.7/en/insert.html):
  `INSERT`. `UPDATE` or `SELECT` for `ON DUPLICATE KEY UPDATE`.
* [postgres](http://www.postgresql.org/docs/9.4/static/sql-insert.html):
  `INSERT`. `SELECT` if using `RETURNING`.
* cockroach: `INSERT` on table. `SELECT` if using `SELECT` clause on tables.

`DELETE`:
* [mysql](https://dev.mysql.com/doc/refman/5.7/en/delete.html):
	`DELETE` on table. `SELECT` if `WHERE` clause operates on tables.
* [postgres](http://www.postgresql.org/docs/9.4/static/sql-delete.html):
	`DELETE` on table. `SELECT` if `USING` or `WHERE` clauses operate on tables.
* cockroach: `DELETE` on table. `SELECT` if `WHERE` clause operates on tables.

`UPDATE`:
* [mysql](https://dev.mysql.com/doc/refman/5.7/en/update.html):
	`UPDATE`. Also `SELECT` for `WHERE` clause with table.
* [postgres](http://www.postgresql.org/docs/9.4/static/sql-update.html):
	`UPDATE`. Also `SELECT` for `WHERE` clause with table.
* cockroach: `UPDATE`. Also `SELECT` for `WHERE` clause with table.

`TRUNCATE`:
* [mysql](https://dev.mysql.com/doc/refman/5.7/en/truncate-table.html):
	`DROP` on table.
* [postgres](http://www.postgresql.org/docs/9.4/static/sql-truncate.html):
  `TRUNCATE` privilege.
* cockroach: `DROP` on table.

`SHOW`:
* [mysql](https://dev.mysql.com/doc/refman/5.7/en/show.html):
  shows objects the user has privileges on.
* [postgres](http://www.postgresql.org/docs/9.4/static/sql-show.html):
  only `SHOW name` is supported. No privileges required.
* cockroach: no privileges required (we may wish something closer to mysql).

`SET`:
* [mysql](https://dev.mysql.com/doc/refman/5.7/en/set.html):
  some require superuser, some cannot be changed after session start.
* [postgres](http://www.postgresql.org/docs/9.4/static/sql-set.html):
  unknown. Probably no privileges.
* cockroach: session variables can be changed at will. Privileges for
  other types of variables will be revisited.

## Storing privileges

Privileges are stored in the `DatabaseDescriptor` and `TableDescriptor` using
a list sorted by user.
This gives the planner access to privileges after a descriptor lookup.

Future improvements include gossiping descriptors [#1743](https://github.com/cockroachdb/cockroach/pull/1743)
and potentially using a real table.

# Drawbacks

Postgres and mysql both have global user tables. We do not require users
to be in the user config, only to be properly authenticated. This means
that we cannot have user-defined super users.

Like all sql servers, privilege logic is very custom. This proposal closely
resembles the mysql logic modified for the lack of a global privilege table.

A proper use of privileges greatly depends on proper documentation. Both
postgres and mysql do an inadequate job of documenting required privileges
for various operations, sometimes mentioning nothing at all.

The right balance has to be found between simplicity and familiarity.
Simplifying `GRANT` to be a privilege in itself may be counter-intuitive
for those familiar with other sql servers.

# Alternatives

Two possible future features are:
* requirement that all users be in a global table/config.
* global permissions table/config.

# Unresolved questions

Examine privilege logic for other sql servers and sql layers on top of non-transactional
databases.

This is an incomplete list of statements. If approved, this RFC should be turned into a
document and kept up-to-date with implementation.
