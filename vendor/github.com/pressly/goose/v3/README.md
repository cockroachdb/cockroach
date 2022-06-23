# goose [![Goose CI](https://github.com/pressly/goose/actions/workflows/ci.yml/badge.svg)](https://github.com/pressly/goose/actions/workflows/ci.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/pressly/goose/v3.svg)](https://pkg.go.dev/github.com/pressly/goose/v3)

<p align="center">
  <img src="assets/goose_logo.png" width="125"">
</p>

Goose is a database migration tool. Manage your database schema by creating incremental SQL changes or Go functions.

Starting with [v3.0.0](https://github.com/pressly/goose/releases/tag/v3.0.0) this project adds Go module support, but maintains backwards compataibility with older `v2.x.y` tags.

Goose supports [embedding SQL migrations](#embedded-sql-migrations), which means you'll need go1.16 and up. If using go1.15 or lower, then pin [v3.0.1](https://github.com/pressly/goose/releases/tag/v3.0.1).

### Goals of this fork

`github.com/pressly/goose` is a fork of `bitbucket.org/liamstask/goose` with the following changes:
- No config files
- [Default goose binary](./cmd/goose/main.go) can migrate SQL files only
- Go migrations:
    - We don't `go build` Go migrations functions on-the-fly
      from within the goose binary
    - Instead, we let you
      [create your own custom goose binary](examples/go-migrations),
      register your Go migration functions explicitly and run complex
      migrations with your own `*sql.DB` connection
    - Go migration functions let you run your code within
      an SQL transaction, if you use the `*sql.Tx` argument
- The goose pkg is decoupled from the binary:
    - goose pkg doesn't register any SQL drivers anymore,
      thus no driver `panic()` conflict within your codebase!
    - goose pkg doesn't have any vendor dependencies anymore
- We use timestamped migrations by default but recommend a hybrid approach of using timestamps in the development process and sequential versions in production.
- Supports missing (out-of-order) migrations with the `-allow-missing` flag, or if using as a library supply the functional option `goose.WithAllowMissing()` to Up, UpTo or UpByOne.
- Supports applying ad-hoc migrations without tracking them in the schema table. Useful for seeding a database after migrations have been applied. Use `-no-versioning` flag or the functional option `goose.WithNoVersioning()`.

# Install

    $ go install github.com/pressly/goose/v3/cmd/goose@latest

This will install the `goose` binary to your `$GOPATH/bin` directory.

For a lite version of the binary without DB connection dependent commands, use the exclusive build tags:

    $ go build -tags='no_postgres no_mysql no_sqlite3' -o goose ./cmd/goose

For macOS users `goose` is available as a [Homebrew Formulae](https://formulae.brew.sh/formula/goose#default):

    $ brew install goose

See the docs for more [installation instructions](https://pressly.github.io/goose/installation/).

# Usage

```
Usage: goose [OPTIONS] DRIVER DBSTRING COMMAND

Drivers:
    postgres
    mysql
    sqlite3
    mssql
    redshift
    tidb
    clickhouse

Examples:
    goose sqlite3 ./foo.db status
    goose sqlite3 ./foo.db create init sql
    goose sqlite3 ./foo.db create add_some_column sql
    goose sqlite3 ./foo.db create fetch_user_data go
    goose sqlite3 ./foo.db up

    goose postgres "user=postgres password=postgres dbname=postgres sslmode=disable" status
    goose mysql "user:password@/dbname?parseTime=true" status
    goose redshift "postgres://user:password@qwerty.us-east-1.redshift.amazonaws.com:5439/db" status
    goose tidb "user:password@/dbname?parseTime=true" status
    goose mssql "sqlserver://user:password@dbname:1433?database=master" status

Options:

  -allow-missing
    	applies missing (out-of-order) migrations
  -certfile string
    	file path to root CA's certificates in pem format (only support on mysql)
  -dir string
    	directory with migration files (default ".")
  -h	print help
  -no-versioning
    	apply migration commands with no versioning, in file order, from directory pointed to
  -s	use sequential numbering for new migrations
  -ssl-cert string
    	file path to SSL certificates in pem format (only support on mysql)
  -ssl-key string
    	file path to SSL key in pem format (only support on mysql)
  -table string
    	migrations table name (default "goose_db_version")
  -v	enable verbose mode
  -version
    	print version

Commands:
    up                   Migrate the DB to the most recent version available
    up-by-one            Migrate the DB up by 1
    up-to VERSION        Migrate the DB to a specific VERSION
    down                 Roll back the version by 1
    down-to VERSION      Roll back to a specific VERSION
    redo                 Re-run the latest migration
    reset                Roll back all migrations
    status               Dump the migration status for the current DB
    version              Print the current version of the database
    create NAME [sql|go] Creates new migration file with the current timestamp
    fix                  Apply sequential ordering to migrations
```

## create

Create a new SQL migration.

    $ goose create add_some_column sql
    $ Created new file: 20170506082420_add_some_column.sql

Edit the newly created file to define the behavior of your migration.

You can also create a Go migration, if you then invoke it with [your own goose binary](#go-migrations):

    $ goose create fetch_user_data go
    $ Created new file: 20170506082421_fetch_user_data.go

## up

Apply all available migrations.

    $ goose up
    $ OK    001_basics.sql
    $ OK    002_next.sql
    $ OK    003_and_again.go

## up-to

Migrate up to a specific version.

    $ goose up-to 20170506082420
    $ OK    20170506082420_create_table.sql

## up-by-one

Migrate up a single migration from the current version

    $ goose up-by-one
    $ OK    20170614145246_change_type.sql

## down

Roll back a single migration from the current version.

    $ goose down
    $ OK    003_and_again.go

## down-to

Roll back migrations to a specific version.

    $ goose down-to 20170506082527
    $ OK    20170506082527_alter_column.sql

## redo

Roll back the most recently applied migration, then run it again.

    $ goose redo
    $ OK    003_and_again.go
    $ OK    003_and_again.go

## status

Print the status of all migrations:

    $ goose status
    $   Applied At                  Migration
    $   =======================================
    $   Sun Jan  6 11:25:03 2013 -- 001_basics.sql
    $   Sun Jan  6 11:25:03 2013 -- 002_next.sql
    $   Pending                  -- 003_and_again.go

Note: for MySQL [parseTime flag](https://github.com/go-sql-driver/mysql#parsetime) must be enabled.

Note: for MySQL [`multiStatements`](https://dev.mysql.com/doc/internals/en/multi-statement.html) must be enabled. This is required when writing multiple queries separated by ';' characters in a single sql file.

## version

Print the current version of the database:

    $ goose version
    $ goose: version 002

# Migrations

goose supports migrations written in SQL or in Go.

## SQL Migrations

A sample SQL migration looks like:

```sql
-- +goose Up
CREATE TABLE post (
    id int NOT NULL,
    title text,
    body text,
    PRIMARY KEY(id)
);

-- +goose Down
DROP TABLE post;
```

Notice the annotations in the comments. Any statements following `-- +goose Up` will be executed as part of a forward migration, and any statements following `-- +goose Down` will be executed as part of a rollback.

By default, all migrations are run within a transaction. Some statements like `CREATE DATABASE`, however, cannot be run within a transaction. You may optionally add `-- +goose NO TRANSACTION` to the top of your migration
file in order to skip transactions within that specific migration file. Both Up and Down migrations within this file will be run without transactions.

By default, SQL statements are delimited by semicolons - in fact, query statements must end with a semicolon to be properly recognized by goose.

More complex statements (PL/pgSQL) that have semicolons within them must be annotated with `-- +goose StatementBegin` and `-- +goose StatementEnd` to be properly recognized. For example:

```sql
-- +goose Up
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION histories_partition_creation( DATE, DATE )
returns void AS $$
DECLARE
  create_query text;
BEGIN
  FOR create_query IN SELECT
      'CREATE TABLE IF NOT EXISTS histories_'
      || TO_CHAR( d, 'YYYY_MM' )
      || ' ( CHECK( created_at >= timestamp '''
      || TO_CHAR( d, 'YYYY-MM-DD 00:00:00' )
      || ''' AND created_at < timestamp '''
      || TO_CHAR( d + INTERVAL '1 month', 'YYYY-MM-DD 00:00:00' )
      || ''' ) ) inherits ( histories );'
    FROM generate_series( $1, $2, '1 month' ) AS d
  LOOP
    EXECUTE create_query;
  END LOOP;  -- LOOP END
END;         -- FUNCTION END
$$
language plpgsql;
-- +goose StatementEnd
```

## Embedded sql migrations
Go 1.16 introduced new feature: [compile-time embedding](https://pkg.go.dev/embed/) files into binary and
corresponding [filesystem abstraction](https://pkg.go.dev/io/fs/).

This feature can be used only for applying existing migrations. Modifying operations such as
`fix` and `create` will continue to operate on OS filesystem even if using embedded files. This is expected
behaviour because `io/fs` interfaces allows read-only access.

Example usage (assuming sql migrations placed in `migrations` directory):
```go
package main

import (
    "database/sql"
    "embed"

    "github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var embedMigrations embed.FS

func main() {
    var db *sql.DB
    // setup database

    goose.SetBaseFS(embedMigrations)

    if err := goose.Up(db, "migrations"); err != nil {
        panic(err)
    }

    // run app
}
```

Note that we pass `"migrations"` as directory argument in `Up` because embedding saves directory structure.

## Go Migrations

1. Create your own goose binary, see [example](./examples/go-migrations)
2. Import `github.com/pressly/goose`
3. Register your migration functions
4. Run goose command, ie. `goose.Up(db *sql.DB, dir string)`

A [sample Go migration 00002_users_add_email.go file](./examples/go-migrations/00002_rename_root.go) looks like:

```go
package migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(Up, Down)
}

func Up(tx *sql.Tx) error {
	_, err := tx.Exec("UPDATE users SET username='admin' WHERE username='root';")
	if err != nil {
		return err
	}
	return nil
}

func Down(tx *sql.Tx) error {
	_, err := tx.Exec("UPDATE users SET username='root' WHERE username='admin';")
	if err != nil {
		return err
	}
	return nil
}
```

# Development

This can be used to build local `goose` binaries without having the latest Go version installed locally.

```bash
DOCKER_BUILDKIT=1  docker build -f Dockerfile.local --output bin .
```

# Hybrid Versioning
Please, read the [versioning problem](https://github.com/pressly/goose/issues/63#issuecomment-428681694) first.

By default, if you attempt to apply missing (out-of-order) migrations `goose` will raise an error. However, If you want to apply these missing migrations pass goose the `-allow-missing` flag, or if using as a library supply the functional option `goose.WithAllowMissing()` to Up, UpTo or UpByOne.

However, we strongly recommend adopting a hybrid versioning approach, using both timestamps and sequential numbers. Migrations created during the development process are timestamped and sequential versions are ran on production. We believe this method will prevent the problem of conflicting versions when writing software in a team environment.

To help you adopt this approach, `create` will use the current timestamp as the migration version. When you're ready to deploy your migrations in a production environment, we also provide a helpful `fix` command to convert your migrations into sequential order, while preserving the timestamp ordering. We recommend running `fix` in the CI pipeline, and only when the migrations are ready for production.

## License

Licensed under [MIT License](./LICENSE)
