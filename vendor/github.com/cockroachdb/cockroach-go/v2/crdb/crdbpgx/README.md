`crdbpgx` is a wrapper around the logic for issuing SQL transactions which
performs retries (as required by CockroachDB) when using
[`github.com/jackc/pgx`](https://github.com/jackc/pgx) in standalone-library
mode. pgx versions below v4 are not supported.

If you're using pgx just as a driver for the standard `database/sql` package,
use the parent `crdb` package instead.
