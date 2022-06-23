// Package pgxpool is a concurrency-safe connection pool for pgx.
/*
pgxpool implements a nearly identical interface to pgx connections.

Establishing a Connection

The primary way of establishing a connection is with `pgxpool.Connect`.

    pool, err := pgxpool.Connect(context.Background(), os.Getenv("DATABASE_URL"))

The database connection string can be in URL or DSN format. PostgreSQL settings, pgx settings, and pool settings can be
specified here. In addition, a config struct can be created by `ParseConfig` and modified before establishing the
connection with `ConnectConfig`.

    config, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
    if err != nil {
        // ...
    }
    config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
        // do something with every new connection
    }

    pool, err := pgxpool.ConnectConfig(context.Background(), config)
*/
package pgxpool
