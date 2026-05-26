// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	crdbpgx "github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgxv5"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// TestShowCommitTimestamp exercises the integration of SHOW COMMIT TIMESTAMP
// with cockroach-go and with the extended wire protocol features as used by
// pgx.
func TestShowCommitTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	resetTable := func(t *testing.T) {
		t.Helper()
		tdb.Exec(t, `
DROP TABLE IF EXISTS foo;
CREATE TABLE foo (i INT PRIMARY KEY)`)
	}
	checkResults := func(t *testing.T, commitTimestamps []string, expTS ...int) {
		t.Helper()
		var exp [][]string
		for i, e := range expTS {
			exp = append(exp, []string{fmt.Sprint(i), commitTimestamps[e]})
		}
		const checkQuery = "select i, crdb_internal_mvcc_timestamp from foo order by i asc"
		tdb.CheckQueryResults(t, checkQuery, exp)
	}
	const showCommitTimestamp = "SHOW COMMIT TIMESTAMP"
	t.Run("cockroach-go", func(t *testing.T) {
		resetTable(t)
		var commitTimestamp string
		require.NoError(t, crdb.ExecuteTx(ctx, sqlDB, nil, func(tx *gosql.Tx) error {
			if _, err := tx.Exec("INSERT INTO foo VALUES (0), (1)"); err != nil {
				return err
			}
			if _, err := tx.Exec("INSERT INTO foo VALUES (2)"); err != nil {
				return err
			}
			return tx.QueryRow(showCommitTimestamp).Scan(&commitTimestamp)
		}))
		checkResults(t, []string{commitTimestamp}, 0, 0, 0)
	})

	testutils.RunTrueAndFalse(t, "pgx batch; simple", func(t *testing.T, simple bool) {
		resetTable(t)
		pgURL, cleanup := s.ApplicationLayer().PGUrl(t)
		defer cleanup()
		conf, err := pgx.ParseConfig(pgURL.String())
		require.NoError(t, err)
		conf.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn, err := pgx.ConnectConfig(ctx, conf)
		require.NoError(t, err)
		defer func() { require.NoError(t, conn.Close(ctx)) }()

		var b pgx.Batch
		stmts := []string{
			"INSERT INTO foo VALUES (0)",
			"INSERT INTO foo VALUES (1)",
			"INSERT INTO foo VALUES (2)",
			"BEGIN",
			"INSERT INTO foo VALUES (3)",
			showCommitTimestamp,
			"COMMIT",
			"BEGIN",
			"SAVEPOINT cockroach_restart",
			"INSERT INTO foo VALUES (4)",
			"INSERT INTO foo VALUES (5)",
			"RELEASE cockroach_restart",
			showCommitTimestamp,
			"COMMIT",
			"INSERT INTO foo VALUES (6)",
			"INSERT INTO foo VALUES (7)",
			showCommitTimestamp,
		}
		for _, s := range stmts {
			b.Queue(s)
		}
		res := conn.SendBatch(ctx, &b)
		var commitTimestamps []string
		for _, s := range stmts {
			if s != showCommitTimestamp {
				_, err = res.Exec()
				require.NoError(t, err)
			} else {
				var r string
				require.NoError(t, res.QueryRow().Scan(&r))
				commitTimestamps = append(commitTimestamps, r)
			}
		}
		require.NoError(t, res.Close())
		require.Len(t, commitTimestamps, 3)
		checkResults(t, commitTimestamps, 0, 0, 0, 0, 1, 1, 2, 2)
	})
	testutils.RunTrueAndFalse(t, "pgx with crdb; simple", func(t *testing.T, simple bool) {
		resetTable(t)
		pgURL, cleanup := s.ApplicationLayer().PGUrl(t)
		defer cleanup()
		conf, err := pgx.ParseConfig(pgURL.String())
		require.NoError(t, err)
		conf.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn, err := pgx.ConnectConfig(ctx, conf)
		require.NoError(t, err)
		defer func() { require.NoError(t, conn.Close(ctx)) }()

		{
			_, err := conn.Exec(ctx, "select 1/0")
			require.ErrorContains(t, err, "division by zero")
		}
		{
			_, err := conn.Exec(ctx, showCommitTimestamp)
			pgErr := new(pgconn.PgError)
			require.True(t, errors.As(err, &pgErr))
			require.Equal(t, pgcode.InvalidTransactionState.String(), pgErr.Code)
			require.ErrorContains(t, err, "no previous transaction")
		}
		{
			_, err := conn.Exec(ctx, "insert into foo values (0)")
			require.NoError(t, err)
		}
		var ts string
		{
			require.NoError(t, conn.QueryRow(ctx, showCommitTimestamp).Scan(&ts))
		}
		checkResults(t, []string{ts}, 0)
		var txTs string
		require.NoError(t, crdbpgx.ExecuteTx(ctx, conn, pgx.TxOptions{}, func(tx pgx.Tx) (err error) {
			if _, err = tx.Exec(ctx, "insert into foo values (1), (2)"); err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, "insert into foo values (3)"); err != nil {
				return err
			}
			if err = tx.QueryRow(ctx, showCommitTimestamp).Scan(&txTs); err != nil {
				return err
			}
			return nil
		}))

		checkResults(t, []string{ts, txTs}, 0, 1, 1, 1)
	})
}
