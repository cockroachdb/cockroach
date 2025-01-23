// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestInsertFastPathExtendedProtocol verifies that the 1PC "insert fast path"
// optimization is applied when doing a simple INSERT with a prepared statement.
func TestInsertFastPathExtendedProtocol(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	_, err := db.Exec(`CREATE TABLE fast_path_test(val int);`)
	require.NoError(t, err)

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "SET tracing = 'on'")
	require.NoError(t, err)
	// Use placeholders to force usage of extended protocol.
	_, err = conn.ExecContext(ctx, "INSERT INTO fast_path_test VALUES($1)", 1)
	require.NoError(t, err)

	fastPathEnabled := false
	rows, err := conn.QueryContext(ctx, "SELECT message, operation FROM [SHOW TRACE FOR SESSION]")
	require.NoError(t, err)
	for rows.Next() {
		var msg, operation string
		err = rows.Scan(&msg, &operation)
		require.NoError(t, err)
		if msg == "autocommit enabled" && operation == "count" {
			fastPathEnabled = true
		}
	}
	require.NoError(t, rows.Err())
	require.True(t, fastPathEnabled)
	_, err = conn.ExecContext(ctx, "SET tracing = 'off'")
	require.NoError(t, err)
	err = conn.Close()
	require.NoError(t, err)

	// Verify that the insert committed successfully.
	var c int
	err = db.QueryRow("SELECT count(*) FROM fast_path_test").Scan(&c)
	require.NoError(t, err)
	require.Equal(t, 1, c, "expected 1 row, got %d", c)
}

// TestInsertFastPathDisableDDLExtendedProtocol verifies that the 1PC "insert fast path"
// optimization is disabled when doing a simple INSERT with a prepared statement,
// executed in the same transaction as a DDL.
func TestInsertFastPathDisableDDLExtendedProtocol(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	_, err := db.Exec(`CREATE TABLE fast_path_test(val int, j int);`)
	require.NoError(t, err)

	// Use pgx so that we can introspect error codes returned from cockroach.
	pgURL, cleanup := s.PGUrl(t)
	defer cleanup()
	conf, err := pgx.ParseConfig(pgURL.String())
	require.NoError(t, err)
	conn, err := pgx.ConnectConfig(ctx, conf)
	require.NoError(t, err)

	require.NoError(t, err)
	_, err = conn.Exec(ctx, "SET tracing = 'on'")
	require.NoError(t, err)
	var batch pgx.Batch
	batch.Queue("CREATE INDEX idx ON fast_path_test(val);")
	batch.Queue("INSERT INTO fast_path_test VALUES($1, $2);", 1, 2)
	br := conn.SendBatch(ctx, &batch)
	_, err = br.Exec()
	require.NoError(t, err)
	require.NoError(t, br.Close())

	fastPathEnabled := false
	rows, err := conn.Query(ctx, "SELECT message, operation FROM [SHOW TRACE FOR SESSION]")
	require.NoError(t, err)
	for rows.Next() {
		var msg, operation string
		err = rows.Scan(&msg, &operation)
		require.NoError(t, err)
		if msg == "autocommit enabled" && operation == "count" {
			fastPathEnabled = true
		}
	}
	require.NoError(t, rows.Err())
	require.False(t, fastPathEnabled)
	_, err = conn.Exec(ctx, "SET tracing = 'off'")
	require.NoError(t, err)
	err = conn.Close(ctx)
	require.NoError(t, err)

	// Verify that the insert committed successfully.
	var c int
	err = db.QueryRow("SELECT count(*) FROM fast_path_test").Scan(&c)
	require.NoError(t, err)
	require.Equal(t, 1, c, "expected 1 row, got %d", c)
	// Verify that a job was created for the create index.
	err = db.QueryRow("SELECT count(*) FROM  [SHOW JOBS] WHERE job_type ='SCHEMA CHANGE' AND description LIKE 'CREATE INDEX idx%' LIMIT 1").Scan(&c)
	require.NoError(t, err)
	require.Equal(t, 1, c, "expected 1 row, got %d", c)
}

// TestErrorDuringExtendedProtocolCommit verifies that the results are correct
// when there's an error during the COMMIT of an extended protocol statement
// in an implicit transaction.
func TestErrorDuringExtendedProtocolCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var shouldErrorOnAutoCommit atomic.Bool
	var traceID atomic.Uint64
	var params base.TestServerArgs
	params.Knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
		DisableAutoCommitDuringExec: true,
		BeforeExecute: func(ctx context.Context, stmt string, descriptors *descs.Collection) {
			if strings.Contains(stmt, "SELECT 'cat'") {
				shouldErrorOnAutoCommit.Store(true)
				traceID.Store(uint64(tracing.SpanFromContext(ctx).TraceID()))
			}
		},
		BeforeAutoCommit: func(ctx context.Context, stmt string) error {
			if shouldErrorOnAutoCommit.Load() {
				// Only inject the error if we're in the same trace as the one we
				// saw when executing our test query. This is so we know that this
				// autocommit corresponds to our test qyery rather than an internal
				// query.
				if traceID.Load() == uint64(tracing.SpanFromContext(ctx).TraceID()) {
					shouldErrorOnAutoCommit.Store(false)
					return errors.New("injected error")
				}
			}
			return nil
		},
	}

	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	// This forces a trace span to have been set up in the BeforeExecute
	// interceptor above.
	s.Tracer().SetActiveSpansRegistryEnabled(true)

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	var i int
	var str string
	// Use placeholders to force usage of extended protocol.
	err = conn.QueryRowContext(ctx, "SELECT 'cat', $1::int8", 1).Scan(&str, &i)
	require.EqualError(t, err, "pq: injected error")
	// Check that the error was handled correctly, and another statement
	// doesn't confuse the server.
	err = conn.QueryRowContext(ctx, "SELECT 'dog', $1::int8", 2).Scan(&str, &i)
	require.NoError(t, err)
	require.Equal(t, 2, i)
}
