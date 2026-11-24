// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestWithSavepoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, rawDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	ctx := context.Background()
	sqlDB := sqlutils.MakeSQLRunner(rawDB)
	sqlDB.Exec(t, "CREATE TABLE test (id STRING PRIMARY KEY, value STRING)")

	require.NoError(t, srv.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		err := withSavepoint(ctx, txn, func() error {
			_, err := srv.InternalExecutor().(*sql.InternalExecutor).ExecEx(
				ctx,
				"test-insert",
				txn,
				sessiondata.NodeUserSessionDataOverride,
				"INSERT INTO defaultdb.test VALUES ('ok', 'is-persisted')",
			)
			return err
		})
		require.NoError(t, err)

		err = withSavepoint(ctx, txn, func() error {
			_, err := srv.InternalExecutor().(*sql.InternalExecutor).ExecEx(
				ctx,
				"test-insert",
				txn,
				sessiondata.NodeUserSessionDataOverride,
				"INSERT INTO defaultdb.test VALUES ('fails', 'is-rolled-back')",
			)
			require.NoError(t, err)
			// NOTE: the query above is okay, which means it wrote things to KV,
			// but we're going to return an error which rolls back the
			// savepoint.
			return errors.New("something to rollback")
		})
		require.ErrorContains(t, err, "something to rollback")

		return nil
	}))

	sqlDB.CheckQueryResults(t, "SELECT id, value FROM test", [][]string{
		{"ok", "is-persisted"},
	})
}
