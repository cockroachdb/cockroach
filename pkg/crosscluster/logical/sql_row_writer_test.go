// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func makeTestRow(t *testing.T, _ catalog.TableDescriptor, id int64, name string) tree.Datums {
	t.Helper()
	return tree.Datums{
		tree.NewDInt(tree.DInt(id)),
		tree.NewDString(name),
		tree.DNull,
	}
}

func TestSQLRowWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartSlimServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	internalDB := s.InternalDB().(isql.DB)

	// Create a test table
	sqlDB.Exec(t, `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			name STRING,
			is_always_null STRING
		)
	`)

	// Create a row writer
	desc := cdctest.GetHydratedTableDescriptor(t, s.ApplicationLayer().ExecutorConfig(), "test_table")
	writer, err := newSQLRowWriter(desc)
	require.NoError(t, err)

	// Test InsertRow
	insertRow := makeTestRow(t, desc, 1, "test")
	require.NoError(t, internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return writer.InsertRow(ctx, txn, s.Clock().Now(), insertRow)
	}))
	require.Equal(t,
		[][]string{{"1", "test", "NULL"}},
		sqlDB.QueryStr(t, "SELECT id, name, is_always_null FROM test_table WHERE id = 1"))

	// Test UpdateRow
	updateRow := makeTestRow(t, desc, 1, "updated")
	require.NoError(t, internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return writer.UpdateRow(ctx, txn, s.Clock().Now(), insertRow, updateRow)
	}))
	require.Equal(t,
		[][]string{{"1", "updated", "NULL"}},
		sqlDB.QueryStr(t, "SELECT id, name, is_always_null FROM test_table WHERE id = 1"))

	// Test UpdateRow with stale previous value
	staleRow := makeTestRow(t, desc, 1, "test") // Using old value
	err = internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return writer.UpdateRow(ctx, txn, s.Clock().Now(), staleRow, updateRow)
	})
	require.ErrorIs(t, err, errStalePreviousValue)

	// Test DeleteRow
	require.NoError(t, internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return writer.DeleteRow(ctx, txn, s.Clock().Now(), updateRow)
	}))
	require.Equal(t,
		[][]string{},
		sqlDB.QueryStr(t, "SELECT id, name, is_always_null FROM test_table WHERE id = 1"))

	// Test DeleteRow with stale value
	err = internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return writer.DeleteRow(ctx, txn, s.Clock().Now(), staleRow)
	})
	require.ErrorIs(t, err, errStalePreviousValue)
}
