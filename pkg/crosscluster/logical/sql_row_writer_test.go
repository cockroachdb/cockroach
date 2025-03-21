// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func makeTestRow(t *testing.T, desc catalog.TableDescriptor, id int64, name string) cdcevent.Row {
	t.Helper()
	datums := rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(id))),
		rowenc.DatumToEncDatum(types.String, tree.NewDString(name)),
		rowenc.DatumToEncDatum(types.String, tree.DNull),
	}
	return cdcevent.TestingMakeEventRow(desc, 0, datums, false)
}

func TestSQLRowWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
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
		return writer.InsertRow(ctx, txn, insertRow)
	}))
	require.Equal(t,
		[][]string{{"1", "test", "NULL"}},
		sqlDB.QueryStr(t, "SELECT id, name, is_always_null FROM test_table WHERE id = 1"))

	// Test UpdateRow
	updateRow := makeTestRow(t, desc, 1, "updated")
	require.NoError(t, internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return writer.UpdateRow(ctx, txn, insertRow, updateRow)
	}))
	require.Equal(t,
		[][]string{{"1", "updated", "NULL"}},
		sqlDB.QueryStr(t, "SELECT id, name, is_always_null FROM test_table WHERE id = 1"))

	// Test UpdateRow with stale previous value
	staleRow := makeTestRow(t, desc, 1, "test") // Using old value
	err = internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return writer.UpdateRow(ctx, txn, staleRow, updateRow)
	})
	require.ErrorIs(t, err, errStalePreviousValue)

	// Test DeleteRow
	require.NoError(t, internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return writer.DeleteRow(ctx, txn, updateRow)
	}))
	require.Equal(t,
		[][]string{},
		sqlDB.QueryStr(t, "SELECT id, name, is_always_null FROM test_table WHERE id = 1"))

	// Test DeleteRow with stale value
	err = internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return writer.DeleteRow(ctx, txn, staleRow)
	})
	require.ErrorIs(t, err, errStalePreviousValue)
}
