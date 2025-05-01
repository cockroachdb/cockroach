// Copyright 2024 The Cockroach Authors.
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

func newInternalSession(t *testing.T, s serverutils.ApplicationLayerInterface) isql.Session {
	session, err := s.InternalDB().(isql.DB).Session(context.Background(), "test_session")
	require.NoError(t, err)
	return session
}

func TestSQLRowWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartSlimServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)

	// Create a test table
	sqlDB.Exec(t, `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			name STRING,
			is_always_null STRING
		)
	`)

	session := newInternalSession(t, s)
	defer session.Close(ctx)

	// Create a row writer
	desc := cdctest.GetHydratedTableDescriptor(t, s.ApplicationLayer().ExecutorConfig(), "test_table")
	writer, err := newSQLRowWriter(ctx, desc, session)
	require.NoError(t, err)

	// Test InsertRow
	insertRow := makeTestRow(t, desc, 1, "test")
	require.NoError(t, writer.InsertRow(ctx, s.Clock().Now(), insertRow))
	require.Equal(t,
		[][]string{{"1", "test", "NULL"}},
		sqlDB.QueryStr(t, "SELECT id, name, is_always_null FROM test_table WHERE id = 1"))

	// Test UpdateRow
	updateRow := makeTestRow(t, desc, 1, "updated")
	require.NoError(t, writer.UpdateRow(ctx, s.Clock().Now(), insertRow, updateRow))
	require.Equal(t,
		[][]string{{"1", "updated", "NULL"}},
		sqlDB.QueryStr(t, "SELECT id, name, is_always_null FROM test_table WHERE id = 1"))

	// Test UpdateRow with stale previous value
	staleRow := makeTestRow(t, desc, 1, "test") // Using old value
	err = writer.UpdateRow(ctx, s.Clock().Now(), staleRow, updateRow)
	require.ErrorIs(t, err, errStalePreviousValue)

	// Test DeleteRow
	require.NoError(t, writer.DeleteRow(ctx, s.Clock().Now(), updateRow))
	require.Equal(t,
		[][]string{},
		sqlDB.QueryStr(t, "SELECT id, name, is_always_null FROM test_table WHERE id = 1"))

	// Test DeleteRow with stale value
	err = writer.DeleteRow(ctx, s.Clock().Now(), staleRow)
	require.ErrorIs(t, err, errStalePreviousValue)
}
