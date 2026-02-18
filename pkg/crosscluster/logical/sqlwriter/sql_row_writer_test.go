// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlwriter

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	ctx := context.Background()
	sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "" /* opName */)
	session, err := NewInternalSession(ctx, s.InternalDB().(isql.DB), sd, s.ClusterSettings())
	require.NoError(t, err)
	return session
}

// hlcToString converts an HLC timestamp to the format used by crdb_internal_origin_timestamp
func hlcToString(ts hlc.Timestamp) string {
	return eval.TimestampToDecimalDatum(ts).String()
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
	writer, err := NewRowWriter(ctx, desc, session)
	require.NoError(t, err)

	// Test InsertRow
	insertRow := makeTestRow(t, desc, 1, "test")
	insertTimestamp := s.Clock().Now()
	require.NoError(t, writer.InsertRow(ctx, insertTimestamp, insertRow))
	require.Equal(t,
		[][]string{{"1", "test", "NULL", hlcToString(insertTimestamp), "1"}},
		sqlDB.QueryStr(t, "SELECT id, name, is_always_null, crdb_internal_origin_timestamp, crdb_internal_origin_id FROM test_table WHERE id = 1"))

	// Test UpdateRow
	updateRow := makeTestRow(t, desc, 1, "updated")
	updateTimestamp := s.Clock().Now()
	require.NoError(t, writer.UpdateRow(ctx, updateTimestamp, insertRow, updateRow))
	require.Equal(t,
		[][]string{{"1", "updated", "NULL", hlcToString(updateTimestamp), "1"}},
		sqlDB.QueryStr(t, "SELECT id, name, is_always_null, crdb_internal_origin_timestamp, crdb_internal_origin_id FROM test_table WHERE id = 1"))

	// Test UpdateRow with stale previous value
	staleRow := makeTestRow(t, desc, 1, "test") // Using old value
	err = writer.UpdateRow(ctx, s.Clock().Now(), staleRow, updateRow)
	require.ErrorIs(t, err, ErrStalePreviousValue)

	// Test DeleteRow - first insert a test row to verify origin data before delete
	insertRow2 := makeTestRow(t, desc, 2, "to_delete")
	insertTimestamp2 := s.Clock().Now()
	require.NoError(t, writer.InsertRow(ctx, insertTimestamp2, insertRow2))

	// Verify the row exists with correct data and origin metadata
	require.Equal(t,
		[][]string{{"2", "to_delete", "NULL", hlcToString(insertTimestamp2), "1"}},
		sqlDB.QueryStr(t, "SELECT id, name, is_always_null, crdb_internal_origin_timestamp, crdb_internal_origin_id FROM test_table WHERE id = 2"))

	// Now delete the row
	require.NoError(t, writer.DeleteRow(ctx, s.Clock().Now(), insertRow2))
	require.Equal(t,
		[][]string{},
		sqlDB.QueryStr(t, "SELECT id, name, is_always_null, crdb_internal_origin_timestamp, crdb_internal_origin_id FROM test_table WHERE id = 2"))

	// Also delete the original row to clean up
	require.NoError(t, writer.DeleteRow(ctx, s.Clock().Now(), updateRow))
	require.Equal(t,
		[][]string{},
		sqlDB.QueryStr(t, "SELECT id, name, is_always_null, crdb_internal_origin_timestamp, crdb_internal_origin_id FROM test_table WHERE id = 1"))

	// Test DeleteRow with stale value
	err = writer.DeleteRow(ctx, s.Clock().Now(), staleRow)
	require.ErrorIs(t, err, ErrStalePreviousValue)

	// Test InsertRow LWW failure against existing row: inserting with an older
	// timestamp than the existing row should return a ConditionFailedError.
	existingRow := makeTestRow(t, desc, 10, "existing")
	newerTimestamp := s.Clock().Now()
	require.NoError(t, writer.InsertRow(ctx, newerTimestamp, existingRow))
	err = session.Txn(ctx, func(ctx context.Context) error {
		return writer.InsertRow(ctx, newerTimestamp.Prev(), makeTestRow(t, desc, 10, "duplicate"))
	})
	var condErr *kvpb.ConditionFailedError
	require.ErrorAs(t, err, &condErr)
	require.True(t, condErr.OriginTimestampOlderThan.IsSet())
}
