// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlwriter

import (
	"context"
	"slices"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionmutator"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestSQLRowReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	sqlRunner.Exec(t, "CREATE TABLE defaultdb.tab (pk INT PRIMARY KEY, payload STRING)")

	session := newInternalSession(t, s)
	defer session.Close(ctx)

	// Insert one row with an origin timetamp
	insertStmt, err := parser.ParseOne("INSERT INTO defaultdb.tab (pk, payload) VALUES ($1, $2)")
	require.NoError(t, err)
	prepared, err := session.Prepare(ctx, "insert", insertStmt, []*types.T{types.Int, types.String})
	require.NoError(t, err)
	require.NoError(t, session.ModifySession(ctx, func(m sessionmutator.SessionDataMutator) {
		m.Data.OriginTimestampForLogicalDataReplication = s.Clock().Now()
	}))
	_, err = session.ExecutePrepared(ctx, prepared, tree.Datums{tree.NewDInt(10), tree.NewDString("remote")})
	require.NoError(t, err)

	// Insert one row without an origin timestamp
	sqlRunner.Exec(t, "INSERT INTO defaultdb.tab (pk, payload) VALUES (20, 'local')")

	// Create sqlRowReader for source table
	desc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "defaultdb", "tab")
	srcReader, err := NewRowReader(ctx, desc, session)
	require.NoError(t, err)

	db := s.InternalDB().(isql.DB)
	readRows := func(t *testing.T, db isql.DB, rows []tree.Datums, reader RowReader) map[int]PriorRow {
		var result map[int]PriorRow
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			result, err = reader.ReadRows(ctx, rows)
			require.NoError(t, err)
			return err
		}))
		return result
	}

	readRowsSql := func(t *testing.T, db *sqlutils.SQLRunner, primaryKeys []int) map[int]PriorRow {
		sqlRows := db.Query(t, `
			SELECT pk,
				   payload,
				   COALESCE(crdb_internal_origin_timestamp, crdb_internal_mvcc_timestamp) as timestamp,
				   crdb_internal_origin_timestamp IS NULL as is_local
			FROM tab
			WHERE pk = ANY($1::int[])
			ORDER BY pk`, pq.Array(primaryKeys))

		result := make(map[int]PriorRow, len(primaryKeys))

		for sqlRows.Next() {
			var pk int
			var payload string
			var mvccTS string
			var isLocal bool
			require.NoError(t, sqlRows.Scan(&pk, &payload, &mvccTS, &isLocal))

			mvccDec, _, err := apd.NewFromString(mvccTS)
			require.NoError(t, err)

			logicalTimestamp, err := hlc.DecimalToHLC(mvccDec)
			require.NoError(t, err)

			result[slices.Index(primaryKeys, pk)] = PriorRow{
				Row:              []tree.Datum{tree.NewDInt(tree.DInt(pk)), tree.NewDString(payload)},
				LogicalTimestamp: logicalTimestamp,
				IsLocal:          isLocal,
			}
		}

		return result
	}

	testRows := []tree.Datums{
		{tree.NewDInt(10), tree.NewDString("one")},   // Row with origin timestamp
		{tree.NewDInt(20), tree.NewDString("two")},   // Row without origin timestamp
		{tree.NewDInt(30), tree.NewDString("three")}, // Does not exist
	}
	primaryKeys := []int{10, 20, 30}

	require.Equal(t,
		readRows(t, db, testRows, srcReader),
		readRowsSql(t, sqlRunner, primaryKeys),
		"reading source did not yield expected rows")
}

func TestSQLRowReaderWithArrayColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	// Create tables with array column
	createStmt := `CREATE TABLE tab_array (pk int[] primary key, value int)`
	runner.Exec(t, createStmt)

	// Insert test data
	runner.Exec(t, "INSERT INTO tab_array VALUES (ARRAY['1', '2'], 10), (ARRAY['1'], 20)")

	// Create sqlRowReader for source table
	desc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "defaultdb", "tab_array")
	session := newInternalSession(t, s)
	defer session.Close(ctx)
	reader, err := NewRowReader(ctx, desc, session)
	require.NoError(t, err)

	db := s.InternalDB().(isql.DB)

	testRows := []tree.Datums{
		{
			tree.NewDArrayFromDatums(types.Int, tree.Datums{tree.NewDInt(1), tree.NewDInt(2)}),
			tree.NewDInt(0),
		},
		{
			tree.NewDArrayFromDatums(types.Int, tree.Datums{tree.NewDInt(1)}),
			tree.NewDInt(0),
		},
	}

	var result map[int]PriorRow
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		result, err = reader.ReadRows(ctx, testRows)
		return err
	}))

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result, 2)
	require.Equal(t, result[0].Row[1], tree.NewDInt(10))
	require.Equal(t, result[1].Row[1], tree.NewDInt(20))
}
