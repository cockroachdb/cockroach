// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestNewTableConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	codec := s.Codec()

	runner := sqlutils.MakeSQLRunner(conn)
	runner.Exec(t, "SET experimental_enable_unique_without_index_constraints = true")

	testCases := []struct {
		name              string
		createStmt        string
		tableName         string
		primaryKeyColumns []int32
		wantUniqueColumns [][]int32
	}{
		{
			name: "single column primary key, single column unique constraint",
			createStmt: `
				CREATE TABLE test1 (
					a INT PRIMARY KEY,
					b INT UNIQUE,
					c INT
				)
			`,
			tableName:         "test1",
			primaryKeyColumns: []int32{0},
			wantUniqueColumns: [][]int32{
				{1}, // unique "b" at index 1
			},
		},
		{
			name: "multi-column primary key, multi-column unique constraint",
			createStmt: `
				CREATE TABLE test2 (
					a INT,
					b INT,
					c INT,
					d INT,
					PRIMARY KEY (a, b),
					UNIQUE (c, d)
				)
			`,
			tableName:         "test2",
			primaryKeyColumns: []int32{0, 1},
			wantUniqueColumns: [][]int32{
				{2, 3}, // unique (c,d)
			},
		},
		{
			name: "multiple unique constraints",
			createStmt: `
				CREATE TABLE test3 (
					id INT PRIMARY KEY,
					email STRING UNIQUE,
					username STRING UNIQUE,
					data STRING
				)
			`,
			tableName:         "test3",
			primaryKeyColumns: []int32{0},
			wantUniqueColumns: [][]int32{
				{1}, // unique "email"
				{2}, // unique "username"
			},
		},
		{
			name: "unique without index constraint",
			createStmt: `
				CREATE TABLE test_uwi (
					id INT PRIMARY KEY,
					a INT,
					b INT,
					UNIQUE WITHOUT INDEX (a, b)
				)
			`,
			tableName:         "test_uwi",
			primaryKeyColumns: []int32{0},
			wantUniqueColumns: [][]int32{
				{1, 2}, // unique without index (a, b)
			},
		},
	}

	// mixins are tracked across all test cases because we expect them to be
	// globally unique, not just unique within a single table.
	mixins := map[uint64]bool{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runner.Exec(t, tc.createStmt)
			tableDesc := desctestutils.TestingGetTableDescriptor(
				kvDB,
				codec,
				"defaultdb",
				"public",
				tc.tableName,
			)

			got, err := newTableConstraints(&eval.Context{}, tableDesc)
			require.NoError(t, err)

			requireUnique := func(mixin uint64) {
				require.NotContains(t, mixins, mixin, "mixin %d is not unique", mixin)
				mixins[mixin] = true
			}

			require.Equal(t, tc.primaryKeyColumns, got.PrimaryKeyConstraint.columns, "primary key columns do not match")
			requireUnique(got.PrimaryKeyConstraint.mixin)

			for i, uc := range got.UniqueConstraints {
				require.Equal(t, tc.wantUniqueColumns[i], uc.columns, "columns for constraint %d do not match", i)
				requireUnique(uc.mixin)
			}
		})
	}
}

func TestDependsOn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	codec := s.Codec()

	runner := sqlutils.MakeSQLRunner(conn)

	runner.Exec(t, `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			unique_value INT UNIQUE
		)
	`)

	tableDesc := desctestutils.TestingGetTableDescriptor(
		kvDB, codec, "defaultdb", "public", "test_table",
	)
	constraints, err := newTableConstraints(&eval.Context{}, tableDesc)
	require.NoError(t, err)

	desc := cdctest.GetHydratedTableDescriptor(
		t, s.ExecutorConfig(), "test_table",
	)
	eb := ldrdecoder.NewTestEventBuilder(t, desc.TableDesc())
	txnTime := s.Clock().Now()

	decoder, err := ldrdecoder.NewTxnDecoder(
		ctx, s.InternalDB().(descs.DB), s.ClusterSettings(),
		[]ldrdecoder.TableMapping{
			{SourceDescriptor: desc, DestID: desc.GetID()},
		},
	)
	require.NoError(t, err)

	row := func(id int, val int) tree.Datums {
		return tree.Datums{tree.NewDInt(tree.DInt(id)), tree.NewDInt(tree.DInt(val))}
	}

	// decodeRow decodes a single event into a DecodedRow via the TxnDecoder.
	decodeRow := func(event streampb.StreamEvent_KV) ldrdecoder.DecodedRow {
		t.Helper()
		txn, err := decoder.DecodeTxn(ctx, []streampb.StreamEvent_KV{event})
		require.NoError(t, err)
		require.Len(t, txn.WriteSet, 1)
		return txn.WriteSet[0]
	}

	testCases := []struct {
		name     string
		eventA   streampb.StreamEvent_KV
		eventB   streampb.StreamEvent_KV
		aBeforeB bool // true if A must come before B
		bBeforeA bool // true if B must come before A
	}{
		{
			name:     "UPDATE-UPDATE: B creates 200, A consumes 200",
			eventA:   eb.UpdateEvent(txnTime, row(1, 300), row(1, 200)), // UPDATE: 200 -> 300
			eventB:   eb.UpdateEvent(txnTime, row(2, 200), row(2, 100)), // UPDATE: 100 -> 200
			aBeforeB: true,
			bBeforeA: false,
		},
		{
			name:     "UPDATE-UPDATE: A creates 200, B consumes 200",
			eventA:   eb.UpdateEvent(txnTime, row(1, 200), row(1, 100)), // UPDATE: 100 -> 200
			eventB:   eb.UpdateEvent(txnTime, row(2, 300), row(2, 200)), // UPDATE: 200 -> 300
			aBeforeB: false,
			bBeforeA: true,
		},
		{
			name:     "INSERT-DELETE: DELETE frees 100, INSERT uses 100",
			eventA:   eb.InsertEvent(txnTime, row(1, 100)), // INSERT: creates 100
			eventB:   eb.DeleteEvent(txnTime, row(2, 100)), // DELETE: frees 100
			aBeforeB: false,
			bBeforeA: true, // delete must come before insert
		},
		{
			name:     "UPDATE-UPDATE loop",
			eventA:   eb.UpdateEvent(txnTime, row(1, 300), row(1, 200)), // UPDATE: 200 -> 300
			eventB:   eb.UpdateEvent(txnTime, row(2, 200), row(2, 300)), // UPDATE: 300 -> 200
			aBeforeB: true,
			bBeforeA: true, // circular dependency
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rowA := decodeRow(tc.eventA)
			rowB := decodeRow(tc.eventB)

			// Check if A depends on B (B must come before A).
			bBeforeA, err := constraints.DependsOn(ctx, rowA, rowB)
			require.NoError(t, err)
			require.Equal(t, tc.bBeforeA, bBeforeA, "DependsOn(A, B)")

			// Check if B depends on A (A must come before B).
			aBeforeB, err := constraints.DependsOn(ctx, rowB, rowA)
			require.NoError(t, err)
			require.Equal(t, tc.aBeforeB, aBeforeB, "DependsOn(B, A)")
		})
	}
}

func TestAddLock(t *testing.T) {
	locks := []Lock{
		{Hash: 123, Read: true},
		{Hash: 456, Read: true},
		{Hash: 123, Read: false},
		{Hash: 456, Read: true},
	}

	var result []Lock
	for _, lock := range locks {
		result = addLock(result, lock)
	}

	expected := []Lock{
		{Hash: 123, Read: false},
		{Hash: 456, Read: true},
	}

	require.Equal(t, expected, result)
}
