// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
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

	testCases := []struct {
		name              string
		createStmt        string
		tableName         string
		wantPK            []int32
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
			wantPK:            []int32{0},     // column "a" at index 0
			wantUniqueColumns: [][]int32{{1}}, // unique "b" at index 1
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
			wantPK:            []int32{0, 1},     // columns "a", "b" at indices 0, 1
			wantUniqueColumns: [][]int32{{2, 3}}, // unique (c,d)
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
			wantPK:            []int32{0},          // column "id" at index 0
			wantUniqueColumns: [][]int32{{1}, {2}}, // unique "email", "username"
		},
	}

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
			want := &tableConstraints{
				evalCtx: got.evalCtx,
			}

			pkMixin, err := tableMixin(tableDesc.GetID())
			require.NoError(t, err)
			want.PrimaryKey = columnSet{
				columns: tc.wantPK,
				mixin:   pkMixin,
			}

			primaryIndex := tableDesc.GetPrimaryIndex()
			idx := 0
			for _, uc := range tableDesc.EnforcedUniqueConstraintsWithIndex() {
				if uc.GetID() != primaryIndex.GetID() {
					ucMixin, err := uniqueIndexMixin(tableDesc.GetID(), uc.GetID())
					require.NoError(t, err)
					want.UniqueConstraints = append(want.UniqueConstraints, columnSet{
						columns: tc.wantUniqueColumns[idx],
						mixin:   ucMixin,
					})
					idx++
				}
			}

			require.Equal(t, want, got)
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

	tableDesc := desctestutils.TestingGetTableDescriptor(kvDB, codec, "defaultdb", "public", "test_table")
	constraints, err := newTableConstraints(&eval.Context{}, tableDesc)
	require.NoError(t, err)

	testCases := []struct {
		name     string
		prevRowA tree.Datums
		rowA     tree.Datums
		prevRowB tree.Datums
		rowB     tree.Datums
		aBeforeB bool // true if A must come before B
		bBeforeA bool // true if B must come before A
	}{
		{
			name:     "UPDATE-UPDATE: B creates 200, A consumes 200",
			prevRowA: tree.Datums{tree.NewDInt(1), tree.NewDInt(200)},
			rowA:     tree.Datums{tree.NewDInt(1), tree.NewDInt(300)}, // UPDATE: 200 -> 300
			prevRowB: tree.Datums{tree.NewDInt(2), tree.NewDInt(100)},
			rowB:     tree.Datums{tree.NewDInt(2), tree.NewDInt(200)}, // UPDATE: 100 -> 200
			aBeforeB: true,
			bBeforeA: false,
		},
		{
			name:     "UPDATE-UPDATE: A creates 200, B consumes 200",
			prevRowA: tree.Datums{tree.NewDInt(1), tree.NewDInt(100)},
			rowA:     tree.Datums{tree.NewDInt(1), tree.NewDInt(200)}, // UPDATE: 100 -> 200
			prevRowB: tree.Datums{tree.NewDInt(2), tree.NewDInt(200)},
			rowB:     tree.Datums{tree.NewDInt(2), tree.NewDInt(300)}, // UPDATE: 200 -> 300
			aBeforeB: false,
			bBeforeA: true,
		},
		{
			name:     "INSERT-DELETE: DELETE frees 100, INSERT uses 100",
			prevRowA: tree.Datums{tree.NewDInt(1), tree.DNull},        // prevRow for INSERT has only PK
			rowA:     tree.Datums{tree.NewDInt(1), tree.NewDInt(100)}, // INSERT: creates 100
			prevRowB: tree.Datums{tree.NewDInt(2), tree.NewDInt(100)}, // PrevRow has full values
			rowB:     tree.Datums{tree.NewDInt(2), tree.DNull},        // DELETE: Row has only PK
			aBeforeB: false,
			bBeforeA: true, // Delete must come before Insert
		},
		{
			name:     "UPDATE-UPDATE loop",
			prevRowA: tree.Datums{tree.NewDInt(1), tree.NewDInt(200)},
			rowA:     tree.Datums{tree.NewDInt(1), tree.NewDInt(300)}, // UPDATE: 200 -> 300
			prevRowB: tree.Datums{tree.NewDInt(2), tree.NewDInt(300)},
			rowB:     tree.Datums{tree.NewDInt(2), tree.NewDInt(200)}, // UPDATE: 300 -> 200
			aBeforeB: true,
			bBeforeA: true, // Circular dependency
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rowA := ldrdecoder.DecodedRow{
				TableID: tableDesc.GetID(),
				PrevRow: tc.prevRowA,
				Row:     tc.rowA,
			}
			rowB := ldrdecoder.DecodedRow{
				TableID: tableDesc.GetID(),
				PrevRow: tc.prevRowB,
				Row:     tc.rowB,
			}

			// Check if A depends on B (B must come before A)
			bBeforeA, err := constraints.DependsOn(ctx, rowA, rowB)
			require.NoError(t, err)
			if bBeforeA != tc.bBeforeA {
				t.Errorf("DependsOn(A, B) returned %v, expected %v", bBeforeA, tc.bBeforeA)
			}

			// Check if B depends on A (A must come before B)
			aBeforeB, err := constraints.DependsOn(ctx, rowB, rowA)
			require.NoError(t, err)
			if aBeforeB != tc.aBeforeB {
				t.Errorf("DependsOn(B, A) returned %v, expected %v", aBeforeB, tc.aBeforeB)
			}
		})
	}
}
