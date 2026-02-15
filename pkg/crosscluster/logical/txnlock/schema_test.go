// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestColumnSet(t *testing.T) {
	rng := rand.New(rand.NewSource(0))

	randomTypes := func(numCols int) []*types.T {
		types := make([]*types.T, numCols)
		for i := range types {
			types[i] = randgen.RandColumnType(rng)
		}
		return types
	}

	randomRow := func(types []*types.T, nulls bool) tree.Datums {
		row := make(tree.Datums, len(types))
		for i, typ := range types {
			row[i] = randgen.RandDatum(rng, typ, false /* nullOk */)
		}
		return row
	}

	test := func(t *testing.T, types []*types.T) {
		evalCtx := eval.Context{}
		colSet := columnSet{
			columns: make([]int32, len(types)),
		}
		for i := range len(types) {
			colSet.columns[i] = int32(i)
		}
		for range 1000 {
			// Generate two random rows
			a := randomRow(types, true)
			b := randomRow(types, true)

			// Ensure that hash(a) == hash(a)
			if colSet.hash(a) != colSet.hash(a) {
				t.Errorf("hash(a) != hash(a)")
			}

			// Ensure that equal(a, a) when !null(a)
			if !colSet.null(a) && !colSet.equal(&evalCtx, a, a) {
				t.Errorf("!equal(a, a) when !null(a)")
			}

			// Ensure that equal(a, b) implies hash(a) == hash(b)
			if colSet.equal(&evalCtx, a, b) && colSet.hash(a) != colSet.hash(b) {
				t.Errorf("equal(a, b) but hash(a) != hash(b)")
			}

			// Ensure that equal(a, b) implies equal(b, a)
			if colSet.equal(&evalCtx, a, b) != colSet.equal(&evalCtx, b, a) {
				t.Errorf("equal(a, b) != equal(b, a)")
			}

			// Ensure that null(a) or null(b) implies !equal(a, b)
			if (colSet.null(a) || colSet.null(b)) && colSet.equal(&evalCtx, a, b) {
				t.Errorf("null(a) or null(b) but equal(a, b) is true")
			}
		}
	}

	for range 100 {
		types := randomTypes(rand.Intn(5) + 1)
		t.Logf("types: %v", types)
		test(t, types)
	}
}

// Test `newTableConstraint` using a handful of table driven test

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
		name       string
		createStmt string
		tableName  string
		want       *tableConstraints
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
			tableName: "test1",
			want: &tableConstraints{
				PrimaryKey:        columnSet{columns: []int32{0}},     // column "a" at index 0
				UniqueConstraints: []columnSet{{columns: []int32{1}}}, // unique "b" at index 1
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
			tableName: "test2",
			want: &tableConstraints{
				PrimaryKey:        columnSet{columns: []int32{0, 1}},     // columns "a", "b" at indices 0, 1
				UniqueConstraints: []columnSet{{columns: []int32{2, 3}}}, // unique (c,d)
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
			tableName: "test3",
			want: &tableConstraints{
				PrimaryKey:        columnSet{columns: []int32{0}},                            // column "id" at index 0
				UniqueConstraints: []columnSet{{columns: []int32{1}}, {columns: []int32{2}}}, // unique "email", "username"
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create the table
			runner.Exec(t, tc.createStmt)

			// Get the table descriptor
			tableDesc := desctestutils.TestingGetTableDescriptor(
				kvDB,
				codec,
				"defaultdb",
				"public",
				tc.tableName,
			)

			// Call newTableConstraints
			got := newTableConstraints(tableDesc)

			// Update expected values with the actual table ID and index IDs
			tc.want.PrimaryKey.prefix = tablePrefix(tableDesc.GetID())
			primaryIndex := tableDesc.GetPrimaryIndex()
			idx := 0
			for _, uc := range tableDesc.EnforcedUniqueConstraintsWithIndex() {
				if uc.GetID() != primaryIndex.GetID() {
					tc.want.UniqueConstraints[idx].prefix = uniqueIndexPrefix(tableDesc.GetID(), uc.GetID())
					idx++
				}
			}

			// Verify constraints using require.Equal
			require.Equal(t, tc.want, got)
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

	// Create a table with a unique constraint
	runner.Exec(t, `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			unique_value INT UNIQUE
		)
	`)

	tableDesc := desctestutils.TestingGetTableDescriptor(kvDB, codec, "defaultdb", "public", "test_table")
	constraints := newTableConstraints(tableDesc)

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
			bBeforeA := constraints.DependsOn(rowA, rowB)
			if bBeforeA != tc.bBeforeA {
				t.Errorf("DependsOn(A, B) returned %v, expected %v", bBeforeA, tc.bBeforeA)
			}

			// Check if B depends on A (A must come before B)
			aBeforeB := constraints.DependsOn(rowB, rowA)
			if aBeforeB != tc.aBeforeB {
				t.Errorf("DependsOn(B, A) returned %v, expected %v", aBeforeB, tc.aBeforeB)
			}
		})
	}
}
