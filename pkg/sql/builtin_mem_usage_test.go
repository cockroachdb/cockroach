// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/lib/pq"
)

// lowMemoryBudget is the memory budget used to test builtins are recording
// their memory use. The budget needs to be large enough to establish the
// initial database connection, but small enough to overflow easily. It's set
// to be comfortably large enough that the server can start up with a bit of
// extra space to overflow.
const lowMemoryBudget = 500000

// rowSize is the length of the string present in each row of the table created
// by createTableWithLongStrings.
const rowSize = 30000

// numRows is the number of rows to insert in createTableWithLongStrings.
// numRows and rowSize were picked arbitrarily but so that rowSize * numRows >
// lowMemoryBudget, so that aggregating them all in a CONCAT_AGG or
// ARRAY_AGG will exhaust lowMemoryBudget.
const numRows = 50

// createTableWithLongStrings creates a table with a modest number of long strings,
// with the intention of using them to exhaust a memory budget.
func createTableWithLongStrings(sqlDB *gosql.DB) error {
	if _, err := sqlDB.Exec(`
CREATE DATABASE d;
CREATE TABLE d.public.t (a STRING)
`); err != nil {
		return err
	}

	for i := 0; i < numRows; i++ {
		if _, err := sqlDB.Exec(`INSERT INTO d.public.t VALUES (REPEAT('a', $1))`, rowSize); err != nil {
			return err
		}
	}
	return nil
}

// TestConcatAggMonitorsMemory verifies that the aggregates incrementally
// record their memory usage as they build up their result.
func TestAggregatesMonitorMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// By avoiding printing the aggregate results we prevent anything
	// besides the aggregate itself from being able to catch the
	// large memory usage.
	statements := []string{
		`SELECT LENGTH(CONCAT_AGG(a)) FROM d.public.t`,
		`SELECT ARRAY_LENGTH(ARRAY_AGG(a), 1) FROM d.public.t`,
		`SELECT JSON_TYPEOF(JSON_AGG(A)) FROM d.public.t`,
	}

	for _, statement := range statements {
		s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
			SQLMemoryPoolSize: lowMemoryBudget,
		})

		defer s.Stopper().Stop(context.Background())

		if err := createTableWithLongStrings(sqlDB); err != nil {
			t.Fatal(err)
		}

		if _, err := sqlDB.Exec(statement); err.(*pq.Error).Code != pgerror.CodeOutOfMemoryError {
			t.Fatalf("Expected \"%s\" to consume too much memory", statement)
		}
	}
}

func TestBuiltinsAccountForMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		builtin            tree.Builtin
		args               tree.Datums
		expectedAllocation int64
	}{
		{builtins.Builtins["repeat"][0],
			tree.Datums{
				tree.NewDString("abc"),
				tree.NewDInt(123),
			},
			int64(3 * 123)},
		{builtins.Builtins["concat"][0],
			tree.Datums{
				tree.NewDString("abc"),
				tree.NewDString("abc"),
			},
			int64(3 + 3)},
		{builtins.Builtins["concat_ws"][0],
			tree.Datums{
				tree.NewDString("!"),
				tree.NewDString("abc"),
				tree.NewDString("abc"),
			},
			int64(3 + 1 + 3)},
		{builtins.Builtins["lower"][0],
			tree.Datums{
				tree.NewDString("ABC"),
			},
			int64(3)},
	}

	for _, test := range testData {
		t.Run("", func(t *testing.T) {
			evalCtx := tree.NewTestingEvalContext()
			defer evalCtx.Stop(context.Background())
			defer evalCtx.ActiveMemAcc.Close(context.Background())
			previouslyAllocated := evalCtx.ActiveMemAcc.Used()
			_, err := test.builtin.Fn(evalCtx, test.args)
			if err != nil {
				t.Fatal(err)
			}
			deltaAllocated := evalCtx.ActiveMemAcc.Used() - previouslyAllocated
			if deltaAllocated != test.expectedAllocation {
				t.Errorf("Expected to allocate %d, actually allocated %d", test.expectedAllocation, deltaAllocated)
			}
		})
	}
}

func TestEvaluatedMemoryIsChecked(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// We select the LENGTH here and elsewhere because if we passed the result of
	// REPEAT up as a result, the memory error would be caught there even if
	// REPEAT was not doing its accounting.
	testData := []string{
		`SELECT LENGTH(REPEAT('abc', 300000))`,
		`SELECT crdb_internal.no_constant_folding(LENGTH(REPEAT('abc', 300000)))`,
	}

	for _, statement := range testData {
		t.Run("", func(t *testing.T) {
			s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
				SQLMemoryPoolSize: lowMemoryBudget,
			})
			defer s.Stopper().Stop(context.Background())

			if _, err := sqlDB.Exec(
				statement,
			); err.(*pq.Error).Code != pgerror.CodeOutOfMemoryError {
				t.Errorf("Expected \"%s\" to OOM, but it didn't", statement)
			}
		})
	}
}

func TestMemoryGetsFreedOnEachRow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// This test verifies that the memory allocated during the computation of a
	// row gets freed before moving on to subsequent rows.

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		SQLMemoryPoolSize: lowMemoryBudget,
	})
	defer s.Stopper().Stop(context.Background())

	stringLength := 300000
	numRows := 100

	// Check that if this string is allocated per-row, we don't OOM.
	if _, err := sqlDB.Exec(
		fmt.Sprintf(
			`SELECT crdb_internal.no_constant_folding(LENGTH(REPEAT('a', %d))) FROM GENERATE_SERIES(1, %d)`,
			stringLength,
			numRows,
		),
	); err != nil {
		t.Fatalf("Expected statement to run successfully, but got %s", err)
	}

	// Ensure that if this memory is all allocated at once, we OOM.
	if _, err := sqlDB.Exec(
		fmt.Sprintf(
			`SELECT crdb_internal.no_constant_folding(LENGTH(REPEAT('a', %d * %d)))`,
			stringLength,
			numRows,
		),
	); err.(*pq.Error).Code != pgerror.CodeOutOfMemoryError {
		t.Fatalf("Expected statement to OOM, but it didn't")
	}
}
