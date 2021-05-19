// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

// lowMemoryBudget is the memory budget used to test builtins are recording
// their memory use. The budget needs to be large enough to establish the
// initial database connection, but small enough to overflow easily. It's set
// to be comfortably large enough that the server can start up with a bit of
// extra space to overflow.
const lowMemoryBudget = 800000

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
CREATE TABLE d.t (a STRING)
`); err != nil {
		return err
	}

	for i := 0; i < numRows; i++ {
		if _, err := sqlDB.Exec(`INSERT INTO d.t VALUES (repeat('a', $1))`, rowSize); err != nil {
			return err
		}
	}
	return nil
}

// TestConcatAggMonitorsMemory verifies that the aggregates incrementally
// record their memory usage as they build up their result.
func TestAggregatesMonitorMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// By avoiding printing the aggregate results we prevent anything
	// besides the aggregate itself from being able to catch the
	// large memory usage.
	statements := []string{
		`SELECT length(concat_agg(a)) FROM d.t`,
		`SELECT array_length(array_agg(a), 1) FROM d.t`,
		`SELECT json_typeof(json_agg(A)) FROM d.t`,
	}

	for _, statement := range statements {
		s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
			SQLMemoryPoolSize: lowMemoryBudget,
		})

		defer s.Stopper().Stop(context.Background())

		if err := createTableWithLongStrings(sqlDB); err != nil {
			t.Fatal(err)
		}

		_, err := sqlDB.Exec(statement)

		if pqErr := (*pq.Error)(nil); !errors.As(err, &pqErr) || pgcode.MakeCode(string(pqErr.Code)) != pgcode.OutOfMemory {
			t.Fatalf("Expected \"%s\" to consume too much memory", statement)
		}
	}
}

func TestEvaluatedMemoryIsChecked(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We select the LENGTH here and elsewhere because if we passed the result of
	// REPEAT up as a result, the memory error would be caught there even if
	// REPEAT was not doing its accounting.
	testData := []string{
		`SELECT length(repeat('abc', 70000000))`,
		`SELECT crdb_internal.no_constant_folding(length(repeat('abc', 70000000)))`,
	}

	for _, statement := range testData {
		t.Run("", func(t *testing.T) {
			s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
				SQLMemoryPoolSize: lowMemoryBudget,
			})
			defer s.Stopper().Stop(context.Background())

			_, err := sqlDB.Exec(
				statement,
			)
			if pqErr := (*pq.Error)(nil); !errors.As(err, &pqErr) || pgcode.MakeCode(string(pqErr.Code)) != pgcode.ProgramLimitExceeded {
				t.Errorf("Expected \"%s\" to OOM, but it didn't", statement)
			}
		})
	}
}
