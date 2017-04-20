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
//
// Author: Justin Jaffray (justin@cockroachlabs.com)

package sql

import (
	gosql "database/sql"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
CREATE TABLE d.t (a STRING)
`); err != nil {
		return err
	}

	for i := 0; i < numRows; i++ {
		if _, err := sqlDB.Exec(`INSERT INTO d.t VALUES (REPEAT('a', $1))`, rowSize); err != nil {
			return err
		}
	}
	return nil
}

// TestConcatAggMonitorsMemory verifies that the aggregates incrementally
// record their memory usage as they builds up their result.
func TestAggregatesMonitorMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// By selecting the LENGTH we prevent anything besides the aggregate itself
	// from being able to catch the large memory usage.
	statements := []string{
		`SELECT LENGTH(CONCAT_AGG(a)) FROM d.t`,
		`SELECT ARRAY_LENGTH(ARRAY_AGG(a), 1) FROM d.t`,
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
