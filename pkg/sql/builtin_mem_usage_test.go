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
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/lib/pq"
	"github.com/pkg/errors"
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
// record their memory usage as they build up their result.
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

// testIncrementalFailure gradually increases the input to a test until it fails. It returns an error if
// it did not find an incremental failing value in between definitelyGood and definitelyBad, exclusive.
// succeedsAt is a func which takes the input and returns true if the value passed and false if it did not.
func testIncrementalFailure(
	definitelyGood int, step int, definitelyBad int, succeedsAt func(int) bool,
) error {
	if !succeedsAt(definitelyGood) {
		return errors.Errorf("expected %d to be a passing value, but failed", definitelyGood)
	}
	for i := definitelyGood; i < definitelyBad; i += step {
		if !succeedsAt(i) {
			return nil
		}
	}
	return errors.Errorf("did not get failure by %d!", definitelyBad)
}

const definitelySmallEnoughLength = 10000
const lengthIncrement = 50000
const definitelyTooLargeLength = 1000000

func TestBuiltinsMonitorMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		SQLMemoryPoolSize: lowMemoryBudget,
	})
	defer s.Stopper().Stop(context.Background())

	// In this test we expect that each of the following statements allocate
	// approximately the same amount of memory, and thus should all fail at the
	// same time.
	err := testIncrementalFailure(
		definitelySmallEnoughLength, lengthIncrement, definitelyTooLargeLength,
		func(overFlowLength int) bool {
			statements := []string{
				// Expressions which are marked as pure and do not contain any variables get
				// constant folded, and thus their evaluation follows a different code path
				// than those which are not constant folded.

				// SELECTing the LENGTH ensures that we capture the memory problem at the
				// correct level.
				fmt.Sprintf(`SELECT LENGTH(REPEAT('a', %d))`, overFlowLength),

				// We are currently overly restrictive with memory allocation. In theory these
				// queries should be allowed, but due to the complexity of accurately tracking
				// all memory, for now we err on the side of rejecting queries. See #15295.
				fmt.Sprintf(`SELECT LENGTH(REPEAT('a', %d)) + LENGTH(REPEAT('a', %d))`, overFlowLength/2, overFlowLength/2),
				// In this query, each REPEAT is counted twice, once upon originally being
				// called and once upon being concatenated.
				fmt.Sprintf(`SELECT LENGTH(CONCAT(REPEAT('a', %d), REPEAT('a', %d)))`, overFlowLength/4, overFlowLength/4),
				fmt.Sprintf(`SELECT LENGTH(CONCAT_WS('!', REPEAT('a', %d), REPEAT('a', %d)))`, overFlowLength/4, overFlowLength/4),

				// By including the `generate_series` variable in this query, we prevent this
				// call to REPEAT from getting constant folded.
				fmt.Sprintf(`SELECT LENGTH(REPEAT('a', %d + generate_series * 0)) FROM GENERATE_SERIES(1,1)`, overFlowLength),
			}

			successes := make([]string, 0, len(statements))
			failures := make([]string, 0, len(statements))

			for _, statement := range statements {
				_, err := sqlDB.Exec(statement)
				if err != nil {
					switch err := err.(type) {
					case *pq.Error:
						if err.Code == pgerror.CodeOutOfMemoryError {
							failures = append(failures, statement)
						} else {
							t.Fatalf("statement failed with non-OOM error: \"%s\"", err)
						}
					default:
						t.Fatalf("statement failed with non-PG error: \"%s\"", err)
					}
				} else {
					successes = append(successes, statement)
				}
			}

			if len(successes) > 0 && len(failures) > 0 {
				t.Fatalf(
					`Expected all statements to OOM at the same time, but at value
	%d these statements failed:
		%s,
	while these statements succeeded:
		%s`, overFlowLength, failures, successes)
			}

			return len(failures) == 0
		})

	if err != nil {
		t.Fatal(err)
	}
}

func TestMemoryGetsFreedOnEachRow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		SQLMemoryPoolSize: lowMemoryBudget,
	})
	defer s.Stopper().Stop(context.Background())

	// We include the reference to generate_series to ensure that the call to
	// REPEAT doesn't get constant-folded.
	if _, err := sqlDB.Exec(
		`SELECT LENGTH(REPEAT('a', 300000 + generate_series * 0)) FROM GENERATE_SERIES(1,100)`,
	); err != nil {
		t.Fatalf("Expected statement to run successfully, but got %s", err)
	}
}
