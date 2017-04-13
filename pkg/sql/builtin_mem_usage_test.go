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
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func createTableWithLongStrings(sqlDB *gosql.DB) error {
	if _, err := sqlDB.Exec(`
CREATE DATABASE d;
CREATE TABLE d.t (a STRING)
`); err != nil {
		return err
	}

	for i := 0; i < 50; i++ {
		if _, err := sqlDB.Exec(`INSERT INTO d.t VALUES (REPEAT('foo', 10000))`); err != nil {
			return err
		}
	}
	return nil
}

// validateErrorMessage checks the returned error against an expected number of
// bytes to have been requested. This is necessary because if the aggregate
// itself is not requesting its memory incrementally, the place it passes it
// off to will check it all at once, which doesn't protect against an aggregate
// building up a large value. Because of this, we need to ensure that the
// amount of memory requested is an incremental amount, rather than the entire
// aggregate.
func validateErrorMessage(t *testing.T, err error, expectedNumBytes int, budget int) {
	expected := fmt.Sprintf("pq: sql: memory budget exceeded: %d bytes requested, %d bytes in budget", expectedNumBytes, budget)

	if err.Error() != expected {
		t.Errorf("Expected error message to be \"%s\", got \"%s\"", expected, err.Error())
	}
}

func TestConcatAggMonitorsMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		SQLMemoryPoolSize: 200000,
	})

	defer s.Stopper().Stop()

	if err := createTableWithLongStrings(sqlDB); err != nil {
		t.Fatal(err)
	}

	_, err := sqlDB.Exec(`SELECT CONCAT_AGG(a) FROM d.t`)

	if err == nil {
		t.Fatal("Expected CONCAT_AGG to consume too much memory")
	}

	validateErrorMessage(t, err, 30720, 200000)
}

func TestArrayAggMonitorsMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		SQLMemoryPoolSize: 200000,
	})

	defer s.Stopper().Stop()

	if err := createTableWithLongStrings(sqlDB); err != nil {
		t.Fatal(err)
	}

	_, err := sqlDB.Exec(`SELECT ARRAY_AGG(a) FROM d.t`)

	if err == nil {
		t.Fatal("Expected ARRAY_AGG to consume too much memory")
	}

	validateErrorMessage(t, err, 30720, 200000)
}
