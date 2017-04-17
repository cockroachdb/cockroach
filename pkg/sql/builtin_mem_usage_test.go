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

const lowMemoryLimit = 200000

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
			SQLMemoryPoolSize: lowMemoryLimit,
		})

		defer s.Stopper().Stop()

		if err := createTableWithLongStrings(sqlDB); err != nil {
			t.Fatal(err)
		}

		if _, err := sqlDB.Exec(statement); err == nil {
			t.Fatalf("Expected \"%s\" to consume too much memory", statement)
		}
	}
}
