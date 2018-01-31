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

package workload_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/workload"
	"github.com/cockroachdb/cockroach/pkg/testutils/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestGet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if _, err := workload.Get(`bank`); err != nil {
		t.Errorf(`expected success got: %+v`, err)
	}
	if _, err := workload.Get(`nope`); !testutils.IsError(err, `unknown generator`) {
		t.Errorf(`expected "unknown generator" error got: %+v`, err)
	}
}

func TestSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		rows      int
		batchSize int
	}{
		{10, 1},
		{10, 9},
		{10, 10},
		{10, 100},
	}

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: `test`})
	defer s.Stopper().Stop(ctx)
	sqlutils.MakeSQLRunner(db).Exec(t, `CREATE DATABASE test`)

	for _, test := range tests {
		t.Run(fmt.Sprintf("rows=%d/batch=%d", test.rows, test.batchSize), func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(db)
			sqlDB.Exec(t, `DROP TABLE IF EXISTS bank`)

			gen := bank.FromRows(test.rows)
			if _, err := workload.Setup(sqlDB.DB, gen, test.batchSize); err != nil {
				t.Fatalf("%+v", err)
			}

			for _, table := range gen.Tables() {
				var c int
				sqlDB.QueryRow(t, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table.Name)).Scan(&c)
				if c != table.InitialRowCount {
					t.Errorf(`%s: got %d rows expected %d`, table.Name, c, table.InitialRowCount)
				}
			}
		})
	}
}

func TestSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const rows, payloadBytes, concurrency = 10, 0, 10
	tests := []int{1, 2, 3, 4, 10}

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: `test`})
	defer s.Stopper().Stop(ctx)
	sqlutils.MakeSQLRunner(db).Exec(t, `CREATE DATABASE test`)

	for _, ranges := range tests {
		t.Run(fmt.Sprintf("ranges=%d", ranges), func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(db)
			sqlDB.Exec(t, `DROP TABLE IF EXISTS bank`)

			gen := bank.FromConfig(rows, payloadBytes, ranges)
			table := gen.Tables()[0]
			sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE %s %s`, table.Name, table.Schema))

			if err := workload.Split(ctx, sqlDB.DB, table, concurrency); err != nil {
				t.Fatalf("%+v", err)
			}

			var actual int
			sqlDB.QueryRow(
				t, `SELECT COUNT(*) FROM [SHOW TESTING_RANGES FROM TABLE test.bank]`,
			).Scan(&actual)
			if ranges != actual {
				t.Errorf(`expected %d got %d`, ranges, actual)
			}
		})
	}
}
