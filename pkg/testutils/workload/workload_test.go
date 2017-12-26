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
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	for _, test := range tests {
		t.Run(fmt.Sprintf("rows=%d/batch=%d", test.rows, test.batchSize), func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(db)
			sqlDB.Exec(t, `DROP DATABASE IF EXISTS test`)
			sqlDB.Exec(t, `CREATE DATABASE test`)
			sqlDB.Exec(t, `USE test`)

			gen := bank.FromRows(test.rows)
			tables := gen.Tables()
			if _, err := workload.Setup(sqlDB.DB, tables, test.batchSize); err != nil {
				t.Fatalf("%+v", err)
			}

			for _, table := range tables {
				var c int
				sqlDB.QueryRow(t, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table.Name)).Scan(&c)
				if c != table.InitialRowCount {
					t.Errorf(`%s: got %d rows expected %d`, table.Name, c, table.InitialRowCount)
				}
			}
		})
	}
}
