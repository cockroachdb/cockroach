// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package workload_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
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
		rows        int
		batchSize   int
		concurrency int
	}{
		{10, 1, 1},
		{10, 9, 1},
		{10, 10, 1},
		{10, 100, 1},
		{10, 1, 4},
		{10, 9, 4},
		{10, 10, 4},
		{10, 100, 4},
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
			if _, err := workload.Setup(ctx, db, gen, test.batchSize, test.concurrency); err != nil {
				t.Fatalf("%+v", err)
			}

			for _, table := range gen.Tables() {
				var c int
				sqlDB.QueryRow(t, fmt.Sprintf(`SELECT count(*) FROM %s`, table.Name)).Scan(&c)
				if c != table.InitialRows.NumTotal {
					t.Errorf(`%s: got %d rows expected %d`,
						table.Name, c, table.InitialRows.NumTotal)
				}
			}
		})
	}
}

func TestSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: `test`})
	defer s.Stopper().Stop(ctx)
	sqlutils.MakeSQLRunner(db).Exec(t, `CREATE DATABASE test`)

	for _, ranges := range []int{1, 2, 3, 4, 10} {

		tables := []workload.Table{
			{
				Name:   `ints`,
				Schema: `(a INT PRIMARY KEY)`,
				Splits: workload.Tuples(ranges-1, func(i int) []interface{} {
					return []interface{}{i}
				}),
			},
			{
				Name:   `floats`,
				Schema: `(a FLOAT PRIMARY KEY)`,
				Splits: workload.Tuples(ranges-1, func(i int) []interface{} {
					return []interface{}{float64(i)}
				}),
			},
			{
				Name:   `strings`,
				Schema: `(a STRING PRIMARY KEY)`,
				Splits: workload.Tuples(ranges-1, func(i int) []interface{} {
					return []interface{}{strconv.Itoa(i)}
				}),
			},
			{
				Name:   `bytes`,
				Schema: `(a BYTES PRIMARY KEY)`,
				Splits: workload.Tuples(ranges-1, func(i int) []interface{} {
					return []interface{}{strconv.Itoa(i)}
				}),
			},
			{
				Name:   `uuids`,
				Schema: `(a UUID PRIMARY KEY)`,
				Splits: workload.Tuples(ranges-1, func(i int) []interface{} {
					u, err := uuid.NewV4()
					if err != nil {
						panic(err)
					}
					return []interface{}{u.String()}
				}),
			},
		}

		t.Run(fmt.Sprintf("ranges=%d", ranges), func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(db)
			for _, table := range tables {
				sqlDB.Exec(t, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table.Name))
				sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE %s %s`, table.Name, table.Schema))

				const concurrency = 10
				if err := workload.Split(ctx, db, table, concurrency); err != nil {
					t.Fatalf("%+v", err)
				}

				countRangesQ := fmt.Sprintf(
					`SELECT count(*) FROM [SHOW EXPERIMENTAL_RANGES FROM TABLE test.%s]`, table.Name,
				)
				var actual int
				sqlDB.QueryRow(t, countRangesQ).Scan(&actual)
				if ranges != actual {
					t.Errorf(`expected %d got %d`, ranges, actual)
				}
			}
		})
	}
}

func TestApproxDatumSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		datum interface{}
		size  int64
	}{
		{nil, 0},
		{int(-1000000), 3},
		{int(-1000), 2},
		{int(-1), 1},
		{int(0), 1},
		{int(1), 1},
		{int(1000), 2},
		{int(1000000), 3},
		{float64(0), 1},
		{float64(3), 8},
		{float64(3.1), 8},
		{float64(3.14), 8},
		{float64(3.141), 8},
		{float64(3.1415), 8},
		{float64(3.14159), 8},
		{float64(3.141592), 8},
		{"", 0},
		{"a", 1},
		{"aa", 2},
	}
	for _, test := range tests {
		if size := workload.ApproxDatumSize(test.datum); size != test.size {
			t.Errorf(`%T: %v got %d expected %d`, test.datum, test.datum, size, test.size)
		}
	}
}
