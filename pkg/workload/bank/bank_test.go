// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bank

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
)

func TestBank(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		rows           int
		ranges         int
		expectedRanges int
	}{
		{10, 0, 1}, // we always have at least one range
		{10, 1, 1},
		{10, 9, 9},
		{10, 10, 10},
		{10, 100, 10}, // don't make more ranges than rows
	}

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: `test`})
	defer s.Stopper().Stop(ctx)
	sqlutils.MakeSQLRunner(db).Exec(t, `CREATE DATABASE test`)

	for _, test := range tests {
		t.Run(fmt.Sprintf("rows=%d/ranges=%d", test.rows, test.ranges), func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(db)
			sqlDB.Exec(t, `DROP TABLE IF EXISTS bank`)

			bank := FromConfig(test.rows, test.rows, defaultPayloadBytes, test.ranges)
			bankTable := bank.Tables()[0]
			sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE %s %s`, bankTable.Name, bankTable.Schema))

			if err := workloadsql.Split(ctx, db, bankTable, 1 /* concurrency */); err != nil {
				t.Fatalf("%+v", err)
			}

			var rangeCount int
			sqlDB.QueryRow(t,
				fmt.Sprintf(`SELECT count(*) FROM [SHOW RANGES FROM TABLE %s]`, bankTable.Name),
			).Scan(&rangeCount)
			if rangeCount != test.expectedRanges {
				t.Errorf("got %d ranges expected %d", rangeCount, test.expectedRanges)
			}
		})
	}
}
