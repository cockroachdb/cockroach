// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
)

func TestInsightsWorkload(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: `test`})
	defer srv.Stopper().Stop(ctx)

	sqlutils.MakeSQLRunner(db).Exec(t, `CREATE DATABASE test`)

	for _, test := range tests {
		t.Run(fmt.Sprintf("rows=%d/ranges=%d", test.rows, test.ranges), func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(db)

			insights := FromConfig(test.rows, test.rows, defaultPayloadBytes, test.ranges)
			insightsTableA := insights.Tables()[0]
			sqlDB.Exec(t, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tree.NameString(insightsTableA.Name)))
			sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE %s %s`, tree.NameString(insightsTableA.Name), insightsTableA.Schema))

			if err := workloadsql.Split(ctx, db, insightsTableA, 1 /* concurrency */); err != nil {
				t.Fatalf("%+v", err)
			}

			var rangeCount int
			sqlDB.QueryRow(t,
				fmt.Sprintf(`SELECT count(*) FROM [SHOW RANGES FROM TABLE %s]`, tree.NameString(insightsTableA.Name)),
			).Scan(&rangeCount)
			if rangeCount != test.expectedRanges {
				t.Errorf("got %d ranges expected %d", rangeCount, test.expectedRanges)
			}
		})
	}
}
