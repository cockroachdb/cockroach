// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestShowRangesWithLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numNodes = 4
	ctx := context.Background()
	tcArgs := base.TestClusterArgs{}

	tc := testcluster.StartTestCluster(t, numNodes, tcArgs)
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE TABLE t (x INT PRIMARY KEY)`)
	sqlDB.Exec(t, `ALTER TABLE t SPLIT AT SELECT i FROM generate_series(0, 20) AS g(i)`)

	const nodeColIdx = 4
	const localityColIdx = 5

	result := sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_RANGES FROM TABLE t`)
	for _, row := range result {
		// Because StartTestCluster changes the locality no matter what the
		// arguments are, we expect whatever the test server sets up.
		locality := fmt.Sprintf("region=test,dc=dc%s", row[nodeColIdx])
		if row[localityColIdx] != locality {
			t.Fatalf("expected %s found %s", locality, row[localityColIdx])
		}
	}
}
