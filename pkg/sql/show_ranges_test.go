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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestShowRangesWithLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numNodes = 3
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE TABLE t (x INT PRIMARY KEY)`)
	sqlDB.Exec(t, `ALTER TABLE t SPLIT AT SELECT i FROM generate_series(0, 20) AS g(i)`)

	const leaseHolderIdx = 0
	const leaseHolderLocalityIdx = 1
	const replicasColIdx = 2
	const localitiesColIdx = 3
	replicas := make([]int, 3)

	// TestClusters get some localities by default.
	q := `SELECT lease_holder, lease_holder_locality, replicas, replica_localities from [SHOW RANGES FROM TABLE t]`
	result := sqlDB.QueryStr(t, q)
	for _, row := range result {
		// Verify the leaseholder localities.
		leaseHolder := row[leaseHolderIdx]
		leaseHolderLocalityExpected := fmt.Sprintf(`region=test,dc=dc%s`, leaseHolder)
		if row[leaseHolderLocalityIdx] != leaseHolderLocalityExpected {
			t.Fatalf("expected %s found %s", leaseHolderLocalityExpected, row[leaseHolderLocalityIdx])
		}

		// Verify the replica localities.
		_, err := fmt.Sscanf(row[replicasColIdx], "{%d,%d,%d}", &replicas[0], &replicas[1], &replicas[2])
		if err != nil {
			t.Fatal(err)
		}
		var builder strings.Builder
		builder.WriteString("{")
		for i, replica := range replicas {
			builder.WriteString(fmt.Sprintf(`"region=test,dc=dc%d"`, replica))
			if i != len(replicas)-1 {
				builder.WriteString(",")
			}
		}
		builder.WriteString("}")
		expected := builder.String()
		if row[localitiesColIdx] != expected {
			t.Fatalf("expected %s found %s", expected, row[localitiesColIdx])
		}
	}
}
