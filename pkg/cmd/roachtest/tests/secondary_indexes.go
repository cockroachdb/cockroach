// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/stretchr/testify/require"
)

// runIndexUpgrade runs a test that creates an index before a version upgrade,
// and modifies it in a mixed version setting. It aims to test the changes made
// to index encodings done to allow secondary indexes to respect column families.
func runIndexUpgrade(
	ctx context.Context, t test.Test, c cluster.Cluster, predecessorVersion string,
) {
	firstExpected := [][]int{
		{2, 3, 4},
		{6, 7, 8},
		{10, 11, 12},
		{14, 15, 17},
	}
	secondExpected := [][]int{
		{2, 3, 4},
		{6, 7, 8},
		{10, 11, 12},
		{14, 15, 17},
		{21, 25, 25},
	}

	roachNodes := c.All()
	// An empty string means that the cockroach binary specified by flag
	// `cockroach` will be used.
	const mainVersion = ""
	u := newVersionUpgradeTest(c,
		uploadAndStart(roachNodes, predecessorVersion),
		waitForUpgradeStep(roachNodes),

		// Fill the cluster with data.
		createDataStep(),

		// Upgrade one of the nodes.
		binaryUpgradeStep(c.Node(1), mainVersion),

		// Modify index data from that node.
		modifyData(1,
			`INSERT INTO t VALUES (13, 14, 15, 16)`,
			`UPDATE t SET w = 17 WHERE y = 14`,
		),

		// Ensure all nodes see valid index data.
		verifyTableData(1, firstExpected),
		verifyTableData(2, firstExpected),
		verifyTableData(3, firstExpected),

		// Upgrade the rest of the cluster.
		binaryUpgradeStep(c.Node(2), mainVersion),
		binaryUpgradeStep(c.Node(3), mainVersion),

		// Finalize the upgrade.
		allowAutoUpgradeStep(1),
		waitForUpgradeStep(roachNodes),

		// Modify some more data now that the cluster is upgraded.
		modifyData(1,
			`INSERT INTO t VALUES (20, 21, 22, 23)`,
			`UPDATE t SET w = 25, z = 25 WHERE y = 21`,
		),

		// Ensure all nodes see valid index data.
		verifyTableData(1, secondExpected),
		verifyTableData(2, secondExpected),
		verifyTableData(3, secondExpected),
	)

	u.run(ctx, t)
}

func createDataStep() versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn := u.conn(ctx, t, 1)
		if _, err := conn.Exec(`
CREATE TABLE t (
	x INT PRIMARY KEY, y INT, z INT, w INT,
	INDEX i (y) STORING (z, w),
	FAMILY (x), FAMILY (y), FAMILY (z), FAMILY (w)
);
INSERT INTO t VALUES (1, 2, 3, 4), (5, 6, 7, 8), (9, 10, 11, 12);
`); err != nil {
			t.Fatal(err)
		}
	}
}

func modifyData(node int, sql ...string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		// Write some data into the table.
		conn := u.conn(ctx, t, node)
		for _, s := range sql {
			if _, err := conn.Exec(s); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func verifyTableData(node int, expected [][]int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn := u.conn(ctx, t, node)
		rows, err := conn.Query(`SELECT y, z, w FROM t@i ORDER BY y`)
		if err != nil {
			t.Fatal(err)
		}
		var y, z, w int
		count := 0
		for ; rows.Next(); count++ {
			if err := rows.Scan(&y, &z, &w); err != nil {
				t.Fatal(err)
			}
			found := []int{y, z, w}
			require.Equal(t, found, expected[count])
		}
	}
}

func registerSecondaryIndexesMultiVersionCluster(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "schemachange/secondary-index-multi-version",
		Owner:   registry.OwnerSQLSchema,
		Cluster: r.MakeClusterSpec(3),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			predV, err := PredecessorVersion(*t.BuildVersion())
			if err != nil {
				t.Fatal(err)
			}
			runIndexUpgrade(ctx, t, c, predV)
		},
	})
}
