// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	gosql "database/sql"
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/util/binfetcher"
	"github.com/stretchr/testify/require"
)

func registerSecondaryIndexesMultiVersionCluster(r *testRegistry) {
	runTest := func(ctx context.Context, t *test, c *cluster) {
		// Start a 3 node 19.2 cluster.
		goos := ifLocal(runtime.GOOS, "linux")
		b, err := binfetcher.Download(ctx, binfetcher.Options{
			Binary:  "cockroach",
			Version: "v19.2.2",
			GOOS:    goos,
			GOARCH:  "amd64",
		})
		if err != nil {
			t.Fatal(err)
		}
		c.Put(ctx, b, "./cockroach", c.All())
		c.Start(ctx, t, c.All())
		// Create a table with some data, and a secondary index.
		conn := c.Conn(ctx, 1)
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
		t.Status("created sample data")

		upgradeNode := func(node int) {
			if err := c.StopCockroachGracefullyOnNode(ctx, node); err != nil {
				t.Fatal(err)
			}
			c.Put(ctx, cockroach, "./cockroach", c.Node(node))
			c.Start(ctx, t, c.Node(node))
		}

		// Upgrade one of the nodes to the current cockroach version.
		upgradeNode(1)
		t.Status("done upgrading node 1")

		// Get a connection to the new node and ensure that we can read the index fine, and
		// an insert in the mixed cluster setting doesn't result in unreadable data.
		conn = c.Conn(ctx, 1)
		if _, err := conn.Exec(`INSERT INTO t VALUES (13, 14, 15, 16)`); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(`UPDATE t SET w = 17 WHERE y = 14`); err != nil {
			t.Fatal(err)
		}
		verifyTable := func(conn *gosql.DB, expected [][]int) {
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
		expected := [][]int{
			{2, 3, 4},
			{6, 7, 8},
			{10, 11, 12},
			{14, 15, 17},
		}
		for i := 1; i <= c.spec.NodeCount; i++ {
			verifyTable(c.Conn(ctx, i), expected)
		}
		t.Status("mixed version cluster passed test")

		// Fully upgrade the cluster and ensure that the data is still valid.
		for i := 2; i <= c.spec.NodeCount; i++ {
			upgradeNode(i)
		}

		conn = c.Conn(ctx, 1)

		if _, err := conn.Exec(`INSERT INTO t VALUES (20, 21, 22, 23)`); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(`UPDATE t SET w = 25, z = 25 WHERE y = 21`); err != nil {
			t.Fatal(err)
		}

		expected = [][]int{
			{2, 3, 4},
			{6, 7, 8},
			{10, 11, 12},
			{14, 15, 17},
			{21, 25, 25},
		}
		for i := 1; i <= c.spec.NodeCount; i++ {
			verifyTable(c.Conn(ctx, i), expected)
		}
		t.Status("passed on fully upgraded cluster")
	}
	r.Add(testSpec{
		Name:       "secondary-index-multi-version",
		Owner:      OwnerSQLExec,
		Cluster:    makeClusterSpec(3),
		MinVersion: "v20.1.0",
		Run:        runTest,
	})
}
