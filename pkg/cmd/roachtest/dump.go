// Copyright 2020 The Cockroach Authors.
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
	"strings"
)

// runDumpBackwardsCompat ensures that `cockroach dump` can be run on lesser
// version clusters successfully. The goal is to ensure that new metadata
// queries that `cockroach dump` performs can handle errors when run on a
// cluster that cannot yet handle those metadata queries.
func runDumpBackwardsCompat(ctx context.Context, t *test, c *cluster, predecessorVersion string) {
	expected := `CREATE TABLE t (
	x INT8 NULL,
	y INT8 NULL,
	FAMILY "primary" (x, y, rowid)
);

INSERT INTO t (x, y) VALUES
	(1, 1),
	(2, 2);`
	roachNodes := c.All()
	// An empty string means that the cockroach binary specified by flag
	// `cockroach` will be used.
	const mainVersion = ""
	u := newVersionUpgradeTest(c,
		uploadAndStart(roachNodes, predecessorVersion),
		waitForUpgradeStep(roachNodes),
		// Fill some data in the cluster.
		fillData(),
		// Get an upgraded version of the Cockroach Binary, and try to dump.
		runDump(roachNodes, mainVersion, expected),
	)

	u.run(ctx, t)
}

func fillData() versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		conn := u.conn(ctx, t, 1)
		if _, err := conn.Exec(`
CREATE DATABASE d;
CREATE TABLE d.t (x INT, y INT);
INSERT INTO d.t VALUES (1, 1), (2, 2);
`); err != nil {
			t.Fatal(err)
		}
	}
}

func runDump(nodes nodeListOption, mainVersion, expected string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		// Put the new version of Cockroach onto the node.
		u.uploadVersion(ctx, t, nodes, mainVersion)
		raw, err := u.c.RunWithBuffer(ctx, t.logger(), nodes, `cockroach dump --insecure d`)
		if err != nil {
			t.Fatal(err)
		}
		output := strings.TrimSpace(string(raw))
		if output != expected {
			t.Errorf("expected %s, but found %s", expected, output)
		}
	}
}

func registerDumpBackwardsCompat(r *testRegistry) {
	r.Add(testSpec{
		Name:       "dump-backwards-compatibility",
		Owner:      OwnerBulkIO,
		Cluster:    makeClusterSpec(1),
		MinVersion: "v20.2.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			predV, err := PredecessorVersion(r.buildVersion)
			if err != nil {
				t.Fatal(err)
			}
			runDumpBackwardsCompat(ctx, t, c, predV)
		},
	})
}
