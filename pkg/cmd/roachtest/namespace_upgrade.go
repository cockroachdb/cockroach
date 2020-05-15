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

import "context"

func registerNamespaceUpgrade(r *testRegistry) {
	r.Add(testSpec{
		Name:  "version/namespace-upgrade",
		Owner: OwnerSQLSchema,
		// This test is a regression test designed to test for #49092.
		// It
		MinVersion: "v20.1.0",
		Cluster:    makeClusterSpec(3),
		Run: func(ctx context.Context, t *test, c *cluster) {
			predV, err := PredecessorVersion(r.buildVersion)
			if err != nil {
				t.Fatal(err)
			}
			runNamespaceUpgrade(ctx, t, c, predV)
		},
	})
}

func createObjectsStep(node int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx,
			`CREATE DATABASE test; CREATE TABLE a (a INT)`)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func dropObjectsStep(node int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx,
			`DROP DATABASE test; DROP TABLE a`)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func showDatabasesStep(node int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, 3)
		_, err := db.ExecContext(ctx,
			`SHOW DATABASES`)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func uploadAndStart(nodes nodeListOption, v string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		// Put and start the binary.
		args := u.uploadVersion(ctx, t, nodes, v)
		// NB: can't start sequentially since cluster already bootstrapped.
		u.c.Start(ctx, t, nodes, args, startArgsDontEncrypt, roachprodArgOption{"--sequential=false"})
	}
}

func runNamespaceUpgrade(ctx context.Context, t *test, c *cluster, predecessorVersion string) {
	roachNodes := c.All()
	// An empty string means that the cockroach binary specified by flag
	// `cockroach` will be used.
	const mainVersion = ""
	u := newVersionUpgradeTest(c,
		uploadAndStart(roachNodes, predecessorVersion),
		waitForUpgradeStep(roachNodes),
		preventAutoUpgradeStep(1),

		// Make some objects on node 1.
		createObjectsStep(1),

		// Upgrade Node 3.
		binaryUpgradeStep(c.Node(3), mainVersion),

		// Drop the objects on node 1, which is still on the old version.
		dropObjectsStep(1),

		// Verify that the new node can still run SHOW DATABASES.
		showDatabasesStep(3),
		// Verify that the new node can recreate the dropped objects.
		createObjectsStep(3),

		// Drop the objects on node 1 again, which is still on the old version.
		dropObjectsStep(1),

		// Upgrade the other 2 nodes.
		binaryUpgradeStep(c.Node(1), mainVersion),
		binaryUpgradeStep(c.Node(2), mainVersion),
		// Finalize upgrade.
		allowAutoUpgradeStep(1),
		waitForUpgradeStep(roachNodes),

		// Verify that the cluster can run SHOW DATABASES and re-use the names.
		showDatabasesStep(3),
		createObjectsStep(3),
	)
	u.run(ctx, t)

}
