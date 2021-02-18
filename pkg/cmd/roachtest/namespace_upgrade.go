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
	"fmt"
)

func registerNamespaceUpgradeMigration(r *testRegistry) {
	r.Add(testSpec{
		Name:       "version/namespace-upgrade/migration",
		Owner:      OwnerSQLSchema,
		MinVersion: "v21.1.0",
		Cluster:    makeClusterSpec(3),
		Run: func(ctx context.Context, t *test, c *cluster) {
			// This test is specifically for the namespace migration in 21.1 and
			// should be removed in later versions.
			if v := r.buildVersion; !(v.Major() == 21 && v.Minor() == 1) {
				t.Skip("test is only for 21.1")
			}
			runNamespaceUpgradeMigration(ctx, t, c)
		},
	})
}

func createTableStep(node int, table string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx,
			fmt.Sprintf(`CREATE TABLE %s (a INT)`, table))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func createDBStep(node int, name string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx,
			fmt.Sprintf(`CREATE DATABASE %s`, name))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func dropTableStep(node int, table string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx,
			fmt.Sprintf(`DROP TABLE %s`, table))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func dropDBStep(node int, name string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx,
			fmt.Sprintf(`DROP DATABASE %s`, name))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func renameDBStep(node int, oldDB string, newDB string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx,
			fmt.Sprintf(`ALTER DATABASE %s RENAME TO %s`, oldDB, newDB))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func renameTableStep(node int, oldTable string, newTable string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx,
			fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, oldTable, newTable))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func truncateTableStep(node int, table string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx,
			fmt.Sprintf(`TRUNCATE %s`, table))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func showDatabasesStep(node int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx,
			`SHOW DATABASES`)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func changeMigrationSetting(node int, enable bool) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx, `SET CLUSTER SETTING testing.system_namespace_migration.enabled = $1`, enable)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func verifyNoOrphanedOldEntries(node int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		// Check that there are no rows in namespace that aren't in namespace2,
		// except for the old entry for namespace (descriptor 2) which we don't
		// copy.
		row := db.QueryRowContext(ctx,
			`SELECT count(*) FROM [2 AS namespace] WHERE id != 2 AND id NOT IN (SELECT id FROM [30 as namespace2])`)
		var count int
		if err := row.Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatal("unexpected entries found in namespace but not in namespace2")
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

func runNamespaceUpgradeMigration(ctx context.Context, t *test, c *cluster) {
	const (
		// These versions were recent point releases when this test was added.
		v19_2 = "19.2.11"
		v20_1 = "20.1.10"
		v20_2 = "20.2.4"
		// An empty string means that the cockroach binary specified by flag
		// `cockroach` will be used.
		mainVersion = ""
	)

	roachNodes := c.All()
	u := newVersionUpgradeTest(c,
		// The initial portion of the test, when we set up the descriptors and
		// namespace entries in 19.2 and 20.1, was taken from the original
		// version/namespace-upgrade test that only ran in 20.1.

		uploadAndStart(roachNodes, v19_2),
		waitForUpgradeStep(roachNodes),
		preventAutoUpgradeStep(1),

		// Make some objects on node 1.
		createTableStep(1, "a"),
		createTableStep(1, "todrop"),
		createTableStep(1, "torename"),
		createTableStep(1, "totruncate"),
		createDBStep(1, "foo"),
		createDBStep(1, "todrop"),
		createDBStep(1, "torename"),

		// Upgrade Node 3.
		binaryUpgradeStep(c.Node(3), v20_1),

		// Disable the async migration that ran in previous versions so that we can
		// test the 21.1 migration.
		changeMigrationSetting(3, false),

		// Drop the objects on node 1, which is still on the old version.
		dropTableStep(1, "a"),
		dropDBStep(1, "foo"),

		// Verify that the new node can still run SHOW DATABASES.
		showDatabasesStep(3),
		// Verify that the new node can recreate the dropped objects.
		createTableStep(3, "a"),
		createDBStep(3, "foo"),

		// Drop the objects on node 1 again, which is still on the old version.
		dropTableStep(1, "a"),
		dropDBStep(1, "foo"),

		// Upgrade the other 2 nodes.
		binaryUpgradeStep(c.Node(1), v20_1),
		binaryUpgradeStep(c.Node(2), v20_1),

		// Finalize upgrade.
		allowAutoUpgradeStep(1),

		waitForUpgradeStep(roachNodes),

		// After finalization, but before upgrade, add a table, drop a table,
		// rename a table, and truncate a table.
		createTableStep(1, "fresh"),
		createDBStep(1, "fresh"),
		dropTableStep(1, "todrop"),
		dropDBStep(1, "todrop"),
		renameTableStep(1, "torename", "new"),
		renameDBStep(1, "torename", "new"),
		truncateTableStep(1, "totruncate"),

		// Now upgrade all the way to 21.1.

		binaryUpgradeStep(c.All(), v20_2),
		waitForUpgradeStep(roachNodes),

		binaryUpgradeStep(c.All(), mainVersion),
		waitForUpgradeStep(roachNodes),

		// Verify that there are no remaining entries that only live in the old
		// namespace table.
		verifyNoOrphanedOldEntries(1),

		// Verify that the cluster can run SHOW DATABASES and re-use the names.
		showDatabasesStep(3),
		createTableStep(1, "a"),
		createDBStep(1, "foo"),
		createTableStep(1, "torename"),
		createDBStep(1, "torename"),
		createTableStep(1, "todrop"),
		createDBStep(1, "todrop"),

		verifyNoOrphanedOldEntries(1),
	)
	u.run(ctx, t)

}
