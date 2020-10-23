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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

func registerNamespaceUpgrade(r *testRegistry) {
	r.Add(testSpec{
		Name:  "version/namespace-upgrade",
		Owner: OwnerSQLSchema,
		// This test is a regression test designed to test for #49092.
		// It drops objects from the 19.2 node after the 20.1 node joins the
		// cluster, and also drops/adds objects in each of the states before, during
		// and after the migration, making sure results are as we expect.
		MinVersion: "v20.1.0",
		Cluster:    makeClusterSpec(3),
		Run: func(ctx context.Context, t *test, c *cluster) {
			predV, err := PredecessorVersion(r.buildVersion)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(predV, "v19.2") {
				t.Skip("wrong version", "this test only makes sense for the v19.2->v20.1 upgrade")
			}
			runNamespaceUpgrade(ctx, t, c, predV)
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
		//copy.
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
		createTableStep(1, "a"),
		createTableStep(1, "todrop"),
		createTableStep(1, "torename"),
		createTableStep(1, "totruncate"),
		createDBStep(1, "foo"),
		createDBStep(1, "todrop"),
		createDBStep(1, "torename"),

		// Upgrade Node 3.
		binaryUpgradeStep(c.Node(3), mainVersion),

		// Disable the migration. We'll re-enable it later.
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
		binaryUpgradeStep(c.Node(1), mainVersion),
		binaryUpgradeStep(c.Node(2), mainVersion),

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

		// Re-enable the migration.
		changeMigrationSetting(1, true),

		// Wait for the migration to finish.
		func(ctx context.Context, t *test, u *versionUpgradeTest) {
			t.l.Printf("waiting for cluster to finish namespace migration\n")

			for _, i := range roachNodes {
				err := retry.ForDuration(30*time.Second, func() error {
					db := u.conn(ctx, t, i)
					// This is copied from pkg/sqlmigrations/migrations.go. We don't
					// export it just for this test because it feels unnecessary.
					const systemNamespaceMigrationName = "upgrade system.namespace post-20.1-finalization"
					var complete bool
					if err := db.QueryRowContext(ctx,
						`SELECT crdb_internal.completed_migrations() @> ARRAY[$1::string]`,
						systemNamespaceMigrationName,
					).Scan(&complete); err != nil {
						t.Fatal(err)
					}
					if !complete {
						return fmt.Errorf("%d: migration not complete", i)
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
		},

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
