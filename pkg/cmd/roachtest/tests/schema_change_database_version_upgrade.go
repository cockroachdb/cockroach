// Copyright 2020 The Cockroach Authors.
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
	gosql "database/sql"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func registerSchemaChangeDatabaseVersionUpgrade(r registry.Registry) {
	// This test tests 2 loosely related things:
	// 1. Correctness of database schema changes during the 20.1/20.2 mixed-
	//    version state, in which 20.2 nodes still use the deprecated database
	//    cache and non-lease-based schema change implementation.
	// 2. Ability to use ALTER DATABASE ... CONVERT TO SCHEMA WITH PARENT on
	//    databases created in 20.1.
	// TODO (lucy): Remove this test in 21.1.
	r.Add(registry.TestSpec{
		Name:    "schemachange/database-version-upgrade",
		Owner:   registry.OwnerSQLSchema,
		Cluster: r.MakeClusterSpec(3),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSchemaChangeDatabaseVersionUpgrade(ctx, t, c, *t.BuildVersion())
		},
	})
}

func createDBStep(node int, name string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx,
			fmt.Sprintf(`CREATE DATABASE %s`, name))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func uploadAndStart(nodes option.NodeListOption, v string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		// Put and start the binary.
		args := u.uploadVersion(ctx, t, nodes, v)
		// NB: can't start sequentially since cluster already bootstrapped.
		u.c.Start(ctx, nodes, args, option.StartArgsDontEncrypt, option.RoachprodArgOption{"--sequential=false"})
	}
}

func runSchemaChangeDatabaseVersionUpgrade(
	ctx context.Context, t test.Test, c cluster.Cluster, buildVersion version.Version,
) {
	// An empty string means that the cockroach binary specified by flag
	// `cockroach` will be used.
	const mainVersion = ""
	predecessorVersion, err := PredecessorVersion(buildVersion)
	if err != nil {
		t.Fatal(err)
	}

	createDatabaseWithTableStep := func(dbName string) versionStep {
		t.L().Printf("creating database %s", dbName)
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			db := u.conn(ctx, t, 1)
			_, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE %s; CREATE TABLE %s.t(a INT)`, dbName, dbName))
			require.NoError(t, err)
		}
	}

	assertDatabaseResolvable := func(ctx context.Context, db *gosql.DB, dbName string) error {
		var tblName string
		row := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT table_name FROM [SHOW TABLES FROM %s]`, dbName))
		if err := row.Scan(&tblName); err != nil {
			return err
		}
		if tblName != "t" {
			return errors.AssertionFailedf("unexpected table name %s", tblName)
		}
		return nil
	}

	assertDatabaseNotResolvable := func(ctx context.Context, db *gosql.DB, dbName string) error {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`SELECT table_name FROM [SHOW TABLES FROM %s]`, dbName))
		if err == nil || err.Error() != "pq: target database or schema does not exist" {
			return errors.AssertionFailedf("unexpected error: %s", err)
		}
		return nil
	}

	// Rename the database, drop it, and create a new database with the original
	// name.
	runSchemaChangesStep := func(dbName string) versionStep {
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			t.L().Printf("running schema changes on %s", dbName)
			newDbName := dbName + "_new_name"
			dbNode1 := u.conn(ctx, t, 1)
			dbNode2 := u.conn(ctx, t, 2)

			// Rename the database.
			_, err := dbNode1.ExecContext(ctx, fmt.Sprintf(`ALTER DATABASE %s RENAME TO %s`, dbName, newDbName))
			require.NoError(t, err)

			if err := assertDatabaseResolvable(ctx, dbNode1, newDbName); err != nil {
				t.Fatal(err)
			}
			if err := assertDatabaseNotResolvable(ctx, dbNode1, dbName); err != nil {
				t.Fatal(err)
			}
			// Also run the above steps connected to a different node. Since we still
			// use the incoherent database cache in the mixed-version state, we retry
			// until these queries produce the expected result.
			if err := testutils.SucceedsSoonError(func() error {
				return assertDatabaseResolvable(ctx, dbNode2, newDbName)
			}); err != nil {
				t.Fatal(err)
			}
			if err := testutils.SucceedsSoonError(func() error {
				return assertDatabaseNotResolvable(ctx, dbNode2, dbName)
			}); err != nil {
				t.Fatal(err)
			}

			// Drop the database.
			_, err = dbNode1.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE %s CASCADE`, newDbName))
			require.NoError(t, err)

			if err := assertDatabaseNotResolvable(ctx, dbNode1, newDbName); err != nil {
				t.Fatal(err)
			}
			if err := testutils.SucceedsSoonError(func() error {
				return assertDatabaseNotResolvable(ctx, dbNode2, newDbName)
			}); err != nil {
				t.Fatal(err)
			}

			// Create a new database with the original name.
			_, err = dbNode1.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE %s; CREATE TABLE %s.t(a INT)`, dbName, dbName))
			require.NoError(t, err)

			if err := assertDatabaseResolvable(ctx, dbNode1, dbName); err != nil {
				t.Fatal(err)
			}
			if err := testutils.SucceedsSoonError(func() error {
				return assertDatabaseResolvable(ctx, dbNode1, dbName)
			}); err != nil {
				t.Fatal(err)
			}
		}
	}

	createParentDatabaseStep := func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		t.L().Printf("creating parent database")
		db := u.conn(ctx, t, 1)
		_, err := db.ExecContext(ctx, `CREATE DATABASE new_parent_db`)
		require.NoError(t, err)
	}

	reparentDatabaseStep := func(dbName string) versionStep {
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			db := u.conn(ctx, t, 1)
			t.L().Printf("reparenting database %s", dbName)
			_, err = db.ExecContext(ctx, fmt.Sprintf(`ALTER DATABASE %s CONVERT TO SCHEMA WITH PARENT new_parent_db;`, dbName))
			require.NoError(t, err)
		}
	}

	validationStep := func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		t.L().Printf("validating")
		buf, err := c.RunWithBuffer(ctx, t.L(), c.Node(1),
			[]string{"./cockroach debug doctor cluster", "--url {pgurl:1}"}...)
		require.NoError(t, err)
		t.L().Printf("%s", buf)
	}

	interactWithReparentedSchemaStep := func(schemaName string) versionStep {
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			t.L().Printf("running schema changes on %s", schemaName)
			db := u.conn(ctx, t, 1)

			_, err = db.ExecContext(ctx, `USE new_parent_db`)
			require.NoError(t, err)

			_, err = db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s.t VALUES (1)`, schemaName))
			require.NoError(t, err)

			_, err = db.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s.t ADD COLUMN b INT`, schemaName))
			require.NoError(t, err)

			_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s.t2()`, schemaName))
			require.NoError(t, err)

			newSchemaName := schemaName + "_new"
			_, err = db.ExecContext(ctx, fmt.Sprintf(`ALTER SCHEMA %s RENAME TO %s`, schemaName, newSchemaName))
			require.NoError(t, err)
		}
	}

	dropDatabaseCascadeStep := func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		t.L().Printf("dropping parent database")
		db := u.conn(ctx, t, 1)
		_, err = db.ExecContext(ctx, `
USE defaultdb;
DROP DATABASE new_parent_db CASCADE;
`)
		require.NoError(t, err)
	}

	// This test creates several databases and then runs schema changes on each
	// one at a different stage (including deleting and re-creating) in the
	// rolling upgrade process. At the end we also test CONVERT TO SCHEMA WITH
	// PARENT on all of them. Note that we always issue schema change statements
	// to node 1 on this 3-node cluster and verify results on nodes 1 and 2.
	u := newVersionUpgradeTest(c,
		uploadAndStart(c.All(), predecessorVersion),
		waitForUpgradeStep(c.All()),
		preventAutoUpgradeStep(1),

		createDatabaseWithTableStep("db_0"),
		createDatabaseWithTableStep("db_1"),
		createDatabaseWithTableStep("db_2"),
		createDatabaseWithTableStep("db_3"),
		createDatabaseWithTableStep("db_4"),
		createDatabaseWithTableStep("db_5"),

		// Start upgrading to 20.2.

		binaryUpgradeStep(c.Node(1), mainVersion),

		runSchemaChangesStep("db_1"),

		binaryUpgradeStep(c.Nodes(2, 3), mainVersion),

		runSchemaChangesStep("db_2"),

		// Roll back to 20.1.

		binaryUpgradeStep(c.Node(1), predecessorVersion),

		runSchemaChangesStep("db_3"),

		binaryUpgradeStep(c.Nodes(2, 3), predecessorVersion),

		runSchemaChangesStep("db_4"),

		// Upgrade nodes to 20.2 again and finalize the upgrade.

		binaryUpgradeStep(c.All(), mainVersion),

		runSchemaChangesStep("db_5"),

		allowAutoUpgradeStep(1),
		waitForUpgradeStep(c.All()),

		createParentDatabaseStep,
		reparentDatabaseStep("db_0"),
		reparentDatabaseStep("db_1"),
		reparentDatabaseStep("db_2"),
		reparentDatabaseStep("db_3"),
		reparentDatabaseStep("db_4"),
		reparentDatabaseStep("db_5"),
		validationStep,

		// Run some schema changes on the re-parented schemas and their tables.

		interactWithReparentedSchemaStep("db_0"),
		interactWithReparentedSchemaStep("db_1"),
		interactWithReparentedSchemaStep("db_2"),
		interactWithReparentedSchemaStep("db_3"),
		interactWithReparentedSchemaStep("db_4"),
		interactWithReparentedSchemaStep("db_5"),
		validationStep,

		dropDatabaseCascadeStep,
		validationStep,
	)
	u.run(ctx, t)
}
