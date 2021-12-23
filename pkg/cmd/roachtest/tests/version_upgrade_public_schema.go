// Copyright 2021 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
)

// this test ensures that privileges stay consistent after version upgrades.
func registerVersionUpgradePublicSchema(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "versionupgrade/publicschema",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(3),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runVersionUpgradePublicSchema(ctx, t, c, *t.BuildVersion())
		},
	})
}

func runVersionUpgradePublicSchema(
	ctx context.Context, t test.Test, c cluster.Cluster, buildVersion version.Version,
) {
	predecessorVersion, err := PredecessorVersion(buildVersion)
	if err != nil {
		t.Fatal(err)
	}

	const currentVersion = ""

	steps := []versionStep{
		resetStep(),
		uploadAndStart(c.All(), predecessorVersion),
		waitForUpgradeStep(c.All()),

		// NB: at this point, cluster and binary version equal predecessorVersion,
		// and auto-upgrades are on.
		preventAutoUpgradeStep(1),
	}

	steps = append(
		steps,
		createDatabaseStep("test1"),

		tryReparentingDatabase(false, ""),

		// Roll nodes forward.
		binaryUpgradeStep(c.Node(3), currentVersion),
		binaryUpgradeStep(c.Node(1), currentVersion),

		tryReparentingDatabase(false, ""),

		binaryUpgradeStep(c.Node(2), currentVersion),

		createDatabaseStep("test2"),

		allowAutoUpgradeStep(1),
		waitForUpgradeStep(c.All()),

		tryReparentingDatabase(true, "pq: cannot perform ALTER DATABASE CONVERT TO SCHEMA"),

		createDatabaseStep("test3"),

		createTableInDatabasePublicSchema("test1"),
		createTableInDatabasePublicSchema("test2"),
		createTableInDatabasePublicSchema("test3"),

		insertIntoTable("test1"),
		insertIntoTable("test2"),
		insertIntoTable("test3"),

		selectFromTable("test1"),
		selectFromTable("test2"),
		selectFromTable("test3"),

		dropTableInDatabase("test1"),
		dropTableInDatabase("test2"),
		dropTableInDatabase("test3"),
	)

	newVersionUpgradeTest(c, steps...).run(ctx, t)
}

func createDatabaseStep(dbName string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, t.L(), loadNode)
		defer func() {
			_ = conn.Close()
		}()
		require.NoError(t, err)
		_, err = conn.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName))
		require.NoError(t, err)
	}
}

func createTableInDatabasePublicSchema(dbName string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, t.L(), loadNode)
		defer func() {
			_ = conn.Close()
		}()
		require.NoError(t, err)
		_, err = conn.Exec(fmt.Sprintf("CREATE TABLE %s.public.t(x INT)", dbName))
		require.NoError(t, err)
	}
}

func dropTableInDatabase(dbName string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, t.L(), loadNode)
		defer func() {
			_ = conn.Close()
		}()
		require.NoError(t, err)
		_, err = conn.Exec(fmt.Sprintf("DROP TABLE %s.public.t", dbName))
		require.NoError(t, err)
	}
}

func insertIntoTable(dbName string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, t.L(), loadNode)
		defer func() {
			_ = conn.Close()
		}()
		require.NoError(t, err)
		_, err = conn.Exec(fmt.Sprintf("INSERT INTO %s.public.t VALUES (0), (1), (2)", dbName))
		require.NoError(t, err)
	}
}

func selectFromTable(dbName string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, t.L(), loadNode)
		defer func() {
			_ = conn.Close()
		}()
		require.NoError(t, err)
		rows, err := conn.Query(fmt.Sprintf("SELECT x FROM %s.public.t ORDER BY x", dbName))
		defer func() {
			_ = rows.Close()
		}()
		require.NoError(t, err)
		numRows := 3
		var x int
		for i := 0; i < numRows; i++ {
			rows.Next()
			err := rows.Scan(&x)
			require.NoError(t, err)
			require.Equal(t, x, i)
		}
	}
}

func tryReparentingDatabase(shouldError bool, errRe string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, t.L(), loadNode)
		defer func() {
			_ = conn.Close()
		}()
		require.NoError(t, err)
		_, err = conn.Exec("CREATE DATABASE to_reparent;")
		require.NoError(t, err)
		_, err = conn.Exec("CREATE DATABASE new_parent")
		require.NoError(t, err)

		_, err = conn.Exec("ALTER DATABASE to_reparent CONVERT TO SCHEMA WITH PARENT new_parent;")

		if !shouldError {
			require.NoError(t, err)
		} else {
			if !testutils.IsError(err, errRe) {
				t.Fatalf("expected error '%s', got: %s", errRe, pgerror.FullError(err))
			}

			_, err = conn.Exec("DROP DATABASE to_reparent")
			require.NoError(t, err)
		}

		_, err = conn.Exec("DROP DATABASE new_parent")
		require.NoError(t, err)
	}
}
