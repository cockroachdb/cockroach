// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/stretchr/testify/require"
)

// registerUserLoginMixedVersion is a test for a regression
// (https://github.com/cockroachdb/cockroach/issues/97178) due to the lack of
// a version gate over the system.privilege table.
func registerUserLoginMixedVersion(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "user-login/mixed-version",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1),
		Run:     runUserLoginMixedVersion,
		Timeout: 30 * time.Minute,
	})
}

func runUserLoginMixedVersion(ctx context.Context, t test.Test, c cluster.Cluster) {
	nodeCount := c.Spec().NodeCount
	require.Equal(t, 1, nodeCount)

	const mainVersion = ""
	const userName = "nobody"

	prevStableVersionStr, err := PredecessorVersion(*t.BuildVersion())

	require.NoError(t, err)
	t.L().Printf("buildVersion: %s\nprevStableVersionStr: %s", t.BuildVersion().String(), prevStableVersionStr)

	prevStableVersion, err := version.Parse(fmt.Sprintf("v%s", prevStableVersionStr))
	require.NoError(t, err)

	checkCurrentVersionStep := func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn := u.c.Conn(ctx, t.L(), 1)
		defer conn.Close()
		var res string
		require.NoError(t, conn.QueryRowContext(ctx, `SELECT version()`).Scan(&res))
		t.L().Printf("current version: %s", res)
	}

	createUserStep := func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn := u.c.Conn(ctx, t.L(), 1)
		defer conn.Close()
		_, err := conn.ExecContext(ctx, fmt.Sprintf(`CREATE USER %s`, userName))
		require.NoError(t, err)
		t.L().Printf("created user %s", userName)
	}

	setPreserveDowngradeOptionStep := func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn := u.c.Conn(ctx, t.L(), 1)
		defer conn.Close()
		_, err := conn.ExecContext(ctx, fmt.Sprintf(`SET CLUSTER SETTING cluster.preserve_downgrade_option = '%d.%d'`, prevStableVersion.Major(), prevStableVersion.Minor()))
		require.NoError(t, err)
		t.L().Printf("finished setting cluster.preserve_downgrade_option")
	}

	connAsNewUserStep := func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnEAsUser(ctx, t.L(), 1, userName)
		require.NoError(t, err)
		var res string
		require.NoError(t, conn.QueryRowContext(ctx, `SELECT current_user()`).Scan(&res))
		require.Equal(t, userName, res)
		t.L().Printf("connected to the db with user %s", res)
	}

	u := newVersionUpgradeTest(c,
		// Have all nodes in the predecessor stable version.
		binaryUpgradeStep(c.All(), prevStableVersionStr),
		waitForUpgradeStep(c.All()),
		checkCurrentVersionStep,
		// Create a new user and set cluster.preserve_downgrade_option.
		createUserStep,
		setPreserveDowngradeOptionStep,
		// Upgrade all nodes to the latest version.
		binaryUpgradeStep(c.All(), mainVersion),
		checkCurrentVersionStep,
		// Connect to the servers as the new user.
		connAsNewUserStep,
	)

	u.run(ctx, t)
}
