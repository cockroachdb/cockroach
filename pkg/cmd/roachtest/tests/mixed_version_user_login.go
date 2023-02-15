// Copyright 2023 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
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
	if nodeCount != 1 {
		t.Fatal("nodeCount should be exactly 1")
	}

	const mainVersion = ""
	const userName = "nobody"

	prevStableVersionStr, err := PredecessorVersion(*t.BuildVersion())
	if err != nil {
		t.Fatal(err)
	}
	t.L().Printf("buildVersion: %s\nprevStableVersionStr: %s", t.BuildVersion().String(), prevStableVersionStr)

	checkCurrentVersionStep := func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn := u.conn(ctx, t, 1)
		var res string
		if err := conn.QueryRowContext(ctx, `SELECT version()`).Scan(&res); err != nil {
			t.Fatal(err)
		}
		t.L().Printf("current version: %s", res)
	}

	createUserStep := func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn := u.conn(ctx, t, 1)
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`CREATE USER %s`, userName)); err != nil {
			t.Fatal(err)
		}
		t.L().Printf("created user %s", userName)
	}

	connAsNewUserStep := func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnEAsUser(ctx, t.L(), 1, userName)
		defer func() {
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
		}()
		if err != nil {
			t.Fatal(err)
		}
		var res string
		if err := conn.QueryRowContext(ctx, `SELECT current_user()`).Scan(&res); err != nil {
			t.Fatal(err)
		}
		if res != userName {
			t.Fatalf("expected username: %s, got: %s", userName, res)
		}
		t.L().Printf("connected to the db with user %s", res)
	}

	u := newVersionUpgradeTest(c,
		// Have all nodes in the predecessor stable version.
		binaryUpgradeStep(c.All(), prevStableVersionStr),
		waitForUpgradeStep(c.All()),
		checkCurrentVersionStep,
		// Create a new user and set cluster.preserve_downgrade_option.
		createUserStep,
		preventAutoUpgradeStep(1),
		// Upgrade all nodes to the latest version.
		binaryUpgradeStep(c.All(), mainVersion),
		checkCurrentVersionStep,
		// Connect to the servers as the new user.
		connAsNewUserStep,
	)
	u.run(ctx, t)
}
