package tests

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/stretchr/testify/require"
)

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

	const (
		mainVersion = ""
		preVersion  = "22.1.13"
	)

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
		_, err := conn.ExecContext(ctx, `CREATE USER nobody`)
		require.NoError(t, err)
		t.L().Printf("created user nobody")
	}

	setPreserveDowngradeOptionStep := func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn := u.c.Conn(ctx, t.L(), 1)
		defer conn.Close()
		_, err := conn.ExecContext(ctx, `SET CLUSTER SETTING cluster.preserve_downgrade_option = '22.1'`)
		require.NoError(t, err)
		t.L().Printf("SET CLUSTER SETTING cluster.preserve_downgrade_option")
	}

	connAsNewUserStep := func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnEAsUser(ctx, t.L(), 1, "nobody")
		require.NoError(t, err)
		var res string
		require.NoError(t, conn.QueryRowContext(ctx, `SELECT current_user()`).Scan(&res))
		t.L().Printf("connected to the db with user %s", res)
	}

	u := newVersionUpgradeTest(c,
		// Have all nodes in 22.1.14.
		binaryUpgradeStep(c.All(), preVersion),
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
