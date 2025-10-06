// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

// TODO(richardjcai): Update this to use the repo owned by brianc once
// https://github.com/brianc/node-postgres/pull/2517 is fixed.
// Currently we cannot pass certs through PG env vars, the PR fixes it.
var repoOwner = "richardjcai"
var supportedBranch = "allowing_passing_certs_through_pg_env"

func registerNodeJSPostgres(r registry.Registry) {
	runNodeJSPostgres := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
	) {
		if c.IsLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		settings := install.MakeClusterSettings()
		err := c.StartE(ctx, t.L(), option.NewStartOpts(sqlClientsInMemoryDB), settings)
		require.NoError(t, err)

		err = repeatRunE(ctx, t, c, node, "create test database",
			fmt.Sprintf(
				`./cockroach sql --certs-dir %s --port={pgport:1} -e "CREATE DATABASE postgres_node_test"`, install.CockroachNodeCertsDir,
			))
		require.NoError(t, err)

		version, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
		require.NoError(t, err)

		err = alterZoneConfigAndClusterSettings(ctx, t, version, c, node[0])
		require.NoError(t, err)

		// In case we are running into a state where machines are being reused, we first check to see if we
		// can use npm to reduce the potential of trying to add another nodesource key
		// (preventing gpg: dearmoring failed: File exists) errors.
		err = c.RunE(
			ctx, option.WithNodes(node), `sudo npm i -g npm`,
		)

		if err != nil {
			err = repeatRunE(
				ctx,
				t,
				c,
				node,
				"add nodesource key and deb repository",
				`
sudo apt-get update && \
sudo apt-get install -y ca-certificates curl gnupg && \
sudo mkdir -p /etc/apt/keyrings && \
curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | sudo gpg --batch --dearmor -o /etc/apt/keyrings/nodesource.gpg && \
echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_18.x nodistro main" | sudo tee /etc/apt/sources.list.d/nodesource.list`,
			)
			require.NoError(t, err)

			err = repeatRunE(
				ctx, t, c, node, "install nodejs and npm", `sudo apt-get update && sudo apt-get -qq install nodejs`,
			)
			require.NoError(t, err)

			err = repeatRunE(
				ctx, t, c, node, "update npm", `sudo npm i -g npm`,
			)
			require.NoError(t, err)
		}

		err = repeatRunE(
			ctx, t, c, node, "install yarn", `sudo npm i -g yarn`,
		)
		require.NoError(t, err)

		err = repeatRunE(
			ctx, t, c, node, "install lerna", `sudo npm i --g lerna`,
		)
		require.NoError(t, err)

		err = repeatRunE(
			ctx, t, c, node, "remove old node-postgres", `sudo rm -rf /mnt/data1/node-postgres`,
		)
		require.NoError(t, err)

		err = repeatGitCloneE(
			ctx,
			t,
			c,
			fmt.Sprintf("https://github.com/%s/node-postgres.git", repoOwner),
			"/mnt/data1/node-postgres",
			supportedBranch,
			node,
		)
		require.NoError(t, err)

		// The upstream repo hasn't updated its dependencies in light of
		// https://github.blog/2021-09-01-improving-git-protocol-security-github/
		// so we need this configuration.
		err = repeatRunE(
			ctx, t, c, node, "configure git to avoid unauthenticated protocol",
			`cd /mnt/data1/node-postgres && sudo git config --global url."https://github".insteadOf "git://github"`,
		)
		require.NoError(t, err)

		err = repeatRunE(
			ctx,
			t,
			c,
			node,
			"building node-postgres",
			`cd /mnt/data1/node-postgres/ && sudo yarn && sudo yarn lerna bootstrap`,
		)
		require.NoError(t, err)

		t.Status("running node-postgres tests")
		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(node),
			fmt.Sprintf(
				`cd /mnt/data1/node-postgres/ && sudo \
PGPORT={pgport:1} PGUSER=%[1]s PGPASSWORD=%[2]s PGSSLMODE=require PGDATABASE=postgres_node_test \
PGSSLCERT=$HOME/certs/client.%[1]s.crt PGSSLKEY=$HOME/certs/client.%[1]s.key PGSSLROOTCERT=$HOME/certs/ca.crt yarn test`,
				install.DefaultUser, install.DefaultPassword,
			),
		)

		// Fatal for a roachprod or transient error. A roachprod error is when result.Err==nil.
		// Proceed for any other (command) errors
		if err != nil && (result.Err == nil || rperrors.IsTransient(err)) {
			t.Fatal(err)
		}

		rawResultsStr := result.Stdout + result.Stderr
		t.L().Printf("Test Results: %s", rawResultsStr)
		if err != nil {
			// The one failing test is `pool size of 1` which
			// fails because it does SELECT count(*) FROM pg_stat_activity which is
			// not implemented in CRDB.
			if strings.Contains(rawResultsStr, "1 failing") &&
				// Failing tests are listed numerically, we only expect one.
				// The one failing test should be "pool size of 1".
				strings.Contains(rawResultsStr, "1) pool size of 1") {
				err = nil
			}
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	r.Add(registry.TestSpec{
		Name:             "node-postgres",
		Owner:            registry.OwnerSQLFoundations,
		Cluster:          r.MakeClusterSpec(1),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly, registry.Driver),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runNodeJSPostgres(ctx, t, c)
		},
	})
}
