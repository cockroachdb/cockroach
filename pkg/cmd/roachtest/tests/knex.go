// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/require"
)

// WARNING: DO NOT MODIFY the name of the below constant/variable without approval from the docs team.
// This is used by docs automation to produce a list of supported versions for ORM's.
const supportedKnexTag = "2.5.1"

// Embed the config file, so we don't need to know where it is
// relative to the roachtest runner, just relative to this test.
// This way we can still find it if roachtest changes paths.
//
//go:embed knexfile.js
var knexfile string

// This test runs one of knex's test suite against a single cockroach
// node.

func registerKnex(r registry.Registry) {
	runKnex := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
	) {
		if c.IsLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Start(ctx, t.L(), option.NewStartOpts(sqlClientsInMemoryDB), install.MakeClusterSettings())

		version, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
		require.NoError(t, err)

		err = alterZoneConfigAndClusterSettings(ctx, t, version, c, node[0])
		require.NoError(t, err)

		err = repeatRunE(
			ctx,
			t,
			c,
			node,
			"create sql database",
			`./cockroach sql --url={pgurl:1} -e "CREATE DATABASE test"`,
		)
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
			ctx, t, c, node, "install mocha", `sudo npm i -g mocha`,
		)
		require.NoError(t, err)

		err = repeatRunE(
			ctx, t, c, node, "remove old knex", `sudo rm -rf /mnt/data1/knex`,
		)
		require.NoError(t, err)

		err = repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/knex/knex.git",
			"/mnt/data1/knex",
			supportedKnexTag,
			node,
		)
		require.NoError(t, err)

		err = repeatRunE(
			ctx, t, c, node, "install knex npm dependencies", `cd /mnt/data1/knex/ && npm i`,
		)
		require.NoError(t, err)

		// Write the knexfile test config into the test suite to use.
		// The default test config does not support ssl connections.
		err = c.PutString(ctx, knexfile, "/mnt/data1/knex/knexfile.js", 0755, c.Node(1))
		require.NoError(t, err)

		t.Status("running knex tests")
		result, err := c.RunWithDetailsSingleNode(
			ctx,
			t.L(),
			option.WithNodes(node),
			fmt.Sprintf(`cd /mnt/data1/knex/ && PGUSER=%s PGPASSWORD=%s PGPORT={pgport:1} PGSSLROOTCERT=$HOME/%s/ca.crt \
				KNEX_TEST='/mnt/data1/knex/knexfile.js' DB='cockroachdb' npm test`,
				install.DefaultUser, install.DefaultPassword, install.CockroachNodeCertsDir),
		)
		rawResultsStr := result.Stdout + result.Stderr
		t.L().Printf("Test Results: %s", rawResultsStr)
		if err != nil {
			// We don't have a good way of parsing test results from javascript, so
			// we do substring matching instead.
			// - (1) and (2) ignore failures from a test that expects `DELETE FROM
			//   ... USING` syntax to fail (https://github.com/cockroachdb/cockroach/issues/40963).
			//   This can be removed once the upstream knex repo updates to test with
			//   v23.1.
			// - (3) ignores a failure caused by our use of the autocommit_before_ddl
			//   setting. It does a migration then checks the transaction is still
			//   opened. Since those include DDL, it was committed, which is unexpected.
			// - Like (3), (4) is related to autocommit_before_ddl. The test drops a
			//   primary key and then re-adds it in the same transaction but fails.
			if !strings.Contains(rawResultsStr, "1) should handle basic delete with join") ||
				!strings.Contains(rawResultsStr, "2) should handle returning") ||
				!strings.Contains(rawResultsStr, "3) should not create column for invalid migration with transaction enabled") ||
				!strings.Contains(rawResultsStr, "4) #1430 - .primary() & .dropPrimary() same for all dialects") ||
				strings.Contains(rawResultsStr, " 5) ") {
				t.Fatal(err)
			}
		}
	}

	r.Add(registry.TestSpec{
		Name:  "knex",
		Owner: registry.OwnerSQLFoundations,
		// Requires a pre-built node-oracledb binary for linux arm64.
		Cluster:          r.MakeClusterSpec(1, spec.Arch(vm.ArchAMD64)),
		Leases:           registry.MetamorphicLeases,
		NativeLibs:       registry.LibGEOS,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly, registry.ORM),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runKnex(ctx, t, c)
		},
	})
}
