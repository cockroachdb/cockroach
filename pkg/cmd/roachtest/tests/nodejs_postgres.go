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

		// Install NodeJS 18.x, update NPM to the latest
		// and install Yarn and Lerna.
		err = installNode18(ctx, t, c, node, nodeOpts{withYarn: true, withLerna: true})
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
			// Check for expected test failures. We allow:
			// 1. One failing test that is "pool size of 1"
			// 2. One failing test that is "events"
			// 3. Two failing tests that are exactly "events" and "pool size of 1"
			if strings.Contains(rawResultsStr, "1 failing") {
				// Single test failure case
				if strings.Contains(rawResultsStr, "1) pool size of 1") ||
					strings.Contains(rawResultsStr, "1) events") {
					err = nil
				}
			} else if strings.Contains(rawResultsStr, "2 failing") {
				// Two test failures case - must be exactly events and pool size of 1
				if strings.Contains(rawResultsStr, "1) events") &&
					strings.Contains(rawResultsStr, "2) pool size of 1") {
					err = nil
				}
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
		CompatibleClouds: registry.AllClouds.NoAWS().NoIBM(),
		Suites:           registry.Suites(registry.Nightly, registry.Driver),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runNodeJSPostgres(ctx, t, c)
		},
	})
}
