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
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
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
		err := c.PutE(ctx, t.L(), t.Cockroach(), "./cockroach", c.All())
		require.NoError(t, err)
		err = c.StartE(ctx, option.StartArgs("--secure"))
		require.NoError(t, err)

		user := "testuser"
		certsDir := "/home/ubuntu/certs"
		localCertsDir, err := filepath.Abs("./certs")
		require.NoError(t, err)

		err = repeatRunE(ctx, t, c, node, "create sql user",
			fmt.Sprintf(
				`./cockroach sql --certs-dir %s -e "CREATE USER %s CREATEDB"`,
				certsDir, user,
			))
		require.NoError(t, err)

		err = repeatRunE(ctx, t, c, c.All(), "create user certs",
			fmt.Sprintf(`./cockroach cert create-client testuser --certs-dir %s --ca-key=%s/ca.key`,
				certsDir, certsDir))
		require.NoError(t, err)

		err = repeatRunE(ctx, t, c, node, "create test database",
			fmt.Sprintf(`./cockroach sql --certs-dir %s -e "CREATE DATABASE postgres_node_test"`, certsDir),
		)
		require.NoError(t, err)

		err = os.RemoveAll(localCertsDir)
		require.NoError(t, err)

		err = c.Get(ctx, t.L(), certsDir, localCertsDir)
		require.NoError(t, err)

		// Certs can have at max 0600 privilege.
		err = filepath.Walk(localCertsDir, func(path string, info os.FileInfo, err error) error {
			// Don't change permissions for the certs directory.
			if path == localCertsDir {
				return nil
			}
			if err != nil {
				return err
			}
			return os.Chmod(path, os.FileMode(0600))
		})
		require.NoError(t, err)

		version, err := fetchCockroachVersion(
			ctx, c, node[0], NewSecureDBConnectionParams(user, localCertsDir, 26257),
		)
		require.NoError(t, err)

		err = alterZoneConfigAndClusterSettings(
			ctx, version, c, node[0],
			NewSecureDBConnectionParams("root", localCertsDir, 26257),
		)
		require.NoError(t, err)

		err = repeatRunE(
			ctx,
			t,
			c,
			node,
			"add nodesource repository",
			`curl -sL https://deb.nodesource.com/setup_12.x | sudo -E bash -`,
		)
		require.NoError(t, err)

		err = repeatRunE(
			ctx, t, c, node, "install nodejs and npm", `sudo apt-get -qq install nodejs`,
		)
		require.NoError(t, err)

		err = repeatRunE(
			ctx, t, c, node, "update npm", `sudo npm i -g npm`,
		)
		require.NoError(t, err)

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
		// Ignore the error, this is expected to fail.
		rawResults, err := c.RunWithBuffer(ctx, t.L(), node,
			fmt.Sprintf(
				`cd /mnt/data1/node-postgres/ && sudo \
PGPORT=26257 PGUSER=%s PGSSLMODE=require PGDATABASE=postgres_node_test \
PGSSLCERT=%s/client.%s.crt PGSSLKEY=%s/client.%s.key PGSSLROOTCERT=%s/ca.crt yarn test`,
				user, certsDir, user, certsDir, user, certsDir,
			),
		)
		rawResultsStr := string(rawResults)
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
		Name:    "node-postgres",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1),
		Tags:    []string{`default`, `driver`},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runNodeJSPostgres(ctx, t, c)
		},
	})
}
