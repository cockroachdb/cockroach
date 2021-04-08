// Copyright 2021 The Cockroach Authors.
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
	"os"
	"path/filepath"
	"strings"

	"github.com/stretchr/testify/require"
)

// TODO(richardjcai): Update this to use the repo owned by brianc once
// https://github.com/brianc/node-postgres/pull/2517 is fixed.
// Currently we cannot pass certs through PG env vars, the PR fixes it.
var repoOwner = "richardjcai"
var supportedBranch = "allowing_passing_certs_through_pg_env"

func registerPostgresNode(r *testRegistry) {
	runPostgresNodeTest := func(
		ctx context.Context,
		t *test,
		c *cluster,
	) {
		if c.isLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		err := c.PutE(ctx, t.l, cockroach, "./cockroach", c.All())
		require.NoError(t, err)
		c.StartE(ctx, startArgs("--secure"))
		require.NoError(t, err)

		user := "testuser"
		certsDir := "/home/ubuntu/certs"
		localCertsDir, err := filepathAbs("./certs")
		require.NoError(t, err)

		err = c.RunE(ctx, node,
			fmt.Sprintf(
				`./cockroach sql --certs-dir %s -e "CREATE USER %s CREATEDB"`,
				certsDir, user,
			))
		require.NoError(t, err)

		err = c.RunE(ctx, c.All(),
			fmt.Sprintf(`./cockroach cert create-client testuser --certs-dir %s --ca-key=%s/ca.key`,
				certsDir, certsDir))
		require.NoError(t, err)

		err = c.RunE(ctx, node,
			fmt.Sprintf(`./cockroach sql --certs-dir %s -e "CREATE DATABASE postgres_node_test"`, certsDir),
		)
		require.NoError(t, err)

		err = os.RemoveAll(localCertsDir)
		require.NoError(t, err)

		err = c.Get(ctx, t.l, certsDir, localCertsDir)
		require.NoError(t, err)

		filepath.Walk(localCertsDir, func(path string, info os.FileInfo, err error) error {
			// Don't change permissions for the certs directory.
			if path == localCertsDir {
				return nil
			}
			if err != nil {
				return err
			}
			return os.Chmod(path, os.FileMode(0600))
		})

		urls := c.ExternalPGUrlSecure(ctx, node, user, certsDir, "26257")
		fmt.Println(urls[0])

		version, err := fetchCockroachVersion(ctx, c, node[0], user, localCertsDir, "26257")
		require.NoError(t, err)

		err = alterZoneConfigAndClusterSettings(ctx, version, c, node[0], "root", localCertsDir, "26257")
		require.NoError(t, err)

		err = repeatRunE(
			ctx,
			c,
			node,
			"add nodesource repository",
			`curl -sL https://deb.nodesource.com/setup_12.x | sudo -E bash -`,
		)
		require.NoError(t, err)

		err = repeatRunE(
			ctx, c, node, "install nodejs and npm", `sudo apt-get -qq install nodejs`,
		)
		require.NoError(t, err)

		err = repeatRunE(
			ctx, c, node, "update npm", `sudo npm i -g npm`,
		)
		require.NoError(t, err)

		err = repeatRunE(
			ctx, c, node, "install yarn", `sudo npm i -g yarn`,
		)
		require.NoError(t, err)

		err = repeatRunE(
			ctx, c, node, "install lerna", `sudo npm i --g lerna`,
		)
		require.NoError(t, err)

		err = repeatRunE(
			ctx, c, node, "remove old postgres-node", `sudo rm -rf /mnt/data1/postgres-node`,
		)
		require.NoError(t, err)

		err = repeatGitCloneE(
			ctx,
			t.l,
			c,
			fmt.Sprintf("https://github.com/%s/node-postgres.git", repoOwner),
			"/mnt/data1/postgres-node",
			supportedBranch,
			node,
		)
		require.NoError(t, err)

		err = repeatRunE(
			ctx,
			c,
			node,
			"building postgres-node",
			`cd /mnt/data1/postgres-node/ && sudo yarn && sudo yarn lerna bootstrap`,
		)
		require.NoError(t, err)

		t.Status("running postgres-node tests")
		// Ignore the error, this is expected to fail.
		rawResults, err := c.RunWithBuffer(ctx, t.l, node,
			fmt.Sprintf(
				`cd /mnt/data1/postgres-node/ && sudo \
PGPORT=26257 PGUSER=%s PGSSLMODE=require PGDATABASE=postgres_node_test \
PGSSLCERT=%s/client.%s.crt PGSSLKEY=%s/client.%s.key PGSSLROOTCERT=%s/ca.crt yarn test`,
				user, certsDir, user, certsDir, user, certsDir,
			),
		)
		rawResultsStr := string(rawResults)
		c.l.Printf("Test Results: %s", rawResultsStr)
		if err != nil {
			// The one failing test is `pool size of 1` which
			// fails because it does SELECT count(*) FROM pg_stat_activity which is
			// not implemented in CRDB.
			if strings.Contains(rawResultsStr, "1 failing") &&
				strings.Contains(rawResultsStr, "Error: expected [ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 ] to sort of equal [ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 ]") {
				err = nil
			}
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	r.Add(testSpec{
		Name:       "postgres-node",
		Owner:      OwnerSQLExperience,
		Cluster:    makeClusterSpec(1),
		MinVersion: "v20.1.0",
		Tags:       []string{`default`, `orm`},
		Run: func(ctx context.Context, t *test, c *cluster) {
			runPostgresNodeTest(ctx, t, c)
		},
	})
}
