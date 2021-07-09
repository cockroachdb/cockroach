// Copyright 2018 The Cockroach Authors.
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
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

var typeORMReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)
var supportedTypeORMRelease = "0.2.32"

// This test runs TypeORM's full test suite against a single cockroach node.
func registerTypeORM(r registry.Registry) {
	runTypeORM := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
	) {
		if c.IsLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
		c.Start(ctx, c.All())

		version, err := fetchCockroachVersion(ctx, c, node[0], nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(
			ctx, version, c, node[0], nil,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning TypeORM and installing prerequisites")
		latestTag, err := repeatGetLatestTag(ctx, t, "typeorm", "typeorm", typeORMReleaseTagRegex)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest TypeORM release is %s.", latestTag)
		t.L().Printf("Supported TypeORM release is %s.", supportedTypeORMRelease)

		if err := repeatRunE(
			ctx, t, c, node, "maybe fix python3-apt",
			`sudo apt-get install --reinstall -y python3-apt`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "update apt-get", `sudo apt-get update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install dependencies",
			`sudo apt-get install -y make python3 libpq-dev python-dev gcc g++ `+
				`software-properties-common build-essential`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"add nodesource repository",
			`curl -sL https://deb.nodesource.com/setup_12.x | sudo -E bash -`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "install nodejs and npm", `sudo apt-get install -y nodejs`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "update npm", `sudo npm i -g npm`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "remove old TypeORM", `sudo rm -rf /mnt/data1/typeorm`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/typeorm/typeorm.git",
			"/mnt/data1/typeorm",
			supportedTypeORMRelease,
			node,
		); err != nil {
			t.Fatal(err)
		}

		// TypeORM is super picky about this file format and if it cannot be parsed
		// it will return a file not found error.
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"configuring tests for cockroach only",
			fmt.Sprintf("echo '%s' > /mnt/data1/typeorm/ormconfig.json", typeORMConfigJSON),
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"patch TypeORM test script to run all tests even on failure",
			`sed -i 's/--bail //' /mnt/data1/typeorm/package.json`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"building TypeORM",
			`cd /mnt/data1/typeorm/ && sudo npm install --unsafe-perm=true --allow-root`,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("running TypeORM test suite - approx 12 mins")
		rawResults, err := c.RunWithBuffer(ctx, t.L(), node,
			`cd /mnt/data1/typeorm/ && sudo npm test --unsafe-perm=true --allow-root`,
		)
		rawResultsStr := string(rawResults)
		t.L().Printf("Test Results: %s", rawResultsStr)
		if err != nil {
			// Ignore the failure discussed in #38180 and in
			// https://github.com/typeorm/typeorm/pull/4298.
			// TODO(jordanlewis): remove this once the failure is resolved.
			if t.IsBuildVersion("v19.2.0") &&
				strings.Contains(rawResultsStr, "1 failing") &&
				strings.Contains(rawResultsStr, "AssertionError: expected 2147483647 to equal '2147483647'") {
				err = nil
			}
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	r.Add(registry.TestSpec{
		Name:    "typeorm",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1),
		Tags:    []string{`default`, `orm`},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTypeORM(ctx, t, c)
		},
	})
}

// This full config is required, but notice that all the non-cockroach databases
// are set to skip.  Some of the unit tests look for a specific config, like
// sqlite and will fail if it is not present.
const typeORMConfigJSON = `
[
  {
    "skip": true,
    "name": "mysql",
    "type": "mysql",
    "host": "localhost",
    "port": 3306,
    "username": "root",
    "password": "admin",
    "database": "test",
    "logging": false
  },
  {
    "skip": true,
    "name": "mariadb",
    "type": "mariadb",
    "host": "localhost",
    "port": 3307,
    "username": "root",
    "password": "admin",
    "database": "test",
    "logging": false
  },
  {
    "skip": true,
    "name": "sqlite",
    "type": "sqlite",
    "database": "temp/sqlitedb.db",
    "logging": false
  },
  {
    "skip": true,
    "name": "postgres",
    "type": "postgres",
    "host": "localhost",
    "port": 5432,
    "username": "test",
    "password": "test",
    "database": "test",
    "logging": false
  },
  {
    "skip": true,
    "name": "mssql",
    "type": "mssql",
    "host": "localhost",
    "username": "sa",
    "password": "Admin12345",
    "database": "tempdb",
    "logging": false
  },
  {
    "skip": true,
    "name": "oracle",
    "type": "oracle",
    "host": "localhost",
    "username": "system",
    "password": "oracle",
    "port": 1521,
    "sid": "xe.oracle.docker",
    "logging": false
  },
  {
    "skip": false,
    "name": "cockroachdb",
    "type": "cockroachdb",
    "host": "localhost",
    "port": 26257,
    "username": "root",
    "password": "",
    "database": "defaultdb"
  },
  {
    "skip": true,
    "disabledIfNotEnabledImplicitly": true,
    "name": "mongodb",
    "type": "mongodb",
    "database": "test",
    "logging": false,
    "useNewUrlParser": true
  }
]
`
