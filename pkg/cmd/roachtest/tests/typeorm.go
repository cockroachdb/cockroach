// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

var typeORMReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)

// Use 0.3.18 from the upstream repo once it is released.
// WARNING: DO NOT MODIFY the name of the below constant/variable without approval from the docs team.
// This is used by docs automation to produce a list of supported versions for ORM's.
const supportedTypeORMRelease = "remove-unsafe-crdb-setting"
const typeORMRepo = "https://github.com/rafiss/typeorm.git"

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
		c.Start(ctx, t.L(), option.NewStartOpts(sqlClientsInMemoryDB), install.MakeClusterSettings(), c.All())

		cockroachVersion, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, t, cockroachVersion, c, node[0]); err != nil {
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
			ctx, t, c, node, "purge apt-get",
			`sudo apt-get purge -y command-not-found`,
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
			`sudo apt-get install -y make python3 libpq-dev gcc g++ `+
				`software-properties-common build-essential`,
		); err != nil {
			t.Fatal(err)
		}

		// In case we are running into a state where machines are being reused, we first check to see if we
		// can use npm to reduce the potential of trying to add another nodesource key
		// (preventing gpg: dearmoring failed: File exists) errors.
		if err := c.RunE(
			ctx, option.WithNodes(node), `sudo npm i -g npm`,
		); err != nil {
			if err := repeatRunE(
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
			); err != nil {
				t.Fatal(err)
			}

			if err := repeatRunE(
				ctx, t, c, node, "install nodejs and npm", `sudo apt-get update && sudo apt-get -qq install nodejs`,
			); err != nil {
				t.Fatal(err)
			}

			if err := repeatRunE(
				ctx, t, c, node, "update npm", `sudo npm i -g npm`,
			); err != nil {
				t.Fatal(err)
			}
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
			typeORMRepo,
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
			`cd /mnt/data1/typeorm/ && npm install`,
		); err != nil {
			t.Fatal(err)
		}

		// We have to pass in the root cert with NODE_EXTRA_CA_CERTS because the JSON
		// config only accepts the actual certificate contents and not a path.
		t.Status("running TypeORM test suite - approx 2 mins")
		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(node),
			`cd /mnt/data1/typeorm/ && NODE_EXTRA_CA_CERTS=$HOME/certs/ca.crt npm test`,
		)
		rawResults := result.Stdout + result.Stderr
		t.L().Printf("Test Results: %s", rawResults)
		if err != nil {
			// We don't have a good way of parsing test results from javascript, so we
			// use substring matching and regexp instead of using a blocklist like
			// we use for other ORM tests.
			numFailingRegex := regexp.MustCompile(`(\d+) failing`)
			matches := numFailingRegex.FindStringSubmatch(rawResults)
			numFailing, convErr := strconv.Atoi(matches[1])
			if convErr != nil {
				t.Fatal(convErr)
			}

			// One test is known to flake during setup.
			if strings.Contains(rawResults, `"before each" hook for "should select specific columns":`) {
				numFailing -= 1
			}

			// Tests are allowed to flake due to transaction retry errors.
			txnRetryErrCount := strings.Count(rawResults, "restart transaction")
			if numFailing == txnRetryErrCount {
				err = nil
			}

			if err != nil {
				t.Fatal(err)
			}
		}
	}

	r.Add(registry.TestSpec{
		Name:             "typeorm",
		Owner:            registry.OwnerSQLFoundations,
		Cluster:          r.MakeClusterSpec(1),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly, registry.ORM),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTypeORM(ctx, t, c)
		},
	})
}

// This full config is required, but notice that all the non-cockroach databases
// are set to skip.  Some of the unit tests look for a specific config, like
// sqlite and will fail if it is not present.
var typeORMConfigJSON = fmt.Sprintf(`
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
    "port": {pgport:1},
    "username": "%s",
    "password": "%s",
    "database": "defaultdb",
    "ssl": true,
      "extra": {
        "ssl": {
          "rejectUnauthorized": true
        }
     }
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
`, install.DefaultUser, install.DefaultPassword)
