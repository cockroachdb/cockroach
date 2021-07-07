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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

var hibernateReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)
var supportedHibernateTag = "5.4.30"

type hibernateOptions struct {
	testName string
	testDir  string
	buildCmd,
	testCmd string
	blocklists  blocklistsForVersion
	dbSetupFunc func(ctx context.Context, t test.Test, c cluster.Cluster)
}

var (
	hibernateOpts = hibernateOptions{
		testName: "hibernate",
		testDir:  "hibernate-core",
		buildCmd: `cd /mnt/data1/hibernate/hibernate-core/ && ./../gradlew test -Pdb=cockroachdb ` +
			`--tests org.hibernate.jdbc.util.BasicFormatterTest.*`,
		testCmd:     "cd /mnt/data1/hibernate/hibernate-core/ && ./../gradlew test -Pdb=cockroachdb",
		blocklists:  hibernateBlocklists,
		dbSetupFunc: nil,
	}
	hibernateSpatialOpts = hibernateOptions{
		testName: "hibernate-spatial",
		testDir:  "hibernate-spatial",
		buildCmd: `cd /mnt/data1/hibernate/hibernate-spatial/ && ./../gradlew test -Pdb=cockroachdb_spatial ` +
			`--tests org.hibernate.spatial.dialect.postgis.*`,
		testCmd: `cd /mnt/data1/hibernate/hibernate-spatial && ` +
			`HIBERNATE_CONNECTION_LEAK_DETECTION=true ./../gradlew test -Pdb=cockroachdb_spatial`,
		blocklists: hibernateSpatialBlocklists,
		dbSetupFunc: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			db := c.Conn(ctx, 1)
			defer db.Close()
			if _, err := db.ExecContext(
				ctx,
				"SET CLUSTER SETTING sql.spatial.experimental_box2d_comparison_operators.enabled = on",
			); err != nil {
				t.Fatal(err)
			}
		},
	}
)

// This test runs one of hibernate's test suite against a single cockroach
// node.

func registerHibernate(r registry.Registry, opt hibernateOptions) {
	runHibernate := func(
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
		if err := c.PutLibraries(ctx, "./lib"); err != nil {
			t.Fatal(err)
		}
		c.Start(ctx, c.All())

		if opt.dbSetupFunc != nil {
			opt.dbSetupFunc(ctx, t, c)
		}

		version, err := fetchCockroachVersion(ctx, c, node[0], nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(
			ctx, version, c, node[0], nil,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning hibernate and installing prerequisites")
		latestTag, err := repeatGetLatestTag(
			ctx, t, "hibernate", "hibernate-orm", hibernateReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest Hibernate release is %s.", latestTag)
		t.L().Printf("Supported Hibernate release is %s.", supportedHibernateTag)

		if err := repeatRunE(
			ctx, t, c, node, "update apt-get", `sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		// TODO(rafi): use openjdk-11-jdk-headless once we are off of Ubuntu 16.
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install dependencies",
			`sudo apt-get -qq install default-jre openjdk-8-jdk-headless gradle`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "remove old Hibernate", `rm -rf /mnt/data1/hibernate`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/hibernate/hibernate-orm.git",
			"/mnt/data1/hibernate",
			supportedHibernateTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("building hibernate (without tests)")
		// Build hibernate and run a single test, this step involves some
		// downloading, so it needs a retry loop as well. Just building was not
		// enough as the test libraries are not downloaded unless at least a
		// single test is invoked.
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"building hibernate (without tests)",
			opt.buildCmd,
		); err != nil {
			t.Fatal(err)
		}

		// Delete the test result; the test will be executed again later.
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"delete test result from build output",
			fmt.Sprintf(`rm -rf /mnt/data1/hibernate/%s/target/test-results/test`, opt.testDir),
		); err != nil {
			t.Fatal(err)
		}

		blocklistName, expectedFailures, _, _ := opt.blocklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No hibernate blocklist defined for cockroach version %s", version)
		}
		t.L().Printf("Running cockroach version %s, using blocklist %s", version, blocklistName)

		t.Status("running hibernate test suite, will take at least 3 hours")
		// Note that this will take upwards of 3 hours.
		// Also note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		_ = c.RunE(ctx, node, opt.testCmd)

		t.Status("collecting the test results")
		// Copy all of the test results to the cockroach logs directory to be
		// copied to the artifacts.

		// Copy the html report for the test.
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"copy html report",
			fmt.Sprintf(`cp /mnt/data1/hibernate/%s/target/reports/tests/test ~/logs/report -a`, opt.testDir),
		); err != nil {
			t.Fatal(err)
		}

		// Copy the individual test result files.
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"copy test result files",
			fmt.Sprintf(`cp /mnt/data1/hibernate/%s/target/test-results/test ~/logs/report/results -a`, opt.testDir),
		); err != nil {
			t.Fatal(err)
		}

		// Load the list of all test results files and parse them individually.
		// Files are here: /mnt/data1/hibernate/hibernate-core/target/test-results/test
		output, err := repeatRunWithBuffer(
			ctx,
			c,
			t,
			node,
			"get list of test files",
			fmt.Sprintf(`ls /mnt/data1/hibernate/%s/target/test-results/test/*.xml`, opt.testDir),
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(output) == 0 {
			t.Fatal("could not find any test result files")
		}

		parseAndSummarizeJavaORMTestsResults(
			ctx, t, c, node, "hibernate" /* ormName */, output,
			blocklistName, expectedFailures, nil /* ignorelist */, version, supportedHibernateTag,
		)
	}

	r.Add(registry.TestSpec{
		Name:    opt.testName,
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1),
		Tags:    []string{`default`, `orm`},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runHibernate(ctx, t, c)
		},
	})
}
