// Copyright 2018 The Cockroach Authors.
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
	"regexp"
)

var hibernateReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)

// This test runs hibernate-core's full test suite against a single cockroach
// node.

func registerHibernate(r *testRegistry) {
	runHibernate := func(
		ctx context.Context,
		t *test,
		c *cluster,
	) {
		if c.isLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Start(ctx, t, c.All())

		version, err := fetchCockroachVersion(ctx, c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning hibernate and installing prerequisites")
		latestTag, err := repeatGetLatestTag(
			ctx, c, "hibernate", "hibernate-orm", hibernateReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest Hibernate release is %s.", latestTag)

		if err := repeatRunE(
			ctx, c, node, "update apt-get", `sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"install dependencies",
			`sudo apt-get -qq install default-jre openjdk-8-jdk-headless gradle`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old Hibernate", `rm -rf /mnt/data1/hibernate`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/hibernate/hibernate-orm.git",
			"/mnt/data1/hibernate",
			latestTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		// In order to get Hibernate's test suite to connect to cockroach, we have
		// to create a dbBundle as it not possible to specify the individual
		// properties. So here we just steamroll the file with our own config.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"configuring tests for cockroach only",
			fmt.Sprintf(
				"echo \"%s\" > /mnt/data1/hibernate/gradle/databases.gradle", hibernateDatabaseGradle,
			),
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
			c,
			node,
			"building hibernate (without tests)",
			`cd /mnt/data1/hibernate/hibernate-core/ && ./../gradlew test -Pdb=cockroach `+
				`--tests org.hibernate.jdbc.util.BasicFormatterTest.*`,
		); err != nil {
			t.Fatal(err)
		}

		blacklistName, expectedFailures, _, _ := hibernateBlacklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No hibernate blacklist defined for cockroach version %s", version)
		}
		c.l.Printf("Running cockroach version %s, using blacklist %s", version, blacklistName)

		t.Status("running hibernate test suite, will take at least 3 hours")
		// When testing, it is helpful to run only a subset of the tests. To do so
		// add "--tests org.hibernate.test.annotations.lob.*" to the end of the
		// test run command.
		// Note that this will take upwards of 3 hours.
		// Also note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		_ = c.RunE(ctx, node,
			`cd /mnt/data1/hibernate/hibernate-core/ && `+
				`HIBERNATE_CONNECTION_LEAK_DETECTION=true ./../gradlew test -Pdb=cockroach`,
		)

		t.Status("collecting the test results")
		// Copy all of the test results to the cockroach logs directory to be
		// copied to the artifacts.

		// Copy the html report for the test.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"copy html report",
			`cp /mnt/data1/hibernate/hibernate-core/target/reports/tests/test ~/logs/report -a`,
		); err != nil {
			t.Fatal(err)
		}

		// Copy the individual test result files.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"copy test result files",
			`cp /mnt/data1/hibernate/hibernate-core/target/test-results/test ~/logs/report/results -a`,
		); err != nil {
			t.Fatal(err)
		}

		// Load the list of all test results files and parse them individually.
		// Files are here: /mnt/data1/hibernate/hibernate-core/target/test-results/test
		output, err := repeatRunWithBuffer(
			ctx,
			c,
			t.l,
			node,
			"get list of test files",
			`ls /mnt/data1/hibernate/hibernate-core/target/test-results/test/*.xml`,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(output) == 0 {
			t.Fatal("could not find any test result files")
		}

		parseAndSummarizeJavaORMTestsResults(
			ctx, t, c, node, "hibernate" /* ormName */, output,
			blacklistName, expectedFailures, version, latestTag,
		)
	}

	r.Add(testSpec{
		Name:    "hibernate",
		Cluster: makeClusterSpec(1),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runHibernate(ctx, t, c)
		},
	})
}

const hibernateDatabaseGradle = `
ext {
  db = project.hasProperty('db') ? project.getProperty('db') : 'h2'
    dbBundle = [
     cockroach : [
       'db.dialect' : 'org.hibernate.dialect.PostgreSQL95Dialect',
       'jdbc.driver': 'org.postgresql.Driver',
       'jdbc.user'  : 'root',
       'jdbc.pass'  : '',
       'jdbc.url'   : 'jdbc:postgresql://localhost:26257/defaultdb?sslmode=disable'
     ],
    ]
}
`
