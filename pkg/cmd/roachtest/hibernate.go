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

import "context"

// TODO(rafi): Once our fork is merged into the main repo, go back to using
// latest release tag.
//var hibernateReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)

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

		// TODO(rafi): Once our fork is merged into the main repo, go back to
		// fetching the latest tag. For now, always use the
		// `HHH-13724-cockroachdb-dialects` branch, where we are building the
		// dialect.
		latestTag := "HHH-13724-cockroachdb-dialects"
		//t.Status("cloning hibernate and installing prerequisites")
		//latestTag, err := repeatGetLatestTag(
		//	ctx, c, "hibernate", "hibernate-orm", hibernateReleaseTagRegex,
		//)
		//if err != nil {
		//	t.Fatal(err)
		//}
		//c.l.Printf("Latest Hibernate release is %s.", latestTag)

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

		// TODO(rafi): Switch back to using the main hibernate/hibernate-orm repo
		// once the CockroachDB dialect is merged into it. For now, we are using
		// a fork so we can make incremental progress on building the dialect.
		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/cockroachdb/hibernate-orm.git",
			"/mnt/data1/hibernate",
			latestTag,
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
			c,
			node,
			"building hibernate (without tests)",
			`cd /mnt/data1/hibernate/hibernate-core/ && ./../gradlew test -Pdb=cockroachdb `+
				`--tests org.hibernate.jdbc.util.BasicFormatterTest.*`,
		); err != nil {
			t.Fatal(err)
		}

		blocklistName, expectedFailures, _, _ := hibernateBlocklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No hibernate blocklist defined for cockroach version %s", version)
		}
		c.l.Printf("Running cockroach version %s, using blocklist %s", version, blocklistName)

		t.Status("running hibernate test suite, will take at least 3 hours")
		// When testing, it is helpful to run only a subset of the tests. To do so
		// add "--tests org.hibernate.test.annotations.lob.*" to the end of the
		// test run command.
		// Note that this will take upwards of 3 hours.
		// Also note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		_ = c.RunE(ctx, node,
			`cd /mnt/data1/hibernate/ && `+
				`HIBERNATE_CONNECTION_LEAK_DETECTION=true ./gradlew test -Pdb=cockroachdb`,
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
			`ls /mnt/data1/hibernate/*/target/test-results/test/*.xml`,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(output) == 0 {
			t.Fatal("could not find any test result files")
		}

		parseAndSummarizeJavaORMTestsResults(
			ctx, t, c, node, "hibernate" /* ormName */, output,
			blocklistName, expectedFailures, nil /* ignorelist */, version, latestTag,
		)
	}

	r.Add(testSpec{
		Name:    "hibernate",
		Owner:   OwnerAppDev,
		Cluster: makeClusterSpec(1),
		Tags:    []string{`default`, `orm`},
		Run: func(ctx context.Context, t *test, c *cluster) {
			runHibernate(ctx, t, c)
		},
	})
}
