// Copyright 2019 The Cockroach Authors.
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

var pgjdbcReleaseTagRegex = regexp.MustCompile(`^REL(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)

// This test runs pgjdbc's full test suite against a single cockroach node.

func registerPgjdbc(r *testRegistry) {
	runPgjdbc := func(
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

		t.Status("cloning pgjdbc and installing prerequisites")
		// Report the latest tag, but do not use it. The newest versions produces output that breaks our xml parser,
		// and we want to pin to the working version for now.
		latestTag, err := repeatGetLatestTag(
			ctx, c, "pgjdbc", "pgjdbc", pgjdbcReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest pgjdbc release is %s.", latestTag)

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
			`sudo apt-get -qq install default-jre openjdk-8-jdk-headless maven`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old pgjdbc", `rm -rf /mnt/data1/pgjdbc`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/pgjdbc/pgjdbc.git",
			"/mnt/data1/pgjdbc",
			"REL42.2.9",
			node,
		); err != nil {
			t.Fatal(err)
		}

		// In order to get pgjdbc's test suite to connect to cockroach, we have
		// to override settings in build.local.properties
		if err := repeatRunE(
			ctx,
			c,
			node,
			"configuring tests for cockroach only",
			fmt.Sprintf(
				"echo \"%s\" > /mnt/data1/pgjdbc/build.local.properties", pgjdbcDatabaseParams,
			),
		); err != nil {
			t.Fatal(err)
		}

		t.Status("building pgjdbc (without tests)")
		// Build pgjdbc and run a single test, this step involves some
		// downloading, so it needs a retry loop as well. Just building was not
		// enough as the test libraries are not downloaded unless at least a
		// single test is invoked.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"building pgjdbc (without tests)",
			`cd /mnt/data1/pgjdbc/pgjdbc/ && mvn -Dtest=OidToStringTest test`,
		); err != nil {
			t.Fatal(err)
		}

		blocklistName, expectedFailures, ignorelistName, ignorelist := pgjdbcBlocklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No pgjdbc blocklist defined for cockroach version %s", version)
		}
		status := fmt.Sprintf("Running cockroach version %s, using blocklist %s", version, blocklistName)
		if ignorelist != nil {
			status = fmt.Sprintf("Running cockroach version %s, using blocklist %s, using ignorelist %s",
				version, blocklistName, ignorelistName)
		}
		c.l.Printf("%s", status)

		t.Status("running pgjdbc test suite")
		// Note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		_ = c.RunE(ctx, node,
			`cd /mnt/data1/pgjdbc/pgjdbc/ && mvn test`,
		)

		_ = c.RunE(ctx, node,
			`mkdir -p ~/logs/report/pgjdbc-results`,
		)

		t.Status("collecting the test results")
		// Copy all of the test results to the cockroach logs directory to be
		// copied to the artifacts.

		// Copy the individual test result files.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"copy test result files",
			`cp /mnt/data1/pgjdbc/pgjdbc/target/surefire-reports ~/logs/report/pgjdbc-results -a`,
		); err != nil {
			t.Fatal(err)
		}

		// Load the list of all test results files and parse them individually.
		// Files are here: /mnt/data1/pgjdbc/pgjdbc-core/target/test-results/test
		output, err := repeatRunWithBuffer(
			ctx,
			c,
			t.l,
			node,
			"get list of test files",
			`ls /mnt/data1/pgjdbc/pgjdbc/target/surefire-reports/*.xml`,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(output) == 0 {
			t.Fatal("could not find any test result files")
		}

		parseAndSummarizeJavaORMTestsResults(
			ctx, t, c, node, "pgjdbc" /* ormName */, output,
			blocklistName, expectedFailures, ignorelist, version, latestTag,
		)
	}

	r.Add(testSpec{
		MinVersion: "v2.1.0",
		Name:       "pgjdbc",
		Owner:      OwnerAppDev,
		Cluster:    makeClusterSpec(1),
		Tags:       []string{`default`, `driver`},
		Run: func(ctx context.Context, t *test, c *cluster) {
			runPgjdbc(ctx, t, c)
		},
	})
}

const pgjdbcDatabaseParams = `
server=localhost
port=26257
secondaryServer=localhost
secondaryPort=5433
secondaryServer2=localhost
secondaryServerPort2=5434
database=defaultdb
username=root
password=
privilegedUser=root
privilegedPassword=
sspiusername=testsspi
preparethreshold=5
loggerLevel=DEBUG
loggerFile=target/pgjdbc-tests.log
protocolVersion=0
sslpassword=sslpwd
`
