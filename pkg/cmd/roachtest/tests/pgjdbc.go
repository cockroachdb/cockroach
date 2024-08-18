// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

var pgjdbcReleaseTagRegex = regexp.MustCompile(`^REL(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)

// WARNING: DO NOT MODIFY the name of the below constant/variable without approval from the docs team.
// This is used by docs automation to produce a list of supported versions for ORM's.
var supportedPGJDBCTag = "REL42.7.3"

// This test runs pgjdbc's full test suite against a single cockroach node.

func registerPgjdbc(r registry.Registry) {
	runPgjdbc := func(
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

		version, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, t, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning pgjdbc and installing prerequisites")
		// Report the latest tag, but do not use it. The newest versions produces output that breaks our xml parser,
		// and we want to pin to the working version for now.
		latestTag, err := repeatGetLatestTag(
			ctx, t, "pgjdbc", "pgjdbc", pgjdbcReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest pgjdbc release is %s.", latestTag)
		t.L().Printf("Supported pgjdbc release is %s.", supportedPGJDBCTag)

		if err := repeatRunE(
			ctx, t, c, node, "update apt-get", `sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install dependencies",
			`sudo apt-get -qq install default-jre openjdk-17-jdk-headless gradle`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "remove old pgjdbc", `rm -rf /mnt/data1/pgjdbc`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/pgjdbc/pgjdbc.git",
			"/mnt/data1/pgjdbc",
			supportedPGJDBCTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		// In order to get pgjdbc's test suite to connect to cockroach, we have
		// to override settings in build.local.properties
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"configuring tests for cockroach only",
			fmt.Sprintf(
				"echo \"%s\" > /mnt/data1/pgjdbc/build.local.properties", pgjdbcDatabaseParams,
			),
		); err != nil {
			t.Fatal(err)
		}

		// Remove an unsupported deferrable qualifier (#31632) from the XA test
		// suite setup. This prevents XADataSourceTest.mappingOfConstraintViolations
		// from running properly (it fails either way), but it allows the rest of
		// the XA tests to run.
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"removing unsupported deferrable qualifier from test",
			"sed -i 's/ deferrable//' /mnt/data1/pgjdbc/pgjdbc/src/test/java/org/postgresql/test/xa/XADataSourceTest.java",
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
			t,
			c,
			node,
			"building pgjdbc (without tests)",
			`cd /mnt/data1/pgjdbc/pgjdbc/ && ../gradlew test --tests OidToStringTest`,
		); err != nil {
			t.Fatal(err)
		}

		const blocklistName = "pgjdbcBlockList"
		const ignorelistName = "pgjdbcIgnorelist"
		expectedFailures := pgjdbcBlockList
		ignorelist := pgjdbcIgnoreList

		status := fmt.Sprintf("Running cockroach version %s, using blocklist %s, using ignorelist %s",
			version, blocklistName, ignorelistName)

		t.L().Printf("%s", status)

		t.Status("running pgjdbc test suite")
		// Note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		_ = c.RunE(ctx, option.WithNodes(node),
			`cd /mnt/data1/pgjdbc/pgjdbc/ && ../gradlew test`,
		)

		_ = c.RunE(ctx, option.WithNodes(node),
			`mkdir -p ~/logs/report/pgjdbc-results`,
		)

		t.Status("collecting the test results")
		// Copy all of the test results to the cockroach logs directory to be
		// copied to the artifacts.

		// Copy the individual test result files.
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"copy test result files",
			`cp /mnt/data1/pgjdbc/pgjdbc/build/test-results/test/ ~/logs/report/pgjdbc-results -a`,
		); err != nil {
			t.Fatal(err)
		}

		// Load the list of all test results files and parse them individually.
		// Files are here: /mnt/data1/pgjdbc/pgjdbc-core/target/test-results/test
		result, err := repeatRunWithDetailsSingleNode(
			ctx,
			c,
			t,
			node,
			"get list of test files",
			`ls /mnt/data1/pgjdbc/pgjdbc/build/test-results/test/*.xml`,
		)
		if err != nil {
			t.Fatal(err)
		}

		if len(result.Stdout) == 0 {
			t.Fatal("could not find any test result files")
		}

		parseAndSummarizeJavaORMTestsResults(
			ctx, t, c, node, "pgjdbc" /* ormName */, []byte(result.Stdout),
			blocklistName, expectedFailures, ignorelist, version, supportedPGJDBCTag,
		)
	}

	r.Add(registry.TestSpec{
		Name:             "pgjdbc",
		Owner:            registry.OwnerSQLFoundations,
		Cluster:          r.MakeClusterSpec(1),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly, registry.Driver),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runPgjdbc(ctx, t, c)
		},
	})
}

const pgjdbcDatabaseParams = `
server=localhost
port={pgport:1}
secondaryServer=localhost
secondaryPort=5433
secondaryServer2=localhost
secondaryServerPort2=5434
database=defaultdb
username=test_admin
password=testpw
privilegedUser=test_admin
privilegedPassword=testpw
sspiusername=testsspi
preparethreshold=5
loggerLevel=DEBUG
loggerFile=target/pgjdbc-tests.log
protocolVersion=0
sslpassword=sslpwd
`
