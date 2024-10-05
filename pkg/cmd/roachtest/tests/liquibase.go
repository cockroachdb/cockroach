// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

var supportedLiquibaseHarnessCommit = "1790ddef2d0339c5c96839ac60ac424c130dadd8"

// This test runs the Liquibase test harness against a single cockroach node.
func registerLiquibase(r registry.Registry) {
	runLiquibase := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
	) {
		if c.IsLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		startOpts := option.DefaultStartOpts()
		startOpts.RoachprodOpts.SQLPort = config.DefaultSQLPort
		// TODO(darrylwong): if https://github.com/liquibase/liquibase-test-harness/pull/724 is merged, enable secure mode
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(install.SecureOption(false)), c.All())

		version, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, t, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "update apt-get", `sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning liquibase test harness and installing prerequisites")
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
			`sudo apt-get -qq install default-jre openjdk-8-jdk-headless maven`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "remove old liquibase test harness",
			`rm -rf /mnt/data1/liquibase-test-harness`,
		); err != nil {
			t.Fatal(err)
		}

		// TODO(richardjcai): When liquibase-test-harness 1.0.3 is released and tagged,
		//    use the tag version instead of the commit.
		if err = c.RunE(ctx, option.WithNodes(node), "cd /mnt/data1/ && git clone https://github.com/liquibase/liquibase-test-harness.git"); err != nil {
			t.Fatal(err)
		}
		if err = c.RunE(ctx, option.WithNodes(node), fmt.Sprintf("cd /mnt/data1/liquibase-test-harness/ && git checkout %s",
			supportedLiquibaseHarnessCommit)); err != nil {
			t.Fatal(err)
		}

		// The liquibase harness comes with a script that sets up the database.
		// The script executes the cockroach binary from /cockroach/cockroach.sh
		// so we symlink that here.
		t.Status("creating database/user used by tests")
		if err = c.RunE(ctx, option.WithNodes(node), `sudo mkdir /cockroach && sudo ln -sf /home/ubuntu/cockroach /cockroach/cockroach.sh`); err != nil {
			t.Fatal(err)
		}
		// TODO(darrylwong): once secure mode is enabled, add --certs-dir=install.CockroachNodeCertsDir
		if err = c.RunE(ctx, option.WithNodes(node), `/mnt/data1/liquibase-test-harness/src/test/resources/docker/setup_db.sh localhost`); err != nil {
			t.Fatal(err)
		}

		t.Status("running liquibase test harness")
		const blocklistName = "liquibaseBlocklist"
		expectedFailures := liquibaseBlocklist
		ignoreList := liquibaseIgnorelist

		const (
			repoDir     = "/mnt/data1/liquibase-test-harness"
			resultsPath = repoDir + "/target/surefire-reports/TEST-liquibase.harness.LiquibaseHarnessSuiteTest.xml"
		)

		// TODO(darrylwong): once secure mode is enabled, add -DdbUsername=roach -DdbPassword=system
		cmd := fmt.Sprintf("cd /mnt/data1/liquibase-test-harness/ && "+
			"mvn surefire-report:report-only test -Dtest=LiquibaseHarnessSuiteTest "+
			"-DdbName=cockroachdb -DdbVersion=20.2 -DoutputDirectory=%s", repoDir)

		err = c.RunE(ctx, option.WithNodes(node), cmd)
		if err != nil {
			t.L().Printf("error whilst running tests (may be expected): %#v", err)
		}

		parseAndSummarizeJavaORMTestsResults(
			ctx, t, c, node, "liquibase" /* ormName */, []byte(resultsPath),
			blocklistName,
			expectedFailures,
			ignoreList,
			version,
			supportedLiquibaseHarnessCommit,
		)

	}

	r.Add(registry.TestSpec{
		Name:             "liquibase",
		Owner:            registry.OwnerSQLFoundations,
		Cluster:          r.MakeClusterSpec(1),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly, registry.Tool),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLiquibase(ctx, t, c)
		},
	})
}
