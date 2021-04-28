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

import "context"

// This test runs the Liquibase test harness against a single cockroach node.

func registerLiquibase(r *testRegistry) {
	runLiquibase := func(
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

		version, err := fetchCockroachVersion(ctx, c, node[0], nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, version, c, node[0], nil); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "update apt-get", `sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning liquibase test harness and installing prerequisites")
		if err := repeatRunE(
			ctx, c, node, "update apt-get", `sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		// TODO(rafi): use openjdk-11-jdk-headless once we are off of Ubuntu 16.
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
			ctx, c, node, "remove old liquibase test harness",
			`rm -rf /mnt/data1/liquibase-test-harness`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/liquibase/liquibase-test-harness.git",
			"/mnt/data1/liquibase-test-harness",
			"main",
			node,
		); err != nil {
			t.Fatal(err)
		}

		// The liquibase harness comes with a script that sets up the database.
		// The script executes the cockroach binary from /cockroach/cockroach.sh
		// so we symlink that here.
		t.Status("creating database/user used by tests")
		c.Run(ctx, node, `sudo mkdir /cockroach && sudo ln -sf /home/ubuntu/cockroach /cockroach/cockroach.sh`)
		c.Run(ctx, node, `/mnt/data1/liquibase-test-harness/src/test/resources/docker/setup_db.sh localhost`)

		t.Status("running liquibase test harness")
		// All tests are expected to pass, so this should not error.
		// The dbVersion is set to 20.2 since that causes all known passing tests
		// to be run.
		if err = c.RunE(ctx, node,
			`cd /mnt/data1/liquibase-test-harness/ && mvn test -DdbName=cockroachdb -DdbVersion=20.2`,
		); err != nil {
			t.Fatal(err)
		}
	}

	r.Add(testSpec{
		MinVersion: "v20.2.0",
		Name:       "liquibase",
		Owner:      OwnerSQLExperience,
		Cluster:    makeClusterSpec(1),
		Tags:       []string{`default`, `tool`},
		Run: func(ctx context.Context, t *test, c *cluster) {
			runLiquibase(ctx, t, c)
		},
	})
}
