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
	"regexp"
)

var sequelizeReleaseTagRegex = regexp.MustCompile(`^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)
var supportedSequelizeRelease = "v6.0.0-alpha.0"

// This test runs sequelize's full test suite against a single cockroach node.

func registerSequelize(r *testRegistry) {
	runSequelize := func(
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
		if err := c.PutLibraries(ctx, "./lib"); err != nil {
			t.Fatal(err)
		}
		c.Start(ctx, t, c.All())

		version, err := fetchCockroachVersion(ctx, c, node[0], nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, version, c, node[0], nil); err != nil {
			t.Fatal(err)
		}

		t.Status("create database used by tests")
		db, err := c.ConnE(ctx, node[0])
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		if _, err := db.ExecContext(
			ctx,
			`CREATE DATABASE sequelize_test`,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning Sequelize and installing prerequisites")
		latestTag, err := repeatGetLatestTag(ctx, c, "cockroachdb", "sequelize-cockroachdb", sequelizeReleaseTagRegex)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest Sequelize release is %s.", latestTag)
		c.l.Printf("Supported Sequelize release is %s.", supportedSequelizeRelease)

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
			`sudo apt-get -qq install make python3 libpq-dev python-dev gcc g++ `+
				`software-properties-common build-essential`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"add nodesource repository",
			`curl -sL https://deb.nodesource.com/setup_12.x | sudo -E bash -`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "install nodejs and npm", `sudo apt-get -qq install nodejs`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "update npm", `sudo npm i -g npm`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old sequelize", `sudo rm -rf /mnt/data1/sequelize`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/cockroachdb/sequelize-cockroachdb.git",
			"/mnt/data1/sequelize",
			supportedSequelizeRelease,
			node,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "install dependencies", `cd /mnt/data1/sequelize && sudo npm i`,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("running Sequelize test suite")
		rawResults, err := c.RunWithBuffer(ctx, t.l, node,
			`cd /mnt/data1/sequelize/ && npm test`,
		)
		rawResultsStr := string(rawResults)
		c.l.Printf("Test Results: %s", rawResultsStr)
		if err != nil {
			t.Fatal(err)
		}
	}

	r.Add(testSpec{
		MinVersion: "v20.2.0",
		Name:       "sequelize",
		Owner:      OwnerSQLExperience,
		Cluster:    makeClusterSpec(1),
		Tags:       []string{`default`, `orm`},
		Run: func(ctx context.Context, t *test, c *cluster) {
			runSequelize(ctx, t, c)
		},
	})
}
