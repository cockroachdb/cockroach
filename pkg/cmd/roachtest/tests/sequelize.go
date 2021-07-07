// Copyright 2021 The Cockroach Authors.
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
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

var sequelizeReleaseTagRegex = regexp.MustCompile(`^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)
var supportedSequelizeRelease = "v6.0.0-alpha.0"

// This test runs sequelize's full test suite against a single cockroach node.

func registerSequelize(r registry.Registry) {
	runSequelize := func(
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
		latestTag, err := repeatGetLatestTag(ctx, t, "cockroachdb", "sequelize-cockroachdb", sequelizeReleaseTagRegex)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest Sequelize release is %s.", latestTag)
		t.L().Printf("Supported Sequelize release is %s.", supportedSequelizeRelease)

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
			`sudo apt-get -qq install make python3 libpq-dev python-dev gcc g++ `+
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
			ctx, t, c, node, "install nodejs and npm", `sudo apt-get -qq install nodejs`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "update npm", `sudo npm i -g npm`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "remove old sequelize", `sudo rm -rf /mnt/data1/sequelize`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/cockroachdb/sequelize-cockroachdb.git",
			"/mnt/data1/sequelize",
			supportedSequelizeRelease,
			node,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "install dependencies", `cd /mnt/data1/sequelize && sudo npm i`,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("running Sequelize test suite")
		rawResults, err := c.RunWithBuffer(ctx, t.L(), node,
			`cd /mnt/data1/sequelize/ && npm test`,
		)
		rawResultsStr := string(rawResults)
		t.L().Printf("Test Results: %s", rawResultsStr)
		if err != nil {
			t.Fatal(err)
		}
	}

	r.Add(registry.TestSpec{
		Name:    "sequelize",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1),
		Tags:    []string{`default`, `orm`},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSequelize(ctx, t, c)
		},
	})
}
