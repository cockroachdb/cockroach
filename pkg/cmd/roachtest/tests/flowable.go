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
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

var flowableReleaseTagRegex = regexp.MustCompile(`^flowable-(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)

// This test runs Flowable test suite against a single cockroach node.

func registerFlowable(r registry.Registry) {
	runFlowable := func(
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
		c.Start(ctx, c.All())

		t.Status("cloning flowable and installing prerequisites")
		latestTag, err := repeatGetLatestTag(
			ctx, t, "flowable", "flowable-engine", flowableReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest Flowable release is %s.", latestTag)

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
			`sudo apt-get -qq install default-jre openjdk-8-jdk-headless gradle maven`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "remove old Flowable", `rm -rf /mnt/data1/flowable-engine`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/flowable/flowable-engine.git",
			"/mnt/data1/flowable-engine",
			latestTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("building Flowable")
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"building Flowable",
			`cd /mnt/data1/flowable-engine/ && mvn clean install -DskipTests`,
		); err != nil {
			t.Fatal(err)
		}

		if err := c.RunE(ctx, node,
			`cd /mnt/data1/flowable-engine/ && mvn clean test -Dtest=Flowable6Test#testLongServiceTaskLoop -Ddb=crdb`,
		); err != nil {
			t.Fatal(err)
		}
	}

	r.Add(registry.TestSpec{
		Name:    "flowable",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runFlowable(ctx, t, c)
		},
	})
}
