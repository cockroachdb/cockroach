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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func registerAsyncpg(r registry.Registry) {
	runAsyncpg := func(
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

		version, err := fetchCockroachVersion(ctx, c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning asyncpg and installing prerequisites")

		if err := gitCloneWithRecurseSubmodules(
			ctx,
			c,
			t.L(),
			"https://github.com/MagicStack/asyncpg.git",
			"/mnt/data1/asyncpg",
			"master",
			node,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install python and pip",
			`sudo apt-get -qq install python3.7 python3-pip libpq-dev python-dev`,
		); err != nil {
			t.Fatal(err)
		}

		// Install postgresql for asyncpg requires pg_ctl and postgres executable to setup.
		// See asyncpg's code here:
		// https://github.com/MagicStack/asyncpg/blob/383c711eb68bc6a042c121e1fddfde0cdefb8068/asyncpg/cluster.py#L407-L419
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install postgresql",
			`sudo apt-get -y install postgresql`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install asyncpg's dependencies",
			"cd /mnt/data1/asyncpg && sudo pip3 install django && pip3 install -e ."); err != nil {
			t.Fatal(err)
		}

		const cockroachPort = 26257
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"run setup.py test for asyncpg",
			fmt.Sprintf("cd /mnt/data1/asyncpg && PGPORT=%d python3 setup.py test", cockroachPort),
		); err != nil {
			t.Fatal(err)
		}
	}

	r.Add(registry.TestSpec{
		Name:    "asyncpg",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1, spec.CPU(16)),
		Tags:    []string{`default`, `orm`},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runAsyncpg(ctx, t, c)
		},
	})
}
