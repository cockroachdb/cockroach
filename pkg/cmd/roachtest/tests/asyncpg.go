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

		// TODO(janexing): This git clone does not use `--recurse-submodules`.
		// If we want `--recurse-submodules`, use:
		// if err := c.RunE(ctx, node, "cd /mnt/data1/ && git clone --recurse-submodules git@github.com:MagicStack/asyncpg.git"); err != nil {
		//		t.Fatal(err)
		//}
		// if err := c.RunE(ctx, node, "cd ./asyncpg); err != nil {
		//		t.Fatal(err)
		//}"
		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/cockroachdb/django-cockroachdb",
			"/mnt/data1/asyncpg",
			"master",
			node,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "set python3.7 as default", `
    		sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.5 1
    		sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 2
    		sudo update-alternatives --config python3`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install asyncpg's dependencies",
			"sudo pip3 install -e ."); err != nil {
			t.Fatal(err)
		}

		const cockroachPort = 26257
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"run setup.py test for asyncpg",
			fmt.Sprintf("PGPORT=%s python3 setup.py test", cockroachPort),
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
