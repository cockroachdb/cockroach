package tests

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

//git clone --recurse-submodules git@github.com:MagicStack/asyncpg.git
//cd asyncpg
//pip install -e .
//# modified asyncpg/connect_utils.py to point at CRDB
//python setup.py test

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

	}

	r.Add(registry.TestSpec{
		Name:    "django",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1, spec.CPU(16)),
		Tags:    []string{`default`, `orm`},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runAsyncpg(ctx, t, c)
		},
	})
}
