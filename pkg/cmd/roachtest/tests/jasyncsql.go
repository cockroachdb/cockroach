package tests

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func registerJasyncSql(r registry.Registry) {
	runJasyncSql := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		if c.IsLocal() {
			t.Fatal("can not be run in local mode")
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

		t.Status("cloning jasync-sql and installing prerequisites")

		//remove old
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"remove old jasync-sql",
			`rm -rf /mnt/data1/jasyncsql`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/e-mbrown/jasync-sql.git",
			"/mnt/data1/jasyncsql",
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
			"install java and gradle",
			`sudo apt-get -qq install default-jre openjdk-11-jdk-headless gradle`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"run jasync test suite",
			`cd /mnt/data1/jasyncsql && ./gradlew :postgresql-async:test`,
		); err != nil {
			t.Fatal(err)
		}
	}

	r.Add(registry.TestSpec{
		Name:    "jasync",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1, spec.CPU(16)),
		Tags:    []string{`default`, `orm`},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runJasyncSql(ctx, t, c)
		},
	})
}
