// Copyright 2021 The Cockroach Authors.
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
	"github.com/stretchr/testify/require"
)

var popReleaseTag = regexp.MustCompile(`^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)
var popSupportedTag = "v5.3.3"

func registerPop(r registry.Registry) {
	runPop := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		if c.IsLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		startOpts := option.NewStartOpts(sqlClientsInMemoryDB)
		// pop expects secure clusters, indicated by cockroach_ssl, to have SQL Port 26259.
		// See: https://github.com/gobuffalo/pop/blob/main/database.yml#L26-L28
		startOpts.RoachprodOpts.SQLPort = 26259
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.All())
		version, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
		if err != nil {
			t.Fatal(err)
		}
		if err := alterZoneConfigAndClusterSettings(ctx, t, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning pop and installing prerequisites")
		latestTag, err := repeatGetLatestTag(
			ctx, t, "gobuffalo", "pop", popReleaseTag)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest pop release is %s.", latestTag)
		t.L().Printf("Supported pop release is %s.", popSupportedTag)

		installGolang(ctx, t, c, node)

		const (
			popPath = "/mnt/data1/pop/"
		)

		// Remove any old pop installations
		if err := repeatRunE(
			ctx, t, c, node, "remove old pop", fmt.Sprintf("rm -rf %s", popPath),
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/gobuffalo/pop.git",
			popPath,
			popSupportedTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("building and setting up tests")

		// pop expects to find certificates in a specific path.
		err = c.RunE(ctx, option.WithNodes(node), "mkdir -p /mnt/data1/pop/crdb/certs")
		require.NoError(t, err)
		err = c.RunE(ctx, option.WithNodes(node), fmt.Sprintf("cp -r %s /mnt/data1/pop/crdb/", install.CockroachNodeCertsDir))
		require.NoError(t, err)

		err = c.RunE(ctx, option.WithNodes(node), fmt.Sprintf(`cd %s && go build -v -tags sqlite -o tsoda ./soda`, popPath))
		require.NoError(t, err)

		err = c.RunE(ctx, option.WithNodes(node), fmt.Sprintf(`cd %s && ./tsoda drop -e cockroach_ssl -c ./database.yml -p ./testdata/migrations`, popPath))
		require.NoError(t, err)

		err = c.RunE(ctx, option.WithNodes(node), fmt.Sprintf(`cd %s && ./tsoda create -e cockroach_ssl -c ./database.yml -p ./testdata/migrations`, popPath))
		require.NoError(t, err)

		err = c.RunE(ctx, option.WithNodes(node), fmt.Sprintf(`cd %s && ./tsoda migrate -e cockroach_ssl -c ./database.yml -p ./testdata/migrations`, popPath))
		require.NoError(t, err)

		t.Status("running pop test suite")

		// No tests are expected to fail.
		err = c.RunE(ctx, option.WithNodes(node), fmt.Sprintf(`cd %s && SODA_DIALECT=cockroach_ssl go test -race -tags sqlite -v ./... -count=1`, popPath))
		require.NoError(t, err, "error while running pop tests")
	}

	r.Add(registry.TestSpec{
		Name:    "pop",
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(1),
		Leases:  registry.MetamorphicLeases,
		// This test requires custom ports but service registration is
		// currently only supported on GCE.
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly, registry.ORM),
		Run:              runPop,
	})
}
