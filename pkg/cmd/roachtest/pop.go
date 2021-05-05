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
	"fmt"
	"regexp"

	"github.com/stretchr/testify/require"
)

var popReleaseTag = regexp.MustCompile(`^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)
var popSupportedTag = "v5.3.3"

func registerPop(r *testRegistry) {
	runPop := func(ctx context.Context, t *test, c *cluster) {
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

		t.Status("cloning pop and installing prerequisites")
		latestTag, err := repeatGetLatestTag(
			ctx, c, "gobuffalo", "pop", popReleaseTag)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest pop release is %s.", latestTag)
		c.l.Printf("Supported pop release is %s.", popSupportedTag)

		installGolang(ctx, t, c, node)

		const (
			popPath = "/mnt/data1/pop/"
		)

		// Remove any old pop installations
		if err := repeatRunE(
			ctx, c, node, "remove old pop", fmt.Sprintf("rm -rf %s", popPath),
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/gobuffalo/pop.git",
			popPath,
			popSupportedTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("building and setting up tests")

		err = c.RunE(ctx, node, fmt.Sprintf(`cd %s && go build -v -tags sqlite -o tsoda ./soda`, popPath))
		require.NoError(t, err)

		err = c.RunE(ctx, node, fmt.Sprintf(`cd %s && ./tsoda drop -e cockroach -c ./database.yml -p ./testdata/migrations`, popPath))
		require.NoError(t, err)

		err = c.RunE(ctx, node, fmt.Sprintf(`cd %s && ./tsoda create -e cockroach -c ./database.yml -p ./testdata/migrations`, popPath))
		require.NoError(t, err)

		err = c.RunE(ctx, node, fmt.Sprintf(`cd %s && ./tsoda migrate -e cockroach -c ./database.yml -p ./testdata/migrations`, popPath))
		require.NoError(t, err)

		t.Status("running pop test suite")

		// No tests are expected to fail.
		err = c.RunE(ctx, node, fmt.Sprintf(`cd %s && SODA_DIALECT=cockroach go test -race -tags sqlite -v ./... -count=1`, popPath))
		require.NoError(t, err, "error while running pop tests")
	}

	r.Add(testSpec{
		Name:       "pop",
		Owner:      OwnerSQLExperience,
		MinVersion: "v20.2.0",
		Cluster:    makeClusterSpec(1),
		Tags:       []string{`default`, `orm`},
		Run:        runPop,
	})
}
