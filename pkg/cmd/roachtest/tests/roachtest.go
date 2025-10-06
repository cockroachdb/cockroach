// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

func registerRoachtest(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "roachtest/noop",
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Roachtest),
		Owner:            registry.OwnerTestEng,
		Run:              func(_ context.Context, _ test.Test, _ cluster.Cluster) {},
		Cluster:          r.MakeClusterSpec(0),
	})
	r.Add(registry.TestSpec{
		Name:             "roachtest/noop-maybefail",
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Roachtest),
		Owner:            registry.OwnerTestEng,
		Run: func(_ context.Context, t test.Test, _ cluster.Cluster) {
			if rand.Float64() <= 0.2 {
				t.Fatal("randomly failing")
			}
		},
		Cluster: r.MakeClusterSpec(0),
	})
	// This test can be run manually to check what happens if a test times out.
	// In particular, can manually verify that suitable artifacts are created.
	r.Add(registry.TestSpec{
		Name:             "roachtest/hang",
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Roachtest),
		Owner:            registry.OwnerTestEng,
		Run: func(_ context.Context, t test.Test, c cluster.Cluster) {
			ctx := context.Background() // intentional
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())
			time.Sleep(time.Hour)
		},
		Timeout: 3 * time.Minute,
		Cluster: r.MakeClusterSpec(3),
	})
}
