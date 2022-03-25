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
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func registerRoachtest(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "roachtest/noop",
		Tags:    []string{"roachtest"},
		Owner:   registry.OwnerTestEng,
		Run:     func(_ context.Context, _ test.Test, _ cluster.Cluster) {},
		Cluster: r.MakeClusterSpec(0),
	})
	r.Add(registry.TestSpec{
		Name:  "roachtest/noop-maybefail",
		Tags:  []string{"roachtest"},
		Owner: registry.OwnerTestEng,
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
		Name:  "roachtest/hang",
		Tags:  []string{"roachtest"},
		Owner: registry.OwnerTestEng,
		Run: func(_ context.Context, t test.Test, c cluster.Cluster) {
			ctx := context.Background() // intentional
			c.Put(ctx, t.Cockroach(), "cockroach", c.All())
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())
			time.Sleep(time.Hour)
		},
		Timeout: 3 * time.Minute,
		Cluster: r.MakeClusterSpec(3),
	})
	r.Add(registry.TestSpec{
		Name:  "roachtest/multitenant",
		Tags:  []string{"roachtest"},
		Owner: registry.OwnerTestEng,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Put(ctx, t.Cockroach(), "cockroach", c.All())
			// KV host cluster on n1.
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(1))
			// Annoyance: need to manually grab details of the KV cluster to set up
			// the tenant. (This could be done by the framework, so the test would
			// only need to specify the node numbers to connect to).
			kvAddrs, err := c.ExternalAddr(ctx, t.L(), c.Node(1))
			require.NoError(t, err)
			// Problem: no conventions around tenant ports, and so no automatic artifacts
			// collection from them (think tenant debug.zip). We should standardize somehow.
			// A simple idea would be to use something like `basePort+tenID` as done here,
			// but recall that a tenant may have multiple instances, so it has to be something
			// like basePort + instance[0-9] + tenID[1-99]. Also, we're not allowed to get out of
			// the registered port range (1024-49151). Also, for --local support, all ports
			// have to be unique (in particular, we want to stay away from often-used ports
			// such as 8000, 8080, and any KV host cluster ports. So a suggestion could be:
			// tenID = 1-99
			// tenHTTPPort = 9000  + instance*1000 + tenID
			// tenSQLPort  = 20000 + instance*1000 + tenID
			//
			// which is how the numbers below were chosen.
			const (
				_           = 4 // instance
				tenID       = 14
				tenHTTPPort = 9414
				tenSQLPort  = 20414
			)
			{
				_, err := c.Conn(ctx, t.L(), 1).Exec(
					`SELECT crdb_internal.create_tenant($1)`, tenID)
				require.NoError(t, err)
			}
			tenant14 := createTenantNode(kvAddrs, tenID, 2 /* n2 */, tenHTTPPort, tenSQLPort)
			tenant14.start(ctx, t, c, "./cockroach")
			defer tenant14.stop(ctx, t, c)

			// Problem: NewMonitor does not work for n2 since we don't have tenant
			// support for the monitor. The impl of `roachprod.Monitor` hard-codes on
			// the KV host cluster, see:
			//
			// https://github.com/cockroachdb/cockroach/blob/08d54307392084c5620026ca16bc290b357004cc/pkg/roachprod/install/cluster_synced.go#L441-L449
			//
			// So there's no way to orchestrate a tenant and to run commands while
			// being receptive to crashes in the tenant cluster. Perhaps this is not a
			// dealbreaker, but it defies user's expectations and causes confusion. We
			// need to be able to monitor tenants, too. (Besides, even forgetting
			// about tenants the Monitor is sometimes confusing and has some sharp
			// edges around the ExpectDeath() functionality and its interaction with
			// when .Wait() is called).
			m := c.NewMonitor(ctx, c.Node(2))
			m.Go(func(ctx context.Context) error {
				select {
				case <-ctx.Done():
				case <-time.After(10 * time.Second):
				}
				return nil
			})
			{
				err := m.WaitE()
				require.True(t, testutils.IsError(err, `2: dead`), "%+v", err)
			}
		},
		Timeout: 3 * time.Minute,
		Cluster: r.MakeClusterSpec(2),
	})
}
