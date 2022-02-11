// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package democluster

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDemoCtx() *Context {
	return &Context{
		NumNodes:            1,
		SQLPoolMemorySize:   128 << 20, // 128MB, chosen to fit 9 nodes on 2GB machine.
		CacheSize:           64 << 20,  // 64MB, chosen to fit 9 nodes on 2GB machine.
		DefaultKeySize:      1024,
		DefaultCALifetime:   24 * time.Hour,
		DefaultCertLifetime: 2 * time.Hour,
	}
}

func TestTestServerArgsForTransientCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stickyEnginesRegistry := server.NewStickyInMemEnginesRegistry()

	testCases := []struct {
		nodeID            roachpb.NodeID
		joinAddr          string
		sqlPoolMemorySize int64
		cacheSize         int64

		expected base.TestServerArgs
	}{
		{
			nodeID:            roachpb.NodeID(1),
			joinAddr:          "127.0.0.1",
			sqlPoolMemorySize: 2 << 10,
			cacheSize:         1 << 10,
			expected: base.TestServerArgs{
				PartOfCluster:           true,
				JoinAddr:                "127.0.0.1",
				DisableTLSForHTTP:       true,
				SQLAddr:                 ":1234",
				HTTPAddr:                ":4567",
				SQLMemoryPoolSize:       2 << 10,
				CacheSize:               1 << 10,
				NoAutoInitializeCluster: true,
				TenantAddr:              new(string),
				EnableDemoLoginEndpoint: true,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						StickyEngineRegistry: stickyEnginesRegistry,
					},
				},
			},
		},
		{
			nodeID:            roachpb.NodeID(3),
			joinAddr:          "127.0.0.1",
			sqlPoolMemorySize: 4 << 10,
			cacheSize:         4 << 10,
			expected: base.TestServerArgs{
				PartOfCluster:           true,
				JoinAddr:                "127.0.0.1",
				SQLAddr:                 ":1236",
				HTTPAddr:                ":4569",
				DisableTLSForHTTP:       true,
				SQLMemoryPoolSize:       4 << 10,
				CacheSize:               4 << 10,
				NoAutoInitializeCluster: true,
				TenantAddr:              new(string),
				EnableDemoLoginEndpoint: true,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						StickyEngineRegistry: stickyEnginesRegistry,
					},
				},
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			demoCtx := newDemoCtx()
			demoCtx.SQLPoolMemorySize = tc.sqlPoolMemorySize
			demoCtx.CacheSize = tc.cacheSize

			actual := demoCtx.testServerArgsForTransientCluster(unixSocketDetails{}, tc.nodeID, tc.joinAddr, "", 1234, 4567, stickyEnginesRegistry)
			stopper := actual.Stopper
			defer stopper.Stop(context.Background())

			assert.Len(t, actual.StoreSpecs, 1)
			assert.Equal(
				t,
				fmt.Sprintf("demo-node%d", tc.nodeID),
				actual.StoreSpecs[0].StickyInMemoryEngineID,
			)

			// We cannot compare these.
			actual.Stopper = nil
			actual.StoreSpecs = nil
			actual.Knobs.JobsTestingKnobs = nil

			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestTransientClusterSimulateLatencies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This is slow under race as it starts a 9-node cluster which
	// has a very high simulated latency between each node.
	skip.UnderRace(t)

	demoCtx := newDemoCtx()
	// Set up an empty 9-node cluster with simulated latencies.
	demoCtx.SimulateLatency = true
	demoCtx.NumNodes = 9

	certsDir, err := ioutil.TempDir("", "cli-demo-test")
	require.NoError(t, err)

	cleanupFunc := securitytest.CreateTestCerts(certsDir)
	defer func() {
		if err := cleanupFunc(); err != nil {
			t.Fatal(err)
		}
	}()

	ctx := context.Background()

	// Setup the transient cluster.
	c := transientCluster{
		demoCtx:              demoCtx,
		stopper:              stop.NewStopper(),
		demoDir:              certsDir,
		stickyEngineRegistry: server.NewStickyInMemEnginesRegistry(),
		infoLog:              log.Infof,
		warnLog:              log.Warningf,
		shoutLog:             log.Ops.Shoutf,
	}
	// Stop the cluster when the test exits, including when it fails.
	// This also calls the Stop() method on the stopper, and thus
	// cancels everything controlled by the stopper.
	defer c.Close(ctx)

	// Also ensure the context gets canceled when the stopper
	// terminates above.
	ctx, _ = c.stopper.WithCancelOnQuiesce(ctx)

	require.NoError(t, c.Start(ctx, func(ctx context.Context, s *server.Server, _ bool, adminUser, adminPassword string) error {
		return s.RunLocalSQL(ctx,
			func(ctx context.Context, ie *sql.InternalExecutor) error {
				_, err := ie.Exec(
					ctx, "admin-user", nil,
					fmt.Sprintf("CREATE USER %s WITH PASSWORD $1", adminUser),
					adminPassword,
				)
				return err
			})
	}))

	for _, tc := range []struct {
		desc    string
		nodeIdx int
		region  string
	}{
		{
			desc:    "from us-east1",
			nodeIdx: 0,
			region:  "us-east1",
		},
		{
			desc:    "from us-west1",
			nodeIdx: 3,
			region:  "us-west1",
		},
		{
			desc:    "from europe-west1",
			nodeIdx: 6,
			region:  "europe-west1",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			url, err := c.getNetworkURLForServer(ctx, tc.nodeIdx,
				true /* includeAppName */, false /* isTenant */)
			require.NoError(t, err)
			sqlConnCtx := clisqlclient.Context{}
			conn := sqlConnCtx.MakeSQLConn(ioutil.Discard, ioutil.Discard, url.ToPQ().String())
			defer func() {
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}
			}()
			// Find the maximum latency in the cluster from the current node.
			var maxLatency time.Duration
			for _, latencyMS := range regionToRegionToLatency[tc.region] {
				if d := time.Duration(latencyMS) * time.Millisecond; d > maxLatency {
					maxLatency = d
				}
			}

			// Attempt to make a query that talks to every node.
			// This should take at least maxLatency.
			startTime := timeutil.Now()
			sqlExecCtx := clisqlexec.Context{}
			_, _, err = sqlExecCtx.RunQuery(
				context.Background(),
				conn,
				clisqlclient.MakeQuery(`SHOW ALL CLUSTER QUERIES`),
				false,
			)
			totalDuration := timeutil.Since(startTime)
			require.NoError(t, err)
			require.Truef(
				t,
				totalDuration >= maxLatency,
				"expected duration at least %s, got %s",
				maxLatency,
				totalDuration,
			)
		})
	}
}

func TestTransientClusterMultitenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test is too slow to complete under the race detector, sometimes.
	skip.UnderRace(t)

	demoCtx := newDemoCtx()
	// Set up an empty 3-node cluster with tenants on each node.
	demoCtx.NumNodes = 3
	demoCtx.Multitenant = true
	demoCtx.Localities = []roachpb.Locality{
		{Tiers: []roachpb.Tier{{Key: "prize-winner", Value: "otan"}}},
		{Tiers: []roachpb.Tier{{Key: "prize-winner", Value: "otan"}}},
		{Tiers: []roachpb.Tier{{Key: "prize-winner", Value: "otan"}}},
	}

	security.ResetAssetLoader()
	certsDir, err := ioutil.TempDir("", "cli-demo-mt-test")
	require.NoError(t, err)

	require.NoError(t, demoCtx.generateCerts(certsDir))

	defer func() {
		if err := os.RemoveAll(certsDir); err != nil {
			t.Fatal(err)
		}
	}()

	ctx := context.Background()

	// Setup the transient cluster.
	c := transientCluster{
		demoCtx:              demoCtx,
		stopper:              stop.NewStopper(),
		demoDir:              certsDir,
		stickyEngineRegistry: server.NewStickyInMemEnginesRegistry(),
		infoLog:              log.Infof,
		warnLog:              log.Warningf,
		shoutLog:             log.Ops.Shoutf,
	}
	// Stop the cluster when the test exits, including when it fails.
	// This also calls the Stop() method on the stopper, and thus
	// cancels everything controlled by the stopper.
	defer c.Close(ctx)

	// Also ensure the context gets canceled when the stopper
	// terminates above.
	ctx, _ = c.stopper.WithCancelOnQuiesce(ctx)

	require.NoError(t, c.Start(ctx, func(ctx context.Context, s *server.Server, _ bool, adminUser, adminPassword string) error {
		return s.RunLocalSQL(ctx,
			func(ctx context.Context, ie *sql.InternalExecutor) error {
				_, err := ie.Exec(ctx, "admin-user", nil, fmt.Sprintf("CREATE USER %s WITH PASSWORD %s", adminUser,
					adminPassword))
				return err
			})
	}))

	for i := 0; i < demoCtx.NumNodes; i++ {
		url, err := c.getNetworkURLForServer(ctx, i,
			true /* includeAppName */, true /* isTenant */)
		require.NoError(t, err)
		sqlConnCtx := clisqlclient.Context{}
		conn := sqlConnCtx.MakeSQLConn(ioutil.Discard, ioutil.Discard, url.ToPQ().String())
		defer func() {
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		// Create a table on each tenant to make sure that the tenants are separate.
		require.NoError(t, conn.Exec(context.Background(), "CREATE TABLE a (a int PRIMARY KEY)"))
	}
}
