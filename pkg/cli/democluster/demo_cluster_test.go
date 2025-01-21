// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package democluster

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils/regionlatency"
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
		SQLPoolMemorySize:   256 << 20, // 256MiB, chosen to fit 9 nodes on 4GB machine.
		CacheSize:           64 << 20,  // 64MiB, chosen to fit 9 nodes on 4GB machine.
		DefaultKeySize:      1024,
		DefaultCALifetime:   24 * time.Hour,
		DefaultCertLifetime: 2 * time.Hour,
	}
}

func TestTestServerArgsForTransientCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stickyVFSRegistry := fs.NewStickyRegistry()

	testCases := []struct {
		serverIdx         int
		joinAddr          string
		sqlPoolMemorySize int64
		cacheSize         int64

		expected base.TestServerArgs
	}{
		{
			serverIdx:         0,
			joinAddr:          "127.0.0.1",
			sqlPoolMemorySize: 2 << 10,
			cacheSize:         1 << 10,
			expected: base.TestServerArgs{
				DefaultTenantName:             "demoapp",
				DefaultTestTenant:             base.TestControlsTenantsExplicitly,
				PartOfCluster:                 true,
				JoinAddr:                      "127.0.0.1",
				DisableTLSForHTTP:             true,
				Addr:                          "127.0.0.1:1334",
				SQLAddr:                       "127.0.0.1:1234",
				HTTPAddr:                      "127.0.0.1:4567",
				ApplicationInternalRPCPortMin: 1332,
				ApplicationInternalRPCPortMax: 2356,
				SQLMemoryPoolSize:             2 << 10,
				CacheSize:                     1 << 10,
				NoAutoInitializeCluster:       true,
				EnableDemoLoginEndpoint:       true,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						StickyVFSRegistry: stickyVFSRegistry,
					},
				},
				ExternalIODir: "nodelocal/n1",
			},
		},
		{
			serverIdx:         2,
			joinAddr:          "127.0.0.1",
			sqlPoolMemorySize: 4 << 10,
			cacheSize:         4 << 10,
			expected: base.TestServerArgs{
				DefaultTenantName:             "demoapp",
				DefaultTestTenant:             base.TestControlsTenantsExplicitly,
				PartOfCluster:                 true,
				JoinAddr:                      "127.0.0.1",
				Addr:                          "127.0.0.1:1336",
				SQLAddr:                       "127.0.0.1:1236",
				HTTPAddr:                      "127.0.0.1:4569",
				ApplicationInternalRPCPortMin: 1334,
				ApplicationInternalRPCPortMax: 2358,
				DisableTLSForHTTP:             true,
				SQLMemoryPoolSize:             4 << 10,
				CacheSize:                     4 << 10,
				NoAutoInitializeCluster:       true,
				EnableDemoLoginEndpoint:       true,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						StickyVFSRegistry: stickyVFSRegistry,
					},
				},
				ExternalIODir: "nodelocal/n3",
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			demoCtx := newDemoCtx()
			demoCtx.SQLPoolMemorySize = tc.sqlPoolMemorySize
			demoCtx.CacheSize = tc.cacheSize
			demoCtx.SQLPort = 1234
			demoCtx.HTTPPort = 4567
			actual := demoCtx.testServerArgsForTransientCluster(unixSocketDetails{}, tc.serverIdx, tc.joinAddr, "", stickyVFSRegistry)
			stopper := actual.Stopper
			defer stopper.Stop(context.Background())

			assert.Len(t, actual.StoreSpecs, 1)
			assert.Equal(
				t,
				fmt.Sprintf("demo-server%d", tc.serverIdx),
				actual.StoreSpecs[0].StickyVFSID,
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

	// This is slow under race and deadlock as it starts a 9-node cluster which
	// has a very high simulated latency between each node.
	skip.UnderRace(t)
	skip.UnderDeadlock(t)

	demoCtx := newDemoCtx()
	// Set up an empty 9-node cluster with simulated latencies.
	demoCtx.SimulateLatency = true
	demoCtx.NumNodes = 9
	demoCtx.Localities = defaultLocalities

	certsDir := t.TempDir()

	cleanupFunc := securitytest.CreateTestCerts(certsDir)
	defer func() {
		if err := cleanupFunc(); err != nil {
			t.Fatal(err)
		}
	}()

	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	// Setup the transient cluster.
	c := transientCluster{
		demoCtx:           demoCtx,
		stopper:           stop.NewStopper(),
		demoDir:           certsDir,
		stickyVFSRegistry: fs.NewStickyRegistry(),
		infoLog:           log.Infof,
		warnLog:           log.Warningf,
		shoutLog:          log.Ops.Shoutf,
	}

	// Ensure the context gets canceled when the stopper terminates.
	ctx, _ = c.stopper.WithCancelOnQuiesce(ctx)

	require.NoError(t, c.Start(ctx))

	// Stop the cluster when the test exits, including when it fails.
	// This also calls the Stop() method on the stopper, and thus
	// cancels everything controlled by the stopper.
	defer c.Close(ctx)

	c.SetSimulatedLatency(true)

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
			conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.ToPQ().String())
			defer func() {
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}
			}()
			// Find the maximum latency in the cluster from the current node.
			var maxLatency regionlatency.RoundTripLatency
			localityLatencies.ForEachLatencyFrom(tc.region, func(
				_ regionlatency.Region, l regionlatency.OneWayLatency,
			) {
				if rtt := l * 2; rtt > maxLatency {
					maxLatency = rtt
				}
			})

			// Attempt to make a query that talks to every node.
			// This should take at least maxLatency.
			startTime := timeutil.Now()
			sqlExecCtx := clisqlexec.Context{}
			_, _, err = sqlExecCtx.RunQuery(
				context.Background(),
				conn,
				clisqlclient.MakeQuery(`SHOW ALL CLUSTER QUERIES`),
				false, /* showMoreChars */
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

	defer TestingForceRandomizeDemoPorts()()

	demoCtx := newDemoCtx()
	// Set up an empty 3-node cluster with tenants on each node.
	demoCtx.NumNodes = 3
	demoCtx.Multitenant = true
	demoCtx.Localities = []roachpb.Locality{
		{Tiers: []roachpb.Tier{{Key: "prize-winner", Value: "otan"}}},
		{Tiers: []roachpb.Tier{{Key: "prize-winner", Value: "otan"}}},
		{Tiers: []roachpb.Tier{{Key: "prize-winner", Value: "otan"}}},
	}

	securityassets.ResetLoader()
	certsDir := t.TempDir()

	ctx := context.Background()

	// Setup the transient cluster.
	c := transientCluster{
		demoCtx:           demoCtx,
		stopper:           stop.NewStopper(),
		demoDir:           certsDir,
		stickyVFSRegistry: fs.NewStickyRegistry(),
		infoLog:           log.Infof,
		warnLog:           log.Warningf,
		shoutLog:          log.Ops.Shoutf,
	}
	// Stop the cluster when the test exits, including when it fails.
	// This also calls the Stop() method on the stopper, and thus
	// cancels everything controlled by the stopper.
	defer c.Close(ctx)

	require.NoError(t, c.generateCerts(ctx, certsDir))
	require.NoError(t, c.Start(ctx))

	// Also ensure the context gets canceled when the stopper
	// terminates above.
	var cancel func()
	ctx, cancel = c.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()

	// Ensure CREATE TABLE below works properly.
	sqlclustersettings.RestrictAccessToSystemInterface.Override(ctx, &c.firstServer.SystemLayer().ClusterSettings().SV, false)

	testutils.RunTrueAndFalse(t, "forSecondaryTenant", func(t *testing.T, forSecondaryTenant bool) {
		url, err := c.getNetworkURLForServer(ctx, 0,
			true /* includeAppName */, serverSelection(forSecondaryTenant))
		require.NoError(t, err)
		sqlConnCtx := clisqlclient.Context{}
		conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.ToPQ().String())
		defer func() {
			require.NoError(t, conn.Close())
		}()

		// Create a table on each tenant to make sure that the tenants are separate.
		require.NoError(t, conn.Exec(ctx, "CREATE TABLE a (a int PRIMARY KEY)"))

		log.Infof(ctx, "test succeeded")
		t.Log("test succeeded")
	})
}

// Ensure that demo clusters are started with privileged tenants.
func TestTenantCapabilities(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test is too slow to complete under the race detector, sometimes.
	skip.UnderRace(t)

	defer TestingForceRandomizeDemoPorts()()

	demoCtx := newDemoCtx()
	demoCtx.NumNodes = 1
	demoCtx.Multitenant = true

	securityassets.ResetLoader()
	certsDir := t.TempDir()

	ctx := context.Background()

	// Set up the transient cluster.
	c := transientCluster{
		demoCtx:           demoCtx,
		stopper:           stop.NewStopper(),
		demoDir:           certsDir,
		stickyVFSRegistry: fs.NewStickyRegistry(),
		infoLog:           log.Infof,
		warnLog:           log.Warningf,
		shoutLog:          log.Ops.Shoutf,
	}
	// Stop the cluster when the test exits, including when it fails.
	// This also calls the Stop() method on the stopper, and thus
	// cancels everything controlled by the stopper.
	defer c.Close(ctx)

	require.NoError(t, c.generateCerts(ctx, certsDir))
	require.NoError(t, c.Start(ctx))

	// Also ensure the context gets canceled when the stopper
	// terminates above.
	var cancel func()
	ctx, cancel = c.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()

	url, err := c.getNetworkURLForServer(ctx, 0, false /* includeAppName */, forSystemTenant)
	require.NoError(t, err)
	sqlConnCtx := clisqlclient.Context{}
	conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.ToPQ().String())
	defer func() {
		require.NoError(t, conn.Close())
	}()

	sqlExecCtx := clisqlexec.Context{}
	cols, rows, err := sqlExecCtx.RunQuery(
		context.Background(),
		conn,
		clisqlclient.MakeQuery(`SHOW TENANT $1 WITH CAPABILITIES`, demoTenantName),
		false, /* showMoreChars */
	)
	require.NoError(t, err)

	expectedCols := []string{
		"id",
		"name",
		"data_state",
		"service_mode",
		"capability_name",
		"capability_value",
	}
	if !reflect.DeepEqual(expectedCols, cols) {
		t.Fatalf("expected:\n%v\ngot:\n%v", expectedCols, cols)
	}

	var expectedRows [][]string
	for _, cap := range tenantcapabilities.IDs {
		capValue := `true`
		if cap == tenantcapabilities.TenantSpanConfigBounds {
			capValue = `{}`
		}
		expectedRows = append(expectedRows, []string{`3`, demoTenantName, `ready`, `shared`, cap.String(), capValue})
	}
	if !reflect.DeepEqual(expectedRows, rows) {
		t.Fatalf("expected:\n%v\ngot:\n%v", expectedRows, rows)
	}
}
