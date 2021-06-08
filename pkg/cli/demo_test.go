// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Example_demo() {
	c := NewCLITest(TestCLIParams{NoServer: true})
	defer c.Cleanup()

	defer func(b bool) { testingForceRandomizeDemoPorts = b }(testingForceRandomizeDemoPorts)
	testingForceRandomizeDemoPorts = true

	testData := [][]string{
		{`demo`, `-e`, `show database`},
		{`demo`, `-e`, `show database`, `--no-example-database`},
		{`demo`, `-e`, `show application_name`},
		{`demo`, `--format=table`, `-e`, `show database`},
		{`demo`, `-e`, `select 1 as "1"`, `-e`, `select 3 as "3"`},
		{`demo`, `--echo-sql`, `-e`, `select 1 as "1"`},
		{`demo`, `--set=errexit=0`, `-e`, `select nonexistent`, `-e`, `select 123 as "123"`},
		{`demo`, `startrek`, `-e`, `SELECT database_name, owner FROM [show databases]`},
		{`demo`, `startrek`, `-e`, `SELECT database_name, owner FROM [show databases]`, `--format=table`},
		// Test that if we start with --insecure we cannot perform
		// commands that require a secure cluster.
		{`demo`, `-e`, `CREATE USER test WITH PASSWORD 'testpass'`},
		{`demo`, `--insecure`, `-e`, `CREATE USER test WITH PASSWORD 'testpass'`},
		{`demo`, `--geo-partitioned-replicas`, `--disable-demo-license`},
	}
	setCLIDefaultsForTests()
	// We must reset the security asset loader here, otherwise the dummy
	// asset loader that is set by default in tests will not be able to
	// find the certs that demo sets up.
	security.ResetAssetLoader()
	for _, cmd := range testData {
		// `demo` sets up a server and log file redirection, which asserts
		// that the logging subsystem has not been initialized yet. Fake
		// this to be true.
		log.TestingResetActive()
		c.RunWithArgs(cmd)
	}

	// Output:
	// demo -e show database
	// database
	// movr
	// demo -e show database --no-example-database
	// database
	// defaultdb
	// demo -e show application_name
	// application_name
	// $ cockroach demo
	// demo --format=table -e show database
	//   database
	// ------------
	//   movr
	// (1 row)
	// demo -e select 1 as "1" -e select 3 as "3"
	// 1
	// 1
	// 3
	// 3
	// demo --echo-sql -e select 1 as "1"
	// > select 1 as "1"
	// 1
	// 1
	// demo --set=errexit=0 -e select nonexistent -e select 123 as "123"
	// ERROR: column "nonexistent" does not exist
	// SQLSTATE: 42703
	// 123
	// 123
	// demo startrek -e SELECT database_name, owner FROM [show databases]
	// database_name	owner
	// defaultdb	root
	// postgres	root
	// startrek	demo
	// system	node
	// demo startrek -e SELECT database_name, owner FROM [show databases] --format=table
	//   database_name | owner
	// ----------------+--------
	//   defaultdb     | root
	//   postgres      | root
	//   startrek      | demo
	//   system        | node
	// (4 rows)
	// demo -e CREATE USER test WITH PASSWORD 'testpass'
	// CREATE ROLE
	// demo --insecure -e CREATE USER test WITH PASSWORD 'testpass'
	// ERROR: setting or updating a password is not supported in insecure mode
	// SQLSTATE: 28P01
	// demo --geo-partitioned-replicas --disable-demo-license
	// ERROR: enterprise features are needed for this demo (--geo-partitioned-replicas)
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
			demoCtxTemp := demoCtx
			demoCtx.sqlPoolMemorySize = tc.sqlPoolMemorySize
			demoCtx.cacheSize = tc.cacheSize

			actual := testServerArgsForTransientCluster(unixSocketDetails{}, tc.nodeID, tc.joinAddr, "", 1234, 4567, stickyEnginesRegistry)
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

			assert.Equal(t, tc.expected, actual)

			// Restore demoCtx state after each test.
			demoCtx = demoCtxTemp
		})
	}
}

func TestTransientClusterSimulateLatencies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This is slow under race as it starts a 9-node cluster which
	// has a very high simulated latency between each node.
	skip.UnderRace(t)

	// Ensure flags are reset on start, and also make sure
	// at the end of the test they are reset.
	TestingReset()
	defer TestingReset()

	// Set up an empty 9-node cluster with simulated latencies.
	demoCtx.simulateLatency = true
	demoCtx.nodes = 9

	certsDir, err := ioutil.TempDir("", "cli-demo-test")
	require.NoError(t, err)

	cleanupFunc := createTestCerts(certsDir)
	defer func() {
		if err := cleanupFunc(); err != nil {
			t.Fatal(err)
		}
	}()

	ctx := context.Background()

	// Setup the transient cluster.
	c := transientCluster{
		stopper:              stop.NewStopper(),
		demoDir:              certsDir,
		stickyEngineRegistry: server.NewStickyInMemEnginesRegistry(),
	}
	// Stop the cluster when the test exits, including when it fails.
	// This also calls the Stop() method on the stopper, and thus
	// cancels everything controlled by the stopper.
	defer c.cleanup(ctx)

	// Also ensure the context gets canceled when the stopper
	// terminates above.
	ctx, _ = c.stopper.WithCancelOnQuiesce(ctx)

	require.NoError(t, c.start(ctx, demoCmd, nil /* gen */))

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
			url, err := c.getNetworkURLForServer(ctx, tc.nodeIdx, true /* includeAppName */)
			require.NoError(t, err)
			conn := makeSQLConn(url.ToPQ().String())
			defer conn.Close()
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
			_, _, err = runQuery(
				conn,
				makeQuery(`SHOW ALL CLUSTER QUERIES`),
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
