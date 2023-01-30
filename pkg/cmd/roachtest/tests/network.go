// Copyright 2018 The Cockroach Authors.
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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	_ "github.com/lib/pq" // register postgres driver
	"github.com/stretchr/testify/require"
)

// runNetworkAuthentication creates a network black hole to the leaseholder
// of system.users, and then validates that the time required to create
// new connections to the cluster afterwards remains under a reasonable limit.
func runNetworkAuthentication(ctx context.Context, t test.Test, c cluster.Cluster) {
	n := c.Spec().NodeCount
	serverNodes, clientNode := c.Range(1, n-1), c.Node(n)

	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())

	t.L().Printf("starting nodes to initialize TLS certs...")
	// NB: we need to start two times, because when we use
	// c.Start() separately on nodes 1 and nodes 2-3,
	// the logic will find the certs don't exist on node 2 and
	// 3 will re-recreate a separate set of certs, which
	// we don't want. Starting all nodes at once ensures
	// that they use coherent certs.
	settings := install.MakeClusterSettings(install.SecureOption(true))

	// Don't create a backup schedule as this test shuts the cluster down immediately.
	c.Start(ctx, t.L(), option.DefaultStartOptsNoBackups(), settings, serverNodes)
	require.NoError(t, c.StopE(ctx, t.L(), option.DefaultStopOpts(), serverNodes))

	t.L().Printf("restarting nodes...")
	// For troubleshooting the test, the engineer can add the following
	// environment variables to make the rebalancing faster.
	// However, they should be removed for the production version
	// of the test, because they make the cluster recover from a failure
	// in a way that is unrealistically fast.
	// "--env=COCKROACH_SCAN_INTERVAL=200ms",
	// "--env=COCKROACH_SCAN_MAX_IDLE_TIME=20ms",
	//
	// Currently, creating a scheduled backup at start fails, potentially due to
	// the induced network partition. Further investigation required to allow scheduled backups
	// to run on this test.
	startOpts := option.DefaultStartOptsNoBackups()
	startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, "--locality=node=1", "--accept-sql-without-tls")
	c.Start(ctx, t.L(), startOpts, settings, c.Node(1))

	// See comment above about env vars.
	// "--env=COCKROACH_SCAN_INTERVAL=200ms",
	// "--env=COCKROACH_SCAN_MAX_IDLE_TIME=20ms",
	startOpts = option.DefaultStartOptsNoBackups()
	startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, "--locality=node=other", "--accept-sql-without-tls")
	c.Start(ctx, t.L(), startOpts, settings, c.Range(2, n-1))

	t.L().Printf("retrieving server addresses...")
	serverAddrs, err := c.InternalAddr(ctx, t.L(), serverNodes)
	require.NoError(t, err)

	t.L().Printf("fetching certs...")
	certsDir := "/home/ubuntu/certs"
	localCertsDir, err := filepath.Abs("./network-certs")
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(localCertsDir))
	require.NoError(t, c.Get(ctx, t.L(), certsDir, localCertsDir, c.Node(1)))
	require.NoError(t, filepath.Walk(localCertsDir, func(path string, info os.FileInfo, err error) error {
		// Don't change permissions for the certs directory.
		if path == localCertsDir {
			return nil
		}
		if err != nil {
			return err
		}
		return os.Chmod(path, os.FileMode(0600))
	}))

	t.L().Printf("connecting to cluster from roachtest...")
	db, err := c.ConnE(ctx, t.L(), 1)
	require.NoError(t, err)
	defer db.Close()

	// Wait for up-replication. This will also print a progress message.
	err = WaitFor3XReplication(ctx, t, db)
	require.NoError(t, err)

	t.L().Printf("creating test user...")
	_, err = db.Exec(`CREATE USER testuser WITH PASSWORD 'password' VALID UNTIL '2060-01-01'`)
	require.NoError(t, err)
	_, err = db.Exec(`GRANT admin TO testuser`)
	require.NoError(t, err)

	const expectedLeaseholder = 1
	lh := fmt.Sprintf("%d", expectedLeaseholder)

	t.L().Printf("configuring zones to move ranges to node 1...")
	for _, zone := range []string{
		`RANGE liveness`,
		`RANGE meta`,
		`RANGE system`,
		`RANGE default`,
		`DATABASE system`,
	} {
		zoneCmd := `ALTER ` + zone + ` CONFIGURE ZONE USING lease_preferences = '[[+node=` + lh + `]]', constraints = '{"+node=` + lh + `": 1}'`
		t.L().Printf("SQL: %s", zoneCmd)
		_, err = db.Exec(zoneCmd)
		require.NoError(t, err)
	}

	t.L().Printf("waiting for leases to move...")
	{
		tStart := timeutil.Now()
		for ok := false; !ok; time.Sleep(time.Second) {
			if timeutil.Since(tStart) > 30*time.Second {
				t.L().Printf("still waiting for leases to move")
				// The leases have not moved yet, so display some progress.
				dumpRangesCmd := fmt.Sprintf(`./cockroach sql --certs-dir %s -e 'TABLE crdb_internal.ranges'`, certsDir)
				t.L().Printf("SQL: %s", dumpRangesCmd)
				err := c.RunE(ctx, c.Node(1), dumpRangesCmd)
				require.NoError(t, err)
			}

			const waitLeases = `
SELECT $1::INT = ALL (
    SELECT lease_holder
    FROM   [SHOW CLUSTER RANGES WITH TABLES, DETAILS]
     WHERE (start_key = '/System/NodeLiveness' AND end_key = '/System/NodeLivenessMax')
       OR  (table_name IN ('users', 'role_members', 'role_options'))
)`
			t.L().Printf("SQL: %s", waitLeases)
			require.NoError(t, db.QueryRow(waitLeases, expectedLeaseholder).Scan(&ok))
		}
	}

	cancelTestCtx, cancelTest := context.WithCancel(ctx)

	// Channel to expedite the end of the waiting below
	// in case an error occurs.
	woopsCh := make(chan struct{}, len(serverNodes)-1)

	m := c.NewMonitor(ctx, serverNodes)

	var numConns uint32

	for i := 1; i <= c.Spec().NodeCount-1; i++ {
		if i == expectedLeaseholder {
			continue
		}

		// Ensure that every goroutine below gets a different copy of i.
		server := i

		// Start a client loop for the server "i".
		m.Go(func(ctx context.Context) error {
			errCount := 0
			for attempt := 0; ; attempt++ {
				select {
				case <-ctx.Done():
					// The monitor has decided that this goroutine needs to go away, presumably
					// because another goroutine encountered an error.
					t.L().Printf("server %d: stopping connections due to error", server)

					// Expedite the wait below. This is not strictly required for correctness,
					// and makes the test faster to terminate in case of failure.
					woopsCh <- struct{}{}

					// Stop this goroutine too.
					return ctx.Err()

				case <-cancelTestCtx.Done():
					// The main goroutine below is instructing this client
					// goroutine to terminate gracefully before the test terminates.
					t.L().Printf("server %d: stopping connections due to end of test", server)
					return nil

				case <-time.After(500 * time.Millisecond):
					// Wait for .5 second between connection attempts.
				}

				// Construct a connection URL to server i.
				url := fmt.Sprintf("postgres://testuser:password@%s/defaultdb?sslmode=require", serverAddrs[server-1])

				// Attempt a client connection to that server.
				t.L().Printf("server %d, attempt %d; url: %s\n", server, attempt, url)

				b, err := c.RunWithDetailsSingleNode(ctx, t.L(), clientNode, "time", "-p", "./cockroach", "sql",
					"--url", url, "--certs-dir", certsDir, "-e", "'SELECT 1'")

				// Report the results of execution.
				t.L().Printf("server %d, attempt %d, result:\n%s\n", server, attempt, b)
				// Indicate, to the main goroutine, that we have at least one connection
				// attempt completed.
				atomic.AddUint32(&numConns, 1)

				if err != nil {
					if errCount == 0 {
						// We tolerate the first error as acceptable.
						t.L().Printf("server %d, attempt %d (1st ERROR, TOLERATE): %v", server, attempt, err)
						errCount++
						continue
					}
					// Any error beyond the first is unacceptable.
					t.L().Printf("server %d, attempt %d (2nd ERROR, BAD): %v", server, attempt, err)

					// Expedite the wait below. This is not strictly required for correctness,
					// and makes the test faster to terminate in case of failure.
					woopsCh <- struct{}{}
					return err
				}
			}
		})
	}

	// Main test goroutine. Run the body of the test, including the
	// network partition, into a sub-function. This ensures that the
	// network partition is resolved by the time the monitor finishes
	// waiting on the servers.
	func() {
		t.L().Printf("waiting for clients to start connecting...")
		testutils.SucceedsSoon(t, func() error {
			select {
			case <-woopsCh:
				t.Fatal("connection error before network partition")
			default:
			}
			if atomic.LoadUint32(&numConns) == 0 {
				return errors.New("no connection yet")
			}
			return nil
		})

		t.L().Printf("blocking networking on node 1...")
		const netConfigCmd = `
# ensure any failure fails the entire script.
set -e;

# Setting default filter policy
sudo iptables -P INPUT ACCEPT;
sudo iptables -P OUTPUT ACCEPT;

# Drop any node-to-node crdb traffic.
sudo iptables -A INPUT -p tcp --dport 26257 -j DROP;
sudo iptables -A OUTPUT -p tcp --dport 26257 -j DROP;

sudo iptables-save
`
		t.L().Printf("partitioning using iptables; config cmd:\n%s", netConfigCmd)
		require.NoError(t, c.RunE(ctx, c.Node(expectedLeaseholder), netConfigCmd))

		// (attempt to) restore iptables when test end, so that cluster
		// can be investigated afterwards.
		defer func() {
			const restoreNet = `
set -e;
sudo iptables -D INPUT -p tcp --dport 26257 -j DROP;
sudo iptables -D OUTPUT -p tcp --dport 26257 -j DROP;
sudo iptables-save
`
			t.L().Printf("restoring iptables; config cmd:\n%s", restoreNet)
			require.NoError(t, c.RunE(ctx, c.Node(expectedLeaseholder), restoreNet))
		}()

		t.L().Printf("waiting while clients attempt to connect...")
		select {
		case <-time.After(20 * time.Second):
		case <-woopsCh:
		}

		// Terminate all the async goroutines.
		cancelTest()
	}()

	// Test finished.
	m.Wait()
}

func registerNetwork(r registry.Registry) {
	const numNodes = 4
	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("network/authentication/nodes=%d", numNodes),
		Owner:   registry.OwnerKV, // Should be moved to new security team once one exists.
		Cluster: r.MakeClusterSpec(numNodes),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runNetworkAuthentication(ctx, t, c)
		},
	})
}
