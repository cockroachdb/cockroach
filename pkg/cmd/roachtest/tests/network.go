// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	_ "github.com/lib/pq" // register postgres driver
	"github.com/stretchr/testify/require"
)

// runNetworkAuthentication creates a network black hole to the leaseholder
// of system.users, and then validates that the time required to create
// new connections to the cluster afterwards remains under a reasonable limit.
func runNetworkAuthentication(ctx context.Context, t test.Test, c cluster.Cluster) {
	if c.IsLocal() {
		t.Fatal("cannot be run in local mode: usage of sudo iptables in local environment discouraged")
	}
	n := c.Spec().NodeCount
	serverNodes, clientNode := c.Range(1, n-1), c.Node(n)

	t.L().Printf("starting nodes to initialize TLS certs...")
	// NB: we need to start two times, because when we use
	// c.Start() separately on nodes 1 and nodes 2-3,
	// the logic will find the certs don't exist on node 2 and
	// 3 will re-recreate a separate set of certs, which
	// we don't want. Starting all nodes at once ensures
	// that they use coherent certs.
	settings := install.MakeClusterSettings()

	// Don't create a backup schedule in this test as the cluster won't be up
	// long and we'll inject network issues.
	// We wait for replication so that we can safely restart the cluster in two
	// steps next.
	c.Start(
		ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule, option.WaitForReplication()), settings, serverNodes,
	)
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
	{
		// We start n2+ first so that there's quorum.
		startOpts := option.NewStartOpts(option.NoBackupSchedule)
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, "--locality=node=other", "--accept-sql-without-tls")
		c.Start(ctx, t.L(), startOpts, settings, c.Range(2, n-1))
	}
	{
		// Now start n1.
		startOpts := option.NewStartOpts(option.NoBackupSchedule)
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, "--locality=node=1", "--accept-sql-without-tls")
		c.Start(ctx, t.L(), startOpts, settings, c.Node(1))
	}

	t.L().Printf("retrieving server addresses...")
	serverUrls, err := c.InternalPGUrl(ctx, t.L(), serverNodes, roachprod.PGURLOptions{Auth: install.AuthUserPassword})
	require.NoError(t, err)

	t.L().Printf("fetching certs...")
	certsDir := fmt.Sprintf("/home/ubuntu/%s", install.CockroachNodeCertsDir)
	localCertsDir, err := filepath.Abs("./network-certs")
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(localCertsDir))
	require.NoError(t, c.Get(ctx, t.L(), certsDir, localCertsDir, c.Node(1)))
	require.NoError(t, filepath.WalkDir(localCertsDir, func(path string, d fs.DirEntry, err error) error {
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
	err = roachtestutil.WaitFor3XReplication(ctx, t.L(), db)
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
				dumpRangesCmd := roachtestutil.NewCommand("./cockroach sql -e 'TABLE crdb_internal.ranges'").
					Flag("certs-dir", certsDir).
					Flag("port", "{pgport:1}").
					String()
				t.L().Printf("SQL: %s", dumpRangesCmd)
				err = c.RunE(ctx, option.WithNodes(c.Node(1)), dumpRangesCmd)
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
				url := serverUrls[server-1]

				// Attempt a client connection to that server.
				t.L().Printf("server %d, attempt %d; url: %s\n", server, attempt, url)

				b, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(clientNode), "time", "-p", "./cockroach", "sql",
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
		netConfigCmd := fmt.Sprintf(`
# ensure any failure fails the entire script.
set -e;

# Setting default filter policy
sudo iptables -P INPUT ACCEPT;
sudo iptables -P OUTPUT ACCEPT;

# Drop any node-to-node crdb traffic.
sudo iptables -A INPUT -p tcp --dport {pgport%s} -j DROP;
sudo iptables -A OUTPUT -p tcp --dport {pgport%s} -j DROP;

sudo iptables-save
`,
			c.Node(expectedLeaseholder), c.Node(expectedLeaseholder))
		t.L().Printf("partitioning using iptables; config cmd:\n%s", netConfigCmd)
		require.NoError(t, c.RunE(ctx, option.WithNodes(c.Node(expectedLeaseholder)), netConfigCmd))

		defer func() {
			// Check that iptable DROP actually blocked traffic.
			t.L().Printf("verify that traffic to node %d is blocked", expectedLeaseholder)
			packetsDropped, err := iptablesPacketsDropped(ctx, t.L(), c, c.Node(expectedLeaseholder))
			if err != nil {
				t.Fatal(err)
			}
			if packetsDropped == 0 {
				t.Fatalf("Expected node %d to be partitioned but reported no packets dropped.", expectedLeaseholder)
			}

			// (attempt to) restore iptables when test end, so that cluster
			// can be investigated afterwards.
			restoreNet := fmt.Sprintf(`
set -e;
sudo iptables -D INPUT -p tcp --dport {pgport%s} -j DROP;
sudo iptables -D OUTPUT -p tcp --dport {pgport%s} -j DROP;
sudo iptables-save
`,
				c.Node(expectedLeaseholder), c.Node(expectedLeaseholder))
			t.L().Printf("restoring iptables; config cmd:\n%s", restoreNet)
			require.NoError(t, c.RunE(ctx, option.WithNodes(c.Node(expectedLeaseholder)), restoreNet))
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

// runClientNetworkConnectionTimeout simulates a scenario where the client and
// server loose connectivity with a connection that is idle. The purpose of this
// test is to confirm that the keep alive settings are enforced.
func runClientNetworkConnectionTimeout(ctx context.Context, t test.Test, c cluster.Cluster) {
	if c.IsLocal() {
		t.Fatal("cannot be run in local mode: usage of sudo iptables in local environment discouraged")
	}
	n := c.Spec().NodeCount
	serverNodes, clientNode := c.Range(1, n-1), c.Nodes(n)
	settings := install.MakeClusterSettings()
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, serverNodes)
	certsDir := "/home/ubuntu/certs"
	t.L().Printf("connecting to cluster from roachtest...")
	db, err := c.ConnE(ctx, t.L(), 1)
	require.NoError(t, err)
	defer db.Close()

	grp := t.NewErrorGroup(task.WithContext(ctx))
	// Startup a connection on the client server, which will be running a
	// long transaction (i.e. just the sleep builtin).
	var runOutput install.RunResultDetails
	grp.Go(func(ctx context.Context, l *logger.Logger) error {
		urls, err := roachprod.PgURL(ctx, l, c.MakeNodes(c.Node(1)), certsDir, roachprod.PGURLOptions{
			External: true,
			Secure:   true,
		})
		if err != nil {
			return err
		}
		commandThatWillDisconnect := fmt.Sprintf(`./cockroach sql --certs-dir %s --url %s -e "SELECT pg_sleep(600)"`, certsDir, urls[0])
		l.Printf("Executing long running query: %s", commandThatWillDisconnect)
		output, err := c.RunWithDetails(ctx, l, option.WithNodes(clientNode), commandThatWillDisconnect)
		runOutput = output[0]
		return err
	})
	// Confirm that the connection was started.
	testutils.SucceedsSoon(t, func() error {
		row := db.QueryRow("SELECT count(*) FROM [SHOW CLUSTER SESSIONS] WHERE active_queries='SELECT pg_sleep(600)'")
		var count int
		if err := row.Scan(&count); err != nil {
			return err
		}
		// Wait for the query to start up.
		if count != 1 {
			return errors.AssertionFailedf("unexepcted count :%v", count)
		}
		return nil
	})

	netConfigCmd := fmt.Sprintf(`
# ensure any failure fails the entire script.
set -e;

# Setting default filter policy
sudo iptables -P INPUT ACCEPT;
sudo iptables -P OUTPUT ACCEPT;

# Drop any client traffic to CRDB.
sudo iptables -A INPUT -p tcp --sport {pgport%s} -j DROP;
sudo iptables -A OUTPUT -p tcp --dport {pgport%s} -j DROP;
`,
		c.Node(1), c.Node(1))
	t.L().Printf("blocking networking on client; config cmd:\n%s", netConfigCmd)
	blockStartTime := timeutil.Now()
	require.NoError(t, c.RunE(ctx, option.WithNodes(clientNode), netConfigCmd))

	// (attempt to) restore iptables when test end, so that the client
	// can be investigated afterward.
	defer func() {
		const restoreNet = `
set -e;
sudo iptables -F INPUT;
sudo iptables -F OUTPUT;
`
		t.L().Printf("restoring iptables; config cmd:\n%s", restoreNet)
		require.NoError(t, c.RunE(ctx, option.WithNodes(clientNode), restoreNet))
	}()

	// We expect the connection to timeout within 30 seconds based on
	// the default settings. We will wait for up to 1 minutes for the
	// connection to drop.
	testutils.SucceedsWithin(t, func() error {
		row := db.QueryRow("SELECT count(*) FROM [SHOW CLUSTER SESSIONS] WHERE active_queries='SELECT pg_sleep(600)'")
		var count int
		if err := row.Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return errors.AssertionFailedf("unexepcted count :%d", count)
		}
		return nil
	},
		time.Minute)
	// Confirm it took at least a minute for the connection to clear out.
	require.Greaterf(t, timeutil.Since(blockStartTime), time.Second*30, "connection dropped earlier than expected")
	t.L().Printf("Connection was dropped after %s", timeutil.Since(blockStartTime))
	// We expect the connection to be dropped with the lower keep alive settings.
	require.NoError(t, grp.WaitE())
	require.Contains(t, runOutput.Stderr, "If the server is running, check --host client-side and --advertise server-side",
		"Did not detect connection failure %s %d", runOutput.Stderr, runOutput.RemoteExitStatus)
}

func registerNetwork(r registry.Registry) {
	const numNodes = 4
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("network/authentication/nodes=%d", numNodes),
		Owner:            registry.OwnerKV, // Should be moved to new security team once one exists.
		Cluster:          r.MakeClusterSpec(numNodes),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runNetworkAuthentication(ctx, t, c)
		},
	})

	r.Add(registry.TestSpec{
		Name:             "network/client-connection-timeout",
		Owner:            registry.OwnerSQLFoundations,
		Cluster:          r.MakeClusterSpec(2), // One server and client
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run:              runClientNetworkConnectionTimeout,
	})
}

// iptablesPacketsDropped returns the number of packets dropped to a given node due to an iptables rule.
// TODO(darrylwong): this is mostly just a validation check to make sure we set up the rules correctly.
// We should remove this in favor for the failure injection library which has it's own validation.
func iptablesPacketsDropped(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, node option.NodeListOption,
) (int, error) {
	// Filter for only rules on the SQL port as roachprod adds firewall rules for node_exporter.
	res, err := c.RunWithDetailsSingleNode(ctx, l, option.WithNodes(node), fmt.Sprintf("sudo iptables -L -x -v -n | grep {pgport%s}", node))
	if err != nil {
		return 0, err
	}
	rows := strings.Split(res.Stdout, "\n")
	// There will be an input and output rule, either works.
	values := strings.Fields(rows[0])
	if len(values) == 0 {
		return 0, errors.Errorf("no configured iptables rules found:\n%s", res.Stdout)
	}
	packetsDropped, err := strconv.Atoi(values[0])
	return packetsDropped, errors.Wrapf(err, "could not find number of packets dropped, rules found:\n%s", res.Stdout)
}
