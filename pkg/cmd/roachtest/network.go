// Copyright 2018 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/client"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
	_ "github.com/lib/pq"
)

// runNetworkSanity is just a sanity check to make sure we're setting up toxiproxy
// correctly. It injects latency between the nodes and verifies that we're not
// seeing the latency on the client connection running `SELECT 1` on each node.
func runNetworkSanity(ctx context.Context, t *test, origC Cluster, nodes int) {
	origC.Put(ctx, cockroach, "./cockroach", origC.All())
	c, err := Toxify(ctx, t, origC, origC.All())
	if err != nil {
		t.Fatal(err)
	}

	c.Start(ctx, t, c.All())

	db := c.Conn(ctx, 1) // unaffected by toxiproxy
	defer db.Close()
	waitForFullReplication(t, db)

	// NB: we're generous with latency in this test because we're checking that
	// the upstream connections aren't affected by latency below, but the fixed
	// cost of starting the binary and processing the query is already close to
	// 100ms.
	const latency = 300 * time.Millisecond
	for i := 1; i <= nodes; i++ {
		// NB: note that these latencies only apply to connections *to* the node
		// on which the toxic is active. That is, if n1 has a (down or upstream)
		// latency toxic of 100ms, then none of its outbound connections are
		// affected but any connections made to it by other nodes will.
		// In particular, it's difficult to simulate intricate network partitions
		// as there's no way to activate toxics only for certain peers.
		proxy := c.Proxy(i)
		if _, err := proxy.AddToxic("", "latency", "downstream", 1, toxiproxy.Attributes{
			"latency": latency / (2 * time.Millisecond), // ms
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := proxy.AddToxic("", "latency", "upstream", 1, toxiproxy.Attributes{
			"latency": latency / (2 * time.Millisecond), // ms
		}); err != nil {
			t.Fatal(err)
		}
	}

	m := newMonitor(ctx, c.Cluster, c.All())
	m.Go(func(ctx context.Context) error {
		c.Measure(ctx, 1, `SET CLUSTER SETTING trace.debug.enable = true`)
		c.Measure(ctx, 1, "CREATE DATABASE test")
		c.Measure(ctx, 1, `CREATE TABLE test.commit (a INT, b INT, v INT, PRIMARY KEY (a, b))`)

		for i := 0; i < 10; i++ {
			duration := c.Measure(ctx, 1, fmt.Sprintf(
				"BEGIN; INSERT INTO test.commit VALUES (2, %[1]d), (1, %[1]d), (3, %[1]d); COMMIT",
				i,
			))
			t.l.Printf("%s\n", duration)
		}

		c.Measure(ctx, 1, `
set tracing=on;
insert into test.commit values(3,1000), (1,1000), (2,1000);
select age, message from [ show trace for session ];
`)

		for i := 1; i <= origC.Spec().NodeCount; i++ {
			if dur := c.Measure(ctx, i, `SELECT 1`); dur > latency {
				t.Fatalf("node %d unexpectedly affected by latency: select 1 took %.2fs", i, dur.Seconds())
			}
		}

		return nil
	})

	m.Wait()
}

// runNetworkAuthentication creates a network black hole to the leaseholder
// of system.users, and then measures how long it takes to be able to create
// new connections to the cluster afterwards.
func runNetworkAuthentication(ctx context.Context, t *test, c Cluster) {
	c.Put(ctx, cockroach, "./cockroach", c.All())

	c.Start(ctx, t, c.Node(1), startArgs("--secure", "--args=--locality=node=1"))
	c.Start(ctx, t, c.Nodes(2, 3), startArgs("--secure", "--args=--locality=node=other"))

	certsDir := "/home/ubuntu/certs"
	localCertsDir, err := filepathAbs("./certs")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(localCertsDir); err != nil {
		t.Fatal(err)
	}

	if err := c.Get(ctx, t.l, certsDir, localCertsDir, c.Node(1)); err != nil {
		t.Fatal(err)
	}

	// Certs can have at max 0600 privilege.
	if err := filepath.Walk(localCertsDir, func(path string, info os.FileInfo, err error) error {
		// Don't change permissions for the certs directory.
		if path == localCertsDir {
			return nil
		}
		if err != nil {
			return err
		}
		return os.Chmod(path, os.FileMode(0600))
	}); err != nil {
		t.Fatal(err)
	}

	db, err := c.ConnSecure(ctx, 2, "root", localCertsDir, 26257)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	waitForFullReplication(t, db)

	if _, err := db.Exec(`CREATE USER testuser WITH PASSWORD 'password'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER RANGE liveness CONFIGURE ZONE USING lease_preferences = '[[+node=1]]', constraints = '{"+node=1": 1}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER RANGE meta CONFIGURE ZONE USING lease_preferences = '[[+node=1]]', constraints = '{"+node=1": 1}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER RANGE system CONFIGURE ZONE USING lease_preferences = '[[+node=1]]', constraints = '{"+node=1": 1}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER DATABASE system CONFIGURE ZONE USING lease_preferences = '[[+node=1]]', constraints = '{"+node=1": 1}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER RANGE default CONFIGURE ZONE USING lease_preferences = '[[+node=1]]', constraints = '{"+node=1": 1}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE system.public.replication_constraint_stats CONFIGURE ZONE USING lease_preferences = '[[+node=1]]', constraints = '{"+node=1": 1}', num_replicas = 3`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE system.public.replication_stats CONFIGURE ZONE USING lease_preferences = '[[+node=1]]', constraints = '{"+node=1": 1}', num_replicas = 3`); err != nil {
		t.Fatal(err)
	}

	const expectedLeaseholder = 1
	testutils.SucceedsSoon(t, func() error {
		var leaseholder int
		if err := db.QueryRow(`
SELECT lease_holder FROM crdb_internal.ranges
WHERE start_pretty = '/System/NodeLiveness' AND end_pretty = '/System/NodeLivenessMax'`,
		).Scan(&leaseholder); err != nil {
			return err
		}
		if leaseholder != expectedLeaseholder {
			return errors.Newf("liveness leaseholder should be node %d", expectedLeaseholder)
		}
		if err := db.QueryRow(
			"SELECT lease_holder FROM [SHOW RANGES FROM TABLE system.public.users]",
		).Scan(&leaseholder); err != nil {
			return err
		}
		if leaseholder != expectedLeaseholder {
			return errors.Newf("system.users leaseholder should be node %d", expectedLeaseholder)
		}
		return nil
	})

	rows, err := db.Query(`
WITH
    at_risk_zones AS (
            SELECT
                zone_id, locality, at_risk_ranges
            FROM
                system.replication_critical_localities
            WHERE
                at_risk_ranges > 0
        ),
    report AS (
            SELECT
                crdb_internal.zones.zone_id,
                target,
                database_name,
                table_name,
                index_name,
                at_risk_zones.at_risk_ranges
            FROM
                crdb_internal.zones, at_risk_zones
            WHERE
                crdb_internal.zones.zone_id
                = at_risk_zones.zone_id
        )
SELECT DISTINCT * FROM report;
`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var at_risk_target string
		err := rows.Scan(&at_risk_target)
		if err != nil {
			t.Fatal(err)
		}
		t.l.Printf("found at risk range. target: %s\n", at_risk_target)
	}

	m := newMonitor(ctx, c, c.All())
	m.Go(func(ctx context.Context) error {
		endTest := time.After(20 * time.Second)
		tick := time.Tick(500 * time.Millisecond)
		errorCount := 0
		keepLooping := true
		for attempt := 0; keepLooping; attempt++ {
			select {
			case <-endTest:
				keepLooping = false
			case <-tick:
			}
			t.l.Printf("connection attempt %d\n", attempt)
			for i := 1; i <= c.Spec().NodeCount; i++ {
				if i == expectedLeaseholder {
					continue
				}

				url := "postgres://testuser:password@localhost:26257/defaultdb?sslmode=require"
				b, err := c.RunWithBuffer(ctx, t.l, c.Node(i),
					"time", "-p", "./cockroach", "sql",
					"--url", url, "--certs-dir", certsDir, "-e", "'SELECT now()'")
				t.l.Printf("%s\n", b)
				if err != nil {
					errorCount++
				}
			}
		}

		if errorCount >= 1 /*c.Spec().NodeCount*/ {
			return errors.Newf("failed authentication %d times; expected fewer than %d", errorCount, 1 /*c.Spec().NodeCount*/)
		}
		return nil
	})

	time.Sleep(5 * time.Second)
	if err := blackholeNode(ctx, c, expectedLeaseholder); err != nil {
		t.Fatal(err)
	}
	m.Wait()
}

// runNetworkAZOutage creates a network black hole to an entire locality of a
// 15-node cluster, then confirms that the cluster is still able to maintain
// quorum.
func runNetworkAZOutage(ctx context.Context, t *test, c Cluster) {
	c.Put(ctx, cockroach, "./cockroach", c.All())

	c.Start(ctx, t, c.Range(1, 5), startArgs("--secure", "--args=--locality=az=1"))
	c.Start(ctx, t, c.Range(6, 10), startArgs("--secure", "--args=--locality=az=2"))
	c.Start(ctx, t, c.Range(11, 15), startArgs("--secure", "--args=--locality=az=3"))

	certsDir := "/home/ubuntu/certs"
	localCertsDir, err := filepathAbs("./certs")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(localCertsDir); err != nil {
		t.Fatal(err)
	}

	if err := c.Get(ctx, t.l, certsDir, localCertsDir, c.Node(1)); err != nil {
		t.Fatal(err)
	}

	// Certs can have at max 0600 privilege.
	if err := filepath.Walk(localCertsDir, func(path string, info os.FileInfo, err error) error {
		// Don't change permissions for the certs directory.
		if path == localCertsDir {
			return nil
		}
		if err != nil {
			return err
		}
		return os.Chmod(path, os.FileMode(0600))
	}); err != nil {
		t.Fatal(err)
	}

	db, err := c.ConnSecure(ctx, 1, "root", localCertsDir, 26257)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	waitForFullReplication(t, db)

	if _, err := db.Exec(`CREATE USER testuser WITH PASSWORD 'password'`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`ALTER RANGE liveness CONFIGURE ZONE USING lease_preferences = '[[+az=1]]', constraints = '{"+az=1": 1}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER RANGE meta CONFIGURE ZONE USING lease_preferences = '[[+az=1]]', constraints = '{"+az=1": 1}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER RANGE system CONFIGURE ZONE USING lease_preferences = '[[+az=1]]', constraints = '{"+az=1": 1}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER DATABASE system CONFIGURE ZONE USING lease_preferences = '[[+az=1]]', constraints = '{"+az=1": 1}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER RANGE default CONFIGURE ZONE USING lease_preferences = '[[+az=1]]', constraints = '{"+az=1": 1}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE system.public.replication_constraint_stats CONFIGURE ZONE USING lease_preferences = '[[+az=1]]', constraints = '{"+az=1": 1}', num_replicas = 3`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE system.public.replication_stats CONFIGURE ZONE USING lease_preferences = '[[+az=1]]', constraints = '{"+az=1": 1}', num_replicas = 3`); err != nil {
		t.Fatal(err)
	}

	const expectedLargestLeaseholder = 5
	testutils.SucceedsSoon(t, func() error {
		var leaseholder int
		if err := db.QueryRow(`
SELECT lease_holder FROM crdb_internal.ranges
WHERE start_pretty = '/System/NodeLiveness' AND end_pretty = '/System/NodeLivenessMax'`,
		).Scan(&leaseholder); err != nil {
			return err
		}
		if leaseholder > expectedLargestLeaseholder {
			return errors.Newf("liveness leaseholder should be node %d", expectedLargestLeaseholder)
		}
		if err := db.QueryRow(
			"SELECT lease_holder FROM [SHOW RANGES FROM TABLE system.public.users]",
		).Scan(&leaseholder); err != nil {
			return err
		}
		if leaseholder > expectedLargestLeaseholder {
			return errors.Newf("system.users leaseholder should be node %d", expectedLargestLeaseholder)
		}
		return nil
	})

	rows, err := db.Query(`
WITH
    at_risk_zones AS (
            SELECT
                zone_id, locality, at_risk_ranges
            FROM
                system.replication_critical_localities
            WHERE
                at_risk_ranges > 0
        ),
    report AS (
            SELECT
                crdb_internal.zones.zone_id,
                target,
                database_name,
                table_name,
                index_name,
                at_risk_zones.at_risk_ranges
            FROM
                crdb_internal.zones, at_risk_zones
            WHERE
                crdb_internal.zones.zone_id
                = at_risk_zones.zone_id
        )
SELECT DISTINCT * FROM report;
`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var at_risk_target string
		err := rows.Scan(&at_risk_target)
		if err != nil {
			t.Fatal(err)
		}
		t.l.Printf("found at risk range. target: %s\n", at_risk_target)
	}

	m := newMonitor(ctx, c, c.All())
	m.Go(func(ctx context.Context) error {
		endTest := time.After(120 * time.Second)
		tick := time.Tick(2 * time.Second)
		errorCount := 0
		keepLooping := true
		for attempt := 0; keepLooping; attempt++ {
			select {
			case <-endTest:
				keepLooping = false
			case <-tick:
			}
			t.l.Printf("status check %d\n", attempt)

			b, err := c.RunWithBuffer(ctx, t.l, c.Node(6),
				"time", "-p", "./cockroach", "node", "status",
				"--certs-dir", certsDir)
			t.l.Printf("liveness report:\n%s\n", b)
			if err != nil {
				errorCount++
			}
		}

		if errorCount > 0 {
			return errors.Newf("failed node status check %d times", errorCount)
		}
		return nil
	})

	time.Sleep(5 * time.Second)
	for i := 1; i <= 5; i++ {
		// Avoid using the loop variable inside the closure.
		i := i
		m.Go(func(ctx context.Context) error {
			return blackholeNode(ctx, c, i)
		})
	}
	m.Wait()
}

func runNetworkTPCC(ctx context.Context, t *test, origC Cluster, nodes int) {
	n := origC.Spec().NodeCount
	serverNodes, workerNode := origC.Range(1, n-1), origC.Node(n)
	origC.Put(ctx, cockroach, "./cockroach", origC.All())
	origC.Put(ctx, workload, "./workload", origC.All())

	c, err := Toxify(ctx, t, origC, serverNodes)
	if err != nil {
		t.Fatal(err)
	}

	const warehouses = 1
	c.Start(ctx, t, serverNodes)
	c.Run(ctx, c.Node(1), tpccImportCmd(warehouses))

	db := c.Conn(ctx, 1)
	defer db.Close()
	waitForFullReplication(t, db)

	duration := time.Hour
	if local {
		// NB: this is really just testing the test with this duration, it won't
		// be able to detect slow goroutine leaks.
		duration = 5 * time.Minute
	}

	// Run TPCC, but don't give it the first node (or it basically won't do anything).
	m := newMonitor(ctx, c.Cluster, serverNodes)

	m.Go(func(ctx context.Context) error {
		t.WorkerStatus("running tpcc")

		cmd := fmt.Sprintf(
			"./workload run tpcc --warehouses=%d --wait=false"+
				" --histograms="+perfArtifactsDir+"/stats.json"+
				" --duration=%s {pgurl:2-%d}",
			warehouses, duration, c.Spec().NodeCount-1)
		return c.RunE(ctx, workerNode, cmd)
	})

	checkGoroutines := func(ctx context.Context) int {
		// NB: at the time of writing, the goroutine count would quickly
		// stabilize near 230 when the network is partitioned, and around 270
		// when it isn't. Experimentally a past "slow" goroutine leak leaked ~3
		// goroutines every minute (though it would likely be more with the tpcc
		// workload above), which over the duration of an hour would easily push
		// us over the threshold.
		const thresh = 350

		uiAddrs, err := c.ExternalAdminUIAddr(ctx, serverNodes)
		if err != nil {
			t.Fatal(err)
		}
		var maxSeen int
		// The goroutine dump may take a while to generate, maybe more
		// than the 3 second timeout of the default http client.
		httpClient := httputil.NewClientWithTimeout(15 * time.Second)
		for _, addr := range uiAddrs {
			url := "http://" + addr + "/debug/pprof/goroutine?debug=2"
			resp, err := httpClient.Get(ctx, url)
			if err != nil {
				t.Fatal(err)
			}
			content, err := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			numGoroutines := bytes.Count(content, []byte("goroutine "))
			if numGoroutines >= thresh {
				t.Fatalf("%s shows %d goroutines (expected <%d)", url, numGoroutines, thresh)
			}
			if maxSeen < numGoroutines {
				maxSeen = numGoroutines
			}
		}
		return maxSeen
	}

	m.Go(func(ctx context.Context) error {
		time.Sleep(10 * time.Second) // give tpcc a head start
		// Give n1 a network partition from the remainder of the cluster. Note that even though it affects
		// both the "upstream" and "downstream" directions, this is in fact an asymmetric partition since
		// it only affects connections *to* the node. n1 itself can connect to the cluster just fine.
		proxy := c.Proxy(1)
		t.l.Printf("letting inbound traffic to first node time out")
		for _, direction := range []string{"upstream", "downstream"} {
			if _, err := proxy.AddToxic("", "timeout", direction, 1, toxiproxy.Attributes{
				"timeout": 0, // forever
			}); err != nil {
				t.Fatal(err)
			}
		}

		t.WorkerStatus("checking goroutines")
		done := time.After(duration)
		var maxSeen int
		for {
			cur := checkGoroutines(ctx)
			if maxSeen < cur {
				t.l.Printf("new goroutine peak: %d", cur)
				maxSeen = cur
			}

			select {
			case <-done:
				t.l.Printf("done checking goroutines, repairing network")
				// Repair the network. Note that the TPCC workload would never
				// finish (despite the duration) without this. In particular,
				// we don't want to m.Wait() before we do this.
				toxics, err := proxy.Toxics()
				if err != nil {
					t.Fatal(err)
				}
				for _, toxic := range toxics {
					if err := proxy.RemoveToxic(toxic.Name); err != nil {
						t.Fatal(err)
					}
				}
				t.l.Printf("network is repaired")

				// Verify that goroutine count doesn't spike.
				for i := 0; i < 20; i++ {
					nowGoroutines := checkGoroutines(ctx)
					t.l.Printf("currently at most %d goroutines per node", nowGoroutines)
					time.Sleep(time.Second)
				}

				return nil
			default:
				time.Sleep(3 * time.Second)
			}
		}
	})

	m.Wait()
}

// blackholeNode sets an iptables rule on the given node to block all
// network traffic except for SSH.
func blackholeNode(ctx context.Context, c Cluster, nodeId int) error {
	nodeIP, err := c.InternalIP(ctx, c.Node(nodeId))
	if err != nil {
		return err
	}
	nodeExternalIP, err := c.ExternalIP(ctx, c.Node(nodeId))
	if err != nil {
		return err
	}
	if err := c.RunE(ctx, c.Node(nodeId), fmt.Sprintf(`
# Setting default filter policy
sudo iptables -P INPUT DROP;
sudo iptables -P OUTPUT DROP;
sudo iptables -P FORWARD DROP;

# Allow unlimited traffic on loopback
sudo iptables -A INPUT -i lo -j ACCEPT;
sudo iptables -A OUTPUT -o lo -j ACCEPT;

# Allow incoming ssh only
sudo iptables -A INPUT -p tcp -s 0/0 -d %[1]s --sport 513:65535 --dport 22 -j ACCEPT;
sudo iptables -A OUTPUT -p tcp -s %[1]s -d 0/0 --sport 22 --dport 513:65535 -j ACCEPT;
sudo iptables -A INPUT -p tcp -s 0/0 -d %[2]s --sport 513:65535 --dport 22 -j ACCEPT;
sudo iptables -A OUTPUT -p tcp -s %[2]s -d 0/0 --sport 22 --dport 513:65535 -j ACCEPT;

# make sure nothing comes or goes out of this box
sudo iptables -A INPUT -j DROP;
sudo iptables -A OUTPUT -j DROP;
sudo iptables-save`, nodeIP[0], nodeExternalIP[0])); err != nil {
		return errors.Wrapf(err, "error applying iptables rules")
	}
	return nil
}

func registerNetwork(r *testRegistry) {
	const numNodes = 4

	r.Add(testSpec{
		Name:    fmt.Sprintf("network/sanity/nodes=%d", numNodes),
		Owner:   OwnerKV,
		Cluster: makeClusterSpec(numNodes),
		Run: func(ctx context.Context, t *test, c Cluster) {
			runNetworkSanity(ctx, t, c, numNodes)
		},
	})
	r.Add(testSpec{
		Name:    fmt.Sprintf("network/authentication/nodes=3"),
		Owner:   OwnerKV,
		Cluster: makeClusterSpec(3),
		Run: func(ctx context.Context, t *test, c Cluster) {
			runNetworkAuthentication(ctx, t, c)
		},
	})
	r.Add(testSpec{
		Name:    fmt.Sprintf("network/azblackhole/nodes=15"),
		Owner:   OwnerKV,
		Cluster: makeClusterSpec(15),
		Run: func(ctx context.Context, t *test, c Cluster) {
			runNetworkAZOutage(ctx, t, c)
		},
	})
	r.Add(testSpec{
		Name:    fmt.Sprintf("network/tpcc/nodes=%d", numNodes),
		Owner:   OwnerKV,
		Cluster: makeClusterSpec(numNodes),
		Skip:    "https://github.com/cockroachdb/cockroach/issues/49901#issuecomment-640666646",
		SkipDetails: `The ordering of steps in the test is:

- install toxiproxy
- start cluster, wait for up-replication
- launch the goroutine that starts the tpcc client command, but do not wait on
it starting
- immediately, cause a network partition
- only then, the goroutine meant to start the tpcc client goes to fetch the
pg URLs and start workload, but of course this fails because network
partition
- tpcc fails to start, so the test tears down before it resolves the network partition
- test tear-down and debug zip fail because the network partition is still active

There are two problems here:

the tpcc client is not actually started yet when the test sets up the
network partition. This is a race condition. there should be a defer in
there to resolve the partition when the test aborts prematurely. (And the
command to resolve the partition should not be sensitive to the test
context's Done() channel, because during a tear-down that is closed already)
`,
		Run: func(ctx context.Context, t *test, c Cluster) {
			runNetworkTPCC(ctx, t, c, numNodes)
		},
	})
}
