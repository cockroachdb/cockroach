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
	"net/http"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/client"
	_ "github.com/lib/pq"
)

// runNetworkSanity is just a sanity check to make sure we're setting up toxiproxy
// correctly.
func runNetworkSanity(ctx context.Context, t *test, origC *cluster, nodes int) {
	origC.Put(ctx, cockroach, "./cockroach", origC.All())
	c, err := Toxify(ctx, t.l, origC, origC.All())
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
	//
	// NB: last node gets no latency injected, but first node gets cut off below.
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

	m := newMonitor(ctx, c.cluster, c.All())
	m.Go(func(ctx context.Context) error {
		c.Measure(ctx, 1, `SET CLUSTER SETTING trace.debug.enable = true`)
		c.Measure(ctx, 1, "CREATE DATABASE test")
		c.Measure(ctx, 1, `CREATE TABLE test.commit (a INT, b INT, v INT, PRIMARY KEY (a, b))`)

		for i := 0; i < 10; i++ {
			duration := c.Measure(ctx, 1, fmt.Sprintf(
				"BEGIN; INSERT INTO test.commit VALUES (2, %[1]d), (1, %[1]d), (3, %[1]d); COMMIT",
				i,
			))
			c.l.Printf("%s\n", duration)
		}

		c.Measure(ctx, 1, `
set tracing=on;
insert into test.commit values(3,1000), (1,1000), (2,1000);
select age, message from [ show trace for session ];
`)

		for i := 1; i < origC.spec.NodeCount; i++ {
			if dur := c.Measure(ctx, i, `SELECT 1`); dur > latency {
				t.Fatalf("node %d unexpectedly affected by latency: select 1 took %.2fs", i, dur.Seconds())
			}
		}

		return nil
	})

	m.Wait()
}

func runNetworkTPCC(ctx context.Context, t *test, origC *cluster, nodes int) {
	n := origC.spec.NodeCount
	serverNodes, workerNode := origC.Range(1, n-1), origC.Node(n)
	origC.Put(ctx, cockroach, "./cockroach", origC.All())
	origC.Put(ctx, workload, "./workload", origC.All())

	c, err := Toxify(ctx, t.l, origC, serverNodes)
	if err != nil {
		t.Fatal(err)
	}

	const warehouses = 1
	c.Start(ctx, t, serverNodes)
	c.Run(ctx, workerNode, fmt.Sprintf(
		`./workload fixtures load tpcc --warehouses=%d {pgurl:1}`, warehouses,
	))

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
	m := newMonitor(ctx, c.cluster, serverNodes)

	m.Go(func(ctx context.Context) error {
		t.WorkerStatus("running tpcc")

		tpccL, err := c.l.ChildLogger("tpcc", quietStdout, quietStderr)
		if err != nil {
			return err
		}

		cmd := fmt.Sprintf(
			"./workload run tpcc --warehouses=%d --wait=false"+
				" --histograms="+perfArtifactsDir+"/stats.json"+
				" --duration=%s {pgurl:2-%d}",
			warehouses, duration, c.spec.NodeCount-1)
		return c.RunL(ctx, tpccL, workerNode, cmd)
	})

	checkGoroutines := func(ctx context.Context) int {
		// NB: at the time of writing, the goroutine count would quickly
		// stabilize near 230 when the network is partitioned, and around 270
		// when it isn't. Experimentally a past "slow" goroutine leak leaked ~3
		// goroutines every minute (though it would likely be more with the tpcc
		// workload above), which over the duration of an hour would easily push
		// us over the threshold.
		const thresh = 350

		uiAddrs := c.ExternalAdminUIAddr(ctx, serverNodes)
		var maxSeen int
		for _, addr := range uiAddrs {
			url := "http://" + addr + "/debug/pprof/goroutine?debug=2"
			resp, err := http.Get(url)
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
		c.l.Printf("letting inbound traffic to first node time out")
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
				c.l.Printf("new goroutine peak: %d", cur)
				maxSeen = cur
			}

			select {
			case <-done:
				c.l.Printf("done checking goroutines, repairing network")
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
				c.l.Printf("network is repaired")

				// Verify that goroutine count doesn't spike.
				for i := 0; i < 20; i++ {
					nowGoroutines := checkGoroutines(ctx)
					c.l.Printf("currently at most %d goroutines per node", nowGoroutines)
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

func registerNetwork(r *testRegistry) {
	const numNodes = 4

	r.Add(testSpec{
		Name:    fmt.Sprintf("network/sanity/nodes=%d", numNodes),
		Cluster: makeClusterSpec(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runNetworkSanity(ctx, t, c, numNodes)
		},
	})
	r.Add(testSpec{
		Name:    fmt.Sprintf("network/tpcc/nodes=%d", numNodes),
		Cluster: makeClusterSpec(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runNetworkTPCC(ctx, t, c, numNodes)
		},
	})
}
