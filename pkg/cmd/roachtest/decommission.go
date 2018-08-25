// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

// TODO(tschottdorf): verify that the logs don't contain the messages
// that would spam the log before #23605. I wonder if we should really
// start grepping the logs. An alternative is to introduce a metric
// that would have signaled this and check that instead.
func runDecommission(t *test, c *cluster, nodes int, duration time.Duration) {
	ctx := context.Background()

	const defaultReplicationFactor = 3
	// The number of nodes we're going to cycle through. Since we're sometimes
	// killing the nodes and then removing them, this means having to be careful
	// with loss of quorum. So only ever touch a fixed minority of nodes and
	// swap them out for as long as the test runs. The math boils down to `1`,
	// but conceivably we'll want to run a test with replication factor five
	// at some point.
	numDecom := (defaultReplicationFactor - 1) / 2

	c.Put(ctx, workload, "./workload", c.Node(nodes))
	c.Put(ctx, cockroach, "./cockroach", c.All())

	for i := 1; i <= numDecom; i++ {
		c.Start(ctx, c.Node(i), startArgs(fmt.Sprintf("-a=--attrs=node%d", i)))
	}

	c.Start(ctx, c.Range(numDecom+1, nodes))
	c.Run(ctx, c.Node(nodes), `./workload init kv --drop`)

	waitReplicatedAwayFrom := func(downNodeID string) error {
		db := c.Conn(ctx, nodes)
		defer db.Close()

		for {
			var count int
			if err := db.QueryRow(
				// Check if the down node has any replicas.
				"SELECT count(*) FROM crdb_internal.ranges WHERE array_position(replicas, $1) IS NOT NULL",
				downNodeID,
			).Scan(&count); err != nil {
				return err
			}
			if count == 0 {
				fullReplicated := false
				if err := db.QueryRow(
					// Check if all ranges are fully replicated.
					"SELECT min(array_length(replicas, 1)) >= 3 FROM crdb_internal.ranges",
				).Scan(&fullReplicated); err != nil {
					return err
				}
				if fullReplicated {
					break
				}
			}
			time.Sleep(time.Second)
		}
		return nil
	}

	waitUpReplicated := func(targetNodeID string) error {
		db := c.Conn(ctx, nodes)
		defer db.Close()

		for ok := false; !ok; {
			stmtReplicaCount := fmt.Sprintf(
				`SELECT count(*) = 0 FROM crdb_internal.ranges WHERE array_position(replicas, %s) IS NULL and database = 'kv';`, targetNodeID)
			t.Status(stmtReplicaCount)
			if err := db.QueryRow(stmtReplicaCount).Scan(&ok); err != nil {
				return err
			}
			time.Sleep(time.Second)
		}
		return nil
	}

	if err := waitReplicatedAwayFrom("0" /* no down node */); err != nil {
		t.Fatal(err)
	}

	loadDuration := " --duration=" + duration.String()

	workloads := []string{
		// TODO(tschottdorf): in remote mode, the ui shows that we consistently write
		// at 330 qps (despite asking for 500 below). Locally we get 500qps (and a lot
		// more without rate limiting). Check what's up with that.
		"./workload run kv --max-rate 500 --tolerate-errors" + loadDuration + " {pgurl:1-%d}",
	}

	run := func(stmt string) {
		db := c.Conn(ctx, nodes)
		defer db.Close()

		t.Status(stmt)
		_, err := db.ExecContext(ctx, stmt)
		if err != nil {
			t.Fatal(err)
		}
		c.l.printf(fmt.Sprintf("run: %s\n", stmt))
	}

	var m *errgroup.Group // see comment in version.go
	m, ctx = errgroup.WithContext(ctx)
	for i, cmd := range workloads {
		cmd := cmd // copy is important for goroutine
		i := i     // ditto

		cmd = fmt.Sprintf(cmd, nodes)
		m.Go(func() error {
			quietL, err := newLogger(cmd, strconv.Itoa(i), "workload"+strconv.Itoa(i), ioutil.Discard, os.Stderr)
			if err != nil {
				return err
			}
			return c.RunL(ctx, quietL, c.Node(nodes), cmd)
		})
	}

	m.Go(func() error {
		nodeID := func(node int) (string, error) {
			dbNode := c.Conn(ctx, node)
			defer dbNode.Close()
			var nodeID string
			if err := dbNode.QueryRow(`SELECT node_id FROM crdb_internal.node_runtime_info LIMIT 1`).Scan(&nodeID); err != nil {
				return "", nil
			}
			return nodeID, nil
		}

		stop := func(node int) error {
			port := fmt.Sprintf("{pgport:%d}", node)
			defer time.Sleep(time.Second) // work around quit returning too early
			return c.RunE(ctx, c.Node(node), "./cockroach quit --insecure --host=:"+port)
		}

		decom := func(id string) error {
			port := fmt.Sprintf("{pgport:%d}", nodes) // always use last node
			t.Status("decommissioning node", id)
			return c.RunE(ctx, c.Node(nodes), "./cockroach node decommission --insecure --wait=live --host=:"+port+" "+id)
		}

		for tBegin, whileDown, node := timeutil.Now(), true, 1; timeutil.Since(tBegin) <= duration; whileDown, node = !whileDown, (node%numDecom)+1 {
			t.Status(fmt.Sprintf("decommissioning %d (down=%t)", node, whileDown))
			id, err := nodeID(node)

			if err != nil {
				return err
			}
			run(fmt.Sprintf(`ALTER RANGE default EXPERIMENTAL CONFIGURE ZONE 'constraints: {"+node%d"}'`, node))

			if err := waitUpReplicated(id); err != nil {
				return err
			}

			if whileDown {
				if err := stop(node); err != nil {
					return err
				}
			}

			run(fmt.Sprintf(`ALTER RANGE default EXPERIMENTAL CONFIGURE ZONE 'constraints: {"-node%d"}'`, node))

			if err := decom(id); err != nil {
				return err
			}

			if err := waitReplicatedAwayFrom(id); err != nil {
				return err
			}

			if !whileDown {
				if err := stop(node); err != nil {
					return err
				}
			}

			if err := c.RunE(ctx, c.Node(node), "rm -rf {store-dir}"); err != nil {
				return err
			}

			db := c.Conn(ctx, 1)
			defer db.Close()

			c.Start(ctx, c.Node(node), startArgs(fmt.Sprintf("-a=--join %s --attrs=node%d",
				c.InternalAddr(ctx, c.Node(nodes))[0], node)))
		}
		// TODO(tschottdorf): run some ui sanity checks about decommissioned nodes
		// having disappeared. Verify that the workloads don't dip their qps or
		// show spikes in latencies.
		return nil
	})
	if err := m.Wait(); err != nil {
		t.Fatal(err)
	}
}

func registerDecommission(r *registry) {
	const numNodes = 4
	duration := time.Hour

	r.Add(testSpec{
		Name:   fmt.Sprintf("decommission/nodes=%d/duration=%s", numNodes, duration),
		Nodes:  nodes(numNodes),
		Stable: true, // DO NOT COPY to new tests
		Run: func(ctx context.Context, t *test, c *cluster) {
			if local {
				duration = 3 * time.Minute
				fmt.Printf("running with duration=%s in local mode\n", duration)
			}
			runDecommission(t, c, numNodes, duration)
		},
	})
}
