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
	c.Start(ctx, c.All())

	waitReplication := func(downNode int) error {
		db := c.Conn(ctx, nodes)
		defer db.Close()

		for ok := false; !ok; {
			if err := db.QueryRow(
				"SELECT min(array_length(replicas, 1)) >= 3 FROM crdb_internal.ranges WHERE array_position(replicas, $1) IS NULL",
				downNode,
			).Scan(&ok); err != nil {
				return err
			}
		}
		return nil
	}

	if err := waitReplication(0 /* no down node */); err != nil {
		t.Fatal(err)
	}

	loadDuration := " --duration=" + duration.String()

	workloads := []string{
		// TODO(tschottdorf): in remote mode, the ui shows that we consistently write
		// at 330 qps (despite asking for 500 below). Locally we get 500qps (and a lot
		// more without rate limiting). Check what's up with that.
		"./workload run kv --max-rate 500 --tolerate-errors --init" + loadDuration + " {pgurl:1-%d}",
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
			return c.RunE(ctx, c.Node(node), "./cockroach quit --insecure --port "+port)
		}

		decom := func(id string) error {
			port := fmt.Sprintf("{pgport:%d}", nodes) // always use last node
			return c.RunE(ctx, c.Node(nodes), "./cockroach node decommission --insecure --wait=live --port "+port+" "+id)
		}

		for tBegin, whileDown, node := timeutil.Now(), true, 1; timeutil.Since(tBegin) <= duration; whileDown, node = !whileDown, (node%numDecom)+1 {
			t.Status(fmt.Sprintf("decommissioning %d (down=%t)", node, whileDown))
			id, err := nodeID(node)
			if err != nil {
				return err
			}
			if whileDown {
				if err := stop(node); err != nil {
					return err
				}
			}
			if err := decom(id); err != nil {
				return err
			}
			if whileDown {
				if err := waitReplication(node); err != nil {
					return err
				}
			} else {
				if err := stop(node); err != nil {
					return err
				}
			}
			if err := c.RunE(ctx, c.Node(node), "rm -rf {store-dir}"); err != nil {
				return err
			}
			c.Start(ctx, c.Node(node), startArgs("-a", "--join "+c.InternalIP(ctx, nodes)))
			t.Status("sleeping")
			select {
			case <-ctx.Done():
				return ctx.Err()
			// Give the cluster some time to mess up.
			//
			// NB: we should have zone configs set so that the newly arrived
			// nodes look appealing to the allocator and then sleep until
			// they've taken on some data. I wasted at least an hour on this
			// but roachprod gets in the way big time as it owns the `start`
			// invocation and thus I can't set store attributes or
			// localities. Additionally, the used localities are a flag to
			// `roachtest` and so outside of the control of the test.
			//
			// As written, the test is likely ineffective at putting data on
			// the nodes being cycled. In fact, the default localities put
			// the first node in the same locality as the fourth, and so it
			// won't usually be considered for diversity.
			//
			// TODO(petermattis): allow overriding args for the start invocation
			// or provide the required functionality otherwise.
			case <-time.After(time.Minute):
			}
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

func init() {
	const numNodes = 4
	duration := time.Hour

	tests.Add(testSpec{
		Name:  fmt.Sprintf("decommission/nodes=%d/duration=%s", numNodes, duration),
		Nodes: nodes(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			if local {
				duration = 3 * time.Minute
				fmt.Printf("running with duration=%s in local mode\n", duration)
			}
			runDecommission(t, c, numNodes, duration)
		},
	})
}
