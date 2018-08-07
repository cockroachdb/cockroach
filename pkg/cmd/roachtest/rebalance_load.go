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
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/sync/errgroup"
)

func registerRebalanceLoad(r *registry) {
	// This test creates a single table for kv to use and splits the table to
	// have one range for every node in the cluster. Because even brand new
	// clusters start with 20+ ranges in them, the number of new ranges in kv's
	// table is small enough that it typically won't trigger rebalancing of
	// leases in the cluster based on lease count alone. We let kv generate a lot
	// of load against the ranges such that when
	// kv.allocator.stat_based_rebalancing.enabled is set to true, we'd expect
	// load-based rebalancing to distribute the load evenly across the nodes in
	// the cluster. Without that setting, the fact that the kv table has so few
	// ranges means that they probablistically won't have their leases evenly
	// spread across all the nodes (they'll often just end up staying on n1).
	//
	// In other words, this test should always pass with
	// kv.allocator.stat_based_rebalancing.enabled set to true, while it should
	// usually (but not always fail) with it set to false.
	rebalanceLoadRun := func(ctx context.Context, t *test, c *cluster, duration time.Duration, concurrency int) {
		roachNodes := c.Range(1, c.nodes-1)
		appNode := c.Node(c.nodes)

		c.Put(ctx, cockroach, "./cockroach", roachNodes)
		args := startArgs(
			"--args=--vmodule=store_rebalancer=5,allocator=5,allocator_scorer=5,replicate_queue=5")
		c.Start(ctx, roachNodes, args)

		c.Put(ctx, workload, "./workload", appNode)
		c.Run(ctx, appNode, `./workload init kv --drop {pgurl:1}`)

		var m *errgroup.Group // see comment in version.go
		m, ctx = errgroup.WithContext(ctx)

		m.Go(func() error {
			c.l.printf("starting load generator\n")

			quietL, err := newLogger("run kv", strconv.Itoa(0), "workload"+strconv.Itoa(0), ioutil.Discard, os.Stderr)
			if err != nil {
				return err
			}
			splits := len(roachNodes) - 1 // n-1 splits => n ranges => 1 lease per node
			return c.RunL(ctx, quietL, appNode, fmt.Sprintf(
				"./workload run kv --read-percent=95 --splits=%d --tolerate-errors --concurrency=%d "+
					"--duration=%s {pgurl:1-3}",
				splits, concurrency, duration.String()))
		})

		m.Go(func() error {
			t.Status(fmt.Sprintf("starting checks for lease balance"))

			db := c.Conn(ctx, 1)
			defer db.Close()

			if _, err := db.ExecContext(
				ctx, `SET CLUSTER SETTING kv.allocator.stat_based_rebalancing.enabled=true`,
			); err != nil {
				return err
			}

			for tBegin := timeutil.Now(); timeutil.Since(tBegin) <= duration; {
				if done, err := isLoadEvenlyDistributed(c.l, db, len(roachNodes)); err != nil {
					return err
				} else if done {
					c.l.printf("successfully achieved lease balance\n")
					return nil
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Second):
				}
			}

			return fmt.Errorf("timed out before leases were evenly spread")
		})
		if err := m.Wait(); err != nil {
			t.Fatal(err)
		}
	}

	minutes := 2 * time.Minute
	numNodes := 4 // the last node is just used to generate load
	concurrency := 128

	r.Add(testSpec{
		Name:   `rebalance-leases-by-load`,
		Nodes:  nodes(numNodes),
		Stable: false, // TODO(a-robinson): Promote to stable
		Run: func(ctx context.Context, t *test, c *cluster) {
			if local {
				concurrency = 32
				fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
			}
			rebalanceLoadRun(ctx, t, c, minutes, concurrency)
		},
	})
}

func isLoadEvenlyDistributed(l *logger, db *gosql.DB, numNodes int) (bool, error) {
	rows, err := db.Query(
		`select lease_holder, count(*) ` +
			`from [show experimental_ranges from table kv.kv] ` +
			`group by lease_holder;`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	leaseCounts := make(map[int]int)
	var rangeCount int
	for rows.Next() {
		var storeID, leaseCount int
		if err := rows.Scan(&storeID, &leaseCount); err != nil {
			return false, err
		}
		leaseCounts[storeID] = leaseCount
		rangeCount += leaseCount
	}
	l.printf("numbers of test.kv leases on each store: %v\n", leaseCounts)

	if len(leaseCounts) < numNodes {
		l.printf("not all nodes have a lease yet: %v\n", leaseCounts)
		return false, nil
	}

	// The simple case is when ranges haven't split. We can require that every
	// store has one lease.
	if rangeCount == numNodes {
		for _, leaseCount := range leaseCounts {
			if leaseCount != 1 {
				l.printf("uneven lease distribution: %v\n", leaseCounts)
				return false, nil
			}
		}
		return true, nil
	}

	// For completeness, if leases have split, verify the leases per store don't
	// differ by any more than 1.
	leases := make([]int, 0, numNodes)
	for _, leaseCount := range leaseCounts {
		leases = append(leases, leaseCount)
	}
	sort.Ints(leases)
	if leases[0]+1 < leases[len(leases)-1] {
		l.printf("leases per store differ by more than one: %v\n", leaseCounts)
		return false, nil
	}

	return true, nil
}
