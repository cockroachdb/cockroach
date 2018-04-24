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
	"time"

	_ "github.com/lib/pq"
)

func runClockMonotonicity(t *test, c *cluster, nodes int, tc clockMonotonicityTestCase) {
	ctx := context.Background()

	c.l.printf("Running %s\n", tc.name)

	// Test with a single node so that the node does not crash due to MaxOffset
	// violation when introducing skew
	if nodes != 1 {
		t.Fatalf("Expected num nodes to be 1, got: %d", nodes)
	}

	t.Status("deploying skew injector")
	skewInjector := newSkewInjector(c)
	skewInjector.deploy(ctx)

	t.Status("starting cockroach")
	c.Put(ctx, cockroach, "./cockroach", c.All())
	c.Start(ctx)

	db := c.Conn(ctx, nodes)
	defer db.Close()
	if _, err := db.Exec(
		fmt.Sprintf(`SET CLUSTER SETTING server.clock.persist_upper_bound_interval = '%v'`,
			tc.persistWallTimeInterval)); err != nil {
		t.Fatal(err)
	}

	if !isAlive(db) {
		t.Fatal("Node unexpectedly crashed")
	}

	preRestartTime, err := dbUnixEpoch(db)
	if err != nil {
		t.Fatal(err)
	}

	defer skewInjector.recover(ctx, nodes)

	// Inject a clock skew after stopping a node
	t.Status("stopping cockroach")
	c.Stop(ctx, c.Node(nodes))
	t.Status("injecting skew")
	skewInjector.skew(ctx, nodes, tc.skew)
	t.Status("starting cockroach post skew")
	c.Start(ctx, c.Node(nodes))

	if !isAlive(db) {
		t.Fatal("Node unexpectedly crashed")
	}

	postRestartTime, err := dbUnixEpoch(db)
	if err != nil {
		t.Fatal(err)
	}

	t.Status("validating clock monotonicity")
	c.l.printf("pre-restart time:  %f\n", preRestartTime)
	c.l.printf("post-restart time: %f\n", postRestartTime)

	if tc.expectIncreasingWallTime {
		if preRestartTime > postRestartTime {
			t.Fatalf("Expected pre-restart time %f < post-restart time %f", preRestartTime, postRestartTime)
		}
	} else {
		if preRestartTime < postRestartTime {
			t.Fatalf("Expected pre-restart time %f > post-restart time %f", preRestartTime, postRestartTime)
		}
	}
}

type clockMonotonicityTestCase struct {
	name                     string
	persistWallTimeInterval  time.Duration
	skew                     time.Duration
	expectIncreasingWallTime bool
}

func registerClockMonotonicity(r *registry) {
	const numNodes = 1

	testCases := []clockMonotonicityTestCase{
		{
			// Without enabling the feature to persist wall time, wall time is
			// currently non monotonic on backward jumps when a node is down.
			name: "non_monotonic_without_persisting_walltm",
			skew: -30 * time.Second,
			persistWallTimeInterval:  0,
			expectIncreasingWallTime: false,
		},
		{
			name: "monotonic_with_persisting_walltm",
			skew: -30 * time.Second,
			persistWallTimeInterval:  500 * time.Millisecond,
			expectIncreasingWallTime: true,
		},
	}

	for i := range testCases {
		tc := testCases[i]
		r.Add(testSpec{
			Name:  fmt.Sprintf("clockmonotonic/nodes=%d/tc=%s", numNodes, tc.name),
			Nodes: nodes(numNodes),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runClockMonotonicity(t, c, numNodes, tc)
			},
		})
	}
}
