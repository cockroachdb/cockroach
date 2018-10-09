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

func runClockMonotonicity(ctx context.Context, t *test, c *cluster, tc clockMonotonicityTestCase) {
	// Test with a single node so that the node does not crash due to MaxOffset
	// violation when introducing offset
	if c.nodes != 1 {
		t.Fatalf("Expected num nodes to be 1, got: %d", c.nodes)
	}

	t.Status("deploying offset injector")
	offsetInjector := newOffsetInjector(c)
	offsetInjector.deploy(ctx)

	if err := c.RunE(ctx, c.Node(1), "test -x ./cockroach"); err != nil {
		c.Put(ctx, cockroach, "./cockroach", c.All())
	}
	c.Wipe(ctx)
	c.Start(ctx, t)

	db := c.Conn(ctx, c.nodes)
	defer db.Close()
	if _, err := db.Exec(
		fmt.Sprintf(`SET CLUSTER SETTING server.clock.persist_upper_bound_interval = '%v'`,
			tc.persistWallTimeInterval)); err != nil {
		t.Fatal(err)
	}

	// Wait for Cockroach to process the above cluster setting
	time.Sleep(10 * time.Second)

	if !isAlive(db) {
		t.Fatal("Node unexpectedly crashed")
	}

	preRestartTime, err := dbUnixEpoch(db)
	if err != nil {
		t.Fatal(err)
	}

	defer offsetInjector.recover(ctx, c.nodes)

	// Inject a clock offset after stopping a node
	t.Status("stopping cockroach")
	c.Stop(ctx, c.Node(c.nodes))
	t.Status("injecting offset")
	offsetInjector.offset(ctx, c.nodes, tc.offset)
	t.Status("starting cockroach post offset")
	c.Start(ctx, t, c.Node(c.nodes))

	if !isAlive(db) {
		t.Fatal("Node unexpectedly crashed")
	}

	postRestartTime, err := dbUnixEpoch(db)
	if err != nil {
		t.Fatal(err)
	}

	t.Status("validating clock monotonicity")
	c.l.Printf("pre-restart time:  %f\n", preRestartTime)
	c.l.Printf("post-restart time: %f\n", postRestartTime)
	difference := postRestartTime - preRestartTime
	c.l.Printf("time-difference: %v\n", time.Duration(difference*float64(time.Second)))

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
	offset                   time.Duration
	expectIncreasingWallTime bool
}

func makeClockMonotonicTests() testSpec {
	testCases := []clockMonotonicityTestCase{
		{
			name:                     "persistent",
			offset:                   -60 * time.Second,
			persistWallTimeInterval:  500 * time.Millisecond,
			expectIncreasingWallTime: true,
		},
	}

	spec := testSpec{
		Name:   "monotonic",
		Stable: true, // DO NOT COPY to new tests
	}

	for i := range testCases {
		tc := testCases[i]
		spec.SubTests = append(spec.SubTests, testSpec{
			Name:   tc.name,
			Stable: true, // DO NOT COPY to new tests
			Run: func(ctx context.Context, t *test, c *cluster) {
				runClockMonotonicity(ctx, t, c, tc)
			},
		})
	}

	return spec
}
