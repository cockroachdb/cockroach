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
	"context"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

func runClockMonotonicity(ctx context.Context, t *test, c *cluster, tc clockMonotonicityTestCase) {
	// Test with a single node so that the node does not crash due to MaxOffset
	// violation when introducing offset
	if c.spec.NodeCount != 1 {
		t.Fatalf("Expected num nodes to be 1, got: %d", c.spec.NodeCount)
	}

	t.Status("deploying offset injector")
	offsetInjector := newOffsetInjector(c)
	if err := offsetInjector.deploy(ctx); err != nil {
		t.Fatal(err)
	}

	if err := c.RunE(ctx, c.Node(1), "test -x ./cockroach"); err != nil {
		c.Put(ctx, cockroach, "./cockroach", c.All())
	}
	c.Wipe(ctx)
	c.Start(ctx, t)

	db := c.Conn(ctx, c.spec.NodeCount)
	defer db.Close()
	if _, err := db.Exec(
		fmt.Sprintf(`SET CLUSTER SETTING server.clock.persist_upper_bound_interval = '%v'`,
			tc.persistWallTimeInterval)); err != nil {
		t.Fatal(err)
	}

	// Wait for Cockroach to process the above cluster setting
	time.Sleep(10 * time.Second)

	if !isAlive(db, c.l) {
		t.Fatal("Node unexpectedly crashed")
	}

	preRestartTime, err := dbUnixEpoch(db)
	if err != nil {
		t.Fatal(err)
	}

	// Recover from the injected clock offset after validation completes.
	defer func() {
		if !isAlive(db, c.l) {
			t.Fatal("Node unexpectedly crashed")
		}
		// Stop cockroach node before recovering from clock offset as this clock
		// jump can crash the node.
		c.Stop(ctx, c.Node(c.spec.NodeCount))
		t.l.Printf("recovering from injected clock offset")

		offsetInjector.recover(ctx, c.spec.NodeCount)

		c.Start(ctx, t, c.Node(c.spec.NodeCount))
		if !isAlive(db, c.l) {
			t.Fatal("Node unexpectedly crashed")
		}
	}()

	// Inject a clock offset after stopping a node
	t.Status("stopping cockroach")
	c.Stop(ctx, c.Node(c.spec.NodeCount))
	t.Status("injecting offset")
	offsetInjector.offset(ctx, c.spec.NodeCount, tc.offset)
	t.Status("starting cockroach post offset")
	c.Start(ctx, t, c.Node(c.spec.NodeCount))

	if !isAlive(db, c.l) {
		t.Fatal("Node unexpectedly crashed")
	}

	postRestartTime, err := dbUnixEpoch(db)
	if err != nil {
		t.Fatal(err)
	}

	t.Status("validating clock monotonicity")
	t.l.Printf("pre-restart time:  %f\n", preRestartTime)
	t.l.Printf("post-restart time: %f\n", postRestartTime)
	difference := postRestartTime - preRestartTime
	t.l.Printf("time-difference: %v\n", time.Duration(difference*float64(time.Second)))

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

func registerClockMonotonicTests(r *testRegistry) {
	testCases := []clockMonotonicityTestCase{
		{
			name:                     "persistent",
			offset:                   -60 * time.Second,
			persistWallTimeInterval:  500 * time.Millisecond,
			expectIncreasingWallTime: true,
		},
	}

	for i := range testCases {
		tc := testCases[i]
		spec := testSpec{
			Name:  "clock/monotonic/" + tc.name,
			Owner: OwnerKV,
			// These tests muck with NTP, therefor we don't want the cluster reused by
			// others.
			Cluster: makeClusterSpec(1, reuseTagged("offset-injector")),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runClockMonotonicity(ctx, t, c, tc)
			},
		}
		r.Add(spec)
	}
}
