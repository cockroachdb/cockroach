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

func runClockJump(t *test, c *cluster, nodes int, tc clockJumpTestCase) {
	ctx := context.Background()

	c.l.printf("Running %s\n", tc.name)

	// Test with a single node so that the node does not crash due to MaxOffset
	// violation when injecting skew
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
		fmt.Sprintf(
			`SET CLUSTER SETTING server.clock.forward_jump_check_enabled= %v`,
			tc.jumpCheckEnabled)); err != nil {
		t.Fatal(err)
	}

	if !isAlive(db) {
		t.Fatal("Node unexpectedly crashed")
	}

	t.Status("injecting skew")
	defer skewInjector.recover(ctx, nodes)
	skewInjector.skew(ctx, nodes, tc.skew)

	t.Status("validating health")
	aliveAfterSkew := isAlive(db)
	if aliveAfterSkew != tc.aliveAfterSkew {
		t.Fatalf("Expected node health %v, got %v", tc.aliveAfterSkew, aliveAfterSkew)
	}
}

type clockJumpTestCase struct {
	name             string
	jumpCheckEnabled bool
	skew             time.Duration
	aliveAfterSkew   bool
}

func registerClockJump(r *registry) {
	const numNodes = 1

	testCases := []clockJumpTestCase{
		{
			name:             "large_forward_jump_with_check",
			skew:             500 * time.Millisecond,
			jumpCheckEnabled: true,
			aliveAfterSkew:   false,
		},
		{
			name:             "small_forward_jump_with_check",
			skew:             200 * time.Millisecond,
			jumpCheckEnabled: true,
			aliveAfterSkew:   true,
		},
		{
			name:             "large_backward_jump_with_check",
			skew:             -500 * time.Millisecond,
			jumpCheckEnabled: true,
			aliveAfterSkew:   true,
		},
		{
			name:             "large_forward_jump_without_check",
			skew:             500 * time.Millisecond,
			jumpCheckEnabled: false,
			aliveAfterSkew:   true,
		},
		{
			name:             "large_backward_jump_without_check",
			skew:             -500 * time.Millisecond,
			jumpCheckEnabled: false,
			aliveAfterSkew:   true,
		},
	}

	for i := range testCases {
		tc := testCases[i]
		r.Add(testSpec{
			Name:  fmt.Sprintf("clockjump/nodes=%d/tc=%s", numNodes, tc.name),
			Nodes: nodes(numNodes),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runClockJump(t, c, numNodes, tc)
			},
		})
	}
}
