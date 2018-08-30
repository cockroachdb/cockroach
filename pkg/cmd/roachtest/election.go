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
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerElectionAfterRestart(r *registry) {
	r.Add(testSpec{
		Name:   "election-after-restart",
		Nodes:  nodes(3),
		Stable: true, // DO NOT COPY to new tests
		Run: func(ctx context.Context, t *test, c *cluster) {
			t.Status("starting up")
			c.Put(ctx, cockroach, "./cockroach")
			c.Start(ctx)

			t.Status("creating table and splits")
			c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "
        CREATE DATABASE IF NOT EXISTS test;
        CREATE TABLE test.kv (k INT PRIMARY KEY, v INT);
        -- Prevent the merge queue from immediately discarding our splits.
        SET CLUSTER SETTING kv.range_merge.queue_enabled = false;
        ALTER TABLE test.kv SPLIT AT SELECT generate_series(0, 10000, 100)"`)

			start := timeutil.Now()
			c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "
        SELECT * FROM test.kv"`)
			duration := timeutil.Since(start)
			c.l.Printf("pre-restart, query took %s\n", duration)

			t.Status("restarting")
			c.Stop(ctx)
			c.Start(ctx)

			// Each of the 100 ranges in this table must elect a leader for
			// this query to complete. In naive raft, each of these
			// elections would require waiting for a 3-second timeout, one
			// at a time. This test verifies that our mechanisms to speed
			// this up are working (we trigger elections eagerly, but not so
			// eagerly that multiple elections conflict with each other).
			start = timeutil.Now()
			c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "
        SELECT * FROM test.kv"`)
			duration = timeutil.Since(start)
			c.l.Printf("post-restart, query took %s\n", duration)
			if expected := 15 * time.Second; duration > expected {
				// In the happy case, this query runs in around 250ms. Prior
				// to the introduction of this test, a bug caused most
				// elections to fail and the query would take over 100
				// seconds. There are still issues that can cause a few
				// elections to fail (the biggest one as I write this is
				// #26448), so we must use a generous timeout here. We may be
				// able to tighten the bounds as we make more improvements.
				t.Fatalf("expected query to succeed in less than %s, took %s", expected, duration)
			}
		},
	})
}
