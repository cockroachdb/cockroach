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
// permissions and limitations under the License.

package main

import (
	"context"
	"math/rand"
	"time"
)

// chaosTimer can be passed into randomKillChaos to get a deterministic schedule
// of kills that ends cleanly when the given context expires.
func chaosTimer(
	ctx context.Context, before, between time.Duration,
) func() (time.Duration, time.Duration) {
	return func() (time.Duration, time.Duration) {
		if ctx.Err() != nil {
			return 0, 0 // done
		}
		return before, between
	}
}

// randomKillChaos repeatedly kills and restarts a random node of the given set
// without setting off the monitor, assuming that they are initially running.
//
// The timing function returns
// 1. the duration during which all nodes are up
// 2. the duration during which one node is down.
//
// If the timing function return the zero tuple, the chaos agent stops without
// returning an error to the monitor.
func randomKillChaos(
	c *cluster, m *monitor, timing func() (time.Duration, time.Duration), nodes nodeListOption,
) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		l, err := c.l.childLogger("CHAOS")
		if err != nil {
			return err
		}
		for ctx.Err() == nil {
			before, between := timing()
			if before == 0 && between == 0 {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(before):
			}

			target := nodes[rand.Intn(len(nodes))]
			l.printf("killing %d (slept %s)\n", target, before)
			m.ExpectDeath()
			c.Stop(ctx, c.Node(target))

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(between):
			}

			c.l.printf("restarting %d after %s of downtime\n", target, between)
			c.Start(ctx, c.Node(target))
		}
		return nil
	}
}
