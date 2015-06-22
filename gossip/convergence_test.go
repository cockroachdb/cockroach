// Copyright 2014 The Cockroach Authors.
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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/gossip/simulation"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// verifyConvergence verifies that info from each node is visible from
// every node in the network within numCycles cycles of the gossip protocol.
// NOTE: This test is non-deterministic because it involves multiple goroutines
// that may not all be able to keep up if the given interval is too small.
// As a rule of thumb, increase the interval until the same number of cycles are used
// for both race and non-race tests; this indicates that no goroutines are being
// left behind by CPU limits.
// TODO(spencer): figure out a more deterministic setup, advancing the clock
// manually and counting cycles accurately instead of relying on real-time sleeps.
func verifyConvergence(numNodes, maxCycles int, interval time.Duration, t *testing.T) {
	network := simulation.NewNetwork(numNodes, "unix", interval)

	if connectedCycle := network.RunUntilFullyConnected(); connectedCycle > maxCycles {
		t.Errorf("expected a fully-connected network within %d cycles; took %d",
			maxCycles, connectedCycle)
	}
	network.Stop()
}

// TestConvergence verifies a 10 node gossip network converges within
// 8 cycles.
func TestConvergence(t *testing.T) {
	defer leaktest.AfterTest(t)
	// 100 milliseconds to accommodate race tests on slower hardware.
	verifyConvergence(10, 8, 100*time.Millisecond, t)
}
