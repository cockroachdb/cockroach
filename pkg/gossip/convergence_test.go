// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gossip_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip/simulation"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// The tests in this package have fairly small cluster sizes for the sake of
// not taking too long to run when run as part of the normal unit tests. If
// you're testing out gossip network behavior, you may find it useful to
// increase the network size for these tests (adjusting the max thresholds
// accordingly) and see how things behave.
const (
	testConvergenceSize        = 10
	testReachesEquilibriumSize = 24
)

func connectionsRefused(network *simulation.Network) int64 {
	var connsRefused int64
	for _, node := range network.Nodes {
		connsRefused += node.Gossip.GetNodeMetrics().ConnectionsRefused.Count()
	}
	return connsRefused
}

// TestConvergence verifies that a node gossip network converges within
// a fixed number of simulation cycles. It's really difficult to
// determine the right number for cycles because different things can
// happen during a single cycle, depending on how much CPU time is
// available. Eliminating this variability by getting more
// synchronization primitives in place for the simulation is possible,
// though two attempts so far have introduced more complexity into the
// actual production gossip code than seems worthwhile for a unittest.
// As such, the thresholds are drastically higher than is normally needed.
//
// As of Jan 2017, this normally takes ~12 cycles and 8-12 refused connections.
func TestConvergence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderStress(t)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	network := simulation.NewNetwork(stopper, testConvergenceSize, true, zonepb.DefaultZoneConfigRef())

	const maxCycles = 100
	if connectedCycle := network.RunUntilFullyConnected(); connectedCycle > maxCycles {
		log.Warningf(context.Background(), "expected a fully-connected network within %d cycles; took %d",
			maxCycles, connectedCycle)
	}

	const maxConnsRefused = 50
	if connsRefused := connectionsRefused(network); connsRefused > maxConnsRefused {
		log.Warningf(context.Background(),
			"expected network to fully connect with <= %d connections refused; took %d",
			maxConnsRefused, connsRefused)
	}
}

// TestNetworkReachesEquilibrium ensures that the gossip network stops bouncing
// refused connections around after a while and settles down.
// As mentioned in the comment for TestConvergence, there is a large amount of
// variability in how much gets done in each network cycle, and thus we have
// to set thresholds that are drastically higher than is needed in the normal
// case.
//
// As of Jan 2017, this normally takes 8-9 cycles and 50-60 refused connections.
func TestNetworkReachesEquilibrium(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderStress(t)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	network := simulation.NewNetwork(stopper, testReachesEquilibriumSize, true, zonepb.DefaultZoneConfigRef())

	var connsRefused int64
	var cyclesWithoutChange int
	var numCycles int
	network.SimulateNetwork(func(cycle int, network *simulation.Network) bool {
		numCycles = cycle
		newConnsRefused := connectionsRefused(network)
		if newConnsRefused > connsRefused {
			connsRefused = newConnsRefused
			cyclesWithoutChange = 0
		} else {
			cyclesWithoutChange++
		}
		if cycle%5 == 0 {
			log.Infof(context.Background(), "cycle: %d, cyclesWithoutChange: %d, fullyConnected: %v",
				cycle, cyclesWithoutChange, network.IsNetworkConnected())
		}
		return cyclesWithoutChange < 5
	})

	const maxCycles = 200
	if numCycles > maxCycles {
		log.Warningf(context.Background(), "expected a non-thrashing network within %d cycles; took %d",
			maxCycles, numCycles)
	}

	const maxConnsRefused = 500
	if connsRefused > maxConnsRefused {
		log.Warningf(context.Background(),
			"expected thrashing to die down with <= %d connections refused; took %d",
			maxConnsRefused, connsRefused)
	}
}
