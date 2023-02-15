// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/montanaflynn/stats"
)

// SimulationAssertion provides methods to assert on properties of a cluster
// simulation over time.
type SimulationAssertion interface {
	// Assert looks at a simulation run history and returns true if the
	// assertion holds and false if not. When the assertion does not hold, the
	// reason is also returned.
	Assert(context.Context, asim.History) (holds bool, reason string)
	// String returns the string representation of the assertion.
	String() string
}

// steadyStateAssertion implements the SimulationAssertion interface. The
// steadyStateAssertion declares an assertion where the given stat for each
// store must be no greater than threshold % of the mean over the assertion
// ticks. This assertion is useful for when a cluster should stop activity and
// converge after a period of initial activity. A natural example is asserting
// that rebalancing activity reaches a steady state, so there is not thrashing.
type steadyStateAssertion struct {
	ticks     int
	stat      string
	threshold float64
}

// Assert looks at a simulation run history and returns true if the declared
// stat's minimum/mean and maximum/mean over the assertion duration are not
// greater than the declared threshold. If over the threshold, holds is
// returned as false and the reason given.
func (sa steadyStateAssertion) Assert(
	ctx context.Context, h asim.History,
) (holds bool, reason string) {
	m := h.Recorded
	ticks := len(m)
	if sa.ticks > ticks {
		log.VInfof(ctx, 2,
			"The history to run assertions against (%d) is shorter than "+
				"the assertion duration (%d)", ticks, sa.ticks)
		return true, ""
	}

	ts := metrics.MakeTS(m)
	statTs := ts[sa.stat]

	// Set holds to be true initially, holds is set to false if the steady
	// state assertion doesn't hold on any store.
	holds = true
	buf := strings.Builder{}

	for i, storeStats := range statTs {
		trimmedStoreStats := storeStats[ticks-sa.ticks-1:]
		mean, _ := stats.Mean(trimmedStoreStats)
		max, _ := stats.Max(trimmedStoreStats)
		min, _ := stats.Min(trimmedStoreStats)

		maxMean := math.Abs(max/mean - 1)
		minMean := math.Abs(min/mean - 1)

		if maxMean > sa.threshold || minMean > sa.threshold {
			if holds {
				fmt.Fprintf(&buf, "  %s\n", sa)
				holds = false
			}
			fmt.Fprintf(&buf,
				"\tstore=%d min/mean=%.2f max/mean=%.2f\n",
				i+1, minMean, maxMean)
		}
	}
	return holds, buf.String()
}

// String returns the string representation of the assertion.
func (sa steadyStateAssertion) String() string {
	return fmt.Sprintf("steady state stat=%s threshold=%.2f ticks=%d",
		sa.stat, sa.threshold, sa.ticks)
}

// balanceAssertion implements the SimulationAssertion interface. The
// balanceAssertion declares an assertion where the max/mean of a given stat
// across all all stores for each tick must be no greater than the threshold
// given, for all assertion ticks. This assertion is useful when a stat is
// being controlled, such as QPS and a correct rebalancing algorithm should
// balance the stat.
// TODO(kvoli): Rationalize this assertion for multi-locality clusters with
// zone configurations. This balance assertion uses the mean and maximum across
// all stores in the cluster. In multi-locality clusters, it is possible for
// balance to be a property that only holds within regions or not at all with
// targeted zone configs. e.g.
//
//	zone config (all ranges)
//	  num_replicas      = 3
//	  constraints       = [{+zone=au-east-2a: 1}, {+zone=au-east-2b: 1},
//	                       {+zone=au-east-2c: 1}]
//	  lease_preferences = [[+zone=au-east-2c]]
//
//	localities
//	  s1-s2 zone = au-east-2a
//	  s3-s4 zone = au-east-2b
//	  s5-s6 zone = au-east-2c
//
// Then in this configuration, the lease for each range should be on either
// s5 or s6 and there should be a replica in each zone. Asserting on the
// balance of the cluster doesn't make sense logically, the configuration
// requires leaseholders are on s5,s6 so naturally they should have greater
// load.
type balanceAssertion struct {
	ticks     int
	stat      string
	threshold float64
}

// Assert looks at a simulation run history and returns true if the declared
// stat's maximum/mean (over all stores) in the cluster is not greater than the
// threshold at each tick. If this holds for all assertion ticks, holds is
// true, otherwise false and the reason given.
func (ba balanceAssertion) Assert(ctx context.Context, h asim.History) (holds bool, reason string) {
	m := h.Recorded
	ticks := len(m)
	if ba.ticks > ticks {
		log.VInfof(ctx, 2,
			"The history to run assertions against (%d) is shorter than "+
				"the assertion duration (%d)", ticks, ba.ticks)
		return true, ""
	}

	ts := metrics.MakeTS(m)
	statTs := metrics.Transpose(ts[ba.stat])

	// Set holds to be true initially, holds is set to false if the steady
	// state assertion doesn't hold on any store.
	holds = true
	buf := strings.Builder{}

	// Check that the assertion holds for the last ba.ticks; from the most
	// recent tick to recent tick - ba.ticks.
	for tick := 0; tick < ba.ticks && tick < ticks; tick++ {
		tickStats := statTs[ticks-tick-1]
		mean, _ := stats.Mean(tickStats)
		max, _ := stats.Max(tickStats)
		maxMeanRatio := max / mean

		log.VInfof(ctx, 2,
			"Balance assertion: stat=%s, max/mean=%.2f, threshold=%.2f raw=%v",
			ba.stat, maxMeanRatio, ba.threshold, tickStats)
		if maxMeanRatio > ba.threshold {
			if holds {
				fmt.Fprintf(&buf, "  %s\n", ba)
				holds = false
			}
			fmt.Fprintf(&buf, "\tmax/mean=%.2f tick=%d\n", maxMeanRatio, tick)
		}
	}
	return holds, buf.String()
}

// String returns the string representation of the assertion.
func (ba balanceAssertion) String() string {
	return fmt.Sprintf(
		"balance stat=%s threshold=%.2f ticks=%d",
		ba.stat, ba.threshold, ba.ticks)
}
