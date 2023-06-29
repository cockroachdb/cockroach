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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/montanaflynn/stats"
)

type thresholdType int

const (
	exactBound thresholdType = iota
	upperBound
	lowerBound
)

// String returns the string representation of thresholdType.
func (tht thresholdType) String() string {
	switch tht {
	case exactBound:
		return "="
	case upperBound:
		return "<"
	case lowerBound:
		return ">"
	default:
		panic("unknown threshold type")
	}
}

// threshold is created by parsing CmdArgs array and is used for assertion to
// validate user-defined threshold constraints.
type threshold struct {
	// value indicates the predefined threshold value specified by arguments.
	value float64
	// thresholdType indicates the predefined threshold bound type specified by
	// arguments.
	thresholdType thresholdType
}

// String returns the string representation of threshold.
func (th threshold) String() string {
	return fmt.Sprintf("(%v%.2f)", th.thresholdType, th.value)
}

// isViolated returns true if the threshold constraint is violated and false
// otherwise. Note that if the provided actual value is NaN, the function
// returns false.
func (th threshold) isViolated(actual float64) bool {
	switch th.thresholdType {
	case upperBound:
		return actual > th.value
	case lowerBound:
		return actual < th.value
	case exactBound:
		return actual != th.value
	default:
		panic("unknown threshold type")
	}
}

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
// steadyStateAssertion declares an assertion. A common use case is to specify
// an upper_bound for the type=steady threshold. With this configuration, the
// given stat for each store must be no greater than threshold % of the mean
// over the assertion ticks. This assertion is useful for when a cluster should
// stop activity and converge after a period of initial activity. A natural
// example is asserting that rebalancing activity reaches a steady state, so
// there is not thrashing.
type steadyStateAssertion struct {
	ticks     int
	stat      string
	threshold threshold
}

// Assert looks at a simulation run history and returns true if the declared
// stat's minimum/mean and maximum/mean meets the threshold constraint at each
// assertion tick. If violated, holds is returned as false along with the
// reason.
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

		if sa.threshold.isViolated(maxMean) || sa.threshold.isViolated(minMean) {
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
	return fmt.Sprintf("steady state stat=%s threshold=%v ticks=%d",
		sa.stat, sa.threshold, sa.ticks)
}

// balanceAssertion implements the SimulationAssertion interface. The
// balanceAssertion declares an assertion. A common use case is to specify an
// upper_bound for the type=balance threshold. With this configuration, the
// given stat across all stores must be no greater than the threshold for all
// assertion ticks. This assertion is useful when a stat is being controlled,
// such as QPS and a correct rebalancing algorithm should balance the stat.
//
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
	threshold threshold
}

// Assert looks at a simulation run history and returns true if the declared
// stat's maximum/mean (over all stores) in the cluster meets the threshold
// constraint at each assertion tick. If violated, holds is returned as false
// along with the reason.
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
			"Balance assertion: stat=%s, max/mean=%.2f, threshold=%+v raw=%v",
			ba.stat, maxMeanRatio, ba.threshold, tickStats)
		if ba.threshold.isViolated(maxMeanRatio) {
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
		"balance stat=%s threshold=%v ticks=%d",
		ba.stat, ba.threshold, ba.ticks)
}

// storeStatAssertion implements the SimulationAssertion interface. The
// storeStatAssertion declares an assertion. A common use case is to specify an
// exact_bound for the type=stat threshold. With this configuration, the given
// stat for each store in stores must be == threshold over the assertion ticks.
type storeStatAssertion struct {
	ticks     int
	stat      string
	stores    []int
	threshold threshold
}

// Assert looks at a simulation run history and returns true if the
// assertion holds and false if not. When the assertion does not hold, the
// reason is also returned.
func (sa storeStatAssertion) Assert(
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
	holds = true
	// Set holds to be true initially, holds is set to false if the steady
	// state assertion doesn't hold on any store.
	holds = true
	buf := strings.Builder{}

	for _, store := range sa.stores {
		trimmedStoreStats := statTs[store-1][ticks-sa.ticks-1:]
		for _, stat := range trimmedStoreStats {
			if sa.threshold.isViolated(stat) {
				if holds {
					holds = false
					fmt.Fprintf(&buf, "  %s\n", sa)
				}
				fmt.Fprintf(&buf,
					"\tstore=%d stat=%.2f\n",
					store, stat)
			}
		}
	}
	return holds, buf.String()
}

// String returns the string representation of the assertion.
func (sa storeStatAssertion) String() string {
	return fmt.Sprintf("stat=%s value=%v ticks=%d",
		sa.stat, sa.threshold, sa.ticks)
}

type conformanceAssertion struct {
	underreplicated int
	overreplicated  int
	violating       int
	unavailable     int
}

// conformanceAssertionSentinel declares a sentinel value which when any of the
// conformanceAssertion parameters are set to, we ignore the conformance
// reports value for that type of conformance.
const conformanceAssertionSentinel = -1

// Assert looks at a simulation run history and returns true if the
// assertion holds and false if not. When the assertion does not hold, the
// reason is also returned.
func (ca conformanceAssertion) Assert(
	ctx context.Context, h asim.History,
) (holds bool, reason string) {
	report := h.S.Report()
	buf := strings.Builder{}
	holds = true

	unavailable, under, over, violating := len(report.Unavailable), len(report.UnderReplicated), len(report.OverReplicated), len(report.ViolatingConstraints)

	maybeInitHolds := func() {
		if holds {
			holds = false
			fmt.Fprintf(&buf, "  %s\n", ca)
			fmt.Fprintf(&buf, "  actual unavailable=%d under=%d, over=%d violating=%d\n",
				unavailable, under, over, violating,
			)
		}
	}

	if ca.unavailable != conformanceAssertionSentinel &&
		ca.unavailable != unavailable {
		maybeInitHolds()
		buf.WriteString(PrintSpanConfigConformanceList(
			"unavailable", report.Unavailable))
	}
	if ca.underreplicated != conformanceAssertionSentinel &&
		ca.underreplicated != under {
		maybeInitHolds()
		buf.WriteString(PrintSpanConfigConformanceList(
			"under replicated", report.UnderReplicated))
	}
	if ca.overreplicated != conformanceAssertionSentinel &&
		ca.overreplicated != over {
		maybeInitHolds()
		buf.WriteString(PrintSpanConfigConformanceList(
			"over replicated", report.OverReplicated))
	}
	if ca.violating != conformanceAssertionSentinel &&
		ca.violating != violating {
		maybeInitHolds()
		buf.WriteString(PrintSpanConfigConformanceList(
			"violating constraints", report.ViolatingConstraints))
	}

	return holds, buf.String()
}

// String returns the string representation of the assertion.
func (ca conformanceAssertion) String() string {
	buf := strings.Builder{}
	fmt.Fprintf(&buf, "conformance ")
	if ca.unavailable != conformanceAssertionSentinel {
		fmt.Fprintf(&buf, "unavailable=%d ", ca.unavailable)
	}
	if ca.underreplicated != conformanceAssertionSentinel {
		fmt.Fprintf(&buf, "under=%d ", ca.underreplicated)
	}
	if ca.overreplicated != conformanceAssertionSentinel {
		fmt.Fprintf(&buf, "over=%d ", ca.overreplicated)
	}
	if ca.violating != conformanceAssertionSentinel {
		fmt.Fprintf(&buf, "violating=%d ", ca.violating)
	}
	return buf.String()
}

func printRangeDesc(r roachpb.RangeDescriptor) string {
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("r%d:", r.RangeID))
	buf.WriteString(r.RSpan().String())
	buf.WriteString(" [")
	if allReplicas := r.Replicas().Descriptors(); len(allReplicas) > 0 {
		for i, rep := range allReplicas {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(rep.String())
		}
	} else {
		buf.WriteString("<no replicas>")
	}
	buf.WriteString("]")
	return buf.String()
}

func PrintSpanConfigConformanceList(tag string, ranges []roachpb.ConformanceReportedRange) string {
	var buf strings.Builder
	for i, r := range ranges {
		if i == 0 {
			buf.WriteString(fmt.Sprintf("%s:\n", tag))
		}
		buf.WriteString(fmt.Sprintf("  %s applying %s\n", printRangeDesc(r.RangeDescriptor),
			spanconfigtestutils.PrintSpanConfigDiffedAgainstDefaults(r.Config)))
	}
	return buf.String()
}
