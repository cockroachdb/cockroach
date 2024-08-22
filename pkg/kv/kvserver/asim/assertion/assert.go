// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package assertion

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/history"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/montanaflynn/stats"
)

type ThresholdType int

const (
	ExactBound ThresholdType = iota
	UpperBound
	LowerBound
)

// String returns the string representation of ThresholdType.
func (tht ThresholdType) String() string {
	switch tht {
	case ExactBound:
		return "="
	case UpperBound:
		return "<"
	case LowerBound:
		return ">"
	default:
		panic("unknown threshold type")
	}
}

// Threshold is created by parsing CmdArgs array and is used for assertion to
// validate user-defined Threshold constraints.
type Threshold struct {
	// Value indicates the predefined Threshold value specified by arguments.
	Value float64
	// ThresholdType indicates the predefined Threshold bound type specified by
	// arguments.
	ThresholdType ThresholdType
}

// String returns the string representation of Threshold.
func (th Threshold) String() string {
	return fmt.Sprintf("(%v%.2f)", th.ThresholdType, th.Value)
}

// isViolated returns true if the Threshold constraint is violated and false
// otherwise. Note that if the provided actual value is NaN, the function
// returns false.
func (th Threshold) isViolated(actual float64) bool {
	switch th.ThresholdType {
	case UpperBound:
		return actual > th.Value
	case LowerBound:
		return actual < th.Value
	case ExactBound:
		return actual != th.Value
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
	Assert(context.Context, history.History) (holds bool, reason string)
	// String returns the string representation of the assertion.
	String() string
}

// SteadyStateAssertion implements the SimulationAssertion interface. The
// SteadyStateAssertion declares an assertion. A common use case is to specify
// an upper_bound for the type=steady threshold. With this configuration, the
// given Stat for each store must be no greater than Threshold % of the mean
// over the assertion Ticks. This assertion is useful for when a cluster should
// stop activity and converge after a period of initial activity. A natural
// example is asserting that rebalancing activity reaches a steady state, so
// there is not thrashing.
type SteadyStateAssertion struct {
	Ticks     int
	Stat      string
	Threshold Threshold
}

// Assert looks at a simulation run history and returns true if the declared
// Stat's minimum/mean and maximum/mean meets the Threshold constraint at each
// assertion tick. If violated, holds is returned as false along with the
// reason.
func (sa SteadyStateAssertion) Assert(
	ctx context.Context, h history.History,
) (holds bool, reason string) {
	m := h.Recorded
	ticks := len(m)
	if sa.Ticks > ticks {
		log.VInfof(ctx, 2,
			"The history to run assertions against (%d) is shorter than "+
				"the assertion duration (%d)", ticks, sa.Ticks)
		return true, ""
	}

	ts := metrics.MakeTS(m)
	statTs := ts[sa.Stat]

	// Set holds to be true initially, holds is set to false if the steady
	// state assertion doesn't hold on any store.
	holds = true
	buf := strings.Builder{}

	for i, storeStats := range statTs {
		trimmedStoreStats := storeStats[ticks-sa.Ticks-1:]
		mean, _ := stats.Mean(trimmedStoreStats)
		max, _ := stats.Max(trimmedStoreStats)
		min, _ := stats.Min(trimmedStoreStats)

		maxMean := math.Abs(max/mean - 1)
		minMean := math.Abs(min/mean - 1)

		if sa.Threshold.isViolated(maxMean) || sa.Threshold.isViolated(minMean) {
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
func (sa SteadyStateAssertion) String() string {
	return fmt.Sprintf("steady state stat=%s threshold=%v ticks=%d",
		sa.Stat, sa.Threshold, sa.Ticks)
}

// BalanceAssertion implements the SimulationAssertion interface. The
// BalanceAssertion declares an assertion. A common use case is to specify an
// upper_bound for the type=balance threshold. With this configuration, the
// given Stat across all Stores must be no greater than the Threshold for all
// assertion Ticks. This assertion is useful when a Stat is being controlled,
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
//	  constraints       = [{+zone=a1: 1}, {+zone=a2: 1},
//	                       {+zone=a3: 1}]
//	  lease_preferences = [[+zone=a3]]
//
//	localities
//	  s1-s2 zone = a1
//	  s3-s4 zone = a2
//	  s5-s6 zone = a3
//
// Then in this configuration, the lease for each range should be on either
// s5 or s6 and there should be a replica in each zone. Asserting on the
// balance of the cluster doesn't make sense logically, the configuration
// requires leaseholders are on s5,s6 so naturally they should have greater
// load.
type BalanceAssertion struct {
	Ticks     int
	Stat      string
	Threshold Threshold
}

// Assert looks at a simulation run history and returns true if the declared
// Stat's maximum/mean (over all stores) in the cluster meets the Threshold
// constraint at each assertion tick. If violated, holds is returned as false
// along with the reason.
func (ba BalanceAssertion) Assert(
	ctx context.Context, h history.History,
) (holds bool, reason string) {
	m := h.Recorded
	ticks := len(m)
	if ba.Ticks > ticks {
		log.VInfof(ctx, 2,
			"The history to run assertions against (%d) is shorter than "+
				"the assertion duration (%d)", ticks, ba.Ticks)
		return true, ""
	}

	ts := metrics.MakeTS(m)
	statTs := metrics.Transpose(ts[ba.Stat])

	// Set holds to be true initially, holds is set to false if the steady
	// state assertion doesn't hold on any store.
	holds = true
	buf := strings.Builder{}

	// Check that the assertion holds for the last ba.Ticks; from the most
	// recent tick to recent tick - ba.Ticks.
	for tick := 0; tick < ba.Ticks && tick < ticks; tick++ {
		tickStats := statTs[ticks-tick-1]
		mean, _ := stats.Mean(tickStats)
		max, _ := stats.Max(tickStats)
		maxMeanRatio := max / mean

		log.VInfof(ctx, 2,
			"Balance assertion: stat=%s, max/mean=%.2f, threshold=%+v raw=%v",
			ba.Stat, maxMeanRatio, ba.Threshold, tickStats)
		if ba.Threshold.isViolated(maxMeanRatio) {
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
func (ba BalanceAssertion) String() string {
	return fmt.Sprintf(
		"balance stat=%s threshold=%v ticks=%d",
		ba.Stat, ba.Threshold, ba.Ticks)
}

// StoreStatAssertion implements the SimulationAssertion interface. The
// StoreStatAssertion declares an assertion. A common use case is to specify an
// exact_bound for the type=stat threshold. With this configuration, the given
// Stat for each store in stores must be == Threshold over the assertion Ticks.
type StoreStatAssertion struct {
	Ticks     int
	Stat      string
	Stores    []int
	Threshold Threshold
}

// Assert looks at a simulation run history and returns true if the
// assertion holds and false if not. When the assertion does not hold, the
// reason is also returned.
func (sa StoreStatAssertion) Assert(
	ctx context.Context, h history.History,
) (holds bool, reason string) {
	m := h.Recorded
	ticks := len(m)
	if sa.Ticks > ticks {
		log.VInfof(ctx, 2,
			"The history to run assertions against (%d) is shorter than "+
				"the assertion duration (%d)", ticks, sa.Ticks)
		return true, ""
	}

	ts := metrics.MakeTS(m)
	statTs := ts[sa.Stat]
	holds = true
	// Set holds to be true initially, holds is set to false if the steady
	// state assertion doesn't hold on any store.
	holds = true
	buf := strings.Builder{}

	for _, store := range sa.Stores {
		trimmedStoreStats := statTs[store-1][ticks-sa.Ticks-1:]
		for _, stat := range trimmedStoreStats {
			if sa.Threshold.isViolated(stat) {
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
func (sa StoreStatAssertion) String() string {
	return fmt.Sprintf("stat=%s value=%v ticks=%d",
		sa.Stat, sa.Threshold, sa.Ticks)
}

type ConformanceAssertion struct {
	BetterFormat              bool
	Underreplicated           int
	Overreplicated            int
	ViolatingConstraints      int
	Unavailable               int
	ViolatingLeasePreferences int
	LessPreferredLeases       int
}

// ConformanceAssertionSentinel declares a sentinel value which when any of the
// ConformanceAssertion parameters are set to, we ignore the conformance
// reports value for that type of conformance.
const ConformanceAssertionSentinel = -1

func leasePreferenceReport(
	ctx context.Context, h history.History,
) (violating, lessPreferred []roachpb.ConformanceReportedRange) {
	ranges := h.S.Ranges()
	for _, r := range ranges {
		if lhStore, ok := h.S.LeaseholderStore(r.RangeID()); ok {
			storeDescriptor := lhStore.Descriptor()
			spanConf := r.SpanConfig()
			status := kvserver.CheckStoreAgainstLeasePreferences(
				storeDescriptor.StoreID,
				storeDescriptor.Attrs,
				storeDescriptor.Node.Attrs,
				storeDescriptor.Node.Locality,
				spanConf.LeasePreferences,
			)
			switch status {
			case kvserver.LeasePreferencesOK:
			case kvserver.LeasePreferencesLessPreferred:
				lessPreferred = append(lessPreferred, roachpb.ConformanceReportedRange{
					RangeDescriptor: *r.Descriptor(),
					Config:          *spanConf,
				})
			case kvserver.LeasePreferencesViolating:
				violating = append(violating, roachpb.ConformanceReportedRange{
					RangeDescriptor: *r.Descriptor(),
					Config:          *spanConf,
				})
			default:
				panic("unknown lease preference status type")
			}
		}
	}

	return
}

// Assert looks at a simulation run history and returns true if the
// assertion holds and false if not. When the assertion does not hold, the
// reason is also returned.
func (ca ConformanceAssertion) Assert(
	ctx context.Context, h history.History,
) (holds bool, reason string) {
	replicaReport := h.S.Report()
	leaseViolatingPrefs, leaseLessPrefs := leasePreferenceReport(ctx, h)
	buf := strings.Builder{}
	holds = true

	unavailable, under, over, violatingConstraints := len(replicaReport.Unavailable), len(replicaReport.UnderReplicated), len(replicaReport.OverReplicated), len(replicaReport.ViolatingConstraints)
	violatingLeases, lessPrefLeases := len(leaseViolatingPrefs), len(leaseLessPrefs)

	maybeInitHolds := func() {
		if holds {
			holds = false
			fmt.Fprintf(&buf, "  %s\n", ca)
			fmt.Fprintf(&buf, "  actual unavailable=%d under=%d, over=%d violating=%d lease-violating=%d lease-less-preferred=%d\n",
				unavailable, under, over, violatingConstraints, violatingLeases, lessPrefLeases,
			)
		}
	}

	if ca.Unavailable != ConformanceAssertionSentinel &&
		ca.Unavailable != unavailable {
		maybeInitHolds()
		if ca.BetterFormat {
			buf.WriteString(PrettyPrintSpanConfigConformanceList(h.S.NodeLocalityMap(),
				"unavailable", replicaReport.Unavailable))
		} else {
			buf.WriteString(PrintSpanConfigConformanceList(h.S.NodeLocalityMap(),
				"unavailable", replicaReport.Unavailable))
		}
	}
	if ca.Underreplicated != ConformanceAssertionSentinel &&
		ca.Underreplicated != under {
		maybeInitHolds()
		if ca.BetterFormat {
			buf.WriteString(PrettyPrintSpanConfigConformanceList(h.S.NodeLocalityMap(),
				"under replicated", replicaReport.UnderReplicated))
		} else {
			buf.WriteString(PrintSpanConfigConformanceList(h.S.NodeLocalityMap(),
				"under replicated", replicaReport.UnderReplicated))
		}
	}
	if ca.Overreplicated != ConformanceAssertionSentinel &&
		ca.Overreplicated != over {
		maybeInitHolds()
		if ca.BetterFormat {
			buf.WriteString(PrettyPrintSpanConfigConformanceList(h.S.NodeLocalityMap(),
				"over replicated", replicaReport.OverReplicated))
		} else {
			buf.WriteString(PrintSpanConfigConformanceList(h.S.NodeLocalityMap(),
				"over replicated", replicaReport.OverReplicated))
		}
	}
	if ca.ViolatingConstraints != ConformanceAssertionSentinel &&
		ca.ViolatingConstraints != violatingConstraints {
		maybeInitHolds()
		if ca.BetterFormat {
			buf.WriteString(PrettyPrintSpanConfigConformanceList(h.S.NodeLocalityMap(),
				"violating constraints", replicaReport.ViolatingConstraints))
		} else {
			buf.WriteString(PrintSpanConfigConformanceList(h.S.NodeLocalityMap(),
				"violating constraints", replicaReport.ViolatingConstraints))
		}
	}
	if ca.ViolatingLeasePreferences != ConformanceAssertionSentinel &&
		ca.ViolatingLeasePreferences != violatingLeases {
		maybeInitHolds()
		if ca.BetterFormat {
			buf.WriteString(PrettyPrintSpanConfigConformanceList(h.S.NodeLocalityMap(),
				"violating lease preferences", leaseViolatingPrefs))
		} else {
			buf.WriteString(PrintSpanConfigConformanceList(h.S.NodeLocalityMap(),
				"violating lease preferences", leaseViolatingPrefs))
		}
	}
	if ca.LessPreferredLeases != ConformanceAssertionSentinel &&
		ca.LessPreferredLeases != lessPrefLeases {
		maybeInitHolds()
		if ca.BetterFormat {
			buf.WriteString(PrettyPrintSpanConfigConformanceList(h.S.NodeLocalityMap(),
				"less preferred preferences", leaseLessPrefs))
		} else {
			buf.WriteString(PrintSpanConfigConformanceList(h.S.NodeLocalityMap(),
				"less preferred preferences", leaseLessPrefs))
		}
	}

	return holds, buf.String()
}

// String returns the string representation of the assertion.
func (ca ConformanceAssertion) String() string {
	buf := strings.Builder{}
	fmt.Fprintf(&buf, "conformance ")
	if ca.Unavailable != ConformanceAssertionSentinel {
		fmt.Fprintf(&buf, "unavailable=%d ", ca.Unavailable)
	}
	if ca.Underreplicated != ConformanceAssertionSentinel {
		fmt.Fprintf(&buf, "under=%d ", ca.Underreplicated)
	}
	if ca.Overreplicated != ConformanceAssertionSentinel {
		fmt.Fprintf(&buf, "over=%d ", ca.Overreplicated)
	}
	if ca.ViolatingConstraints != ConformanceAssertionSentinel {
		fmt.Fprintf(&buf, "violating=%d ", ca.ViolatingConstraints)
	}
	if ca.ViolatingLeasePreferences != ConformanceAssertionSentinel {
		fmt.Fprintf(&buf, "lease-violating=%d ", ca.ViolatingLeasePreferences)
	}
	if ca.LessPreferredLeases != ConformanceAssertionSentinel {
		fmt.Fprintf(&buf, "lease-less-preferred=%d ", ca.LessPreferredLeases)
	}
	return buf.String()
}

func formatType(t roachpb.ReplicaType) string {
	switch t {
	case roachpb.VOTER_FULL:
		return "voter"
	case roachpb.NON_VOTER:
		return "non-voter"
	case roachpb.LEARNER:
		return "learner"
	case roachpb.VOTER_INCOMING:
		return "voter-incoming"
	case roachpb.VOTER_OUTGOING:
		return "voter-outgoing"
	case roachpb.VOTER_DEMOTING_LEARNER:
		return "voter-demoting-learner"
	case roachpb.VOTER_DEMOTING_NON_VOTER:
		return "voter-demoting-non-voter"
	default:
		panic("unknown replica type")
	}
}

func sumRangeInfo(
	nodes map[state.NodeID]roachpb.Locality, replicas []roachpb.ReplicaDescriptor,
) string {
	if len(replicas) <= 0 {
		return "<no replicas>"
	}

	var buf strings.Builder
	tiers := make(map[string]map[roachpb.ReplicaType]int)
	for _, rep := range replicas {
		val, ok := nodes[state.NodeID(rep.NodeID)]
		if !ok {
			panic(fmt.Sprintf("node %d not found", rep.NodeID))
		}
		for _, tier := range val.Tiers {
			if _, ok := tiers[tier.Value]; !ok {
				tiers[tier.Value] = make(map[roachpb.ReplicaType]int)
			}
			tiers[tier.Value][rep.Type]++
		}
	}

	keys := make([]string, 0, len(tiers))
	for k := range tiers {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := tiers[k]
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%s: ", k))
		keys := make([]int, 0, len(v))
		for k := range v {
			keys = append(keys, int(k))
		}
		sort.Ints(keys)
		for i, t := range keys {
			if i > 0 {
				buf.WriteString(" ")
			}
			count := v[roachpb.ReplicaType(t)]
			if count != 0 {
				if count > 1 {
					buf.WriteString(fmt.Sprintf("%d %ss", count, formatType(roachpb.ReplicaType(t))))
				} else {
					buf.WriteString(fmt.Sprintf("%d %s", count, formatType(roachpb.ReplicaType(t))))
				}
			}
		}
	}
	return buf.String()
}

func prettyPrintRangeDesc(
	nodes map[state.NodeID]roachpb.Locality, r roachpb.RangeDescriptor,
) string {
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("r%d:", r.RangeID))
	buf.WriteString(" [")
	// todo(wenyi): reivist replicaset helper functions
	minLen := min(3, len(r.Replicas().Descriptors()))
	buf.WriteString(sumRangeInfo(nodes, r.Replicas().Descriptors()[:minLen]))
	buf.WriteString("]")
	return buf.String()
}

func printRangeDesc(nodes map[state.NodeID]roachpb.Locality, r roachpb.RangeDescriptor) string {
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("r%d:", r.RangeID))
	buf.WriteString(r.RSpan().String())
	buf.WriteString(" [")
	// todo(wenyi): reivist replicaset helper functions
	minLen := min(3, len(r.Replicas().Descriptors()))
	buf.WriteString(sumRangeInfo(nodes, r.Replicas().Descriptors()[:minLen]))
	buf.WriteString("]")
	return buf.String()
}

func PrettyPrintSpanConfigConformanceList(
	nodes map[state.NodeID]roachpb.Locality, tag string, ranges []roachpb.ConformanceReportedRange,
) string {
	var buf strings.Builder
	for i, r := range ranges {
		if i == 3 {
			return buf.String() + fmt.Sprintf("... and %d more", len(ranges)-3)
		}
		if i == 0 {
			buf.WriteString(fmt.Sprintf("%s:\n", tag))
		}
		buf.WriteString(fmt.Sprintf("  %s", prettyPrintRangeDesc(nodes, r.RangeDescriptor)))
		if i != len(ranges)-1 {
			buf.WriteString("\n")
		}
	}
	return buf.String()
}

func PrintSpanConfigConformanceList(
	nodes map[state.NodeID]roachpb.Locality, tag string, ranges []roachpb.ConformanceReportedRange,
) string {
	var buf strings.Builder
	for i, r := range ranges {
		if i == 0 {
			buf.WriteString(fmt.Sprintf("%s:\n", tag))
		}
		buf.WriteString(fmt.Sprintf("  %s applying %s", printRangeDesc(nodes, r.RangeDescriptor),
			spanconfigtestutils.PrintSpanConfigDiffedAgainstDefaults(r.Config)))
		if i != len(ranges)-1 {
			buf.WriteString("\n")
		}
	}
	return buf.String()
}
