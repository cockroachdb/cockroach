// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/montanaflynn/stats"
)

var (
	qpsStat = clusterstats.ClusterStat{LabelName: "node", Query: "rebalancing_queriespersecond"}
	wpsStat = clusterstats.ClusterStat{LabelName: "node", Query: "rebalancing_writespersecond"}
	rpsStat = clusterstats.ClusterStat{LabelName: "node", Query: "rebalancing_readspersecond"}
	// NB: CPU is fractional, roachperf will only take integers on a detailed
	//     view. Scale by 100 to get % here. When aggregating on cpu, measure
	//     of distribution that are normalized are preferred, due to scale.
	cpuStat = clusterstats.ClusterStat{LabelName: "node", Query: "avg_over_time(sys_cpu_combined_percent_normalized[5m]) * 100"}
	// NB: These are recorded as counters. These are then rated, as we are
	//     interested in the progression at points in time.
	ioWriteStat = clusterstats.ClusterStat{LabelName: "node", Query: divQuery("rate(sys_host_disk_write_bytes[5m])", 1<<20)}

	rangeRebalancesStat = clusterstats.ClusterStat{LabelName: "node", Query: "rebalancing_range_rebalances"}
	leaseTransferStat   = clusterstats.ClusterStat{LabelName: "node", Query: "rebalancing_lease_transfers"}
	rangeSplitStat      = clusterstats.ClusterStat{LabelName: "node", Query: "range_splits"}

	rebalanceSnapshotSentStat = clusterstats.ClusterStat{
		LabelName: "node", Query: divQuery("rate(range_snapshots_rebalancing_sent_bytes[5m])", 1<<20)}

	admissionAvgWaitSecs = clusterstats.ClusterStat{
		LabelName: "node",
		Query:     divQuery("rate(admission_wait_durations_kv_sum[1m])", int(time.Second.Nanoseconds())),
	}
	admissionControlIOOverload = clusterstats.ClusterStat{
		LabelName: "node",
		Query:     "admission_io_overload * 100",
	}

	actionsSummary = []clusterstats.AggQuery{
		{Stat: rangeRebalancesStat, Query: applyAggQuery("sum", rangeRebalancesStat.Query)},
		{Stat: leaseTransferStat, Query: applyAggQuery("sum", leaseTransferStat.Query)},
		{Stat: rangeSplitStat, Query: applyAggQuery("sum", rangeSplitStat.Query)},
	}
	rebalanceCostSummary = []clusterstats.AggQuery{
		// NB: Use the cumulative rebalancing snapshot bytes rather than
		// the instantaneous like the stat query.
		{
			Stat:  rebalanceSnapshotSentStat,
			Query: applyAggQuery("sum", divQuery("range_snapshots_rebalancing_sent_bytes", 1<<20)),
		},
		{Stat: leaseTransferStat, Query: applyAggQuery("sum", leaseTransferStat.Query)},
	}
	resourceBalanceSummary = []clusterstats.AggQuery{
		// NB: cv is an abbreviation for coefficient of variation.
		{Stat: cpuStat, AggFn: distributionAggregate},
		{Stat: ioWriteStat, AggFn: distributionAggregate},
	}
	requestBalanceSummary = []clusterstats.AggQuery{
		{Stat: qpsStat, AggFn: distributionAggregate},
		{Stat: wpsStat, AggFn: distributionAggregate},
		{Stat: rpsStat, AggFn: distributionAggregate},
	}
	resourceMinMaxSummary = []clusterstats.AggQuery{
		{Stat: cpuStat, Query: minMaxAggQuery(divQuery(cpuStat.Query, 100 /* 100% */))},
		{Stat: ioWriteStat, Query: minMaxAggQuery(divQuery(ioWriteStat.Query, 400 /* 400mb */))},
	}
	overloadMaxSummary = []clusterstats.AggQuery{
		{Stat: admissionAvgWaitSecs, Query: applyAggQuery("max", admissionAvgWaitSecs.Query)},
		{Stat: admissionControlIOOverload, Query: applyAggQuery("max", admissionControlIOOverload.Query)},
	}
)

func joinSummaryQueries(queries ...[]clusterstats.AggQuery) []clusterstats.AggQuery {
	ret := make([]clusterstats.AggQuery, 0, 1)
	for _, q := range queries {
		ret = append(ret, q...)
	}
	return ret
}

func applyAggQuery(agg string, query string) string {
	return fmt.Sprintf("%s(%s)", agg, query)
}

func divQuery(query string, val int) string {
	return fmt.Sprintf("%s / %d", query, val)
}

// minMaxAggQuery returns the maximum within a series vector minus the minimum
// of each series vector, scaled by 100. It is intended to be used for queries
// which return a relative utilization (%) between 0-1.
func minMaxAggQuery(query string) string {
	return fmt.Sprintf("(%s - %s) * 100",
		applyAggQuery("max", query),
		applyAggQuery("min", query),
	)
}

func distributionAggregate(query string, series [][]float64) (string, []float64) {
	return fmt.Sprintf("cv(%s)", query), scale(applyToSeries(series, coefficientOfVariation), 100)
}

func applyToSeries(timeseries [][]float64, aggFn func(vals []float64) float64) []float64 {
	ret := make([]float64, len(timeseries))
	for i := range timeseries {
		ret[i] = aggFn(timeseries[i])
	}
	return ret
}

// coefficientOfVariation returns the standard deviation over the mean. No mean
// or standard deviation can be computed when there are no samples, in which
// case we return 0. When the sample mean is 0, the coefficient of variation
// cannot be calculated, we return 0.
func coefficientOfVariation(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	stdev, _ := stats.StandardDeviationSample(vals)
	mean, _ := stats.Mean(vals)
	if mean == 0 {
		return 0
	}
	return stdev / mean
}

// arithmeticMean is an average measure where avg = sum / count. This fn is
// included to return 0 instead of NaN like the stats pkg used underneath.
func arithmeticMean(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	mean, _ := stats.Mean(vals)
	return mean
}

func roundFraction(val, n, decimals float64) float64 {
	percent := (val / n)
	rounding := math.Pow(10, decimals)
	return math.Round(percent*rounding) / rounding
}

func scale(vals []float64, scale float64) []float64 {
	scaled := make([]float64, len(vals))
	for i := range vals {
		scaled[i] = vals[i] * scale
	}
	return scaled
}
