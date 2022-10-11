// Copyright 2022 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/montanaflynn/stats"
)

var (
	replicasStat = clusterstats.ClusterStat{LabelName: "store", Query: "replicas"}
	leasesStat   = clusterstats.ClusterStat{LabelName: "store", Query: "replicas_leaseholders"}
	qpsStat      = clusterstats.ClusterStat{LabelName: "store", Query: "rebalancing_queriespersecond"}
	wpsStat      = clusterstats.ClusterStat{LabelName: "store", Query: "rebalancing_writespersecond"}
	wbpsStat     = clusterstats.ClusterStat{LabelName: "store", Query: "rebalancing_writebytespersecond"}
	rpsStat      = clusterstats.ClusterStat{LabelName: "store", Query: "rebalancing_readspersecond"}
	rbpsStat     = clusterstats.ClusterStat{LabelName: "store", Query: "rebalancing_readbytespersecond"}
	// NB: CPU is fractional, roachperf will only take integers on a detailed
	//     view. Scale by 100 to get % here. When aggregating on cpu, measure
	//     of distribution that are normalized are preferred, due to scale.
	cpuStat = clusterstats.ClusterStat{LabelName: "instance", Query: "sys_cpu_combined_percent_normalized * 100"}
	// NB: These are recorded as counters. These are then rated, as we are
	//     interested in the progression at points in time.
	ioReadStat  = clusterstats.ClusterStat{LabelName: "instance", Query: "rate(sys_host_disk_read_bytes[5m])"}
	ioWriteStat = clusterstats.ClusterStat{LabelName: "instance", Query: "rate(sys_host_disk_write_bytes[5m])"}

	rangeRebalancesStat = clusterstats.ClusterStat{LabelName: "store", Query: "rebalancing_range_rebalances"}
	leaseTransferStat   = clusterstats.ClusterStat{LabelName: "store", Query: "rebalancing_lease_transfers"}
	rangeSplitStat      = clusterstats.ClusterStat{LabelName: "store", Query: "range_splits"}

	rebalanceSnapshotSentStat = clusterstats.ClusterStat{LabelName: "store", Query: "rate(range_snapshots_rebalancing_sent_bytes[5m]) / (1024 * 1024)"}
	rebalanceSnapshotRcvdStat = clusterstats.ClusterStat{LabelName: "store", Query: "rate(range_snapshots_rebalancing_rcvd_bytes[5m]) / (1024 * 1024)"}
	recoverySnapshotSentStat  = clusterstats.ClusterStat{LabelName: "store", Query: "rate(range_snapshots_recovery_sent_bytes[5m]) / (1024 * 1024)"}
	recoverySnapshotRcvdStat  = clusterstats.ClusterStat{LabelName: "store", Query: "rate(range_snapshots_recovery_rcvd_bytes[5m]) / (1024 * 1024)"}

	actionsSummary = []clusterstats.AggQuery{
		{Stat: rangeRebalancesStat, Query: "sum(rebalancing_range_rebalances)"},
		{Stat: leaseTransferStat, Query: "sum(rebalancing_lease_transfers)"},
		{Stat: rangeSplitStat, Query: "sum(range_splits)"},
	}
	snapshotCostSummary = []clusterstats.AggQuery{
		{Stat: rebalanceSnapshotSentStat, Query: "sum(rate(range_snapshots_rebalancing_sent_bytes[5m])) / (1024 * 1024)"},
		{Stat: rebalanceSnapshotRcvdStat, Query: "sum(rate(range_snapshots_rebalancing_rcvd_bytes[5m])) / (1024 * 1024)"},
		{Stat: recoverySnapshotSentStat, Query: "sum(rate(range_snapshots_recovery_sent_bytes[5m])) / (1024 * 1024)"},
		{Stat: recoverySnapshotRcvdStat, Query: "sum(rate(range_snapshots_recovery_rcvd_bytes[5m])) / (1024 * 1024)"},
	}
	resourceBalanceSummary = []clusterstats.AggQuery{
		// NB: cv is an abbreviation for coefficient of variation.
		{Stat: cpuStat, AggFn: distributionAggregate},
		{Stat: ioWriteStat, AggFn: distributionAggregate},
		{Stat: ioReadStat, AggFn: distributionAggregate},
	}
	requestBalanceSummary = []clusterstats.AggQuery{
		{Stat: qpsStat, AggFn: distributionAggregate},
		{Stat: wpsStat, AggFn: distributionAggregate},
		{Stat: rpsStat, AggFn: distributionAggregate},
	}
)

func joinSummaryQueries(queries ...[]clusterstats.AggQuery) []clusterstats.AggQuery {
	ret := make([]clusterstats.AggQuery, 0, 1)
	for _, q := range queries {
		ret = append(ret, q...)
	}
	return ret
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

func scale(vals []float64, scale float64) []float64 {
	scaled := make([]float64, len(vals))
	for i := range vals {
		scaled[i] = vals[i] * scale
	}
	return scaled
}
