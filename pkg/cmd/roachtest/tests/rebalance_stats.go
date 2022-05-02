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

import "github.com/montanaflynn/stats"

var (
	qpsStat  = clusterStat{tag: "qps", query: "rebalancing_queriespersecond"}
	rqpsStat = clusterStat{tag: "rqps", query: "rebalancing_requestspersecond"}
	wpsStat  = clusterStat{tag: "wps", query: "rebalancing_writespersecond"}
	rpsStat  = clusterStat{tag: "rps", query: "rebalancing_readspersecond"}
	// NB: CPU is fractional, roachperf will only take integers on a detailed
	//     view. Scale by 100 to get % here. When aggregating on cpu, measure
	//     of distribution that are normalized are preferred, due to scale.
	cpuStat = clusterStat{tag: "cpu", query: "sys_cpu_combined_percent_normalized * 100"}
	// NB: These are recorded as counters. These are then rated, as we are
	//     interested in the progression at points in time.
	ioReadStat          = clusterStat{tag: "io_read", query: "rate(sys_host_disk_read_bytes[10m])"}
	ioWriteStat         = clusterStat{tag: "io_write", query: "rate(sys_host_disk_write_bytes[10m])"}
	netSendStat         = clusterStat{tag: "net_send", query: "rate(sys_host_net_send_bytes[10m])"}
	netRecvStat         = clusterStat{tag: "net_recv", query: "rate(sys_host_net_recv_bytes[10m])"}
	rangeCountStat      = clusterStat{tag: "range_count", query: "ranges"}
	underreplicatedStat = clusterStat{tag: "underreplicated", query: "ranges_underreplicated"}
	replicasStat        = clusterStat{tag: "replica_count", query: "replicas"}
	leaseTransferStat   = clusterStat{tag: "lease_transfers", query: "rebalancing_lease_transfers"}
	rangeRebalancesStat = clusterStat{tag: "range_rebalance", query: "rebalancing_range_rebalances"}
	rangeSplitStat      = clusterStat{tag: "range_splits", query: "range_splits"}

	underReplicatedSummary = []statSummaryQuery{
		{stat: underreplicatedStat, aggTag: "sum(underreplicated)", aggQuery: "sum(ranges_underreplicated)"},
	}
	rangeBalanceSummary = []statSummaryQuery{
		{stat: rangeCountStat, aggTag: "cv(ranges)", aggFn: distributionAggregate},
	}
	actionsSummary = []statSummaryQuery{
		{stat: rangeRebalancesStat, aggTag: "sum(rebalances)", aggQuery: "sum(rebalancing_range_rebalances)"},
		{stat: leaseTransferStat, aggTag: "sum(transfers)", aggQuery: "sum(rebalancing_lease_transfers)"},
		{stat: rangeSplitStat, aggTag: "sum(splits)", aggQuery: "sum(range_splits)"},
	}
	resourceBalanceSummary = []statSummaryQuery{
		// NB: cv is an abbreviation for coefficient of variation.
		{stat: cpuStat, aggTag: "cv(cpu)", aggFn: distributionAggregate},
		{stat: ioWriteStat, aggTag: "cv(io_write)", aggFn: distributionAggregate},
		{stat: ioReadStat, aggTag: "cv(io_read)", aggFn: distributionAggregate},
	}
	requestBalanceSummary = []statSummaryQuery{
		{stat: qpsStat, aggTag: "cv(qps)", aggFn: distributionAggregate},
		{stat: wpsStat, aggTag: "cv(wps)", aggFn: distributionAggregate},
		{stat: rpsStat, aggTag: "cv(rps)", aggFn: distributionAggregate},
	}
)

func joinSummaryQueries(queries ...[]statSummaryQuery) []statSummaryQuery {
	ret := make([]statSummaryQuery, 0, 1)
	for _, q := range queries {
		ret = append(ret, q...)
	}
	return ret
}

func distributionAggregate(vals map[tag][]float64) []float64 {
	timeseries := make([][]float64, 0, 1)
	for _, v := range vals {
		for i := range v {
			if i >= len(timeseries) {
				timeseries = append(timeseries, make([]float64, 0, 1))
			}
			timeseries[i] = append(timeseries[i], v[i])
		}
	}
	return scale(applyToSeries(timeseries, coefficientOfVariation), 100)
}

func applyToSeries(timeseries [][]float64, aggFn func(vals []float64) float64) []float64 {
	ret := make([]float64, len(timeseries))
	for i := range timeseries {
		ret[i] = aggFn(timeseries[i])
	}
	return ret
}

func coefficientOfVariation(vals []float64) float64 {
	stdev, _ := stats.StandardDeviationSample(vals)
	mean, _ := stats.Mean(vals)
	return stdev / mean
}

func scale(vals []float64, scale float64) []float64 {
	scaled := make([]float64, len(vals))
	for i := range vals {
		scaled[i] = vals[i] * scale
	}
	return scaled
}
