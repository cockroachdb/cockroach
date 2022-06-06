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
	qpsStat  = clusterstats.ClusterStat{LabelName: "instance", Query: "rebalancing_queriespersecond"}
	rqpsStat = clusterstats.ClusterStat{LabelName: "instance", Query: "rebalancing_requestspersecond"}
	wpsStat  = clusterstats.ClusterStat{LabelName: "instance", Query: "rebalancing_writespersecond"}
	rpsStat  = clusterstats.ClusterStat{LabelName: "instance", Query: "rebalancing_readspersecond"}
	// NB: CPU is fractional, roachperf will only take integers on a detailed
	//     view. Scale by 100 to get % here. When aggregating on cpu, measure
	//     of distribution that are normalized are preferred, due to scale.
	cpuStat = clusterstats.ClusterStat{LabelName: "instance", Query: "sys_cpu_combined_percent_normalized * 100"}
	// NB: These are recorded as counters. These are then rated, as we are
	//     interested in the progression at points in time.
	ioReadStat          = clusterstats.ClusterStat{LabelName: "instance", Query: "rate(sys_host_disk_read_bytes[10m])"}
	ioWriteStat         = clusterstats.ClusterStat{LabelName: "instance", Query: "rate(sys_host_disk_write_bytes[10m])"}
	netSendStat         = clusterstats.ClusterStat{LabelName: "instance", Query: "rate(sys_host_net_send_bytes[10m])"}
	netRecvStat         = clusterstats.ClusterStat{LabelName: "instance", Query: "rate(sys_host_net_recv_bytes[10m])"}
	rangeCountStat      = clusterstats.ClusterStat{LabelName: "instance", Query: "ranges"}
	underreplicatedStat = clusterstats.ClusterStat{LabelName: "instance", Query: "ranges_underreplicated"}
	replicasStat        = clusterstats.ClusterStat{LabelName: "instance", Query: "replicas"}
	leaseTransferStat   = clusterstats.ClusterStat{LabelName: "instance", Query: "rebalancing_lease_transfers"}
	rangeRebalancesStat = clusterstats.ClusterStat{LabelName: "instance", Query: "rebalancing_range_rebalances"}
	rangeSplitStat      = clusterstats.ClusterStat{LabelName: "instance", Query: "range_splits"}

	underReplicatedSummary = []clusterstats.AggQuery{
		{Stat: underreplicatedStat, Query: "sum(ranges_underreplicated)"},
	}
	rangeBalanceSummary = []clusterstats.AggQuery{
		{Stat: rangeCountStat, AggFn: distributionAggregate},
	}
	actionsSummary = []clusterstats.AggQuery{
		{Stat: rangeRebalancesStat, Query: "sum(rebalancing_range_rebalances)"},
		{Stat: leaseTransferStat, Query: "sum(rebalancing_lease_transfers)"},
		{Stat: rangeSplitStat, Query: "sum(range_splits)"},
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
