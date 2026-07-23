// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"

var (
	// sqlServiceLatency is the sql_service_latency_bucket prometheus metric.
	sqlServiceLatency = clusterstats.ClusterStat{LabelName: "node", Query: "sql_service_latency_bucket"}
	// sqlServiceLatencyAgg is the P99 latency of foreground SQL traffic across all nodes measured in ms.
	sqlServiceLatencyAgg = clusterstats.AggQuery{
		Stat:  sqlServiceLatency,
		Query: "histogram_quantile(0.99, sum by(le) (rate(sql_service_latency_bucket[2m]))) / (1000*1000)",
		Tag:   "P99 Foreground Latency (ms)",
	}

	// changefeedThroughput is the changefeed_emitted_bytes prometheus metric.
	changefeedThroughput = clusterstats.ClusterStat{LabelName: "node", Query: "changefeed_emitted_bytes"}
	// changefeedThroughputAgg is the total rate of bytes being emitted by a cluster measured in MB/s.
	changefeedThroughputAgg = clusterstats.AggQuery{
		Stat:  changefeedThroughput,
		Query: "sum(rate(changefeed_emitted_bytes[1m]) / (1000 * 1000))",
		Tag:   "Throughput (MBps)",
	}

	// cpuUsage is the sys_cpu_combined_percent_normalized prometheus metric per mode.
	cpuUsage = clusterstats.ClusterStat{LabelName: "node", Query: "sys_cpu_combined_percent_normalized"}
	// cpuUsageAgg is the average CPU usage across all nodes.
	cpuUsageAgg = clusterstats.AggQuery{
		Stat:  cpuUsage,
		Query: "avg(sys_cpu_combined_percent_normalized) * 100",
		Tag:   "CPU Utilization (%)",
	}
)
