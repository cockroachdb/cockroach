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

	// changefeedCommitLatency is the changefeed.commit_latency prometheus
	// histogram (nanoseconds). It measures the gap between an event's MVCC
	// timestamp and downstream sink acknowledgement.
	changefeedCommitLatency = clusterstats.ClusterStat{LabelName: "node", Query: "changefeed_commit_latency_bucket"}
	// changefeedCommitLatencyP50Agg is the cluster-wide p50 commit latency in ms.
	changefeedCommitLatencyP50Agg = clusterstats.AggQuery{
		Stat:  changefeedCommitLatency,
		Query: "histogram_quantile(0.50, sum by(le) (rate(changefeed_commit_latency_bucket[30s]))) / (1000*1000)",
		Tag:   "Commit Latency P50 (ms)",
	}
	// changefeedCommitLatencyP99Agg is the cluster-wide p99 commit latency in ms.
	changefeedCommitLatencyP99Agg = clusterstats.AggQuery{
		Stat:  changefeedCommitLatency,
		Query: "histogram_quantile(0.99, sum by(le) (rate(changefeed_commit_latency_bucket[30s]))) / (1000*1000)",
		Tag:   "Commit Latency P99 (ms)",
	}

	// changefeedEmittedMessages is the changefeed.emitted_messages prometheus counter.
	changefeedEmittedMessages = clusterstats.ClusterStat{LabelName: "node", Query: "changefeed_emitted_messages"}
	// changefeedEmittedMessagesRateAgg is the cluster-wide emitted rows per second.
	changefeedEmittedMessagesRateAgg = clusterstats.AggQuery{
		Stat:  changefeedEmittedMessages,
		Query: "sum(rate(changefeed_emitted_messages[30s]))",
		Tag:   "Emitted Rows/sec",
	}
)
