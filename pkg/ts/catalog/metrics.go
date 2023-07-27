// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import "sort"

var internalTSMetricsNames []string

func init() {
	internalTSMetricsNames = allInternalTSMetricsNames()
}

// AllInternalTimeseriesMetricNames returns a slice that returns all of the
// metric names used by CockroachDB's internal timeseries database. This is
// *not* the list of names as it appears on prometheus. In particular, since
// the internal TS DB does not support histograms, it instead records timeseries
// for a number of quantiles.
//
// For technical reasons, the returned set will contain metrics names that do
// not exist. This is because a running server is required to determine the
// proper prefix for each metric (per-node or per-store); we don't have one
// here, so we return both possible prefixes.
func AllInternalTimeseriesMetricNames() []string {
	return internalTSMetricsNames
}

// The histogramMetricsNames variable below was originally seeded using the invocation
// below. It's kept up to date via `server.TestChartCatalogMetrics`.
var _ = `
./cockroach demo --empty -e \
    "select name from crdb_internal.node_metrics where name like '%-p50'" | \
    sed -E 's/^(.*)-p50$/"\1": {},/'
`

var histogramMetricsNames = map[string]struct{}{
	"sql.txn.latency.internal":                  {},
	"sql.conn.latency":                          {},
	"sql.mem.sql.session.max":                   {},
	"sql.stats.flush.duration":                  {},
	"changefeed.checkpoint_hist_nanos":          {},
	"admission.wait_durations.sql-sql-response": {},
	"admission.wait_durations.sql-kv-response":  {},
	"sql.exec.latency":                          {},
	"sql.stats.mem.max.internal":                {},
	"admission.wait_durations.sql-leaf-start":   {},
	"sql.disk.distsql.max":                      {},
	"txn.restarts":                              {},
	"sql.stats.flush.duration.internal":         {},
	"sql.distsql.exec.latency":                  {},
	"sql.mem.internal.txn.max":                  {},
	"changefeed.emit_hist_nanos":                {},
	"changefeed.flush_hist_nanos":               {},
	"sql.service.latency":                       {},
	"round-trip-latency":                        {},
	"admission.wait_durations.kv":               {},
	"sql.mem.distsql.max":                       {},
	"kv.prober.write.latency":                   {},
	"exec.latency":                              {},
	"admission.wait_durations.sql-root-start":   {},
	"sql.mem.bulk.max":                          {},
	"sql.distsql.flows.queue_wait":              {},
	"sql.txn.latency":                           {},
	"sql.mem.root.max":                          {},
	"admission.wait_durations.kv-stores":        {},
	"sql.stats.mem.max":                         {},
	"sql.distsql.service.latency.internal":      {},
	"sql.stats.reported.mem.max.internal":       {},
	"sql.stats.reported.mem.max":                {},
	"sql.exec.latency.internal":                 {},
	"sql.mem.internal.session.max":              {},
	"sql.distsql.exec.latency.internal":         {},
	"kv.prober.read.latency":                    {},
	"sql.distsql.service.latency":               {},
	"sql.service.latency.internal":              {},
	"sql.mem.sql.txn.max":                       {},
	"liveness.heartbeatlatency":                 {},
	"txn.durations":                             {},
	"raft.process.handleready.latency":          {},
	"raft.process.commandcommit.latency":        {},
	"raft.process.logcommit.latency":            {},
	"raft.quota_pool.percent_used":              {},
	"raft.scheduler.latency":                    {},
	"txnwaitqueue.pusher.wait_time":             {},
	"txnwaitqueue.query.wait_time":              {},
	"raft.process.applycommitted.latency":       {},
	"sql.stats.txn_stats_collection.duration":   {},
	"rebalancing.l0_sublevels_histogram":        {},
	"changefeed.admit_latency":                  {},
	"changefeed.message_size_hist":              {},
	"changefeed.commit_latency":                 {},
	"changefeed.sink_batch_hist_nanos":          {},
	"changefeed.parallel_io_queue_nanos":        {},
	"replication.admit_latency":                 {},
	"replication.commit_latency":                {},
	"replication.flush_hist_nanos":              {},
	"kv.replica_read_batch_evaluate.latency":    {},
	"kv.replica_write_batch_evaluate.latency":   {},
	"leases.requests.latency":                   {},
}

func allInternalTSMetricsNames() []string {
	m := map[string]struct{}{}
	for _, section := range charts {
		for _, chart := range section.Charts {
			for _, metric := range chart.Metrics {
				// Jump through hoops to create the correct internal timeseries metrics names.
				// See:
				// https://github.com/cockroachdb/cockroach/issues/64373
				_, isHist := histogramMetricsNames[metric]
				if !isHist {
					m[metric] = struct{}{}
					continue
				}
				for _, p := range []string{
					"p50",
					"p75",
					"p90",
					"p99",
					"p99.9",
					"p99.99",
					"p99.999",
				} {
					m[metric+"-"+p] = struct{}{}
				}
			}
		}
	}
	// TODO(tbg): these prefixes come from pkg/server/status/recorder.go.
	// Unfortunately, the chart catalog does not tell us which ones are per-node
	// or per-store, so we simply tag both (one of them will be empty).
	names := make([]string, 0, 2*len(m))
	for name := range m {
		names = append(names, "cr.store."+name, "cr.node."+name)
	}
	sort.Strings(names)
	return names
}
