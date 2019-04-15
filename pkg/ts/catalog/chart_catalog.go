package catalog

import (
	fmt "fmt"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/pkg/errors"

	prometheusgo "github.com/prometheus/client_model/go"
)

// chartDescription describes an individual chart.
// Only Title, Organization, and Metrics must be set; other values have useful
// defaults if the included metrics are similar.
type chartDescription struct {
	// Title of the chart.
	Title string
	// Organization identifies the sections in which to place the chart.
	// (Sections are created ad hoc as ChartSections whenever they're identified in
	// a chartDescription.)
	// Inner array is Level 0 (req), Level 1 (req), Level 2 (opt).
	// Outer array lets you use the same chart in multiple places.
	Organization [][]string
	// Metrics to include in the chart using their util.metric.Metadata.name;
	// these values are used to generate ChartMetrics.
	// NOTE: All Metrics must be of the same prometheusgo.MetricType.
	Metrics []string
	// Units in which the chart is viewed, e.g. BYTES for storage.
	// Does not need to be set if all Metrics have the same Unit value.
	Units AxisUnits
	// Axis label for the chart's y-axis.
	// Does not need to be set if all Metrics have the same Measurement value.
	AxisLabel string
	// The downsampler function the chart uses.
	Downsampler DescribeAggregator
	// The aggregator function for the chart's downsampled values.
	Aggregator DescribeAggregator
	// The derivative function the chart uses (e.g. NONE).
	Rate DescribeDerivative
	// Whether or not the chart should be converted into percentiles.
	// True only for Histograms. Unsupported by other metric types.
	Percentiles bool
}

// chartDefault provides default values to simplify adding charts to the catalog.
type chartDefault struct {
	Downsampler DescribeAggregator
	Aggregator  DescribeAggregator
	Rate        DescribeDerivative
	Percentiles bool
}

// chartDefaultsPerMetricType defines default values for the chart's
// Downsampler, Aggregator, Rate, and Percentiles based on the metric's type.
var chartDefaultsPerMetricType = map[prometheusgo.MetricType]chartDefault{
	prometheusgo.MetricType_COUNTER: chartDefault{
		Downsampler: DescribeAggregator_AVG,
		Aggregator:  DescribeAggregator_SUM,
		Rate:        DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
		Percentiles: false,
	},
	prometheusgo.MetricType_GAUGE: chartDefault{
		Downsampler: DescribeAggregator_AVG,
		Aggregator:  DescribeAggregator_AVG,
		Rate:        DescribeDerivative_NONE,
		Percentiles: false,
	},
	prometheusgo.MetricType_HISTOGRAM: chartDefault{
		Downsampler: DescribeAggregator_AVG,
		Aggregator:  DescribeAggregator_AVG,
		Rate:        DescribeDerivative_NONE,
		Percentiles: true,
	},
}

// These consts represent the catalog's Level 0 organization options.
const (
	Process            = `Process`
	SQLLayer           = `SQL Layer`
	KVTransactionLayer = `KV Transaction Layer`
	DistributionLayer  = `Distribution Layer`
	ReplicationLayer   = `Replication Layer`
	StorageLayer       = `Storage Layer`
	Timeseries         = `Timeseries`
)

// charts defines all of the charts in the catalog.
// NOTE: This is an small section of the complete catalog, meant only to
// demonstrate how charts will be identified. The rest of this list will
// added in a following commit.
var charts = []chartDescription{
	{
		Title:        "Abandoned",
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Metrics:      []string{"txn.abandons"},
	},
	{
		Title:        "Aborts",
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Metrics:      []string{"txn.aborts"},
	},
	{
		Title:        "Add Replica Count",
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Metrics:      []string{"queue.replicate.addreplica"},
	},
	{
		Title:        "Ingestions",
		Organization: [][]string{{StorageLayer, "RocksDB", "SSTables"}},
		Metrics: []string{
			"addsstable.copies",
			"addsstable.applications",
			"addsstable.proposals",
		},
	},
	{
		Title:        "Auto Retries",
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Metrics:      []string{"txn.autoretries"},
	},
	{
		Title:        "Count",
		Organization: [][]string{{KVTransactionLayer, "Transactions", "Savepoints"}},
		Metrics: []string{
			"sql.savepoint.count",
			"sql.savepoint.count.internal",
		},
		AxisLabel: "SQL Statements",
	},
	{
		Title:        "Restarts",
		Organization: [][]string{{KVTransactionLayer, "Transactions", "Savepoints"}},
		Metrics: []string{
			"sql.restart_savepoint.count",
			"sql.restart_savepoint.release.count",
			"sql.restart_savepoint.rollback.count",
		},
	},
	{
		Title:        "Restarts (Internal)",
		Organization: [][]string{{KVTransactionLayer, "Transactions", "Savepoints"}},
		Metrics: []string{
			"sql.restart_savepoint.count.internal",
			"sql.restart_savepoint.release.count.internal",
			"sql.restart_savepoint.rollback.count.internal",
		},
	},
	{
		Title:        "Overview",
		Organization: [][]string{{KVTransactionLayer, "Transactions", "Intents"}},
		Metrics: []string{
			"intents.abort-attempts",
			"intents.poison-attempts",
			"intents.resolve-attempts",
		},
	},
	{
		Title:        "Intent Resolver",
		Organization: [][]string{{KVTransactionLayer, "Transactions", "Intents"}},
		Metrics: []string{
			"intentresolver.async.throttled",
		},
	},
	{
		Title:        "Waiting",
		Organization: [][]string{{KVTransactionLayer, "Transactions", "TxnWaitQueue"}},
		Metrics: []string{
			"txnwaitqueue.pushee.waiting",
			"txnwaitqueue.pusher.waiting",
			"txnwaitqueue.query.waiting",
		},
		AxisLabel: "Actors",
	},
	{
		Title:        "Deadlocks",
		Organization: [][]string{{KVTransactionLayer, "Transactions", "TxnWaitQueue"}},
		Metrics: []string{
			"txnwaitqueue.deadlocks_total",
		},
	},
	{
		Title:        "Wait Time",
		Organization: [][]string{{KVTransactionLayer, "Transactions", "TxnWaitQueue"}},
		Metrics: []string{
			"txnwaitqueue.pusher.wait_time",
			"txnwaitqueue.query.wait_time",
		},
		AxisLabel: "Wait Time",
	},
	{
		Title:        "Slow Pusher",
		Organization: [][]string{{KVTransactionLayer, "Transactions", "TxnWaitQueue"}},
		Metrics: []string{
			"txnwaitqueue.pusher.slow",
		},
	},
	{
		Title:        "Capacity",
		Organization: [][]string{{StorageLayer, "Storage", "Overview"}},
		Metrics: []string{
			"capacity.available",
			"capacity",
			"capacity.reserved",
			"capacity.used",
		},
	},
	{
		Title:        "Keys/Sec Avg.",
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Metrics:      []string{"rebalancing.writespersecond"},
	},
	{
		Title: "Writes Waiting on Range Split",
		Organization: [][]string{
			{KVTransactionLayer, "Requests", "Backpressure"},
			{ReplicationLayer, "Requests", "Backpressure"},
		},
		Downsampler: DescribeAggregator_MAX,
		Percentiles: false,
		Metrics:     []string{"requests.backpressure.split"},
	},
	{
		Title:        "Backpressued Writes Waiting on Split",
		Organization: [][]string{{ReplicationLayer, "Ranges"}},
		Metrics:      []string{"requests.backpressure.split"},
	},
	{
		Title:        "Count",
		Organization: [][]string{{KVTransactionLayer, "Follower Reads"}},
		Metrics:      []string{"follower_reads.success_count"},
	},
	{
		Title:        "Closed Timestamp",
		Organization: [][]string{{KVTransactionLayer, "Follower Reads"}},
		Metrics:      []string{"kv.closed_timestamp.max_behind_nanos"},
	},
	{
		Title:        "Batches",
		Organization: [][]string{{DistributionLayer, "DistSender"}},
		Metrics: []string{
			"distsender.batches",
			"distsender.batches.partial",
			"distsender.batches.async.sent",
			"distsender.batches.async.throttled",
		},
		AxisLabel: "Batches",
	},
	{
		Title:        "Timestamp",
		Organization: [][]string{{Process, "Build Info"}},
		Downsampler:  DescribeAggregator_MAX,
		Percentiles:  false,
		Metrics:      []string{"build.timestamp"},
	},
	{
		Title:        "Overview",
		Organization: [][]string{{StorageLayer, "Storage", "Compactor"}},
		Metrics: []string{
			"compactor.suggestionbytes.compacted",
			"compactor.suggestionbytes.skipped",
		},
	},
	{
		Title:        "Queued",
		Organization: [][]string{{StorageLayer, "Storage", "Compactor"}},
		Metrics: []string{
			"compactor.suggestionbytes.queued",
		},
	},
	{
		Title:        "Byte I/O",
		Organization: [][]string{{SQLLayer, "SQL"}},
		Metrics: []string{
			"sql.bytesin",
			"sql.bytesout",
		},
	},
	{
		Title:        "Bytes",
		Organization: [][]string{{DistributionLayer, "Gossip"}},
		Metrics: []string{
			"gossip.bytes.received",
			"gossip.bytes.sent",
		},
	},
	{
		Title:        "CA Expiration",
		Organization: [][]string{{Process, "Certificates"}},
		Downsampler:  DescribeAggregator_MAX,
		Percentiles:  false,
		Metrics:      []string{"security.certificate.expiration.ca"},
	},
	{
		Title:        "Memory",
		Organization: [][]string{{Process, "Server", "cgo"}},
		Metrics: []string{
			"sys.cgo.allocbytes",
			"sys.cgo.totalbytes",
		},
	},
	{
		Title:        "Calls",
		Organization: [][]string{{Process, "Server", "cgo"}},
		Metrics:      []string{"sys.cgocalls"},
	},
	{
		Title: "Offsets",
		Organization: [][]string{
			{KVTransactionLayer, "Clocks"},
			{Process, "Clocks"},
		},
		Metrics: []string{
			"clock-offset.meannanos",
			"clock-offset.stddevnanos",
		},
	},
	{
		Title:        "Commits",
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Metrics: []string{
			"txn.commits",
			"txn.commits1PC",
		},
	},
	{
		Title:        "Time",
		Organization: [][]string{{StorageLayer, "Storage", "Compactor"}},
		Metrics:      []string{"compactor.compactingnanos"},
	},
	{
		Title:        "Success",
		Organization: [][]string{{StorageLayer, "Storage", "Compactor"}},
		Metrics: []string{
			"compactor.compactions.failure",
			"compactor.compactions.success",
		},
	},
	{
		Title:        "Active Connections",
		Organization: [][]string{{DistributionLayer, "Gossip"}},
		Downsampler:  DescribeAggregator_MAX,
		Aggregator:   DescribeAggregator_MAX,
		Metrics: []string{
			"gossip.connections.incoming",
			"gossip.connections.outgoing",
		},
	},
	{
		Title:        "Refused Connections",
		Organization: [][]string{{DistributionLayer, "Gossip"}},
		Downsampler:  DescribeAggregator_MAX,
		Aggregator:   DescribeAggregator_MAX,
		Metrics:      []string{"gossip.connections.refused"},
	},
	{
		Title:        "Connections",
		Organization: [][]string{{SQLLayer, "SQL"}},
		Metrics:      []string{"sql.conns"},
	},
	{
		Title:        "Successes",
		Organization: [][]string{{ReplicationLayer, "Consistency Checker Queue"}},
		Metrics: []string{
			"queue.consistency.process.failure",
			"queue.consistency.process.success",
		},
	},
	{
		Title:        "Pending",
		Organization: [][]string{{ReplicationLayer, "Consistency Checker Queue"}},
		Metrics:      []string{"queue.consistency.pending"},
	},
	{
		Title:        "Time Spent",
		Organization: [][]string{{ReplicationLayer, "Consistency Checker Queue"}},
		Metrics:      []string{"queue.consistency.processingnanos"},
	},
	{
		Title:        "Time",
		Organization: [][]string{{Process, "CPU"}},
		Metrics: []string{
			"sys.cpu.sys.ns",
			"sys.cpu.user.ns",
		},
	},
	{
		Title:        "Percentage",
		Organization: [][]string{{Process, "CPU"}},
		Metrics: []string{
			"sys.cpu.sys.percent",
			"sys.cpu.user.percent",
			"sys.cpu.combined.percent-normalized",
		},
	},
	{
		Title:        "Count",
		Organization: [][]string{{SQLLayer, "Optimizer"}},
		Metrics: []string{
			"sql.optimizer.count",
			"sql.optimizer.count.internal",
		},
		AxisLabel: "SQL Statements",
	},
	{
		Title:        "Fallback",
		Organization: [][]string{{SQLLayer, "Optimizer"}},
		Metrics: []string{
			"sql.optimizer.fallback.count",
			"sql.optimizer.fallback.count.internal",
		},
		AxisLabel: "Fallbacks",
	},
	{
		Title:        "Plan Cache",
		Organization: [][]string{{SQLLayer, "Optimizer"}},
		Metrics: []string{
			"sql.optimizer.plan_cache.hits",
			"sql.optimizer.plan_cache.hits.internal",
			"sql.optimizer.plan_cache.misses",
			"sql.optimizer.plan_cache.misses.internal",
		},
		AxisLabel: "Plane Cache Accesses",
	},
	{
		Title:        "Current Memory Usage",
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Metrics:      []string{"sql.mem.distsql.current"},
	},
	{
		Title:        "Current",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Admin"}},
		Metrics:      []string{"sql.mem.admin.current"},
	},
	{
		Title:        "Current",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Metrics:      []string{"sql.mem.internal.current"},
	},
	{
		Title:        "Current",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Metrics:      []string{"sql.mem.conns.current"},
	},
	{
		Title:        "Current",
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL"}},
		Metrics:      []string{"sql.mem.sql.current"},
	},
	{
		Title:        "Max",
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL"}},
		Metrics:      []string{"sql.mem.sql.max"},
	},
	{
		Title:        "Current",
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL Session"}},
		Metrics:      []string{"sql.mem.sql.session.current"},
	},
	{
		Title:        "Max",
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL Session"}},
		Metrics:      []string{"sql.mem.sql.session.max"},
	},
	{
		Title:        "Current",
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL Txn"}},
		Metrics:      []string{"sql.mem.sql.txn.current"},
	},
	{
		Title:        "Max",
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL Txn"}},
		Metrics:      []string{"sql.mem.sql.txn.max"},
	},
	{
		Title:        "Current",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Metrics:      []string{"sql.mem.internal.current"},
	},
	{
		Title:        "Current",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Metrics:      []string{"sql.mem.conns.current"},
	},
	{
		Title:        "DDL Count",
		Organization: [][]string{{SQLLayer, "SQL"}},
		Metrics: []string{
			"sql.ddl.count",
			"sql.query.count",
			"sql.ddl.count.internal",
		},
		AxisLabel: "SQL Statements",
	},
	{
		Title:        "DML Mix",
		Organization: [][]string{{SQLLayer, "SQL"}},
		Metrics: []string{
			"sql.delete.count",
			"sql.insert.count",
			"sql.misc.count",
			"sql.query.count",
			"sql.select.count",
			"sql.update.count",
			"sql.failure.count",
		},
	},
	{
		Title:        "DML Mix (Internal)",
		Organization: [][]string{{SQLLayer, "SQL"}},
		Metrics: []string{
			"sql.delete.count.internal",
			"sql.insert.count.internal",
			"sql.misc.count.internal",
			"sql.query.count.internal",
			"sql.select.count.internal",
			"sql.update.count.internal",
			"sql.failure.count.internal",
		},
	},
	{
		Title:        "Exec Latency",
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Metrics: []string{
			"sql.distsql.exec.latency",
			"sql.distsql.exec.latency.internal",
		},
		AxisLabel: "Latency",
	},
	{
		Title:        "DML Mix",
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Metrics: []string{
			"sql.distsql.select.count",
			"sql.distsql.select.count.internal",
		},
		AxisLabel: "SQL Statements",
	},
	{
		Title:        "Service Latency",
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Metrics: []string{
			"sql.distsql.service.latency",
			"sql.distsql.service.latency.internal",
		},
		AxisLabel: "Latency",
	},
	{
		Title:        "Durations",
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Metrics:      []string{"txn.durations"},
	},
	{
		Title:        "Epoch Increment Count",
		Organization: [][]string{{ReplicationLayer, "Node Liveness"}},
		Metrics:      []string{"liveness.epochincrements"},
	},
	{
		Title:        "Success",
		Organization: [][]string{{KVTransactionLayer, "Requests", "Overview"}},
		Downsampler:  DescribeAggregator_MAX,
		Rate:         DescribeDerivative_DERIVATIVE,
		Percentiles:  false,
		Metrics: []string{
			"exec.error",
			"exec.success",
		},
	},
	{
		Title:        "File Descriptors (FD)",
		Organization: [][]string{{Process, "Server", "Overview"}},
		Metrics: []string{
			"sys.fd.open",
			"sys.fd.softlimit",
		},
	},
	{
		Title: "Time",
		Organization: [][]string{
			{Process, "Server", "Disk"},
			{StorageLayer, "Host Disk"},
		},
		Metrics: []string{
			"sys.host.disk.io.time",
			"sys.host.disk.weightedio.time",
			"sys.host.disk.read.time",
			"sys.host.disk.write.time",
		},
	},
	{
		Title: "Operations Count",
		Organization: [][]string{
			{Process, "Server", "Disk"},
			{StorageLayer, "Host Disk"},
		},
		Metrics: []string{
			"sys.host.disk.read.count",
			"sys.host.disk.write.count",
		},
	},
	{
		Title: "Operations Size",
		Organization: [][]string{
			{Process, "Server", "Disk"},
			{StorageLayer, "Host Disk"},
		},
		Metrics: []string{
			"sys.host.disk.read.bytes",
			"sys.host.disk.write.bytes",
		},
	},
	{
		Title: "IOPS in Progress",
		Organization: [][]string{
			{Process, "Server", "Disk"},
			{StorageLayer, "Host Disk"},
		},
		Metrics: []string{
			"sys.host.disk.iopsinprogress",
		},
	},
	{
		Title:        "Active",
		Organization: [][]string{{SQLLayer, "DistSQL", "Flows"}},
		Metrics:      []string{"sql.distsql.flows.active"},
	},
	{
		Title:        "Total",
		Organization: [][]string{{SQLLayer, "DistSQL", "Flows"}},
		Metrics:      []string{"sql.distsql.flows.total"},
	},
	{
		Title:        "Queued",
		Organization: [][]string{{SQLLayer, "DistSQL", "Flows"}},
		Metrics:      []string{"sql.distsql.flows.queued"},
	},
	{
		Title:        "Queue Wait",
		Organization: [][]string{{SQLLayer, "DistSQL", "Flows"}},
		Metrics:      []string{"sql.distsql.flows.queue_wait"},
	},
	{
		Title:        "AbortSpan",
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Metrics: []string{
			"queue.gc.info.abortspanconsidered",
			"queue.gc.info.abortspangcnum",
			"queue.gc.info.abortspanscanned",
		},
	},
	{
		Title:        "Cumultative Age of Non-Live Data",
		Organization: [][]string{{KVTransactionLayer, "Storage"}},
		Metrics:      []string{"gcbytesage"},
	},
	{
		Title:        "Cumultative Age of Non-Live Data",
		Organization: [][]string{{StorageLayer, "Storage", "KV"}},
		Metrics:      []string{"gcbytesage"},
	},
	{
		Title:        "Total GC Runs",
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Overview"}},
		Metrics:      []string{"sys.gc.count"},
	},
	{
		Title:        "Old Intents",
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Metrics:      []string{"queue.gc.info.intentsconsidered"},
	},
	{
		Title:        "Distinct Txns",
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Metrics:      []string{"queue.gc.info.intenttxns"},
	},
	{
		Title:        "Keys with GC'able Data",
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Metrics:      []string{"queue.gc.info.numkeysaffected"},
	},
	{
		Title:        "Total GC Pause (NS)",
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Overview"}},
		Metrics:      []string{"sys.gc.pause.ns"},
	},
	{
		Title:        "Current GC Pause Percent",
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Overview"}},
		Metrics:      []string{"sys.gc.pause.percent"},
	},
	{
		Title:        "Pushes",
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Metrics:      []string{"queue.gc.info.pushtxn"},
	},
	{
		Title: "Queue Success",
		Organization: [][]string{
			{ReplicationLayer, "Garbage Collection"},
			{StorageLayer, "Garbage Collection"},
		},
		Metrics: []string{
			"queue.gc.process.failure",
			"queue.gc.process.success",
		},
	},
	{
		Title: "Queue Pending",
		Organization: [][]string{
			{ReplicationLayer, "Garbage Collection"},
			{StorageLayer, "Garbage Collection"},
		},
		Metrics: []string{"queue.gc.pending"},
	},
	{
		Title: "Queue Time",
		Organization: [][]string{
			{ReplicationLayer, "Garbage Collection"},
			{StorageLayer, "Garbage Collection"},
		},
		Metrics: []string{"queue.gc.processingnanos"},
	},
	{
		Title:        "Intents",
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Metrics: []string{
			"queue.gc.info.resolvesuccess",
			"queue.gc.info.resolvetotal",
		},
	},
	{
		Title:        "Txn Relationship",
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Metrics: []string{
			"queue.gc.info.transactionspangcaborted",
			"queue.gc.info.transactionspangccommitted",
			"queue.gc.info.transactionspangcpending",
			"queue.gc.info.transactionspangcstaging",
		},
	},
	{
		Title:        "Enteries in Txn Spans",
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Metrics:      []string{"queue.gc.info.transactionspanscanned"},
	},
	{
		Title:        "Memory",
		Organization: [][]string{{Process, "Server", "go"}},
		Metrics: []string{
			"sys.go.allocbytes",
			"sys.go.totalbytes",
		},
	},
	{
		Title:        "goroutines",
		Organization: [][]string{{Process, "Server", "go"}},
		Metrics:      []string{"sys.goroutines"},
	},
	{
		Title:        "Packets",
		Organization: [][]string{{Process, "Network"}},
		Metrics: []string{
			"sys.host.net.recv.packets",
			"sys.host.net.send.packets",
		},
	},
	{
		Title:        "Size",
		Organization: [][]string{{Process, "Network"}},
		Metrics: []string{
			"sys.host.net.recv.bytes",
			"sys.host.net.send.bytes",
		},
	},
	{
		Title:        "Heartbeats Success",
		Organization: [][]string{{ReplicationLayer, "Node Liveness"}},
		Metrics: []string{
			"liveness.heartbeatfailures",
			"liveness.heartbeatsuccesses",
		},
	},
	{
		Title:        "Heartbeat Latency",
		Organization: [][]string{{ReplicationLayer, "Node Liveness"}},
		Metrics:      []string{"liveness.heartbeatlatency"},
	},
	{
		Title:        "Infos",
		Organization: [][]string{{DistributionLayer, "Gossip"}},
		Metrics: []string{
			"gossip.infos.received",
			"gossip.infos.sent",
		},
	},
	{
		Title:        "Cumultative Intent Age",
		Organization: [][]string{{KVTransactionLayer, "Storage"}},
		Metrics:      []string{"intentage"},
	},
	{
		Title:        "Cumultative Intent Age",
		Organization: [][]string{{StorageLayer, "Storage", "KV"}},
		Metrics:      []string{"intentage"},
	},
	{
		Title:        "Size",
		Organization: [][]string{{KVTransactionLayer, "Storage"}},
		Metrics: []string{
			"intentbytes",
			"keybytes",
			"livebytes",
			"sysbytes",
			"totalbytes",
			"valbytes",
		},
	},
	{
		Title:        "Size",
		Organization: [][]string{{StorageLayer, "Storage", "KV"}},
		Metrics: []string{
			"intentbytes",
			"keybytes",
			"livebytes",
			"sysbytes",
			"totalbytes",
			"valbytes",
		},
	},
	{
		Title:        "Counts",
		Organization: [][]string{{KVTransactionLayer, "Storage"}},
		AxisLabel:    "MVCC Keys & Values",
		Metrics: []string{
			"intentcount",
			"keycount",
			"livecount",
			"syscount",
			"valcount",
		},
	},
	{
		Title:        "Counts",
		Organization: [][]string{{StorageLayer, "Storage", "KV"}},
		AxisLabel:    "MVCC Keys & Values",
		Metrics: []string{
			"intentcount",
			"keycount",
			"livecount",
			"syscount",
			"valcount",
		},
	},
	{
		Title:        "Metric Update Frequency",
		Organization: [][]string{{KVTransactionLayer, "Storage"}},
		Rate:         DescribeDerivative_DERIVATIVE,
		Percentiles:  false,
		Metrics:      []string{"lastupdatenanos"},
	},
	{
		Title:        "Metric Update Frequency",
		Organization: [][]string{{StorageLayer, "Storage", "KV"}},
		Metrics:      []string{"lastupdatenanos"},
	},
	{
		Title:        "Latency",
		Organization: [][]string{{KVTransactionLayer, "Requests", "Overview"}},
		Metrics:      []string{"exec.latency"},
	},
	{
		Title:        "Roundtrip Latency",
		Organization: [][]string{{KVTransactionLayer, "Clocks"}},
		Metrics:      []string{"round-trip-latency"},
	},
	{
		Title:        "Roundtrip Latency",
		Organization: [][]string{{Process, "Clocks"}},
		Metrics:      []string{"round-trip-latency"},
	},
	{
		Title:        "Total",
		Organization: [][]string{{ReplicationLayer, "Leases"}},
		Metrics: []string{
			"leases.epoch",
			"leases.expiration",
			"replicas.leaseholders",
			"replicas.leaders_not_leaseholders",
		},
	},
	{
		Title:        "Leaseholders",
		Organization: [][]string{{ReplicationLayer, "Replicas", "Overview"}},
		Metrics:      []string{"replicas.leaseholders"},
	},
	{
		Title:        "Succcess Rate",
		Organization: [][]string{{ReplicationLayer, "Leases"}},
		Metrics: []string{
			"leases.error",
			"leases.success",
		},
	},
	{
		Title:        "Transfer Success Rate",
		Organization: [][]string{{ReplicationLayer, "Leases"}},
		Metrics: []string{
			"leases.transfers.error",
			"leases.transfers.success",
		},
	},
	{
		Title: "Rebalancing Lease Transfers",
		Organization: [][]string{
			{DistributionLayer, "Rebalancing"},
			{ReplicationLayer, "Leases"},
		},
		Metrics: []string{"rebalancing.lease.transfers"},
	},
	{
		Title: "QPS",
		Organization: [][]string{
			{DistributionLayer, "Rebalancing"},
		},
		Metrics: []string{"rebalancing.queriespersecond"},
	},
	{
		Title: "Range Rebalances",
		Organization: [][]string{
			{DistributionLayer, "Rebalancing"},
			{ReplicationLayer, "Ranges"},
		},
		Metrics: []string{"rebalancing.range.rebalances"},
	},
	{
		Title:        "Node Count",
		Organization: [][]string{{ReplicationLayer, "Node Liveness"}},
		Downsampler:  DescribeAggregator_MAX,
		Percentiles:  false,
		Metrics:      []string{"liveness.livenodes"},
	},
	{
		Title:        "RPCs",
		Organization: [][]string{{DistributionLayer, "DistSender"}},
		Metrics: []string{
			"distsender.rpc.sent.local",
			"distsender.rpc.sent",
		},
	},
	{
		Title:        "Memory Usage per Statement",
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Metrics:      []string{"sql.mem.distsql.max"},
	},
	{
		Title:        "All",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Admin"}},
		Metrics:      []string{"sql.mem.admin.max"},
	},
	{
		Title:        "All",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Metrics:      []string{"sql.mem.internal.max"},
	},
	{
		Title:        "All",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Metrics:      []string{"sql.mem.conns.max"},
	},
	{
		Title:        "Errors",
		Organization: [][]string{{DistributionLayer, "DistSender"}},
		Metrics: []string{
			"distsender.rpc.sent.nextreplicaerror",
			"distsender.errors.notleaseholder",
			"distsender.errors.inleasetransferbackoffs",
		},
		AxisLabel: "Error Count",
	},
	{
		Title:        "Node Cert Expiration",
		Organization: [][]string{{Process, "Certificates"}},
		Downsampler:  DescribeAggregator_MAX,
		Aggregator:   DescribeAggregator_MAX,
		Metrics:      []string{"security.certificate.expiration.node"},
	},
	{
		Title:        "ID",
		Organization: [][]string{{Process, "Node"}},
		Downsampler:  DescribeAggregator_MAX,
		Percentiles:  false,
		Metrics:      []string{"node-id"},
	},
	{
		Title:        "Page Rotations",
		Organization: [][]string{{KVTransactionLayer, "Timestamp Cache"}},
		Metrics: []string{
			"tscache.skl.read.rotations",
			"tscache.skl.write.rotations",
		},
	},
	{
		Title:        "Page Counts",
		Organization: [][]string{{KVTransactionLayer, "Timestamp Cache"}},
		Metrics: []string{
			"tscache.skl.read.pages",
			"tscache.skl.write.pages",
		},
	},
	{
		Title:        "Active Queries",
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Metrics:      []string{"sql.distsql.queries.active"},
	},
	{
		Title:        "Total Queries",
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Metrics:      []string{"sql.distsql.queries.total"},
	},
	{
		Title:        "Entries",
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Metrics: []string{
			"changefeed.buffer_entries.in",
			"changefeed.buffer_entries.out",
		},
	},
	{
		Title:        "Total Time Spent",
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Metrics: []string{
			"changefeed.emit_nanos",
			"changefeed.flush_nanos",
			"changefeed.processing_nanos",
			"changefeed.table_metadata_nanos",
		},
	},
	{
		Title:        "Poll Request Time",
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Metrics: []string{
			"changefeed.poll_request_nanos",
		},
	},
	{
		Title:        "Max Behind Nanos",
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Metrics: []string{
			"changefeed.max_behind_nanos",
		},
	},
	{
		Title:        "Emitted Bytes",
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Metrics: []string{
			"changefeed.emitted_bytes",
		},
	},
	{
		Title:        "Emitted Messages",
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Metrics: []string{
			"changefeed.emitted_messages",
		},
	},
	{
		Title:        "Errors",
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Metrics: []string{
			"changefeed.error_retries",
		},
	},
	{
		Title:        "Flushes",
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Metrics: []string{
			"changefeed.flushes",
		},
	},
	{
		Title:        "Min High Water",
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Metrics: []string{
			"changefeed.min_high_water",
		},
	},
	{
		Title:        "Count",
		Organization: [][]string{{ReplicationLayer, "Replicas", "Overview"}},
		Metrics: []string{
			"replicas.quiescent",
			"replicas",
			"replicas.reserved",
		},
	},
	{
		Title:        "Pending",
		Organization: [][]string{{ReplicationLayer, "Raft", "Heartbeats"}},
		Metrics:      []string{"raft.heartbeats.pending"},
	},
	{
		Title:        "Command Commit",
		Organization: [][]string{{ReplicationLayer, "Raft", "Latency"}},
		Metrics:      []string{"raft.process.commandcommit.latency"},
	},
	{
		Title:        "Commands Count",
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Metrics:      []string{"raft.commandsapplied"},
	},
	{
		Title:        "Enqueued",
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Metrics:      []string{"raft.enqueued.pending"},
	},
	{
		Title:        "Leaders",
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Metrics:      []string{"replicas.leaders"},
	},
	{
		Title:        "Log Commit",
		Organization: [][]string{{ReplicationLayer, "Raft", "Latency"}},
		Metrics:      []string{"raft.process.logcommit.latency"},
	},
	{
		Title:        "Apply Committed",
		Organization: [][]string{{ReplicationLayer, "Raft", "Latency"}},
		Metrics:      []string{"raft.process.applycommitted.latency"},
	},
	{
		Title:        "Handle Ready",
		Organization: [][]string{{ReplicationLayer, "Raft", "Latency"}},
		Metrics:      []string{"raft.process.handleready.latency"},
	},
	{
		Title:        "Followers Behind By...",
		Organization: [][]string{{ReplicationLayer, "Raft", "Log"}},
		Metrics:      []string{"raftlog.behind"},
	},
	{
		Title:        "Log Successes",
		Organization: [][]string{{ReplicationLayer, "Raft", "Queues"}},
		Metrics: []string{
			"queue.raftlog.process.failure",
			"queue.raftlog.process.success",
		},
	},
	{
		Title:        "Log Pending",
		Organization: [][]string{{ReplicationLayer, "Raft", "Queues"}},
		Metrics:      []string{"queue.raftlog.pending"},
	},
	{
		Title:        "Log Processing Time Spent",
		Organization: [][]string{{ReplicationLayer, "Raft", "Queues"}},
		Metrics:      []string{"queue.raftlog.processingnanos"},
	},
	{
		Title:        "Entries Truncated",
		Organization: [][]string{{ReplicationLayer, "Raft", "Log"}},
		Metrics:      []string{"raftlog.truncated"},
	},
	{
		Title:        "MsgApp Count",
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Metrics:      []string{"raft.rcvd.app"},
	},
	{
		Title:        "MsgAppResp Count",
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Metrics:      []string{"raft.rcvd.appresp"},
	},
	{
		Title:        "Dropped",
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Metrics:      []string{"raft.rcvd.dropped"},
	},
	{
		Title:        "Heartbeat Count",
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Metrics:      []string{"raft.rcvd.heartbeat"},
	},
	{
		Title:        "MsgHeartbeatResp Count",
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Metrics:      []string{"raft.rcvd.heartbeatresp"},
	},
	{
		Title:        "MsgHeartbeatResp Count",
		Organization: [][]string{{ReplicationLayer, "Raft", "Heartbeats"}},
		Metrics:      []string{"raft.rcvd.heartbeatresp"},
	},
	{
		Title:        "MsgPreVote Count",
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Metrics:      []string{"raft.rcvd.prevote"},
	},
	{
		Title:        "MsgPreVoteResp Count",
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Metrics:      []string{"raft.rcvd.prevoteresp"},
	},
	{
		Title:        "MsgProp Count",
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Metrics:      []string{"raft.rcvd.prop"},
	},
	{
		Title:        "MsgSnap Count",
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Metrics:      []string{"raft.rcvd.snap"},
	},
	{
		Title:        "MsgTimeoutNow Count",
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Metrics:      []string{"raft.rcvd.timeoutnow"},
	},
	{
		Title:        "MsgTransferLeader Count",
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Metrics:      []string{"raft.rcvd.transferleader"},
	},
	{
		Title:        "MsgTransferLeader Count",
		Organization: [][]string{{ReplicationLayer, "Raft", "Heartbeats"}},
		Metrics:      []string{"raft.rcvd.transferleader"},
	},
	{
		Title:        "MsgVote Count",
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Metrics:      []string{"raft.rcvd.vote"},
	},
	{
		Title:        "MsgVoteResp Count",
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Metrics:      []string{"raft.rcvd.voteresp"},
	},
	{
		Title:        "Snapshot Successes",
		Organization: [][]string{{ReplicationLayer, "Raft", "Queues"}},
		Metrics: []string{
			"queue.raftsnapshot.process.failure",
			"queue.raftsnapshot.process.success",
		},
	},
	{
		Title:        "Snapshots Pending",
		Organization: [][]string{{ReplicationLayer, "Raft", "Queues"}},
		Metrics:      []string{"queue.raftsnapshot.pending"},
	},
	{
		Title:        "Snapshot Processing Time Spent",
		Organization: [][]string{{ReplicationLayer, "Raft", "Queues"}},
		Metrics:      []string{"queue.raftsnapshot.processingnanos"},
	},
	{
		Title:        "Working vs. Ticking TIme",
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Metrics: []string{
			"raft.process.tickingnanos",
			"raft.process.workingnanos",
		},
	},
	{
		Title:        "Ticks Queued",
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Metrics:      []string{"raft.ticks"},
	},
	{
		Title:        "Entries",
		Organization: [][]string{{ReplicationLayer, "Raft", "Entry Cache"}},
		Metrics: []string{
			"raft.entrycache.size",
		},
	},
	{
		Title:        "Hits",
		Organization: [][]string{{ReplicationLayer, "Raft", "Entry Cache"}},
		Metrics: []string{
			"raft.entrycache.accesses",
			"raft.entrycache.hits",
		},
		AxisLabel: "Entry Cache Operations",
	},
	{
		Title:        "Size",
		Organization: [][]string{{ReplicationLayer, "Raft", "Entry Cache"}},
		Metrics: []string{
			"raft.entrycache.bytes",
		},
	},
	{
		Title: "Add, Split, Remove",
		Organization: [][]string{
			{DistributionLayer, "Ranges"},
			{ReplicationLayer, "Ranges"},
		},
		Metrics: []string{
			"range.adds",
			"range.removes",
			"range.splits",
			"range.merges",
		},
	},
	{
		Title: "Overview",
		Organization: [][]string{
			{DistributionLayer, "Ranges"},
			{ReplicationLayer, "Ranges"},
		},
		Metrics: []string{
			"ranges",
			"ranges.unavailable",
			"ranges.underreplicated",
			"ranges.overreplicated",
		},
	},
	{
		Title: "Rangefeed",
		Organization: [][]string{
			{DistributionLayer, "Ranges"},
			{ReplicationLayer, "Ranges"},
		},
		Metrics: []string{
			"kv.rangefeed.catchup_scan_nanos",
		},
	},
	{
		Title:        "Leader Transfers",
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Metrics:      []string{"range.raftleadertransfers"},
	},
	{
		Title:        "Raft Leader Transfers",
		Organization: [][]string{{ReplicationLayer, "Ranges"}},
		Metrics:      []string{"range.raftleadertransfers"},
	},
	{
		Title: "Snapshots",
		Organization: [][]string{
			{DistributionLayer, "Ranges"},
			{ReplicationLayer, "Ranges"},
		},
		Metrics: []string{
			"range.snapshots.generated",
			"range.snapshots.normal-applied",
			"range.snapshots.preemptive-applied",
		},
	},
	{
		Title:        "Success",
		Organization: [][]string{{StorageLayer, "RocksDB", "Block Cache"}},
		Metrics: []string{
			"rocksdb.block.cache.hits",
			"rocksdb.block.cache.misses",
		},
	},
	{
		Title:        "Size",
		Organization: [][]string{{StorageLayer, "RocksDB", "Block Cache"}},
		Metrics: []string{
			"rocksdb.block.cache.pinned-usage",
			"rocksdb.block.cache.usage",
		},
	},
	{
		Title:        "Bloom Filter",
		Organization: [][]string{{StorageLayer, "RocksDB", "Overview"}},
		Metrics: []string{
			"rocksdb.bloom.filter.prefix.checked",
			"rocksdb.bloom.filter.prefix.useful",
		},
	},
	{
		Title:        "Compactions",
		Organization: [][]string{{StorageLayer, "RocksDB", "Overview"}},
		Metrics:      []string{"rocksdb.compactions"},
	},
	{
		Title:        "Flushes",
		Organization: [][]string{{StorageLayer, "RocksDB", "Overview"}},
		Metrics:      []string{"rocksdb.flushes"},
	},
	{
		Title:        "Memtable",
		Organization: [][]string{{StorageLayer, "RocksDB", "Overview"}},
		Metrics:      []string{"rocksdb.memtable.total-size"},
	},
	{
		Title:        "Count",
		Organization: [][]string{{StorageLayer, "RocksDB", "SSTables"}},
		Metrics:      []string{"rocksdb.num-sstables"},
	},
	{
		Title:        "Read Amplification",
		Organization: [][]string{{StorageLayer, "RocksDB", "Overview"}},
		Metrics:      []string{"rocksdb.read-amplification"},
	},
	{
		Title:        "Index & Filter Block Size",
		Organization: [][]string{{StorageLayer, "RocksDB", "Overview"}},
		Metrics:      []string{"rocksdb.table-readers-mem-estimate"},
	},
	{
		Title:        "Algorithm Enum",
		Organization: [][]string{{StorageLayer, "RocksDB", "Encryption at Rest"}},
		Metrics:      []string{"rocksdb.encryption.algorithm"},
	},
	{
		Title:        "Reblance Count",
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Metrics:      []string{"queue.replicate.rebalancereplica"},
	},
	{
		Title:        "Remove Replica Count",
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Metrics: []string{
			"queue.replicate.removedeadreplica",
			"queue.replicate.removereplica",
		},
	},
	{
		Title:        "Removal Count",
		Organization: [][]string{{ReplicationLayer, "Replica GC Queue"}},
		Metrics:      []string{"queue.replicagc.removereplica"},
	},
	{
		Title:        "Successes",
		Organization: [][]string{{ReplicationLayer, "Replica GC Queue"}},
		Metrics: []string{
			"queue.replicagc.process.failure",
			"queue.replicagc.process.success",
		},
	},
	{
		Title:        "Pending",
		Organization: [][]string{{ReplicationLayer, "Replica GC Queue"}},
		Metrics:      []string{"queue.replicagc.pending"},
	},
	{
		Title:        "Time Spent",
		Organization: [][]string{{ReplicationLayer, "Replica GC Queue"}},
		Metrics:      []string{"queue.replicagc.processingnanos"},
	},
	{
		Title:        "Successes",
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Metrics: []string{
			"queue.replicate.process.failure",
			"queue.replicate.process.success",
		},
	},
	{
		Title:        "Purgatory",
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Metrics:      []string{"queue.replicate.purgatory"},
	},
	{
		Title:        "Pending",
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Metrics:      []string{"queue.replicate.pending"},
	},
	{
		Title:        "Time Spent",
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Metrics:      []string{"queue.replicate.processingnanos"},
	},
	{
		Title:        "Restarts",
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Downsampler:  DescribeAggregator_MAX,
		Percentiles:  true,
		Metrics:      []string{"txn.restarts"},
	},
	{
		Title:        "Restart Cause Mix",
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Metrics: []string{
			"txn.restarts.possiblereplay",
			"txn.restarts.serializable",
			"txn.restarts.writetooold",
			"txn.restarts.asyncwritefailure",
			"txn.restarts.readwithinuncertainty",
			"txn.restarts.txnaborted",
			"txn.restarts.txnpush",
			"txn.restarts.unknown",
			"txn.restarts.writetoooldmulti",
		},
	},
	{
		Title:        "RSS",
		Organization: [][]string{{Process, "Server", "Overview"}},
		Metrics:      []string{"sys.rss"},
	},
	{
		Title:        "Session Current",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Admin"}},
		Metrics:      []string{"sql.mem.admin.session.current"},
	},
	{
		Title:        "Session Current",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Metrics:      []string{"sql.mem.internal.session.current"},
	},
	{
		Title:        "Session Current",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Metrics:      []string{"sql.mem.conns.session.current"},
	},
	{
		Title:        "Session All",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Admin"}},
		Metrics:      []string{"sql.mem.admin.session.max"},
	},
	{
		Title:        "Session All",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Metrics:      []string{"sql.mem.internal.session.max"},
	},
	{
		Title:        "Session All",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Metrics:      []string{"sql.mem.conns.session.max"},
	},
	{
		Title: "Stuck Acquiring Lease",
		Organization: [][]string{
			{KVTransactionLayer, "Requests", "Slow"},
			{ReplicationLayer, "Requests", "Slow"},
		},
		Downsampler: DescribeAggregator_MAX,
		Percentiles: false,
		Metrics:     []string{"requests.slow.lease"},
	},
	{
		Title:        "Stuck Acquisition Count",
		Organization: [][]string{{ReplicationLayer, "Leases"}},
		Metrics:      []string{"requests.slow.lease"},
	},
	{
		Title: "Stuck in Raft",
		Organization: [][]string{
			{KVTransactionLayer, "Requests", "Slow"},
			{ReplicationLayer, "Requests", "Slow"},
		},
		Downsampler: DescribeAggregator_MAX,
		Percentiles: false,
		Metrics:     []string{"requests.slow.raft"},
	},
	{
		Title: "Latch",
		Organization: [][]string{
			{KVTransactionLayer, "Requests", "Slow"},
			{ReplicationLayer, "Requests", "Slow"},
		},
		Downsampler: DescribeAggregator_MAX,
		Percentiles: false,
		Metrics:     []string{"requests.slow.latch"},
	},
	{
		Title:        "Stuck Request Count",
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Metrics:      []string{"requests.slow.raft"},
	},
	{
		Title: "Successes",
		Organization: [][]string{
			{DistributionLayer, "Split Queue"},
			{ReplicationLayer, "Split Queue"},
		},
		Downsampler: DescribeAggregator_MAX,
		Percentiles: false,
		Metrics: []string{
			"queue.split.process.failure",
			"queue.split.process.success",
		},
	},
	{
		Title: "Pending",
		Organization: [][]string{
			{DistributionLayer, "Split Queue"},
			{ReplicationLayer, "Split Queue"},
		},
		Downsampler: DescribeAggregator_MAX,
		Percentiles: false,
		Metrics: []string{
			"queue.split.pending",
			"queue.split.purgatory",
		},
	},
	{
		Title: "Time Spent",
		Organization: [][]string{
			{DistributionLayer, "Split Queue"},
			{ReplicationLayer, "Split Queue"},
		},
		Metrics: []string{"queue.split.processingnanos"},
	},
	{
		Title: "Successes",
		Organization: [][]string{
			{DistributionLayer, "Merge Queue"},
			{ReplicationLayer, "Merge Queue"},
		},
		Downsampler: DescribeAggregator_MAX,
		Percentiles: false,
		Metrics: []string{
			"queue.merge.process.failure",
			"queue.merge.process.success",
		},
	},
	{
		Title: "Pending",
		Organization: [][]string{
			{DistributionLayer, "Merge Queue"},
			{ReplicationLayer, "Merge Queue"},
		},
		Downsampler: DescribeAggregator_MAX,
		Percentiles: false,
		Metrics: []string{
			"queue.merge.pending",
			"queue.merge.purgatory",
		},
	},
	{
		Title: "Time Spent",
		Organization: [][]string{
			{DistributionLayer, "Merge Queue"},
			{ReplicationLayer, "Merge Queue"},
		},
		Metrics: []string{"queue.merge.processingnanos"},
	},
	{
		Title:        "Exec Latency",
		Organization: [][]string{{SQLLayer, "SQL"}},
		Metrics: []string{
			"sql.exec.latency",
			"sql.exec.latency.internal",
		},
		AxisLabel: "Latency",
	},
	{
		Title:        "Service Latency",
		Organization: [][]string{{SQLLayer, "SQL"}},
		Metrics: []string{
			"sql.service.latency",
			"sql.service.latency.internal",
		},
		AxisLabel: "Latency",
	},
	{
		Title:        "Successes",
		Organization: [][]string{{Timeseries, "Maintenance Queue"}},
		Metrics: []string{
			"queue.tsmaintenance.process.success",
			"queue.tsmaintenance.process.failure",
		},
	},
	{
		Title:        "Pending",
		Organization: [][]string{{Timeseries, "Maintenance Queue"}},
		Metrics:      []string{"queue.tsmaintenance.pending"},
	},
	{
		Title:        "Time Spent",
		Organization: [][]string{{Timeseries, "Maintenance Queue"}},
		Metrics:      []string{"queue.tsmaintenance.processingnanos"},
	},
	{
		Title:        "Lease Transfer Count",
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Metrics:      []string{"queue.replicate.transferlease"},
	},
	{
		Title:        "Transaction Control Mix",
		Organization: [][]string{{SQLLayer, "SQL"}},
		Metrics: []string{
			"sql.txn.abort.count",
			"sql.txn.begin.count",
			"sql.txn.commit.count",
			"sql.txn.rollback.count",
		},
	},
	{
		Title:        "Transaction Control Mix (Internal)",
		Organization: [][]string{{SQLLayer, "SQL"}},
		Metrics: []string{
			"sql.txn.abort.count.internal",
			"sql.txn.begin.count.internal",
			"sql.txn.commit.count.internal",
			"sql.txn.rollback.count.internal",
		},
	},
	{
		Title:        "Txn Current",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Admin"}},
		Metrics:      []string{"sql.mem.admin.txn.current"},
	},
	{
		Title:        "Txn Current",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Metrics:      []string{"sql.mem.internal.txn.current"},
	},
	{
		Title:        "Txn Current",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Metrics:      []string{"sql.mem.conns.txn.current"},
	},
	{
		Title:        "Txn All",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Admin"}},
		Metrics:      []string{"sql.mem.admin.txn.max"},
	},
	{
		Title:        "Txn All",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Metrics:      []string{"sql.mem.internal.txn.max"},
	},
	{
		Title:        "Txn All",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Metrics:      []string{"sql.mem.conns.txn.max"},
	},
	{
		Title:        "Uptime",
		Organization: [][]string{{Process, "Server", "Overview"}},
		Metrics:      []string{"sys.uptime"},
	},
	{
		Title:        "Size",
		Organization: [][]string{{Timeseries, "Overview"}},
		Metrics:      []string{"timeseries.write.bytes"},
	},
	{
		Title:        "Error Count",
		Organization: [][]string{{Timeseries, "Overview"}},
		Metrics:      []string{"timeseries.write.errors"},
	},
	{
		Title:        "Count",
		Organization: [][]string{{Timeseries, "Overview"}},
		Metrics:      []string{"timeseries.write.samples"}},
}

// chartCatalog represents the entire chart catalog, which is an array of
// ChartSections, to which the individual charts and subsections defined above
// are added.
var chartCatalog = []ChartSection{
	{
		Title:           Process,
		LongTitle:       Process,
		CollectionTitle: "process-all",
		Description: `These charts detail the overall performance of the 
		<code>cockroach</code> process running on this server.`,
		Level: 0,
	},
	{
		Title:           SQLLayer,
		LongTitle:       SQLLayer,
		CollectionTitle: "sql-layer-all",
		Description: `In the SQL layer, nodes receive commands and then parse, plan, 
		and execute them. <br/><br/><a class="catalog-link" 
		href="https://www.cockroachlabs.com/docs/stable/architecture/sql-layer.html">
		SQL Layer Architecture Docs >></a>"`,
		Level: 0,
	},
	{
		Title:           KVTransactionLayer,
		LongTitle:       KVTransactionLayer,
		CollectionTitle: "kv-transaction-layer-all",
		Description: `The KV Transaction Layer coordinates concurrent requests as 
		key-value operations. To maintain consistency, this is also where the cluster 
		manages time. <br/><br/><a class="catalog-link" 
		href="https://www.cockroachlabs.com/docs/stable/architecture/transaction-layer.html">
		Transaction Layer Architecture Docs >></a>`,
		Level: 0,
	},
	{
		Title:           DistributionLayer,
		LongTitle:       DistributionLayer,
		CollectionTitle: "distribution-layer-all",
		Description: `The Distribution Layer provides a unified view of your cluster’s data, 
		which are actually broken up into many key-value ranges. <br/><br/><a class="catalog-link" 
		href="https://www.cockroachlabs.com/docs/stable/architecture/distribution-layer.html"> 
		Distribution Layer Architecture Docs >></a>`,
		Level: 0,
	},
	{
		Title:           ReplicationLayer,
		LongTitle:       ReplicationLayer,
		CollectionTitle: "replication-layer-all",
		Description: `The Replication Layer maintains consistency between copies of ranges 
		(known as replicas) through our consensus algorithm, Raft. <br/><br/><a class="catalog-link" 
			href="https://www.cockroachlabs.com/docs/stable/architecture/replication-layer.html"> 
			Replication Layer Architecture Docs >></a>`,
		Level: 0,
	},
	{
		Title:           StorageLayer,
		LongTitle:       StorageLayer,
		CollectionTitle: "replication-layer-all",
		Description: `The Storage Layer reads and writes data to disk, as well as manages 
		garbage collection. <br/><br/><a class="catalog-link" 
		href="https://www.cockroachlabs.com/docs/stable/architecture/storage-layer.html">
		Storage Layer Architecture Docs >></a>`,
		Level: 0,
	},
	{
		Title:           Timeseries,
		LongTitle:       Timeseries,
		CollectionTitle: "timeseries-all",
		Description: `Your cluster collects data about its own performance, which is used to 
		power the very charts you\'re using, among other things.`,
		Level: 0,
	},
}

// catalogKey indexes the array position of the Level 0 ChartSections.
// This optimizes generating the chart catalog by limiting the search space every
// chart uses when being added to the catalog.
var catalogKey = map[string]int{
	Process:            0,
	SQLLayer:           1,
	KVTransactionLayer: 2,
	DistributionLayer:  3,
	ReplicationLayer:   4,
	StorageLayer:       5,
	Timeseries:         6,
}

// unitsKey converts between metric.Unit and catalog.AxisUnits which is
// necessary because charts only support a subset of unit types.
var unitsKey = map[metric.Unit]AxisUnits{
	metric.Unit_BYTES:         AxisUnits_BYTES,
	metric.Unit_CONST:         AxisUnits_COUNT,
	metric.Unit_COUNT:         AxisUnits_COUNT,
	metric.Unit_NANOSECONDS:   AxisUnits_DURATION,
	metric.Unit_PERCENT:       AxisUnits_COUNT,
	metric.Unit_SECONDS:       AxisUnits_DURATION,
	metric.Unit_TIMESTAMP_NS:  AxisUnits_DURATION,
	metric.Unit_TIMESTAMP_SEC: AxisUnits_DURATION,
}

// aggKey converts between catalog.DescribeAggregator to
// tspb.TimeSeriesQueryAggregator which is necessary because
// tspb.TimeSeriesQueryAggregator doesn't have a checkable zero value, which the
// catalog requires to support defaults (DescribeAggregator_UNSET_AGG).
var aggKey = map[DescribeAggregator]tspb.TimeSeriesQueryAggregator{
	DescribeAggregator_AVG: tspb.TimeSeriesQueryAggregator_AVG,
	DescribeAggregator_MAX: tspb.TimeSeriesQueryAggregator_MAX,
	DescribeAggregator_MIN: tspb.TimeSeriesQueryAggregator_MIN,
	DescribeAggregator_SUM: tspb.TimeSeriesQueryAggregator_SUM,
}

// derKey converts between catalog.DescribeDerivative to
// tspb.TimeSeriesQueryDerivative which is necessary because
// tspb.TimeSeriesQueryDerivative doesn't have a checkable zero value, which the
// catalog requires to support defaults (DescribeDerivative_UNSET_DER).
var derKey = map[DescribeDerivative]tspb.TimeSeriesQueryDerivative{
	DescribeDerivative_DERIVATIVE:              tspb.TimeSeriesQueryDerivative_DERIVATIVE,
	DescribeDerivative_NONE:                    tspb.TimeSeriesQueryDerivative_NONE,
	DescribeDerivative_NON_NEGATIVE_DERIVATIVE: tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE,
}

// GenerateCatalog creates an array of ChartSections, which is served at
// _admin/v1/chartcatalog.
func GenerateCatalog(metadata map[string]metric.Metadata) ([]ChartSection, error) {

	// Range over all chartDescriptions.
	for _, cd := range charts {

		// Range over each level of organization.
		for i, organization := range cd.Organization {

			// Ensure the organization has both Level 0 and Level 1 organization.
			if len(organization) < 2 {
				return nil, errors.Errorf(`%s must have at Level 0 and Level 1 organization,
				 only has %v`, cd.Title, organization)
			}

			// Make sure the organization's Level 0 is in the catalog.
			level0CatalogIndex, ok := catalogKey[organization[0]]
			if !ok {
				return nil, errors.Errorf("Invalid Level 0 for %s using %v", cd.Title, organization)
			}

			ic, err := createIndividualChart(metadata, cd, i)

			if err != nil {
				return nil, err
			}

			// If ic has no Metrics, skip. Note that this isn't necessarily an error
			// e.g. nodes without SSLs do not have certificate expiration timestamps,
			// so those charts should not be added to the catalog.
			if len(ic.Metrics) == 0 {
				continue
			}

			// Add the individual chart and its subsections to the chart catalog
			chartCatalog[level0CatalogIndex].addChartAndSubsections(organization, ic)
		}
	}

	// Find all metrics not in the catalog
	for _, v := range chartCatalog {
		v.accountForMetrics(metadata)
	}

	for k := range metadata {
		fmt.Println(k)
	}

	return chartCatalog, nil
}

func (c ChartSection) accountForMetrics(metadata map[string]metric.Metadata) {
	for _, x := range c.Charts {
		for _, metric := range x.Metrics {
			_, ok := metadata[metric.Name]
			if ok {
				delete(metadata, metric.Name)
			}
		}
	}

	for _, x := range c.Subsections {
		x.accountForMetrics(metadata)
	}
}

// createIndividualChart creates an individual chart, which will later be added
// to the approrpiate ChartSection.
// - metadata is used to find meaningful information about cd.Metrics' values.
// - cd describes the individual chart to create.
// - organizationIndex specifies which array from cd.Organization to use.
func createIndividualChart(
	metadata map[string]metric.Metadata,
	cd chartDescription,
	organizationIndex int) (IndividualChart, error) {

	var ic IndividualChart

	err := ic.addMetrics(cd, metadata)

	if err != nil {
		return ic, err
	}

	// If chart has no data, abandon. Note that this isn't necessarily an error
	// e.g. nodes without SSLs do not have certificate expiration timestamps,
	// so those charts should not be added to the catalog.
	if len(ic.Metrics) == 0 {
		return ic, nil
	}

	// Get first metric's MetricType, which will be used to determine its default values.
	mt := ic.Metrics[0].MetricType

	err = ic.addDisplayProperties(cd, mt)

	if err != nil {
		return ic, err
	}

	ic.addNames(cd, organizationIndex)

	return ic, nil
}

// addMetrics sets the IndividualChart's Metric values by looking up the chartDescription
// metrics in the metadata map.
func (ic *IndividualChart) addMetrics(
	cd chartDescription, metadata map[string]metric.Metadata,
) error {
	for _, x := range cd.Metrics {

		md, ok := metadata[x]

		// If metric is missing from metadata, don't add it to this chart e.g.
		// insecure nodes do not metadata related to SSL certificates, so those metrics
		// should not be added to any charts.
		if !ok {
			continue
		}

		unit, ok := unitsKey[md.Unit]

		if !ok {
			return errors.Errorf(
				"%s's metric.Metadata has an unrecognized Unit, %v", md.Name, md.Unit,
			)
		}

		ic.Metrics = append(ic.Metrics, ChartMetric{
			Name:           md.Name,
			Help:           md.Help,
			AxisLabel:      md.Measurement,
			PreferredUnits: unit,
			MetricType:     md.MetricType,
		})

		if ic.Metrics[0].MetricType != md.MetricType {
			return errors.Errorf(`%s and %s have different MetricTypes, but are being 
			added to the same chart, %v`, ic.Metrics[0].Name, md.Name, ic)
		}
	}

	return nil
}

// addNames sets the IndividualChart's Title, Longname, and CollectionName.
func (ic *IndividualChart) addNames(cd chartDescription, organizationIndex int) {

	ic.Title = cd.Title

	// Find string delimeters that are not dashes, including spaces, slashes, and
	// commas.
	nondashDelimeters := regexp.MustCompile("( )|/|,")

	// Longnames look like "SQL Layer | SQL | Connections".
	// CollectionNames look like "sql-layer-sql-connections".
	for _, n := range cd.Organization[organizationIndex] {
		ic.LongTitle += n + " | "
		ic.CollectionTitle += nondashDelimeters.ReplaceAllString(strings.ToLower(n), "-") + "-"
	}

	ic.LongTitle += cd.Title
	ic.CollectionTitle += nondashDelimeters.ReplaceAllString(strings.ToLower(cd.Title), "-")

}

// addDisplayProperties sets the IndividualChart's display properties, such as
// its Downsampler and Aggregator.
func (ic *IndividualChart) addDisplayProperties(
	cd chartDescription, mt prometheusgo.MetricType,
) error {

	defaults := chartDefaultsPerMetricType[mt]

	// Create copy of cd to avoid argument mutation.
	cdFull := cd

	// Set all zero values to the chartDefault's value
	if cdFull.Downsampler == DescribeAggregator_UNSET_AGG {
		cdFull.Downsampler = defaults.Downsampler
	}
	if cdFull.Aggregator == DescribeAggregator_UNSET_AGG {
		cdFull.Aggregator = defaults.Aggregator
	}
	if cdFull.Rate == DescribeDerivative_UNSET_DER {
		cdFull.Rate = defaults.Rate
	}
	if cdFull.Percentiles == false {
		cdFull.Percentiles = defaults.Percentiles
	}

	// Set unspecified AxisUnits to the first metric's value.
	if cdFull.Units == AxisUnits_UNSET_UNITS {

		pu := ic.Metrics[0].PreferredUnits
		for _, m := range ic.Metrics {
			if m.PreferredUnits != pu {
				return errors.Errorf(`Chart %s has metrics with different preferred 
				units; need to specify Units in its chartDescription: %v`, cd.Title, ic)
			}
		}

		cdFull.Units = pu
	}

	// Set unspecified AxisLabels to the first metric's value.
	if cdFull.AxisLabel == "" {
		al := ic.Metrics[0].AxisLabel

		for _, m := range ic.Metrics {
			if m.AxisLabel != al {
				return errors.Errorf(`Chart %s has metrics with different axis labels; 
				need to specify an AxisLabel in its chartDescription: %v`, cd.Title, ic)
			}
		}

		cdFull.AxisLabel = al
	}

	// Populate the IndividualChart values.
	ds, ok := aggKey[cdFull.Downsampler]
	if !ok {
		return errors.Errorf(
			"%s's chartDescription has an unrecognized Downsampler, %v", cdFull.Title, cdFull.Downsampler,
		)
	}
	ic.Downsampler = &ds

	agg, ok := aggKey[cdFull.Aggregator]
	if !ok {
		return errors.Errorf(
			"%s's chartDescription has an unrecognized Aggregator, %v", cdFull.Title, cdFull.Aggregator,
		)
	}
	ic.Aggregator = &agg

	der, ok := derKey[cdFull.Rate]
	if !ok {
		return errors.Errorf(
			"%s's chartDescription has an unrecognized Rate, %v", cdFull.Title, cdFull.Rate,
		)
	}
	ic.Derivative = &der

	ic.Percentiles = cdFull.Percentiles
	ic.Units = cdFull.Units
	ic.AxisLabel = cdFull.AxisLabel

	return nil
}

// addChartAndSubsections adds subsections identified in the organization to the calling
// ChartSection until it reaches the last level of organization, where it adds the chart
// to the last subsection.
func (cs *ChartSection) addChartAndSubsections(organization []string, ic IndividualChart) {

	// subsection is either an existing or new element of cs.Subsections that will either contain
	// more Subsections or the IndividualChart we want to add.
	var subsection *ChartSection

	// subsectionLevel identifies the level of the organization slice we're using; it will always
	// be one greater than its parent's level.
	subsectionLevel := int(cs.Level + 1)

	// To identify how to treat subsection, we need to search for a cs.Subsections element with
	// the same name as the current organization index. However, because ChartSection contains
	// a slice of pointers, it cannot be compared to an empty ChartSection, so use a bool to
	// to track if it's found.
	found := false

	for _, ss := range cs.Subsections {
		if ss.Title == organization[subsectionLevel] {
			found = true
			subsection = ss
			break
		}
	}

	// If not found, create a new ChartSection and append it as a subsection.
	if !found {

		// Find string delimeters that are not dashes, including spaces, slashes, and
		// commas.
		nondashDelimeters := regexp.MustCompile("( )|/|,")

		subsection = &ChartSection{
			Title: organization[subsectionLevel],
			// Longnames look like "SQL Layer | SQL".
			LongTitle: "All",
			// CollectionNames look like "sql-layer-sql".
			CollectionTitle: nondashDelimeters.ReplaceAllString(strings.ToLower(organization[0]), "-"),
			Level:           int32(subsectionLevel),
		}

		// Complete Longname and Colectionname values.
		for i := 1; i <= subsectionLevel; i++ {
			subsection.LongTitle += " " + organization[i]
			subsection.CollectionTitle += "-" + nondashDelimeters.ReplaceAllString(strings.ToLower(organization[i]), "-")
		}

		cs.Subsections = append(cs.Subsections, subsection)
	}

	// If this is the last level of the organization, add the IndividualChart here. Otheriwse, recurse.
	if subsectionLevel == (len(organization) - 1) {
		subsection.Charts = append(subsection.Charts, &ic)
	} else {
		subsection.addChartAndSubsections(organization, ic)
	}
}
