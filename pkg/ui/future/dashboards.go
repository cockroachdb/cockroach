package future

// Metric represents a single time series metric to be displayed on a graph.
type Metric struct {
	Name            string  `json:"name"`                      // Metric name without 'cr.node.' or 'cr.store.' prefix
	Title           string  `json:"title"`                     // Display title for this metric series
	Derivative      string  `json:"derivative,omitempty"`      // e.g., "NON_NEGATIVE_DERIVATIVE"
	Downsampler     string  `json:"downsampler,omitempty"`     // e.g., "MAX"
	Aggregation     string  `json:"aggregation,omitempty"`     // e.g., "SUM", "MAX", "AVG"
	NonNegativeRate bool    `json:"nonNegativeRate,omitempty"` // Apply non-negative rate transformation
	PerNode         bool    `json:"-"`                         // If true, create one series per node (expanded at render time)
	PerStore        bool    `json:"-"`                         // If true, create one series per store (expanded at render time)
	Sources         []int   `json:"sources,omitempty"`         // Specific node/store IDs (empty means aggregate all)
	Scale           float64 `json:"scale,omitempty"`           // Scale factor to apply
	Color           string  `json:"color,omitempty"`           // Optional color override

	StorePrefix bool `json:"-"`
}

// DashboardGraph represents a single graph/chart on a dashboard.
type DashboardGraph struct {
	Title   string   `json:"title"`
	Tooltip string   `json:"tooltip"` // HTML tooltip text
	Metrics []Metric `json:"metrics"`

	Units      string `json:"units,omitempty"` // e.g., "BYTES", "DURATION", "COUNT", "PERCENTAGE"
	UnitsLabel string `json:"unitsLabel"`      // Axis label

	IsKVGraph bool `json:"isKvGraph,omitempty"` // Whether this is a KV-level graph
}

var DashboardDisplayNames = map[string]string{
	"overview": "Overview",
	"sql":      "SQL",
	"requests": "Slow Requests",
}

var DASHBOARDS = map[string][]DashboardGraph{
	"overview": {
		{
			Title:      "SQL Queries Per Second",
			Tooltip:    "A moving average of the number of SELECT, INSERT, UPDATE, and DELETE statements, and the sum of all four, successfully executed per second across all nodes.",
			UnitsLabel: "queries per second",
			Metrics: []Metric{
				{Name: "sql.select.count", Title: "Selects", NonNegativeRate: true},
				{Name: "sql.update.count", Title: "Updates", NonNegativeRate: true},
				{Name: "sql.insert.count", Title: "Inserts", NonNegativeRate: true},
				{Name: "sql.delete.count", Title: "Deletes", NonNegativeRate: true},
				{Name: "sql.crud_query.count", Title: "Total Queries", NonNegativeRate: true},
			},
		},
		{
			Title:      "Service Latency: SQL Statements, 99th percentile",
			Tooltip:    "Over the last minute, this node executed 99% of SQL statements within this time. This time only includes SELECT, INSERT, UPDATE and DELETE statements and does not include network latency between the node and client.",
			Units:      "DURATION",
			UnitsLabel: "latency",
			Metrics: []Metric{
				{Name: "sql.service.latency-p99", PerNode: true, Downsampler: "MAX"},
			},
		},
		{
			Title:      "SQL Statement Contention",
			Tooltip:    "A moving average of the number of SQL statements executed per second that experienced contention across all nodes.",
			UnitsLabel: "Average number of queries per second",
			Metrics: []Metric{
				{Name: "sql.distsql.contended_queries.count", Title: "Contention", NonNegativeRate: true},
			},
		},
		{
			Title:      "Replicas per Node",
			Tooltip:    "The number of range replicas stored on this node. Ranges are subsets of your data, which are replicated to ensure survivability.",
			UnitsLabel: "replicas",
			IsKVGraph:  true,
			Metrics: []Metric{
				{Name: "replicas", PerStore: true},
			},
		},
		{
			Title:      "Capacity",
			Tooltip:    "Summary of total, available, and used capacity across all nodes.",
			Units:      "BYTES",
			UnitsLabel: "capacity",
			IsKVGraph:  true,
			Metrics: []Metric{
				{Name: "capacity", Title: "Max", StorePrefix: true},
				{Name: "capacity.available", Title: "Available", StorePrefix: true},
				{Name: "capacity.used", Title: "Used", StorePrefix: true},
			},
		},
	},
	"hardware": {
		{
			Title:      "CPU Percent",
			Tooltip:    "CPU usage for the CRDB nodes.",
			Units:      "PERCENTAGE",
			UnitsLabel: "CPU",
			Metrics: []Metric{
				{Name: "sys.cpu.combined.percent-normalized", PerNode: true},
			},
		},
		{
			Title:      "Host CPU Percent",
			Tooltip:    "Machine-wide CPU usage.",
			Units:      "PERCENTAGE",
			UnitsLabel: "CPU",
			Metrics: []Metric{
				{Name: "sys.cpu.host.combined.percent-normalized", PerNode: true},
			},
		},
		{
			Title:      "Memory Usage",
			Tooltip:    "Memory in use across all nodes.",
			Units:      "BYTES",
			UnitsLabel: "memory usage",
			Metrics: []Metric{
				{Name: "sys.rss", PerNode: true},
			},
		},
		{
			Title:      "Disk Read Bytes/s",
			Units:      "BYTES",
			UnitsLabel: "bytes",
			Metrics: []Metric{
				{Name: "sys.host.disk.read.bytes", PerNode: true, NonNegativeRate: true},
			},
		},
		{
			Title:      "Disk Write Bytes/s",
			Units:      "BYTES",
			UnitsLabel: "bytes",
			Metrics: []Metric{
				{Name: "sys.host.disk.write.bytes", PerNode: true, NonNegativeRate: true},
			},
		},
		{
			Title:      "Disk Read IOPS",
			Units:      "COUNT",
			UnitsLabel: "IOPS",
			Metrics: []Metric{
				{Name: "sys.host.disk.read.count", PerNode: true, NonNegativeRate: true},
			},
		},
		{
			Title:      "Disk Write IOPS",
			Units:      "COUNT",
			UnitsLabel: "IOPS",
			Metrics: []Metric{
				{Name: "sys.host.disk.write.count", PerNode: true, NonNegativeRate: true},
			},
		},
		{
			Title:      "Disk Ops In Progress",
			Units:      "COUNT",
			UnitsLabel: "Ops",
			Metrics: []Metric{
				{Name: "sys.host.disk.iopsinprogress", PerNode: true},
			},
		},
		{
			Title:      "Available Disk Capacity",
			Tooltip:    "Available disk capacity per store.",
			Units:      "BYTES",
			UnitsLabel: "capacity",
			Metrics: []Metric{
				{Name: "capacity.available", PerNode: true, PerStore: true},
			},
		},
	},
	"runtime": {
		{
			Title:      "Live Node Count",
			Tooltip:    "The number of live nodes in the cluster.",
			UnitsLabel: "nodes",
			Metrics: []Metric{
				{Name: "liveness.livenodes", Title: "Live Nodes", Aggregation: "MAX"},
			},
		},
		{
			Title:      "Memory Usage",
			Tooltip:    "Memory in use: RSS (Total memory in use by CockroachDB), Go Allocated (Memory allocated by the Go layer), Go Total (Total memory managed by the Go layer), Go Limit (Go soft memory limit), C Allocated (Memory allocated by the C layer), C Total (Total memory managed by the C layer).",
			Units:      "BYTES",
			UnitsLabel: "memory usage",
			Metrics: []Metric{
				{Name: "sys.rss", Title: "Total memory (RSS)"},
				{Name: "sys.go.allocbytes", Title: "Go Allocated"},
				{Name: "sys.go.totalbytes", Title: "Go Total"},
				{Name: "sys.go.limitbytes", Title: "Go Limit"},
				{Name: "sys.cgo.allocbytes", Title: "CGo Allocated"},
				{Name: "sys.cgo.totalbytes", Title: "CGo Total"},
			},
		},
		{
			Title:      "Goroutine Count",
			Tooltip:    "The number of Goroutines. This count should rise and fall based on load.",
			Units:      "COUNT",
			UnitsLabel: "goroutines",
			Metrics: []Metric{
				{Name: "sys.goroutines", PerNode: true},
			},
		},
		{
			Title:      "Goroutine Scheduling Latency: 99th percentile",
			Tooltip:    "P99 scheduling latency for goroutines",
			Units:      "DURATION",
			UnitsLabel: "latency",
			Metrics: []Metric{
				{Name: "go.scheduler_latency-p99", PerNode: true, Downsampler: "MAX"},
			},
		},
		{
			Title:      "Runnable Goroutines per CPU",
			Tooltip:    "The number of Goroutines waiting for CPU. This count should rise and fall based on load.",
			UnitsLabel: "goroutines",
			Metrics: []Metric{
				{Name: "sys.runnable.goroutines.per.cpu", PerNode: true},
			},
		},
		{
			Title:      "GC Runs",
			Tooltip:    "The number of times that Go's garbage collector was invoked per second.",
			UnitsLabel: "runs",
			Metrics: []Metric{
				{Name: "sys.gc.count", Title: "GC Runs", NonNegativeRate: true},
			},
		},
		{
			Title:      "GC Pause Time",
			Tooltip:    "The amount of processor time used by Go's garbage collector per second. During garbage collection, application code execution is paused.",
			Units:      "DURATION",
			UnitsLabel: "pause time",
			Metrics: []Metric{
				{Name: "sys.gc.pause.ns", Title: "GC Pause Time", NonNegativeRate: true},
			},
		},
		{
			Title:      "CPU Time",
			Tooltip:    "The amount of CPU time used by CockroachDB (User) and system-level operations (Sys).",
			Units:      "DURATION",
			UnitsLabel: "cpu time",
			Metrics: []Metric{
				{Name: "sys.cpu.user.ns", Title: "User CPU Time", NonNegativeRate: true},
				{Name: "sys.cpu.sys.ns", Title: "Sys CPU Time", NonNegativeRate: true},
			},
		},
		{
			Title:      "Clock Offset",
			Tooltip:    "Mean clock offset of each node against the rest of the cluster.",
			Units:      "DURATION",
			UnitsLabel: "offset",
			Metrics: []Metric{
				{Name: "clock-offset.meannanos", PerNode: true},
			},
		},
	},
	"sql": {
		{
			Title:      "Open SQL Sessions",
			Tooltip:    "The total number of open SQL Sessions.",
			UnitsLabel: "connections",
			Metrics: []Metric{
				{Name: "sql.conns", PerNode: true, Downsampler: "MAX"},
			},
		},
		{
			Title:      "SQL Queries Per Second",
			Tooltip:    "A ten-second moving average of the # of SELECT, INSERT, UPDATE, and DELETE statements, and the sum of all four, successfully executed per second.",
			UnitsLabel: "queries",
			Metrics: []Metric{
				{Name: "sql.select.count", Title: "Selects", NonNegativeRate: true},
				{Name: "sql.update.count", Title: "Updates", NonNegativeRate: true},
				{Name: "sql.insert.count", Title: "Inserts", NonNegativeRate: true},
				{Name: "sql.delete.count", Title: "Deletes", NonNegativeRate: true},
				{Name: "sql.crud_query.count", Title: "Total Queries", NonNegativeRate: true},
			},
		},
		{
			Title:      "SQL Statement Errors",
			Tooltip:    "The number of statements which returned a planning, runtime, or client-side retry error.",
			UnitsLabel: "errors",
			Metrics: []Metric{
				{Name: "sql.failure.count", Title: "Errors", NonNegativeRate: true},
			},
		},
		{
			Title:      "Service Latency: SQL Statements, 99th percentile",
			Tooltip:    "Over the last minute, this node executed 99% of SQL statements within this time. This time only includes SELECT, INSERT, UPDATE and DELETE statements and does not include network latency between the node and client.",
			Units:      "DURATION",
			UnitsLabel: "latency",
			Metrics: []Metric{
				{Name: "sql.service.latency-p99", PerNode: true, Downsampler: "MAX"},
			},
		},
		{
			Title:      "Transactions",
			Tooltip:    "The total number of transactions initiated, committed, rolled back, or aborted per second.",
			UnitsLabel: "transactions",
			Metrics: []Metric{
				{Name: "sql.txn.begin.count", Title: "Begin", NonNegativeRate: true},
				{Name: "sql.txn.commit.count", Title: "Commits", NonNegativeRate: true},
				{Name: "sql.txn.rollback.count", Title: "Rollbacks", NonNegativeRate: true},
				{Name: "sql.txn.abort.count", Title: "Aborts", NonNegativeRate: true},
			},
		},
		{
			Title:      "Transaction Latency: 99th percentile",
			Tooltip:    "Over the last minute, this node executed 99% of transactions within this time. This time does not include network latency between the node and client.",
			Units:      "DURATION",
			UnitsLabel: "latency",
			Metrics: []Metric{
				{Name: "sql.txn.latency-p99", PerNode: true, Downsampler: "MAX"},
			},
		},
		{
			Title:      "SQL Memory",
			Tooltip:    "The current amount of allocated SQL memory. This amount is compared against the node's --max-sql-memory flag.",
			Units:      "BYTES",
			UnitsLabel: "allocated bytes",
			Metrics: []Metric{
				{Name: "sql.mem.root.current", PerNode: true, Downsampler: "MAX"},
			},
		},
		{
			Title:      "Schema Changes",
			Tooltip:    "The total number of DDL statements per second.",
			UnitsLabel: "statements",
			Metrics: []Metric{
				{Name: "sql.ddl.count", Title: "DDL Statements", NonNegativeRate: true},
			},
		},
	},
	"storage": {
		{
			Title:      "Capacity",
			Tooltip:    "Summary of total, available, and used capacity.",
			Units:      "BYTES",
			UnitsLabel: "capacity",
			IsKVGraph:  true,
			Metrics: []Metric{
				{Name: "capacity", Title: "Max"},
				{Name: "capacity.available", Title: "Available"},
				{Name: "capacity.used", Title: "Used"},
			},
		},
		{
			Title:      "Live Bytes",
			Tooltip:    "Number of logical bytes stored in live key-value pairs.",
			Units:      "BYTES",
			UnitsLabel: "live bytes",
			Metrics: []Metric{
				{Name: "livebytes", Title: "Live"},
				{Name: "sysbytes", Title: "System"},
			},
		},
		{
			Title:      "Log Commit Latency: 99th Percentile",
			Tooltip:    "The 99th %ile latency for commits to the Raft Log. This measures essentially an fdatasync to the storage engine's write-ahead log.",
			Units:      "DURATION",
			UnitsLabel: "latency",
			IsKVGraph:  true,
			Metrics: []Metric{
				{Name: "raft.process.logcommit.latency-p99", PerNode: true, PerStore: true, Aggregation: "MAX"},
			},
		},
		{
			Title:      "Command Commit Latency: 99th Percentile",
			Tooltip:    "The 99th %ile latency for commits of Raft commands. This measures applying a batch to the storage engine (including writes to the write-ahead log), but no fsync.",
			Units:      "DURATION",
			UnitsLabel: "latency",
			IsKVGraph:  true,
			Metrics: []Metric{
				{Name: "raft.process.commandcommit.latency-p99", PerNode: true, PerStore: true, Aggregation: "MAX"},
			},
		},
		{
			Title:      "Read Amplification",
			Tooltip:    "The average number of real read operations executed per logical read operation.",
			UnitsLabel: "factor",
			IsKVGraph:  true,
			Metrics: []Metric{
				{Name: "rocksdb.read-amplification", PerNode: true, PerStore: true, Aggregation: "AVG"},
			},
		},
		{
			Title:      "SSTables",
			Tooltip:    "The number of SSTables in use.",
			UnitsLabel: "sstables",
			IsKVGraph:  true,
			Metrics: []Metric{
				{Name: "rocksdb.num-sstables", PerNode: true, PerStore: true},
			},
		},
		{
			Title:      "File Descriptors",
			Tooltip:    "The number of open file descriptors, compared with the file descriptor limit.",
			UnitsLabel: "descriptors",
			IsKVGraph:  true,
			Metrics: []Metric{
				{Name: "sys.fd.open", Title: "Open"},
				{Name: "sys.fd.softlimit", Title: "Limit"},
			},
		},
		{
			Title:      "Compactions",
			Tooltip:    "Bytes written by compactions.",
			Units:      "BYTES",
			UnitsLabel: "written bytes",
			IsKVGraph:  true,
			Metrics: []Metric{
				{Name: "rocksdb.compacted-bytes-written", PerNode: true, PerStore: true, NonNegativeRate: true},
			},
		},
	},
	"replication": {
		{
			Title:      "Ranges",
			Tooltip:    "Various details about the status of ranges.",
			UnitsLabel: "ranges",
			Metrics: []Metric{
				{Name: "ranges", Title: "Ranges"},
				{Name: "replicas.leaders", Title: "Leaders"},
				{Name: "replicas.leaseholders", Title: "Lease Holders"},
				{Name: "replicas.leaders_not_leaseholders", Title: "Leaders w/o Lease"},
				{Name: "ranges.unavailable", Title: "Unavailable", Color: "#F16969"},
				{Name: "ranges.underreplicated", Title: "Under-replicated"},
				{Name: "ranges.overreplicated", Title: "Over-replicated"},
			},
		},
		{
			Title:      "Replicas per Node",
			Tooltip:    "The number of replicas on each node.",
			UnitsLabel: "replicas",
			Metrics: []Metric{
				{Name: "replicas", PerNode: true, PerStore: true},
			},
		},
		{
			Title:      "Leaseholders per Node",
			Tooltip:    "The number of leaseholder replicas on each node. A leaseholder replica is the one that receives and coordinates all read and write requests for its range.",
			UnitsLabel: "leaseholders",
			Metrics: []Metric{
				{Name: "replicas.leaseholders", PerNode: true, PerStore: true},
			},
		},
		{
			Title:      "Average Replica Queries per Node",
			Tooltip:    "Moving average of the number of KV batch requests processed by leaseholder replicas on each node per second. Tracks roughly the last 30 minutes of requests. Used for load-based rebalancing decisions.",
			UnitsLabel: "queries",
			Metrics: []Metric{
				{Name: "rebalancing.queriespersecond", PerNode: true, PerStore: true},
			},
		},
		{
			Title:      "Logical Bytes per Node",
			Tooltip:    "Number of logical bytes stored per node.",
			Units:      "BYTES",
			UnitsLabel: "logical store size",
			Metrics: []Metric{
				{Name: "totalbytes", PerNode: true, PerStore: true},
			},
		},
		{
			Title:      "Range Operations",
			UnitsLabel: "ranges",
			Metrics: []Metric{
				{Name: "range.splits", Title: "Splits", NonNegativeRate: true},
				{Name: "range.merges", Title: "Merges", NonNegativeRate: true},
				{Name: "range.adds", Title: "Adds", NonNegativeRate: true},
				{Name: "range.removes", Title: "Removes", NonNegativeRate: true},
				{Name: "leases.transfers.success", Title: "Lease Transfers", NonNegativeRate: true},
			},
		},
		{
			Title:      "Snapshots",
			UnitsLabel: "snapshots",
			Metrics: []Metric{
				{Name: "range.snapshots.generated", Title: "Generated", NonNegativeRate: true},
				{Name: "range.snapshots.applied-voter", Title: "Applied (Voters)", NonNegativeRate: true},
				{Name: "range.snapshots.applied-initial", Title: "Applied (Initial Upreplication)", NonNegativeRate: true},
				{Name: "range.snapshots.applied-non-voter", Title: "Applied (Non-Voters)", NonNegativeRate: true},
			},
		},
	},
	"distributed": {
		{
			Title:      "Batches",
			UnitsLabel: "batches",
			Metrics: []Metric{
				{Name: "distsender.batches", Title: "Batches", NonNegativeRate: true},
				{Name: "distsender.batches.partial", Title: "Partial Batches", NonNegativeRate: true},
			},
		},
		{
			Title:      "RPCs",
			UnitsLabel: "rpcs",
			Metrics: []Metric{
				{Name: "distsender.rpc.sent", Title: "RPCs Sent", NonNegativeRate: true},
				{Name: "distsender.rpc.sent.local", Title: "Local Fast-path", NonNegativeRate: true},
			},
		},
		{
			Title:      "RPC Errors",
			UnitsLabel: "errors",
			Metrics: []Metric{
				{Name: "distsender.rpc.sent.sendnexttimeout", Title: "RPC Timeouts", NonNegativeRate: true},
				{Name: "distsender.rpc.sent.nextreplicaerror", Title: "Replica Errors", NonNegativeRate: true},
				{Name: "distsender.errors.notleaseholder", Title: "Not Leaseholder Errors", NonNegativeRate: true},
			},
		},
		{
			Title:      "KV Transactions",
			UnitsLabel: "transactions",
			Metrics: []Metric{
				{Name: "txn.commits", Title: "Committed", NonNegativeRate: true},
				{Name: "txn.commits1PC", Title: "Fast-path Committed", NonNegativeRate: true},
				{Name: "txn.aborts", Title: "Aborted", NonNegativeRate: true},
			},
		},
		{
			Title:      "KV Transaction Durations: 99th percentile",
			Tooltip:    "The 99th percentile of transaction durations over a 1 minute period. Values are displayed individually for each node.",
			Units:      "DURATION",
			UnitsLabel: "transaction duration",
			Metrics: []Metric{
				{Name: "txn.durations-p99", PerNode: true, Downsampler: "MAX"},
			},
		},
		{
			Title:      "Node Heartbeat Latency: 99th percentile",
			Tooltip:    "The 99th percentile of latency to heartbeat a node's internal liveness record over a 1 minute period. Values are displayed individually for each node.",
			Units:      "DURATION",
			UnitsLabel: "heartbeat latency",
			Metrics: []Metric{
				{Name: "liveness.heartbeatlatency-p99", PerNode: true, Downsampler: "MAX"},
			},
		},
	},
	"queues": {
		{
			Title:      "Queue Processing Failures",
			Units:      "COUNT",
			UnitsLabel: "failures",
			Metrics: []Metric{
				{Name: "queue.gc.process.failure", Title: "GC", NonNegativeRate: true},
				{Name: "queue.replicagc.process.failure", Title: "Replica GC", NonNegativeRate: true},
				{Name: "queue.replicate.process.failure", Title: "Replication", NonNegativeRate: true},
				{Name: "queue.split.process.failure", Title: "Split", NonNegativeRate: true},
				{Name: "queue.merge.process.failure", Title: "Merge", NonNegativeRate: true},
			},
		},
		{
			Title:      "Replication Queue",
			Units:      "COUNT",
			UnitsLabel: "actions",
			Metrics: []Metric{
				{Name: "queue.replicate.process.success", Title: "Successful Actions / sec", NonNegativeRate: true},
				{Name: "queue.replicate.pending", Title: "Pending Actions"},
				{Name: "queue.replicate.addreplica", Title: "Replicas Added / sec", NonNegativeRate: true},
				{Name: "queue.replicate.removereplica", Title: "Replicas Removed / sec", NonNegativeRate: true},
			},
		},
		{
			Title:      "Split Queue",
			Units:      "COUNT",
			UnitsLabel: "actions",
			Metrics: []Metric{
				{Name: "queue.split.process.success", Title: "Successful Actions / sec", NonNegativeRate: true},
				{Name: "queue.split.pending", Title: "Pending Actions", Downsampler: "MAX"},
			},
		},
		{
			Title:      "Merge Queue",
			Units:      "COUNT",
			UnitsLabel: "actions",
			Metrics: []Metric{
				{Name: "queue.merge.process.success", Title: "Successful Actions / sec", NonNegativeRate: true},
				{Name: "queue.merge.pending", Title: "Pending Actions", Downsampler: "MAX"},
			},
		},
		{
			Title:      "MVCC GC Queue",
			Units:      "COUNT",
			UnitsLabel: "actions",
			Metrics: []Metric{
				{Name: "queue.gc.process.success", Title: "Successful Actions / sec", NonNegativeRate: true},
				{Name: "queue.gc.pending", Title: "Pending Actions", Downsampler: "MAX"},
			},
		},
	},
	"networking": {
		{
			Title:      "Network Bytes Sent",
			Units:      "BYTES",
			UnitsLabel: "bytes",
			Metrics: []Metric{
				{Name: "sys.host.net.send.bytes", PerNode: true, NonNegativeRate: true},
			},
		},
		{
			Title:      "Network Bytes Received",
			Units:      "BYTES",
			UnitsLabel: "bytes",
			Metrics: []Metric{
				{Name: "sys.host.net.recv.bytes", PerNode: true, NonNegativeRate: true},
			},
		},
		{
			Title:      "RPC Heartbeat Latency: 99th percentile",
			Tooltip:    "Round-trip latency for recent successful outgoing heartbeats.",
			Units:      "DURATION",
			UnitsLabel: "latency",
			Metrics: []Metric{
				{Name: "round-trip-latency-p99", PerNode: true, Downsampler: "MAX"},
			},
		},
	},
	"requests": {
		{
			Title:      "Slow Raft Proposals",
			UnitsLabel: "proposals",
			Metrics: []Metric{
				{Name: "requests.slow.raft", Title: "Slow Raft Proposals", Downsampler: "MAX"},
			},
		},
		{
			Title:      "Slow DistSender RPCs",
			UnitsLabel: "proposals",
			Metrics: []Metric{
				{Name: "requests.slow.distsender", Title: "Slow DistSender RPCs", Downsampler: "MAX"},
			},
		},
		{
			Title:      "Slow Lease Acquisitions",
			UnitsLabel: "lease acquisitions",
			Metrics: []Metric{
				{Name: "requests.slow.lease", Title: "Slow Lease Acquisitions", Downsampler: "MAX"},
			},
		},
		{
			Title:      "Slow Latch Acquisitions",
			UnitsLabel: "latch acquisitions",
			Metrics: []Metric{
				{Name: "requests.slow.latch", Title: "Slow Latch Acquisitions", Downsampler: "MAX"},
			},
		},
	},
	"overload": {
		{
			Title:      "CPU Utilization",
			Tooltip:    "CPU utilization of the CockroachDB process as measured by the host, displayed per node.",
			Units:      "PERCENTAGE",
			UnitsLabel: "CPU Utilization",
			Metrics: []Metric{
				{Name: "sys.cpu.combined.percent-normalized", PerNode: true},
			},
		},
		{
			Title:      "Goroutine Scheduling Latency: 99th percentile",
			Tooltip:    "P99 scheduling latency for goroutines. A value above 1ms here indicates high load that causes background (elastic) CPU work to be throttled.",
			Units:      "DURATION",
			UnitsLabel: "latency",
			Metrics: []Metric{
				{Name: "go.scheduler_latency-p99", PerNode: true, Downsampler: "MAX"},
			},
		},
		{
			Title:      "Runnable Goroutines per CPU",
			Tooltip:    "The number of Goroutines waiting per CPU. A value above the value set in admission.kv_slot_adjuster.overload_threshold (sampled at 1ms) is used by admission control to throttle regular CPU work.",
			UnitsLabel: "goroutines",
			Metrics: []Metric{
				{Name: "sys.runnable.goroutines.per.cpu", PerNode: true},
			},
		},
	},
	"changefeeds": {
		{
			Title:      "Changefeed Status",
			Units:      "COUNT",
			UnitsLabel: "count",
			Metrics: []Metric{
				{Name: "jobs.changefeed.currently_running", Title: "Running"},
				{Name: "jobs.changefeed.currently_paused", Title: "Paused"},
				{Name: "jobs.changefeed.resume_failed", Title: "Failed"},
			},
		},
		{
			Title:      "Commit Latency",
			Tooltip:    "The difference between an event's MVCC timestamp and the time it was acknowledged as received by the downstream sink.",
			Units:      "DURATION",
			UnitsLabel: "latency",
			Metrics: []Metric{
				{Name: "changefeed.commit_latency-p99", Title: "99th Percentile", Downsampler: "MAX", Aggregation: "MAX"},
				{Name: "changefeed.commit_latency-p90", Title: "90th Percentile", Downsampler: "MAX", Aggregation: "MAX"},
				{Name: "changefeed.commit_latency-p50", Title: "50th Percentile", Downsampler: "MAX", Aggregation: "MAX"},
			},
		},
		{
			Title:      "Emitted Bytes",
			Units:      "BYTES",
			UnitsLabel: "bytes",
			Metrics: []Metric{
				{Name: "changefeed.emitted_bytes", Title: "Emitted Bytes", NonNegativeRate: true},
			},
		},
		{
			Title:      "Max Checkpoint Lag",
			Tooltip:    "The most any changefeed's persisted checkpoint is behind the present. Larger values indicate issues with successfully ingesting or emitting changes.",
			Units:      "DURATION",
			UnitsLabel: "time",
			Metrics: []Metric{
				{Name: "changefeed.max_behind_nanos", Title: "Max Checkpoint Latency", Downsampler: "MAX", Aggregation: "MAX"},
			},
		},
	},
	"ttl": {
		{
			Title:      "Processing Rate",
			Units:      "COUNT",
			UnitsLabel: "rows per second",
			Metrics: []Metric{
				{Name: "jobs.row_level_ttl.rows_selected", Title: "rows selected", NonNegativeRate: true},
				{Name: "jobs.row_level_ttl.rows_deleted", Title: "rows deleted", NonNegativeRate: true},
			},
		},
		{
			Title:      "Job Latency",
			Tooltip:    "Latency of scanning and deleting within the job.",
			Units:      "DURATION",
			UnitsLabel: "latency",
			Metrics: []Metric{
				{Name: "jobs.row_level_ttl.select_duration-p99", Title: "scan latency (p99)", Downsampler: "MAX"},
				{Name: "jobs.row_level_ttl.delete_duration-p99", Title: "delete latency (p99)", Downsampler: "MAX"},
			},
		},
	},
}
