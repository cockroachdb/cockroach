package catalog

var charts = []chartDescription{
	{
		Organization: [][]string{{Process, "Build Info"}},
		Title:        "Timestamp",
		Downsampler:  DescribeAggregator_MAX,
		Percentiles:  false,
		Metrics:      []string{"build.timestamp"},
	},
	{
		Organization: [][]string{{Process, "Certificates"}},
		Title:        "CA Expiration",
		Downsampler:  DescribeAggregator_MAX,
		Percentiles:  false,
		Metrics:      []string{"security.certificate.expiration.ca"},
	},
	{
		Organization: [][]string{{Process, "Certificates"}},
		Title:        "Client CA Expiration",
		Downsampler:  DescribeAggregator_MAX,
		Percentiles:  false,
		Metrics:      []string{"security.certificate.expiration.client-ca"},
	},
	{
		Organization: [][]string{{Process, "Certificates"}},
		Title:        "Node Cert Expiration",
		Downsampler:  DescribeAggregator_MAX,
		Aggregator:   DescribeAggregator_MAX,
		Metrics:      []string{"security.certificate.expiration.node"},
	},
	{
		Organization: [][]string{{Process, "Certificates"}},
		Title:        "Node Client Cert Expiration",
		Downsampler:  DescribeAggregator_MAX,
		Aggregator:   DescribeAggregator_MAX,
		Metrics:      []string{"security.certificate.expiration.node-client"},
	},
	{
		Organization: [][]string{{Process, "Certificates"}},
		Title:        "UI CA Expiration",
		Downsampler:  DescribeAggregator_MAX,
		Aggregator:   DescribeAggregator_MAX,
		Metrics:      []string{"security.certificate.expiration.ui-ca"},
	},
	{
		Organization: [][]string{{Process, "Certificates"}},
		Title:        "UI Cert Expiration",
		Downsampler:  DescribeAggregator_MAX,
		Aggregator:   DescribeAggregator_MAX,
		Metrics:      []string{"security.certificate.expiration.ui"},
	},
	{
		Organization: [][]string{{Process, "Clocks"}},
		Title:        "Roundtrip Latency",
		Metrics:      []string{"round-trip-latency"},
	},
	{
		Organization: [][]string{{Process, "CPU"}},
		Title:        "Percentage",
		Metrics: []string{
			"sys.cpu.sys.percent",
			"sys.cpu.user.percent",
			"sys.cpu.combined.percent-normalized",
		},
	},
	{
		Organization: [][]string{{Process, "CPU"}},
		Title:        "Time",
		Metrics: []string{
			"sys.cpu.sys.ns",
			"sys.cpu.user.ns",
		},
	},
	{
		Organization: [][]string{{Process, "Network"}},
		Title:        "Packets",
		Metrics: []string{
			"sys.host.net.recv.packets",
			"sys.host.net.send.packets",
		},
	},
	{
		Organization: [][]string{{Process, "Network"}},
		Title:        "Size",
		Metrics: []string{
			"sys.host.net.recv.bytes",
			"sys.host.net.send.bytes",
		},
	},
	{
		Organization: [][]string{{Process, "Node"}},
		Title:        "ID",
		Downsampler:  DescribeAggregator_MAX,
		Percentiles:  false,
		Metrics:      []string{"node-id"},
	},
	{
		Organization: [][]string{{Process, "Server", "cgo"}},
		Title:        "Calls",
		Metrics:      []string{"sys.cgocalls"},
	},
	{
		Organization: [][]string{{Process, "Server", "cgo"}},
		Title:        "Memory",
		Metrics: []string{
			"sys.cgo.allocbytes",
			"sys.cgo.totalbytes",
		},
	},
	{
		Organization: [][]string{{Process, "Server", "go"}},
		Title:        "goroutines",
		Metrics:      []string{"sys.goroutines"},
	},
	{
		Organization: [][]string{{Process, "Server", "go"}},
		Title:        "Memory",
		Metrics: []string{
			"sys.go.allocbytes",
			"sys.go.totalbytes",
		},
	},
	{
		Organization: [][]string{{Process, "Server", "Overview"}},
		Title:        "File Descriptors (FD)",
		Metrics: []string{
			"sys.fd.open",
			"sys.fd.softlimit",
		},
	},
	{
		Organization: [][]string{{Process, "Server", "Overview"}},
		Title:        "RSS",
		Metrics:      []string{"sys.rss"},
	},
	{
		Organization: [][]string{{Process, "Server", "Overview"}},
		Title:        "Uptime",
		Metrics:      []string{"sys.uptime"},
	},
	{
		Organization: [][]string{
			{Process, "Server", "Disk"},
			{StorageLayer, "Host Disk"},
		},
		Title: "IOPS in Progress",
		Metrics: []string{
			"sys.host.disk.iopsinprogress",
		},
	},
	{
		Organization: [][]string{
			{Process, "Server", "Disk"},
			{StorageLayer, "Host Disk"},
		},
		Title: "Operations Count",
		Metrics: []string{
			"sys.host.disk.read.count",
			"sys.host.disk.write.count",
		},
	},
	{
		Organization: [][]string{
			{Process, "Server", "Disk"},
			{StorageLayer, "Host Disk"},
		},
		Title: "Operations Size",
		Metrics: []string{
			"sys.host.disk.read.bytes",
			"sys.host.disk.write.bytes",
		},
	},
	{
		Organization: [][]string{
			{Process, "Server", "Disk"},
			{StorageLayer, "Host Disk"},
		},
		Title: "Time",
		Metrics: []string{
			"sys.host.disk.io.time",
			"sys.host.disk.weightedio.time",
			"sys.host.disk.read.time",
			"sys.host.disk.write.time",
		},
	},
	{
		Organization: [][]string{{DistributionLayer, "DistSender"}},
		Title:        "Batches",
		Metrics: []string{
			"distsender.batches",
			"distsender.batches.partial",
			"distsender.batches.async.sent",
			"distsender.batches.async.throttled",
		},
		AxisLabel: "Batches",
	},
	{
		Organization: [][]string{{DistributionLayer, "RPC", "Heartbeats"}},
		Title:        "Overview",
		Metrics: []string{
			"rpc.heartbeats.initializing",
			"rpc.heartbeats.nominal",
			"rpc.heartbeats.failed",
		},
		AxisLabel: "Heartbeats",
	},
	{
		Organization: [][]string{{DistributionLayer, "RPC", "Heartbeats"}},
		Title:        "Loops",
		Metrics: []string{
			"rpc.heartbeats.loops.exited",
			"rpc.heartbeats.loops.started",
		},
		AxisLabel: "Heartbeat Loops",
	},
	{
		Organization: [][]string{{DistributionLayer, "DistSender"}},
		Title:        "Errors",
		Metrics: []string{
			"distsender.rpc.sent.nextreplicaerror",
			"distsender.errors.notleaseholder",
			"distsender.errors.inleasetransferbackoffs",
		},
		AxisLabel: "Error Count",
	},
	{
		Organization: [][]string{{DistributionLayer, "DistSender"}},
		Title:        "RPCs",
		Metrics: []string{
			"distsender.rpc.sent.local",
			"distsender.rpc.sent",
		},
	},
	{
		Organization: [][]string{{DistributionLayer, "Gossip"}},
		Title:        "Active Connections",
		Downsampler:  DescribeAggregator_MAX,
		Aggregator:   DescribeAggregator_MAX,
		Metrics: []string{
			"gossip.connections.incoming",
			"gossip.connections.outgoing",
		},
	},
	{
		Organization: [][]string{{DistributionLayer, "Gossip"}},
		Title:        "Bytes",
		Metrics: []string{
			"gossip.bytes.received",
			"gossip.bytes.sent",
		},
	},
	{
		Organization: [][]string{{DistributionLayer, "Gossip"}},
		Title:        "Infos",
		Metrics: []string{
			"gossip.infos.received",
			"gossip.infos.sent",
		},
	},
	{
		Organization: [][]string{{DistributionLayer, "Gossip"}},
		Title:        "Refused Connections",
		Downsampler:  DescribeAggregator_MAX,
		Aggregator:   DescribeAggregator_MAX,
		Metrics:      []string{"gossip.connections.refused"},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Merge Queue"},
			{ReplicationLayer, "Merge Queue"},
		},
		Title:       "Pending",
		Downsampler: DescribeAggregator_MAX,
		Percentiles: false,
		Metrics: []string{
			"queue.merge.pending",
			"queue.merge.purgatory",
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Merge Queue"},
			{ReplicationLayer, "Merge Queue"},
		},
		Title:       "Successes",
		Downsampler: DescribeAggregator_MAX,
		Percentiles: false,
		Metrics: []string{
			"queue.merge.process.failure",
			"queue.merge.process.success",
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Merge Queue"},
			{ReplicationLayer, "Merge Queue"},
		},
		Title:   "Time Spent",
		Metrics: []string{"queue.merge.processingnanos"},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Ranges"},
			{ReplicationLayer, "Ranges"},
		},
		Title: "Add, Split, Remove",
		Metrics: []string{
			"range.adds",
			"range.removes",
			"range.splits",
			"range.merges",
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Ranges"},
			{ReplicationLayer, "Ranges"},
		},
		Title: "Overview",
		Metrics: []string{
			"ranges",
			"ranges.unavailable",
			"ranges.underreplicated",
			"ranges.overreplicated",
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Ranges"},
			{ReplicationLayer, "Ranges"},
		},
		Title: "Rangefeed",
		Metrics: []string{
			"kv.rangefeed.catchup_scan_nanos",
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Ranges"},
			{ReplicationLayer, "Ranges"},
		},
		Title: "Snapshots",
		Metrics: []string{
			"range.snapshots.generated",
			"range.snapshots.normal-applied",
			"range.snapshots.preemptive-applied",
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Rebalancing"},
			{ReplicationLayer, "Leases"},
		},
		Title:   "Rebalancing Lease Transfers",
		Metrics: []string{"rebalancing.lease.transfers"},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Rebalancing"},
			{ReplicationLayer, "Ranges"},
		},
		Title:   "Range Rebalances",
		Metrics: []string{"rebalancing.range.rebalances"},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Rebalancing"},
		},
		Title:   "QPS",
		Metrics: []string{"rebalancing.queriespersecond"},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Split Queue"},
			{ReplicationLayer, "Split Queue"},
		},
		Title:       "Pending",
		Downsampler: DescribeAggregator_MAX,
		Percentiles: false,
		Metrics: []string{
			"queue.split.pending",
			"queue.split.purgatory",
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Split Queue"},
			{ReplicationLayer, "Split Queue"},
		},
		Title:       "Successes",
		Downsampler: DescribeAggregator_MAX,
		Percentiles: false,
		Metrics: []string{
			"queue.split.process.failure",
			"queue.split.process.success",
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Split Queue"},
			{ReplicationLayer, "Split Queue"},
		},
		Title:   "Time Spent",
		Metrics: []string{"queue.split.processingnanos"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Clocks"}},
		Title:        "Roundtrip Latency",
		Metrics:      []string{"round-trip-latency"},
	},
	{
		Organization: [][]string{
			{KVTransactionLayer, "Clocks"},
			{Process, "Clocks"},
		},
		Title: "Offsets",
		Metrics: []string{
			"clock-offset.meannanos",
			"clock-offset.stddevnanos",
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Follower Reads"}},
		Title:        "Closed Timestamp",
		Metrics:      []string{"kv.closed_timestamp.max_behind_nanos"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Follower Reads"}},
		Title:        "Count",
		Metrics:      []string{"follower_reads.success_count"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Title:        "AbortSpan",
		Metrics: []string{
			"queue.gc.info.abortspanconsidered",
			"queue.gc.info.abortspangcnum",
			"queue.gc.info.abortspanscanned",
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Title:        "Distinct Txns",
		Metrics:      []string{"queue.gc.info.intenttxns"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Title:        "Enteries in Txn Spans",
		Metrics:      []string{"queue.gc.info.transactionspanscanned"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Title:        "Intents",
		Metrics: []string{
			"queue.gc.info.resolvesuccess",
			"queue.gc.info.resolvetotal",
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Title:        "Keys with GC'able Data",
		Metrics:      []string{"queue.gc.info.numkeysaffected"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Title:        "Old Intents",
		Metrics:      []string{"queue.gc.info.intentsconsidered"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Title:        "Pushes",
		Metrics:      []string{"queue.gc.info.pushtxn"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Title:        "Txn Relationship",
		Metrics: []string{
			"queue.gc.info.transactionspangcaborted",
			"queue.gc.info.transactionspangccommitted",
			"queue.gc.info.transactionspangcpending",
			"queue.gc.info.transactionspangcstaging",
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Overview"}},
		Title:        "Current GC Pause Percent",
		Metrics:      []string{"sys.gc.pause.percent"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Overview"}},
		Title:        "Total GC Pause (NS)",
		Metrics:      []string{"sys.gc.pause.ns"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Overview"}},
		Title:        "Total GC Runs",
		Metrics:      []string{"sys.gc.count"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Requests", "Overview"}},
		Title:        "Latency",
		Metrics:      []string{"exec.latency"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Requests", "Overview"}},
		Title:        "Success",
		Downsampler:  DescribeAggregator_MAX,
		Rate:         DescribeDerivative_DERIVATIVE,
		Percentiles:  false,
		Metrics: []string{
			"exec.error",
			"exec.success",
		},
	},
	{
		Organization: [][]string{
			{KVTransactionLayer, "Requests", "Backpressure"},
			{ReplicationLayer, "Requests", "Backpressure"},
		},
		Title:       "Writes Waiting on Range Split",
		Downsampler: DescribeAggregator_MAX,
		Percentiles: false,
		Metrics:     []string{"requests.backpressure.split"},
	},
	{
		Organization: [][]string{
			{KVTransactionLayer, "Requests", "Slow"},
			{ReplicationLayer, "Requests", "Slow"},
		},
		Title:       "Latch",
		Downsampler: DescribeAggregator_MAX,
		Percentiles: false,
		Metrics:     []string{"requests.slow.latch"},
	},
	{
		Organization: [][]string{
			{KVTransactionLayer, "Requests", "Slow"},
			{ReplicationLayer, "Requests", "Slow"},
		},
		Title:       "Stuck Acquiring Lease",
		Downsampler: DescribeAggregator_MAX,
		Percentiles: false,
		Metrics:     []string{"requests.slow.lease"},
	},
	{
		Organization: [][]string{
			{KVTransactionLayer, "Requests", "Slow"},
			{ReplicationLayer, "Requests", "Slow"},
		},
		Title:       "Stuck in Raft",
		Downsampler: DescribeAggregator_MAX,
		Percentiles: false,
		Metrics:     []string{"requests.slow.raft"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Storage"}},
		Rate:         DescribeDerivative_DERIVATIVE,
		Percentiles:  false,
		Title:        "Metric Update Frequency",
		Metrics:      []string{"lastupdatenanos"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Storage"}},
		Title:        "Counts",
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
		Organization: [][]string{{KVTransactionLayer, "Storage"}},
		Title:        "Cumultative Age of Non-Live Data",
		Metrics:      []string{"gcbytesage"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Storage"}},
		Title:        "Cumultative Intent Age",
		Metrics:      []string{"intentage"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Storage"}},
		Title:        "Size",
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
		Organization: [][]string{{KVTransactionLayer, "Timestamp Cache"}},
		Title:        "Page Counts",
		Metrics: []string{
			"tscache.skl.read.pages",
			"tscache.skl.write.pages",
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Timestamp Cache"}},
		Title:        "Page Rotations",
		Metrics: []string{
			"tscache.skl.read.rotations",
			"tscache.skl.write.rotations",
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions", "Intents"}},
		Title:        "Intent Resolver",
		Metrics: []string{
			"intentresolver.async.throttled",
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions", "Intents"}},
		Title:        "Overview",
		Metrics: []string{
			"intents.abort-attempts",
			"intents.poison-attempts",
			"intents.resolve-attempts",
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions", "Savepoints"}},
		Title:        "Count",
		Metrics: []string{
			"sql.savepoint.count",
			"sql.savepoint.count.internal",
		},
		AxisLabel: "SQL Statements",
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions", "Savepoints"}},
		Title:        "Restarts (Internal)",
		Metrics: []string{
			"sql.restart_savepoint.count.internal",
			"sql.restart_savepoint.release.count.internal",
			"sql.restart_savepoint.rollback.count.internal",
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions", "Savepoints"}},
		Title:        "Restarts",
		Metrics: []string{
			"sql.restart_savepoint.count",
			"sql.restart_savepoint.release.count",
			"sql.restart_savepoint.rollback.count",
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions", "TxnWaitQueue"}},
		Title:        "Deadlocks",
		Metrics: []string{
			"txnwaitqueue.deadlocks_total",
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions", "TxnWaitQueue"}},
		Title:        "Slow Pusher",
		Metrics: []string{
			"txnwaitqueue.pusher.slow",
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions", "TxnWaitQueue"}},
		Title:        "Wait Time",
		Metrics: []string{
			"txnwaitqueue.pusher.wait_time",
			"txnwaitqueue.query.wait_time",
		},
		AxisLabel: "Wait Time",
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions", "TxnWaitQueue"}},
		Title:        "Waiting",
		Metrics: []string{
			"txnwaitqueue.pushee.waiting",
			"txnwaitqueue.pusher.waiting",
			"txnwaitqueue.query.waiting",
		},
		AxisLabel: "Actors",
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Title:        "Aborts",
		Metrics:      []string{"txn.aborts"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Title:        "Auto Retries",
		Metrics:      []string{"txn.autoretries"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Title:        "Commits",
		Metrics: []string{
			"txn.commits",
			"txn.commits1PC",
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Title:        "Durations",
		Metrics:      []string{"txn.durations"},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Title:        "Restart Cause Mix",
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
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Title:        "Restarts",
		Downsampler:  DescribeAggregator_MAX,
		Percentiles:  true,
		Metrics:      []string{"txn.restarts"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Title:        "Emitted Bytes",
		Metrics: []string{
			"changefeed.emitted_bytes",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Title:        "Emitted Messages",
		Metrics: []string{
			"changefeed.emitted_messages",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Title:        "Entries",
		Metrics: []string{
			"changefeed.buffer_entries.in",
			"changefeed.buffer_entries.out",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Title:        "Errors",
		Metrics: []string{
			"changefeed.error_retries",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Title:        "Flushes",
		Metrics: []string{
			"changefeed.flushes",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Title:        "Max Behind Nanos",
		Metrics: []string{
			"changefeed.max_behind_nanos",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Title:        "Min High Water",
		Metrics: []string{
			"changefeed.min_high_water",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Title:        "Poll Request Time",
		Metrics: []string{
			"changefeed.poll_request_nanos",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Title:        "Total Time Spent",
		Metrics: []string{
			"changefeed.emit_nanos",
			"changefeed.flush_nanos",
			"changefeed.processing_nanos",
			"changefeed.table_metadata_nanos",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Consistency Checker Queue"}},
		Title:        "Pending",
		Metrics:      []string{"queue.consistency.pending"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Consistency Checker Queue"}},
		Title:        "Successes",
		Metrics: []string{
			"queue.consistency.process.failure",
			"queue.consistency.process.success",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Consistency Checker Queue"}},
		Title:        "Time Spent",
		Metrics:      []string{"queue.consistency.processingnanos"},
	},
	{
		Organization: [][]string{
			{ReplicationLayer, "Garbage Collection"},
			{StorageLayer, "Garbage Collection"},
		},
		Title:   "Queue Pending",
		Metrics: []string{"queue.gc.pending"},
	},
	{
		Organization: [][]string{
			{ReplicationLayer, "Garbage Collection"},
			{StorageLayer, "Garbage Collection"},
		},
		Title: "Queue Success",
		Metrics: []string{
			"queue.gc.process.failure",
			"queue.gc.process.success",
		},
	},
	{
		Organization: [][]string{
			{ReplicationLayer, "Garbage Collection"},
			{StorageLayer, "Garbage Collection"},
		},
		Title:   "Queue Time",
		Metrics: []string{"queue.gc.processingnanos"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Leases"}},
		Title:        "Stuck Acquisition Count",
		Metrics:      []string{"requests.slow.lease"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Leases"}},
		Title:        "Succcess Rate",
		Metrics: []string{
			"leases.error",
			"leases.success",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Leases"}},
		Title:        "Total",
		Metrics: []string{
			"leases.epoch",
			"leases.expiration",
			"replicas.leaseholders",
			"replicas.leaders_not_leaseholders",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Leases"}},
		Title:        "Transfer Success Rate",
		Metrics: []string{
			"leases.transfers.error",
			"leases.transfers.success",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Node Liveness"}},
		Title:        "Epoch Increment Count",
		Metrics:      []string{"liveness.epochincrements"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Node Liveness"}},
		Title:        "Heartbeat Latency",
		Metrics:      []string{"liveness.heartbeatlatency"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Node Liveness"}},
		Title:        "Heartbeats Success",
		Metrics: []string{
			"liveness.heartbeatfailures",
			"liveness.heartbeatsuccesses",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Node Liveness"}},
		Title:        "Node Count",
		Downsampler:  DescribeAggregator_MAX,
		Percentiles:  false,
		Metrics:      []string{"liveness.livenodes"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Entry Cache"}},
		Title:        "Entries",
		Metrics: []string{
			"raft.entrycache.size",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Entry Cache"}},
		Title:        "Hits",
		Metrics: []string{
			"raft.entrycache.accesses",
			"raft.entrycache.hits",
		},
		AxisLabel: "Entry Cache Operations",
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Entry Cache"}},
		Title:        "Size",
		Metrics: []string{
			"raft.entrycache.bytes",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Heartbeats"}},
		Title:        "MsgHeartbeatResp Count",
		Metrics:      []string{"raft.rcvd.heartbeatresp"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Heartbeats"}},
		Title:        "MsgTransferLeader Count",
		Metrics:      []string{"raft.rcvd.transferleader"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Heartbeats"}},
		Title:        "Pending",
		Metrics:      []string{"raft.heartbeats.pending"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Latency"}},
		Title:        "Apply Committed",
		Metrics:      []string{"raft.process.applycommitted.latency"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Latency"}},
		Title:        "Command Commit",
		Metrics:      []string{"raft.process.commandcommit.latency"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Latency"}},
		Title:        "Handle Ready",
		Metrics:      []string{"raft.process.handleready.latency"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Latency"}},
		Title:        "Log Commit",
		Metrics:      []string{"raft.process.logcommit.latency"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Log"}},
		Title:        "Entries Truncated",
		Metrics:      []string{"raftlog.truncated"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Log"}},
		Title:        "Followers Behind By...",
		Metrics:      []string{"raftlog.behind"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Title:        "Commands Count",
		Metrics:      []string{"raft.commandsapplied"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Title:        "Enqueued",
		Metrics:      []string{"raft.enqueued.pending"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Title:        "Keys/Sec Avg.",
		Metrics:      []string{"rebalancing.writespersecond"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Title:        "Leader Transfers",
		Metrics:      []string{"range.raftleadertransfers"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Title:        "Leaders",
		Metrics:      []string{"replicas.leaders"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Title:        "Stuck Request Count",
		Metrics:      []string{"requests.slow.raft"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Title:        "Ticks Queued",
		Metrics:      []string{"raft.ticks"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Title:        "Working vs. Ticking TIme",
		Metrics: []string{
			"raft.process.tickingnanos",
			"raft.process.workingnanos",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Queues"}},
		Title:        "Log Pending",
		Metrics:      []string{"queue.raftlog.pending"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Queues"}},
		Title:        "Log Processing Time Spent",
		Metrics:      []string{"queue.raftlog.processingnanos"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Queues"}},
		Title:        "Log Successes",
		Metrics: []string{
			"queue.raftlog.process.failure",
			"queue.raftlog.process.success",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Queues"}},
		Title:        "Snapshot Processing Time Spent",
		Metrics:      []string{"queue.raftsnapshot.processingnanos"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Queues"}},
		Title:        "Snapshot Successes",
		Metrics: []string{
			"queue.raftsnapshot.process.failure",
			"queue.raftsnapshot.process.success",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Queues"}},
		Title:        "Snapshots Pending",
		Metrics:      []string{"queue.raftsnapshot.pending"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Title:        "Dropped",
		Metrics:      []string{"raft.rcvd.dropped"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Title:        "Heartbeat Count",
		Metrics:      []string{"raft.rcvd.heartbeat"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Title:        "MsgApp Count",
		Metrics:      []string{"raft.rcvd.app"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Title:        "MsgAppResp Count",
		Metrics:      []string{"raft.rcvd.appresp"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Title:        "MsgHeartbeatResp Count",
		Metrics:      []string{"raft.rcvd.heartbeatresp"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Title:        "MsgPreVote Count",
		Metrics:      []string{"raft.rcvd.prevote"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Title:        "MsgPreVoteResp Count",
		Metrics:      []string{"raft.rcvd.prevoteresp"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Title:        "MsgProp Count",
		Metrics:      []string{"raft.rcvd.prop"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Title:        "MsgSnap Count",
		Metrics:      []string{"raft.rcvd.snap"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Title:        "MsgTimeoutNow Count",
		Metrics:      []string{"raft.rcvd.timeoutnow"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Title:        "MsgTransferLeader Count",
		Metrics:      []string{"raft.rcvd.transferleader"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Title:        "MsgVote Count",
		Metrics:      []string{"raft.rcvd.vote"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Title:        "MsgVoteResp Count",
		Metrics:      []string{"raft.rcvd.voteresp"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Ranges"}},
		Title:        "Backpressued Writes Waiting on Split",
		Metrics:      []string{"requests.backpressure.split"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Ranges"}},
		Title:        "Raft Leader Transfers",
		Metrics:      []string{"range.raftleadertransfers"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replica GC Queue"}},
		Title:        "Pending",
		Metrics:      []string{"queue.replicagc.pending"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replica GC Queue"}},
		Title:        "Removal Count",
		Metrics:      []string{"queue.replicagc.removereplica"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replica GC Queue"}},
		Title:        "Successes",
		Metrics: []string{
			"queue.replicagc.process.failure",
			"queue.replicagc.process.success",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replica GC Queue"}},
		Title:        "Time Spent",
		Metrics:      []string{"queue.replicagc.processingnanos"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replicas", "Overview"}},
		Title:        "Count",
		Metrics: []string{
			"replicas.quiescent",
			"replicas",
			"replicas.reserved",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replicas", "Overview"}},
		Title:        "Leaseholders",
		Metrics:      []string{"replicas.leaseholders"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Title:        "Add Replica Count",
		Metrics:      []string{"queue.replicate.addreplica"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Title:        "Lease Transfer Count",
		Metrics:      []string{"queue.replicate.transferlease"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Title:        "Pending",
		Metrics:      []string{"queue.replicate.pending"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Title:        "Purgatory",
		Metrics:      []string{"queue.replicate.purgatory"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Title:        "Reblance Count",
		Metrics:      []string{"queue.replicate.rebalancereplica"},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Title:        "Remove Replica Count",
		Metrics: []string{
			"queue.replicate.removedeadreplica",
			"queue.replicate.removereplica",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Title:        "Successes",
		Metrics: []string{
			"queue.replicate.process.failure",
			"queue.replicate.process.success",
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Title:        "Time Spent",
		Metrics:      []string{"queue.replicate.processingnanos"},
	},
	{
		Organization: [][]string{{SQLLayer, "DistSQL", "Flows"}},
		Title:        "Active",
		Metrics:      []string{"sql.distsql.flows.active"},
	},
	{
		Organization: [][]string{{SQLLayer, "DistSQL", "Flows"}},
		Title:        "Queue Wait",
		Metrics:      []string{"sql.distsql.flows.queue_wait"},
	},
	{
		Organization: [][]string{{SQLLayer, "DistSQL", "Flows"}},
		Title:        "Queued",
		Metrics:      []string{"sql.distsql.flows.queued"},
	},
	{
		Organization: [][]string{{SQLLayer, "DistSQL", "Flows"}},
		Title:        "Total",
		Metrics:      []string{"sql.distsql.flows.total"},
	},
	{
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Title:        "Active Queries",
		Metrics:      []string{"sql.distsql.queries.active"},
	},
	{
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Title:        "Current Memory Usage",
		Metrics:      []string{"sql.mem.distsql.current"},
	},
	{
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Title:        "DML Mix",
		Metrics: []string{
			"sql.distsql.select.count",
			"sql.distsql.select.count.internal",
		},
		AxisLabel: "SQL Statements",
	},
	{
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Title:        "Exec Latency",
		Metrics: []string{
			"sql.distsql.exec.latency",
			"sql.distsql.exec.latency.internal",
		},
		AxisLabel: "Latency",
	},
	{
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Title:        "Memory Usage per Statement",
		Metrics:      []string{"sql.mem.distsql.max"},
	},
	{
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Title:        "Service Latency",
		Metrics: []string{
			"sql.distsql.service.latency",
			"sql.distsql.service.latency.internal",
		},
		AxisLabel: "Latency",
	},
	{
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Title:        "Total Queries",
		Metrics:      []string{"sql.distsql.queries.total"},
	},
	{
		Organization: [][]string{{SQLLayer, "Optimizer"}},
		Title:        "Count",
		Metrics: []string{
			"sql.optimizer.count",
			"sql.optimizer.count.internal",
		},
		AxisLabel: "SQL Statements",
	},
	{
		Organization: [][]string{{SQLLayer, "Optimizer"}},
		Title:        "Fallback",
		Metrics: []string{
			"sql.optimizer.fallback.count",
			"sql.optimizer.fallback.count.internal",
		},
		AxisLabel: "Fallbacks",
	},
	{
		Organization: [][]string{{SQLLayer, "Optimizer"}},
		Title:        "Plan Cache",
		Metrics: []string{
			"sql.optimizer.plan_cache.hits",
			"sql.optimizer.plan_cache.hits.internal",
			"sql.optimizer.plan_cache.misses",
			"sql.optimizer.plan_cache.misses.internal",
		},
		AxisLabel: "Plane Cache Accesses",
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Admin"}},
		Title:        "All",
		Metrics:      []string{"sql.mem.admin.max"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Admin"}},
		Title:        "Current",
		Metrics:      []string{"sql.mem.admin.current"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Admin"}},
		Title:        "Session All",
		Metrics:      []string{"sql.mem.admin.session.max"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Admin"}},
		Title:        "Session Current",
		Metrics:      []string{"sql.mem.admin.session.current"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Admin"}},
		Title:        "Txn All",
		Metrics:      []string{"sql.mem.admin.txn.max"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Admin"}},
		Title:        "Txn Current",
		Metrics:      []string{"sql.mem.admin.txn.current"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Title:        "All",
		Metrics:      []string{"sql.mem.conns.max"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Title:        "Current",
		Metrics:      []string{"sql.mem.conns.current"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Title:        "Current",
		Metrics:      []string{"sql.mem.conns.current"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Title:        "Session All",
		Metrics:      []string{"sql.mem.conns.session.max"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Title:        "Session Current",
		Metrics:      []string{"sql.mem.conns.session.current"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Title:        "Txn All",
		Metrics:      []string{"sql.mem.conns.txn.max"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Title:        "Txn Current",
		Metrics:      []string{"sql.mem.conns.txn.current"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Title:        "All",
		Metrics:      []string{"sql.mem.internal.max"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Title:        "Current",
		Metrics:      []string{"sql.mem.internal.current"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Title:        "Current",
		Metrics:      []string{"sql.mem.internal.current"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Title:        "Session All",
		Metrics:      []string{"sql.mem.internal.session.max"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Title:        "Session Current",
		Metrics:      []string{"sql.mem.internal.session.current"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Title:        "Txn All",
		Metrics:      []string{"sql.mem.internal.txn.max"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Title:        "Txn Current",
		Metrics:      []string{"sql.mem.internal.txn.current"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL Session"}},
		Title:        "Current",
		Metrics:      []string{"sql.mem.sql.session.current"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL Session"}},
		Title:        "Max",
		Metrics:      []string{"sql.mem.sql.session.max"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL Txn"}},
		Title:        "Current",
		Metrics:      []string{"sql.mem.sql.txn.current"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL Txn"}},
		Title:        "Max",
		Metrics:      []string{"sql.mem.sql.txn.max"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL"}},
		Title:        "Current",
		Metrics:      []string{"sql.mem.sql.current"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL"}},
		Title:        "Max",
		Metrics:      []string{"sql.mem.sql.max"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL"}},
		Title:        "Byte I/O",
		Metrics: []string{
			"sql.bytesin",
			"sql.bytesout",
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL"}},
		Title:        "Connections",
		Metrics:      []string{"sql.conns"},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL"}},
		Title:        "DDL Count",
		Metrics: []string{
			"sql.ddl.count",
			"sql.query.count",
			"sql.ddl.count.internal",
		},
		AxisLabel: "SQL Statements",
	},
	{
		Organization: [][]string{{SQLLayer, "SQL"}},
		Title:        "DML Mix (Internal)",
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
		Organization: [][]string{{SQLLayer, "SQL"}},
		Title:        "DML Mix",
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
		Organization: [][]string{{SQLLayer, "SQL"}},
		Title:        "Exec Latency",
		Metrics: []string{
			"sql.exec.latency",
			"sql.exec.latency.internal",
		},
		AxisLabel: "Latency",
	},
	{
		Organization: [][]string{{SQLLayer, "SQL"}},
		Title:        "Service Latency",
		Metrics: []string{
			"sql.service.latency",
			"sql.service.latency.internal",
		},
		AxisLabel: "Latency",
	},
	{
		Organization: [][]string{{SQLLayer, "SQL"}},
		Title:        "Transaction Control Mix (Internal)",
		Metrics: []string{
			"sql.txn.abort.count.internal",
			"sql.txn.begin.count.internal",
			"sql.txn.commit.count.internal",
			"sql.txn.rollback.count.internal",
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL"}},
		Title:        "Transaction Control Mix",
		Metrics: []string{
			"sql.txn.abort.count",
			"sql.txn.begin.count",
			"sql.txn.commit.count",
			"sql.txn.rollback.count",
		},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "Block Cache"}},
		Title:        "Size",
		Metrics: []string{
			"rocksdb.block.cache.pinned-usage",
			"rocksdb.block.cache.usage",
		},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "Block Cache"}},
		Title:        "Success",
		Metrics: []string{
			"rocksdb.block.cache.hits",
			"rocksdb.block.cache.misses",
		},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "Encryption at Rest"}},
		Title:        "Algorithm Enum",
		Metrics:      []string{"rocksdb.encryption.algorithm"},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "Overview"}},
		Title:        "Bloom Filter",
		Metrics: []string{
			"rocksdb.bloom.filter.prefix.checked",
			"rocksdb.bloom.filter.prefix.useful",
		},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "Overview"}},
		Title:        "Compactions",
		Metrics:      []string{"rocksdb.compactions"},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "Overview"}},
		Title:        "Flushes",
		Metrics:      []string{"rocksdb.flushes"},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "Overview"}},
		Title:        "Index & Filter Block Size",
		Metrics:      []string{"rocksdb.table-readers-mem-estimate"},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "Overview"}},
		Title:        "Memtable",
		Metrics:      []string{"rocksdb.memtable.total-size"},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "Overview"}},
		Title:        "Read Amplification",
		Metrics:      []string{"rocksdb.read-amplification"},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "SSTables"}},
		Title:        "Count",
		Metrics:      []string{"rocksdb.num-sstables"},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "SSTables"}},
		Title:        "Ingestions",
		Metrics: []string{
			"addsstable.copies",
			"addsstable.applications",
			"addsstable.proposals",
		},
	},
	{
		Organization: [][]string{{StorageLayer, "Storage", "Compactor"}},
		Title:        "Overview",
		Metrics: []string{
			"compactor.suggestionbytes.compacted",
			"compactor.suggestionbytes.skipped",
		},
	},
	{
		Organization: [][]string{{StorageLayer, "Storage", "Compactor"}},
		Title:        "Queued",
		Metrics: []string{
			"compactor.suggestionbytes.queued",
		},
	},
	{
		Organization: [][]string{{StorageLayer, "Storage", "Compactor"}},
		Title:        "Success",
		Metrics: []string{
			"compactor.compactions.failure",
			"compactor.compactions.success",
		},
	},
	{
		Organization: [][]string{{StorageLayer, "Storage", "Compactor"}},
		Title:        "Time",
		Metrics:      []string{"compactor.compactingnanos"},
	},
	{
		Organization: [][]string{{StorageLayer, "Storage", "KV"}},
		Title:        "Counts",
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
		Organization: [][]string{{StorageLayer, "Storage", "KV"}},
		Title:        "Cumultative Age of Non-Live Data",
		Metrics:      []string{"gcbytesage"},
	},
	{
		Organization: [][]string{{StorageLayer, "Storage", "KV"}},
		Title:        "Cumultative Intent Age",
		Metrics:      []string{"intentage"},
	},
	{
		Organization: [][]string{{StorageLayer, "Storage", "KV"}},
		Title:        "Metric Update Frequency",
		Metrics:      []string{"lastupdatenanos"},
	},
	{
		Organization: [][]string{{StorageLayer, "Storage", "KV"}},
		Title:        "Size",
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
		Organization: [][]string{{StorageLayer, "Storage", "Overview"}},
		Title:        "Capacity",
		Metrics: []string{
			"capacity.available",
			"capacity",
			"capacity.reserved",
			"capacity.used",
		},
	},
	{
		Organization: [][]string{{Timeseries, "Maintenance Queue"}},
		Title:        "Pending",
		Metrics:      []string{"queue.tsmaintenance.pending"},
	},
	{
		Organization: [][]string{{Timeseries, "Maintenance Queue"}},
		Title:        "Successes",
		Metrics: []string{
			"queue.tsmaintenance.process.success",
			"queue.tsmaintenance.process.failure",
		},
	},
	{
		Organization: [][]string{{Timeseries, "Maintenance Queue"}},
		Title:        "Time Spent",
		Metrics:      []string{"queue.tsmaintenance.processingnanos"},
	},
	{
		Organization: [][]string{{Timeseries, "Overview"}},
		Title:        "Count",
		Metrics:      []string{"timeseries.write.samples"},
	},
	{
		Organization: [][]string{{Timeseries, "Overview"}},
		Title:        "Error Count",
		Metrics:      []string{"timeseries.write.errors"},
	},
	{
		Organization: [][]string{{Timeseries, "Overview"}},
		Title:        "Size",
		Metrics:      []string{"timeseries.write.bytes"},
	},
}
