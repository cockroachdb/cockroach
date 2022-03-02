// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

// chart_catalog.go represents a catalog of pre-defined DB Console charts
// to aid users in debugging CockroachDB clusters. This file represents
// a simplified structure of the catalog, meant to make it easier for
// developers to add charts to the catalog. You can find more detail at
// pkg/ts/catalog/catalog_generator.go.

// The structure of the catalog is not rigorously defined and should be
// iterated on as it's used.

// A few notes:
// - When adding sectionDescriptions...
//		- The first element of the organization field's inner array values
//       must be one of the consts defined in catalog_generator.go.
//		- All of the inner arrays must have either 2 or 3 levels.
// - When adding chartDescriptions...
//		Undefined values in chartDescriptions will be completed using default
//		values based on the type of metrics used in the chart
//		(chartDefaultsPerMetricType).

var charts = []sectionDescription{
	{
		Organization: [][]string{{Process, "Build Info"}},
		Charts: []chartDescription{
			{
				Title:       "Timestamp",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"build.timestamp"},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Certificates"}},
		Charts: []chartDescription{
			{
				Title:       "CA Expiration",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"security.certificate.expiration.ca"},
			},
			{
				Title:       "Client CA Expiration",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"security.certificate.expiration.client-ca"},
			},
			{
				Title:       "Node Cert Expiration",
				Downsampler: DescribeAggregator_MAX,
				Aggregator:  DescribeAggregator_MAX,
				Metrics:     []string{"security.certificate.expiration.node"},
			},
			{
				Title:       "Node Client Cert Expiration",
				Downsampler: DescribeAggregator_MAX,
				Aggregator:  DescribeAggregator_MAX,
				Metrics:     []string{"security.certificate.expiration.node-client"},
			},
			{
				Title:       "UI Cert Expiration",
				Downsampler: DescribeAggregator_MAX,
				Aggregator:  DescribeAggregator_MAX,
				Metrics:     []string{"security.certificate.expiration.ui"},
			},
			{
				Title:       "UI CA Cert Expiration",
				Downsampler: DescribeAggregator_MAX,
				Aggregator:  DescribeAggregator_MAX,
				Metrics:     []string{"security.certificate.expiration.ui-ca"},
			},
			{
				Title:       "Tenant Client CA Cert Expiration",
				Downsampler: DescribeAggregator_MAX,
				Aggregator:  DescribeAggregator_MAX,
				Metrics:     []string{"security.certificate.expiration.ca-client-tenant"},
			},
			{
				Title:       "Tenant Client Cert Expiration",
				Downsampler: DescribeAggregator_MAX,
				Aggregator:  DescribeAggregator_MAX,
				Metrics:     []string{"security.certificate.expiration.client-tenant"},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Clocks"}},
		Charts: []chartDescription{
			{
				Title:   "Roundtrip Latency",
				Metrics: []string{"round-trip-latency"},
			},
		},
	},
	{
		Organization: [][]string{{Process, "CPU"}},
		Charts: []chartDescription{
			{
				Title: "Percentage",
				Metrics: []string{
					"sys.cpu.sys.percent",
					"sys.cpu.user.percent",
					"sys.cpu.combined.percent-normalized",
				},
			},
			{
				Title: "Time",
				Metrics: []string{
					"sys.cpu.sys.ns",
					"sys.cpu.user.ns",
					"sys.cpu.now.ns",
				},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Network"}},
		Charts: []chartDescription{
			{
				Title: "Packets",
				Metrics: []string{
					"sys.host.net.recv.packets",
					"sys.host.net.send.packets",
				},
			},
			{
				Title: "Size",
				Metrics: []string{
					"sys.host.net.recv.bytes",
					"sys.host.net.send.bytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Node"}},
		Charts: []chartDescription{
			{
				Title:       "ID",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"node-id"},
			},
			{
				Title:       "License TTL",
				Downsampler: DescribeAggregator_MIN,
				Percentiles: false,
				Metrics:     []string{"seconds_until_enterprise_license_expiry"},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Server", "cgo"}},
		Charts: []chartDescription{
			{
				Title:   "Calls",
				Metrics: []string{"sys.cgocalls"},
			},
			{
				Title: "Memory",
				Metrics: []string{
					"sys.cgo.allocbytes",
					"sys.cgo.totalbytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Server", "go"}},
		Charts: []chartDescription{
			{
				Title:   "goroutines",
				Metrics: []string{"sys.goroutines", "sys.runnable.goroutines.per.cpu"},
			},
			{
				Title: "Memory",
				Metrics: []string{
					"sys.go.allocbytes",
					"sys.go.totalbytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Server", "Overview"}},
		Charts: []chartDescription{
			{
				Title: "File Descriptors (FD)",
				Metrics: []string{
					"sys.fd.open",
					"sys.fd.softlimit",
				},
			},
			{
				Title:   "RSS",
				Metrics: []string{"sys.rss"},
			},
			{
				Title:   "Uptime",
				Metrics: []string{"sys.uptime"},
			},
		},
	},
	{
		Organization: [][]string{
			{Process, "Server", "Disk"},
			{StorageLayer, "Host Disk"},
		},
		Charts: []chartDescription{
			{
				Title: "IOPS in Progress",
				Metrics: []string{
					"sys.host.disk.iopsinprogress",
				},
			},
			{
				Title: "Operations Count",
				Metrics: []string{
					"sys.host.disk.read.count",
					"sys.host.disk.write.count",
				},
			},
			{
				Title: "Operations Size",
				Metrics: []string{
					"sys.host.disk.read.bytes",
					"sys.host.disk.write.bytes",
				},
			},
			{
				Title: "Time",
				Metrics: []string{
					"sys.host.disk.io.time",
					"sys.host.disk.weightedio.time",
					"sys.host.disk.read.time",
					"sys.host.disk.write.time",
				},
			},
		},
	},
	{
		Organization: [][]string{{DistributionLayer, "DistSender"}},
		Charts: []chartDescription{
			{
				Title: "Batches",
				Metrics: []string{
					"distsender.batches",
					"distsender.batches.partial",
					"distsender.batches.async.sent",
					"distsender.batches.async.throttled",
				},
				AxisLabel: "Batches",
			},
			{
				Title: "Errors",
				Metrics: []string{
					"distsender.rpc.sent.nextreplicaerror",
					"distsender.errors.notleaseholder",
					"distsender.errors.inleasetransferbackoffs",
				},
				AxisLabel: "Error Count",
			},
			{
				Title: "Range Lookups",
				Metrics: []string{
					"distsender.rangelookups",
				},
			},
			{
				Title: "RPCs",
				Metrics: []string{
					"distsender.rpc.sent.local",
					"distsender.rpc.sent",
				},
			},
			{
				Title: "Batch RPCs Received",
				Metrics: []string{
					"rpc.batches.recv",
				},
			},
			{
				Title: "Batch Requests Received",
				Metrics: []string{
					"rpc.method.addsstable.recv",
					"rpc.method.adminchangereplicas.recv",
					"rpc.method.adminmerge.recv",
					"rpc.method.adminrelocaterange.recv",
					"rpc.method.adminscatter.recv",
					"rpc.method.adminsplit.recv",
					"rpc.method.admintransferlease.recv",
					"rpc.method.adminunsplit.recv",
					"rpc.method.adminverifyprotectedtimestamp.recv",
					"rpc.method.barrier.recv",
					"rpc.method.checkconsistency.recv",
					"rpc.method.clearrange.recv",
					"rpc.method.computechecksum.recv",
					"rpc.method.conditionalput.recv",
					"rpc.method.delete.recv",
					"rpc.method.deleterange.recv",
					"rpc.method.endtxn.recv",
					"rpc.method.export.recv",
					"rpc.method.gc.recv",
					"rpc.method.get.recv",
					"rpc.method.heartbeattxn.recv",
					"rpc.method.import.recv",
					"rpc.method.increment.recv",
					"rpc.method.initput.recv",
					"rpc.method.leaseinfo.recv",
					"rpc.method.merge.recv",
					"rpc.method.migrate.recv",
					"rpc.method.probe.recv",
					"rpc.method.pushtxn.recv",
					"rpc.method.put.recv",
					"rpc.method.queryintent.recv",
					"rpc.method.queryresolvedtimestamp.recv",
					"rpc.method.querytxn.recv",
					"rpc.method.rangestats.recv",
					"rpc.method.recomputestats.recv",
					"rpc.method.recovertxn.recv",
					"rpc.method.refresh.recv",
					"rpc.method.refreshrange.recv",
					"rpc.method.requestlease.recv",
					"rpc.method.resolveintent.recv",
					"rpc.method.resolveintentrange.recv",
					"rpc.method.reversescan.recv",
					"rpc.method.revertrange.recv",
					"rpc.method.scan.recv",
					"rpc.method.scaninterleavedintents.recv",
					"rpc.method.subsume.recv",
					"rpc.method.transferlease.recv",
					"rpc.method.truncatelog.recv",
					"rpc.method.writebatch.recv",
				},
			},
			{
				Title: "Requests",
				Metrics: []string{
					"distsender.rpc.addsstable.sent",
					"distsender.rpc.adminchangereplicas.sent",
					"distsender.rpc.adminmerge.sent",
					"distsender.rpc.adminrelocaterange.sent",
					"distsender.rpc.adminscatter.sent",
					"distsender.rpc.adminsplit.sent",
					"distsender.rpc.admintransferlease.sent",
					"distsender.rpc.adminunsplit.sent",
					"distsender.rpc.adminverifyprotectedtimestamp.sent",
					"distsender.rpc.barrier.sent",
					"distsender.rpc.checkconsistency.sent",
					"distsender.rpc.clearrange.sent",
					"distsender.rpc.computechecksum.sent",
					"distsender.rpc.conditionalput.sent",
					"distsender.rpc.delete.sent",
					"distsender.rpc.deleterange.sent",
					"distsender.rpc.endtxn.sent",
					"distsender.rpc.export.sent",
					"distsender.rpc.gc.sent",
					"distsender.rpc.get.sent",
					"distsender.rpc.heartbeattxn.sent",
					"distsender.rpc.import.sent",
					"distsender.rpc.increment.sent",
					"distsender.rpc.initput.sent",
					"distsender.rpc.leaseinfo.sent",
					"distsender.rpc.merge.sent",
					"distsender.rpc.migrate.sent",
					"distsender.rpc.pushtxn.sent",
					"distsender.rpc.put.sent",
					"distsender.rpc.queryintent.sent",
					"distsender.rpc.queryresolvedtimestamp.sent",
					"distsender.rpc.querytxn.sent",
					"distsender.rpc.rangestats.sent",
					"distsender.rpc.recomputestats.sent",
					"distsender.rpc.recovertxn.sent",
					"distsender.rpc.refresh.sent",
					"distsender.rpc.refreshrange.sent",
					"distsender.rpc.requestlease.sent",
					"distsender.rpc.resolveintent.sent",
					"distsender.rpc.resolveintentrange.sent",
					"distsender.rpc.reversescan.sent",
					"distsender.rpc.revertrange.sent",
					"distsender.rpc.scan.sent",
					"distsender.rpc.scaninterleavedintents.sent",
					"distsender.rpc.subsume.sent",
					"distsender.rpc.transferlease.sent",
					"distsender.rpc.probe.sent",
					"distsender.rpc.truncatelog.sent",
					"distsender.rpc.writebatch.sent",
				},
			},
			{
				Title: "Errors",
				Metrics: []string{
					"distsender.rpc.err.ambiguousresulterrtype",
					"distsender.rpc.err.batchtimestampbeforegcerrtype",
					"distsender.rpc.err.communicationerrtype",
					"distsender.rpc.err.conditionfailederrtype",
					"distsender.rpc.err.errordetailtype(0)",
					"distsender.rpc.err.errordetailtype(15)",
					"distsender.rpc.err.errordetailtype(19)",
					"distsender.rpc.err.errordetailtype(20)",
					"distsender.rpc.err.errordetailtype(21)",
					"distsender.rpc.err.errordetailtype(23)",
					"distsender.rpc.err.errordetailtype(24)",
					"distsender.rpc.err.errordetailtype(29)",
					"distsender.rpc.err.errordetailtype(30)",
					"distsender.rpc.err.errordetailtype(33)",
					"distsender.rpc.err.indeterminatecommiterrtype",
					"distsender.rpc.err.integeroverflowerrtype",
					"distsender.rpc.err.intentmissingerrtype",
					"distsender.rpc.err.internalerrtype",
					"distsender.rpc.err.invalidleaseerrtype",
					"distsender.rpc.err.leaserejectederrtype",
					"distsender.rpc.err.mergeinprogresserrtype",
					"distsender.rpc.err.mintimestampboundunsatisfiableerrtype",
					"distsender.rpc.err.mvcchistorymutationerrtype",
					"distsender.rpc.err.nodeunavailableerrtype",
					"distsender.rpc.err.notleaseholdererrtype",
					"distsender.rpc.err.oprequirestxnerrtype",
					"distsender.rpc.err.optimisticevalconflictserrtype",
					"distsender.rpc.err.refreshfailederrtype",
					"distsender.rpc.err.raftgroupdeletederrtype",
					"distsender.rpc.err.rangefeedretryerrtype",
					"distsender.rpc.err.rangekeymismatcherrtype",
					"distsender.rpc.err.rangenotfounderrtype",
					"distsender.rpc.err.readwithinuncertaintyintervalerrtype",
					"distsender.rpc.err.replicacorruptionerrtype",
					"distsender.rpc.err.replicatooolderrtype",
					"distsender.rpc.err.storenotfounderrtype",
					"distsender.rpc.err.transactionabortederrtype",
					"distsender.rpc.err.transactionpusherrtype",
					"distsender.rpc.err.transactionretryerrtype",
					"distsender.rpc.err.transactionretrywithprotorefresherrtype",
					"distsender.rpc.err.transactionstatuserrtype",
					"distsender.rpc.err.txnalreadyencounterederrtype",
					"distsender.rpc.err.unsupportedrequesterrtype",
					"distsender.rpc.err.writeintenterrtype",
					"distsender.rpc.err.writetooolderrtype",
				},
			},
		},
	},
	{
		Organization: [][]string{{DistributionLayer, "RPC", "Heartbeats"}},
		Charts: []chartDescription{
			{
				Title: "Overview",
				Metrics: []string{
					"rpc.heartbeats.initializing",
					"rpc.heartbeats.nominal",
					"rpc.heartbeats.failed",
				},
				AxisLabel: "Heartbeats",
			},
			{
				Title: "Loops",
				Metrics: []string{
					"rpc.heartbeats.loops.exited",
					"rpc.heartbeats.loops.started",
				},
				AxisLabel: "Heartbeat Loops",
			},
		},
	},
	{
		Organization: [][]string{{DistributionLayer, "Gossip"}},
		Charts: []chartDescription{
			{
				Title:       "Active Connections",
				Downsampler: DescribeAggregator_MAX,
				Aggregator:  DescribeAggregator_MAX,
				Metrics: []string{
					"gossip.connections.incoming",
					"gossip.connections.outgoing",
				},
			},
			{
				Title: "Bytes",
				Metrics: []string{
					"gossip.bytes.received",
					"gossip.bytes.sent",
				},
			},
			{
				Title: "Infos",
				Metrics: []string{
					"gossip.infos.received",
					"gossip.infos.sent",
				},
			},
			{
				Title:       "Refused Connections",
				Downsampler: DescribeAggregator_MAX,
				Aggregator:  DescribeAggregator_MAX,
				Metrics:     []string{"gossip.connections.refused"},
			},
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Merge Queue"},
			{ReplicationLayer, "Merge Queue"},
		},
		Charts: []chartDescription{
			{
				Title:       "Pending",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics: []string{
					"queue.merge.pending",
					"queue.merge.purgatory",
				},
			},
			{
				Title:       "Successes",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics: []string{
					"queue.merge.process.failure",
					"queue.merge.process.success",
				},
			},
			{
				Title:   "Time Spent",
				Metrics: []string{"queue.merge.processingnanos"},
			},
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Ranges"},
			{ReplicationLayer, "Ranges"},
		},
		Charts: []chartDescription{
			{
				Title: "Overview",
				Metrics: []string{
					"ranges",
					"ranges.unavailable",
					"ranges.underreplicated",
					"ranges.overreplicated",
				},
			},
			{
				Title: "Operations",
				Metrics: []string{
					"range.adds",
					"range.splits",
					"range.merges",
					"range.removes",
				},
			},
			{
				Title: "Rangefeed",
				Metrics: []string{
					"kv.rangefeed.catchup_scan_nanos",
				},
			},
			{
				Title: "Rangefeed Memory Allocations",
				Metrics: []string{
					"kv.rangefeed.budget_allocation_failed",
					"kv.rangefeed.budget_allocation_blocked",
				},
			},
			{
				Title: "Memory Usage",
				Metrics: []string{
					"kv.rangefeed.mem_shared",
					"kv.rangefeed.mem_system",
				},
			},
			{
				Title: "Snapshots",
				Metrics: []string{
					"range.snapshots.generated",
					"range.snapshots.applied-voter",
					"range.snapshots.applied-initial",
					"range.snapshots.applied-non-voter",
				},
			},
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Rebalancing"},
			{ReplicationLayer, "Leases"},
		},
		Charts: []chartDescription{
			{
				Title:   "Rebalancing Lease Transfers",
				Metrics: []string{"rebalancing.lease.transfers"},
			},
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Rebalancing"},
			{ReplicationLayer, "Ranges"},
		},
		Charts: []chartDescription{
			{
				Title:   "Range Rebalances",
				Metrics: []string{"rebalancing.range.rebalances"},
			},
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Rebalancing"},
		},
		Charts: []chartDescription{
			{
				Title:   "QPS",
				Metrics: []string{"rebalancing.queriespersecond"},
			},
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Split Queue"},
			{ReplicationLayer, "Split Queue"},
		},
		Charts: []chartDescription{
			{
				Title:       "Pending",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics: []string{
					"queue.split.pending",
					"queue.split.purgatory",
				},
			},
			{
				Title:       "Successes",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics: []string{
					"queue.split.process.failure",
					"queue.split.process.success",
				},
			},
			{
				Title:   "Time Spent",
				Metrics: []string{"queue.split.processingnanos"},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Clocks"}}, Charts: []chartDescription{
			{
				Title:   "Roundtrip Latency",
				Metrics: []string{"round-trip-latency"},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Prober"}}, Charts: []chartDescription{
			{
				Title: "Availability",
				Metrics: []string{
					"kv.prober.planning_attempts",
					"kv.prober.planning_failures",
					"kv.prober.read.attempts",
					"kv.prober.read.failures",
					"kv.prober.write.attempts",
					"kv.prober.write.failures",
				},
				AxisLabel: "Probes",
			},
			{
				Title: "Latency",
				Metrics: []string{
					"kv.prober.read.latency",
					"kv.prober.write.latency",
				},
			},
		},
	},
	{
		Organization: [][]string{
			{KVTransactionLayer, "Clocks"},
			{Process, "Clocks"},
		},
		Charts: []chartDescription{
			{
				Title: "Offsets",
				Metrics: []string{
					"clock-offset.meannanos",
					"clock-offset.stddevnanos",
				},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Follower Reads"}},
		Charts: []chartDescription{
			{
				Title:   "Closed Timestamp",
				Metrics: []string{"kv.closed_timestamp.max_behind_nanos"},
			},
			{
				Title:   "Count",
				Metrics: []string{"follower_reads.success_count"},
			},
			{
				Title:   "Failed Attempts To Close",
				Metrics: []string{"kv.closed_timestamp.failures_to_close"},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Protected Timestamps", "Reconciliation"}},
		Charts: []chartDescription{
			{
				Title: "Records Processed",
				Metrics: []string{
					"kv.protectedts.reconciliation.errors",
					"kv.protectedts.reconciliation.records_processed",
					"kv.protectedts.reconciliation.records_removed",
				},
			},
			{
				Title: "Reconciliation Runs",
				Metrics: []string{
					"kv.protectedts.reconciliation.num_runs",
				},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "MVCC Garbage Collection (GC)", "Keys"}},
		Charts: []chartDescription{
			{
				Title: "AbortSpan",
				Metrics: []string{
					"queue.gc.info.abortspanconsidered",
					"queue.gc.info.abortspangcnum",
					"queue.gc.info.abortspanscanned",
				},
			},
			{
				Title:   "Distinct Txns",
				Metrics: []string{"queue.gc.info.intenttxns"},
			},
			{
				Title:   "Enteries in Txn Spans",
				Metrics: []string{"queue.gc.info.transactionspanscanned"},
			},
			{
				Title: "Intents",
				Metrics: []string{
					"queue.gc.info.resolvesuccess",
					"queue.gc.info.resolvetotal",
				},
			},
			{
				Title:   "Keys with GC'able Data",
				Metrics: []string{"queue.gc.info.numkeysaffected"},
			},
			{
				Title:   "Old Intents",
				Metrics: []string{"queue.gc.info.intentsconsidered"},
			},
			{
				Title:   "Pushes",
				Metrics: []string{"queue.gc.info.pushtxn"},
			},
			{
				Title: "Txn Relationship",
				Metrics: []string{
					"queue.gc.info.transactionspangcaborted",
					"queue.gc.info.transactionspangccommitted",
					"queue.gc.info.transactionspangcpending",
					"queue.gc.info.transactionspangcstaging",
				},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Overview"}},
		Charts: []chartDescription{
			{
				Title:   "Current GC Pause Percent",
				Metrics: []string{"sys.gc.pause.percent"},
			},
			{
				Title:   "Total GC Pause (NS)",
				Metrics: []string{"sys.gc.pause.ns"},
			},
			{
				Title:   "Total GC Runs",
				Metrics: []string{"sys.gc.count"},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Requests", "Overview"}},
		Charts: []chartDescription{
			{
				Title:   "Latency",
				Metrics: []string{"exec.latency"},
			},
			{
				Title:       "Success",
				Downsampler: DescribeAggregator_MAX,
				Rate:        DescribeDerivative_DERIVATIVE,
				Percentiles: false,
				Metrics: []string{
					"exec.error",
					"exec.success",
				},
			},
			{
				Title:       "Storage Engine Stalls",
				Downsampler: DescribeAggregator_MAX,
				Rate:        DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
				Percentiles: false,
				Metrics:     []string{"engine.stalls"},
			},
		},
	},
	{
		Organization: [][]string{
			{KVTransactionLayer, "Requests", "Backpressure"},
			{ReplicationLayer, "Requests", "Backpressure"},
		},
		Charts: []chartDescription{
			{
				Title:       "Writes Waiting on Range Split",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"requests.backpressure.split"},
			},
		},
	},
	{
		Organization: [][]string{
			{KVTransactionLayer, "Requests", "Tenant Rate Limiting"}},
		Charts: []chartDescription{
			{
				Title:       "Requests Blocked on Tenant Rate Limiting",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"kv.tenant_rate_limit.current_blocked"},
			},
			{
				Title:       "Number of Tenants",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"kv.tenant_rate_limit.num_tenants"},
			},
			{
				Title:       "Read Requests Admitted by Rate Limiter",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"kv.tenant_rate_limit.read_requests_admitted"},
			},
			{
				Title:       "Write Requests Admitted by Rate Limiter",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"kv.tenant_rate_limit.write_requests_admitted"},
			},
			{
				Title:       "Read Bytes Admitted by Rate Limiter",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"kv.tenant_rate_limit.read_bytes_admitted"},
			},
			{
				Title:       "Write Bytes Admitted by Rate Limiter",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"kv.tenant_rate_limit.write_bytes_admitted"},
			},
		},
	},
	{
		Organization: [][]string{
			{KVTransactionLayer, "Requests", "Slow"},
			{ReplicationLayer, "Requests", "Slow"},
			{DistributionLayer, "Requests", "Slow"},
		},
		Charts: []chartDescription{
			{
				Title:       "Latch",
				Downsampler: DescribeAggregator_MAX,
				Metrics:     []string{"requests.slow.latch"},
			},
			{
				Title:       "Stuck Acquiring Lease",
				Downsampler: DescribeAggregator_MAX,
				Metrics:     []string{"requests.slow.lease"},
			},
			{
				Title:       "Stuck in Raft",
				Downsampler: DescribeAggregator_MAX,
				Metrics:     []string{"requests.slow.raft"},
			},
			{
				Title:       "Stuck sending RPCs to range",
				Downsampler: DescribeAggregator_MAX,
				Metrics:     []string{"requests.slow.distsender"},
			},
			{
				Title:       "Replicas with tripped circuit breakers",
				Downsampler: DescribeAggregator_SUM,
				Metrics:     []string{"kv.replica_circuit_breaker.num_tripped_replicas"},
			},
			{
				Title:       "Replica circuit breaker trip events",
				Downsampler: DescribeAggregator_MAX,
				Rate:        DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
				Metrics:     []string{"kv.replica_circuit_breaker.num_tripped_events"},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Storage"}},
		Charts: []chartDescription{
			{
				Title:     "Counts",
				AxisLabel: "MVCC Keys & Values",
				Metrics: []string{
					"intentcount",
					"keycount",
					"livecount",
					"syscount",
					"valcount",
				},
			},
			{
				Title:   "Cumultative Age of Non-Live Data",
				Metrics: []string{"gcbytesage"},
			},
			{
				Title:   "Cumultative Intent Age",
				Metrics: []string{"intentage"},
			},
			{
				Title: "Size",
				Metrics: []string{
					"abortspanbytes",
					"intentbytes",
					"keybytes",
					"livebytes",
					"sysbytes",
					"totalbytes",
					"valbytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Timestamp Cache"}},
		Charts: []chartDescription{
			{
				Title: "Page Counts",
				Metrics: []string{
					"tscache.skl.pages",
				},
			},
			{
				Title: "Page Rotations",
				Metrics: []string{
					"tscache.skl.rotations",
				},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions", "Intents"}},
		Charts: []chartDescription{
			{
				Title: "Intent Resolver",
				Metrics: []string{
					"intentresolver.async.throttled",
				},
			},
			{
				Title: "Overview",
				Metrics: []string{
					"intents.abort-attempts",
					"intents.poison-attempts",
					"intents.resolve-attempts",
				},
			},
			{
				Title: "Leak Tracking",
				Metrics: []string{
					"queue.gc.info.transactionresolvefailed",
					"queue.gc.info.resolvefailed",
					"intentresolver.finalized_txns.failed",
					"intentresolver.intents.failed",
				},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions", "LockTable"}},
		Charts: []chartDescription{
			{
				Title: "Locks",
				Metrics: []string{
					"kv.concurrency.locks",
					"kv.concurrency.locks_with_wait_queues",
				},
			},
			{
				Title: "Lock Hold Durations",
				Metrics: []string{
					"kv.concurrency.avg_lock_hold_duration_nanos",
					"kv.concurrency.max_lock_hold_duration_nanos",
				},
			},
			{
				Title: "Waiters",
				Metrics: []string{
					"kv.concurrency.lock_wait_queue_waiters",
					"kv.concurrency.max_lock_wait_queue_waiters_for_lock",
				},
			},
			{
				Title: "Lock Wait Durations",
				Metrics: []string{
					"kv.concurrency.avg_lock_wait_duration_nanos",
					"kv.concurrency.max_lock_wait_duration_nanos",
				},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions", "TxnWaitQueue"}},
		Charts: []chartDescription{
			{
				Title: "Deadlocks",
				Metrics: []string{
					"txnwaitqueue.deadlocks_total",
				},
			},
			{
				Title: "Slow Pusher",
				Metrics: []string{
					"txnwaitqueue.pusher.slow",
				},
			},
			{
				Title: "Wait Time",
				Metrics: []string{
					"txnwaitqueue.pusher.wait_time",
					"txnwaitqueue.query.wait_time",
				},
				AxisLabel: "Wait Time",
			},
			{
				Title: "Waiting",
				Metrics: []string{
					"txnwaitqueue.pushee.waiting",
					"txnwaitqueue.pusher.waiting",
					"txnwaitqueue.query.waiting",
				},
				AxisLabel: "Actors",
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Charts: []chartDescription{
			{
				Title:   "Aborts",
				Metrics: []string{"txn.aborts"},
			},
			{
				Title: "Failed Aborts",
				Metrics: []string{
					"txn.rollbacks.failed",
					"txn.rollbacks.async.failed",
				},
			},
			{
				Title:   "Successful refreshes",
				Metrics: []string{"txn.refresh.success"},
			},
			{
				Title:   "Failed refreshes",
				Metrics: []string{"txn.refresh.fail"},
			},
			{
				Title:   "Failed refreshes with condensed spans",
				Metrics: []string{"txn.refresh.fail_with_condensed_spans"},
			},
			{
				Title:   "Transactions exceeding refresh spans memory limit",
				Metrics: []string{"txn.refresh.memory_limit_exceeded"},
			},
			{
				Title:   "Auto-Retries",
				Metrics: []string{"txn.refresh.auto_retries"},
			},
			{
				Title: "Commits",
				Metrics: []string{
					"txn.commits",
					"txn.commits1PC",
					"txn.parallelcommits",
					"txn.commit_waits",
					"txn.commit_waits.before_commit_trigger",
				},
			},
			{
				Title:   "Failed Commits",
				Metrics: []string{"txn.commits.failed"},
			},
			{
				Title:   "Durations",
				Metrics: []string{"txn.durations"},
			},
			{
				Title: "Restart Cause Mix",
				Metrics: []string{
					"txn.restarts.serializable",
					"txn.restarts.writetooold",
					"txn.restarts.asyncwritefailure",
					"txn.restarts.commitdeadlineexceeded",
					"txn.restarts.readwithinuncertainty",
					"txn.restarts.txnaborted",
					"txn.restarts.txnpush",
					"txn.restarts.unknown",
					"txn.restarts.writetoooldmulti",
				},
			},
			{
				Title:       "Restarts",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: true,
				Metrics:     []string{"txn.restarts"},
			},
			{
				Title:       "Intents condensing - historical",
				Downsampler: DescribeAggregator_MAX,
				Metrics: []string{
					"txn.condensed_intent_spans",
				},
			},
			{
				Title:       "Intents condensing - current",
				Downsampler: DescribeAggregator_MAX,
				Metrics: []string{
					"txn.condensed_intent_spans_gauge",
				},
			},
			{
				Title:       "Intents condensing - transactions rejected",
				Downsampler: DescribeAggregator_MAX,
				Metrics: []string{
					"txn.condensed_intent_spans_rejected",
				},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions", "Recovery"}},
		Charts: []chartDescription{
			{
				Title: "Successes",
				Metrics: []string{
					"txnrecovery.successes.committed",
					"txnrecovery.successes.pending",
					"txnrecovery.successes.aborted",
				},
			},
			{
				Title: "Total Attempts",
				Metrics: []string{
					"txnrecovery.attempts.total",
				},
			},
			{
				Title: "Pending Attempts",
				Metrics: []string{
					"txnrecovery.attempts.pending",
				},
			},
			{
				Title: "Failures",
				Metrics: []string{
					"txnrecovery.failures",
				},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Charts: []chartDescription{
			{
				Title: "Emitted Bytes",
				Metrics: []string{
					"changefeed.emitted_bytes",
				},
			},
			{
				Title: "Emitted Messages",
				Metrics: []string{
					"changefeed.emitted_messages",
				},
			},
			{
				Title: "Entries",
				Metrics: []string{
					"changefeed.buffer_entries.in",
					"changefeed.buffer_entries.out",
				},
			},
			{
				Title: "Errors",
				Metrics: []string{
					"changefeed.error_retries",
					"changefeed.failures",
				},
			},
			{
				Title: "Flushes",
				Metrics: []string{
					"changefeed.flushes",
				},
			},
			{
				Title: "Max Behind Nanos",
				Metrics: []string{
					"changefeed.max_behind_nanos",
				},
			},
			{
				Title: "Min High Water",
				Metrics: []string{
					"changefeed.min_high_water",
				},
			},
			{
				Title: "Currently Running",
				Metrics: []string{
					"changefeed.running",
				},
			},
			{
				Title: "Total Time Spent",
				Metrics: []string{
					"changefeed.emit_nanos",
					"changefeed.flush_nanos",
					"changefeed.processing_nanos",
					"changefeed.table_metadata_nanos",
				},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Consistency Checker Queue"}},
		Charts: []chartDescription{
			{
				Title:   "Pending",
				Metrics: []string{"queue.consistency.pending"},
			},
			{
				Title: "Successes",
				Metrics: []string{
					"queue.consistency.process.failure",
					"queue.consistency.process.success",
				},
			},
			{
				Title:   "Time Spent",
				Metrics: []string{"queue.consistency.processingnanos"},
			},
		},
	},
	{
		Organization: [][]string{
			{ReplicationLayer, "Garbage Collection"},
			{StorageLayer, "Garbage Collection"},
		},
		Charts: []chartDescription{
			{
				Title:   "Queue Pending",
				Metrics: []string{"queue.gc.pending"},
			},
			{
				Title: "Queue Success",
				Metrics: []string{
					"queue.gc.process.failure",
					"queue.gc.process.success",
				},
			},
			{
				Title:   "Queue Time",
				Metrics: []string{"queue.gc.processingnanos"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Leases"}},
		Charts: []chartDescription{
			{
				Title:   "Stuck Acquisition Count",
				Metrics: []string{"requests.slow.lease"},
			},
			{
				Title: "Succcess Rate",
				Metrics: []string{
					"leases.error",
					"leases.success",
				},
			},
			{
				Title: "Total",
				Metrics: []string{
					"leases.epoch",
					"leases.expiration",
					"replicas.leaseholders",
					"replicas.leaders_not_leaseholders",
				},
			},
			{
				Title: "Transfer Success Rate",
				Metrics: []string{
					"leases.transfers.error",
					"leases.transfers.success",
				},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Node Liveness"}},
		Charts: []chartDescription{
			{
				Title:   "Epoch Increment Count",
				Metrics: []string{"liveness.epochincrements"},
			},
			{
				Title:   "Heartbeats In-Flight",
				Metrics: []string{"liveness.heartbeatsinflight"},
			},
			{
				Title:   "Heartbeat Latency",
				Metrics: []string{"liveness.heartbeatlatency"},
			},
			{
				Title: "Heartbeats Success",
				Metrics: []string{
					"liveness.heartbeatfailures",
					"liveness.heartbeatsuccesses",
				},
			},
			{
				Title:       "Node Count",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"liveness.livenodes"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Entry Cache"}},
		Charts: []chartDescription{
			{
				Title: "Entries",
				Metrics: []string{
					"raft.entrycache.size",
				},
			},
			{
				Title: "Hits",
				Metrics: []string{
					"raft.entrycache.accesses",
					"raft.entrycache.hits",
				},
				AxisLabel: "Entry Cache Operations",
			},
			{
				Title: "Size",
				Metrics: []string{
					"raft.entrycache.bytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Heartbeats"}},
		Charts: []chartDescription{
			{
				Title:   "MsgHeartbeatResp Count",
				Metrics: []string{"raft.rcvd.heartbeatresp"},
			},
			{
				Title:   "MsgTransferLeader Count",
				Metrics: []string{"raft.rcvd.transferleader"},
			},
			{
				Title:   "Pending",
				Metrics: []string{"raft.heartbeats.pending"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Latency"}},
		Charts: []chartDescription{
			{
				Title:   "Apply Committed",
				Metrics: []string{"raft.process.applycommitted.latency"},
			},
			{
				Title:   "Command Commit",
				Metrics: []string{"raft.process.commandcommit.latency"},
			},
			{
				Title:   "Handle Ready",
				Metrics: []string{"raft.process.handleready.latency"},
			},
			{
				Title:   "Log Commit",
				Metrics: []string{"raft.process.logcommit.latency"},
			},
			{
				Title:   "Scheduler",
				Metrics: []string{"raft.scheduler.latency"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Log"}},
		Charts: []chartDescription{
			{
				Title:   "Entries Truncated",
				Metrics: []string{"raftlog.truncated"},
			},
			{
				Title:   "Followers Behind By...",
				Metrics: []string{"raftlog.behind"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Charts: []chartDescription{
			{
				Title:   "Commands Count",
				Metrics: []string{"raft.commandsapplied"},
			},
			{
				Title:   "Enqueued",
				Metrics: []string{"raft.enqueued.pending"},
			},
			{
				Title:   "Keys/Sec Avg.",
				Metrics: []string{"rebalancing.writespersecond"},
			},
			{
				Title:   "Leader Transfers",
				Metrics: []string{"range.raftleadertransfers"},
			},
			{
				Title:   "Leaders",
				Metrics: []string{"replicas.leaders"},
			},
			{
				Title:   "Stuck Request Count",
				Metrics: []string{"requests.slow.raft"},
			},
			{
				Title:   "Heartbeat Timeouts",
				Metrics: []string{"raft.timeoutcampaign"},
			},
			{
				Title:   "Ticks Queued",
				Metrics: []string{"raft.ticks"},
			},
			{
				Title: "Working vs. Ticking TIme",
				Metrics: []string{
					"raft.process.tickingnanos",
					"raft.process.workingnanos",
				},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Queues"}},
		Charts: []chartDescription{
			{
				Title:   "Log Pending",
				Metrics: []string{"queue.raftlog.pending"},
			},
			{
				Title:   "Log Processing Time Spent",
				Metrics: []string{"queue.raftlog.processingnanos"},
			},
			{
				Title: "Log Successes",
				Metrics: []string{
					"queue.raftlog.process.failure",
					"queue.raftlog.process.success",
				},
			},
			{
				Title:   "Snapshot Processing Time Spent",
				Metrics: []string{"queue.raftsnapshot.processingnanos"},
			},
			{
				Title: "Snapshot Successes",
				Metrics: []string{
					"queue.raftsnapshot.process.failure",
					"queue.raftsnapshot.process.success",
				},
			},
			{
				Title:   "Snapshots Pending",
				Metrics: []string{"queue.raftsnapshot.pending"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Charts: []chartDescription{
			{
				Title:   "Dropped",
				Metrics: []string{"raft.rcvd.dropped"},
			},
			{
				Title:   "Heartbeat Count",
				Metrics: []string{"raft.rcvd.heartbeat"},
			},
			{
				Title:   "MsgApp Count",
				Metrics: []string{"raft.rcvd.app"},
			},
			{
				Title:   "MsgAppResp Count",
				Metrics: []string{"raft.rcvd.appresp"},
			},
			{
				Title:   "MsgHeartbeatResp Count",
				Metrics: []string{"raft.rcvd.heartbeatresp"},
			},
			{
				Title:   "MsgPreVote Count",
				Metrics: []string{"raft.rcvd.prevote"},
			},
			{
				Title:   "MsgPreVoteResp Count",
				Metrics: []string{"raft.rcvd.prevoteresp"},
			},
			{
				Title:   "MsgProp Count",
				Metrics: []string{"raft.rcvd.prop"},
			},
			{
				Title:   "MsgSnap Count",
				Metrics: []string{"raft.rcvd.snap"},
			},
			{
				Title:   "MsgTimeoutNow Count",
				Metrics: []string{"raft.rcvd.timeoutnow"},
			},
			{
				Title:   "MsgTransferLeader Count",
				Metrics: []string{"raft.rcvd.transferleader"},
			},
			{
				Title:   "MsgVote Count",
				Metrics: []string{"raft.rcvd.vote"},
			},
			{
				Title:   "MsgVoteResp Count",
				Metrics: []string{"raft.rcvd.voteresp"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Ranges"}},
		Charts: []chartDescription{
			{
				Title:   "Backpressued Writes Waiting on Split",
				Metrics: []string{"requests.backpressure.split"},
			},
			{
				Title:   "Raft Leader Transfers",
				Metrics: []string{"range.raftleadertransfers"},
			},
			{
				Title:   "Unsafe Loss of Quorum Recoveries",
				Metrics: []string{"range.recoveries"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replica GC Queue"}},
		Charts: []chartDescription{
			{
				Title:   "Pending",
				Metrics: []string{"queue.replicagc.pending"},
			},
			{
				Title:   "Removal Count",
				Metrics: []string{"queue.replicagc.removereplica"},
			},
			{
				Title: "Successes",
				Metrics: []string{
					"queue.replicagc.process.failure",
					"queue.replicagc.process.success",
				},
			},
			{
				Title:   "Time Spent",
				Metrics: []string{"queue.replicagc.processingnanos"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replicas", "Overview"}},
		Charts: []chartDescription{
			{
				Title: "Count",
				Metrics: []string{
					"replicas.quiescent",
					"replicas.uninitialized",
					"replicas",
					"replicas.reserved",
				},
			},
			{
				Title:   "Leaseholders",
				Metrics: []string{"replicas.leaseholders"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Charts: []chartDescription{
			{
				Title:   "Add Replica Count",
				Metrics: []string{"queue.replicate.addreplica"},
			},
			{
				Title:   "Lease Transfer Count",
				Metrics: []string{"queue.replicate.transferlease"},
			},
			{
				Title:   "Pending",
				Metrics: []string{"queue.replicate.pending"},
			},
			{
				Title:   "Purgatory",
				Metrics: []string{"queue.replicate.purgatory"},
			},
			{
				Title:   "Rebalance Count",
				Metrics: []string{"queue.replicate.rebalancereplica"},
			},
			{
				Title:   "Demotions of Voters to Non Voters",
				Metrics: []string{"queue.replicate.voterdemotions"},
			},
			{
				Title:   "Promotions of Non Voters to Voters",
				Metrics: []string{"queue.replicate.nonvoterpromotions"},
			},
			{
				Title: "Remove Replica Count",
				Metrics: []string{
					"queue.replicate.removedeadreplica",
					"queue.replicate.removereplica",
					"queue.replicate.removelearnerreplica",
				},
			},
			{
				Title: "Successes",
				Metrics: []string{
					"queue.replicate.process.failure",
					"queue.replicate.process.success",
				},
			},
			{
				Title:   "Time Spent",
				Metrics: []string{"queue.replicate.processingnanos"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "Schema Changer"}},
		Charts: []chartDescription{
			{
				Title:   "Running",
				Metrics: []string{"sql.schema_changer.running"},
			},
			{
				Title:       "Run Outcomes",
				Downsampler: DescribeAggregator_MAX,
				Aggregator:  DescribeAggregator_SUM,
				Metrics: []string{
					"sql.schema_changer.permanent_errors",
					"sql.schema_changer.retry_errors",
					"sql.schema_changer.successes",
				},
				AxisLabel: "Schema Change Executions",
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "DistSQL", "Flows"}},
		Charts: []chartDescription{
			{
				Title:   "Active",
				Metrics: []string{"sql.distsql.flows.active"},
			},
			{
				Title:   "Queue Wait",
				Metrics: []string{"sql.distsql.flows.queue_wait"},
			},
			{
				Title:   "Queued",
				Metrics: []string{"sql.distsql.flows.queued"},
			},
			{
				Title:   "Total",
				Metrics: []string{"sql.distsql.flows.total"},
			},
			{
				Title:   "Scheduled",
				Metrics: []string{"sql.distsql.flows.scheduled"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Catalog", "SQL Leases"}},
		Charts: []chartDescription{
			{
				Title: "Outstanding SQL Leases",
				Metrics: []string{
					"sql.leases.active",
				},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Catalog", "Hydrated Descriptor Cache"}},
		Charts: []chartDescription{
			{
				Title: "Cache Hits and Misses",
				Metrics: []string{
					"sql.hydrated_table_cache.hits",
					"sql.hydrated_table_cache.misses",
				},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Liveness"}},
		Charts: []chartDescription{
			{
				Title: "Session Writes",
				Metrics: []string{
					"sqlliveness.write_successes",
					"sqlliveness.write_failures",
				},
			},
			{
				Title: "IsAlive cache",
				Metrics: []string{
					"sqlliveness.is_alive.cache_hits",
					"sqlliveness.is_alive.cache_misses",
				},
			},
			{
				Title: "Session deletion",
				Metrics: []string{
					"sqlliveness.sessions_deletion_runs",
					"sqlliveness.sessions_deleted",
				},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Charts: []chartDescription{
			{
				Title:   "Active Queries",
				Metrics: []string{"sql.distsql.queries.active"},
			},
			{
				Title:   "Current Memory Usage",
				Metrics: []string{"sql.mem.distsql.current"},
			},
			{
				Title: "DML Mix",
				Metrics: []string{
					"sql.distsql.select.count",
					"sql.distsql.select.count.internal",
				},
				AxisLabel: "SQL Statements",
			},
			{
				Title: "Exec Latency",
				Metrics: []string{
					"sql.distsql.exec.latency",
					"sql.distsql.exec.latency.internal",
				},
				AxisLabel: "Latency",
			},
			{
				Title:   "Memory Usage per Statement",
				Metrics: []string{"sql.mem.distsql.max"},
			},
			{
				Title: "Service Latency",
				Metrics: []string{
					"sql.distsql.service.latency",
					"sql.distsql.service.latency.internal",
				},
				AxisLabel: "Latency",
			},
			{
				Title:   "Total Queries",
				Metrics: []string{"sql.distsql.queries.total"},
			},
			{
				Title:   "Contended Queries",
				Metrics: []string{"sql.distsql.contended_queries.count"},
			},
			{
				Title:   "Vectorized Temporary Storage Open File Descriptors",
				Metrics: []string{"sql.distsql.vec.openfds"},
			},
			{
				Title:   "Current Disk Usage",
				Metrics: []string{"sql.disk.distsql.current"},
			},
			{
				Title:   "Disk Usage per Statement",
				Metrics: []string{"sql.disk.distsql.max"},
			},
			{
				Title:   "Number of Queries Spilled To Disk",
				Metrics: []string{"sql.distsql.queries.spilled"},
			},
			{
				Title:   "Number of Bytes Written Due to Disk Spilling",
				Metrics: []string{"sql.disk.distsql.spilled.bytes.written"},
			},
			{
				Title:   "Number of Bytes Read Due to Disk Spilling",
				Metrics: []string{"sql.disk.distsql.spilled.bytes.read"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Stats"}},
		Charts: []chartDescription{
			{
				Title:   "Memory usage for fingerprint storage",
				Metrics: []string{"sql.stats.mem.max"},
			},
			{
				Title:   "Current memory usage for fingerprint storage",
				Metrics: []string{"sql.stats.mem.current"},
			},
			{
				Title:   "Memory usage for reported fingerprint storage",
				Metrics: []string{"sql.stats.reported.mem.max"},
			},
			{
				Title:   "Current memory usage for reported fingerprint storage",
				Metrics: []string{"sql.stats.reported.mem.current"},
			},
			{
				Title:   "Number of fingerprint statistics being discarded",
				Metrics: []string{"sql.stats.discarded.current"},
			},
			{
				Title:   "Number of times SQL Stats are flushed to persistent storage",
				Metrics: []string{"sql.stats.flush.count"},
			},
			{
				Title:   "Number of errors encountered when flushing SQL Stats",
				Metrics: []string{"sql.stats.flush.error"},
			},
			{
				Title:   "Time took to complete SQL Stats flush",
				Metrics: []string{"sql.stats.flush.duration"},
			},
			{
				Title:   "Number of stale statement/transaction roles removed by cleanup job",
				Metrics: []string{"sql.stats.cleanup.rows_removed"},
			},
			{
				Title:   "Time took in nanoseconds to collect transaction stats",
				Metrics: []string{"sql.stats.txn_stats_collection.duration"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "Contention"}},
		Charts: []chartDescription{
			{
				Title:   "Transaction ID Cache Miss",
				Metrics: []string{"sql.contention.txn_id_cache.miss"},
			},
			{
				Title:   "Transaction ID Cache Read",
				Metrics: []string{"sql.contention.txn_id_cache.read"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "Bulk"}},
		Charts: []chartDescription{
			{
				Title:   "Current Memory Usage",
				Metrics: []string{"sql.mem.bulk.current"},
			},
			{
				Title:   "Memory Usage per Statement",
				Metrics: []string{"sql.mem.bulk.max"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "Optimizer"}},
		Charts: []chartDescription{
			{
				Title: "Count",
				Metrics: []string{
					"sql.optimizer.count",
					"sql.optimizer.count.internal",
				},
				AxisLabel: "SQL Statements",
			},
			{
				Title: "Fallback",
				Metrics: []string{
					"sql.optimizer.fallback.count",
					"sql.optimizer.fallback.count.internal",
				},
				AxisLabel: "Fallbacks",
			},
			{
				Title: "Plan Cache",
				Metrics: []string{
					"sql.optimizer.plan_cache.hits",
					"sql.optimizer.plan_cache.hits.internal",
					"sql.optimizer.plan_cache.misses",
					"sql.optimizer.plan_cache.misses.internal",
				},
				AxisLabel: "Plane Cache Accesses",
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "Row-Level TTL"}},
		Charts: []chartDescription{
			{
				Title: "Jobs Running",
				Metrics: []string{
					"jobs.row_level_ttl.currently_running",
					"jobs.row_level_ttl.currently_idle",
				},
			},
			{
				Title: "Jobs Statistics",
				Metrics: []string{
					"jobs.row_level_ttl.fail_or_cancel_completed",
					"jobs.row_level_ttl.fail_or_cancel_failed",
					"jobs.row_level_ttl.fail_or_cancel_retry_error",
					"jobs.row_level_ttl.resume_completed",
					"jobs.row_level_ttl.resume_failed",
					"jobs.row_level_ttl.resume_retry_error",
				},
			},
			{
				Title: "Scheduled Jobs Statistics",
				Metrics: []string{
					"schedules.scheduled-row-level-ttl-executor.succeeded",
					"schedules.scheduled-row-level-ttl-executor.started",
					"schedules.scheduled-row-level-ttl-executor.failed",
				},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Admin"}},
		Charts: []chartDescription{
			{
				Title:   "All",
				Metrics: []string{"sql.mem.admin.max"},
			},
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.admin.current"},
			},
			{
				Title:   "Session All",
				Metrics: []string{"sql.mem.admin.session.max"},
			},
			{
				Title:   "Session Current",
				Metrics: []string{"sql.mem.admin.session.current"},
			},
			{
				Title:   "Txn All",
				Metrics: []string{"sql.mem.admin.txn.max"},
			},
			{
				Title:   "Txn Current",
				Metrics: []string{"sql.mem.admin.txn.current"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Charts: []chartDescription{
			{
				Title:   "All",
				Metrics: []string{"sql.mem.conns.max"},
			},
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.conns.current"},
			},
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.conns.current"},
			},
			{
				Title:   "Session All",
				Metrics: []string{"sql.mem.conns.session.max"},
			},
			{
				Title:   "Session Current",
				Metrics: []string{"sql.mem.conns.session.current"},
			},
			{
				Title:   "Txn All",
				Metrics: []string{"sql.mem.conns.txn.max"},
			},
			{
				Title:   "Txn Current",
				Metrics: []string{"sql.mem.conns.txn.current"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Charts: []chartDescription{
			{
				Title:   "All",
				Metrics: []string{"sql.mem.internal.max"},
			},
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.internal.current"},
			},
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.internal.current"},
			},
			{
				Title:   "Session All",
				Metrics: []string{"sql.mem.internal.session.max"},
			},
			{
				Title:   "Session Current",
				Metrics: []string{"sql.mem.internal.session.current"},
			},
			{
				Title:   "Txn All",
				Metrics: []string{"sql.mem.internal.txn.max"},
			},
			{
				Title:   "Txn Current",
				Metrics: []string{"sql.mem.internal.txn.current"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL Session"}},
		Charts: []chartDescription{
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.sql.session.current"},
			},
			{
				Title:   "Max",
				Metrics: []string{"sql.mem.sql.session.max"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL Txn"}},
		Charts: []chartDescription{
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.sql.txn.current"},
			},
			{
				Title:   "Max",
				Metrics: []string{"sql.mem.sql.txn.max"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory"}},
		Charts: []chartDescription{
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.root.current"},
			},
			{
				Title:   "Max",
				Metrics: []string{"sql.mem.root.max"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL"}},
		Charts: []chartDescription{
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.sql.current"},
			},
			{
				Title:   "Max",
				Metrics: []string{"sql.mem.sql.max"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "Temporary Objects Cleanup"}},
		Charts: []chartDescription{
			{
				Title:   "Active Cleaners",
				Metrics: []string{"sql.temp_object_cleaner.active_cleaners"},
			},
			{
				Title: "Deletion Rate",
				Metrics: []string{
					"sql.temp_object_cleaner.schemas_to_delete",
					"sql.temp_object_cleaner.schemas_deletion_success",
					"sql.temp_object_cleaner.schemas_deletion_error",
				},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL"}},
		Charts: []chartDescription{
			{
				Title: "Active Connections",
				Metrics: []string{
					"sql.conns",
				},
			},
			{
				Title: "New Connections",
				Metrics: []string{
					"sql.new_conns",
				},
			},
			{
				Title: "Connection Latency",
				Metrics: []string{
					"sql.conn.latency",
				},
				AxisLabel: "Latency",
			},
			{
				Title: "Open Transactions",
				Metrics: []string{
					"sql.txns.open",
					"sql.txns.open.internal",
				},
				AxisLabel: "Transactions",
			},
			{
				Title: "Active Statements",
				Metrics: []string{
					"sql.statements.active",
					"sql.statements.active.internal",
				},
				AxisLabel: "Active Statements",
			},
			{
				Title: "Full Table Index Scans",
				Metrics: []string{
					"sql.full.scan.count",
					"sql.full.scan.count.internal",
				},
				AxisLabel: "SQL Statements",
			},
			{
				Title: "Byte I/O",
				Metrics: []string{
					"sql.bytesin",
					"sql.bytesout",
				},
			},
			{
				Title: "Exec Latency",
				Metrics: []string{
					"sql.exec.latency",
					"sql.exec.latency.internal",
				},
				AxisLabel: "Latency",
			},
			{
				Title: "Service Latency",
				Metrics: []string{
					"sql.service.latency",
					"sql.service.latency.internal",
				},
				AxisLabel: "Latency",
			},
			{
				Title: "Transaction Latency",
				Metrics: []string{
					"sql.txn.latency",
					"sql.txn.latency.internal",
				},
				AxisLabel: "Latency",
			},
			{
				Title: "Cancel Requests (Postgres Protocol)",
				Metrics: []string{
					"sql.pgwire_cancel.total",
					"sql.pgwire_cancel.ignored",
					"sql.pgwire_cancel.successful",
				},
				AxisLabel: "Count",
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL", "DDL"}},
		Charts: []chartDescription{
			{
				Title: "Counts",
				Metrics: []string{
					"sql.ddl.count",
					"sql.ddl.started.count",
				},
				AxisLabel: "SQL Statements",
			},
			{
				Title: "Counts (Internal)",
				Metrics: []string{
					"sql.ddl.count.internal",
					"sql.ddl.started.count.internal",
				},
				AxisLabel: "SQL Statements",
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL", "DML"}},
		Charts: []chartDescription{
			{
				Title: "Mix",
				Metrics: []string{
					"sql.query.count",
					"sql.delete.count",
					"sql.insert.count",
					"sql.misc.count",
					"sql.copy.count",
					"sql.query.count",
					"sql.select.count",
					"sql.update.count",
					"sql.failure.count",
				},
			},
			{
				Title: "Started Mix",
				Metrics: []string{
					"sql.query.started.count",
					"sql.delete.started.count",
					"sql.insert.started.count",
					"sql.misc.started.count",
					"sql.copy.started.count",
					"sql.query.started.count",
					"sql.select.started.count",
					"sql.update.started.count",
					"sql.failure.started.count",
				},
			},
			{
				Title: "Mix (Internal)",
				Metrics: []string{
					"sql.delete.count.internal",
					"sql.insert.count.internal",
					"sql.misc.count.internal",
					"sql.copy.count.internal",
					"sql.query.count.internal",
					"sql.select.count.internal",
					"sql.update.count.internal",
					"sql.failure.count.internal",
				},
			},
			{
				Title: "Started Mix (Internal)",
				Metrics: []string{
					"sql.delete.started.count.internal",
					"sql.insert.started.count.internal",
					"sql.misc.started.count.internal",
					"sql.copy.started.count.internal",
					"sql.query.started.count.internal",
					"sql.select.started.count.internal",
					"sql.update.started.count.internal",
					"sql.failure.started.count.internal",
				},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL", "Transaction Control"}},
		Charts: []chartDescription{
			{
				Title: "Transaction Control Mix",
				Metrics: []string{
					"sql.txn.abort.count",
					"sql.txn.begin.count",
					"sql.txn.commit.count",
					"sql.txn.rollback.count",
					"sql.txn.begin.started.count",
					"sql.txn.commit.started.count",
					"sql.txn.rollback.started.count",
				},
			},
			{
				Title: "Transaction Control Mix (Internal)",
				Metrics: []string{
					"sql.txn.abort.count.internal",
					"sql.txn.begin.count.internal",
					"sql.txn.commit.count.internal",
					"sql.txn.rollback.count.internal",
					"sql.txn.begin.started.count.internal",
					"sql.txn.commit.started.count.internal",
					"sql.txn.rollback.started.count.internal",
				},
			},
			{
				Title: "Savepoints",
				Metrics: []string{
					"sql.savepoint.count",
					"sql.savepoint.count.internal",
					"sql.savepoint.started.count",
					"sql.savepoint.started.count.internal",
					"sql.savepoint.rollback.count",
					"sql.savepoint.rollback.count.internal",
					"sql.savepoint.rollback.started.count",
					"sql.savepoint.rollback.started.count.internal",
					"sql.savepoint.release.count",
					"sql.savepoint.release.count.internal",
					"sql.savepoint.release.started.count",
					"sql.savepoint.release.started.count.internal",
				},
				AxisLabel: "SQL Statements",
			},
			{
				Title: "Restarts",
				Metrics: []string{
					"sql.restart_savepoint.count",
					"sql.restart_savepoint.release.count",
					"sql.restart_savepoint.rollback.count",
					"sql.restart_savepoint.started.count",
					"sql.restart_savepoint.release.started.count",
					"sql.restart_savepoint.rollback.started.count",
				},
			},
			{
				Title: "Restarts (Internal)",
				Metrics: []string{
					"sql.restart_savepoint.count.internal",
					"sql.restart_savepoint.release.count.internal",
					"sql.restart_savepoint.rollback.count.internal",
					"sql.restart_savepoint.started.count.internal",
					"sql.restart_savepoint.release.started.count.internal",
					"sql.restart_savepoint.rollback.started.count.internal",
				},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL", "Row Level TTL"}},
		Charts: []chartDescription{
			{
				Title: "Active Range Deletes",
				Metrics: []string{
					"jobs.row_level_ttl.num_active_ranges",
				},
				AxisLabel: "Num Running",
			},
			{
				Title: "Processing Count",
				Metrics: []string{
					"jobs.row_level_ttl.rows_selected",
					"jobs.row_level_ttl.rows_deleted",
				},
				AxisLabel: "Count",
			},
			{
				Title: "Processing Latency",
				Metrics: []string{
					"jobs.row_level_ttl.select_duration",
					"jobs.row_level_ttl.delete_duration",
				},
				AxisLabel: "Latency (nanoseconds)",
			},
			{
				Title: "Net Processing Latency",
				Metrics: []string{
					"jobs.row_level_ttl.range_total_duration",
				},
				AxisLabel: "Latency (nanoseconds)",
			},
			{
				Title: "Row Statistics",
				Metrics: []string{
					"jobs.row_level_ttl.total_rows",
					"jobs.row_level_ttl.total_expired_rows",
				},
				AxisLabel: "Number of Rows",
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL", "Feature Flag"}},
		Charts: []chartDescription{
			{
				Title: "Feature Flag Denials",
				Metrics: []string{
					"sql.feature_flag_denial",
				},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "Guardrails"}},
		Charts: []chartDescription{
			{
				Title: "Transaction Row Count Limit Violations",
				Metrics: []string{
					"sql.guardrails.transaction_rows_written_log.count",
					"sql.guardrails.transaction_rows_written_log.count.internal",
					"sql.guardrails.transaction_rows_written_err.count",
					"sql.guardrails.transaction_rows_written_err.count.internal",
					"sql.guardrails.transaction_rows_read_log.count",
					"sql.guardrails.transaction_rows_read_log.count.internal",
					"sql.guardrails.transaction_rows_read_err.count",
					"sql.guardrails.transaction_rows_read_err.count.internal",
				},
				AxisLabel: "Transactions",
			},
			{
				Title: "Maximum Row Size Violations",
				Metrics: []string{
					"sql.guardrails.max_row_size_log.count",
					"sql.guardrails.max_row_size_log.count.internal",
					"sql.guardrails.max_row_size_err.count",
					"sql.guardrails.max_row_size_err.count.internal",
				},
				AxisLabel: "Rows",
			},
			{
				Title: "Rejected Large Full Table or Index Scans",
				Metrics: []string{
					"sql.guardrails.full_scan_rejected.count",
					"sql.guardrails.full_scan_rejected.count.internal",
				},
				AxisLabel: "SQL Statements",
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "Block Cache"}},
		Charts: []chartDescription{
			{
				Title: "Size",
				Metrics: []string{
					"rocksdb.block.cache.pinned-usage",
					"rocksdb.block.cache.usage",
				},
			},
			{
				Title: "Success",
				Metrics: []string{
					"rocksdb.block.cache.hits",
					"rocksdb.block.cache.misses",
				},
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "Encryption at Rest"}},
		Charts: []chartDescription{
			{
				Title:   "Algorithm Enum",
				Metrics: []string{"rocksdb.encryption.algorithm"},
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "Overview"}},
		Charts: []chartDescription{
			{
				Title: "Bloom Filter",
				Metrics: []string{
					"rocksdb.bloom.filter.prefix.checked",
					"rocksdb.bloom.filter.prefix.useful",
				},
			},
			{
				Title:   "Compactions",
				Metrics: []string{"rocksdb.compactions"},
			},
			{
				Title:   "Flushes",
				Metrics: []string{"rocksdb.flushes"},
			},
			{
				Title:   "Index & Filter Block Size",
				Metrics: []string{"rocksdb.table-readers-mem-estimate"},
			},
			{
				Title:   "Memtable",
				Metrics: []string{"rocksdb.memtable.total-size"},
			},
			{
				Title:   "Read Amplification",
				Metrics: []string{"rocksdb.read-amplification"},
			},
			{
				Title:   "Pending Compaction",
				Metrics: []string{"rocksdb.estimated-pending-compaction"},
			},
			{
				Title:   "L0 Sublevels",
				Metrics: []string{"storage.l0-sublevels"},
			},
			{
				Title:   "L0 Files",
				Metrics: []string{"storage.l0-num-files"},
			},
			{
				Title:   "Ingestion",
				Metrics: []string{"rocksdb.ingested-bytes"},
			},
			{
				Title: "Flush & Compaction",
				Metrics: []string{
					"rocksdb.compacted-bytes-read",
					"rocksdb.compacted-bytes-written",
					"rocksdb.flushed-bytes",
				},
				AxisLabel: "Bytes",
			},
			{
				Title:   "Stalls",
				Metrics: []string{"storage.write-stalls"},
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "SSTables"}},
		Charts: []chartDescription{
			{
				Title:   "Count",
				Metrics: []string{"rocksdb.num-sstables"},
			},
			{
				Title: "Ingestions",
				Metrics: []string{
					"addsstable.copies",
					"addsstable.applications",
					"addsstable.proposals",
					"addsstable.aswrites",
				},
			},
			{
				Title: "Ingestion Delays",
				Metrics: []string{
					"addsstable.delay.total",
					"addsstable.delay.enginebackpressure",
				},
			},
		},
	},
	{
		Organization: [][]string{{DistributionLayer, "Bulk", "Egress"}},
		Charts: []chartDescription{
			{
				Title: "Export Delays",
				Metrics: []string{
					"exportrequest.delay.total",
				},
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "Storage", "KV"}},
		Charts: []chartDescription{
			{
				Title:     "Counts",
				AxisLabel: "MVCC Keys & Values",
				Metrics: []string{
					"intentcount",
					"keycount",
					"livecount",
					"syscount",
					"valcount",
				},
			},
			{
				Title:   "Cumultative Age of Non-Live Data",
				Metrics: []string{"gcbytesage"},
			},
			{
				Title:   "Cumultative Intent Age",
				Metrics: []string{"intentage"},
			},
			{
				Title: "Size",
				Metrics: []string{
					"abortspanbytes",
					"intentbytes",
					"keybytes",
					"livebytes",
					"sysbytes",
					"totalbytes",
					"valbytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "Storage", "Overview"}},
		Charts: []chartDescription{
			{
				Title: "Capacity",
				Metrics: []string{
					"capacity.available",
					"capacity",
					"capacity.reserved",
					"capacity.used",
				},
			},
			{
				Title: "Disk Health",
				Metrics: []string{
					"storage.disk-slow",
					"storage.disk-stalled",
				},
			},
		},
	},
	{
		Organization: [][]string{{Timeseries, "Maintenance Queue"}},
		Charts: []chartDescription{
			{
				Title:   "Pending",
				Metrics: []string{"queue.tsmaintenance.pending"},
			},
			{
				Title: "Successes",
				Metrics: []string{
					"queue.tsmaintenance.process.success",
					"queue.tsmaintenance.process.failure",
				},
			},
			{
				Title:   "Time Spent",
				Metrics: []string{"queue.tsmaintenance.processingnanos"},
			},
		},
	},
	{
		Organization: [][]string{{Timeseries, "Overview"}},
		Charts: []chartDescription{
			{
				Title:   "Count",
				Metrics: []string{"timeseries.write.samples"},
			},
			{
				Title:   "Error Count",
				Metrics: []string{"timeseries.write.errors"},
			},
			{
				Title:   "Size",
				Metrics: []string{"timeseries.write.bytes"},
			},
		},
	},
	{
		Organization: [][]string{{Jobs, "Schedules", "Daemon"}},
		Charts: []chartDescription{
			{
				Title: "Round",
				Metrics: []string{
					"schedules.round.schedules-ready-to-run",
					"schedules.round.reschedule-skip",
					"schedules.round.reschedule-wait",
					"schedules.round.jobs-started",
					"schedules.round.num-jobs-running",
				},
				AxisLabel: "Count",
			},
			{
				Title: "Total",
				Metrics: []string{
					"schedules.malformed",
					"schedules.error",
					"schedules.total.started",
					"schedules.total.succeeded",
					"schedules.total.failed",
				},
				AxisLabel: "Count",
			},
		},
	},
	{
		Organization: [][]string{{Jobs, "Schedules", "Backup"}},
		Charts: []chartDescription{
			{
				Title: "Counts",
				Metrics: []string{
					"schedules.BACKUP.started",
					"schedules.BACKUP.succeeded",
					"schedules.BACKUP.failed",
				},
			},
		},
	},
	{
		Organization: [][]string{{Jobs, "Schedules", "SQL Stats"}},
		Charts: []chartDescription{
			{
				Title: "Counts",
				Metrics: []string{
					"schedules.scheduled-sql-stats-compaction-executor.started",
					"schedules.scheduled-sql-stats-compaction-executor.succeeded",
					"schedules.scheduled-sql-stats-compaction-executor.failed",
				},
			},
		},
	},
	{
		Organization: [][]string{{Jobs, "Execution"}},
		Charts: []chartDescription{
			{
				Title: "Currently Running",
				Metrics: []string{
					"jobs.auto_create_stats.currently_running",
					"jobs.backup.currently_running",
					"jobs.changefeed.currently_running",
					"jobs.create_stats.currently_running",
					"jobs.import.currently_running",
					"jobs.restore.currently_running",
					"jobs.schema_change.currently_running",
					"jobs.new_schema_change.currently_running",
					"jobs.schema_change_gc.currently_running",
					"jobs.typedesc_schema_change.currently_running",
					"jobs.stream_ingestion.currently_running",
					"jobs.migration.currently_running",
					"jobs.auto_span_config_reconciliation.currently_running",
					"jobs.auto_sql_stats_compaction.currently_running",
					"jobs.stream_replication.currently_running",
				},
			},
			{
				Title: "Currently Idle",
				Metrics: []string{
					"jobs.auto_create_stats.currently_idle",
					"jobs.auto_span_config_reconciliation.currently_idle",
					"jobs.auto_sql_stats_compaction.currently_idle",
					"jobs.backup.currently_idle",
					"jobs.changefeed.currently_idle",
					"jobs.create_stats.currently_idle",
					"jobs.import.currently_idle",
					"jobs.migration.currently_idle",
					"jobs.new_schema_change.currently_idle",
					"jobs.restore.currently_idle",
					"jobs.schema_change.currently_idle",
					"jobs.schema_change_gc.currently_idle",
					"jobs.stream_ingestion.currently_idle",
					"jobs.stream_replication.currently_idle",
					"jobs.typedesc_schema_change.currently_idle",
				},
			},
			{
				Title: "Auto Create Stats",
				Metrics: []string{
					"jobs.auto_create_stats.fail_or_cancel_completed",
					"jobs.auto_create_stats.fail_or_cancel_failed",
					"jobs.auto_create_stats.fail_or_cancel_retry_error",
					"jobs.auto_create_stats.resume_completed",
					"jobs.auto_create_stats.resume_failed",
					"jobs.auto_create_stats.resume_retry_error",
				},
				Rate: DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
			},
			{
				Title: "Backup",
				Metrics: []string{
					"jobs.backup.fail_or_cancel_completed",
					"jobs.backup.fail_or_cancel_failed",
					"jobs.backup.fail_or_cancel_retry_error",
					"jobs.backup.resume_completed",
					"jobs.backup.resume_failed",
					"jobs.backup.resume_retry_error",
				},
				Rate: DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
			},
			{
				Title: "Changefeed",
				Metrics: []string{
					"jobs.changefeed.fail_or_cancel_completed",
					"jobs.changefeed.fail_or_cancel_failed",
					"jobs.changefeed.fail_or_cancel_retry_error",
					"jobs.changefeed.resume_completed",
					"jobs.changefeed.resume_failed",
					"jobs.changefeed.resume_retry_error",
				},
				Rate: DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
			},
			{
				Title: "Create Stats",
				Metrics: []string{
					"jobs.create_stats.fail_or_cancel_completed",
					"jobs.create_stats.fail_or_cancel_failed",
					"jobs.create_stats.fail_or_cancel_retry_error",
					"jobs.create_stats.resume_completed",
					"jobs.create_stats.resume_failed",
					"jobs.create_stats.resume_retry_error",
				},
				Rate: DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
			},
			{
				Title: "Import",
				Metrics: []string{
					"jobs.import.fail_or_cancel_completed",
					"jobs.import.fail_or_cancel_failed",
					"jobs.import.fail_or_cancel_retry_error",
					"jobs.import.resume_completed",
					"jobs.import.resume_failed",
					"jobs.import.resume_retry_error",
				},
				Rate: DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
			},
			{
				Title: "Restore",
				Metrics: []string{
					"jobs.restore.fail_or_cancel_completed",
					"jobs.restore.fail_or_cancel_failed",
					"jobs.restore.fail_or_cancel_retry_error",
					"jobs.restore.resume_completed",
					"jobs.restore.resume_failed",
					"jobs.restore.resume_retry_error",
				},
				Rate: DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
			},
			{
				Title: "Schema Change",
				Metrics: []string{
					"jobs.schema_change.fail_or_cancel_completed",
					"jobs.schema_change.fail_or_cancel_failed",
					"jobs.schema_change.fail_or_cancel_retry_error",
					"jobs.schema_change.resume_completed",
					"jobs.schema_change.resume_failed",
					"jobs.schema_change.resume_retry_error",
				},
				Rate: DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
			},
			{
				Title: "Schema Change (New Implementation)",
				Metrics: []string{
					"jobs.new_schema_change.fail_or_cancel_completed",
					"jobs.new_schema_change.fail_or_cancel_failed",
					"jobs.new_schema_change.fail_or_cancel_retry_error",
					"jobs.new_schema_change.resume_completed",
					"jobs.new_schema_change.resume_failed",
					"jobs.new_schema_change.resume_retry_error",
				},
				Rate: DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
			},
			{
				Title: "Schema Change GC",
				Metrics: []string{
					"jobs.schema_change_gc.fail_or_cancel_completed",
					"jobs.schema_change_gc.fail_or_cancel_failed",
					"jobs.schema_change_gc.fail_or_cancel_retry_error",
					"jobs.schema_change_gc.resume_completed",
					"jobs.schema_change_gc.resume_failed",
					"jobs.schema_change_gc.resume_retry_error",
				},
				Rate: DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
			},
			{
				Title: "Type Descriptor Change",
				Metrics: []string{
					"jobs.typedesc_schema_change.fail_or_cancel_completed",
					"jobs.typedesc_schema_change.fail_or_cancel_failed",
					"jobs.typedesc_schema_change.fail_or_cancel_retry_error",
					"jobs.typedesc_schema_change.resume_completed",
					"jobs.typedesc_schema_change.resume_failed",
					"jobs.typedesc_schema_change.resume_retry_error",
				},
				Rate: DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
			},
			{
				Title: "Stream Ingestion",
				Metrics: []string{
					"jobs.stream_ingestion.fail_or_cancel_completed",
					"jobs.stream_ingestion.fail_or_cancel_failed",
					"jobs.stream_ingestion.fail_or_cancel_retry_error",
					"jobs.stream_ingestion.resume_completed",
					"jobs.stream_ingestion.resume_failed",
					"jobs.stream_ingestion.resume_retry_error",
				},
			},
			{
				Title: "Stream Replication",
				Metrics: []string{
					"jobs.stream_replication.fail_or_cancel_completed",
					"jobs.stream_replication.fail_or_cancel_failed",
					"jobs.stream_replication.fail_or_cancel_retry_error",
					"jobs.stream_replication.resume_completed",
					"jobs.stream_replication.resume_failed",
					"jobs.stream_replication.resume_retry_error",
				},
			},
			{
				Title: "Long Running Migrations",
				Metrics: []string{
					"jobs.migration.fail_or_cancel_completed",
					"jobs.migration.fail_or_cancel_failed",
					"jobs.migration.fail_or_cancel_retry_error",
					"jobs.migration.resume_completed",
					"jobs.migration.resume_failed",
					"jobs.migration.resume_retry_error",
				},
			},
			{
				Title: "Auto Span Config Reconciliation",
				Metrics: []string{
					"jobs.auto_span_config_reconciliation.fail_or_cancel_completed",
					"jobs.auto_span_config_reconciliation.fail_or_cancel_failed",
					"jobs.auto_span_config_reconciliation.fail_or_cancel_retry_error",
					"jobs.auto_span_config_reconciliation.resume_completed",
					"jobs.auto_span_config_reconciliation.resume_failed",
					"jobs.auto_span_config_reconciliation.resume_retry_error"},
			},
			{
				Title: "SQL Stats Compaction",
				Metrics: []string{
					"jobs.auto_sql_stats_compaction.fail_or_cancel_completed",
					"jobs.auto_sql_stats_compaction.fail_or_cancel_failed",
					"jobs.auto_sql_stats_compaction.fail_or_cancel_retry_error",
					"jobs.auto_sql_stats_compaction.resume_completed",
					"jobs.auto_sql_stats_compaction.resume_failed",
					"jobs.auto_sql_stats_compaction.resume_retry_error",
				},
			},
		},
	},
	{
		Organization: [][]string{{Jobs, "Registry"}},
		Charts: []chartDescription{
			{
				Title: "Jobs Registry Stats",
				Metrics: []string{
					"jobs.adopt_iterations",
					"jobs.claimed_jobs",
					"jobs.resumed_claimed_jobs",
				},
				AxisLabel: "Count",
			},
		},
	},
	{
		Organization: [][]string{{Process, "Node", "Admission"}},
		Charts: []chartDescription{
			{
				Title: "Work Queue Admission Counter",
				Metrics: []string{
					"admission.requested.kv",
					"admission.admitted.kv",
					"admission.errored.kv",
					"admission.requested.kv-stores",
					"admission.admitted.kv-stores",
					"admission.errored.kv-stores",
					"admission.requested.sql-kv-response",
					"admission.admitted.sql-kv-response",
					"admission.errored.sql-kv-response",
					"admission.requested.sql-sql-response",
					"admission.admitted.sql-sql-response",
					"admission.errored.sql-sql-response",
					"admission.requested.sql-leaf-start",
					"admission.admitted.sql-leaf-start",
					"admission.errored.sql-leaf-start",
					"admission.requested.sql-root-start",
					"admission.admitted.sql-root-start",
					"admission.errored.sql-root-start",
				},
			},
			{
				Title: "Work Queue Length",
				Metrics: []string{
					"admission.wait_queue_length.kv",
					"admission.wait_queue_length.kv-stores",
					"admission.wait_queue_length.sql-kv-response",
					"admission.wait_queue_length.sql-sql-response",
					"admission.wait_queue_length.sql-leaf-start",
					"admission.wait_queue_length.sql-root-start",
				},
			},
			{
				Title: "Work Queue Admission Latency Sum",
				Metrics: []string{
					"admission.wait_sum.kv",
					"admission.wait_sum.kv-stores",
					"admission.wait_sum.sql-kv-response",
					"admission.wait_sum.sql-sql-response",
					"admission.wait_sum.sql-leaf-start",
					"admission.wait_sum.sql-root-start",
				},
			},
			{
				Title: "Work Queue Latency Distribution",
				Metrics: []string{
					"admission.wait_durations.kv",
					"admission.wait_durations.kv-stores",
					"admission.wait_durations.sql-kv-response",
					"admission.wait_durations.sql-sql-response",
					"admission.wait_durations.sql-leaf-start",
					"admission.wait_durations.sql-root-start",
				},
			},
			{
				Title: "Granter",
				Metrics: []string{
					"admission.granter.total_slots.kv",
					"admission.granter.used_slots.kv",
					"admission.granter.used_slots.sql-leaf-start",
					"admission.granter.used_slots.sql-root-start",
				},
			},
			{
				Title: "IO Tokens Exhausted Duration Sum",
				Metrics: []string{
					"admission.granter.io_tokens_exhausted_duration.kv",
				},
			},
		},
	},
}
