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

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
)

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
					"sys.cpu.host.combined.percent-normalized",
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
				Title: "Rangefeed",
				Metrics: []string{
					"distsender.rangefeed.total_ranges",
					"distsender.rangefeed.catchup_ranges",
				},
			},
			{
				Title: "Rangefeed Errors",
				Metrics: []string{
					"distsender.rangefeed.error_catchup_ranges",
				},
			},
			{
				Title: "Stuck Rangefeeds",
				Metrics: []string{
					"distsender.rangefeed.restart_stuck",
				},
			},
			{
				Title: "Restarted Ranges",
				Metrics: []string{
					"distsender.rangefeed.restart_ranges",
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
					"rpc.method.increment.recv",
					"rpc.method.initput.recv",
					"rpc.method.leaseinfo.recv",
					"rpc.method.merge.recv",
					"rpc.method.migrate.recv",
					"rpc.method.probe.recv",
					"rpc.method.pushtxn.recv",
					"rpc.method.put.recv",
					"rpc.method.queryintent.recv",
					"rpc.method.querylocks.recv",
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
					"rpc.method.subsume.recv",
					"rpc.method.transferlease.recv",
					"rpc.method.truncatelog.recv",
					"rpc.method.writebatch.recv",
					"rpc.method.isspanempty.recv",
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
					"distsender.rpc.increment.sent",
					"distsender.rpc.initput.sent",
					"distsender.rpc.leaseinfo.sent",
					"distsender.rpc.merge.sent",
					"distsender.rpc.migrate.sent",
					"distsender.rpc.pushtxn.sent",
					"distsender.rpc.put.sent",
					"distsender.rpc.queryintent.sent",
					"distsender.rpc.querylocks.sent",
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
					"distsender.rpc.subsume.sent",
					"distsender.rpc.transferlease.sent",
					"distsender.rpc.probe.sent",
					"distsender.rpc.truncatelog.sent",
					"distsender.rpc.writebatch.sent",
					"distsender.rpc.isspanempty.sent",
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
					"rpc.heartbeats.nominal",
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
				Title: "Paused Followers",
				Metrics: []string{
					"admission.raft.paused_replicas",
				},
			},
			{
				Title: "Paused Followers Dropped Messages",
				Metrics: []string{
					"admission.raft.paused_replicas_dropped_msgs",
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
				Title: "Rangefeed Registrations",
				Metrics: []string{
					"kv.rangefeed.registrations",
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
					"range.snapshots.delegate.successes",
					"range.snapshots.delegate.failures",
					"range.snapshots.recv-failed",
					"range.snapshots.recv-unusable",
				},
			},
			{
				Title: "Snapshot Queues",
				Metrics: []string{
					"range.snapshots.send-queue",
					"range.snapshots.recv-queue",
					"range.snapshots.send-in-progress",
					"range.snapshots.recv-in-progress",
					"range.snapshots.send-total-in-progress",
					"range.snapshots.recv-total-in-progress",
					"range.snapshots.delegate.in-progress",
				},
			},
			{
				Title: "Snapshot Bytes",
				Metrics: []string{
					"range.snapshots.rcvd-bytes",
					"range.snapshots.sent-bytes",
					"range.snapshots.recovery.rcvd-bytes",
					"range.snapshots.recovery.sent-bytes",
					"range.snapshots.rebalancing.rcvd-bytes",
					"range.snapshots.rebalancing.sent-bytes",
					"range.snapshots.unknown.rcvd-bytes",
					"range.snapshots.unknown.sent-bytes",
					"range.snapshots.delegate.sent-bytes",
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
				Title:   "Rebalancing Exhausted Options",
				Metrics: []string{"rebalancing.state.imbalanced_overfull_options_exhausted"},
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
			{
				Title:   "Requests Per Second",
				Metrics: []string{"rebalancing.requestspersecond"},
			},
			{
				Title:   "Keys Read Per Second",
				Metrics: []string{"rebalancing.readspersecond"},
			},
			{
				Title:   "Bytes Read Per Second",
				Metrics: []string{"rebalancing.readbytespersecond"},
			},
			{
				Title:   "Bytes Written Per Second",
				Metrics: []string{"rebalancing.writebytespersecond"},
			},
			{
				Title:   "CPU Nanos Used Per Second",
				Metrics: []string{"rebalancing.cpunanospersecond"},
			},
			{
				Title:   "Replica CPU Nanos Used Per Second",
				Metrics: []string{"rebalancing.replicas.cpunanospersecond"},
			},
			{
				Title:   "Replica Queries Per Second",
				Metrics: []string{"rebalancing.replicas.queriespersecond"},
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
				Title: "Allocator Load-Based Lease Transfer Decisions",
				Metrics: []string{
					"kv.allocator.load_based_lease_transfers.follow_the_workload",
					"kv.allocator.load_based_lease_transfers.should_transfer",
					"kv.allocator.load_based_lease_transfers.missing_stats_for_existing_stores",
					"kv.allocator.load_based_lease_transfers.delta_not_significant",
					"kv.allocator.load_based_lease_transfers.existing_not_overfull",
					"kv.allocator.load_based_lease_transfers.cannot_find_better_candidate",
				},
			},
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Rebalancing"},
			{ReplicationLayer, "Replicas"},
		},
		Charts: []chartDescription{
			{
				Title: "Allocator Load-Based Lease Transfer Decisions",
				Metrics: []string{
					"kv.allocator.load_based_replica_rebalancing.should_transfer",
					"kv.allocator.load_based_replica_rebalancing.missing_stats_for_existing_store",
					"kv.allocator.load_based_replica_rebalancing.delta_not_significant",
					"kv.allocator.load_based_replica_rebalancing.existing_not_overfull",
					"kv.allocator.load_based_replica_rebalancing.cannot_find_better_candidate",
				},
			},
		},
	},
	{
		Organization: [][]string{{DistributionLayer, "Load", "Splitter"}},
		Charts: []chartDescription{
			{
				Title: "Load Splitter",
				Metrics: []string{
					"kv.loadsplitter.popularkey",
					"kv.loadsplitter.nosplitkey",
				},
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
			{
				Title:   "Sized Based Splits",
				Metrics: []string{"queue.split.size_based"},
			},
			{
				Title:   "Load Based Splits",
				Metrics: []string{"queue.split.load_based"},
			},
			{
				Title:   "Span Configuration Based Splits",
				Metrics: []string{"queue.split.span_config_based"},
			},
		},
	},
	{
		Organization: [][]string{
			{Process, "Span Configs"},
		},
		Charts: []chartDescription{
			{
				Title: "KVSubscriber Lag Metrics",
				Metrics: []string{
					"spanconfig.kvsubscriber.oldest_protected_record_nanos",
					"spanconfig.kvsubscriber.update_behind_nanos",
				},
			},
			{
				Title: "KVSubscriber Protected Record Count",
				Metrics: []string{
					"spanconfig.kvsubscriber.protected_record_count",
				},
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
			{
				Title: "Duration",
				Metrics: []string{
					"kv.prober.write.quarantine.oldest_duration",
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
				Title:   "Range Keys with GC'able Data",
				Metrics: []string{"queue.gc.info.numrangekeysaffected"},
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
			{
				Title: "GC Clear Range",
				Metrics: []string{
					"queue.gc.info.clearrangesuccess",
					"queue.gc.info.clearrangefailed",
				},
			},
			{
				Title: "GC Enqueue with High Priority",
				Metrics: []string{
					"queue.gc.info.enqueuehighpriority",
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
				Title:       "Read Batches Admitted by Rate Limiter",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"kv.tenant_rate_limit.read_batches_admitted"},
			},
			{
				Title:       "Write Batches Admitted by Rate Limiter",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"kv.tenant_rate_limit.write_batches_admitted"},
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
					"rangekeycount",
					"rangevalcount",
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
					"rangekeybytes",
					"rangevalbytes",
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
				Title: "Server Side Retry",
				Metrics: []string{
					"txn.server_side_retry.write_evaluation.success",
					"txn.server_side_retry.write_evaluation.failure",
					"txn.server_side_retry.read_evaluation.success",
					"txn.server_side_retry.read_evaluation.failure",
					"txn.server_side_retry.uncertainty_interval_error.success",
					"txn.server_side_retry.uncertainty_interval_error.failure",
				},
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
				Title: "Filtered Messages",
				Metrics: []string{
					"changefeed.filtered_messages",
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
				Title: "Size Based Flushes",
				Metrics: []string{
					"changefeed.size_based_flushes",
				},
			},
			{
				Title: "Max Behind Nanos",
				Metrics: []string{
					"changefeed.max_behind_nanos",
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
					"changefeed.schemafeed.table_metadata_nanos",
				},
			},
			{
				Title: "Table History Scans",
				Metrics: []string{
					"changefeed.schemafeed.table_history_scans",
				},
			},
			{
				Title: "Event admission latency",
				Metrics: []string{
					"changefeed.admit_latency",
				},
			},
			{
				Title: "Release/Acquire",
				Metrics: []string{
					"changefeed.buffer_entries.released",
					"changefeed.buffer_entries_mem.acquired",
					"changefeed.buffer_entries_mem.released",
				},
			},
			{
				Title: "Frontier Updates",
				Metrics: []string{
					"changefeed.frontier_updates",
				},
			},
			{
				Title: "Replan Count",
				Metrics: []string{
					"changefeed.replan_count",
				},
			},
			{
				Title: "Nprocs Consume Event Nanos",
				Metrics: []string{
					"changefeed.nprocs_consume_event_nanos",
				},
			},
			{
				Title: "Nprocs Flush Nanos",
				Metrics: []string{
					"changefeed.nprocs_flush_nanos",
				},
			},
			{
				Title: "Nprocs In Flight Count",
				Metrics: []string{
					"changefeed.nprocs_in_flight_count",
				},
			},
			{
				Title: "Flushed Bytes",
				Metrics: []string{
					"changefeed.flushed_bytes",
				},
			},
			{
				Title: "Forwarded Resolved Messages",
				Metrics: []string{
					"changefeed.forwarded_resolved_messages",
				},
			},
			{
				Title: "Messages Size",
				Metrics: []string{
					"changefeed.message_size_hist",
				},
			},
			{
				Title: "Commits Latency",
				Metrics: []string{
					"changefeed.commit_latency",
				},
			},
			{
				Title: "Time spent",
				Metrics: []string{
					"changefeed.checkpoint_hist_nanos",
					"changefeed.flush_hist_nanos",
					"changefeed.sink_batch_hist_nanos",
					"changefeed.parallel_io_queue_nanos",
				},
			},
			{
				Title: "Backfill",
				Metrics: []string{
					"changefeed.backfill_count",
					"changefeed.backfill_pending_ranges",
				},
			},
			{
				Title: "Time Spent Waiting",
				Metrics: []string{
					"changefeed.buffer_pushback_nanos",
					"changefeed.queue_time_nanos",
				},
			},
			{
				Title: "Throttled Time",
				Metrics: []string{
					"changefeed.bytes.messages_pushback_nanos",
					"changefeed.messages.messages_pushback_nanos",
					"changefeed.flush.messages_pushback_nanos",
				},
			},
			{
				Title: "Sink IO",
				Metrics: []string{
					"changefeed.sink_io_inflight",
				},
			},
			{
				Title: "Batching",
				Metrics: []string{
					"changefeed.batch_reduction_count",
				},
			},
			{
				Title: "Internal Retries",
				Metrics: []string{
					"changefeed.internal_retry_message_count",
				},
			},
			{
				Title: "Schema Registry Retries",
				Metrics: []string{
					"changefeed.schema_registry.retry_count",
				},
			},
			{
				Title: "Schema Registry Registrations",
				Metrics: []string{
					"changefeed.schema_registry.registrations",
				},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Stream Replication"}},
		Charts: []chartDescription{
			{
				Title:   "Currently Running",
				Metrics: []string{"replication.running"},
			},
			{
				Title: "Event admission latency",
				Metrics: []string{
					"replication.admit_latency",
				},
			},
			{
				Title: "Commits Latency",
				Metrics: []string{
					"replication.commit_latency",
				},
			},
			{
				Title: "Time spent",
				Metrics: []string{
					"replication.flush_hist_nanos",
				},
			},
			{
				Title: "Ingested Events",
				Metrics: []string{
					"replication.events_ingested",
					"replication.resolved_events_ingested",
				},
			},
			{
				Title: "Flushes",
				Metrics: []string{
					"replication.flushes",
				},
			},
			{
				Title: "Logical Bytes",
				Metrics: []string{
					"replication.logical_bytes",
				},
			},
			{
				Title: "SST Bytes",
				Metrics: []string{
					"replication.sst_bytes",
				},
			},
			{
				Title:   "Earliest Data Processor Checkpoint Span",
				Metrics: []string{"replication.earliest_data_checkpoint_span"},
			},
			{
				Title:   "Latest Data Processor Checkpoint Span",
				Metrics: []string{"replication.latest_data_checkpoint_span"},
			},
			{
				Title:   "Data Checkpoint Span Count",
				Metrics: []string{"replication.data_checkpoint_span_count"},
			},
			{
				Title:   "Frontier Checkpoint Span Count",
				Metrics: []string{"replication.frontier_checkpoint_span_count"},
			},
			{
				Title:   "Frontier Lag",
				Metrics: []string{"replication.frontier_lag_nanos"},
			},
			{
				Title:   "Job Progress Updates",
				Metrics: []string{"replication.job_progress_updates"},
			},
			{
				Title:   "Ranges To Revert",
				Metrics: []string{"replication.cutover_progress"},
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
				Title:   "Lease Request Latency",
				Metrics: []string{"leases.requests.latency"},
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
					"replicas.leaders_invalid_lease",
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
				Title:   "Quota Pool Utilization (0-100)",
				Metrics: []string{"raft.quota_pool.percent_used"},
			},
			{
				Title:   "Commands Count",
				Metrics: []string{"raft.commandsapplied"},
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
		Organization: [][]string{{ReplicationLayer, "Raft", "Receive Queue"}},
		Charts: []chartDescription{
			{
				Title: "Dropped Entry Count",
				Metrics: []string{
					"raft.rcvd.dropped",
				},
			},
			{
				Title: "Dropped Entry Bytes",
				Metrics: []string{
					"raft.rcvd.dropped_bytes",
				},
			},
			{
				Title: "Entry Bytes in Queue",
				Metrics: []string{
					"raft.rcvd.queued_bytes",
				},
			},
			{
				Title: "Entry Bytes Stepped into Raft",
				Metrics: []string{
					"raft.rcvd.stepped_bytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Charts: []chartDescription{
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
		Organization: [][]string{{ReplicationLayer, "Raft", "Transport"}},
		Charts: []chartDescription{
			{
				Title:   "Send Queue Messages Count",
				Metrics: []string{"raft.transport.send-queue-size"},
			},
			{
				Title:   "Send Queue Byte Size",
				Metrics: []string{"raft.transport.send-queue-bytes"},
			},
			{
				Title:   "Raft Message Sends Dropped",
				Metrics: []string{"raft.transport.sends-dropped"},
			},
			{
				Title:   "Raft Messages Sent",
				Metrics: []string{"raft.transport.sent"},
			},
			{
				Title:   "Raft Messages Received",
				Metrics: []string{"raft.transport.rcvd"},
			},
			{
				Title:   "Reverse Messages Sent",
				Metrics: []string{"raft.transport.reverse-sent"},
			},
			{
				Title:   "Reverse Messages Received",
				Metrics: []string{"raft.transport.reverse-rcvd"},
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
				Title: "Add Replica Count",
				Metrics: []string{
					"queue.replicate.addreplica",
					"queue.replicate.addvoterreplica",
					"queue.replicate.addnonvoterreplica",
				},
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
				Title: "Rebalance Count",
				Metrics: []string{
					"queue.replicate.rebalancereplica",
					"queue.replicate.rebalancevoterreplica",
					"queue.replicate.rebalancenonvoterreplica",
				},
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
					"queue.replicate.removedeadvoterreplica",
					"queue.replicate.removedeadnonvoterreplica",
					"queue.replicate.removereplica",
					"queue.replicate.removevoterreplica",
					"queue.replicate.removenonvoterreplica",
					"queue.replicate.removelearnerreplica",
					"queue.replicate.removedecommissioningreplica",
					"queue.replicate.removedecommissioningvoterreplica",
					"queue.replicate.removedecommissioningnonvoterreplica",
				},
			},
			{
				Title: "Successes by Action",
				Metrics: []string{
					"queue.replicate.addreplica.success",
					"queue.replicate.removereplica.success",
					"queue.replicate.replacedeadreplica.success",
					"queue.replicate.removedeadreplica.success",
					"queue.replicate.replacedecommissioningreplica.success",
					"queue.replicate.removedecommissioningreplica.success",
				},
			},
			{
				Title: "Errors by Action",
				Metrics: []string{
					"queue.replicate.addreplica.error",
					"queue.replicate.removereplica.error",
					"queue.replicate.replacedeadreplica.error",
					"queue.replicate.removedeadreplica.error",
					"queue.replicate.replacedecommissioningreplica.error",
					"queue.replicate.removedecommissioningreplica.error",
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
				Title:   "Total",
				Metrics: []string{"sql.distsql.flows.total"},
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
					"sql.hydrated_udf_cache.hits",
					"sql.hydrated_udf_cache.misses",
					"sql.hydrated_schema_cache.hits",
					"sql.hydrated_schema_cache.misses",
					"sql.hydrated_type_cache.hits",
					"sql.hydrated_type_cache.misses",
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
	}, {
		Organization: [][]string{{SQLLayer, "SQL Stats", "Insights"}},
		Charts: []chartDescription{
			{
				Title:   "Current number of statement fingerprints being monitored for anomaly detection",
				Metrics: []string{"sql.insights.anomaly_detection.fingerprints"},
			},
			{
				Title:   "Current memory used to support anomaly detection",
				Metrics: []string{"sql.insights.anomaly_detection.memory"},
			},
			{
				Title:   "Evictions of fingerprint latency summaries due to memory pressure",
				Metrics: []string{"sql.insights.anomaly_detection.evictions"},
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
			{
				Title:   "Resolver Queue Size",
				Metrics: []string{"sql.contention.resolver.queue_size"},
			},
			{
				Title:   "Resolver Retries",
				Metrics: []string{"sql.contention.resolver.retries"},
			},
			{
				Title:   "Resolver Failures",
				Metrics: []string{"sql.contention.resolver.failed_resolutions"},
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
		Organization: [][]string{{SQLLayer, "Schema Telemetry"}},
		Charts: []chartDescription{
			{
				Title: "Jobs Running",
				Metrics: []string{
					"jobs.auto_schema_telemetry.currently_running",
					"jobs.auto_schema_telemetry.currently_idle",
				},
			},
			{
				Title: "Jobs Statistics",
				Metrics: []string{
					"jobs.auto_schema_telemetry.fail_or_cancel_completed",
					"jobs.auto_schema_telemetry.fail_or_cancel_failed",
					"jobs.auto_schema_telemetry.fail_or_cancel_retry_error",
					"jobs.auto_schema_telemetry.resume_completed",
					"jobs.auto_schema_telemetry.resume_failed",
					"jobs.auto_schema_telemetry.resume_retry_error",
				},
			},
			{
				Title: "Scheduled Jobs Statistics",
				Metrics: []string{
					"schedules.scheduled-schema-telemetry-executor.succeeded",
					"schedules.scheduled-schema-telemetry-executor.started",
					"schedules.scheduled-schema-telemetry-executor.failed",
				},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Charts: []chartDescription{
			{
				Title:   "Session All",
				Metrics: []string{"sql.mem.internal.session.max"},
			},
			{
				Title:   "Session Current",
				Metrics: []string{"sql.mem.internal.session.current"},
			},
			{
				Title:   "Prepared Statements All",
				Metrics: []string{"sql.mem.internal.session.prepared.max"},
			},
			{
				Title:   "Prepared Statements Current",
				Metrics: []string{"sql.mem.internal.session.prepared.current"},
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
			{
				Title:   "Prepared Statements Current",
				Metrics: []string{"sql.mem.sql.session.prepared.current"},
			},
			{
				Title:   "Prepared Statements Max",
				Metrics: []string{"sql.mem.sql.session.prepared.max"},
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
		Organization: [][]string{{SQLLayer, "SQL, Prior to tenant selection"}},
		Charts: []chartDescription{
			{
				Title: "New Connections",
				Metrics: []string{
					"sql.pre_serve.new_conns",
				},
			},
			{
				Title: "Connection Failures",
				Metrics: []string{
					"sql.pre_serve.conn.failures",
				},
				AxisLabel: "Failures",
			},
			{
				Title: "Byte I/O",
				Metrics: []string{
					"sql.pre_serve.bytesin",
					"sql.pre_serve.bytesout",
				},
			},
			{
				Title: "Memory usage, Current",
				Metrics: []string{
					"sql.pre_serve.mem.cur",
				},
			},
			{
				Title: "Memory usage, Max",
				Metrics: []string{
					"sql.pre_serve.mem.max",
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
				Title: "Connection Failures",
				Metrics: []string{
					"sql.conn.failures",
				},
				AxisLabel: "Failures",
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
				Title: "Cluster.preserve_downgrade_option Last Updated",
				Metrics: []string{
					"cluster.preserve-downgrade-option.last-updated",
				},
				AxisLabel: "Last Updated Timestamp",
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
			{
				Title: "SQL Transaction Contention",
				Metrics: []string{
					"sql.txn.contended.count",
					"sql.txn.contended.count.internal",
				},
				AxisLabel: "Transactions",
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
					"sql.copy.nonatomic.count",
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
					"sql.copy.nonatomic.started.count",
					"sql.query.started.count",
					"sql.select.started.count",
					"sql.update.started.count",
				},
			},
			{
				Title: "Mix (Internal)",
				Metrics: []string{
					"sql.delete.count.internal",
					"sql.insert.count.internal",
					"sql.misc.count.internal",
					"sql.copy.count.internal",
					"sql.copy.nonatomic.count.internal",
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
					"sql.copy.nonatomic.started.count.internal",
					"sql.query.started.count.internal",
					"sql.select.started.count.internal",
					"sql.update.started.count.internal",
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
				Title: "Active Span Deletes",
				Metrics: []string{
					"jobs.row_level_ttl.num_active_spans",
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
					"jobs.row_level_ttl.span_total_duration",
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
		Organization: [][]string{{StorageLayer, "Engine", "Block Cache"}},
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
		Organization: [][]string{{StorageLayer, "Engine", "Encryption at Rest"}},
		Charts: []chartDescription{
			{
				Title:   "Algorithm Enum",
				Metrics: []string{"rocksdb.encryption.algorithm"},
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "Engine", "Keys"}},
		Charts: []chartDescription{
			{
				Title:   "Range Key Set Count",
				Metrics: []string{"storage.keys.range-key-set.count"},
			},
			{
				Title:   "Tombstone Count",
				Metrics: []string{"storage.keys.tombstone.count"},
			},
			{
				Title:   "Pinned Keys Written",
				Metrics: []string{"storage.compactions.keys.pinned.count"},
			},
			{
				Title:   "Pinned Key Bytes Written",
				Metrics: []string{"storage.compactions.keys.pinned.bytes"},
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "Engine", "Migrations"}},
		Charts: []chartDescription{
			{
				Title:   "SSTables Marked for Compaction",
				Metrics: []string{"storage.marked-for-compaction-files"},
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "Engine", "Overview"}},
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
				Title: "Flushes",
				Metrics: []string{
					"rocksdb.flushes",
					"storage.flush.ingest.count",
				},
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
				Title: "Shared Storage Reads/Writes",
				Metrics: []string{
					"storage.shared-storage.read",
					"storage.shared-storage.write",
				},
			},
			{
				Title: "L0 Files",
				Metrics: []string{
					"storage.l0-num-files",
				},
			},
			{
				Title:   "Bytes flushed to Level 0",
				Metrics: []string{"storage.l0-bytes-flushed"},
			},
			{
				Title: "Bytes Ingested per Level",
				Metrics: []string{
					"storage.l0-bytes-ingested",
					"storage.l1-bytes-ingested",
					"storage.l2-bytes-ingested",
					"storage.l3-bytes-ingested",
					"storage.l4-bytes-ingested",
					"storage.l5-bytes-ingested",
					"storage.l6-bytes-ingested",
				},
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
					"storage.flush.ingest.table.bytes",
				},
				AxisLabel: "Bytes",
			},
			{
				Title: "Flushable Ingestions",
				Metrics: []string{
					"storage.flush.ingest.table.count",
				},
			},
			{
				Title:   "Stalls",
				Metrics: []string{"storage.write-stalls"},
			},
			{
				Title:     "Stall Duration",
				Metrics:   []string{"storage.write-stall-nanos"},
				AxisLabel: "Duration (nanos)",
			},
			{
				Title:     "Checkpoints",
				Metrics:   []string{"storage.checkpoints"},
				AxisLabel: "Directories",
			},
			{
				Title: "Bytes Used Per Level",
				Metrics: []string{
					"storage.l0-level-size",
					"storage.l1-level-size",
					"storage.l2-level-size",
					"storage.l3-level-size",
					"storage.l4-level-size",
					"storage.l5-level-size",
					"storage.l6-level-size",
				},
				AxisLabel: "Bytes",
			},
			{
				Title: "Compaction Score Per Level",
				Metrics: []string{
					"storage.l0-level-score",
					"storage.l1-level-score",
					"storage.l2-level-score",
					"storage.l3-level-score",
					"storage.l4-level-score",
					"storage.l5-level-score",
					"storage.l6-level-score",
				},
			},
			{
				Title:   "Flush Utilization",
				Metrics: []string{"storage.flush.utilization"},
			},
			{
				Title:   "WAL Fsync Latency",
				Metrics: []string{"storage.wal.fsync.latency"},
			},
			{
				Title: "Iterator Block Loads",
				Metrics: []string{
					"storage.iterator.block-load.bytes",
					"storage.iterator.block-load.cached-bytes",
				},
				AxisLabel: "Bytes",
			},
			{
				Title:     "Iterator I/O",
				Metrics:   []string{"storage.iterator.block-load.read-duration"},
				AxisLabel: "Duration (nanos)",
			},
			{
				Title: "Iterator Operations",
				Metrics: []string{
					"storage.iterator.external.seeks",
					"storage.iterator.external.steps",
					"storage.iterator.internal.seeks",
					"storage.iterator.internal.steps",
				},
				AxisLabel: "Ops",
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "Engine", "SSTables"}},
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
					"rangekeycount",
					"rangevalcount",
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
					"rangekeybytes",
					"rangevalbytes",
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
					"schedules.round.reschedule-skip",
					"schedules.round.reschedule-wait",
					"schedules.round.jobs-started",
				},
				AxisLabel: "Count",
			},
			{
				Title: "Total",
				Metrics: []string{
					"schedules.malformed",
					"schedules.error",
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
			{
				Title: "PTS Counts",
				Metrics: []string{
					"schedules.BACKUP.protected_record_count",
				},
			},
			{
				Title: "PTS Age",
				Metrics: []string{
					"schedules.BACKUP.protected_age_sec",
				},
			},
			{
				Title: "Last Completed Backups",
				Metrics: []string{
					"schedules.BACKUP.last-completed-time",
				},
			},
		},
	},
	{
		Organization: [][]string{{Jobs, "Schedules", "Changefeed"}},
		Charts: []chartDescription{
			{
				Title: "Counts",
				Metrics: []string{
					"schedules.CHANGEFEED.started",
					"schedules.CHANGEFEED.succeeded",
					"schedules.CHANGEFEED.failed",
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
				Title: "Active",
				Metrics: []string{
					"jobs.running_non_idle",
				},
			},
			jobTypeCharts("Currently Running", "currently_running"),
			jobTypeCharts("Currently Idle", "currently_idle"),
			jobTypeCharts("Currently Paused", "currently_paused"),
			jobTypeCharts("PTS Age", "protected_age_sec"),
			jobTypeCharts("PTS Record Count", "protected_record_count"),
			jobTypeCharts("Expired PTS Records", "expired_pts_records"),
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
			{
				Title: "Key Visualizer",
				Metrics: []string{
					"jobs.key_visualizer.fail_or_cancel_completed",
					"jobs.key_visualizer.fail_or_cancel_failed",
					"jobs.key_visualizer.fail_or_cancel_retry_error",
					"jobs.key_visualizer.resume_completed",
					"jobs.key_visualizer.resume_failed",
					"jobs.key_visualizer.resume_retry_error",
				},
			},
			{
				Title: "SQL Activity Updater",
				Metrics: []string{
					"jobs.auto_update_sql_activity.fail_or_cancel_completed",
					"jobs.auto_update_sql_activity.fail_or_cancel_failed",
					"jobs.auto_update_sql_activity.fail_or_cancel_retry_error",
					"jobs.auto_update_sql_activity.resume_completed",
					"jobs.auto_update_sql_activity.resume_failed",
					"jobs.auto_update_sql_activity.resume_retry_error",
				},
			},
			{
				Title: "Jobs Stats Polling Job",
				Metrics: []string{
					"jobs.poll_jobs_stats.fail_or_cancel_completed",
					"jobs.poll_jobs_stats.fail_or_cancel_failed",
					"jobs.poll_jobs_stats.fail_or_cancel_retry_error",
					"jobs.poll_jobs_stats.resume_completed",
					"jobs.poll_jobs_stats.resume_failed",
					"jobs.poll_jobs_stats.resume_retry_error",
				},
			},
			{
				Title: "Auto Config Top-level Runner Job",
				Metrics: []string{
					"jobs.auto_config_runner.fail_or_cancel_completed",
					"jobs.auto_config_runner.fail_or_cancel_failed",
					"jobs.auto_config_runner.fail_or_cancel_retry_error",
					"jobs.auto_config_runner.resume_completed",
					"jobs.auto_config_runner.resume_failed",
					"jobs.auto_config_runner.resume_retry_error",
				},
			},
			{
				Title: "Auto Config Per-environment Runner Jobs",
				Metrics: []string{
					"jobs.auto_config_env_runner.fail_or_cancel_completed",
					"jobs.auto_config_env_runner.fail_or_cancel_failed",
					"jobs.auto_config_env_runner.fail_or_cancel_retry_error",
					"jobs.auto_config_env_runner.resume_completed",
					"jobs.auto_config_env_runner.resume_failed",
					"jobs.auto_config_env_runner.resume_retry_error",
				},
			},
			{
				Title: "Auto Config Tasks",
				Metrics: []string{
					"jobs.auto_config_task.fail_or_cancel_completed",
					"jobs.auto_config_task.fail_or_cancel_failed",
					"jobs.auto_config_task.fail_or_cancel_retry_error",
					"jobs.auto_config_task.resume_completed",
					"jobs.auto_config_task.resume_failed",
					"jobs.auto_config_task.resume_retry_error",
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
		Organization: [][]string{{Jobs, "External Storage"}},
		Charts: []chartDescription{
			{
				Title: "External Storage",
				Metrics: []string{
					"cloud.read_bytes",
					"cloud.write_bytes",
				},
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
					"admission.requested.elastic-cpu",
					"admission.admitted.elastic-cpu",
					"admission.errored.elastic-cpu",
					"admission.admitted.elastic-cpu.normal-pri",
					"admission.admitted.elastic-cpu.bulk-normal-pri",
					"admission.admitted.kv-stores.bulk-normal-pri",
					"admission.admitted.kv-stores.locking-pri",
					"admission.admitted.kv-stores.ttl-low-pri",
					"admission.admitted.kv-stores.normal-pri",
					"admission.admitted.kv-stores.high-pri",
					"admission.admitted.kv.locking-pri",
					"admission.admitted.kv.normal-pri",
					"admission.admitted.kv.high-pri",
					"admission.admitted.sql-kv-response.locking-pri",
					"admission.admitted.sql-kv-response.normal-pri",
					"admission.admitted.sql-leaf-start.locking-pri",
					"admission.admitted.sql-leaf-start.normal-pri",
					"admission.admitted.sql-root-start.locking-pri",
					"admission.admitted.sql-root-start.normal-pri",
					"admission.admitted.sql-sql-response.locking-pri",
					"admission.admitted.sql-sql-response.normal-pri",
					"admission.errored.elastic-cpu.normal-pri",
					"admission.errored.elastic-cpu.bulk-normal-pri",
					"admission.errored.kv-stores.bulk-normal-pri",
					"admission.errored.kv-stores.locking-pri",
					"admission.errored.kv-stores.ttl-low-pri",
					"admission.errored.kv-stores.normal-pri",
					"admission.errored.kv-stores.high-pri",
					"admission.errored.kv.locking-pri",
					"admission.errored.kv.normal-pri",
					"admission.errored.kv.high-pri",
					"admission.errored.sql-kv-response.locking-pri",
					"admission.errored.sql-kv-response.normal-pri",
					"admission.errored.sql-leaf-start.locking-pri",
					"admission.errored.sql-leaf-start.normal-pri",
					"admission.errored.sql-root-start.locking-pri",
					"admission.errored.sql-root-start.normal-pri",
					"admission.errored.sql-sql-response.locking-pri",
					"admission.errored.sql-sql-response.normal-pri",
					"admission.requested.elastic-cpu.normal-pri",
					"admission.requested.elastic-cpu.bulk-normal-pri",
					"admission.requested.kv-stores.bulk-normal-pri",
					"admission.requested.kv-stores.locking-pri",
					"admission.requested.kv-stores.ttl-low-pri",
					"admission.requested.kv-stores.normal-pri",
					"admission.requested.kv-stores.high-pri",
					"admission.requested.kv.locking-pri",
					"admission.requested.kv.normal-pri",
					"admission.requested.kv.high-pri",
					"admission.requested.sql-kv-response.locking-pri",
					"admission.requested.sql-kv-response.normal-pri",
					"admission.requested.sql-leaf-start.locking-pri",
					"admission.requested.sql-leaf-start.normal-pri",
					"admission.requested.sql-root-start.locking-pri",
					"admission.requested.sql-root-start.normal-pri",
					"admission.requested.sql-sql-response.locking-pri",
					"admission.requested.sql-sql-response.normal-pri",
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
					"admission.wait_queue_length.elastic-cpu",
					"admission.wait_queue_length.elastic-cpu.normal-pri",
					"admission.wait_queue_length.elastic-cpu.bulk-normal-pri",
					"admission.wait_queue_length.kv-stores.bulk-normal-pri",
					"admission.wait_queue_length.kv-stores.locking-pri",
					"admission.wait_queue_length.kv-stores.ttl-low-pri",
					"admission.wait_queue_length.kv-stores.normal-pri",
					"admission.wait_queue_length.kv-stores.high-pri",
					"admission.wait_queue_length.kv.locking-pri",
					"admission.wait_queue_length.kv.normal-pri",
					"admission.wait_queue_length.kv.high-pri",
					"admission.wait_queue_length.sql-kv-response.locking-pri",
					"admission.wait_queue_length.sql-kv-response.normal-pri",
					"admission.wait_queue_length.sql-leaf-start.locking-pri",
					"admission.wait_queue_length.sql-leaf-start.normal-pri",
					"admission.wait_queue_length.sql-root-start.locking-pri",
					"admission.wait_queue_length.sql-root-start.normal-pri",
					"admission.wait_queue_length.sql-sql-response.locking-pri",
					"admission.wait_queue_length.sql-sql-response.normal-pri",
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
					"admission.wait_durations.elastic-cpu",
					"admission.wait_durations.elastic-cpu.normal-pri",
					"admission.wait_durations.elastic-cpu.bulk-normal-pri",
					"admission.wait_durations.kv-stores.bulk-normal-pri",
					"admission.wait_durations.kv-stores.locking-pri",
					"admission.wait_durations.kv-stores.ttl-low-pri",
					"admission.wait_durations.kv-stores.normal-pri",
					"admission.wait_durations.kv-stores.high-pri",
					"admission.wait_durations.kv.locking-pri",
					"admission.wait_durations.kv.normal-pri",
					"admission.wait_durations.kv.high-pri",
					"admission.wait_durations.sql-kv-response.locking-pri",
					"admission.wait_durations.sql-kv-response.normal-pri",
					"admission.wait_durations.sql-leaf-start.locking-pri",
					"admission.wait_durations.sql-leaf-start.normal-pri",
					"admission.wait_durations.sql-root-start.locking-pri",
					"admission.wait_durations.sql-root-start.normal-pri",
					"admission.wait_durations.sql-sql-response.locking-pri",
					"admission.wait_durations.sql-sql-response.normal-pri",
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
				Title: "Granter Slot Counters",
				Metrics: []string{
					"admission.granter.slot_adjuster_increments.kv",
					"admission.granter.slot_adjuster_decrements.kv",
				},
			},
			{
				Title: "Granter Slot Durations",
				Metrics: []string{
					"admission.granter.slots_exhausted_duration.kv",
					"admission.granter.cpu_load_short_period_duration.kv",
					"admission.granter.cpu_load_long_period_duration.kv",
				},
			},
			{
				Title: "Elastic CPU Utilization",
				Metrics: []string{
					"admission.elastic_cpu.utilization",
					"admission.elastic_cpu.utilization_limit",
				},
			},
			{
				Title: "Scheduler Latency Listener",
				Metrics: []string{
					"admission.scheduler_latency_listener.p99_nanos",
				},
			},
			{
				Title: "Scheduler Latency",
				Metrics: []string{
					"go.scheduler_latency",
				},
			},
			{
				Title: "Elastic CPU Durations",
				Metrics: []string{
					"admission.elastic_cpu.acquired_nanos",
					"admission.elastic_cpu.returned_nanos",
					"admission.elastic_cpu.max_available_nanos",
					"admission.elastic_cpu.pre_work_nanos",
				},
			},
			{
				Title: "Elastic CPU Available Tokens",
				Metrics: []string{
					"admission.elastic_cpu.available_nanos",
				},
			},
			{
				Title: "Elastic CPU Tokens Exhausted Duration Sum",
				Metrics: []string{
					"admission.elastic_cpu.nanos_exhausted_duration",
				},
			},
			{
				Title: "Elastic CPU Tokens Over Limit Duration",
				Metrics: []string{
					"admission.elastic_cpu.over_limit_durations",
				},
			},
			{
				Title: "IO Tokens Exhausted Duration Sum",
				Metrics: []string{
					"admission.granter.io_tokens_exhausted_duration.kv",
				},
			},
			{
				Title: "IO Overload - IOThreshold Score",
				Metrics: []string{
					"admission.io.overload",
				},
			},
		},
	},
	{
		Organization: [][]string{{Tenants, "Consumption"}},
		Charts: []chartDescription{
			{
				Title: "Total number of KV read/write batches",
				Metrics: []string{
					"tenant.consumption.write_batches",
					"tenant.consumption.read_batches",
				},
			},
			{
				Title: "Total number of KV read/write requests",
				Metrics: []string{
					"tenant.consumption.write_requests",
					"tenant.consumption.read_requests",
				},
			},
			{
				Title: "Total RU consumption",
				Metrics: []string{
					"tenant.consumption.request_units",
					"tenant.consumption.kv_request_units",
				},
			},
			{
				Title: "Total amount of CPU used by SQL pods",
				Metrics: []string{
					"tenant.consumption.sql_pods_cpu_seconds",
				},
			},
			{
				Title: "Total number of bytes",
				Metrics: []string{
					"tenant.consumption.write_bytes",
					"tenant.consumption.pgwire_egress_bytes",
					"tenant.consumption.read_bytes",
					"tenant.consumption.external_io_egress_bytes",
					"tenant.consumption.external_io_ingress_bytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Latency"}},
		Charts: []chartDescription{
			{
				Title:   "Execution duration for read batch evaluation.",
				Metrics: []string{"kv.replica_read_batch_evaluate.latency"},
			},
			{
				Title:   "Execution duration for read batch evaluation.",
				Metrics: []string{"kv.replica_write_batch_evaluate.latency"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Batches"}},
		Charts: []chartDescription{
			{
				Title: "Total number of attempts to evaluate read-only batches",
				Metrics: []string{
					"kv.replica_read_batch_evaluate.dropped_latches_before_eval",
					"kv.replica_read_batch_evaluate.without_interleaving_iter",
				},
			},
		},
	},
}

func jobTypeCharts(title string, varName string) chartDescription {
	var metrics []string
	for i := 0; i < jobspb.NumJobTypes; i++ {
		jt := jobspb.Type(i)
		if jt == jobspb.TypeUnspecified {
			continue
		}
		metrics = append(metrics,
			fmt.Sprintf("jobs.%s.%s", strings.ToLower(jobspb.Type_name[int32(i)]), varName))
	}
	return chartDescription{
		Title:   title,
		Metrics: metrics,
	}
}
