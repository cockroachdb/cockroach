// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"runtime/debug"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

var (
	// Replica metrics.
	metaReplicaCount = metric.Metadata{
		Name:        "replicas",
		Help:        "Number of replicas",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReservedReplicaCount = metric.Metadata{
		Name:        "replicas.reserved",
		Help:        "Number of replicas reserved for snapshots",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftLeaderCount = metric.Metadata{
		Name:        "replicas.leaders",
		Help:        "Number of raft leaders",
		Measurement: "Raft Leaders",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftLeaderNotLeaseHolderCount = metric.Metadata{
		Name:        "replicas.leaders_not_leaseholders",
		Help:        "Number of replicas that are Raft leaders whose range lease is held by another store",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaLeaseHolderCount = metric.Metadata{
		Name:        "replicas.leaseholders",
		Help:        "Number of lease holders",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaQuiescentCount = metric.Metadata{
		Name:        "replicas.quiescent",
		Help:        "Number of quiesced replicas",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaUninitializedCount = metric.Metadata{
		Name:        "replicas.uninitialized",
		Help:        "Number of uninitialized replicas, this does not include uninitialized replicas that can lie dormant in a persistent state.",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}

	// Range metrics.
	metaRangeCount = metric.Metadata{
		Name:        "ranges",
		Help:        "Number of ranges",
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}
	metaUnavailableRangeCount = metric.Metadata{
		Name:        "ranges.unavailable",
		Help:        "Number of ranges with fewer live replicas than needed for quorum",
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}
	metaUnderReplicatedRangeCount = metric.Metadata{
		Name:        "ranges.underreplicated",
		Help:        "Number of ranges with fewer live replicas than the replication target",
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}
	metaOverReplicatedRangeCount = metric.Metadata{
		Name:        "ranges.overreplicated",
		Help:        "Number of ranges with more live replicas than the replication target",
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}

	// Lease request metrics.
	metaLeaseRequestSuccessCount = metric.Metadata{
		Name:        "leases.success",
		Help:        "Number of successful lease requests",
		Measurement: "Lease Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaLeaseRequestErrorCount = metric.Metadata{
		Name:        "leases.error",
		Help:        "Number of failed lease requests",
		Measurement: "Lease Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaLeaseTransferSuccessCount = metric.Metadata{
		Name:        "leases.transfers.success",
		Help:        "Number of successful lease transfers",
		Measurement: "Lease Transfers",
		Unit:        metric.Unit_COUNT,
	}
	metaLeaseTransferErrorCount = metric.Metadata{
		Name:        "leases.transfers.error",
		Help:        "Number of failed lease transfers",
		Measurement: "Lease Transfers",
		Unit:        metric.Unit_COUNT,
	}
	metaLeaseExpirationCount = metric.Metadata{
		Name:        "leases.expiration",
		Help:        "Number of replica leaseholders using expiration-based leases",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaLeaseEpochCount = metric.Metadata{
		Name:        "leases.epoch",
		Help:        "Number of replica leaseholders using epoch-based leases",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}

	// Storage metrics.
	metaLiveBytes = metric.Metadata{
		Name:        "livebytes",
		Help:        "Number of bytes of live data (keys plus values)",
		Measurement: "Storage",
		Unit:        metric.Unit_BYTES,
	}
	metaKeyBytes = metric.Metadata{
		Name:        "keybytes",
		Help:        "Number of bytes taken up by keys",
		Measurement: "Storage",
		Unit:        metric.Unit_BYTES,
	}
	metaValBytes = metric.Metadata{
		Name:        "valbytes",
		Help:        "Number of bytes taken up by values",
		Measurement: "Storage",
		Unit:        metric.Unit_BYTES,
	}
	metaTotalBytes = metric.Metadata{
		Name:        "totalbytes",
		Help:        "Total number of bytes taken up by keys and values including non-live data",
		Measurement: "Storage",
		Unit:        metric.Unit_BYTES,
	}
	metaIntentBytes = metric.Metadata{
		Name:        "intentbytes",
		Help:        "Number of bytes in intent KV pairs",
		Measurement: "Storage",
		Unit:        metric.Unit_BYTES,
	}
	metaLiveCount = metric.Metadata{
		Name:        "livecount",
		Help:        "Count of live keys",
		Measurement: "Keys",
		Unit:        metric.Unit_COUNT,
	}
	metaKeyCount = metric.Metadata{
		Name:        "keycount",
		Help:        "Count of all keys",
		Measurement: "Keys",
		Unit:        metric.Unit_COUNT,
	}
	metaValCount = metric.Metadata{
		Name:        "valcount",
		Help:        "Count of all values",
		Measurement: "MVCC Values",
		Unit:        metric.Unit_COUNT,
	}
	metaIntentCount = metric.Metadata{
		Name:        "intentcount",
		Help:        "Count of intent keys",
		Measurement: "Keys",
		Unit:        metric.Unit_COUNT,
	}
	metaIntentAge = metric.Metadata{
		Name:        "intentage",
		Help:        "Cumulative age of intents",
		Measurement: "Age",
		Unit:        metric.Unit_SECONDS,
	}
	metaGcBytesAge = metric.Metadata{
		Name:        "gcbytesage",
		Help:        "Cumulative age of non-live data",
		Measurement: "Age",
		Unit:        metric.Unit_SECONDS,
	}

	// Contention and intent resolution metrics.
	metaResolveCommit = metric.Metadata{
		Name:        "intents.resolve-attempts",
		Help:        "Count of (point or range) intent commit evaluation attempts",
		Measurement: "Operations",
		Unit:        metric.Unit_COUNT,
	}
	metaResolveAbort = metric.Metadata{
		Name:        "intents.abort-attempts",
		Help:        "Count of (point or range) non-poisoning intent abort evaluation attempts",
		Measurement: "Operations",
		Unit:        metric.Unit_COUNT,
	}
	metaResolvePoison = metric.Metadata{
		Name:        "intents.poison-attempts",
		Help:        "Count of (point or range) poisoning intent abort evaluation attempts",
		Measurement: "Operations",
		Unit:        metric.Unit_COUNT,
	}

	// Disk usage diagram (CR=Cockroach):
	//                            ---------------------------------
	// Entire hard drive:         | non-CR data | CR data | empty |
	//                            ---------------------------------
	// Metrics:
	//                "capacity": |===============================|
	//                    "used":               |=========|
	//               "available":                         |=======|
	// "usable" (computed in UI):               |=================|
	metaCapacity = metric.Metadata{
		Name:        "capacity",
		Help:        "Total storage capacity",
		Measurement: "Storage",
		Unit:        metric.Unit_BYTES,
	}
	metaAvailable = metric.Metadata{
		Name:        "capacity.available",
		Help:        "Available storage capacity",
		Measurement: "Storage",
		Unit:        metric.Unit_BYTES,
	}
	metaUsed = metric.Metadata{
		Name:        "capacity.used",
		Help:        "Used storage capacity",
		Measurement: "Storage",
		Unit:        metric.Unit_BYTES,
	}

	metaReserved = metric.Metadata{
		Name:        "capacity.reserved",
		Help:        "Capacity reserved for snapshots",
		Measurement: "Storage",
		Unit:        metric.Unit_BYTES,
	}
	metaSysBytes = metric.Metadata{
		Name:        "sysbytes",
		Help:        "Number of bytes in system KV pairs",
		Measurement: "Storage",
		Unit:        metric.Unit_BYTES,
	}
	metaSysCount = metric.Metadata{
		Name:        "syscount",
		Help:        "Count of system KV pairs",
		Measurement: "Keys",
		Unit:        metric.Unit_COUNT,
	}
	metaAbortSpanBytes = metric.Metadata{
		Name:        "abortspanbytes",
		Help:        "Number of bytes in the abort span",
		Measurement: "Storage",
		Unit:        metric.Unit_BYTES,
	}

	// Metrics used by the rebalancing logic that aren't already captured elsewhere.
	metaAverageQueriesPerSecond = metric.Metadata{
		Name:        "rebalancing.queriespersecond",
		Help:        "Number of kv-level requests received per second by the store, averaged over a large time period as used in rebalancing decisions",
		Measurement: "Keys/Sec",
		Unit:        metric.Unit_COUNT,
	}
	metaAverageWritesPerSecond = metric.Metadata{
		Name:        "rebalancing.writespersecond",
		Help:        "Number of keys written (i.e. applied by raft) per second to the store, averaged over a large time period as used in rebalancing decisions",
		Measurement: "Keys/Sec",
		Unit:        metric.Unit_COUNT,
	}

	// Metric for tracking follower reads.
	metaFollowerReadsCount = metric.Metadata{
		Name:        "follower_reads.success_count",
		Help:        "Number of reads successfully processed by any replica",
		Measurement: "Read Ops",
		Unit:        metric.Unit_COUNT,
	}

	// Server-side transaction metrics.
	metaCommitWaitBeforeCommitTriggerCount = metric.Metadata{
		Name: "txn.commit_waits.before_commit_trigger",
		Help: "Number of KV transactions that had to commit-wait on the server " +
			"before committing because they had a commit trigger",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}

	// RocksDB/Pebble metrics.
	metaRdbBlockCacheHits = metric.Metadata{
		Name:        "rocksdb.block.cache.hits",
		Help:        "Count of block cache hits",
		Measurement: "Cache Ops",
		Unit:        metric.Unit_COUNT,
	}
	metaRdbBlockCacheMisses = metric.Metadata{
		Name:        "rocksdb.block.cache.misses",
		Help:        "Count of block cache misses",
		Measurement: "Cache Ops",
		Unit:        metric.Unit_COUNT,
	}
	metaRdbBlockCacheUsage = metric.Metadata{
		Name:        "rocksdb.block.cache.usage",
		Help:        "Bytes used by the block cache",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaRdbBlockCachePinnedUsage = metric.Metadata{
		Name:        "rocksdb.block.cache.pinned-usage",
		Help:        "Bytes pinned by the block cache",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaRdbBloomFilterPrefixChecked = metric.Metadata{
		Name:        "rocksdb.bloom.filter.prefix.checked",
		Help:        "Number of times the bloom filter was checked",
		Measurement: "Bloom Filter Ops",
		Unit:        metric.Unit_COUNT,
	}
	metaRdbBloomFilterPrefixUseful = metric.Metadata{
		Name:        "rocksdb.bloom.filter.prefix.useful",
		Help:        "Number of times the bloom filter helped avoid iterator creation",
		Measurement: "Bloom Filter Ops",
		Unit:        metric.Unit_COUNT,
	}
	metaRdbMemtableTotalSize = metric.Metadata{
		Name:        "rocksdb.memtable.total-size",
		Help:        "Current size of memtable in bytes",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaRdbFlushes = metric.Metadata{
		Name:        "rocksdb.flushes",
		Help:        "Number of table flushes",
		Measurement: "Flushes",
		Unit:        metric.Unit_COUNT,
	}
	metaRdbFlushedBytes = metric.Metadata{
		Name:        "rocksdb.flushed-bytes",
		Help:        "Bytes written during flush",
		Measurement: "Bytes Written",
		Unit:        metric.Unit_BYTES,
	}
	metaRdbCompactions = metric.Metadata{
		Name:        "rocksdb.compactions",
		Help:        "Number of table compactions",
		Measurement: "Compactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRdbIngestedBytes = metric.Metadata{
		Name:        "rocksdb.ingested-bytes",
		Help:        "Bytes ingested",
		Measurement: "Bytes Ingested",
		Unit:        metric.Unit_BYTES,
	}
	metaRdbCompactedBytesRead = metric.Metadata{
		Name:        "rocksdb.compacted-bytes-read",
		Help:        "Bytes read during compaction",
		Measurement: "Bytes Read",
		Unit:        metric.Unit_BYTES,
	}
	metaRdbCompactedBytesWritten = metric.Metadata{
		Name:        "rocksdb.compacted-bytes-written",
		Help:        "Bytes written during compaction",
		Measurement: "Bytes Written",
		Unit:        metric.Unit_BYTES,
	}
	metaRdbTableReadersMemEstimate = metric.Metadata{
		Name:        "rocksdb.table-readers-mem-estimate",
		Help:        "Memory used by index and filter blocks",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaRdbReadAmplification = metric.Metadata{
		Name:        "rocksdb.read-amplification",
		Help:        "Number of disk reads per query",
		Measurement: "Disk Reads per Query",
		Unit:        metric.Unit_COUNT,
	}
	metaRdbNumSSTables = metric.Metadata{
		Name:        "rocksdb.num-sstables",
		Help:        "Number of rocksdb SSTables",
		Measurement: "SSTables",
		Unit:        metric.Unit_COUNT,
	}
	metaRdbPendingCompaction = metric.Metadata{
		Name:        "rocksdb.estimated-pending-compaction",
		Help:        "Estimated pending compaction bytes",
		Measurement: "Storage",
		Unit:        metric.Unit_BYTES,
	}
	metaRdbL0Sublevels = metric.Metadata{
		Name:        "storage.l0-sublevels",
		Help:        "Number of Level 0 sublevels",
		Measurement: "Storage",
		Unit:        metric.Unit_COUNT,
	}
	metaRdbL0NumFiles = metric.Metadata{
		Name:        "storage.l0-num-files",
		Help:        "Number of Level 0 files",
		Measurement: "Storage",
		Unit:        metric.Unit_COUNT,
	}
	metaRdbWriteStalls = metric.Metadata{
		Name:        "storage.write-stalls",
		Help:        "Number of instances of intentional write stalls to backpressure incoming writes",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}

	// Disk health metrics.
	metaDiskSlow = metric.Metadata{
		Name:        "storage.disk-slow",
		Help:        "Number of instances of disk operations taking longer than 10s",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaDiskStalled = metric.Metadata{
		Name:        "storage.disk-stalled",
		Help:        "Number of instances of disk operations taking longer than 30s",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}

	// Range event metrics.
	metaRangeSplits = metric.Metadata{
		Name:        "range.splits",
		Help:        "Number of range splits",
		Measurement: "Range Ops",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeMerges = metric.Metadata{
		Name:        "range.merges",
		Help:        "Number of range merges",
		Measurement: "Range Ops",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeAdds = metric.Metadata{
		Name:        "range.adds",
		Help:        "Number of range additions",
		Measurement: "Range Ops",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeRemoves = metric.Metadata{
		Name:        "range.removes",
		Help:        "Number of range removals",
		Measurement: "Range Ops",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeSnapshotsGenerated = metric.Metadata{
		Name:        "range.snapshots.generated",
		Help:        "Number of generated snapshots",
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeSnapshotsAppliedByVoters = metric.Metadata{
		Name:        "range.snapshots.applied-voter",
		Help:        "Number of snapshots applied by voter replicas",
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeSnapshotsAppliedForInitialUpreplication = metric.Metadata{
		Name:        "range.snapshots.applied-initial",
		Help:        "Number of snapshots applied for initial upreplication",
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeSnapshotsAppliedByNonVoter = metric.Metadata{
		Name:        "range.snapshots.applied-non-voter",
		Help:        "Number of snapshots applied by non-voter replicas",
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeRaftLeaderTransfers = metric.Metadata{
		Name:        "range.raftleadertransfers",
		Help:        "Number of raft leader transfers",
		Measurement: "Leader Transfers",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeLossOfQuorumRecoveries = metric.Metadata{
		Name: "range.recoveries",
		Help: `Count of offline loss of quorum recovery operations performed on ranges.

This count increments for every range recovered in offline loss of quorum
recovery operation. Metric is updated when node on which survivor replica
is located starts following the recovery.`,
		Measurement: "Quorum Recoveries",
		Unit:        metric.Unit_COUNT,
	}

	// Raft processing metrics.
	metaRaftTicks = metric.Metadata{
		Name:        "raft.ticks",
		Help:        "Number of Raft ticks queued",
		Measurement: "Ticks",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftWorkingDurationNanos = metric.Metadata{
		Name: "raft.process.workingnanos",
		Help: `Nanoseconds spent in store.processRaft() working.

This is the sum of the measurements passed to the raft.process.handleready.latency
histogram.
`,
		Measurement: "Processing Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRaftTickingDurationNanos = metric.Metadata{
		Name:        "raft.process.tickingnanos",
		Help:        "Nanoseconds spent in store.processRaft() processing replica.Tick()",
		Measurement: "Processing Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRaftCommandsApplied = metric.Metadata{
		Name: "raft.commandsapplied",
		Help: `Count of Raft commands applied.

This measurement is taken on the Raft apply loops of all Replicas (leaders and
followers alike), meaning that it does not measure the number of Raft commands
*proposed* (in the hypothetical extreme case, all Replicas may apply all commands
through snapshots, thus not increasing this metric at all).
Instead, it is a proxy for how much work is being done advancing the Replica
state machines on this node.`,
		Measurement: "Commands",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftLogCommitLatency = metric.Metadata{
		Name: "raft.process.logcommit.latency",
		Help: `Latency histogram for committing Raft log entries to stable storage

This measures the latency of durably committing a group of newly received Raft
entries as well as the HardState entry to disk. This excludes any data
processing, i.e. we measure purely the commit latency of the resulting Engine
write. Homogeneous bands of p50-p99 latencies (in the presence of regular Raft
traffic), make it likely that the storage layer is healthy. Spikes in the
latency bands can either hint at the presence of large sets of Raft entries
being received, or at performance issues at the storage layer.
`,
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRaftCommandCommitLatency = metric.Metadata{
		Name: "raft.process.commandcommit.latency",
		Help: `Latency histogram for applying a batch of Raft commands to the state machine.

This metric is misnamed: it measures the latency for *applying* a batch of
committed Raft commands to a Replica state machine. This requires only
non-durable I/O (except for replication configuration changes).

Note that a "batch" in this context is really a sub-batch of the batch received
for application during raft ready handling. The
'raft.process.applycommitted.latency' histogram is likely more suitable in most
cases, as it measures the total latency across all sub-batches (i.e. the sum of
commandcommit.latency for a complete batch).
`,
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	// TODO(tbg): I think this metric skews low because we will often handle Readies
	// for which the result is that there is nothing to do. Do we want to change this
	// metric to only record ready handling when there is a Ready? That seems more
	// useful, experimentally it seems that we're recording 50% no-ops right now.
	// Though they aren't really no-ops, they still have to get a mutex and check
	// for a Ready, etc, but I still think it would be better to avoid those measure-
	// ments and to count the number of noops instead if we really want to.
	metaRaftHandleReadyLatency = metric.Metadata{
		Name: "raft.process.handleready.latency",
		Help: `Latency histogram for handling a Raft ready.

This measures the end-to-end-latency of the Raft state advancement loop, and
in particular includes:
- snapshot application
- SST ingestion
- durably appending to the Raft log (i.e. includes fsync)
- entry application (incl. replicated side effects, notably log truncation)
as well as updates to in-memory structures.

The above steps include the work measured in 'raft.process.commandcommit.latency',
as well as 'raft.process.applycommitted.latency'. Note that matching percentiles
of these metrics may nevertheless be *higher* than that of the handlready latency.
This is because not every handleready cycle leads to an update to the applycommitted
and commandcommit latencies. For example, under tpcc-100 on a single node, the
handleready count is approximately twice the logcommit count (and logcommit count
tracks closely with applycommitted count).

High percentile outliers can be caused by individual large Raft commands or
storage layer blips. An increase in lower (say the 50th) percentile is often
driven by either CPU exhaustion or a slowdown at the storage layer.
`,
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRaftApplyCommittedLatency = metric.Metadata{
		Name: "raft.process.applycommitted.latency",
		Help: `Latency histogram for applying all committed Raft commands in a Raft ready.

This measures the end-to-end latency of applying all commands in a Raft ready. Note that
this closes over possibly multiple measurements of the 'raft.process.commandcommit.latency'
metric, which receives datapoints for each sub-batch processed in the process.`,
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRaftSchedulerLatency = metric.Metadata{
		Name: "raft.scheduler.latency",
		Help: `Queueing durations for ranges waiting to be processed by the Raft scheduler.

This histogram measures the delay from when a range is registered with the scheduler
for processing to when it is actually processed. This does not include the duration
of processing.
`,
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRaftTimeoutCampaign = metric.Metadata{
		Name:        "raft.timeoutcampaign",
		Help:        "Number of Raft replicas campaigning after missed heartbeats from leader",
		Measurement: "Elections called after timeout",
		Unit:        metric.Unit_COUNT,
	}

	// Raft message metrics.
	metaRaftRcvdProp = metric.Metadata{
		Name:        "raft.rcvd.prop",
		Help:        "Number of MsgProp messages received by this store",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftRcvdApp = metric.Metadata{
		Name:        "raft.rcvd.app",
		Help:        "Number of MsgApp messages received by this store",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftRcvdAppResp = metric.Metadata{
		Name:        "raft.rcvd.appresp",
		Help:        "Number of MsgAppResp messages received by this store",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftRcvdVote = metric.Metadata{
		Name:        "raft.rcvd.vote",
		Help:        "Number of MsgVote messages received by this store",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftRcvdVoteResp = metric.Metadata{
		Name:        "raft.rcvd.voteresp",
		Help:        "Number of MsgVoteResp messages received by this store",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftRcvdPreVote = metric.Metadata{
		Name:        "raft.rcvd.prevote",
		Help:        "Number of MsgPreVote messages received by this store",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftRcvdPreVoteResp = metric.Metadata{
		Name:        "raft.rcvd.prevoteresp",
		Help:        "Number of MsgPreVoteResp messages received by this store",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftRcvdSnap = metric.Metadata{
		Name:        "raft.rcvd.snap",
		Help:        "Number of MsgSnap messages received by this store",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftRcvdHeartbeat = metric.Metadata{
		Name:        "raft.rcvd.heartbeat",
		Help:        "Number of (coalesced, if enabled) MsgHeartbeat messages received by this store",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftRcvdHeartbeatResp = metric.Metadata{
		Name:        "raft.rcvd.heartbeatresp",
		Help:        "Number of (coalesced, if enabled) MsgHeartbeatResp messages received by this store",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftRcvdTransferLeader = metric.Metadata{
		Name:        "raft.rcvd.transferleader",
		Help:        "Number of MsgTransferLeader messages received by this store",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftRcvdTimeoutNow = metric.Metadata{
		Name:        "raft.rcvd.timeoutnow",
		Help:        "Number of MsgTimeoutNow messages received by this store",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftRcvdDropped = metric.Metadata{
		Name:        "raft.rcvd.dropped",
		Help:        "Number of dropped incoming Raft messages",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftEnqueuedPending = metric.Metadata{
		Name: "raft.enqueued.pending",
		Help: `Number of pending outgoing messages in the Raft Transport queue.

The queue is bounded in size, so instead of unbounded growth one would observe a
ceiling value in the tens of thousands.`,
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftCoalescedHeartbeatsPending = metric.Metadata{
		Name:        "raft.heartbeats.pending",
		Help:        "Number of pending heartbeats and responses waiting to be coalesced",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}

	// Raft log metrics.
	metaRaftLogFollowerBehindCount = metric.Metadata{
		Name: "raftlog.behind",
		Help: `Number of Raft log entries followers on other stores are behind.

This gauge provides a view of the aggregate number of log entries the Raft leaders
on this node think the followers are behind. Since a raft leader may not always
have a good estimate for this information for all of its followers, and since
followers are expected to be behind (when they are not required as part of a
quorum) *and* the aggregate thus scales like the count of such followers, it is
difficult to meaningfully interpret this metric.`,
		Measurement: "Log Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftLogTruncated = metric.Metadata{
		Name:        "raftlog.truncated",
		Help:        "Number of Raft log entries truncated",
		Measurement: "Log Entries",
		Unit:        metric.Unit_COUNT,
	}

	// Replica queue metrics.
	metaMVCCGCQueueSuccesses = metric.Metadata{
		Name:        "queue.gc.process.success",
		Help:        "Number of replicas successfully processed by the MVCC GC queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaMVCCGCQueueFailures = metric.Metadata{
		Name:        "queue.gc.process.failure",
		Help:        "Number of replicas which failed processing in the MVCC GC queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaMVCCGCQueuePending = metric.Metadata{
		Name:        "queue.gc.pending",
		Help:        "Number of pending replicas in the MVCC GC queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaMVCCGCQueueProcessingNanos = metric.Metadata{
		Name:        "queue.gc.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the MVCC GC queue",
		Measurement: "Processing Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaMergeQueueSuccesses = metric.Metadata{
		Name:        "queue.merge.process.success",
		Help:        "Number of replicas successfully processed by the merge queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaMergeQueueFailures = metric.Metadata{
		Name:        "queue.merge.process.failure",
		Help:        "Number of replicas which failed processing in the merge queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaMergeQueuePending = metric.Metadata{
		Name:        "queue.merge.pending",
		Help:        "Number of pending replicas in the merge queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaMergeQueueProcessingNanos = metric.Metadata{
		Name:        "queue.merge.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the merge queue",
		Measurement: "Processing Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaMergeQueuePurgatory = metric.Metadata{
		Name:        "queue.merge.purgatory",
		Help:        "Number of replicas in the merge queue's purgatory, waiting to become mergeable",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftLogQueueSuccesses = metric.Metadata{
		Name:        "queue.raftlog.process.success",
		Help:        "Number of replicas successfully processed by the Raft log queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftLogQueueFailures = metric.Metadata{
		Name:        "queue.raftlog.process.failure",
		Help:        "Number of replicas which failed processing in the Raft log queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftLogQueuePending = metric.Metadata{
		Name:        "queue.raftlog.pending",
		Help:        "Number of pending replicas in the Raft log queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftLogQueueProcessingNanos = metric.Metadata{
		Name:        "queue.raftlog.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the Raft log queue",
		Measurement: "Processing Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRaftSnapshotQueueSuccesses = metric.Metadata{
		Name:        "queue.raftsnapshot.process.success",
		Help:        "Number of replicas successfully processed by the Raft repair queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftSnapshotQueueFailures = metric.Metadata{
		Name:        "queue.raftsnapshot.process.failure",
		Help:        "Number of replicas which failed processing in the Raft repair queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftSnapshotQueuePending = metric.Metadata{
		Name:        "queue.raftsnapshot.pending",
		Help:        "Number of pending replicas in the Raft repair queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftSnapshotQueueProcessingNanos = metric.Metadata{
		Name:        "queue.raftsnapshot.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the Raft repair queue",
		Measurement: "Processing Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaConsistencyQueueSuccesses = metric.Metadata{
		Name:        "queue.consistency.process.success",
		Help:        "Number of replicas successfully processed by the consistency checker queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaConsistencyQueueFailures = metric.Metadata{
		Name:        "queue.consistency.process.failure",
		Help:        "Number of replicas which failed processing in the consistency checker queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaConsistencyQueuePending = metric.Metadata{
		Name:        "queue.consistency.pending",
		Help:        "Number of pending replicas in the consistency checker queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaConsistencyQueueProcessingNanos = metric.Metadata{
		Name:        "queue.consistency.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the consistency checker queue",
		Measurement: "Processing Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaReplicaGCQueueSuccesses = metric.Metadata{
		Name:        "queue.replicagc.process.success",
		Help:        "Number of replicas successfully processed by the replica GC queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicaGCQueueFailures = metric.Metadata{
		Name:        "queue.replicagc.process.failure",
		Help:        "Number of replicas which failed processing in the replica GC queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicaGCQueuePending = metric.Metadata{
		Name:        "queue.replicagc.pending",
		Help:        "Number of pending replicas in the replica GC queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicaGCQueueProcessingNanos = metric.Metadata{
		Name:        "queue.replicagc.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the replica GC queue",
		Measurement: "Processing Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaReplicateQueueSuccesses = metric.Metadata{
		Name:        "queue.replicate.process.success",
		Help:        "Number of replicas successfully processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueFailures = metric.Metadata{
		Name:        "queue.replicate.process.failure",
		Help:        "Number of replicas which failed processing in the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueuePending = metric.Metadata{
		Name:        "queue.replicate.pending",
		Help:        "Number of pending replicas in the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueProcessingNanos = metric.Metadata{
		Name:        "queue.replicate.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the replicate queue",
		Measurement: "Processing Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaReplicateQueuePurgatory = metric.Metadata{
		Name:        "queue.replicate.purgatory",
		Help:        "Number of replicas in the replicate queue's purgatory, awaiting allocation options",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaSplitQueueSuccesses = metric.Metadata{
		Name:        "queue.split.process.success",
		Help:        "Number of replicas successfully processed by the split queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaSplitQueueFailures = metric.Metadata{
		Name:        "queue.split.process.failure",
		Help:        "Number of replicas which failed processing in the split queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaSplitQueuePending = metric.Metadata{
		Name:        "queue.split.pending",
		Help:        "Number of pending replicas in the split queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaSplitQueueProcessingNanos = metric.Metadata{
		Name:        "queue.split.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the split queue",
		Measurement: "Processing Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaSplitQueuePurgatory = metric.Metadata{
		Name:        "queue.split.purgatory",
		Help:        "Number of replicas in the split queue's purgatory, waiting to become splittable",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaTimeSeriesMaintenanceQueueSuccesses = metric.Metadata{
		Name:        "queue.tsmaintenance.process.success",
		Help:        "Number of replicas successfully processed by the time series maintenance queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaTimeSeriesMaintenanceQueueFailures = metric.Metadata{
		Name:        "queue.tsmaintenance.process.failure",
		Help:        "Number of replicas which failed processing in the time series maintenance queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaTimeSeriesMaintenanceQueuePending = metric.Metadata{
		Name:        "queue.tsmaintenance.pending",
		Help:        "Number of pending replicas in the time series maintenance queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaTimeSeriesMaintenanceQueueProcessingNanos = metric.Metadata{
		Name:        "queue.tsmaintenance.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the time series maintenance queue",
		Measurement: "Processing Time",
		Unit:        metric.Unit_NANOSECONDS,
	}

	// GCInfo cumulative totals.
	metaGCNumKeysAffected = metric.Metadata{
		Name:        "queue.gc.info.numkeysaffected",
		Help:        "Number of keys with GC'able data",
		Measurement: "Keys",
		Unit:        metric.Unit_COUNT,
	}
	metaGCIntentsConsidered = metric.Metadata{
		Name:        "queue.gc.info.intentsconsidered",
		Help:        "Number of 'old' intents",
		Measurement: "Intents",
		Unit:        metric.Unit_COUNT,
	}
	metaGCIntentTxns = metric.Metadata{
		Name:        "queue.gc.info.intenttxns",
		Help:        "Number of associated distinct transactions",
		Measurement: "Txns",
		Unit:        metric.Unit_COUNT,
	}
	metaGCTransactionSpanScanned = metric.Metadata{
		Name:        "queue.gc.info.transactionspanscanned",
		Help:        "Number of entries in transaction spans scanned from the engine",
		Measurement: "Txn Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaGCTransactionSpanGCAborted = metric.Metadata{
		Name:        "queue.gc.info.transactionspangcaborted",
		Help:        "Number of GC'able entries corresponding to aborted txns",
		Measurement: "Txn Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaGCTransactionSpanGCCommitted = metric.Metadata{
		Name:        "queue.gc.info.transactionspangccommitted",
		Help:        "Number of GC'able entries corresponding to committed txns",
		Measurement: "Txn Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaGCTransactionSpanGCStaging = metric.Metadata{
		Name:        "queue.gc.info.transactionspangcstaging",
		Help:        "Number of GC'able entries corresponding to staging txns",
		Measurement: "Txn Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaGCTransactionSpanGCPending = metric.Metadata{
		Name:        "queue.gc.info.transactionspangcpending",
		Help:        "Number of GC'able entries corresponding to pending txns",
		Measurement: "Txn Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaGCAbortSpanScanned = metric.Metadata{
		Name:        "queue.gc.info.abortspanscanned",
		Help:        "Number of transactions present in the AbortSpan scanned from the engine",
		Measurement: "Txn Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaGCAbortSpanConsidered = metric.Metadata{
		Name:        "queue.gc.info.abortspanconsidered",
		Help:        "Number of AbortSpan entries old enough to be considered for removal",
		Measurement: "Txn Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaGCAbortSpanGCNum = metric.Metadata{
		Name:        "queue.gc.info.abortspangcnum",
		Help:        "Number of AbortSpan entries fit for removal",
		Measurement: "Txn Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaGCPushTxn = metric.Metadata{
		Name:        "queue.gc.info.pushtxn",
		Help:        "Number of attempted pushes",
		Measurement: "Pushes",
		Unit:        metric.Unit_COUNT,
	}
	metaGCResolveTotal = metric.Metadata{
		Name:        "queue.gc.info.resolvetotal",
		Help:        "Number of attempted intent resolutions",
		Measurement: "Intent Resolutions",
		Unit:        metric.Unit_COUNT,
	}
	metaGCResolveSuccess = metric.Metadata{
		Name:        "queue.gc.info.resolvesuccess",
		Help:        "Number of successful intent resolutions",
		Measurement: "Intent Resolutions",
		Unit:        metric.Unit_COUNT,
	}
	metaGCResolveFailed = metric.Metadata{
		Name:        "queue.gc.info.resolvefailed",
		Help:        "Number of cleanup intent failures during GC",
		Measurement: "Intent Resolutions",
		Unit:        metric.Unit_COUNT,
	}
	metaGCTxnIntentsResolveFailed = metric.Metadata{
		Name:        "queue.gc.info.transactionresolvefailed",
		Help:        "Number of intent cleanup failures for local transactions during GC",
		Measurement: "Intent Resolutions",
		Unit:        metric.Unit_COUNT,
	}

	// Slow request metrics.
	metaLatchRequests = metric.Metadata{
		Name: "requests.slow.latch",
		Help: `Number of requests that have been stuck for a long time acquiring latches.

Latches moderate access to the KV keyspace for the purpose of evaluating and
replicating commands. A slow latch acquisition attempt is often caused by
another request holding and not releasing its latches in a timely manner. This
in turn can either be caused by a long delay in evaluation (for example, under
severe system overload) or by delays at the replication layer.

This gauge registering a nonzero value usually indicates a serious problem and
should be investigated.
`,
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaSlowLeaseRequests = metric.Metadata{
		Name: "requests.slow.lease",
		Help: `Number of requests that have been stuck for a long time acquiring a lease.

This gauge registering a nonzero value usually indicates range or replica
unavailability, and should be investigated. In the common case, we also
expect to see 'requests.slow.raft' to register a nonzero value, indicating
that the lease requests are not getting a timely response from the replication
layer.
`,
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaSlowRaftRequests = metric.Metadata{
		Name: "requests.slow.raft",
		Help: `Number of requests that have been stuck for a long time in the replication layer.

An (evaluated) request has to pass through the replication layer, notably the
quota pool and raft. If it fails to do so within a highly permissive duration,
the gauge is incremented (and decremented again once the request is either
applied or returns an error).

A nonzero value indicates range or replica unavailability, and should be investigated.
`,
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	// Backpressure metrics.
	metaBackpressuredOnSplitRequests = metric.Metadata{
		Name: "requests.backpressure.split",
		Help: `Number of backpressured writes waiting on a Range split.

A Range will backpressure (roughly) non-system traffic when the range is above
the configured size until the range splits. When the rate of this metric is
nonzero over extended periods of time, it should be investigated why splits are
not occurring.
`,
		Measurement: "Writes",
		Unit:        metric.Unit_COUNT,
	}

	// AddSSTable metrics.
	metaAddSSTableProposals = metric.Metadata{
		Name:        "addsstable.proposals",
		Help:        "Number of SSTable ingestions proposed (i.e. sent to Raft by lease holders)",
		Measurement: "Ingestions",
		Unit:        metric.Unit_COUNT,
	}
	metaAddSSTableApplications = metric.Metadata{
		Name:        "addsstable.applications",
		Help:        "Number of SSTable ingestions applied (i.e. applied by Replicas)",
		Measurement: "Ingestions",
		Unit:        metric.Unit_COUNT,
	}
	metaAddSSTableApplicationCopies = metric.Metadata{
		Name:        "addsstable.copies",
		Help:        "number of SSTable ingestions that required copying files during application",
		Measurement: "Ingestions",
		Unit:        metric.Unit_COUNT,
	}
	metaAddSSTableAsWrites = metric.Metadata{
		Name: "addsstable.aswrites",
		Help: `Number of SSTables ingested as normal writes.

These AddSSTable requests do not count towards the addsstable metrics
'proposals', 'applications', or 'copies', as they are not ingested as AddSSTable
Raft commands, but rather normal write commands. However, if these requests get
throttled they do count towards 'delay.total' and 'delay.enginebackpressure'.
`,
		Measurement: "Ingestions",
		Unit:        metric.Unit_COUNT,
	}
	metaAddSSTableEvalTotalDelay = metric.Metadata{
		Name:        "addsstable.delay.total",
		Help:        "Amount by which evaluation of AddSSTable requests was delayed",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaAddSSTableEvalEngineDelay = metric.Metadata{
		Name:        "addsstable.delay.enginebackpressure",
		Help:        "Amount by which evaluation of AddSSTable requests was delayed by storage-engine backpressure",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	// Export request counter.
	metaExportEvalTotalDelay = metric.Metadata{
		Name:        "exportrequest.delay.total",
		Help:        "Amount by which evaluation of Export requests was delayed",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	// Encryption-at-rest metrics.
	// TODO(mberhault): metrics for key age, per-key file/bytes counts.
	metaEncryptionAlgorithm = metric.Metadata{
		Name:        "rocksdb.encryption.algorithm",
		Help:        "Algorithm in use for encryption-at-rest, see ccl/storageccl/engineccl/enginepbccl/key_registry.proto",
		Measurement: "Encryption At Rest",
		Unit:        metric.Unit_CONST,
	}

	// Concurrency control metrics.
	metaConcurrencyLocks = metric.Metadata{
		Name:        "kv.concurrency.locks",
		Help:        "Number of active locks held in lock tables. Does not include replicated locks (intents) that are not held in memory",
		Measurement: "Locks",
		Unit:        metric.Unit_COUNT,
	}
	metaConcurrencyAverageLockHoldDurationNanos = metric.Metadata{
		Name: "kv.concurrency.avg_lock_hold_duration_nanos",
		Help: "Average lock hold duration across locks currently held in lock tables. " +
			"Does not include replicated locks (intents) that are not held in memory",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaConcurrencyMaxLockHoldDurationNanos = metric.Metadata{
		Name: "kv.concurrency.max_lock_hold_duration_nanos",
		Help: "Maximum length of time any lock in a lock table is held. " +
			"Does not include replicated locks (intents) that are not held in memory",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaConcurrencyLocksWithWaitQueues = metric.Metadata{
		Name:        "kv.concurrency.locks_with_wait_queues",
		Help:        "Number of active locks held in lock tables with active wait-queues",
		Measurement: "Locks",
		Unit:        metric.Unit_COUNT,
	}
	metaConcurrencyLockWaitQueueWaiters = metric.Metadata{
		Name:        "kv.concurrency.lock_wait_queue_waiters",
		Help:        "Number of requests actively waiting in a lock wait-queue",
		Measurement: "Lock-Queue Waiters",
		Unit:        metric.Unit_COUNT,
	}
	metaConcurrencyAverageLockWaitDurationNanos = metric.Metadata{
		Name:        "kv.concurrency.avg_lock_wait_duration_nanos",
		Help:        "Average lock wait duration across requests currently waiting in lock wait-queues",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaConcurrencyMaxLockWaitDurationNanos = metric.Metadata{
		Name:        "kv.concurrency.max_lock_wait_duration_nanos",
		Help:        "Maximum lock wait duration across requests currently waiting in lock wait-queues",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaConcurrencyMaxLockWaitQueueWaitersForLock = metric.Metadata{
		Name:        "kv.concurrency.max_lock_wait_queue_waiters_for_lock",
		Help:        "Maximum number of requests actively waiting in any single lock wait-queue",
		Measurement: "Lock-Queue Waiters",
		Unit:        metric.Unit_COUNT,
	}

	// Closed timestamp metrics.
	metaClosedTimestampMaxBehindNanos = metric.Metadata{
		Name:        "kv.closed_timestamp.max_behind_nanos",
		Help:        "Largest latency between realtime and replica max closed timestamp",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaClosedTimestampFailuresToClose = metric.Metadata{
		Name:        "kv.closed_timestamp.failures_to_close",
		Help:        "Number of times the min prop tracker failed to close timestamps due to epoch mismatch or pending evaluations",
		Measurement: "Attempts",
		Unit:        metric.Unit_COUNT,
	}

	// Replica circuit breaker.
	metaReplicaCircuitBreakerCurTripped = metric.Metadata{
		Name: "kv.replica_circuit_breaker.num_tripped_replicas",
		Help: `Number of Replicas for which the per-Replica circuit breaker is currently tripped.

A nonzero value indicates range or replica unavailability, and should be investigated.
Replicas in this state will fail-fast all inbound requests.
`,
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}

	// Replica circuit breaker.
	metaReplicaCircuitBreakerCumTripped = metric.Metadata{
		Name:        "kv.replica_circuit_breaker.num_tripped_events",
		Help:        `Number of times the per-Replica circuit breakers tripped since process start.`,
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
)

// StoreMetrics is the set of metrics for a given store.
type StoreMetrics struct {
	registry *metric.Registry

	// TenantStorageMetrics stores aggregate metrics for storage usage on a per
	// tenant basis.
	*TenantsStorageMetrics

	// Replica metrics.
	ReplicaCount                  *metric.Gauge // Does not include uninitialized or reserved replicas.
	ReservedReplicaCount          *metric.Gauge
	RaftLeaderCount               *metric.Gauge
	RaftLeaderNotLeaseHolderCount *metric.Gauge
	LeaseHolderCount              *metric.Gauge
	QuiescentCount                *metric.Gauge
	UninitializedCount            *metric.Gauge

	// Range metrics.
	RangeCount                *metric.Gauge
	UnavailableRangeCount     *metric.Gauge
	UnderReplicatedRangeCount *metric.Gauge
	OverReplicatedRangeCount  *metric.Gauge

	// Lease request metrics for successful and failed lease requests. These
	// count proposals (i.e. it does not matter how many replicas apply the
	// lease).
	LeaseRequestSuccessCount  *metric.Counter
	LeaseRequestErrorCount    *metric.Counter
	LeaseTransferSuccessCount *metric.Counter
	LeaseTransferErrorCount   *metric.Counter
	LeaseExpirationCount      *metric.Gauge
	LeaseEpochCount           *metric.Gauge

	// Storage metrics.
	ResolveCommitCount *metric.Counter
	ResolveAbortCount  *metric.Counter
	ResolvePoisonCount *metric.Counter
	Capacity           *metric.Gauge
	Available          *metric.Gauge
	Used               *metric.Gauge
	Reserved           *metric.Gauge

	// Rebalancing metrics.
	AverageQueriesPerSecond *metric.GaugeFloat64
	AverageWritesPerSecond  *metric.GaugeFloat64

	// Follower read metrics.
	FollowerReadsCount *metric.Counter

	// Server-side transaction metrics.
	CommitWaitsBeforeCommitTrigger *metric.Counter

	// RocksDB metrics.
	RdbBlockCacheHits           *metric.Gauge
	RdbBlockCacheMisses         *metric.Gauge
	RdbBlockCacheUsage          *metric.Gauge
	RdbBlockCachePinnedUsage    *metric.Gauge
	RdbBloomFilterPrefixChecked *metric.Gauge
	RdbBloomFilterPrefixUseful  *metric.Gauge
	RdbMemtableTotalSize        *metric.Gauge
	RdbFlushes                  *metric.Gauge
	RdbFlushedBytes             *metric.Gauge
	RdbCompactions              *metric.Gauge
	RdbIngestedBytes            *metric.Gauge
	RdbCompactedBytesRead       *metric.Gauge
	RdbCompactedBytesWritten    *metric.Gauge
	RdbTableReadersMemEstimate  *metric.Gauge
	RdbReadAmplification        *metric.Gauge
	RdbNumSSTables              *metric.Gauge
	RdbPendingCompaction        *metric.Gauge
	RdbL0Sublevels              *metric.Gauge
	RdbL0NumFiles               *metric.Gauge
	RdbWriteStalls              *metric.Gauge

	// Disk health metrics.
	DiskSlow    *metric.Gauge
	DiskStalled *metric.Gauge

	// TODO(mrtracy): This should be removed as part of #4465. This is only
	// maintained to keep the current structure of NodeStatus; it would be
	// better to convert the Gauges above into counters which are adjusted
	// accordingly.

	// Range event metrics.
	RangeSplits                                  *metric.Counter
	RangeMerges                                  *metric.Counter
	RangeAdds                                    *metric.Counter
	RangeRemoves                                 *metric.Counter
	RangeSnapshotsGenerated                      *metric.Counter
	RangeSnapshotsAppliedByVoters                *metric.Counter
	RangeSnapshotsAppliedForInitialUpreplication *metric.Counter
	RangeSnapshotsAppliedByNonVoters             *metric.Counter
	RangeRaftLeaderTransfers                     *metric.Counter
	RangeLossOfQuorumRecoveries                  *metric.Counter

	// Raft processing metrics.
	RaftTicks                 *metric.Counter
	RaftWorkingDurationNanos  *metric.Counter
	RaftTickingDurationNanos  *metric.Counter
	RaftCommandsApplied       *metric.Counter
	RaftLogCommitLatency      *metric.Histogram
	RaftCommandCommitLatency  *metric.Histogram
	RaftHandleReadyLatency    *metric.Histogram
	RaftApplyCommittedLatency *metric.Histogram
	RaftSchedulerLatency      *metric.Histogram
	RaftTimeoutCampaign       *metric.Counter

	// Raft message metrics.
	//
	// An array for conveniently finding the appropriate metric.
	RaftRcvdMessages   [maxRaftMsgType + 1]*metric.Counter
	RaftRcvdMsgDropped *metric.Counter

	// Raft log metrics.
	RaftLogFollowerBehindCount *metric.Gauge
	RaftLogTruncated           *metric.Counter

	RaftEnqueuedPending            *metric.Gauge
	RaftCoalescedHeartbeatsPending *metric.Gauge

	// Replica queue metrics.
	MVCCGCQueueSuccesses                      *metric.Counter
	MVCCGCQueueFailures                       *metric.Counter
	MVCCGCQueuePending                        *metric.Gauge
	MVCCGCQueueProcessingNanos                *metric.Counter
	MergeQueueSuccesses                       *metric.Counter
	MergeQueueFailures                        *metric.Counter
	MergeQueuePending                         *metric.Gauge
	MergeQueueProcessingNanos                 *metric.Counter
	MergeQueuePurgatory                       *metric.Gauge
	RaftLogQueueSuccesses                     *metric.Counter
	RaftLogQueueFailures                      *metric.Counter
	RaftLogQueuePending                       *metric.Gauge
	RaftLogQueueProcessingNanos               *metric.Counter
	RaftSnapshotQueueSuccesses                *metric.Counter
	RaftSnapshotQueueFailures                 *metric.Counter
	RaftSnapshotQueuePending                  *metric.Gauge
	RaftSnapshotQueueProcessingNanos          *metric.Counter
	ConsistencyQueueSuccesses                 *metric.Counter
	ConsistencyQueueFailures                  *metric.Counter
	ConsistencyQueuePending                   *metric.Gauge
	ConsistencyQueueProcessingNanos           *metric.Counter
	ReplicaGCQueueSuccesses                   *metric.Counter
	ReplicaGCQueueFailures                    *metric.Counter
	ReplicaGCQueuePending                     *metric.Gauge
	ReplicaGCQueueProcessingNanos             *metric.Counter
	ReplicateQueueSuccesses                   *metric.Counter
	ReplicateQueueFailures                    *metric.Counter
	ReplicateQueuePending                     *metric.Gauge
	ReplicateQueueProcessingNanos             *metric.Counter
	ReplicateQueuePurgatory                   *metric.Gauge
	SplitQueueSuccesses                       *metric.Counter
	SplitQueueFailures                        *metric.Counter
	SplitQueuePending                         *metric.Gauge
	SplitQueueProcessingNanos                 *metric.Counter
	SplitQueuePurgatory                       *metric.Gauge
	TimeSeriesMaintenanceQueueSuccesses       *metric.Counter
	TimeSeriesMaintenanceQueueFailures        *metric.Counter
	TimeSeriesMaintenanceQueuePending         *metric.Gauge
	TimeSeriesMaintenanceQueueProcessingNanos *metric.Counter

	// GCInfo cumulative totals.
	GCNumKeysAffected            *metric.Counter
	GCIntentsConsidered          *metric.Counter
	GCIntentTxns                 *metric.Counter
	GCTransactionSpanScanned     *metric.Counter
	GCTransactionSpanGCAborted   *metric.Counter
	GCTransactionSpanGCCommitted *metric.Counter
	GCTransactionSpanGCStaging   *metric.Counter
	GCTransactionSpanGCPending   *metric.Counter
	GCAbortSpanScanned           *metric.Counter
	GCAbortSpanConsidered        *metric.Counter
	GCAbortSpanGCNum             *metric.Counter
	GCPushTxn                    *metric.Counter
	GCResolveTotal               *metric.Counter
	GCResolveSuccess             *metric.Counter
	// Failures resolving intents that belong to transactions in other ranges.
	GCResolveFailed *metric.Counter
	// Failures resolving intents that belong to local transactions.
	GCTxnIntentsResolveFailed *metric.Counter

	// Slow request counts.
	SlowLatchRequests *metric.Gauge
	SlowLeaseRequests *metric.Gauge
	SlowRaftRequests  *metric.Gauge

	// Backpressure counts.
	BackpressuredOnSplitRequests *metric.Gauge

	// AddSSTable stats: how many AddSSTable commands were proposed and how many
	// were applied? How many applications required writing a copy?
	AddSSTableProposals           *metric.Counter
	AddSSTableApplications        *metric.Counter
	AddSSTableApplicationCopies   *metric.Counter
	AddSSTableAsWrites            *metric.Counter
	AddSSTableProposalTotalDelay  *metric.Counter
	AddSSTableProposalEngineDelay *metric.Counter

	// Export request stats.
	ExportRequestProposalTotalDelay *metric.Counter

	// Encryption-at-rest stats.
	// EncryptionAlgorithm is an enum representing the cipher in use, so we use a gauge.
	EncryptionAlgorithm *metric.Gauge

	// RangeFeed counts.
	RangeFeedMetrics *rangefeed.Metrics

	// Concurrency control metrics.
	Locks                          *metric.Gauge
	AverageLockHoldDurationNanos   *metric.Gauge
	MaxLockHoldDurationNanos       *metric.Gauge
	LocksWithWaitQueues            *metric.Gauge
	LockWaitQueueWaiters           *metric.Gauge
	AverageLockWaitDurationNanos   *metric.Gauge
	MaxLockWaitDurationNanos       *metric.Gauge
	MaxLockWaitQueueWaitersForLock *metric.Gauge

	// Closed timestamp metrics.
	ClosedTimestampMaxBehindNanos *metric.Gauge

	// Replica circuit breaker.
	ReplicaCircuitBreakerCurTripped *metric.Gauge
	ReplicaCircuitBreakerCumTripped *metric.Counter
}

type tenantMetricsRef struct {
	// All fields are internal. Don't access them.

	_tenantID roachpb.TenantID
	_state    int32 // atomic; 0=usable 1=poisoned

	// _stack helps diagnose use-after-release when it occurs.
	// This field is populated in releaseTenant and printed
	// in assertions on failure.
	_stack struct {
		syncutil.Mutex
		string
	}
}

func (ref *tenantMetricsRef) assert(ctx context.Context) {
	if atomic.LoadInt32(&ref._state) != 0 {
		ref._stack.Lock()
		defer ref._stack.Unlock()
		log.FatalfDepth(ctx, 1, "tenantMetricsRef already finalized in:\n%s", ref._stack.string)
	}
}

// TenantsStorageMetrics are metrics which are aggregated over all tenants
// present on the server. The struct maintains child metrics used by each
// tenant to track their individual values. The struct expects that children
// call acquire and release to properly reference count the metrics for
// individual tenants.
type TenantsStorageMetrics struct {
	LiveBytes      *aggmetric.AggGauge
	KeyBytes       *aggmetric.AggGauge
	ValBytes       *aggmetric.AggGauge
	TotalBytes     *aggmetric.AggGauge
	IntentBytes    *aggmetric.AggGauge
	LiveCount      *aggmetric.AggGauge
	KeyCount       *aggmetric.AggGauge
	ValCount       *aggmetric.AggGauge
	IntentCount    *aggmetric.AggGauge
	IntentAge      *aggmetric.AggGauge
	GcBytesAge     *aggmetric.AggGauge
	SysBytes       *aggmetric.AggGauge
	SysCount       *aggmetric.AggGauge
	AbortSpanBytes *aggmetric.AggGauge

	// This struct is invisible to the metric package.
	//
	// NB: note that the int64 conversion in this map is lossless, so
	// everything will work with tenantsIDs in excess of math.MaxInt64
	// except that should one ever look at this map through a debugger
	// the int64->uint64 conversion has to be done manually.
	tenants syncutil.IntMap // map[int64(roachpb.TenantID)]*tenantStorageMetrics
}

var _ metric.Struct = (*TenantsStorageMetrics)(nil)

// MetricStruct makes TenantsStorageMetrics a metric.Struct.
func (sm *TenantsStorageMetrics) MetricStruct() {}

// acquireTenant allocates the child metrics for a given tenant. Calls to this
// method are reference counted with decrements occurring in the corresponding
// releaseTenant call. This method must be called prior to adding or subtracting
// MVCC stats.
func (sm *TenantsStorageMetrics) acquireTenant(tenantID roachpb.TenantID) *tenantMetricsRef {
	// incRef increments the reference count if it is not already zero indicating
	// that the struct has already been destroyed.
	incRef := func(m *tenantStorageMetrics) (alreadyDestroyed bool) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.mu.refCount == 0 {
			return true
		}
		m.mu.refCount++
		return false
	}
	key := int64(tenantID.ToUint64())
	for {
		if mPtr, ok := sm.tenants.Load(key); ok {
			m := (*tenantStorageMetrics)(mPtr)
			if alreadyDestroyed := incRef(m); !alreadyDestroyed {
				return &tenantMetricsRef{
					_tenantID: tenantID,
				}
			}
			// Somebody else concurrently took the reference count to zero, go back
			// around. Because of the locking in releaseTenant, we know that we'll
			// find a different value or no value at all on the next iteration.
		} else {
			m := &tenantStorageMetrics{}
			m.mu.Lock()
			_, loaded := sm.tenants.LoadOrStore(key, unsafe.Pointer(m))
			if loaded {
				// Lost the race with another goroutine to add the instance, go back
				// around.
				continue
			}
			// Successfully stored a new instance, initialize it and then unlock it.
			tenantIDStr := tenantID.String()
			m.mu.refCount++
			m.LiveBytes = sm.LiveBytes.AddChild(tenantIDStr)
			m.KeyBytes = sm.KeyBytes.AddChild(tenantIDStr)
			m.ValBytes = sm.ValBytes.AddChild(tenantIDStr)
			m.TotalBytes = sm.TotalBytes.AddChild(tenantIDStr)
			m.IntentBytes = sm.IntentBytes.AddChild(tenantIDStr)
			m.LiveCount = sm.LiveCount.AddChild(tenantIDStr)
			m.KeyCount = sm.KeyCount.AddChild(tenantIDStr)
			m.ValCount = sm.ValCount.AddChild(tenantIDStr)
			m.IntentCount = sm.IntentCount.AddChild(tenantIDStr)
			m.IntentAge = sm.IntentAge.AddChild(tenantIDStr)
			m.GcBytesAge = sm.GcBytesAge.AddChild(tenantIDStr)
			m.SysBytes = sm.SysBytes.AddChild(tenantIDStr)
			m.SysCount = sm.SysCount.AddChild(tenantIDStr)
			m.AbortSpanBytes = sm.AbortSpanBytes.AddChild(tenantIDStr)
			m.mu.Unlock()
			return &tenantMetricsRef{
				_tenantID: tenantID,
			}
		}
	}
}

// releaseTenant releases the reference to the metrics for this tenant which was
// acquired with acquireTenant. It will fatally log if no entry exists for this
// tenant.
func (sm *TenantsStorageMetrics) releaseTenant(ctx context.Context, ref *tenantMetricsRef) {
	m := sm.getTenant(ctx, ref) // NB: asserts against use-after-release
	if atomic.SwapInt32(&ref._state, 1) != 0 {
		ref.assert(ctx) // this will fatal
		return          // unreachable
	}
	ref._stack.Lock()
	ref._stack.string = string(debug.Stack())
	ref._stack.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.refCount--
	if m.mu.refCount < 0 {
		log.Fatalf(ctx, "invalid refCount on metrics for tenant %v: %d", ref._tenantID, m.mu.refCount)
	} else if m.mu.refCount > 0 {
		return
	}

	// The refCount is zero, delete this instance after destroying its metrics.
	// Note that concurrent attempts to create an instance will detect the zero
	// refCount value and construct a new instance.
	m.LiveBytes.Destroy()
	m.KeyBytes.Destroy()
	m.ValBytes.Destroy()
	m.TotalBytes.Destroy()
	m.IntentBytes.Destroy()
	m.LiveCount.Destroy()
	m.KeyCount.Destroy()
	m.ValCount.Destroy()
	m.IntentCount.Destroy()
	m.IntentAge.Destroy()
	m.GcBytesAge.Destroy()
	m.SysBytes.Destroy()
	m.SysCount.Destroy()
	m.AbortSpanBytes.Destroy()
	sm.tenants.Delete(int64(ref._tenantID.ToUint64()))
}

// getTenant is a helper method used to retrieve the metrics for a tenant. The
// call will log fatally if no such tenant has been previously acquired.
func (sm *TenantsStorageMetrics) getTenant(
	ctx context.Context, ref *tenantMetricsRef,
) *tenantStorageMetrics {
	ref.assert(ctx)
	key := int64(ref._tenantID.ToUint64())
	mPtr, ok := sm.tenants.Load(key)
	if !ok {
		log.Fatalf(ctx, "no metrics exist for tenant %v", ref._tenantID)
	}
	return (*tenantStorageMetrics)(mPtr)
}

type tenantStorageMetrics struct {
	mu struct {
		syncutil.Mutex
		refCount int
	}

	LiveBytes      *aggmetric.Gauge
	KeyBytes       *aggmetric.Gauge
	ValBytes       *aggmetric.Gauge
	TotalBytes     *aggmetric.Gauge
	IntentBytes    *aggmetric.Gauge
	LiveCount      *aggmetric.Gauge
	KeyCount       *aggmetric.Gauge
	ValCount       *aggmetric.Gauge
	IntentCount    *aggmetric.Gauge
	IntentAge      *aggmetric.Gauge
	GcBytesAge     *aggmetric.Gauge
	SysBytes       *aggmetric.Gauge
	SysCount       *aggmetric.Gauge
	AbortSpanBytes *aggmetric.Gauge
}

func newTenantsStorageMetrics() *TenantsStorageMetrics {
	b := aggmetric.MakeBuilder(multitenant.TenantIDLabel)
	sm := &TenantsStorageMetrics{
		LiveBytes:      b.Gauge(metaLiveBytes),
		KeyBytes:       b.Gauge(metaKeyBytes),
		ValBytes:       b.Gauge(metaValBytes),
		TotalBytes:     b.Gauge(metaTotalBytes),
		IntentBytes:    b.Gauge(metaIntentBytes),
		LiveCount:      b.Gauge(metaLiveCount),
		KeyCount:       b.Gauge(metaKeyCount),
		ValCount:       b.Gauge(metaValCount),
		IntentCount:    b.Gauge(metaIntentCount),
		IntentAge:      b.Gauge(metaIntentAge),
		GcBytesAge:     b.Gauge(metaGcBytesAge),
		SysBytes:       b.Gauge(metaSysBytes),
		SysCount:       b.Gauge(metaSysCount),
		AbortSpanBytes: b.Gauge(metaAbortSpanBytes),
	}
	return sm
}

func newStoreMetrics(histogramWindow time.Duration) *StoreMetrics {
	storeRegistry := metric.NewRegistry()
	sm := &StoreMetrics{
		registry:              storeRegistry,
		TenantsStorageMetrics: newTenantsStorageMetrics(),

		// Replica metrics.
		ReplicaCount:                  metric.NewGauge(metaReplicaCount),
		ReservedReplicaCount:          metric.NewGauge(metaReservedReplicaCount),
		RaftLeaderCount:               metric.NewGauge(metaRaftLeaderCount),
		RaftLeaderNotLeaseHolderCount: metric.NewGauge(metaRaftLeaderNotLeaseHolderCount),
		LeaseHolderCount:              metric.NewGauge(metaLeaseHolderCount),
		QuiescentCount:                metric.NewGauge(metaQuiescentCount),
		UninitializedCount:            metric.NewGauge(metaUninitializedCount),

		// Range metrics.
		RangeCount:                metric.NewGauge(metaRangeCount),
		UnavailableRangeCount:     metric.NewGauge(metaUnavailableRangeCount),
		UnderReplicatedRangeCount: metric.NewGauge(metaUnderReplicatedRangeCount),
		OverReplicatedRangeCount:  metric.NewGauge(metaOverReplicatedRangeCount),

		// Lease request metrics.
		LeaseRequestSuccessCount:  metric.NewCounter(metaLeaseRequestSuccessCount),
		LeaseRequestErrorCount:    metric.NewCounter(metaLeaseRequestErrorCount),
		LeaseTransferSuccessCount: metric.NewCounter(metaLeaseTransferSuccessCount),
		LeaseTransferErrorCount:   metric.NewCounter(metaLeaseTransferErrorCount),
		LeaseExpirationCount:      metric.NewGauge(metaLeaseExpirationCount),
		LeaseEpochCount:           metric.NewGauge(metaLeaseEpochCount),

		// Intent resolution metrics.
		ResolveCommitCount: metric.NewCounter(metaResolveCommit),
		ResolveAbortCount:  metric.NewCounter(metaResolveAbort),
		ResolvePoisonCount: metric.NewCounter(metaResolvePoison),

		Capacity:  metric.NewGauge(metaCapacity),
		Available: metric.NewGauge(metaAvailable),
		Used:      metric.NewGauge(metaUsed),
		Reserved:  metric.NewGauge(metaReserved),

		// Rebalancing metrics.
		AverageQueriesPerSecond: metric.NewGaugeFloat64(metaAverageQueriesPerSecond),
		AverageWritesPerSecond:  metric.NewGaugeFloat64(metaAverageWritesPerSecond),

		// Follower reads metrics.
		FollowerReadsCount: metric.NewCounter(metaFollowerReadsCount),

		// Server-side transaction metrics.
		CommitWaitsBeforeCommitTrigger: metric.NewCounter(metaCommitWaitBeforeCommitTriggerCount),

		// RocksDB/Pebble metrics.
		RdbBlockCacheHits:           metric.NewGauge(metaRdbBlockCacheHits),
		RdbBlockCacheMisses:         metric.NewGauge(metaRdbBlockCacheMisses),
		RdbBlockCacheUsage:          metric.NewGauge(metaRdbBlockCacheUsage),
		RdbBlockCachePinnedUsage:    metric.NewGauge(metaRdbBlockCachePinnedUsage),
		RdbBloomFilterPrefixChecked: metric.NewGauge(metaRdbBloomFilterPrefixChecked),
		RdbBloomFilterPrefixUseful:  metric.NewGauge(metaRdbBloomFilterPrefixUseful),
		RdbMemtableTotalSize:        metric.NewGauge(metaRdbMemtableTotalSize),
		RdbFlushes:                  metric.NewGauge(metaRdbFlushes),
		RdbFlushedBytes:             metric.NewGauge(metaRdbFlushedBytes),
		RdbCompactions:              metric.NewGauge(metaRdbCompactions),
		RdbIngestedBytes:            metric.NewGauge(metaRdbIngestedBytes),
		RdbCompactedBytesRead:       metric.NewGauge(metaRdbCompactedBytesRead),
		RdbCompactedBytesWritten:    metric.NewGauge(metaRdbCompactedBytesWritten),
		RdbTableReadersMemEstimate:  metric.NewGauge(metaRdbTableReadersMemEstimate),
		RdbReadAmplification:        metric.NewGauge(metaRdbReadAmplification),
		RdbNumSSTables:              metric.NewGauge(metaRdbNumSSTables),
		RdbPendingCompaction:        metric.NewGauge(metaRdbPendingCompaction),
		RdbL0Sublevels:              metric.NewGauge(metaRdbL0Sublevels),
		RdbL0NumFiles:               metric.NewGauge(metaRdbL0NumFiles),
		RdbWriteStalls:              metric.NewGauge(metaRdbWriteStalls),

		// Disk health metrics.
		DiskSlow:    metric.NewGauge(metaDiskSlow),
		DiskStalled: metric.NewGauge(metaDiskStalled),

		// Range event metrics.
		RangeSplits:                   metric.NewCounter(metaRangeSplits),
		RangeMerges:                   metric.NewCounter(metaRangeMerges),
		RangeAdds:                     metric.NewCounter(metaRangeAdds),
		RangeRemoves:                  metric.NewCounter(metaRangeRemoves),
		RangeSnapshotsGenerated:       metric.NewCounter(metaRangeSnapshotsGenerated),
		RangeSnapshotsAppliedByVoters: metric.NewCounter(metaRangeSnapshotsAppliedByVoters),
		RangeSnapshotsAppliedForInitialUpreplication: metric.NewCounter(metaRangeSnapshotsAppliedForInitialUpreplication),
		RangeSnapshotsAppliedByNonVoters:             metric.NewCounter(metaRangeSnapshotsAppliedByNonVoter),
		RangeRaftLeaderTransfers:                     metric.NewCounter(metaRangeRaftLeaderTransfers),
		RangeLossOfQuorumRecoveries:                  metric.NewCounter(metaRangeLossOfQuorumRecoveries),

		// Raft processing metrics.
		RaftTicks:                 metric.NewCounter(metaRaftTicks),
		RaftWorkingDurationNanos:  metric.NewCounter(metaRaftWorkingDurationNanos),
		RaftTickingDurationNanos:  metric.NewCounter(metaRaftTickingDurationNanos),
		RaftCommandsApplied:       metric.NewCounter(metaRaftCommandsApplied),
		RaftLogCommitLatency:      metric.NewLatency(metaRaftLogCommitLatency, histogramWindow),
		RaftCommandCommitLatency:  metric.NewLatency(metaRaftCommandCommitLatency, histogramWindow),
		RaftHandleReadyLatency:    metric.NewLatency(metaRaftHandleReadyLatency, histogramWindow),
		RaftApplyCommittedLatency: metric.NewLatency(metaRaftApplyCommittedLatency, histogramWindow),
		RaftSchedulerLatency:      metric.NewLatency(metaRaftSchedulerLatency, histogramWindow),
		RaftTimeoutCampaign:       metric.NewCounter(metaRaftTimeoutCampaign),

		// Raft message metrics.
		RaftRcvdMessages: [...]*metric.Counter{
			raftpb.MsgProp:           metric.NewCounter(metaRaftRcvdProp),
			raftpb.MsgApp:            metric.NewCounter(metaRaftRcvdApp),
			raftpb.MsgAppResp:        metric.NewCounter(metaRaftRcvdAppResp),
			raftpb.MsgVote:           metric.NewCounter(metaRaftRcvdVote),
			raftpb.MsgVoteResp:       metric.NewCounter(metaRaftRcvdVoteResp),
			raftpb.MsgPreVote:        metric.NewCounter(metaRaftRcvdPreVote),
			raftpb.MsgPreVoteResp:    metric.NewCounter(metaRaftRcvdPreVoteResp),
			raftpb.MsgSnap:           metric.NewCounter(metaRaftRcvdSnap),
			raftpb.MsgHeartbeat:      metric.NewCounter(metaRaftRcvdHeartbeat),
			raftpb.MsgHeartbeatResp:  metric.NewCounter(metaRaftRcvdHeartbeatResp),
			raftpb.MsgTransferLeader: metric.NewCounter(metaRaftRcvdTransferLeader),
			raftpb.MsgTimeoutNow:     metric.NewCounter(metaRaftRcvdTimeoutNow),
		},
		RaftRcvdMsgDropped: metric.NewCounter(metaRaftRcvdDropped),

		// Raft log metrics.
		RaftLogFollowerBehindCount: metric.NewGauge(metaRaftLogFollowerBehindCount),
		RaftLogTruncated:           metric.NewCounter(metaRaftLogTruncated),

		RaftEnqueuedPending: metric.NewGauge(metaRaftEnqueuedPending),

		// This Gauge measures the number of heartbeats queued up just before
		// the queue is cleared, to avoid flapping wildly.
		RaftCoalescedHeartbeatsPending: metric.NewGauge(metaRaftCoalescedHeartbeatsPending),

		// Replica queue metrics.
		MVCCGCQueueSuccesses:                      metric.NewCounter(metaMVCCGCQueueSuccesses),
		MVCCGCQueueFailures:                       metric.NewCounter(metaMVCCGCQueueFailures),
		MVCCGCQueuePending:                        metric.NewGauge(metaMVCCGCQueuePending),
		MVCCGCQueueProcessingNanos:                metric.NewCounter(metaMVCCGCQueueProcessingNanos),
		MergeQueueSuccesses:                       metric.NewCounter(metaMergeQueueSuccesses),
		MergeQueueFailures:                        metric.NewCounter(metaMergeQueueFailures),
		MergeQueuePending:                         metric.NewGauge(metaMergeQueuePending),
		MergeQueueProcessingNanos:                 metric.NewCounter(metaMergeQueueProcessingNanos),
		MergeQueuePurgatory:                       metric.NewGauge(metaMergeQueuePurgatory),
		RaftLogQueueSuccesses:                     metric.NewCounter(metaRaftLogQueueSuccesses),
		RaftLogQueueFailures:                      metric.NewCounter(metaRaftLogQueueFailures),
		RaftLogQueuePending:                       metric.NewGauge(metaRaftLogQueuePending),
		RaftLogQueueProcessingNanos:               metric.NewCounter(metaRaftLogQueueProcessingNanos),
		RaftSnapshotQueueSuccesses:                metric.NewCounter(metaRaftSnapshotQueueSuccesses),
		RaftSnapshotQueueFailures:                 metric.NewCounter(metaRaftSnapshotQueueFailures),
		RaftSnapshotQueuePending:                  metric.NewGauge(metaRaftSnapshotQueuePending),
		RaftSnapshotQueueProcessingNanos:          metric.NewCounter(metaRaftSnapshotQueueProcessingNanos),
		ConsistencyQueueSuccesses:                 metric.NewCounter(metaConsistencyQueueSuccesses),
		ConsistencyQueueFailures:                  metric.NewCounter(metaConsistencyQueueFailures),
		ConsistencyQueuePending:                   metric.NewGauge(metaConsistencyQueuePending),
		ConsistencyQueueProcessingNanos:           metric.NewCounter(metaConsistencyQueueProcessingNanos),
		ReplicaGCQueueSuccesses:                   metric.NewCounter(metaReplicaGCQueueSuccesses),
		ReplicaGCQueueFailures:                    metric.NewCounter(metaReplicaGCQueueFailures),
		ReplicaGCQueuePending:                     metric.NewGauge(metaReplicaGCQueuePending),
		ReplicaGCQueueProcessingNanos:             metric.NewCounter(metaReplicaGCQueueProcessingNanos),
		ReplicateQueueSuccesses:                   metric.NewCounter(metaReplicateQueueSuccesses),
		ReplicateQueueFailures:                    metric.NewCounter(metaReplicateQueueFailures),
		ReplicateQueuePending:                     metric.NewGauge(metaReplicateQueuePending),
		ReplicateQueueProcessingNanos:             metric.NewCounter(metaReplicateQueueProcessingNanos),
		ReplicateQueuePurgatory:                   metric.NewGauge(metaReplicateQueuePurgatory),
		SplitQueueSuccesses:                       metric.NewCounter(metaSplitQueueSuccesses),
		SplitQueueFailures:                        metric.NewCounter(metaSplitQueueFailures),
		SplitQueuePending:                         metric.NewGauge(metaSplitQueuePending),
		SplitQueueProcessingNanos:                 metric.NewCounter(metaSplitQueueProcessingNanos),
		SplitQueuePurgatory:                       metric.NewGauge(metaSplitQueuePurgatory),
		TimeSeriesMaintenanceQueueSuccesses:       metric.NewCounter(metaTimeSeriesMaintenanceQueueSuccesses),
		TimeSeriesMaintenanceQueueFailures:        metric.NewCounter(metaTimeSeriesMaintenanceQueueFailures),
		TimeSeriesMaintenanceQueuePending:         metric.NewGauge(metaTimeSeriesMaintenanceQueuePending),
		TimeSeriesMaintenanceQueueProcessingNanos: metric.NewCounter(metaTimeSeriesMaintenanceQueueProcessingNanos),

		// GCInfo cumulative totals.
		GCNumKeysAffected:            metric.NewCounter(metaGCNumKeysAffected),
		GCIntentsConsidered:          metric.NewCounter(metaGCIntentsConsidered),
		GCIntentTxns:                 metric.NewCounter(metaGCIntentTxns),
		GCTransactionSpanScanned:     metric.NewCounter(metaGCTransactionSpanScanned),
		GCTransactionSpanGCAborted:   metric.NewCounter(metaGCTransactionSpanGCAborted),
		GCTransactionSpanGCCommitted: metric.NewCounter(metaGCTransactionSpanGCCommitted),
		GCTransactionSpanGCStaging:   metric.NewCounter(metaGCTransactionSpanGCStaging),
		GCTransactionSpanGCPending:   metric.NewCounter(metaGCTransactionSpanGCPending),
		GCAbortSpanScanned:           metric.NewCounter(metaGCAbortSpanScanned),
		GCAbortSpanConsidered:        metric.NewCounter(metaGCAbortSpanConsidered),
		GCAbortSpanGCNum:             metric.NewCounter(metaGCAbortSpanGCNum),
		GCPushTxn:                    metric.NewCounter(metaGCPushTxn),
		GCResolveTotal:               metric.NewCounter(metaGCResolveTotal),
		GCResolveSuccess:             metric.NewCounter(metaGCResolveSuccess),
		GCResolveFailed:              metric.NewCounter(metaGCResolveFailed),
		GCTxnIntentsResolveFailed:    metric.NewCounter(metaGCTxnIntentsResolveFailed),

		// Wedge request counters.
		SlowLatchRequests: metric.NewGauge(metaLatchRequests),
		SlowLeaseRequests: metric.NewGauge(metaSlowLeaseRequests),
		SlowRaftRequests:  metric.NewGauge(metaSlowRaftRequests),

		// Backpressure counters.
		BackpressuredOnSplitRequests: metric.NewGauge(metaBackpressuredOnSplitRequests),

		// AddSSTable proposal + applications counters.
		AddSSTableProposals:           metric.NewCounter(metaAddSSTableProposals),
		AddSSTableApplications:        metric.NewCounter(metaAddSSTableApplications),
		AddSSTableAsWrites:            metric.NewCounter(metaAddSSTableAsWrites),
		AddSSTableApplicationCopies:   metric.NewCounter(metaAddSSTableApplicationCopies),
		AddSSTableProposalTotalDelay:  metric.NewCounter(metaAddSSTableEvalTotalDelay),
		AddSSTableProposalEngineDelay: metric.NewCounter(metaAddSSTableEvalEngineDelay),

		// ExportRequest proposal.
		ExportRequestProposalTotalDelay: metric.NewCounter(metaExportEvalTotalDelay),

		// Encryption-at-rest.
		EncryptionAlgorithm: metric.NewGauge(metaEncryptionAlgorithm),

		// RangeFeed counters.
		RangeFeedMetrics: rangefeed.NewMetrics(),

		// Concurrency control metrics.
		Locks:                          metric.NewGauge(metaConcurrencyLocks),
		AverageLockHoldDurationNanos:   metric.NewGauge(metaConcurrencyAverageLockHoldDurationNanos),
		MaxLockHoldDurationNanos:       metric.NewGauge(metaConcurrencyMaxLockHoldDurationNanos),
		LocksWithWaitQueues:            metric.NewGauge(metaConcurrencyLocksWithWaitQueues),
		LockWaitQueueWaiters:           metric.NewGauge(metaConcurrencyLockWaitQueueWaiters),
		AverageLockWaitDurationNanos:   metric.NewGauge(metaConcurrencyAverageLockWaitDurationNanos),
		MaxLockWaitDurationNanos:       metric.NewGauge(metaConcurrencyMaxLockWaitDurationNanos),
		MaxLockWaitQueueWaitersForLock: metric.NewGauge(metaConcurrencyMaxLockWaitQueueWaitersForLock),

		// Closed timestamp metrics.
		ClosedTimestampMaxBehindNanos: metric.NewGauge(metaClosedTimestampMaxBehindNanos),

		// Replica circuit breaker.
		ReplicaCircuitBreakerCurTripped: metric.NewGauge(metaReplicaCircuitBreakerCurTripped),
		ReplicaCircuitBreakerCumTripped: metric.NewCounter(metaReplicaCircuitBreakerCumTripped),
	}
	storeRegistry.AddMetricStruct(sm)

	return sm
}

// incMVCCGauges increments each individual metric from an MVCCStats delta. The
// method uses a series of atomic operations without any external locking, so a
// single snapshot of these gauges in the registry might mix the values of two
// subsequent updates.
func (sm *TenantsStorageMetrics) incMVCCGauges(
	ctx context.Context, ref *tenantMetricsRef, delta enginepb.MVCCStats,
) {
	ref.assert(ctx)
	tm := sm.getTenant(ctx, ref)
	tm.LiveBytes.Inc(delta.LiveBytes)
	tm.KeyBytes.Inc(delta.KeyBytes)
	tm.ValBytes.Inc(delta.ValBytes)
	tm.TotalBytes.Inc(delta.Total())
	tm.IntentBytes.Inc(delta.IntentBytes)
	tm.LiveCount.Inc(delta.LiveCount)
	tm.KeyCount.Inc(delta.KeyCount)
	tm.ValCount.Inc(delta.ValCount)
	tm.IntentCount.Inc(delta.IntentCount)
	tm.IntentAge.Inc(delta.IntentAge)
	tm.GcBytesAge.Inc(delta.GCBytesAge)
	tm.SysBytes.Inc(delta.SysBytes)
	tm.SysCount.Inc(delta.SysCount)
	tm.AbortSpanBytes.Inc(delta.AbortSpanBytes)
}

func (sm *TenantsStorageMetrics) addMVCCStats(
	ctx context.Context, ref *tenantMetricsRef, delta enginepb.MVCCStats,
) {
	sm.incMVCCGauges(ctx, ref, delta)
}

func (sm *TenantsStorageMetrics) subtractMVCCStats(
	ctx context.Context, ref *tenantMetricsRef, delta enginepb.MVCCStats,
) {
	var neg enginepb.MVCCStats
	neg.Subtract(delta)
	sm.incMVCCGauges(ctx, ref, neg)
}

func (sm *StoreMetrics) updateEngineMetrics(m storage.Metrics) {
	sm.RdbBlockCacheHits.Update(m.BlockCache.Hits)
	sm.RdbBlockCacheMisses.Update(m.BlockCache.Misses)
	sm.RdbBlockCacheUsage.Update(m.BlockCache.Size)
	// TODO(jackson): Delete RdbBlockCachePinnedUsage or calculate the
	// equivalent (the sum of IteratorMetrics.ReadAmp for all open iterator,
	// times the block size).
	sm.RdbBlockCachePinnedUsage.Update(0)
	sm.RdbBloomFilterPrefixUseful.Update(m.Filter.Hits)
	sm.RdbBloomFilterPrefixChecked.Update(m.Filter.Hits + m.Filter.Misses)
	sm.RdbMemtableTotalSize.Update(int64(m.MemTable.Size))
	sm.RdbFlushes.Update(m.Flush.Count)
	sm.RdbFlushedBytes.Update(int64(m.Levels[0].BytesFlushed))
	sm.RdbCompactions.Update(m.Compact.Count)
	sm.RdbIngestedBytes.Update(int64(m.IngestedBytes()))
	compactedRead, compactedWritten := m.CompactedBytes()
	sm.RdbCompactedBytesRead.Update(int64(compactedRead))
	sm.RdbCompactedBytesWritten.Update(int64(compactedWritten))
	sm.RdbTableReadersMemEstimate.Update(m.TableCache.Size)
	sm.RdbReadAmplification.Update(int64(m.ReadAmp()))
	sm.RdbPendingCompaction.Update(int64(m.Compact.EstimatedDebt))
	sm.RdbL0Sublevels.Update(int64(m.Levels[0].Sublevels))
	sm.RdbL0NumFiles.Update(m.Levels[0].NumFiles)
	sm.RdbNumSSTables.Update(m.NumSSTables())
	sm.RdbWriteStalls.Update(m.WriteStallCount)
	sm.DiskSlow.Update(m.DiskSlowCount)
	sm.DiskStalled.Update(m.DiskStallCount)
}

func (sm *StoreMetrics) updateEnvStats(stats storage.EnvStats) {
	sm.EncryptionAlgorithm.Update(int64(stats.EncryptionType))
}

func (sm *StoreMetrics) handleMetricsResult(ctx context.Context, metric result.Metrics) {
	sm.LeaseRequestSuccessCount.Inc(int64(metric.LeaseRequestSuccess))
	metric.LeaseRequestSuccess = 0
	sm.LeaseRequestErrorCount.Inc(int64(metric.LeaseRequestError))
	metric.LeaseRequestError = 0
	sm.LeaseTransferSuccessCount.Inc(int64(metric.LeaseTransferSuccess))
	metric.LeaseTransferSuccess = 0
	sm.LeaseTransferErrorCount.Inc(int64(metric.LeaseTransferError))
	metric.LeaseTransferError = 0

	sm.ResolveCommitCount.Inc(int64(metric.ResolveCommit))
	metric.ResolveCommit = 0
	sm.ResolveAbortCount.Inc(int64(metric.ResolveAbort))
	metric.ResolveAbort = 0
	sm.ResolvePoisonCount.Inc(int64(metric.ResolvePoison))
	metric.ResolvePoison = 0

	sm.AddSSTableAsWrites.Inc(int64(metric.AddSSTableAsWrites))
	metric.AddSSTableAsWrites = 0

	if metric != (result.Metrics{}) {
		log.Fatalf(ctx, "unhandled fields in metrics result: %+v", metric)
	}
}
