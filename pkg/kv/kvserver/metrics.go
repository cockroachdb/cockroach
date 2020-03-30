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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"go.etcd.io/etcd/raft/raftpb"
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
	metaLastUpdateNanos = metric.Metadata{
		Name:        "lastupdatenanos",
		Help:        "Timestamp at which bytes/keys/intents metrics were last updated",
		Measurement: "Last Update",
		Unit:        metric.Unit_TIMESTAMP_NS,
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

	// RocksDB metrics.
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
	metaRangeSnapshotsNormalApplied = metric.Metadata{
		Name:        "range.snapshots.normal-applied",
		Help:        "Number of applied snapshots",
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeSnapshotsLearnerApplied = metric.Metadata{
		Name:        "range.snapshots.learner-applied",
		Help:        "Number of applied learner snapshots",
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeRaftLeaderTransfers = metric.Metadata{
		Name:        "range.raftleadertransfers",
		Help:        "Number of raft leader transfers",
		Measurement: "Leader Transfers",
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
		Name:        "raft.process.workingnanos",
		Help:        "Nanoseconds spent in store.processRaft() working",
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
		Name:        "raft.commandsapplied",
		Help:        "Count of Raft commands applied",
		Measurement: "Commands",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftLogCommitLatency = metric.Metadata{
		Name:        "raft.process.logcommit.latency",
		Help:        "Latency histogram for committing Raft log entries",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRaftCommandCommitLatency = metric.Metadata{
		Name:        "raft.process.commandcommit.latency",
		Help:        "Latency histogram for committing Raft commands",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRaftHandleReadyLatency = metric.Metadata{
		Name:        "raft.process.handleready.latency",
		Help:        "Latency histogram for handling a Raft ready",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRaftApplyCommittedLatency = metric.Metadata{
		Name:        "raft.process.applycommitted.latency",
		Help:        "Latency histogram for applying all committed Raft commands in a Raft ready",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
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
		Name:        "raft.enqueued.pending",
		Help:        "Number of pending outgoing messages in the Raft Transport queue",
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
		Name:        "raftlog.behind",
		Help:        "Number of Raft log entries followers on other stores are behind",
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
	metaGCQueueSuccesses = metric.Metadata{
		Name:        "queue.gc.process.success",
		Help:        "Number of replicas successfully processed by the GC queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaGCQueueFailures = metric.Metadata{
		Name:        "queue.gc.process.failure",
		Help:        "Number of replicas which failed processing in the GC queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaGCQueuePending = metric.Metadata{
		Name:        "queue.gc.pending",
		Help:        "Number of pending replicas in the GC queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaGCQueueProcessingNanos = metric.Metadata{
		Name:        "queue.gc.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the GC queue",
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

	// Slow request metrics.
	metaLatchRequests = metric.Metadata{
		Name:        "requests.slow.latch",
		Help:        "Number of requests that have been stuck for a long time acquiring latches",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaSlowLeaseRequests = metric.Metadata{
		Name:        "requests.slow.lease",
		Help:        "Number of requests that have been stuck for a long time acquiring a lease",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaSlowRaftRequests = metric.Metadata{
		Name:        "requests.slow.raft",
		Help:        "Number of requests that have been stuck for a long time in raft",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	// Backpressure metrics.
	metaBackpressuredOnSplitRequests = metric.Metadata{
		Name:        "requests.backpressure.split",
		Help:        "Number of backpressured writes waiting on a Range split",
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

	// Encryption-at-rest metrics.
	// TODO(mberhault): metrics for key age, per-key file/bytes counts.
	metaEncryptionAlgorithm = metric.Metadata{
		Name:        "rocksdb.encryption.algorithm",
		Help:        "algorithm in use for encryption-at-rest, see ccl/storageccl/engineccl/enginepbccl/key_registry.proto",
		Measurement: "Encryption At Rest",
		Unit:        metric.Unit_CONST,
	}

	// Closed timestamp metrics.
	metaClosedTimestampMaxBehindNanos = metric.Metadata{
		Name:        "kv.closed_timestamp.max_behind_nanos",
		Help:        "Largest latency between realtime and replica max closed timestamp",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// StoreMetrics is the set of metrics for a given store.
type StoreMetrics struct {
	registry *metric.Registry

	// Replica metrics.
	ReplicaCount                  *metric.Gauge // Does not include uninitialized or reserved replicas.
	ReservedReplicaCount          *metric.Gauge
	RaftLeaderCount               *metric.Gauge
	RaftLeaderNotLeaseHolderCount *metric.Gauge
	LeaseHolderCount              *metric.Gauge
	QuiescentCount                *metric.Gauge

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
	LiveBytes          *metric.Gauge
	KeyBytes           *metric.Gauge
	ValBytes           *metric.Gauge
	TotalBytes         *metric.Gauge
	IntentBytes        *metric.Gauge
	LiveCount          *metric.Gauge
	KeyCount           *metric.Gauge
	ValCount           *metric.Gauge
	IntentCount        *metric.Gauge
	IntentAge          *metric.Gauge
	GcBytesAge         *metric.Gauge
	LastUpdateNanos    *metric.Gauge
	ResolveCommitCount *metric.Counter
	ResolveAbortCount  *metric.Counter
	ResolvePoisonCount *metric.Counter
	Capacity           *metric.Gauge
	Available          *metric.Gauge
	Used               *metric.Gauge
	Reserved           *metric.Gauge
	SysBytes           *metric.Gauge
	SysCount           *metric.Gauge

	// Rebalancing metrics.
	AverageQueriesPerSecond *metric.GaugeFloat64
	AverageWritesPerSecond  *metric.GaugeFloat64

	// Follower read metrics.
	FollowerReadsCount *metric.Counter

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

	// TODO(mrtracy): This should be removed as part of #4465. This is only
	// maintained to keep the current structure of NodeStatus; it would be
	// better to convert the Gauges above into counters which are adjusted
	// accordingly.

	// Range event metrics.
	RangeSplits                  *metric.Counter
	RangeMerges                  *metric.Counter
	RangeAdds                    *metric.Counter
	RangeRemoves                 *metric.Counter
	RangeSnapshotsGenerated      *metric.Counter
	RangeSnapshotsNormalApplied  *metric.Counter
	RangeSnapshotsLearnerApplied *metric.Counter
	RangeRaftLeaderTransfers     *metric.Counter

	// Raft processing metrics.
	RaftTicks                 *metric.Counter
	RaftWorkingDurationNanos  *metric.Counter
	RaftTickingDurationNanos  *metric.Counter
	RaftCommandsApplied       *metric.Counter
	RaftLogCommitLatency      *metric.Histogram
	RaftCommandCommitLatency  *metric.Histogram
	RaftHandleReadyLatency    *metric.Histogram
	RaftApplyCommittedLatency *metric.Histogram

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
	GCQueueSuccesses                          *metric.Counter
	GCQueueFailures                           *metric.Counter
	GCQueuePending                            *metric.Gauge
	GCQueueProcessingNanos                    *metric.Counter
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
	AddSSTableProposalTotalDelay  *metric.Counter
	AddSSTableProposalEngineDelay *metric.Counter

	// Encryption-at-rest stats.
	// EncryptionAlgorithm is an enum representing the cipher in use, so we use a gauge.
	EncryptionAlgorithm *metric.Gauge

	// RangeFeed counts.
	RangeFeedMetrics *rangefeed.Metrics

	// Closed timestamp metrics.
	ClosedTimestampMaxBehindNanos *metric.Gauge
}

func newStoreMetrics(histogramWindow time.Duration) *StoreMetrics {
	storeRegistry := metric.NewRegistry()
	sm := &StoreMetrics{
		registry: storeRegistry,

		// Replica metrics.
		ReplicaCount:                  metric.NewGauge(metaReplicaCount),
		ReservedReplicaCount:          metric.NewGauge(metaReservedReplicaCount),
		RaftLeaderCount:               metric.NewGauge(metaRaftLeaderCount),
		RaftLeaderNotLeaseHolderCount: metric.NewGauge(metaRaftLeaderNotLeaseHolderCount),
		LeaseHolderCount:              metric.NewGauge(metaLeaseHolderCount),
		QuiescentCount:                metric.NewGauge(metaQuiescentCount),

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

		// Storage metrics.
		LiveBytes:       metric.NewGauge(metaLiveBytes),
		KeyBytes:        metric.NewGauge(metaKeyBytes),
		ValBytes:        metric.NewGauge(metaValBytes),
		TotalBytes:      metric.NewGauge(metaTotalBytes),
		IntentBytes:     metric.NewGauge(metaIntentBytes),
		LiveCount:       metric.NewGauge(metaLiveCount),
		KeyCount:        metric.NewGauge(metaKeyCount),
		ValCount:        metric.NewGauge(metaValCount),
		IntentCount:     metric.NewGauge(metaIntentCount),
		IntentAge:       metric.NewGauge(metaIntentAge),
		GcBytesAge:      metric.NewGauge(metaGcBytesAge),
		LastUpdateNanos: metric.NewGauge(metaLastUpdateNanos),

		ResolveCommitCount: metric.NewCounter(metaResolveCommit),
		ResolveAbortCount:  metric.NewCounter(metaResolveAbort),
		ResolvePoisonCount: metric.NewCounter(metaResolvePoison),

		Capacity:  metric.NewGauge(metaCapacity),
		Available: metric.NewGauge(metaAvailable),
		Used:      metric.NewGauge(metaUsed),
		Reserved:  metric.NewGauge(metaReserved),
		SysBytes:  metric.NewGauge(metaSysBytes),
		SysCount:  metric.NewGauge(metaSysCount),

		// Rebalancing metrics.
		AverageQueriesPerSecond: metric.NewGaugeFloat64(metaAverageQueriesPerSecond),
		AverageWritesPerSecond:  metric.NewGaugeFloat64(metaAverageWritesPerSecond),

		// Follower reads metrics.
		FollowerReadsCount: metric.NewCounter(metaFollowerReadsCount),

		// RocksDB metrics.
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

		// Range event metrics.
		RangeSplits:                  metric.NewCounter(metaRangeSplits),
		RangeMerges:                  metric.NewCounter(metaRangeMerges),
		RangeAdds:                    metric.NewCounter(metaRangeAdds),
		RangeRemoves:                 metric.NewCounter(metaRangeRemoves),
		RangeSnapshotsGenerated:      metric.NewCounter(metaRangeSnapshotsGenerated),
		RangeSnapshotsNormalApplied:  metric.NewCounter(metaRangeSnapshotsNormalApplied),
		RangeSnapshotsLearnerApplied: metric.NewCounter(metaRangeSnapshotsLearnerApplied),
		RangeRaftLeaderTransfers:     metric.NewCounter(metaRangeRaftLeaderTransfers),

		// Raft processing metrics.
		RaftTicks:                 metric.NewCounter(metaRaftTicks),
		RaftWorkingDurationNanos:  metric.NewCounter(metaRaftWorkingDurationNanos),
		RaftTickingDurationNanos:  metric.NewCounter(metaRaftTickingDurationNanos),
		RaftCommandsApplied:       metric.NewCounter(metaRaftCommandsApplied),
		RaftLogCommitLatency:      metric.NewLatency(metaRaftLogCommitLatency, histogramWindow),
		RaftCommandCommitLatency:  metric.NewLatency(metaRaftCommandCommitLatency, histogramWindow),
		RaftHandleReadyLatency:    metric.NewLatency(metaRaftHandleReadyLatency, histogramWindow),
		RaftApplyCommittedLatency: metric.NewLatency(metaRaftApplyCommittedLatency, histogramWindow),

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
		GCQueueSuccesses:                          metric.NewCounter(metaGCQueueSuccesses),
		GCQueueFailures:                           metric.NewCounter(metaGCQueueFailures),
		GCQueuePending:                            metric.NewGauge(metaGCQueuePending),
		GCQueueProcessingNanos:                    metric.NewCounter(metaGCQueueProcessingNanos),
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

		// Wedge request counters.
		SlowLatchRequests: metric.NewGauge(metaLatchRequests),
		SlowLeaseRequests: metric.NewGauge(metaSlowLeaseRequests),
		SlowRaftRequests:  metric.NewGauge(metaSlowRaftRequests),

		// Backpressure counters.
		BackpressuredOnSplitRequests: metric.NewGauge(metaBackpressuredOnSplitRequests),

		// AddSSTable proposal + applications counters.
		AddSSTableProposals:           metric.NewCounter(metaAddSSTableProposals),
		AddSSTableApplications:        metric.NewCounter(metaAddSSTableApplications),
		AddSSTableApplicationCopies:   metric.NewCounter(metaAddSSTableApplicationCopies),
		AddSSTableProposalTotalDelay:  metric.NewCounter(metaAddSSTableEvalTotalDelay),
		AddSSTableProposalEngineDelay: metric.NewCounter(metaAddSSTableEvalEngineDelay),

		// Encryption-at-rest.
		EncryptionAlgorithm: metric.NewGauge(metaEncryptionAlgorithm),

		// RangeFeed counters.
		RangeFeedMetrics: rangefeed.NewMetrics(),

		// Closed timestamp metrics.
		ClosedTimestampMaxBehindNanos: metric.NewGauge(metaClosedTimestampMaxBehindNanos),
	}

	storeRegistry.AddMetricStruct(sm)

	return sm
}

// incMVCCGauges increments each individual metric from an MVCCStats delta. The
// method uses a series of atomic operations without any external locking, so a
// single snapshot of these gauges in the registry might mix the values of two
// subsequent updates.
func (sm *StoreMetrics) incMVCCGauges(delta enginepb.MVCCStats) {
	sm.LiveBytes.Inc(delta.LiveBytes)
	sm.KeyBytes.Inc(delta.KeyBytes)
	sm.ValBytes.Inc(delta.ValBytes)
	sm.TotalBytes.Inc(delta.Total())
	sm.IntentBytes.Inc(delta.IntentBytes)
	sm.LiveCount.Inc(delta.LiveCount)
	sm.KeyCount.Inc(delta.KeyCount)
	sm.ValCount.Inc(delta.ValCount)
	sm.IntentCount.Inc(delta.IntentCount)
	sm.IntentAge.Inc(delta.IntentAge)
	sm.GcBytesAge.Inc(delta.GCBytesAge)
	sm.LastUpdateNanos.Inc(delta.LastUpdateNanos)
	sm.SysBytes.Inc(delta.SysBytes)
	sm.SysCount.Inc(delta.SysCount)
}

func (sm *StoreMetrics) addMVCCStats(delta enginepb.MVCCStats) {
	sm.incMVCCGauges(delta)
}

func (sm *StoreMetrics) subtractMVCCStats(delta enginepb.MVCCStats) {
	var neg enginepb.MVCCStats
	neg.Subtract(delta)
	sm.incMVCCGauges(neg)
}

func (sm *StoreMetrics) updateRocksDBStats(stats storage.Stats) {
	// We do not grab a lock here, because it's not possible to get a point-in-
	// time snapshot of RocksDB stats. Retrieving RocksDB stats doesn't grab any
	// locks, and there's no way to retrieve multiple stats in a single operation.
	sm.RdbBlockCacheHits.Update(stats.BlockCacheHits)
	sm.RdbBlockCacheMisses.Update(stats.BlockCacheMisses)
	sm.RdbBlockCacheUsage.Update(stats.BlockCacheUsage)
	sm.RdbBlockCachePinnedUsage.Update(stats.BlockCachePinnedUsage)
	sm.RdbBloomFilterPrefixUseful.Update(stats.BloomFilterPrefixUseful)
	sm.RdbBloomFilterPrefixChecked.Update(stats.BloomFilterPrefixChecked)
	sm.RdbMemtableTotalSize.Update(stats.MemtableTotalSize)
	sm.RdbFlushes.Update(stats.Flushes)
	sm.RdbFlushedBytes.Update(stats.FlushedBytes)
	sm.RdbCompactions.Update(stats.Compactions)
	sm.RdbIngestedBytes.Update(stats.IngestedBytes)
	sm.RdbCompactedBytesRead.Update(stats.CompactedBytesRead)
	sm.RdbCompactedBytesWritten.Update(stats.CompactedBytesWritten)
	sm.RdbTableReadersMemEstimate.Update(stats.TableReadersMemEstimate)
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

	if metric != (result.Metrics{}) {
		log.Fatalf(ctx, "unhandled fields in metrics result: %+v", metric)
	}
}
