// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package storage

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/coreos/etcd/raft/raftpb"
)

var (
	// Replica metrics.
	metaReplicaCount                  = metric.Metadata{Name: "replicas"}
	metaReservedReplicaCount          = metric.Metadata{Name: "replicas.reserved"}
	metaRaftLeaderCount               = metric.Metadata{Name: "replicas.leaders"}
	metaRaftLeaderNotLeaseHolderCount = metric.Metadata{
		Name: "replicas.leaders_not_leaseholders",
		Help: "Total number of Replicas that are Raft leaders whose Range lease is held by another store.",
	}
	metaLeaseHolderCount = metric.Metadata{Name: "replicas.leaseholders"}
	metaQuiescentCount   = metric.Metadata{Name: "replicas.quiescent"}

	// Replica CommandQueue metrics. Max size metrics track the maximum value
	// seen for all replicas during a single replica scan.
	metaMaxCommandQueueSize = metric.Metadata{Name: "replicas.commandqueue.maxsize",
		Help: "Largest number of commands in any CommandQueue"}
	metaMaxCommandQueueWriteCount = metric.Metadata{Name: "replicas.commandqueue.maxwritecount",
		Help: "Largest number of read-write commands in any CommandQueue"}
	metaMaxCommandQueueReadCount = metric.Metadata{Name: "replicas.commandqueue.maxreadcount",
		Help: "Largest number of read-only commands in any CommandQueue"}
	metaMaxCommandQueueTreeSize = metric.Metadata{Name: "replicas.commandqueue.maxtreesize",
		Help: "Largest number of intervals in any CommandQueue's interval tree"}
	metaMaxCommandQueueOverlaps = metric.Metadata{Name: "replicas.commandqueue.maxoverlaps",
		Help: "Largest number of overlapping commands seen when adding to any CommandQueue"}
	metaCombinedCommandQueueSize = metric.Metadata{Name: "replicas.commandqueue.combinedqueuesize",
		Help: "Number of commands in all CommandQueues combined"}
	metaCombinedCommandWriteCount = metric.Metadata{Name: "replicas.commandqueue.combinedwritecount",
		Help: "Number of read-write commands in all CommandQueues combined"}
	metaCombinedCommandReadCount = metric.Metadata{Name: "replicas.commandqueue.combinedreadcount",
		Help: "Number of read-only commands in all CommandQueues combined"}

	// Range metrics.
	metaRangeCount          = metric.Metadata{Name: "ranges"}
	metaAvailableRangeCount = metric.Metadata{Name: "ranges.available"}

	// Replication metrics.
	metaReplicaAllocatorNoopCount       = metric.Metadata{Name: "ranges.allocator.noop"}
	metaReplicaAllocatorRemoveCount     = metric.Metadata{Name: "ranges.allocator.remove"}
	metaReplicaAllocatorAddCount        = metric.Metadata{Name: "ranges.allocator.add"}
	metaReplicaAllocatorRemoveDeadCount = metric.Metadata{Name: "ranges.allocator.removedead"}

	// Lease request metrics.
	metaLeaseRequestSuccessCount = metric.Metadata{Name: "leases.success"}
	metaLeaseRequestErrorCount   = metric.Metadata{Name: "leases.error"}

	// Storage metrics.
	metaLiveBytes       = metric.Metadata{Name: "livebytes"}
	metaKeyBytes        = metric.Metadata{Name: "keybytes"}
	metaValBytes        = metric.Metadata{Name: "valbytes"}
	metaIntentBytes     = metric.Metadata{Name: "intentbytes"}
	metaLiveCount       = metric.Metadata{Name: "livecount"}
	metaKeyCount        = metric.Metadata{Name: "keycount"}
	metaValCount        = metric.Metadata{Name: "valcount"}
	metaIntentCount     = metric.Metadata{Name: "intentcount"}
	metaIntentAge       = metric.Metadata{Name: "intentage"}
	metaGcBytesAge      = metric.Metadata{Name: "gcbytesage"}
	metaLastUpdateNanos = metric.Metadata{Name: "lastupdatenanos"}
	metaCapacity        = metric.Metadata{Name: "capacity"}
	metaAvailable       = metric.Metadata{Name: "capacity.available"}
	metaReserved        = metric.Metadata{Name: "capacity.reserved"}
	metaSysBytes        = metric.Metadata{Name: "sysbytes"}
	metaSysCount        = metric.Metadata{Name: "syscount"}

	// RocksDB metrics.
	metaRdbBlockCacheHits           = metric.Metadata{Name: "rocksdb.block.cache.hits"}
	metaRdbBlockCacheMisses         = metric.Metadata{Name: "rocksdb.block.cache.misses"}
	metaRdbBlockCacheUsage          = metric.Metadata{Name: "rocksdb.block.cache.usage"}
	metaRdbBlockCachePinnedUsage    = metric.Metadata{Name: "rocksdb.block.cache.pinned-usage"}
	metaRdbBloomFilterPrefixChecked = metric.Metadata{Name: "rocksdb.bloom.filter.prefix.checked"}
	metaRdbBloomFilterPrefixUseful  = metric.Metadata{Name: "rocksdb.bloom.filter.prefix.useful"}
	metaRdbMemtableHits             = metric.Metadata{Name: "rocksdb.memtable.hits"}
	metaRdbMemtableMisses           = metric.Metadata{Name: "rocksdb.memtable.misses"}
	metaRdbMemtableTotalSize        = metric.Metadata{Name: "rocksdb.memtable.total-size"}
	metaRdbFlushes                  = metric.Metadata{Name: "rocksdb.flushes"}
	metaRdbCompactions              = metric.Metadata{Name: "rocksdb.compactions"}
	metaRdbTableReadersMemEstimate  = metric.Metadata{Name: "rocksdb.table-readers-mem-estimate"}
	metaRdbReadAmplification        = metric.Metadata{Name: "rocksdb.read-amplification"}
	metaRdbNumSSTables              = metric.Metadata{
		Name: "rocksdb.num-sstables",
		Help: "Number of rocksdb SSTables",
	}

	// Range event metrics.
	metaRangeSplits                     = metric.Metadata{Name: "range.splits"}
	metaRangeAdds                       = metric.Metadata{Name: "range.adds"}
	metaRangeRemoves                    = metric.Metadata{Name: "range.removes"}
	metaRangeSnapshotsGenerated         = metric.Metadata{Name: "range.snapshots.generated"}
	metaRangeSnapshotsNormalApplied     = metric.Metadata{Name: "range.snapshots.normal-applied"}
	metaRangeSnapshotsPreemptiveApplied = metric.Metadata{Name: "range.snapshots.preemptive-applied"}

	// Raft processing metrics.
	metaRaftTicks = metric.Metadata{
		Name: "raft.ticks",
		Help: "Number of Raft ticks queued",
	}
	metaRaftWorkingDurationNanos = metric.Metadata{Name: "raft.process.workingnanos",
		Help: "Nanoseconds spent in store.processRaft() working",
	}
	metaRaftTickingDurationNanos = metric.Metadata{Name: "raft.process.tickingnanos",
		Help: "Nanoseconds spent in store.processRaft() processing replica.Tick()",
	}

	// Raft message metrics.
	metaRaftRcvdProp = metric.Metadata{
		Name: "raft.rcvd.prop",
		Help: "Total number of MsgProp messages received by this store",
	}
	metaRaftRcvdApp = metric.Metadata{
		Name: "raft.rcvd.app",
		Help: "Total number of MsgApp messages received by this store",
	}
	metaRaftRcvdAppResp = metric.Metadata{
		Name: "raft.rcvd.appresp",
		Help: "Total number of MsgAppResp messages received by this store",
	}
	metaRaftRcvdVote = metric.Metadata{
		Name: "raft.rcvd.vote",
		Help: "Total number of MsgVote messages received by this store",
	}
	metaRaftRcvdVoteResp = metric.Metadata{
		Name: "raft.rcvd.voteresp",
		Help: "Total number of MsgVoteResp messages received by this store",
	}
	metaRaftRcvdPreVote = metric.Metadata{
		Name: "raft.rcvd.prevote",
		Help: "Total number of MsgPreVote messages received by this store",
	}
	metaRaftRcvdPreVoteResp = metric.Metadata{
		Name: "raft.rcvd.prevoteresp",
		Help: "Total number of MsgPreVoteResp messages received by this store",
	}
	metaRaftRcvdSnap = metric.Metadata{
		Name: "raft.rcvd.snap",
		Help: "Total number of MsgSnap messages received by this store",
	}
	metaRaftRcvdHeartbeat = metric.Metadata{
		Name: "raft.rcvd.heartbeat",
		Help: "Total number of (coalesced, if enabled) MsgHeartbeat messages received by this store",
	}
	metaRaftRcvdHeartbeatResp = metric.Metadata{
		Name: "raft.rcvd.heartbeatresp",
		Help: "Total number of (coalesced, if enabled) MsgHeartbeatResp messages received by this store",
	}
	metaRaftRcvdTransferLeader = metric.Metadata{
		Name: "raft.rcvd.transferleader",
		Help: "Total number of MsgTransferLeader messages received by this store",
	}
	metaRaftRcvdTimeoutNow = metric.Metadata{
		Name: "raft.rcvd.timeoutnow",
		Help: "Total number of MsgTimeoutNow messages received by this store",
	}
	metaRaftRcvdDropped = metric.Metadata{
		Name: "raft.rcvd.dropped",
		Help: "Number of dropped incoming Raft messages",
	}
	metaRaftEnqueuedPending = metric.Metadata{
		Name: "raft.enqueued.pending",
		Help: "Number of pending outgoing messages in the Raft Transport queue",
	}
	metaRaftCoalescedHeartbeatsPending = metric.Metadata{
		Name: "raft.heartbeats.pending",
		Help: "Number of pending heartbeats and responses waiting to be coalesced",
	}

	// Replica queue metrics.
	metaGCQueueSuccesses = metric.Metadata{Name: "queue.gc.process.success",
		Help: "Number of replicas successfully processed by the GC queue"}
	metaGCQueueFailures = metric.Metadata{Name: "queue.gc.process.failure",
		Help: "Number of replicas which failed processing in the GC queue"}
	metaGCQueuePending = metric.Metadata{Name: "queue.gc.pending",
		Help: "Number of pending replicas in the GC queue"}
	metaGCQueueProcessingNanos = metric.Metadata{Name: "queue.gc.processingnanos",
		Help: "Nanoseconds spent processing replicas in the GC queue"}
	metaRaftLogQueueSuccesses = metric.Metadata{Name: "queue.raftlog.process.success",
		Help: "Number of replicas successfully processed by the raft log queue"}
	metaRaftLogQueueFailures = metric.Metadata{Name: "queue.raftlog.process.failure",
		Help: "Number of replicas which failed processing in the raft log queue"}
	metaRaftLogQueuePending = metric.Metadata{Name: "queue.raftlog.pending",
		Help: "Number of pending replicas in the raft log queue"}
	metaRaftLogQueueProcessingNanos = metric.Metadata{Name: "queue.raftlog.processingnanos",
		Help: "Nanoseconds spent processing replicas in the raft log queue"}
	metaConsistencyQueueSuccesses = metric.Metadata{Name: "queue.consistency.process.success",
		Help: "Number of replicas successfully processed by the consistency checker queue"}
	metaConsistencyQueueFailures = metric.Metadata{Name: "queue.consistency.process.failure",
		Help: "Number of replicas which failed processing in the consistency checker queue"}
	metaConsistencyQueuePending = metric.Metadata{Name: "queue.consistency.pending",
		Help: "Number of pending replicas in the consistency checker queue"}
	metaConsistencyQueueProcessingNanos = metric.Metadata{Name: "queue.consistency.processingnanos",
		Help: "Nanoseconds spent processing replicas in the consistency checker queue"}
	metaReplicaGCQueueSuccesses = metric.Metadata{Name: "queue.replicagc.process.success",
		Help: "Number of replicas successfully processed by the replica GC queue"}
	metaReplicaGCQueueFailures = metric.Metadata{Name: "queue.replicagc.process.failure",
		Help: "Number of replicas which failed processing in the replica GC queue"}
	metaReplicaGCQueuePending = metric.Metadata{Name: "queue.replicagc.pending",
		Help: "Number of pending replicas in the replica GC queue"}
	metaReplicaGCQueueProcessingNanos = metric.Metadata{Name: "queue.replicagc.processingnanos",
		Help: "Nanoseconds spent processing replicas in the replica GC queue"}
	metaReplicateQueueSuccesses = metric.Metadata{Name: "queue.replicate.process.success",
		Help: "Number of replicas successfully processed by the replicate queue"}
	metaReplicateQueueFailures = metric.Metadata{Name: "queue.replicate.process.failure",
		Help: "Number of replicas which failed processing in the replicate queue"}
	metaReplicateQueuePending = metric.Metadata{Name: "queue.replicate.pending",
		Help: "Number of pending replicas in the replicate queue"}
	metaReplicateQueueProcessingNanos = metric.Metadata{Name: "queue.replicate.processingnanos",
		Help: "Nanoseconds spent processing replicas in the replicate queue"}
	metaReplicateQueuePurgatory = metric.Metadata{Name: "queue.replicate.purgatory",
		Help: "Number of replicas in the replicate queue's purgatory, awaiting allocation options"}
	metaSplitQueueSuccesses = metric.Metadata{Name: "queue.split.process.success",
		Help: "Number of replicas successfully processed by the split queue"}
	metaSplitQueueFailures = metric.Metadata{Name: "queue.split.process.failure",
		Help: "Number of replicas which failed processing in the split queue"}
	metaSplitQueuePending = metric.Metadata{Name: "queue.split.pending",
		Help: "Number of pending replicas in the split queue"}
	metaSplitQueueProcessingNanos = metric.Metadata{Name: "queue.split.processingnanos",
		Help: "Nanoseconds spent processing replicas in the split queue"}

	metaTimeSeriesMaintenanceQueueSuccesses = metric.Metadata{Name: "queue.tsmaintenance.process.success",
		Help: "Number of replicas successfully processed by the time series maintenance queue"}
	metaTimeSeriesMaintenanceQueueFailures = metric.Metadata{Name: "queue.tsmaintenance.process.failure",
		Help: "Number of replicas which failed processing in the time series maintenance queue"}
	metaTimeSeriesMaintenanceQueuePending = metric.Metadata{Name: "queue.tsmaintenance.pending",
		Help: "Number of pending replicas in the time series maintenance queue"}
	metaTimeSeriesMaintenanceQueueProcessingNanos = metric.Metadata{Name: "queue.tsmaintenance.processingnanos",
		Help: "Nanoseconds spent processing replicas in the time series maintenance queue"}

	// GCInfo cumulative totals.
	metaGCNumKeysAffected = metric.Metadata{Name: "queue.gc.info.numkeysaffected",
		Help: "Number of keys with GC'able data"}
	metaGCIntentsConsidered = metric.Metadata{Name: "queue.gc.info.intentsconsidered",
		Help: "Number of 'old' intents"}
	metaGCIntentTxns = metric.Metadata{Name: "queue.gc.info.intenttxns",
		Help: "Number of associated distinct transactions"}
	metaGCTransactionSpanScanned = metric.Metadata{Name: "queue.gc.info.transactionspanscanned",
		Help: "Total number of entries in the transaction span scanned from the engine"}
	metaGCTransactionSpanGCAborted = metric.Metadata{Name: "queue.gc.info.transactionspangcaborted",
		Help: "Number of GC'able entries corresponding to aborted txns"}
	metaGCTransactionSpanGCCommitted = metric.Metadata{Name: "queue.gc.info.transactionspangccommitted",
		Help: "Number of GC'able entries corresponding to committed txns"}
	metaGCTransactionSpanGCPending = metric.Metadata{Name: "queue.gc.info.transactionspangcpending",
		Help: "Number of GC'able entries corresponding to pending txns"}
	metaGCAbortSpanScanned = metric.Metadata{Name: "queue.gc.info.abortspanscanned",
		Help: "Total number of transactions present in the abort cache scanned from the engine"}
	metaGCAbortSpanConsidered = metric.Metadata{Name: "queue.gc.info.abortspanconsidered",
		Help: "Number of abort cache entries old enough to be considered for removal"}
	metaGCAbortSpanGCNum = metric.Metadata{Name: "queue.gc.info.abortspangcnum",
		Help: "Number of abort cache entries fit for removal"}
	metaGCPushTxn = metric.Metadata{Name: "queue.gc.info.pushtxn",
		Help: "Total number of attempted pushes"}
	metaGCResolveTotal = metric.Metadata{Name: "queue.gc.info.resolvetotal",
		Help: "Total number of attempted intent resolutions"}
	metaGCResolveSuccess = metric.Metadata{Name: "queue.gc.info.resolvesuccess",
		Help: "Number of successful intent resolutions"}

	metaMuReplicaNanos = metric.Metadata{Name: "mutex.replicananos",
		Help: "Duration of Replica mutex critical sections"}
	metaMuCommandQueueNanos = metric.Metadata{Name: "mutex.commandqueuenanos",
		Help: "Duration of Command Queue mutex critical sections"}
	metaMuRaftNanos = metric.Metadata{Name: "mutex.raftnanos",
		Help: "Duration of Replica Raft mutex critical sections"}
	metaMuStoreNanos = metric.Metadata{Name: "mutex.storenanos",
		Help: "Duration of Store mutex critical sections"}
	metaMuSchedulerNanos = metric.Metadata{Name: "mutex.schedulernanos",
		Help: "Duration of Raft Scheduler mutex critical sections"}
)

// StoreMetrics is the set of metrics for a given store.
type StoreMetrics struct {
	registry *metric.Registry

	// Replica metrics.
	ReplicaCount                  *metric.Counter // Does not include reserved replicas.
	ReservedReplicaCount          *metric.Counter
	RaftLeaderCount               *metric.Gauge
	RaftLeaderNotLeaseHolderCount *metric.Gauge
	LeaseHolderCount              *metric.Gauge
	QuiescentCount                *metric.Gauge

	// Replica CommandQueue metrics.
	MaxCommandQueueSize       *metric.Gauge
	MaxCommandQueueWriteCount *metric.Gauge
	MaxCommandQueueReadCount  *metric.Gauge
	MaxCommandQueueTreeSize   *metric.Gauge
	MaxCommandQueueOverlaps   *metric.Gauge
	CombinedCommandQueueSize  *metric.Gauge
	CombinedCommandWriteCount *metric.Gauge
	CombinedCommandReadCount  *metric.Gauge

	// Range metrics.
	RangeCount          *metric.Gauge
	AvailableRangeCount *metric.Gauge

	// Replication metrics.
	ReplicaAllocatorNoopCount       *metric.Gauge
	ReplicaAllocatorRemoveCount     *metric.Gauge
	ReplicaAllocatorAddCount        *metric.Gauge
	ReplicaAllocatorRemoveDeadCount *metric.Gauge

	// Lease request metrics for successful and failed lease requests. These
	// count proposals (i.e. it does not matter how many Replicas apply the
	// lease).
	LeaseRequestSuccessCount *metric.Counter
	LeaseRequestErrorCount   *metric.Counter

	// Storage metrics.
	LiveBytes       *metric.Gauge
	KeyBytes        *metric.Gauge
	ValBytes        *metric.Gauge
	IntentBytes     *metric.Gauge
	LiveCount       *metric.Gauge
	KeyCount        *metric.Gauge
	ValCount        *metric.Gauge
	IntentCount     *metric.Gauge
	IntentAge       *metric.Gauge
	GcBytesAge      *metric.Gauge
	LastUpdateNanos *metric.Gauge
	Capacity        *metric.Gauge
	Available       *metric.Gauge
	Reserved        *metric.Counter
	SysBytes        *metric.Gauge
	SysCount        *metric.Gauge

	// RocksDB metrics.
	RdbBlockCacheHits           *metric.Gauge
	RdbBlockCacheMisses         *metric.Gauge
	RdbBlockCacheUsage          *metric.Gauge
	RdbBlockCachePinnedUsage    *metric.Gauge
	RdbBloomFilterPrefixChecked *metric.Gauge
	RdbBloomFilterPrefixUseful  *metric.Gauge
	RdbMemtableHits             *metric.Gauge
	RdbMemtableMisses           *metric.Gauge
	RdbMemtableTotalSize        *metric.Gauge
	RdbFlushes                  *metric.Gauge
	RdbCompactions              *metric.Gauge
	RdbTableReadersMemEstimate  *metric.Gauge
	RdbReadAmplification        *metric.Gauge
	RdbNumSSTables              *metric.Gauge

	// TODO(mrtracy): This should be removed as part of #4465. This is only
	// maintained to keep the current structure of StatusSummaries; it would be
	// better to convert the Gauges above into counters which are adjusted
	// accordingly.

	// Range event metrics.
	RangeSplits                     *metric.Counter
	RangeAdds                       *metric.Counter
	RangeRemoves                    *metric.Counter
	RangeSnapshotsGenerated         *metric.Counter
	RangeSnapshotsNormalApplied     *metric.Counter
	RangeSnapshotsPreemptiveApplied *metric.Counter

	// Raft processing metrics.
	RaftTicks                *metric.Counter
	RaftWorkingDurationNanos *metric.Counter
	RaftTickingDurationNanos *metric.Counter

	// Raft message metrics.
	RaftRcvdMsgProp           *metric.Counter
	RaftRcvdMsgApp            *metric.Counter
	RaftRcvdMsgAppResp        *metric.Counter
	RaftRcvdMsgVote           *metric.Counter
	RaftRcvdMsgVoteResp       *metric.Counter
	RaftRcvdMsgPreVote        *metric.Counter
	RaftRcvdMsgPreVoteResp    *metric.Counter
	RaftRcvdMsgSnap           *metric.Counter
	RaftRcvdMsgHeartbeat      *metric.Counter
	RaftRcvdMsgHeartbeatResp  *metric.Counter
	RaftRcvdMsgTransferLeader *metric.Counter
	RaftRcvdMsgTimeoutNow     *metric.Counter
	RaftRcvdMsgDropped        *metric.Counter

	// A map for conveniently finding the appropriate metric. The individual
	// metric references must exist as AddMetricStruct adds them by reflection
	// on this struct and does not process map types.
	// TODO(arjun): eliminate this duplication.
	raftRcvdMessages map[raftpb.MessageType]*metric.Counter

	RaftEnqueuedPending            *metric.Gauge
	RaftCoalescedHeartbeatsPending *metric.Gauge

	// Replica queue metrics.
	GCQueueSuccesses                          *metric.Counter
	GCQueueFailures                           *metric.Counter
	GCQueuePending                            *metric.Gauge
	GCQueueProcessingNanos                    *metric.Counter
	RaftLogQueueSuccesses                     *metric.Counter
	RaftLogQueueFailures                      *metric.Counter
	RaftLogQueuePending                       *metric.Gauge
	RaftLogQueueProcessingNanos               *metric.Counter
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
	GCTransactionSpanGCPending   *metric.Counter
	GCAbortSpanScanned           *metric.Counter
	GCAbortSpanConsidered        *metric.Counter
	GCAbortSpanGCNum             *metric.Counter
	GCPushTxn                    *metric.Counter
	GCResolveTotal               *metric.Counter
	GCResolveSuccess             *metric.Counter

	// Mutex timing information.
	MuStoreNanos        *metric.Histogram
	MuSchedulerNanos    *metric.Histogram
	MuRaftNanos         *metric.Histogram
	MuReplicaNanos      *metric.Histogram
	MuCommandQueueNanos *metric.Histogram

	// Stats for efficient merges.
	mu struct {
		syncutil.Mutex
		stats enginepb.MVCCStats
	}
}

func newStoreMetrics(sampleInterval time.Duration) *StoreMetrics {
	storeRegistry := metric.NewRegistry()
	sm := &StoreMetrics{
		registry: storeRegistry,

		// Replica metrics.
		ReplicaCount:                  metric.NewCounter(metaReplicaCount),
		ReservedReplicaCount:          metric.NewCounter(metaReservedReplicaCount),
		RaftLeaderCount:               metric.NewGauge(metaRaftLeaderCount),
		RaftLeaderNotLeaseHolderCount: metric.NewGauge(metaRaftLeaderNotLeaseHolderCount),
		LeaseHolderCount:              metric.NewGauge(metaLeaseHolderCount),
		QuiescentCount:                metric.NewGauge(metaQuiescentCount),

		// Replica CommandQueue metrics.
		MaxCommandQueueSize:       metric.NewGauge(metaMaxCommandQueueSize),
		MaxCommandQueueWriteCount: metric.NewGauge(metaMaxCommandQueueWriteCount),
		MaxCommandQueueReadCount:  metric.NewGauge(metaMaxCommandQueueReadCount),
		MaxCommandQueueTreeSize:   metric.NewGauge(metaMaxCommandQueueTreeSize),
		MaxCommandQueueOverlaps:   metric.NewGauge(metaMaxCommandQueueOverlaps),
		CombinedCommandQueueSize:  metric.NewGauge(metaCombinedCommandQueueSize),
		CombinedCommandWriteCount: metric.NewGauge(metaCombinedCommandWriteCount),
		CombinedCommandReadCount:  metric.NewGauge(metaCombinedCommandReadCount),

		// Range metrics.
		RangeCount:          metric.NewGauge(metaRangeCount),
		AvailableRangeCount: metric.NewGauge(metaAvailableRangeCount),

		// Replication metrics.
		ReplicaAllocatorNoopCount:       metric.NewGauge(metaReplicaAllocatorNoopCount),
		ReplicaAllocatorRemoveCount:     metric.NewGauge(metaReplicaAllocatorRemoveCount),
		ReplicaAllocatorAddCount:        metric.NewGauge(metaReplicaAllocatorAddCount),
		ReplicaAllocatorRemoveDeadCount: metric.NewGauge(metaReplicaAllocatorRemoveDeadCount),

		// Lease request metrics.
		LeaseRequestSuccessCount: metric.NewCounter(metaLeaseRequestSuccessCount),
		LeaseRequestErrorCount:   metric.NewCounter(metaLeaseRequestErrorCount),

		// Storage metrics.
		LiveBytes:       metric.NewGauge(metaLiveBytes),
		KeyBytes:        metric.NewGauge(metaKeyBytes),
		ValBytes:        metric.NewGauge(metaValBytes),
		IntentBytes:     metric.NewGauge(metaIntentBytes),
		LiveCount:       metric.NewGauge(metaLiveCount),
		KeyCount:        metric.NewGauge(metaKeyCount),
		ValCount:        metric.NewGauge(metaValCount),
		IntentCount:     metric.NewGauge(metaIntentCount),
		IntentAge:       metric.NewGauge(metaIntentAge),
		GcBytesAge:      metric.NewGauge(metaGcBytesAge),
		LastUpdateNanos: metric.NewGauge(metaLastUpdateNanos),
		Capacity:        metric.NewGauge(metaCapacity),
		Available:       metric.NewGauge(metaAvailable),
		Reserved:        metric.NewCounter(metaReserved),
		SysBytes:        metric.NewGauge(metaSysBytes),
		SysCount:        metric.NewGauge(metaSysCount),

		// RocksDB metrics.
		RdbBlockCacheHits:           metric.NewGauge(metaRdbBlockCacheHits),
		RdbBlockCacheMisses:         metric.NewGauge(metaRdbBlockCacheMisses),
		RdbBlockCacheUsage:          metric.NewGauge(metaRdbBlockCacheUsage),
		RdbBlockCachePinnedUsage:    metric.NewGauge(metaRdbBlockCachePinnedUsage),
		RdbBloomFilterPrefixChecked: metric.NewGauge(metaRdbBloomFilterPrefixChecked),
		RdbBloomFilterPrefixUseful:  metric.NewGauge(metaRdbBloomFilterPrefixUseful),
		RdbMemtableHits:             metric.NewGauge(metaRdbMemtableHits),
		RdbMemtableMisses:           metric.NewGauge(metaRdbMemtableMisses),
		RdbMemtableTotalSize:        metric.NewGauge(metaRdbMemtableTotalSize),
		RdbFlushes:                  metric.NewGauge(metaRdbFlushes),
		RdbCompactions:              metric.NewGauge(metaRdbCompactions),
		RdbTableReadersMemEstimate:  metric.NewGauge(metaRdbTableReadersMemEstimate),
		RdbReadAmplification:        metric.NewGauge(metaRdbReadAmplification),
		RdbNumSSTables:              metric.NewGauge(metaRdbNumSSTables),

		// Range event metrics.
		RangeSplits:                     metric.NewCounter(metaRangeSplits),
		RangeAdds:                       metric.NewCounter(metaRangeAdds),
		RangeRemoves:                    metric.NewCounter(metaRangeRemoves),
		RangeSnapshotsGenerated:         metric.NewCounter(metaRangeSnapshotsGenerated),
		RangeSnapshotsNormalApplied:     metric.NewCounter(metaRangeSnapshotsNormalApplied),
		RangeSnapshotsPreemptiveApplied: metric.NewCounter(metaRangeSnapshotsPreemptiveApplied),

		// Raft processing metrics.
		RaftTicks:                metric.NewCounter(metaRaftTicks),
		RaftWorkingDurationNanos: metric.NewCounter(metaRaftWorkingDurationNanos),
		RaftTickingDurationNanos: metric.NewCounter(metaRaftTickingDurationNanos),

		// Raft message metrics.
		RaftRcvdMsgProp:           metric.NewCounter(metaRaftRcvdProp),
		RaftRcvdMsgApp:            metric.NewCounter(metaRaftRcvdApp),
		RaftRcvdMsgAppResp:        metric.NewCounter(metaRaftRcvdAppResp),
		RaftRcvdMsgVote:           metric.NewCounter(metaRaftRcvdVote),
		RaftRcvdMsgVoteResp:       metric.NewCounter(metaRaftRcvdVoteResp),
		RaftRcvdMsgPreVote:        metric.NewCounter(metaRaftRcvdPreVote),
		RaftRcvdMsgPreVoteResp:    metric.NewCounter(metaRaftRcvdPreVoteResp),
		RaftRcvdMsgSnap:           metric.NewCounter(metaRaftRcvdSnap),
		RaftRcvdMsgHeartbeat:      metric.NewCounter(metaRaftRcvdHeartbeat),
		RaftRcvdMsgHeartbeatResp:  metric.NewCounter(metaRaftRcvdHeartbeatResp),
		RaftRcvdMsgTransferLeader: metric.NewCounter(metaRaftRcvdTransferLeader),
		RaftRcvdMsgTimeoutNow:     metric.NewCounter(metaRaftRcvdTimeoutNow),
		RaftRcvdMsgDropped:        metric.NewCounter(metaRaftRcvdDropped),
		raftRcvdMessages:          make(map[raftpb.MessageType]*metric.Counter, len(raftpb.MessageType_name)),

		RaftEnqueuedPending: metric.NewGauge(metaRaftEnqueuedPending),

		// This Gauge measures the number of heartbeats queued up just before
		// the queue is cleared, to avoid flapping wildly.
		RaftCoalescedHeartbeatsPending: metric.NewGauge(metaRaftCoalescedHeartbeatsPending),

		// Replica queue metrics.
		GCQueueSuccesses:                          metric.NewCounter(metaGCQueueSuccesses),
		GCQueueFailures:                           metric.NewCounter(metaGCQueueFailures),
		GCQueuePending:                            metric.NewGauge(metaGCQueuePending),
		GCQueueProcessingNanos:                    metric.NewCounter(metaGCQueueProcessingNanos),
		RaftLogQueueSuccesses:                     metric.NewCounter(metaRaftLogQueueSuccesses),
		RaftLogQueueFailures:                      metric.NewCounter(metaRaftLogQueueFailures),
		RaftLogQueuePending:                       metric.NewGauge(metaRaftLogQueuePending),
		RaftLogQueueProcessingNanos:               metric.NewCounter(metaRaftLogQueueProcessingNanos),
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
		TimeSeriesMaintenanceQueueSuccesses:       metric.NewCounter(metaTimeSeriesMaintenanceQueueFailures),
		TimeSeriesMaintenanceQueueFailures:        metric.NewCounter(metaTimeSeriesMaintenanceQueueSuccesses),
		TimeSeriesMaintenanceQueuePending:         metric.NewGauge(metaTimeSeriesMaintenanceQueuePending),
		TimeSeriesMaintenanceQueueProcessingNanos: metric.NewCounter(metaTimeSeriesMaintenanceQueueProcessingNanos),

		// GCInfo cumulative totals.
		GCNumKeysAffected:            metric.NewCounter(metaGCNumKeysAffected),
		GCIntentsConsidered:          metric.NewCounter(metaGCIntentsConsidered),
		GCIntentTxns:                 metric.NewCounter(metaGCIntentTxns),
		GCTransactionSpanScanned:     metric.NewCounter(metaGCTransactionSpanScanned),
		GCTransactionSpanGCAborted:   metric.NewCounter(metaGCTransactionSpanGCAborted),
		GCTransactionSpanGCCommitted: metric.NewCounter(metaGCTransactionSpanGCCommitted),
		GCTransactionSpanGCPending:   metric.NewCounter(metaGCTransactionSpanGCPending),
		GCAbortSpanScanned:           metric.NewCounter(metaGCAbortSpanScanned),
		GCAbortSpanConsidered:        metric.NewCounter(metaGCAbortSpanConsidered),
		GCAbortSpanGCNum:             metric.NewCounter(metaGCAbortSpanGCNum),
		GCPushTxn:                    metric.NewCounter(metaGCPushTxn),
		GCResolveTotal:               metric.NewCounter(metaGCResolveTotal),
		GCResolveSuccess:             metric.NewCounter(metaGCResolveSuccess),

		// Mutex timing.
		//
		// TODO(tschottdorf): Histograms don't work very well as they were
		// inherently built in a windowed (i.e. events-discarding) way, which
		// is not at all the correct way. Discard at one-minute interval which
		// gives sane (though mathematically nonsensical) results when exposed
		// at the moment.
		MuReplicaNanos: metric.NewHistogram(
			metaMuReplicaNanos, sampleInterval,
			time.Second.Nanoseconds(), 1,
		),
		MuCommandQueueNanos: metric.NewHistogram(
			metaMuCommandQueueNanos, sampleInterval,
			time.Second.Nanoseconds(), 1,
		),
		MuRaftNanos: metric.NewHistogram(
			metaMuRaftNanos, sampleInterval,
			time.Second.Nanoseconds(), 1,
		),
		MuStoreNanos: metric.NewHistogram(
			metaMuStoreNanos, sampleInterval,
			time.Second.Nanoseconds(), 1,
		),
		MuSchedulerNanos: metric.NewHistogram(
			metaMuSchedulerNanos, time.Minute,
			time.Second.Nanoseconds(), 1,
		),
	}

	sm.raftRcvdMessages[raftpb.MsgProp] = sm.RaftRcvdMsgProp
	sm.raftRcvdMessages[raftpb.MsgApp] = sm.RaftRcvdMsgApp
	sm.raftRcvdMessages[raftpb.MsgAppResp] = sm.RaftRcvdMsgAppResp
	sm.raftRcvdMessages[raftpb.MsgVote] = sm.RaftRcvdMsgVote
	sm.raftRcvdMessages[raftpb.MsgVoteResp] = sm.RaftRcvdMsgVoteResp
	sm.raftRcvdMessages[raftpb.MsgPreVote] = sm.RaftRcvdMsgPreVote
	sm.raftRcvdMessages[raftpb.MsgPreVoteResp] = sm.RaftRcvdMsgPreVoteResp
	sm.raftRcvdMessages[raftpb.MsgSnap] = sm.RaftRcvdMsgSnap
	sm.raftRcvdMessages[raftpb.MsgHeartbeat] = sm.RaftRcvdMsgHeartbeat
	sm.raftRcvdMessages[raftpb.MsgHeartbeatResp] = sm.RaftRcvdMsgHeartbeatResp
	sm.raftRcvdMessages[raftpb.MsgTransferLeader] = sm.RaftRcvdMsgTransferLeader
	sm.raftRcvdMessages[raftpb.MsgTimeoutNow] = sm.RaftRcvdMsgTimeoutNow

	storeRegistry.AddMetricStruct(sm)

	return sm
}

// updateGaugesLocked breaks out individual metrics from the MVCCStats object.
// This process should be locked with each stat application to ensure that all
// gauges increase/decrease in step with the application of updates. However,
// this locking is not exposed to the registry level, and therefore a single
// snapshot of these gauges in the registry might mix the values of two
// subsequent updates.
func (sm *StoreMetrics) updateMVCCGaugesLocked() {
	sm.LiveBytes.Update(sm.mu.stats.LiveBytes)
	sm.KeyBytes.Update(sm.mu.stats.KeyBytes)
	sm.ValBytes.Update(sm.mu.stats.ValBytes)
	sm.IntentBytes.Update(sm.mu.stats.IntentBytes)
	sm.LiveCount.Update(sm.mu.stats.LiveCount)
	sm.KeyCount.Update(sm.mu.stats.KeyCount)
	sm.ValCount.Update(sm.mu.stats.ValCount)
	sm.IntentCount.Update(sm.mu.stats.IntentCount)
	sm.IntentAge.Update(sm.mu.stats.IntentAge)
	sm.GcBytesAge.Update(sm.mu.stats.GCBytesAge)
	sm.LastUpdateNanos.Update(sm.mu.stats.LastUpdateNanos)
	sm.SysBytes.Update(sm.mu.stats.SysBytes)
	sm.SysCount.Update(sm.mu.stats.SysCount)
}

func (sm *StoreMetrics) addMVCCStats(stats enginepb.MVCCStats) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.stats.Add(stats)
	sm.updateMVCCGaugesLocked()
}

func (sm *StoreMetrics) subtractMVCCStats(stats enginepb.MVCCStats) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.stats.Subtract(stats)
	sm.updateMVCCGaugesLocked()
}

func (sm *StoreMetrics) updateRocksDBStats(stats engine.Stats) {
	// We do not grab a lock here, because it's not possible to get a point-in-
	// time snapshot of RocksDB stats. Retrieving RocksDB stats doesn't grab any
	// locks, and there's no way to retrieve multiple stats in a single operation.
	sm.RdbBlockCacheHits.Update(stats.BlockCacheHits)
	sm.RdbBlockCacheMisses.Update(stats.BlockCacheMisses)
	sm.RdbBlockCacheUsage.Update(stats.BlockCacheUsage)
	sm.RdbBlockCachePinnedUsage.Update(stats.BlockCachePinnedUsage)
	sm.RdbBloomFilterPrefixUseful.Update(stats.BloomFilterPrefixUseful)
	sm.RdbBloomFilterPrefixChecked.Update(stats.BloomFilterPrefixChecked)
	sm.RdbMemtableHits.Update(stats.MemtableHits)
	sm.RdbMemtableMisses.Update(stats.MemtableMisses)
	sm.RdbMemtableTotalSize.Update(stats.MemtableTotalSize)
	sm.RdbFlushes.Update(stats.Flushes)
	sm.RdbCompactions.Update(stats.Compactions)
	sm.RdbTableReadersMemEstimate.Update(stats.TableReadersMemEstimate)
}

func (sm *StoreMetrics) leaseRequestComplete(success bool) {
	if success {
		sm.LeaseRequestSuccessCount.Inc(1)
	} else {
		sm.LeaseRequestErrorCount.Inc(1)
	}
}
