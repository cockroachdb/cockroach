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
	metaReplicaCount = metric.Metadata{
		Name: "replicas",
		Help: "Number of replicas"}
	metaReservedReplicaCount = metric.Metadata{
		Name: "replicas.reserved",
		Help: "Number of replicas reserved for snapshots"}
	metaRaftLeaderCount = metric.Metadata{
		Name: "replicas.leaders",
		Help: "Number of raft leaders"}
	metaRaftLeaderNotLeaseHolderCount = metric.Metadata{
		Name: "replicas.leaders_not_leaseholders",
		Help: "Number of replicas that are Raft leaders whose range lease is held by another store",
	}
	metaLeaseHolderCount = metric.Metadata{
		Name: "replicas.leaseholders",
		Help: "Number of lease holders"}
	metaQuiescentCount = metric.Metadata{
		Name: "replicas.quiescent",
		Help: "Number of quiesced replicas"}

	// Replica CommandQueue metrics. Max size metrics track the maximum value
	// seen for all replicas during a single replica scan.
	metaMaxCommandQueueSize = metric.Metadata{
		Name: "replicas.commandqueue.maxsize",
		Help: "Largest number of commands in any CommandQueue"}
	metaMaxCommandQueueWriteCount = metric.Metadata{
		Name: "replicas.commandqueue.maxwritecount",
		Help: "Largest number of read-write commands in any CommandQueue"}
	metaMaxCommandQueueReadCount = metric.Metadata{
		Name: "replicas.commandqueue.maxreadcount",
		Help: "Largest number of read-only commands in any CommandQueue"}
	metaMaxCommandQueueTreeSize = metric.Metadata{
		Name: "replicas.commandqueue.maxtreesize",
		Help: "Largest number of intervals in any CommandQueue's interval tree"}
	metaMaxCommandQueueOverlaps = metric.Metadata{
		Name: "replicas.commandqueue.maxoverlaps",
		Help: "Largest number of overlapping commands seen when adding to any CommandQueue"}
	metaCombinedCommandQueueSize = metric.Metadata{
		Name: "replicas.commandqueue.combinedqueuesize",
		Help: "Number of commands in all CommandQueues combined"}
	metaCombinedCommandWriteCount = metric.Metadata{
		Name: "replicas.commandqueue.combinedwritecount",
		Help: "Number of read-write commands in all CommandQueues combined"}
	metaCombinedCommandReadCount = metric.Metadata{
		Name: "replicas.commandqueue.combinedreadcount",
		Help: "Number of read-only commands in all CommandQueues combined"}

	// Range metrics.
	metaRangeCount = metric.Metadata{
		Name: "ranges",
		Help: "Number of ranges"}
	metaUnavailableRangeCount = metric.Metadata{
		Name: "ranges.unavailable",
		Help: "Number of ranges with fewer live replicas than needed for quorum"}
	metaUnderReplicatedRangeCount = metric.Metadata{
		Name: "ranges.underreplicated",
		Help: "Number of ranges with fewer live replicas than the replication target"}

	// Lease request metrics.
	metaLeaseRequestSuccessCount = metric.Metadata{
		Name: "leases.success",
		Help: "Number of successful lease requests"}
	metaLeaseRequestErrorCount = metric.Metadata{
		Name: "leases.error",
		Help: "Number of failed lease requests"}
	metaLeaseTransferSuccessCount = metric.Metadata{
		Name: "leases.transfers.success",
		Help: "Number of successful lease transfers"}
	metaLeaseTransferErrorCount = metric.Metadata{
		Name: "leases.transfers.error",
		Help: "Number of failed lease transfers"}
	metaLeaseExpirationCount = metric.Metadata{
		Name: "leases.expiration",
		Help: "Number of replicas using expiration-based leases"}
	metaLeaseEpochCount = metric.Metadata{
		Name: "leases.epoch",
		Help: "Number of replicas using epoch-based leases"}

	// Storage metrics.
	metaLiveBytes = metric.Metadata{
		Name: "livebytes",
		Help: "Number of bytes of live data (keys plus values)"}
	metaKeyBytes = metric.Metadata{
		Name: "keybytes",
		Help: "Number of bytes taken up by keys"}
	metaValBytes = metric.Metadata{
		Name: "valbytes",
		Help: "Number of bytes taken up by values"}
	metaIntentBytes = metric.Metadata{
		Name: "intentbytes",
		Help: "Number of bytes in intent KV pairs"}
	metaLiveCount = metric.Metadata{
		Name: "livecount",
		Help: "Count of live keys"}
	metaKeyCount = metric.Metadata{
		Name: "keycount",
		Help: "Count of all keys"}
	metaValCount = metric.Metadata{
		Name: "valcount",
		Help: "Count of all values"}
	metaIntentCount = metric.Metadata{
		Name: "intentcount",
		Help: "Count of intent keys"}
	metaIntentAge = metric.Metadata{
		Name: "intentage",
		Help: "Cumulative age of intents"}
	metaGcBytesAge = metric.Metadata{
		Name: "gcbytesage",
		Help: "Cumulative age of non-live data"}
	metaLastUpdateNanos = metric.Metadata{
		Name: "lastupdatenanos",
		Help: "Time at which ages were last updated"}
	metaCapacity = metric.Metadata{
		Name: "capacity",
		Help: "Total storage capacity"}
	metaAvailable = metric.Metadata{
		Name: "capacity.available",
		Help: "Available storage capacity"}
	metaReserved = metric.Metadata{
		Name: "capacity.reserved",
		Help: "Capacity reserved for snapshots"}
	metaSysBytes = metric.Metadata{
		Name: "sysbytes",
		Help: "Number of bytes in system KV pairs"}
	metaSysCount = metric.Metadata{
		Name: "syscount",
		Help: "Count of system KV pairs"}

	// RocksDB metrics.
	metaRdbBlockCacheHits = metric.Metadata{
		Name: "rocksdb.block.cache.hits",
		Help: "Count of block cache hits"}
	metaRdbBlockCacheMisses = metric.Metadata{
		Name: "rocksdb.block.cache.misses",
		Help: "Count of block cache misses"}
	metaRdbBlockCacheUsage = metric.Metadata{
		Name: "rocksdb.block.cache.usage",
		Help: "Bytes used by the block cache"}
	metaRdbBlockCachePinnedUsage = metric.Metadata{
		Name: "rocksdb.block.cache.pinned-usage",
		Help: "Bytes pinned by the block cache"}
	metaRdbBloomFilterPrefixChecked = metric.Metadata{
		Name: "rocksdb.bloom.filter.prefix.checked",
		Help: "Number of times the bloom filter was checked"}
	metaRdbBloomFilterPrefixUseful = metric.Metadata{
		Name: "rocksdb.bloom.filter.prefix.useful",
		Help: "Number of times the bloom filter helped avoid iterator creation"}
	metaRdbMemtableHits = metric.Metadata{
		Name: "rocksdb.memtable.hits",
		Help: "Number of memtable hits"}
	metaRdbMemtableMisses = metric.Metadata{
		Name: "rocksdb.memtable.misses",
		Help: "Number of memtable misses"}
	metaRdbMemtableTotalSize = metric.Metadata{
		Name: "rocksdb.memtable.total-size",
		Help: "Current size of memtable"}
	metaRdbFlushes = metric.Metadata{
		Name: "rocksdb.flushes",
		Help: "Number of table flushes"}
	metaRdbCompactions = metric.Metadata{
		Name: "rocksdb.compactions",
		Help: "Number of table compactions"}
	metaRdbTableReadersMemEstimate = metric.Metadata{
		Name: "rocksdb.table-readers-mem-estimate",
		Help: "Memory used by index and filter blocks"}
	metaRdbReadAmplification = metric.Metadata{
		Name: "rocksdb.read-amplification",
		Help: "Number of disk reads per query"}
	metaRdbNumSSTables = metric.Metadata{
		Name: "rocksdb.num-sstables",
		Help: "Number of rocksdb SSTables"}

	// Range event metrics.
	metaRangeSplits = metric.Metadata{
		Name: "range.splits",
		Help: "Number of range splits"}
	metaRangeAdds = metric.Metadata{
		Name: "range.adds",
		Help: "Number of range additions"}
	metaRangeRemoves = metric.Metadata{
		Name: "range.removes",
		Help: "Number of range removals"}
	metaRangeSnapshotsGenerated = metric.Metadata{
		Name: "range.snapshots.generated",
		Help: "Number of generated snapshots"}
	metaRangeSnapshotsNormalApplied = metric.Metadata{
		Name: "range.snapshots.normal-applied",
		Help: "Number of applied snapshots"}
	metaRangeSnapshotsPreemptiveApplied = metric.Metadata{
		Name: "range.snapshots.preemptive-applied",
		Help: "Number of applied pre-emptive snapshots"}
	metaRangeRaftLeaderTransfers = metric.Metadata{
		Name: "range.raftleadertransfers",
		Help: "Number of raft leader transfers"}

	// Raft processing metrics.
	metaRaftTicks = metric.Metadata{
		Name: "raft.ticks",
		Help: "Number of Raft ticks queued"}
	metaRaftWorkingDurationNanos = metric.Metadata{
		Name: "raft.process.workingnanos",
		Help: "Nanoseconds spent in store.processRaft() working"}
	metaRaftTickingDurationNanos = metric.Metadata{
		Name: "raft.process.tickingnanos",
		Help: "Nanoseconds spent in store.processRaft() processing replica.Tick()"}
	metaRaftCommandsApplied = metric.Metadata{
		Name: "raft.commandsapplied",
		Help: "Count of Raft commands applied"}

	// Raft message metrics.
	metaRaftRcvdProp = metric.Metadata{
		Name: "raft.rcvd.prop",
		Help: "Number of MsgProp messages received by this store"}
	metaRaftRcvdApp = metric.Metadata{
		Name: "raft.rcvd.app",
		Help: "Number of MsgApp messages received by this store"}
	metaRaftRcvdAppResp = metric.Metadata{
		Name: "raft.rcvd.appresp",
		Help: "Number of MsgAppResp messages received by this store"}
	metaRaftRcvdVote = metric.Metadata{
		Name: "raft.rcvd.vote",
		Help: "Number of MsgVote messages received by this store"}
	metaRaftRcvdVoteResp = metric.Metadata{
		Name: "raft.rcvd.voteresp",
		Help: "Number of MsgVoteResp messages received by this store"}
	metaRaftRcvdPreVote = metric.Metadata{
		Name: "raft.rcvd.prevote",
		Help: "Number of MsgPreVote messages received by this store"}
	metaRaftRcvdPreVoteResp = metric.Metadata{
		Name: "raft.rcvd.prevoteresp",
		Help: "Number of MsgPreVoteResp messages received by this store"}
	metaRaftRcvdSnap = metric.Metadata{
		Name: "raft.rcvd.snap",
		Help: "Number of MsgSnap messages received by this store"}
	metaRaftRcvdHeartbeat = metric.Metadata{
		Name: "raft.rcvd.heartbeat",
		Help: "Number of (coalesced, if enabled) MsgHeartbeat messages received by this store"}
	metaRaftRcvdHeartbeatResp = metric.Metadata{
		Name: "raft.rcvd.heartbeatresp",
		Help: "Number of (coalesced, if enabled) MsgHeartbeatResp messages received by this store"}
	metaRaftRcvdTransferLeader = metric.Metadata{
		Name: "raft.rcvd.transferleader",
		Help: "Number of MsgTransferLeader messages received by this store"}
	metaRaftRcvdTimeoutNow = metric.Metadata{
		Name: "raft.rcvd.timeoutnow",
		Help: "Number of MsgTimeoutNow messages received by this store"}
	metaRaftRcvdDropped = metric.Metadata{
		Name: "raft.rcvd.dropped",
		Help: "Number of dropped incoming Raft messages"}
	metaRaftEnqueuedPending = metric.Metadata{
		Name: "raft.enqueued.pending",
		Help: "Number of pending outgoing messages in the Raft Transport queue"}
	metaRaftCoalescedHeartbeatsPending = metric.Metadata{
		Name: "raft.heartbeats.pending",
		Help: "Number of pending heartbeats and responses waiting to be coalesced"}

	// Raft log metrics.
	metaRaftLogFollowerBehindCount = metric.Metadata{
		Name: "raftlog.behind",
		Help: "Number of Raft log entries followers on other stores are behind"}
	metaRaftLogSelfBehindCount = metric.Metadata{
		Name: "raftlog.selfbehind",
		Help: "Number of Raft log entries followers on this store are behind"}
	metaRaftLogTruncated = metric.Metadata{
		Name: "raftlog.truncated",
		Help: "Number of Raft log entries truncated"}

	// Replica queue metrics.
	metaGCQueueSuccesses = metric.Metadata{
		Name: "queue.gc.process.success",
		Help: "Number of replicas successfully processed by the GC queue"}
	metaGCQueueFailures = metric.Metadata{
		Name: "queue.gc.process.failure",
		Help: "Number of replicas which failed processing in the GC queue"}
	metaGCQueuePending = metric.Metadata{
		Name: "queue.gc.pending",
		Help: "Number of pending replicas in the GC queue"}
	metaGCQueueProcessingNanos = metric.Metadata{
		Name: "queue.gc.processingnanos",
		Help: "Nanoseconds spent processing replicas in the GC queue"}
	metaRaftLogQueueSuccesses = metric.Metadata{
		Name: "queue.raftlog.process.success",
		Help: "Number of replicas successfully processed by the Raft log queue"}
	metaRaftLogQueueFailures = metric.Metadata{
		Name: "queue.raftlog.process.failure",
		Help: "Number of replicas which failed processing in the Raft log queue"}
	metaRaftLogQueuePending = metric.Metadata{
		Name: "queue.raftlog.pending",
		Help: "Number of pending replicas in the Raft log queue"}
	metaRaftLogQueueProcessingNanos = metric.Metadata{
		Name: "queue.raftlog.processingnanos",
		Help: "Nanoseconds spent processing replicas in the Raft log queue"}
	metaRaftSnapshotQueueSuccesses = metric.Metadata{
		Name: "queue.raftsnapshot.process.success",
		Help: "Number of replicas successfully processed by the Raft repair queue"}
	metaRaftSnapshotQueueFailures = metric.Metadata{
		Name: "queue.raftsnapshot.process.failure",
		Help: "Number of replicas which failed processing in the Raft repair queue"}
	metaRaftSnapshotQueuePending = metric.Metadata{
		Name: "queue.raftsnapshot.pending",
		Help: "Number of pending replicas in the Raft repair queue"}
	metaRaftSnapshotQueueProcessingNanos = metric.Metadata{
		Name: "queue.raftsnapshot.processingnanos",
		Help: "Nanoseconds spent processing replicas in the Raft repair queue"}
	metaConsistencyQueueSuccesses = metric.Metadata{
		Name: "queue.consistency.process.success",
		Help: "Number of replicas successfully processed by the consistency checker queue"}
	metaConsistencyQueueFailures = metric.Metadata{
		Name: "queue.consistency.process.failure",
		Help: "Number of replicas which failed processing in the consistency checker queue"}
	metaConsistencyQueuePending = metric.Metadata{
		Name: "queue.consistency.pending",
		Help: "Number of pending replicas in the consistency checker queue"}
	metaConsistencyQueueProcessingNanos = metric.Metadata{
		Name: "queue.consistency.processingnanos",
		Help: "Nanoseconds spent processing replicas in the consistency checker queue"}
	metaReplicaGCQueueSuccesses = metric.Metadata{
		Name: "queue.replicagc.process.success",
		Help: "Number of replicas successfully processed by the replica GC queue"}
	metaReplicaGCQueueFailures = metric.Metadata{
		Name: "queue.replicagc.process.failure",
		Help: "Number of replicas which failed processing in the replica GC queue"}
	metaReplicaGCQueuePending = metric.Metadata{
		Name: "queue.replicagc.pending",
		Help: "Number of pending replicas in the replica GC queue"}
	metaReplicaGCQueueProcessingNanos = metric.Metadata{
		Name: "queue.replicagc.processingnanos",
		Help: "Nanoseconds spent processing replicas in the replica GC queue"}
	metaReplicateQueueSuccesses = metric.Metadata{
		Name: "queue.replicate.process.success",
		Help: "Number of replicas successfully processed by the replicate queue"}
	metaReplicateQueueFailures = metric.Metadata{
		Name: "queue.replicate.process.failure",
		Help: "Number of replicas which failed processing in the replicate queue"}
	metaReplicateQueuePending = metric.Metadata{
		Name: "queue.replicate.pending",
		Help: "Number of pending replicas in the replicate queue"}
	metaReplicateQueueProcessingNanos = metric.Metadata{
		Name: "queue.replicate.processingnanos",
		Help: "Nanoseconds spent processing replicas in the replicate queue"}
	metaReplicateQueuePurgatory = metric.Metadata{
		Name: "queue.replicate.purgatory",
		Help: "Number of replicas in the replicate queue's purgatory, awaiting allocation options"}
	metaSplitQueueSuccesses = metric.Metadata{
		Name: "queue.split.process.success",
		Help: "Number of replicas successfully processed by the split queue"}
	metaSplitQueueFailures = metric.Metadata{
		Name: "queue.split.process.failure",
		Help: "Number of replicas which failed processing in the split queue"}
	metaSplitQueuePending = metric.Metadata{
		Name: "queue.split.pending",
		Help: "Number of pending replicas in the split queue"}
	metaSplitQueueProcessingNanos = metric.Metadata{
		Name: "queue.split.processingnanos",
		Help: "Nanoseconds spent processing replicas in the split queue"}
	metaTimeSeriesMaintenanceQueueSuccesses = metric.Metadata{
		Name: "queue.tsmaintenance.process.success",
		Help: "Number of replicas successfully processed by the time series maintenance queue"}
	metaTimeSeriesMaintenanceQueueFailures = metric.Metadata{
		Name: "queue.tsmaintenance.process.failure",
		Help: "Number of replicas which failed processing in the time series maintenance queue"}
	metaTimeSeriesMaintenanceQueuePending = metric.Metadata{
		Name: "queue.tsmaintenance.pending",
		Help: "Number of pending replicas in the time series maintenance queue"}
	metaTimeSeriesMaintenanceQueueProcessingNanos = metric.Metadata{
		Name: "queue.tsmaintenance.processingnanos",
		Help: "Nanoseconds spent processing replicas in the time series maintenance queue"}

	// GCInfo cumulative totals.
	metaGCNumKeysAffected = metric.Metadata{
		Name: "queue.gc.info.numkeysaffected",
		Help: "Number of keys with GC'able data"}
	metaGCIntentsConsidered = metric.Metadata{
		Name: "queue.gc.info.intentsconsidered",
		Help: "Number of 'old' intents"}
	metaGCIntentTxns = metric.Metadata{
		Name: "queue.gc.info.intenttxns",
		Help: "Number of associated distinct transactions"}
	metaGCTransactionSpanScanned = metric.Metadata{
		Name: "queue.gc.info.transactionspanscanned",
		Help: "Number of entries in the transaction span scanned from the engine"}
	metaGCTransactionSpanGCAborted = metric.Metadata{
		Name: "queue.gc.info.transactionspangcaborted",
		Help: "Number of GC'able entries corresponding to aborted txns"}
	metaGCTransactionSpanGCCommitted = metric.Metadata{
		Name: "queue.gc.info.transactionspangccommitted",
		Help: "Number of GC'able entries corresponding to committed txns"}
	metaGCTransactionSpanGCPending = metric.Metadata{
		Name: "queue.gc.info.transactionspangcpending",
		Help: "Number of GC'able entries corresponding to pending txns"}
	metaGCAbortSpanScanned = metric.Metadata{
		Name: "queue.gc.info.abortspanscanned",
		Help: "Number of transactions present in the abort cache scanned from the engine"}
	metaGCAbortSpanConsidered = metric.Metadata{
		Name: "queue.gc.info.abortspanconsidered",
		Help: "Number of abort cache entries old enough to be considered for removal"}
	metaGCAbortSpanGCNum = metric.Metadata{
		Name: "queue.gc.info.abortspangcnum",
		Help: "Number of abort cache entries fit for removal"}
	metaGCPushTxn = metric.Metadata{
		Name: "queue.gc.info.pushtxn",
		Help: "Number of attempted pushes"}
	metaGCResolveTotal = metric.Metadata{
		Name: "queue.gc.info.resolvetotal",
		Help: "Number of attempted intent resolutions"}
	metaGCResolveSuccess = metric.Metadata{
		Name: "queue.gc.info.resolvesuccess",
		Help: "Number of successful intent resolutions"}

	// Slow request metrics.
	metaSlowCommandQueueRequests = metric.Metadata{
		Name: "requests.slow.commandqueue",
		Help: "Number of requests that have been stuck for a long time in the command queue"}
	metaSlowLeaseRequests = metric.Metadata{
		Name: "requests.slow.lease",
		Help: "Number of requests that have been stuck for a long time acquiring a lease"}
	metaSlowRaftRequests = metric.Metadata{
		Name: "requests.slow.raft",
		Help: "Number of requests that have been stuck for a long time in raft"}
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
	RangeCount                *metric.Gauge
	UnavailableRangeCount     *metric.Gauge
	UnderReplicatedRangeCount *metric.Gauge

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
	RangeRaftLeaderTransfers        *metric.Counter

	// Raft processing metrics.
	RaftTicks                *metric.Counter
	RaftWorkingDurationNanos *metric.Counter
	RaftTickingDurationNanos *metric.Counter
	RaftCommandsApplied      *metric.Counter

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

	// Raft log metrics.
	RaftLogFollowerBehindCount *metric.Gauge
	RaftLogSelfBehindCount     *metric.Gauge
	RaftLogTruncated           *metric.Counter

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

	// Slow request counts.
	SlowCommandQueueRequests *metric.Gauge
	SlowLeaseRequests        *metric.Gauge
	SlowRaftRequests         *metric.Gauge

	// Stats for efficient merges.
	mu struct {
		syncutil.Mutex
		stats enginepb.MVCCStats
	}
}

func newStoreMetrics(histogramWindow time.Duration) *StoreMetrics {
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
		RangeCount:                metric.NewGauge(metaRangeCount),
		UnavailableRangeCount:     metric.NewGauge(metaUnavailableRangeCount),
		UnderReplicatedRangeCount: metric.NewGauge(metaUnderReplicatedRangeCount),

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
		RangeRaftLeaderTransfers:        metric.NewCounter(metaRangeRaftLeaderTransfers),

		// Raft processing metrics.
		RaftTicks:                metric.NewCounter(metaRaftTicks),
		RaftWorkingDurationNanos: metric.NewCounter(metaRaftWorkingDurationNanos),
		RaftTickingDurationNanos: metric.NewCounter(metaRaftTickingDurationNanos),
		RaftCommandsApplied:      metric.NewCounter(metaRaftCommandsApplied),

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

		// Raft log metrics.
		RaftLogFollowerBehindCount: metric.NewGauge(metaRaftLogFollowerBehindCount),
		RaftLogSelfBehindCount:     metric.NewGauge(metaRaftLogSelfBehindCount),
		RaftLogTruncated:           metric.NewCounter(metaRaftLogTruncated),

		// Replica queue metrics.
		GCQueueSuccesses:                          metric.NewCounter(metaGCQueueSuccesses),
		GCQueueFailures:                           metric.NewCounter(metaGCQueueFailures),
		GCQueuePending:                            metric.NewGauge(metaGCQueuePending),
		GCQueueProcessingNanos:                    metric.NewCounter(metaGCQueueProcessingNanos),
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

		// Wedge request counters.
		SlowCommandQueueRequests: metric.NewGauge(metaSlowCommandQueueRequests),
		SlowLeaseRequests:        metric.NewGauge(metaSlowLeaseRequests),
		SlowRaftRequests:         metric.NewGauge(metaSlowRaftRequests),
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

func (sm *StoreMetrics) leaseTransferComplete(success bool) {
	if success {
		sm.LeaseTransferSuccessCount.Inc(1)
	} else {
		sm.LeaseTransferErrorCount.Inc(1)
	}
}
