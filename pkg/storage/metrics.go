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
		Name:        "replicas",
		Help:        "Number of replicas",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaReservedReplicaCount = metric.Metadata{
		Name:        "replicas.reserved",
		Help:        "Number of replicas reserved for snapshots",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftLeaderCount = metric.Metadata{
		Name:        "replicas.leaders",
		Help:        "Number of raft leaders",
		Unit:        "Raft Leaders",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftLeaderNotLeaseHolderCount = metric.Metadata{
		Name:        "replicas.leaders_not_leaseholders",
		Help:        "Number of replicas that are Raft leaders whose range lease is held by another store",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaLeaseHolderCount = metric.Metadata{
		Name:        "replicas.leaseholders",
		Help:        "Number of lease holders",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaQuiescentCount = metric.Metadata{
		Name:        "replicas.quiescent",
		Help:        "Number of quiesced replicas",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}

	// Replica CommandQueue metrics. Max size metrics track the maximum value
	// seen for all replicas during a single replica scan.
	metaMaxCommandQueueSize = metric.Metadata{
		Name:        "replicas.commandqueue.maxsize",
		Help:        "Largest number of commands in any CommandQueue",
		Unit:        "Commands",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaMaxCommandQueueWriteCount = metric.Metadata{
		Name:        "replicas.commandqueue.maxwritecount",
		Help:        "Largest number of read-write commands in any CommandQueue",
		Unit:        "Commands",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaMaxCommandQueueReadCount = metric.Metadata{
		Name:        "replicas.commandqueue.maxreadcount",
		Help:        "Largest number of read-only commands in any CommandQueue",
		Unit:        "Commands",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaMaxCommandQueueTreeSize = metric.Metadata{
		Name:        "replicas.commandqueue.maxtreesize",
		Help:        "Largest number of intervals in any CommandQueue's interval tree",
		Unit:        "Intervals",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaMaxCommandQueueOverlaps = metric.Metadata{
		Name:        "replicas.commandqueue.maxoverlaps",
		Help:        "Largest number of overlapping commands seen when adding to any CommandQueue",
		Unit:        "Commands",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaCombinedCommandQueueSize = metric.Metadata{
		Name:        "replicas.commandqueue.combinedqueuesize",
		Help:        "Number of commands in all CommandQueues combined",
		Unit:        "Commands",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaCombinedCommandWriteCount = metric.Metadata{
		Name:        "replicas.commandqueue.combinedwritecount",
		Help:        "Number of read-write commands in all CommandQueues combined",
		Unit:        "Commands",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaCombinedCommandReadCount = metric.Metadata{
		Name:        "replicas.commandqueue.combinedreadcount",
		Help:        "Number of read-only commands in all CommandQueues combined",
		Unit:        "Commands",
		DisplayUnit: metric.DisplayUnit_Count,
	}

	// Range metrics.
	metaRangeCount = metric.Metadata{
		Name:        "ranges",
		Help:        "Number of ranges",
		Unit:        "Ranges",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaUnavailableRangeCount = metric.Metadata{
		Name:        "ranges.unavailable",
		Help:        "Number of ranges with fewer live replicas than needed for quorum",
		Unit:        "Ranges",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaUnderReplicatedRangeCount = metric.Metadata{
		Name:        "ranges.underreplicated",
		Help:        "Number of ranges with fewer live replicas than the replication target",
		Unit:        "Ranges",
		DisplayUnit: metric.DisplayUnit_Count,
	}

	// Lease request metrics.
	metaLeaseRequestSuccessCount = metric.Metadata{
		Name:        "leases.success",
		Help:        "Number of successful lease requests",
		Unit:        "Lease Requests",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaLeaseRequestErrorCount = metric.Metadata{
		Name:        "leases.error",
		Help:        "Number of failed lease requests",
		Unit:        "Lease Requests",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaLeaseTransferSuccessCount = metric.Metadata{
		Name:        "leases.transfers.success",
		Help:        "Number of successful lease transfers",
		Unit:        "Lease Transfers",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaLeaseTransferErrorCount = metric.Metadata{
		Name:        "leases.transfers.error",
		Help:        "Number of failed lease transfers",
		Unit:        "Lease Transfers",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaLeaseExpirationCount = metric.Metadata{
		Name:        "leases.expiration",
		Help:        "Number of replica leaseholders using expiration-based leases",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaLeaseEpochCount = metric.Metadata{
		Name:        "leases.epoch",
		Help:        "Number of replica leaseholders using epoch-based leases",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}

	// Storage metrics.
	metaLiveBytes = metric.Metadata{
		Name:        "livebytes",
		Help:        "Number of bytes of live data (keys plus values)",
		Unit:        "Storage",
		DisplayUnit: metric.DisplayUnit_Bytes,
	}
	metaKeyBytes = metric.Metadata{
		Name:        "keybytes",
		Help:        "Number of bytes taken up by keys",
		Unit:        "Storage",
		DisplayUnit: metric.DisplayUnit_Bytes,
	}
	metaValBytes = metric.Metadata{
		Name:        "valbytes",
		Help:        "Number of bytes taken up by values",
		Unit:        "Storage",
		DisplayUnit: metric.DisplayUnit_Bytes,
	}
	metaTotalBytes = metric.Metadata{
		Name:        "totalbytes",
		Help:        "Total number of bytes taken up by keys and values including non-live data",
		Unit:        "Storage",
		DisplayUnit: metric.DisplayUnit_Bytes,
	}
	metaIntentBytes = metric.Metadata{
		Name:        "intentbytes",
		Help:        "Number of bytes in intent KV pairs",
		Unit:        "Storage",
		DisplayUnit: metric.DisplayUnit_Bytes,
	}
	metaLiveCount = metric.Metadata{
		Name:        "livecount",
		Help:        "Count of live keys",
		Unit:        "Keys",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaKeyCount = metric.Metadata{
		Name:        "keycount",
		Help:        "Count of all keys",
		Unit:        "Keys",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaValCount = metric.Metadata{
		Name:        "valcount",
		Help:        "Count of all values",
		Unit:        "MVCC Values",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaIntentCount = metric.Metadata{
		Name:        "intentcount",
		Help:        "Count of intent keys",
		Unit:        "Keys",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaIntentAge = metric.Metadata{
		Name:        "intentage",
		Help:        "Cumulative age of intents in seconds",
		Unit:        "Age",
		DisplayUnit: metric.DisplayUnit_Nanoseconds,
	}
	metaGcBytesAge = metric.Metadata{
		Name:        "gcbytesage",
		Help:        "Cumulative age of non-live data in seconds",
		Unit:        "Age",
		DisplayUnit: metric.DisplayUnit_Nanoseconds,
	}
	metaLastUpdateNanos = metric.Metadata{
		Name:        "lastupdatenanos",
		Help:        "Time in nanoseconds since Unix epoch at which bytes/keys/intents metrics were last updated",
		Unit:        "Last Update",
		DisplayUnit: metric.DisplayUnit_Timestamp,
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
		Unit:        "Storage",
		DisplayUnit: metric.DisplayUnit_Bytes,
	}
	metaAvailable = metric.Metadata{
		Name:        "capacity.available",
		Help:        "Available storage capacity",
		Unit:        "Storage",
		DisplayUnit: metric.DisplayUnit_Bytes,
	}
	metaUsed = metric.Metadata{
		Name:        "capacity.used",
		Help:        "Used storage capacity",
		Unit:        "Storage",
		DisplayUnit: metric.DisplayUnit_Bytes,
	}

	metaReserved = metric.Metadata{
		Name:        "capacity.reserved",
		Help:        "Capacity reserved for snapshots",
		Unit:        "Storage",
		DisplayUnit: metric.DisplayUnit_Bytes,
	}
	metaSysBytes = metric.Metadata{
		Name:        "sysbytes",
		Help:        "Number of bytes in system KV pairs",
		Unit:        "Storage",
		DisplayUnit: metric.DisplayUnit_Bytes,
	}
	metaSysCount = metric.Metadata{
		Name:        "syscount",
		Help:        "Count of system KV pairs",
		Unit:        "Keys",
		DisplayUnit: metric.DisplayUnit_Count,
	}

	// Metrics used by the rebalancing logic that aren't already captured elsewhere.
	metaAverageWritesPerSecond = metric.Metadata{
		Name:        "rebalancing.writespersecond",
		Help:        "Number of keys written (i.e. applied by raft) per second to the store, averaged over a large time period as used in rebalancing decisions",
		Unit:        "Keys/Sec",
		DisplayUnit: metric.DisplayUnit_Count,
	}

	// RocksDB metrics.
	metaRdbBlockCacheHits = metric.Metadata{
		Name:        "rocksdb.block.cache.hits",
		Help:        "Count of block cache hits",
		Unit:        "Cache Ops",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRdbBlockCacheMisses = metric.Metadata{
		Name:        "rocksdb.block.cache.misses",
		Help:        "Count of block cache misses",
		Unit:        "Cache Ops",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRdbBlockCacheUsage = metric.Metadata{
		Name:        "rocksdb.block.cache.usage",
		Help:        "Bytes used by the block cache",
		Unit:        "Memory",
		DisplayUnit: metric.DisplayUnit_Bytes,
	}
	metaRdbBlockCachePinnedUsage = metric.Metadata{
		Name:        "rocksdb.block.cache.pinned-usage",
		Help:        "Bytes pinned by the block cache",
		Unit:        "Memory",
		DisplayUnit: metric.DisplayUnit_Bytes,
	}
	metaRdbBloomFilterPrefixChecked = metric.Metadata{
		Name:        "rocksdb.bloom.filter.prefix.checked",
		Help:        "Number of times the bloom filter was checked",
		Unit:        "Bloom Filter Ops",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRdbBloomFilterPrefixUseful = metric.Metadata{
		Name:        "rocksdb.bloom.filter.prefix.useful",
		Help:        "Number of times the bloom filter helped avoid iterator creation",
		Unit:        "Bloom Filter Ops",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRdbMemtableTotalSize = metric.Metadata{
		Name:        "rocksdb.memtable.total-size",
		Help:        "Current size of memtable in bytes",
		Unit:        "Memory",
		DisplayUnit: metric.DisplayUnit_Bytes,
	}
	metaRdbFlushes = metric.Metadata{
		Name:        "rocksdb.flushes",
		Help:        "Number of table flushes",
		Unit:        "Flushes",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRdbCompactions = metric.Metadata{
		Name:        "rocksdb.compactions",
		Help:        "Number of table compactions",
		Unit:        "Compactions",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRdbTableReadersMemEstimate = metric.Metadata{
		Name:        "rocksdb.table-readers-mem-estimate",
		Help:        "Memory used by index and filter blocks",
		Unit:        "Memory",
		DisplayUnit: metric.DisplayUnit_Bytes,
	}
	metaRdbReadAmplification = metric.Metadata{
		Name:        "rocksdb.read-amplification",
		Help:        "Number of disk reads per query",
		Unit:        "Disk Reads per Query",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRdbNumSSTables = metric.Metadata{
		Name:        "rocksdb.num-sstables",
		Help:        "Number of rocksdb SSTables",
		Unit:        "SSTables",
		DisplayUnit: metric.DisplayUnit_Count,
	}

	// Range event metrics.
	metaRangeSplits = metric.Metadata{
		Name:        "range.splits",
		Help:        "Number of range splits",
		Unit:        "Range Ops",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRangeAdds = metric.Metadata{
		Name:        "range.adds",
		Help:        "Number of range additions",
		Unit:        "Range Ops",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRangeRemoves = metric.Metadata{
		Name:        "range.removes",
		Help:        "Number of range removals",
		Unit:        "Range Ops",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRangeSnapshotsGenerated = metric.Metadata{
		Name:        "range.snapshots.generated",
		Help:        "Number of generated snapshots",
		Unit:        "Snapshots",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRangeSnapshotsNormalApplied = metric.Metadata{
		Name:        "range.snapshots.normal-applied",
		Help:        "Number of applied snapshots",
		Unit:        "Snapshots",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRangeSnapshotsPreemptiveApplied = metric.Metadata{
		Name:        "range.snapshots.preemptive-applied",
		Help:        "Number of applied pre-emptive snapshots",
		Unit:        "Snapshots",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRangeRaftLeaderTransfers = metric.Metadata{
		Name:        "range.raftleadertransfers",
		Help:        "Number of raft leader transfers",
		Unit:        "Leader Transfers",
		DisplayUnit: metric.DisplayUnit_Count,
	}

	// Raft processing metrics.
	metaRaftTicks = metric.Metadata{
		Name:        "raft.ticks",
		Help:        "Number of Raft ticks queued",
		Unit:        "Ticks",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftWorkingDurationNanos = metric.Metadata{
		Name:        "raft.process.workingnanos",
		Help:        "Nanoseconds spent in store.processRaft() working",
		Unit:        "Processing Time",
		DisplayUnit: metric.DisplayUnit_Nanoseconds,
	}
	metaRaftTickingDurationNanos = metric.Metadata{
		Name:        "raft.process.tickingnanos",
		Help:        "Nanoseconds spent in store.processRaft() processing replica.Tick()",
		Unit:        "Processing Time",
		DisplayUnit: metric.DisplayUnit_Nanoseconds,
	}
	metaRaftCommandsApplied = metric.Metadata{
		Name:        "raft.commandsapplied",
		Help:        "Count of Raft commands applied",
		Unit:        "Commands",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftLogCommitLatency = metric.Metadata{
		Name:        "raft.process.logcommit.latency",
		Help:        "Latency histogram in nanoseconds for committing Raft log entries",
		Unit:        "Latency",
		DisplayUnit: metric.DisplayUnit_Nanoseconds,
	}
	metaRaftCommandCommitLatency = metric.Metadata{
		Name:        "raft.process.commandcommit.latency",
		Help:        "Latency histogram in nanoseconds for committing Raft commands",
		Unit:        "Latency",
		DisplayUnit: metric.DisplayUnit_Nanoseconds,
	}

	// Raft message metrics.
	metaRaftRcvdProp = metric.Metadata{
		Name:        "raft.rcvd.prop",
		Help:        "Number of MsgProp messages received by this store",
		Unit:        "Messages",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftRcvdApp = metric.Metadata{
		Name:        "raft.rcvd.app",
		Help:        "Number of MsgApp messages received by this store",
		Unit:        "Messages",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftRcvdAppResp = metric.Metadata{
		Name:        "raft.rcvd.appresp",
		Help:        "Number of MsgAppResp messages received by this store",
		Unit:        "Messages",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftRcvdVote = metric.Metadata{
		Name:        "raft.rcvd.vote",
		Help:        "Number of MsgVote messages received by this store",
		Unit:        "Messages",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftRcvdVoteResp = metric.Metadata{
		Name:        "raft.rcvd.voteresp",
		Help:        "Number of MsgVoteResp messages received by this store",
		Unit:        "Messages",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftRcvdPreVote = metric.Metadata{
		Name:        "raft.rcvd.prevote",
		Help:        "Number of MsgPreVote messages received by this store",
		Unit:        "Messages",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftRcvdPreVoteResp = metric.Metadata{
		Name:        "raft.rcvd.prevoteresp",
		Help:        "Number of MsgPreVoteResp messages received by this store",
		Unit:        "Messages",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftRcvdSnap = metric.Metadata{
		Name:        "raft.rcvd.snap",
		Help:        "Number of MsgSnap messages received by this store",
		Unit:        "Messages",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftRcvdHeartbeat = metric.Metadata{
		Name:        "raft.rcvd.heartbeat",
		Help:        "Number of (coalesced, if enabled) MsgHeartbeat messages received by this store",
		Unit:        "Messages",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftRcvdHeartbeatResp = metric.Metadata{
		Name:        "raft.rcvd.heartbeatresp",
		Help:        "Number of (coalesced, if enabled) MsgHeartbeatResp messages received by this store",
		Unit:        "Messages",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftRcvdTransferLeader = metric.Metadata{
		Name:        "raft.rcvd.transferleader",
		Help:        "Number of MsgTransferLeader messages received by this store",
		Unit:        "Messages",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftRcvdTimeoutNow = metric.Metadata{
		Name:        "raft.rcvd.timeoutnow",
		Help:        "Number of MsgTimeoutNow messages received by this store",
		Unit:        "Messages",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftRcvdDropped = metric.Metadata{
		Name:        "raft.rcvd.dropped",
		Help:        "Number of dropped incoming Raft messages",
		Unit:        "Messages",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftEnqueuedPending = metric.Metadata{
		Name:        "raft.enqueued.pending",
		Help:        "Number of pending outgoing messages in the Raft Transport queue",
		Unit:        "Messages",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftCoalescedHeartbeatsPending = metric.Metadata{
		Name:        "raft.heartbeats.pending",
		Help:        "Number of pending heartbeats and responses waiting to be coalesced",
		Unit:        "Messages",
		DisplayUnit: metric.DisplayUnit_Count,
	}

	// Raft log metrics.
	metaRaftLogFollowerBehindCount = metric.Metadata{
		Name:        "raftlog.behind",
		Help:        "Number of Raft log entries followers on other stores are behind",
		Unit:        "Log Entries",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftLogTruncated = metric.Metadata{
		Name:        "raftlog.truncated",
		Help:        "Number of Raft log entries truncated",
		Unit:        "Log Entries",
		DisplayUnit: metric.DisplayUnit_Count,
	}

	// Replica queue metrics.
	metaGCQueueSuccesses = metric.Metadata{
		Name:        "queue.gc.process.success",
		Help:        "Number of replicas successfully processed by the GC queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaGCQueueFailures = metric.Metadata{
		Name:        "queue.gc.process.failure",
		Help:        "Number of replicas which failed processing in the GC queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaGCQueuePending = metric.Metadata{
		Name:        "queue.gc.pending",
		Help:        "Number of pending replicas in the GC queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaGCQueueProcessingNanos = metric.Metadata{
		Name:        "queue.gc.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the GC queue",
		Unit:        "Processing Time",
		DisplayUnit: metric.DisplayUnit_Nanoseconds,
	}
	metaRaftLogQueueSuccesses = metric.Metadata{
		Name:        "queue.raftlog.process.success",
		Help:        "Number of replicas successfully processed by the Raft log queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftLogQueueFailures = metric.Metadata{
		Name:        "queue.raftlog.process.failure",
		Help:        "Number of replicas which failed processing in the Raft log queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftLogQueuePending = metric.Metadata{
		Name:        "queue.raftlog.pending",
		Help:        "Number of pending replicas in the Raft log queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftLogQueueProcessingNanos = metric.Metadata{
		Name:        "queue.raftlog.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the Raft log queue",
		Unit:        "Processing Time",
		DisplayUnit: metric.DisplayUnit_Nanoseconds,
	}
	metaRaftSnapshotQueueSuccesses = metric.Metadata{
		Name:        "queue.raftsnapshot.process.success",
		Help:        "Number of replicas successfully processed by the Raft repair queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftSnapshotQueueFailures = metric.Metadata{
		Name:        "queue.raftsnapshot.process.failure",
		Help:        "Number of replicas which failed processing in the Raft repair queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftSnapshotQueuePending = metric.Metadata{
		Name:        "queue.raftsnapshot.pending",
		Help:        "Number of pending replicas in the Raft repair queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaRaftSnapshotQueueProcessingNanos = metric.Metadata{
		Name:        "queue.raftsnapshot.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the Raft repair queue",
		Unit:        "Processing Time",
		DisplayUnit: metric.DisplayUnit_Nanoseconds,
	}
	metaConsistencyQueueSuccesses = metric.Metadata{
		Name:        "queue.consistency.process.success",
		Help:        "Number of replicas successfully processed by the consistency checker queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaConsistencyQueueFailures = metric.Metadata{
		Name:        "queue.consistency.process.failure",
		Help:        "Number of replicas which failed processing in the consistency checker queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaConsistencyQueuePending = metric.Metadata{
		Name:        "queue.consistency.pending",
		Help:        "Number of pending replicas in the consistency checker queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaConsistencyQueueProcessingNanos = metric.Metadata{
		Name:        "queue.consistency.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the consistency checker queue",
		Unit:        "Processing Time",
		DisplayUnit: metric.DisplayUnit_Nanoseconds,
	}
	metaReplicaGCQueueSuccesses = metric.Metadata{
		Name:        "queue.replicagc.process.success",
		Help:        "Number of replicas successfully processed by the replica GC queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaReplicaGCQueueFailures = metric.Metadata{
		Name:        "queue.replicagc.process.failure",
		Help:        "Number of replicas which failed processing in the replica GC queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaReplicaGCQueuePending = metric.Metadata{
		Name:        "queue.replicagc.pending",
		Help:        "Number of pending replicas in the replica GC queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaReplicaGCQueueProcessingNanos = metric.Metadata{
		Name:        "queue.replicagc.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the replica GC queue",
		Unit:        "Processing Time",
		DisplayUnit: metric.DisplayUnit_Nanoseconds,
	}
	metaReplicateQueueSuccesses = metric.Metadata{
		Name:        "queue.replicate.process.success",
		Help:        "Number of replicas successfully processed by the replicate queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaReplicateQueueFailures = metric.Metadata{
		Name:        "queue.replicate.process.failure",
		Help:        "Number of replicas which failed processing in the replicate queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaReplicateQueuePending = metric.Metadata{
		Name:        "queue.replicate.pending",
		Help:        "Number of pending replicas in the replicate queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaReplicateQueueProcessingNanos = metric.Metadata{
		Name:        "queue.replicate.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the replicate queue",
		Unit:        "Processing Time",
		DisplayUnit: metric.DisplayUnit_Nanoseconds,
	}
	metaReplicateQueuePurgatory = metric.Metadata{
		Name:        "queue.replicate.purgatory",
		Help:        "Number of replicas in the replicate queue's purgatory, awaiting allocation options",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaSplitQueueSuccesses = metric.Metadata{
		Name:        "queue.split.process.success",
		Help:        "Number of replicas successfully processed by the split queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaSplitQueueFailures = metric.Metadata{
		Name:        "queue.split.process.failure",
		Help:        "Number of replicas which failed processing in the split queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaSplitQueuePending = metric.Metadata{
		Name:        "queue.split.pending",
		Help:        "Number of pending replicas in the split queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaSplitQueueProcessingNanos = metric.Metadata{
		Name:        "queue.split.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the split queue",
		Unit:        "CPU Time",
		DisplayUnit: metric.DisplayUnit_Nanoseconds,
	}
	metaSplitQueuePurgatory = metric.Metadata{
		Name:        "queue.split.purgatory",
		Help:        "Number of replicas in the split queue's purgatory, waiting to become splittable",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaTimeSeriesMaintenanceQueueSuccesses = metric.Metadata{
		Name:        "queue.tsmaintenance.process.success",
		Help:        "Number of replicas successfully processed by the time series maintenance queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaTimeSeriesMaintenanceQueueFailures = metric.Metadata{
		Name:        "queue.tsmaintenance.process.failure",
		Help:        "Number of replicas which failed processing in the time series maintenance queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaTimeSeriesMaintenanceQueuePending = metric.Metadata{
		Name:        "queue.tsmaintenance.pending",
		Help:        "Number of pending replicas in the time series maintenance queue",
		Unit:        "Replicas",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaTimeSeriesMaintenanceQueueProcessingNanos = metric.Metadata{
		Name:        "queue.tsmaintenance.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the time series maintenance queue",
		Unit:        "Processing Time",
		DisplayUnit: metric.DisplayUnit_Nanoseconds,
	}

	// GCInfo cumulative totals.
	metaGCNumKeysAffected = metric.Metadata{
		Name:        "queue.gc.info.numkeysaffected",
		Help:        "Number of keys with GC'able data",
		Unit:        "Keys",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaGCIntentsConsidered = metric.Metadata{
		Name:        "queue.gc.info.intentsconsidered",
		Help:        "Number of 'old' intents",
		Unit:        "Intents",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaGCIntentTxns = metric.Metadata{
		Name:        "queue.gc.info.intenttxns",
		Help:        "Number of associated distinct transactions",
		Unit:        "Txns",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaGCTransactionSpanScanned = metric.Metadata{
		Name:        "queue.gc.info.transactionspanscanned",
		Help:        "Number of entries in transaction spans scanned from the engine",
		Unit:        "Txn Entries",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaGCTransactionSpanGCAborted = metric.Metadata{
		Name:        "queue.gc.info.transactionspangcaborted",
		Help:        "Number of GC'able entries corresponding to aborted txns",
		Unit:        "Txn Entries",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaGCTransactionSpanGCCommitted = metric.Metadata{
		Name:        "queue.gc.info.transactionspangccommitted",
		Help:        "Number of GC'able entries corresponding to committed txns",
		Unit:        "Txn Entries",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaGCTransactionSpanGCPending = metric.Metadata{
		Name:        "queue.gc.info.transactionspangcpending",
		Help:        "Number of GC'able entries corresponding to pending txns",
		Unit:        "Txn Entries",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaGCAbortSpanScanned = metric.Metadata{
		Name:        "queue.gc.info.abortspanscanned",
		Help:        "Number of transactions present in the AbortSpan scanned from the engine",
		Unit:        "Txn Entries",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaGCAbortSpanConsidered = metric.Metadata{
		Name:        "queue.gc.info.abortspanconsidered",
		Help:        "Number of AbortSpan entries old enough to be considered for removal",
		Unit:        "Txn Entries",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaGCAbortSpanGCNum = metric.Metadata{
		Name:        "queue.gc.info.abortspangcnum",
		Help:        "Number of AbortSpan entries fit for removal",
		Unit:        "Txn Entries",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaGCPushTxn = metric.Metadata{
		Name:        "queue.gc.info.pushtxn",
		Help:        "Number of attempted pushes",
		Unit:        "Pushes",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaGCResolveTotal = metric.Metadata{
		Name:        "queue.gc.info.resolvetotal",
		Help:        "Number of attempted intent resolutions",
		Unit:        "Intent Resolutions",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaGCResolveSuccess = metric.Metadata{
		Name:        "queue.gc.info.resolvesuccess",
		Help:        "Number of successful intent resolutions",
		Unit:        "Intent Resolutions",
		DisplayUnit: metric.DisplayUnit_Count,
	}

	// Slow request metrics.
	metaSlowCommandQueueRequests = metric.Metadata{
		Name:        "requests.slow.commandqueue",
		Help:        "Number of requests that have been stuck for a long time in the command queue",
		Unit:        "Requests",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaSlowLeaseRequests = metric.Metadata{
		Name:        "requests.slow.lease",
		Help:        "Number of requests that have been stuck for a long time acquiring a lease",
		Unit:        "Requests",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaSlowRaftRequests = metric.Metadata{
		Name:        "requests.slow.raft",
		Help:        "Number of requests that have been stuck for a long time in raft",
		Unit:        "Requests",
		DisplayUnit: metric.DisplayUnit_Count,
	}

	// Backpressure metrics.
	metaBackpressuredOnSplitRequests = metric.Metadata{
		Name:        "requests.backpressure.split",
		Help:        "Number of backpressured writes waiting on a Range split",
		Unit:        "Writes",
		DisplayUnit: metric.DisplayUnit_Count,
	}

	// AddSSTable metrics.
	metaAddSSTableProposals = metric.Metadata{
		Name:        "addsstable.proposals",
		Help:        "Number of SSTable ingestions proposed (i.e. sent to Raft by lease holders)",
		Unit:        "Ingestions",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaAddSSTableApplications = metric.Metadata{
		Name:        "addsstable.applications",
		Help:        "Number of SSTable ingestions applied (i.e. applied by Replicas)",
		Unit:        "Ingestions",
		DisplayUnit: metric.DisplayUnit_Count,
	}
	metaAddSSTableApplicationCopies = metric.Metadata{
		Name:        "addsstable.copies",
		Help:        "number of SSTable ingestions that required copying files during application",
		Unit:        "Ingestions",
		DisplayUnit: metric.DisplayUnit_Count,
	}
)

// StoreMetrics is the set of metrics for a given store.
type StoreMetrics struct {
	registry *metric.Registry

	// Replica metrics.
	ReplicaCount                  *metric.Gauge // Does not include reserved replicas.
	ReservedReplicaCount          *metric.Gauge
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
	TotalBytes      *metric.Gauge
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
	Used            *metric.Gauge
	Reserved        *metric.Gauge
	SysBytes        *metric.Gauge
	SysCount        *metric.Gauge

	// Rebalancing metrics.
	AverageWritesPerSecond *metric.GaugeFloat64

	// RocksDB metrics.
	RdbBlockCacheHits           *metric.Gauge
	RdbBlockCacheMisses         *metric.Gauge
	RdbBlockCacheUsage          *metric.Gauge
	RdbBlockCachePinnedUsage    *metric.Gauge
	RdbBloomFilterPrefixChecked *metric.Gauge
	RdbBloomFilterPrefixUseful  *metric.Gauge
	RdbMemtableTotalSize        *metric.Gauge
	RdbFlushes                  *metric.Gauge
	RdbCompactions              *metric.Gauge
	RdbTableReadersMemEstimate  *metric.Gauge
	RdbReadAmplification        *metric.Gauge
	RdbNumSSTables              *metric.Gauge

	// TODO(mrtracy): This should be removed as part of #4465. This is only
	// maintained to keep the current structure of NodeStatus; it would be
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
	RaftLogCommitLatency     *metric.Histogram
	RaftCommandCommitLatency *metric.Histogram

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

	// Backpressure counts.
	BackpressuredOnSplitRequests *metric.Gauge

	// AddSSTable stats: how many AddSSTable commands were proposed and how many
	// were applied? How many applications required writing a copy?
	AddSSTableProposals         *metric.Counter
	AddSSTableApplications      *metric.Counter
	AddSSTableApplicationCopies *metric.Counter

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
		ReplicaCount:                  metric.NewGauge(metaReplicaCount),
		ReservedReplicaCount:          metric.NewGauge(metaReservedReplicaCount),
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
		TotalBytes:      metric.NewGauge(metaTotalBytes),
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
		Used:            metric.NewGauge(metaUsed),
		Reserved:        metric.NewGauge(metaReserved),
		SysBytes:        metric.NewGauge(metaSysBytes),
		SysCount:        metric.NewGauge(metaSysCount),

		// Rebalancing metrics.
		AverageWritesPerSecond: metric.NewGaugeFloat64(metaAverageWritesPerSecond),

		// RocksDB metrics.
		RdbBlockCacheHits:           metric.NewGauge(metaRdbBlockCacheHits),
		RdbBlockCacheMisses:         metric.NewGauge(metaRdbBlockCacheMisses),
		RdbBlockCacheUsage:          metric.NewGauge(metaRdbBlockCacheUsage),
		RdbBlockCachePinnedUsage:    metric.NewGauge(metaRdbBlockCachePinnedUsage),
		RdbBloomFilterPrefixChecked: metric.NewGauge(metaRdbBloomFilterPrefixChecked),
		RdbBloomFilterPrefixUseful:  metric.NewGauge(metaRdbBloomFilterPrefixUseful),
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
		RaftLogCommitLatency:     metric.NewLatency(metaRaftLogCommitLatency, histogramWindow),
		RaftCommandCommitLatency: metric.NewLatency(metaRaftCommandCommitLatency, histogramWindow),

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

		// Backpressure counters.
		BackpressuredOnSplitRequests: metric.NewGauge(metaBackpressuredOnSplitRequests),

		// AddSSTable proposal + applications counters.
		AddSSTableProposals:         metric.NewCounter(metaAddSSTableProposals),
		AddSSTableApplications:      metric.NewCounter(metaAddSSTableApplications),
		AddSSTableApplicationCopies: metric.NewCounter(metaAddSSTableApplicationCopies),
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
	sm.TotalBytes.Update(sm.mu.stats.Total())
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
