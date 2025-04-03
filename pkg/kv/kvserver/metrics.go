// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/split"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/disk"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/debugutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
)

func init() {
	// Inject the TenantStorageMetricsSet available in the kvbase pkg to
	// avoid import cycles.
	kvbase.TenantsStorageMetricsSet = tenantsStorageMetricsSet()
}

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
	metaRaftLeaderNotFortifiedCount = metric.Metadata{
		Name:        "replicas.leaders_not_fortified",
		Help:        "Number of replicas that are not fortified Raft leaders",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftLeaderInvalidLeaseCount = metric.Metadata{
		Name:        "replicas.leaders_invalid_lease",
		Help:        "Number of replicas that are Raft leaders whose lease is invalid",
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
	metaAsleepCount = metric.Metadata{
		Name:        "replicas.asleep",
		Help:        "Number of asleep replicas. Similarly to quiesced replicas, asleep replicas do not tick in Raft.",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaUninitializedCount = metric.Metadata{
		Name:        "replicas.uninitialized",
		Help:        "Number of uninitialized replicas, this does not include uninitialized replicas that can lie dormant in a persistent state.",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}

	metaRaftFlowsReplicate = metric.Metadata{
		Name:        "raft.flows.state_replicate",
		Help:        "Number of leader->peer flows in StateReplicate",
		Measurement: "Flows",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftFlowsProbe = metric.Metadata{
		Name:        "raft.flows.state_probe",
		Help:        "Number of leader->peer flows in StateProbe",
		Measurement: "Flows",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftFlowsSnapshot = metric.Metadata{
		Name:        "raft.flows.state_snapshot",
		Help:        "Number of leader->peer flows in StateSnapshot",
		Measurement: "Flows",
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
	metaDecommissioningRangeCount = metric.Metadata{
		Name:        "ranges.decommissioning",
		Help:        "Number of ranges with at lease one replica on a decommissioning node",
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
	metaLeaseRequestLatency = metric.Metadata{
		Name:        "leases.requests.latency",
		Help:        "Lease request latency (all types and outcomes, coalesced)",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
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
	metaLeaseTransferLocksWrittenCount = metric.Metadata{
		Name:        "leases.transfers.locks_written",
		Help:        "Number of locks written to storage during lease transfers",
		Measurement: "Locks Written",
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
	metaLeaseLeaderCount = metric.Metadata{
		Name:        "leases.leader",
		Help:        "Number of replica leaseholders using leader leases",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaLeaseLivenessCount = metric.Metadata{
		Name:        "leases.liveness",
		Help:        "Number of replica leaseholders for the liveness range(s)",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaLeaseViolatingPreferencesCount = metric.Metadata{
		Name:        "leases.preferences.violating",
		Help:        "Number of replica leaseholders which violate lease preferences",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaLeaseLessPreferredCount = metric.Metadata{
		Name: "leases.preferences.less-preferred",
		Help: "Number of replica leaseholders which satisfy a lease " +
			"preference which is not the most preferred",
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
	metaRangeKeyBytes = metric.Metadata{
		Name:        "rangekeybytes",
		Help:        "Number of bytes taken up by range keys (e.g. MVCC range tombstones)",
		Measurement: "Storage",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeValBytes = metric.Metadata{
		Name:        "rangevalbytes",
		Help:        "Number of bytes taken up by range key values (e.g. MVCC range tombstones)",
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
	metaLockBytes = metric.Metadata{
		Name:        "lockbytes",
		Help:        "Number of bytes taken up by replicated lock key-values (shared and exclusive strength, not intent strength)",
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
	metaRangeKeyCount = metric.Metadata{
		Name:        "rangekeycount",
		Help:        "Count of all range keys (e.g. MVCC range tombstones)",
		Measurement: "Keys",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeValCount = metric.Metadata{
		Name:        "rangevalcount",
		Help:        "Count of all range key values (e.g. MVCC range tombstones)",
		Measurement: "MVCC Values",
		Unit:        metric.Unit_COUNT,
	}
	metaIntentCount = metric.Metadata{
		Name:        "intentcount",
		Help:        "Count of intent keys",
		Measurement: "Keys",
		Unit:        metric.Unit_COUNT,
	}
	metaLockCount = metric.Metadata{
		Name:        "lockcount",
		Help:        "Count of replicated locks (shared, exclusive, and intent strength)",
		Measurement: "Locks",
		Unit:        metric.Unit_COUNT,
	}
	// TODO(nvanbenschoten): rename "intentage" metric to "lockage".
	metaLockAge = metric.Metadata{
		Name:        "intentage",
		Help:        "Cumulative age of locks",
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
		Help:        "Number of kv-level requests received per second by the store, considering the last 30 minutes, as used in rebalancing decisions.",
		Measurement: "Queries/Sec",
		Unit:        metric.Unit_COUNT,
	}
	metaAverageWritesPerSecond = metric.Metadata{
		Name:        "rebalancing.writespersecond",
		Help:        "Number of keys written (i.e. applied by raft) per second to the store, considering the last 30 minutes.",
		Measurement: "Keys/Sec",
		Unit:        metric.Unit_COUNT,
	}
	metaAverageRequestsPerSecond = metric.Metadata{
		Name:        "rebalancing.requestspersecond",
		Help:        "Number of requests received recently per second, considering the last 30 minutes.",
		Measurement: "Requests/Sec",
		Unit:        metric.Unit_COUNT,
	}
	metaAverageReadsPerSecond = metric.Metadata{
		Name:        "rebalancing.readspersecond",
		Help:        "Number of keys read recently per second, considering the last 30 minutes.",
		Measurement: "Keys/Sec",
		Unit:        metric.Unit_COUNT,
	}
	metaAverageWriteBytesPerSecond = metric.Metadata{
		Name:        "rebalancing.writebytespersecond",
		Help:        "Number of bytes written recently per second, considering the last 30 minutes.",
		Measurement: "Bytes/Sec",
		Unit:        metric.Unit_BYTES,
	}
	metaAverageReadBytesPerSecond = metric.Metadata{
		Name:        "rebalancing.readbytespersecond",
		Help:        "Number of bytes read recently per second, considering the last 30 minutes.",
		Measurement: "Bytes/Sec",
		Unit:        metric.Unit_BYTES,
	}
	metaAverageCPUNanosPerSecond = metric.Metadata{
		Name:        "rebalancing.cpunanospersecond",
		Help:        "Average CPU nanoseconds spent on processing replica operations in the last 30 minutes.",
		Measurement: "Nanoseconds/Sec",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRecentReplicaCPUNanosPerSecond = metric.Metadata{
		Name: "rebalancing.replicas.cpunanospersecond",
		Help: "Histogram of average CPU nanoseconds spent on processing " +
			"replica operations in the last 30 minutes.",
		Measurement: "Nanoseconds/Sec",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRecentReplicaQueriesPerSecond = metric.Metadata{
		Name: "rebalancing.replicas.queriespersecond",
		Help: "Histogram of average kv-level requests received per second by " +
			"replicas on the store in the last 30 minutes.",
		Measurement: "Queries/Sec",
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
	metaWriteEvaluationServerSideRetrySuccess = metric.Metadata{
		Name:        "txn.server_side_retry.write_evaluation.success",
		Help:        "Number of write batches that were successfully refreshed server side",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaWriteEvaluationServerSideRetryFailure = metric.Metadata{
		Name:        "txn.server_side_retry.write_evaluation.failure",
		Help:        "Number of write batches that were not successfully refreshed server side",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaReadEvaluationServerSideRetrySuccess = metric.Metadata{
		Name:        "txn.server_side_retry.read_evaluation.success",
		Help:        "Number of read batches that were successfully refreshed server side",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaReadEvaluationServerSideRetryFailure = metric.Metadata{
		Name:        "txn.server_side_retry.read_evaluation.failure",
		Help:        "Number of read batches that were not successfully refreshed server side",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaReadWithinUncertaintyIntervalErrorServerSideRetrySuccess = metric.Metadata{
		Name: "txn.server_side_retry.uncertainty_interval_error.success",
		Help: "Number of batches that ran into uncertainty interval errors that were " +
			"successfully refreshed server side",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaReadWithinUncertaintyIntervalErrorServerSideRetryFailure = metric.Metadata{
		Name: "txn.server_side_retry.uncertainty_interval_error.failure",
		Help: "Number of batches that ran into uncertainty interval errors that were not " +
			"successfully refreshed server side",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaOnePhaseCommitSuccess = metric.Metadata{
		Name:        "txn.server_side.1PC.success",
		Help:        "Number of batches that attempted to commit using 1PC and succeeded",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaOnePhaseCommitFailure = metric.Metadata{
		Name:        "txn.server_side.1PC.failure",
		Help:        "Number of batches that attempted to commit using 1PC and failed",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}

	//Ingest metrics
	metaIngestCount = metric.Metadata{
		Name:        "storage.ingest.count",
		Help:        "Number of successful ingestions performed",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}

	// Pebble metrics.
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
		Help:        "Number of storage engine SSTables",
		Measurement: "SSTables",
		Unit:        metric.Unit_COUNT,
	}
	metaRdbPendingCompaction = metric.Metadata{
		Name:        "rocksdb.estimated-pending-compaction",
		Help:        "Estimated pending compaction bytes",
		Measurement: "Storage",
		Unit:        metric.Unit_BYTES,
	}
	metaRdbMarkedForCompactionFiles = metric.Metadata{
		Name:        "storage.marked-for-compaction-files",
		Help:        "Count of SSTables marked for compaction",
		Measurement: "SSTables",
		Unit:        metric.Unit_COUNT,
	}
	metaRdbKeysRangeKeySets = metric.Metadata{
		Name:        "storage.keys.range-key-set.count",
		Help:        "Approximate count of RangeKeySet internal keys across the storage engine.",
		Measurement: "Keys",
		Unit:        metric.Unit_COUNT,
	}
	metaRdbKeysTombstones = metric.Metadata{
		Name:        "storage.keys.tombstone.count",
		Help:        "Approximate count of DEL, SINGLEDEL and RANGEDEL internal keys across the storage engine.",
		Measurement: "Keys",
		Unit:        metric.Unit_COUNT,
	}
	// NB: bytes only ever get flushed into L0, so this metric does not
	// exist for any other level.
	metaRdbL0BytesFlushed = storageLevelMetricMetadata(
		"bytes-flushed",
		"Number of bytes flushed (from memtables) into Level %d",
		"Bytes",
		metric.Unit_BYTES,
	)[0]

	// NB: sublevels is trivial (zero or one) except on L0.
	metaRdbL0Sublevels = storageLevelMetricMetadata(
		"sublevels",
		"Number of Level %d sublevels",
		"Sublevels",
		metric.Unit_COUNT,
	)[0]

	// NB: we only expose the file count in L0 because it matters for
	// admission control. The other file counts are less interesting.
	metaRdbL0NumFiles = storageLevelMetricMetadata(
		"num-files",
		"Number of SSTables in Level %d",
		"SSTables",
		metric.Unit_COUNT,
	)[0]

	metaRdbBytesIngested = storageLevelMetricMetadata(
		"bytes-ingested",
		"Number of bytes ingested directly into Level %d",
		"Bytes",
		metric.Unit_BYTES,
	)

	metaRdbLevelSize = storageLevelMetricMetadata(
		"level-size",
		"Size of the SSTables in level %d",
		"Bytes",
		metric.Unit_BYTES,
	)

	metaRdbLevelScores = storageLevelMetricMetadata(
		"level-score",
		"Compaction score of level %d",
		"Score",
		metric.Unit_COUNT,
	)

	metaRdbWriteStalls = metric.Metadata{
		Name:        "storage.write-stalls",
		Help:        "Number of instances of intentional write stalls to backpressure incoming writes",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaRdbWriteStallNanos = metric.Metadata{
		Name:        "storage.write-stall-nanos",
		Help:        "Total write stall duration in nanos",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	metaRdbCheckpoints = metric.Metadata{
		Name: "storage.checkpoints",
		Help: `The number of checkpoint directories found in storage.

This is the number of directories found in the auxiliary/checkpoints directory.
Each represents an immutable point-in-time storage engine checkpoint. They are
cheap (consisting mostly of hard links), but over time they effectively become a
full copy of the old state, which increases their relative cost. Checkpoints
must be deleted once acted upon (e.g. copied elsewhere or investigated).

A likely cause of having a checkpoint is that one of the ranges in this store
had inconsistent data among its replicas. Such checkpoint directories are
located in auxiliary/checkpoints/rN_at_M, where N is the range ID, and M is the
Raft applied index at which this checkpoint was taken.`,

		Measurement: "Directories",
		Unit:        metric.Unit_COUNT,
	}

	metaBlockBytes = metric.Metadata{
		Name:        "storage.iterator.block-load.bytes",
		Help:        "Bytes loaded by storage engine iterators (possibly cached). See storage.AggregatedIteratorStats for details.",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaBlockBytesInCache = metric.Metadata{
		Name:        "storage.iterator.block-load.cached-bytes",
		Help:        "Bytes loaded by storage engine iterators from the block cache. See storage.AggregatedIteratorStats for details.",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaBlockReadDuration = metric.Metadata{
		Name:        "storage.iterator.block-load.read-duration",
		Help:        "Cumulative time storage engine iterators spent loading blocks from durable storage. See storage.AggregatedIteratorStats for details.",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaIterExternalSeeks = metric.Metadata{
		Name:        "storage.iterator.external.seeks",
		Help:        "Cumulative count of seeks performed on storage engine iterators. See storage.AggregatedIteratorStats for details.",
		Measurement: "Iterator Ops",
		Unit:        metric.Unit_COUNT,
	}
	metaIterExternalSteps = metric.Metadata{
		Name:        "storage.iterator.external.steps",
		Help:        "Cumulative count of steps performed on storage engine iterators. See storage.AggregatedIteratorStats for details.",
		Measurement: "Iterator Ops",
		Unit:        metric.Unit_COUNT,
	}
	metaIterInternalSeeks = metric.Metadata{
		Name: "storage.iterator.internal.seeks",
		Help: `Cumulative count of seeks performed internally within storage engine iterators.

A value high relative to 'storage.iterator.external.seeks'
is a good indication that there's an accumulation of garbage
internally within the storage engine.

See storage.AggregatedIteratorStats for details.`,
		Measurement: "Iterator Ops",
		Unit:        metric.Unit_COUNT,
	}
	metaIterInternalSteps = metric.Metadata{
		Name: "storage.iterator.internal.steps",
		Help: `Cumulative count of steps performed internally within storage engine iterators.

A value high relative to 'storage.iterator.external.steps'
is a good indication that there's an accumulation of garbage
internally within the storage engine.

See storage.AggregatedIteratorStats for more details.`,
		Measurement: "Iterator Ops",
		Unit:        metric.Unit_COUNT,
	}
	metaStorageCompactionsDuration = metric.Metadata{
		Name: "storage.compactions.duration",
		Help: `Cumulative sum of all compaction durations.

The rate of this value provides the effective compaction concurrency of a store,
which can be useful to determine whether the maximum compaction concurrency is
fully utilized.`,
		Measurement: "Processing Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaStorageWriteAmplification = metric.Metadata{
		Name: "storage.write-amplification",
		Help: `Running measure of write-amplification.

Write amplification is measured as the ratio of bytes written to disk relative to the logical
bytes present in sstables, over the life of a store. This metric is a running average
of the write amplification as tracked by Pebble.`,
		Measurement: "Ratio of bytes written to logical bytes",
		Unit:        metric.Unit_COUNT,
	}
	metaStorageCompactionsKeysPinnedCount = metric.Metadata{
		Name: "storage.compactions.keys.pinned.count",
		Help: `Cumulative count of storage engine KVs written to sstables during flushes and compactions due to open LSM snapshots.

Various subsystems of CockroachDB take LSM snapshots to maintain a consistent view
of the database over an extended duration. In order to maintain the consistent view,
flushes and compactions within the storage engine must preserve keys that otherwise
would have been dropped. This increases write amplification, and introduces keys
that must be skipped during iteration. This metric records the cumulative count of
KVs preserved during flushes and compactions over the lifetime of the process.
`,
		Measurement: "Keys",
		Unit:        metric.Unit_COUNT,
	}
	metaStorageCompactionsKeysPinnedBytes = metric.Metadata{
		Name: "storage.compactions.keys.pinned.bytes",
		Help: `Cumulative size of storage engine KVs written to sstables during flushes and compactions due to open LSM snapshots.

Various subsystems of CockroachDB take LSM snapshots to maintain a consistent view
of the database over an extended duration. In order to maintain the consistent view,
flushes and compactions within the storage engine must preserve keys that otherwise
would have been dropped. This increases write amplification, and introduces keys
that must be skipped during iteration. This metric records the cumulative number of
bytes preserved during flushes and compactions over the lifetime of the process.
`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaStorageCompactionsCancelledCount = metric.Metadata{
		Name:        "storage.compactions.cancelled.count",
		Help:        `Cumulative count of compactions that were cancelled before they completed due to a conflicting operation.`,
		Measurement: "Compactions",
		Unit:        metric.Unit_COUNT,
	}
	metaStorageCompactionsCancelledBytes = metric.Metadata{
		Name:        "storage.compactions.cancelled.bytes",
		Help:        `Cumulative volume of data written to sstables during compactions that were ultimately cancelled due to a conflicting operation.`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	// TODO(sumeer): remove, since can fire due to delete-only compactions.
	metaStorageSingleDelInvariantViolationCount = metric.Metadata{
		Name:        "storage.single-delete.invariant-violation",
		Help:        "Number of SingleDelete invariant violations",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	// TODO(sumeer): remove, since can fire due to delete-only compactions.
	metaStorageSingleDelIneffectualCount = metric.Metadata{
		Name:        "storage.single-delete.ineffectual",
		Help:        "Number of SingleDeletes that were ineffectual",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaSharedStorageBytesWritten = metric.Metadata{
		Name:        "storage.shared-storage.write",
		Help:        "Bytes written to external storage",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaSharedStorageBytesRead = metric.Metadata{
		Name:        "storage.shared-storage.read",
		Help:        "Bytes read from shared storage",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaBlockLoadsInProgress = metric.Metadata{
		Name:        "storage.block-load.active",
		Help:        "The number of sstable block loads currently in progress",
		Measurement: "Block loads",
		Unit:        metric.Unit_COUNT,
	}
	metaBlockLoadsQueued = metric.Metadata{
		Name: "storage.block-load.queued",
		Help: "The cumulative number of SSTable block loads that were delayed because too many loads " +
			"were active (see also: `storage.block_load.node_max_active`)",
		Measurement: "Block loads",
		Unit:        metric.Unit_COUNT,
	}
	metaSecondaryCacheSize = metric.Metadata{
		Name:        "storage.secondary-cache.size",
		Help:        "The number of sstable bytes stored in the secondary cache",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaSecondaryCacheCount = metric.Metadata{
		Name:        "storage.secondary-cache.count",
		Help:        "The count of cache blocks in the secondary cache (not sstable blocks)",
		Measurement: "Cache items",
		Unit:        metric.Unit_COUNT,
	}
	metaSecondaryCacheTotalReads = metric.Metadata{
		Name:        "storage.secondary-cache.reads-total",
		Help:        "The number of reads from the secondary cache",
		Measurement: "Num reads",
		Unit:        metric.Unit_COUNT,
	}
	metaSecondaryCacheMultiShardReads = metric.Metadata{
		Name:        "storage.secondary-cache.reads-multi-shard",
		Help:        "The number of secondary cache reads that require reading data from 2+ shards",
		Measurement: "Num reads",
		Unit:        metric.Unit_COUNT,
	}
	metaSecondaryCacheMultiBlockReads = metric.Metadata{
		Name:        "storage.secondary-cache.reads-multi-block",
		Help:        "The number of secondary cache reads that require reading data from 2+ cache blocks",
		Measurement: "Num reads",
		Unit:        metric.Unit_COUNT,
	}
	metaSecondaryCacheReadsWithFullHit = metric.Metadata{
		Name:        "storage.secondary-cache.reads-full-hit",
		Help:        "The number of reads where all data returned was read from the secondary cache",
		Measurement: "Num reads",
		Unit:        metric.Unit_COUNT,
	}
	metaSecondaryCacheReadsWithPartialHit = metric.Metadata{
		Name:        "storage.secondary-cache.reads-partial-hit",
		Help:        "The number of reads where some data returned was read from the secondary cache",
		Measurement: "Num reads",
		Unit:        metric.Unit_COUNT,
	}
	metaSecondaryCacheReadsWithNoHit = metric.Metadata{
		Name:        "storage.secondary-cache.reads-no-hit",
		Help:        "The number of reads where no data returned was read from the secondary cache",
		Measurement: "Num reads",
		Unit:        metric.Unit_COUNT,
	}
	metaSecondaryCacheEvictions = metric.Metadata{
		Name:        "storage.secondary-cache.evictions",
		Help:        "The number of times a cache block was evicted from the secondary cache",
		Measurement: "Num evictions",
		Unit:        metric.Unit_COUNT,
	}
	metaSecondaryCacheWriteBackFailures = metric.Metadata{
		Name:        "storage.secondary-cache.write-back-failures",
		Help:        "The number of times writing a cache block to the secondary cache failed",
		Measurement: "Num failures",
		Unit:        metric.Unit_COUNT,
	}
	metaFlushableIngestCount = metric.Metadata{
		Name:        "storage.flush.ingest.count",
		Help:        "Flushes performing an ingest (flushable ingestions)",
		Measurement: "Flushes",
		Unit:        metric.Unit_COUNT,
	}
	metaFlushableIngestTableCount = metric.Metadata{
		Name:        "storage.flush.ingest.table.count",
		Help:        "Tables ingested via flushes (flushable ingestions)",
		Measurement: "Tables",
		Unit:        metric.Unit_COUNT,
	}
	metaFlushableIngestTableBytes = metric.Metadata{
		Name:        "storage.flush.ingest.table.bytes",
		Help:        "Bytes ingested via flushes (flushable ingestions)",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaBatchCommitCount = metric.Metadata{
		Name:        "storage.batch-commit.count",
		Help:        "Count of batch commits. See storage.AggregatedBatchCommitStats for details.",
		Measurement: "Commit Ops",
		Unit:        metric.Unit_COUNT,
	}
	metaBatchCommitDuration = metric.Metadata{
		Name: "storage.batch-commit.duration",
		Help: "Cumulative time spent in batch commit. " +
			"See storage.AggregatedBatchCommitStats for details.",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaBatchCommitSemWaitDuration = metric.Metadata{
		Name: "storage.batch-commit.sem-wait.duration",
		Help: "Cumulative time spent in semaphore wait, for batch commit. " +
			"See storage.AggregatedBatchCommitStats for details.",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaBatchCommitWALQWaitDuration = metric.Metadata{
		Name: "storage.batch-commit.wal-queue-wait.duration",
		Help: "Cumulative time spent waiting for memory blocks in the WAL queue, for batch commit. " +
			"See storage.AggregatedBatchCommitStats for details.",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaBatchCommitMemStallDuration = metric.Metadata{
		Name: "storage.batch-commit.mem-stall.duration",
		Help: "Cumulative time spent in a write stall due to too many memtables, for batch commit. " +
			"See storage.AggregatedBatchCommitStats for details.",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaBatchCommitL0StallDuration = metric.Metadata{
		Name: "storage.batch-commit.l0-stall.duration",
		Help: "Cumulative time spent in a write stall due to high read amplification in L0, for batch commit. " +
			"See storage.AggregatedBatchCommitStats for details.",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaBatchCommitWALRotDuration = metric.Metadata{
		Name: "storage.batch-commit.wal-rotation.duration",
		Help: "Cumulative time spent waiting for WAL rotation, for batch commit. " +
			"See storage.AggregatedBatchCommitStats for details.",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaBatchCommitCommitWaitDuration = metric.Metadata{
		Name: "storage.batch-commit.commit-wait.duration",
		Help: "Cumulative time spent waiting for WAL sync, for batch commit. " +
			"See storage.AggregatedBatchCommitStats for details.",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaSSTableZombieBytes = metric.Metadata{
		Name: "storage.sstable.zombie.bytes",
		Help: "Bytes in SSTables that have been logically deleted, " +
			"but can't yet be physically deleted because an " +
			"open iterator may be reading them.",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaSSTableCompressionSnappy = metric.Metadata{
		Name: "storage.sstable.compression.snappy.count",
		Help: "Count of SSTables that have been compressed with the snappy " +
			"compression algorithm.",
		Measurement: "SSTables",
		Unit:        metric.Unit_COUNT,
	}
	metaSSTableCompressionZstd = metric.Metadata{
		Name: "storage.sstable.compression.zstd.count",
		Help: "Count of SSTables that have been compressed with the zstd " +
			"compression algorithm.",
		Measurement: "SSTables",
		Unit:        metric.Unit_COUNT,
	}
	metaSSTableCompressionUnknown = metric.Metadata{
		Name:        "storage.sstable.compression.unknown.count",
		Help:        "Count of SSTables that have an unknown compression algorithm.",
		Measurement: "SSTables",
		Unit:        metric.Unit_COUNT,
	}
	metaSSTableCompressionNone = metric.Metadata{
		Name:        "storage.sstable.compression.none.count",
		Help:        "Count of SSTables that are uncompressed.",
		Measurement: "SSTables",
		Unit:        metric.Unit_COUNT,
	}
)

var (
	// Disk health metrics.
	metaDiskSlow = metric.Metadata{
		Name:        "storage.disk-slow",
		Help:        "Number of instances of disk operations taking longer than 10s",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	// TODO(jackson): Consider removing the `storage.disk-stalled` metric.
	// Stalls fatal the node. It's unlikely this metric will ever be reported
	// greater than zero.
	metaDiskStalled = metric.Metadata{
		Name:        "storage.disk-stalled",
		Help:        "Number of instances of disk operations taking longer than 20s",
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
	metaRangeSnapshotRcvdBytes = metric.Metadata{
		Name:        "range.snapshots.rcvd-bytes",
		Help:        "Number of snapshot bytes received",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeSnapshotSentBytes = metric.Metadata{
		Name:        "range.snapshots.sent-bytes",
		Help:        "Number of snapshot bytes sent",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeSnapshotUnknownRcvdBytes = metric.Metadata{
		Name:        "range.snapshots.unknown.rcvd-bytes",
		Help:        "Number of unknown snapshot bytes received",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeSnapshotUnknownSentBytes = metric.Metadata{
		Name:        "range.snapshots.unknown.sent-bytes",
		Help:        "Number of unknown snapshot bytes sent",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeSnapshotRebalancingRcvdBytes = metric.Metadata{
		Name:        "range.snapshots.rebalancing.rcvd-bytes",
		Help:        "Number of rebalancing snapshot bytes received",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeSnapshotRebalancingSentBytes = metric.Metadata{
		Name:        "range.snapshots.rebalancing.sent-bytes",
		Help:        "Number of rebalancing snapshot bytes sent",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeSnapshotRecoveryRcvdBytes = metric.Metadata{
		Name:        "range.snapshots.recovery.rcvd-bytes",
		Help:        "Number of raft recovery snapshot bytes received",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeSnapshotRecoverySentBytes = metric.Metadata{
		Name:        "range.snapshots.recovery.sent-bytes",
		Help:        "Number of raft recovery snapshot bytes sent",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeSnapshotUpreplicationRcvdBytes = metric.Metadata{
		Name:        "range.snapshots.upreplication.rcvd-bytes",
		Help:        "Number of upreplication snapshot bytes received",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeSnapshotUpreplicationSentBytes = metric.Metadata{
		Name:        "range.snapshots.upreplication.sent-bytes",
		Help:        "Number of upreplication snapshot bytes sent",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeSnapshotRecvFailed = metric.Metadata{
		Name:        "range.snapshots.recv-failed",
		Help:        "Number of range snapshot initialization messages that errored out on the recipient, typically before any data is transferred",
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeSnapshotRecvUnusable = metric.Metadata{
		Name:        "range.snapshots.recv-unusable",
		Help:        "Number of range snapshot that were fully transmitted but determined to be unnecessary or unusable",
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeSnapShotCrossRegionSentBytes = metric.Metadata{
		Name:        "range.snapshots.cross-region.sent-bytes",
		Help:        "Number of snapshot bytes sent cross region",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeSnapShotCrossRegionRcvdBytes = metric.Metadata{
		Name:        "range.snapshots.cross-region.rcvd-bytes",
		Help:        "Number of snapshot bytes received cross region",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeSnapShotCrossZoneSentBytes = metric.Metadata{
		Name: "range.snapshots.cross-zone.sent-bytes",
		Help: `Number of snapshot bytes sent cross zone within same region or if
		region tiers are not configured. This count increases for each snapshot sent
		between different zones within the same region. However, if the region tiers
		are not configured, this count may also include snapshot data sent between
		different regions. Ensuring consistent configuration of region and zone
		tiers across nodes helps to accurately monitor the data transmitted.`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeSnapShotCrossZoneRcvdBytes = metric.Metadata{
		Name: "range.snapshots.cross-zone.rcvd-bytes",
		Help: `Number of snapshot bytes received cross zone within same region or if
		region tiers are not configured. This count increases for each snapshot
		received between different zones within the same region. However, if the
		region tiers are not configured, this count may also include snapshot data
		received between different regions. Ensuring consistent configuration of
		region and zone tiers across nodes helps to accurately monitor the data
		transmitted.`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeSnapshotSendQueueLength = metric.Metadata{
		Name:        "range.snapshots.send-queue",
		Help:        "Number of snapshots queued to send",
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeSnapshotRecvQueueLength = metric.Metadata{
		Name:        "range.snapshots.recv-queue",
		Help:        "Number of snapshots queued to receive",
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeSnapshotSendInProgress = metric.Metadata{
		Name:        "range.snapshots.send-in-progress",
		Help:        "Number of non-empty snapshots being sent",
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeSnapshotRecvInProgress = metric.Metadata{
		Name:        "range.snapshots.recv-in-progress",
		Help:        "Number of non-empty snapshots being received",
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeSnapshotSendTotalInProgress = metric.Metadata{
		Name:        "range.snapshots.send-total-in-progress",
		Help:        "Number of total snapshots being sent",
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeSnapshotRecvTotalInProgress = metric.Metadata{
		Name:        "range.snapshots.recv-total-in-progress",
		Help:        "Number of total snapshots being received",
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeSnapshotSendQueueSize = metric.Metadata{
		Name:        "range.snapshots.send-queue-bytes",
		Help:        "Total size of all snapshots in the snapshot send queue",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeSnapshotRecvQueueSize = metric.Metadata{
		Name:        "range.snapshots.recv-queue-bytes",
		Help:        "Total size of all snapshots in the snapshot receive queue",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}

	metaRangeRaftLeaderTransfers = metric.Metadata{
		Name:        "range.raftleadertransfers",
		Help:        "Number of raft leader transfers",
		Measurement: "Leader Transfers",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeRaftLeaderRemovals = metric.Metadata{
		Name:        "range.raftleaderremovals",
		Help:        "Number of times the current Raft leader was removed from a range",
		Measurement: "Raft leader removals",
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
	metaDelegateSnapshotSendBytes = metric.Metadata{
		Name: "range.snapshots.delegate.sent-bytes",
		Help: `Bytes sent using a delegate.

The number of bytes sent as a result of a delegate snapshot request
that was originated from a different node. This metric is useful in
evaluating the network savings of not sending cross region traffic.
`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaDelegateSnapshotSuccesses = metric.Metadata{
		Name: "range.snapshots.delegate.successes",
		Help: `Number of snapshots that were delegated to a different node and
resulted in success on that delegate. This does not count self delegated snapshots.
`,
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}
	metaDelegateSnapshotFailures = metric.Metadata{
		Name: "range.snapshots.delegate.failures",
		Help: `Number of snapshots that were delegated to a different node and
resulted in failure on that delegate. There are numerous reasons a failure can
occur on a delegate such as timeout, the delegate Raft log being too far behind
or the delegate being too busy to send.
`,
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}
	metaDelegateSnapshotInProgress = metric.Metadata{
		Name:        "range.snapshots.delegate.in-progress",
		Help:        `Number of delegated snapshots that are currently in-flight.`,
		Measurement: "Snapshots",
		Unit:        metric.Unit_COUNT,
	}

	// Quota pool metrics.
	metaRaftQuotaPoolPercentUsed = metric.Metadata{
		Name:        "raft.quota_pool.percent_used",
		Help:        `Histogram of proposal quota pool utilization (0-100) per leaseholder per metrics interval`,
		Measurement: "Percent",
		// TODO(kv-obs): There is Unit_PERCENT but it seems to operate on float64
		// (0 to 1.0) so it probably won't produce useful results here.
		Unit: metric.Unit_COUNT,
	}
	// Raft entry bytes loaded in memory.
	metaRaftLoadedEntriesBytes = metric.Metadata{
		Name:        "raft.loaded_entries.bytes",
		Help:        `Bytes allocated by raft Storage.Entries calls that are still kept in memory`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}

	// Raft processing metrics.
	metaRaftTicks = metric.Metadata{
		Name:        "raft.ticks",
		Help:        "Number of Raft ticks queued",
		Measurement: "Ticks",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftProposalsDropped = metric.Metadata{
		Name:        "raft.dropped",
		Help:        "Number of Raft proposals dropped (this counts individial raftpb.Entry, not raftpb.MsgProp)",
		Measurement: "Proposals",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftProposalsDroppedLeader = metric.Metadata{
		Name: "raft.dropped_leader",
		Help: "Number of Raft proposals dropped by a Replica that believes itself to be the leader; " +
			"each update also increments `raft.dropped` " +
			"(this counts individial raftpb.Entry, not raftpb.MsgProp)",
		Measurement: "Proposals",
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
	metaRaftCommandsProposed = metric.Metadata{
		Name: "raft.commands.proposed",
		Help: `Number of Raft commands proposed.

The number of proposals and all kinds of reproposals made by leaseholders. This
metric approximates the number of commands submitted through Raft.`,
		Measurement: "Commands",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftCommandsReproposed = metric.Metadata{
		Name: "raft.commands.reproposed.unchanged",
		Help: `Number of Raft commands re-proposed without modification.

The number of Raft commands that leaseholders re-proposed without modification.
Such re-proposals happen for commands that are not committed/applied within a
timeout, and have a high chance of being dropped.`,
		Measurement: "Commands",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftCommandsReproposedLAI = metric.Metadata{
		Name: "raft.commands.reproposed.new-lai",
		Help: `Number of Raft commands re-proposed with a newer LAI.

The number of Raft commands that leaseholders re-proposed with a modified LAI.
Such re-proposals happen for commands that are committed to Raft out of intended
order, and hence can not be applied as is.`,
		Measurement: "Commands",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftCommandsPending = metric.Metadata{
		Name: "raft.commands.pending",
		Help: `Number of Raft commands proposed and pending.

The number of Raft commands that the leaseholders are tracking as in-flight.
These commands will be periodically reproposed until they are applied or until
they fail, either unequivocally or ambiguously.`,
		Measurement: "Commands",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftCommandsApplied = metric.Metadata{
		Name: "raft.commandsapplied",
		Help: `Number of Raft commands applied.

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

This measures the end-to-end-latency of the Raft state advancement loop, including:
- snapshot application
- SST ingestion
- durably appending to the Raft log (i.e. includes fsync)
- entry application (incl. replicated side effects, notably log truncation)

These include work measured in 'raft.process.commandcommit.latency' and
'raft.process.applycommitted.latency'. However, matching percentiles of these
metrics may be *higher* than handleready, since not every handleready cycle
leads to an update of the others. For example, under tpcc-100 on a single node,
the handleready count is approximately twice the logcommit count (and logcommit
count tracks closely with applycommitted count).

High percentile outliers can be caused by individual large Raft commands or
storage layer blips. Lower percentile (e.g. 50th) increases are often driven by
CPU exhaustion or storage layer slowdowns.
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
	metaRaftReplicationLatency = metric.Metadata{
		Name: "raft.replication.latency",
		Help: `The duration elapsed between having evaluated a BatchRequest and it being
reflected in the proposer's state machine (i.e. having applied fully).

This encompasses time spent in the quota pool, in replication (including
reproposals), and application, but notably *not* sequencing latency (i.e.
contention and latch acquisition).

No measurement is recorded for read-only commands as well as read-write commands
which end up not writing (such as a DeleteRange on an empty span). Commands that
result in 'above-replication' errors (i.e. txn retries, etc) are similarly
excluded. Errors that arise while waiting for the in-flight replication result
or result from application of the command are included.

Note also that usually, clients are signalled at beginning of application, but
the recorded measurement captures the entirety of log application.

The duration is always measured on the proposer, even if the Raft leader and
leaseholder are not colocated, or the request is proposed from a follower.

Commands that use async consensus will still cause a measurement that reflects
the actual replication latency, despite returning early to the client.`,
		Measurement: "Latency",
		Unit:        metric.Unit_COUNT,
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
	metaRaftStorageReadBytes = metric.Metadata{
		Name: "raft.storage.read_bytes",
		Help: `Counter of raftpb.Entry.Size() read from pebble for raft log entries.

These are the bytes returned from the (raft.Storage).Entries method that were not
returned via the raft entry cache. This metric plus the raft.entrycache.read_bytes
metric represent the total bytes returned from the Entries method.

Since pebble might serve these entries from the block cache, only a fraction of this
throughput might manifest in disk metrics.

Entries tracked in this metric incur an unmarshalling-related CPU and memory
overhead that would not be incurred would the entries be served from the raft
entry cache.

The bytes returned here do not correspond 1:1 to bytes read from pebble. This
metric measures the in-memory size of the raftpb.Entry, whereas we read its
encoded representation from pebble. As there is no compression involved, these
will generally be comparable.

A common reason for elevated measurements on this metric is that a store is
falling behind on raft log application. The raft entry cache generally tracks
entries that were recently appended, so if log application falls behind the
cache will already have moved on to newer entries.
`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRaftStorageError = metric.Metadata{
		Name:        "raft.storage.error",
		Help:        "Number of Raft storage errors",
		Measurement: "Error Count",
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
	metaRaftRcvdFortifyLeader = metric.Metadata{
		Name:        "raft.rcvd.fortifyleader",
		Help:        "Number of MsgFortifyLeader messages received by this store",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftRcvdFortifyLeaderResp = metric.Metadata{
		Name:        "raft.rcvd.fortifyleaderresp",
		Help:        "Number of MsgFortifyLeaderResp messages received by this store",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftRcvdDeFortifyLeader = metric.Metadata{
		Name:        "raft.rcvd.defortifyleader",
		Help:        "Number of MsgDeFortifyLeader messages received by this store",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftRcvdDropped = metric.Metadata{
		Name:        "raft.rcvd.dropped",
		Help:        "Number of incoming Raft messages dropped (due to queue length or size)",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftRcvdDroppedBytes = metric.Metadata{
		Name:        "raft.rcvd.dropped_bytes",
		Help:        "Bytes of dropped incoming Raft messages",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRaftRcvdQueuedBytes = metric.Metadata{
		Name:        "raft.rcvd.queued_bytes",
		Help:        "Number of bytes in messages currently waiting for raft processing",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRaftRcvdSteppedBytes = metric.Metadata{
		Name: "raft.rcvd.stepped_bytes",
		Help: `Number of bytes in messages processed by Raft.

Messages reflected here have been handed to Raft (via RawNode.Step). This does not imply that the
messages are no longer held in memory or that IO has been performed. Raft delegates IO activity to
Raft ready handling, which occurs asynchronously. Since handing messages to Raft serializes with
Raft ready handling and size the size of an entry is dominated by the contained pebble WriteBatch,
on average the rate at which this metric increases is a good proxy for the rate at which Raft ready
handling consumes writes.
`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}

	metaRaftRcvdBytes = metric.Metadata{
		Name: "raft.rcvd.bytes",
		Help: `Number of bytes in Raft messages received by this store. Note
		that this does not include raft snapshot received.`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRaftRcvdCrossRegionBytes = metric.Metadata{
		Name: "raft.rcvd.cross_region.bytes",
		Help: `Number of bytes received by this store for cross region Raft messages
		(when region tiers are configured). Note that this does not include raft
		snapshot received.`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRaftRcvdCrossZoneBytes = metric.Metadata{
		Name: "raft.rcvd.cross_zone.bytes",
		Help: `Number of bytes received by this store for cross zone, same region
		Raft messages (when region and zone tiers are configured). If region tiers
		are not configured, this count may include data sent between different
		regions. To ensure accurate monitoring of transmitted data, it is important
		to set up a consistent locality configuration across nodes. Note that this
		does not include raft snapshot received.`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRaftSentBytes = metric.Metadata{
		Name: "raft.sent.bytes",
		Help: `Number of bytes in Raft messages sent by this store. Note that
		this does not include raft snapshot sent.`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRaftSentCrossRegionBytes = metric.Metadata{
		Name: "raft.sent.cross_region.bytes",
		Help: `Number of bytes sent by this store for cross region Raft messages
		(when region tiers are configured). Note that this does not include raft
		snapshot sent.`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRaftSentCrossZoneBytes = metric.Metadata{
		Name: "raft.sent.cross_zone.bytes",
		Help: `Number of bytes sent by this store for cross zone, same region Raft
		messages (when region and zone tiers are configured). If region tiers are
		not configured, this count may include data sent between different regions.
		To ensure accurate monitoring of transmitted data, it is important to set up
		a consistent locality configuration across nodes. Note that this does not
		include raft snapshot sent.`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
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

	metaRaftFollowerPaused = metric.Metadata{
		Name: "admission.raft.paused_replicas",
		Help: `Number of followers (i.e. Replicas) to which replication is currently paused to help them recover from I/O overload.

Such Replicas will be ignored for the purposes of proposal quota, and will not
receive replication traffic. They are essentially treated as offline for the
purpose of replication. This serves as a crude form of admission control.

The count is emitted by the leaseholder of each range.`,
		Measurement: "Followers",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftPausedFollowerDroppedMsgs = metric.Metadata{
		Name: "admission.raft.paused_replicas_dropped_msgs",
		Help: `Number of messages dropped instead of being sent to paused replicas.

The messages are dropped to help these replicas to recover from I/O overload.`,
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}

	metaIOOverload = metric.Metadata{
		Name:        "admission.io.overload",
		Help:        `1-normalized float indicating whether IO admission control considers the store as overloaded with respect to compaction out of L0 (considers sub-level and file counts).`,
		Measurement: "Threshold",
		Unit:        metric.Unit_PERCENT,
	}

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
	metaLeaseQueueSuccesses = metric.Metadata{
		Name:        "queue.lease.process.success",
		Help:        "Number of replicas successfully processed by the replica lease queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaLeaseQueueFailures = metric.Metadata{
		Name:        "queue.lease.process.failure",
		Help:        "Number of replicas which failed processing in the replica lease queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaLeaseQueuePending = metric.Metadata{
		Name:        "queue.lease.pending",
		Help:        "Number of pending replicas in the replica lease queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaLeaseQueueProcessingNanos = metric.Metadata{
		Name:        "queue.lease.processingnanos",
		Help:        "Nanoseconds spent processing replicas in the replica lease queue",
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
	metaLeaseQueuePurgatory = metric.Metadata{
		Name:        "queue.lease.purgatory",
		Help:        "Number of replicas in the lease queue's purgatory, awaiting lease transfer operations",
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
	metaGCNumRangeKeysAffected = metric.Metadata{
		Name:        "queue.gc.info.numrangekeysaffected",
		Help:        "Number of range keys GC'able",
		Measurement: "Range Keys",
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
	metaGCTransactionSpanGCPrepared = metric.Metadata{
		Name:        "queue.gc.info.transactionspangcprepared",
		Help:        "Number of GC'able entries corresponding to prepared txns",
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
	metaGCUsedClearRange = metric.Metadata{
		Name:        "queue.gc.info.clearrangesuccess",
		Help:        "Number of successful ClearRange operations during GC",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaGCFailedClearRange = metric.Metadata{
		Name:        "queue.gc.info.clearrangefailed",
		Help:        "Number of failed ClearRange operations during GC",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaGCEnqueueHighPriority = metric.Metadata{
		Name:        "queue.gc.info.enqueuehighpriority",
		Help:        "Number of replicas enqueued for GC with high priority",
		Measurement: "Replicas",
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
		Help:        "Algorithm in use for encryption-at-rest, see storage/enginepb/key_registry.proto",
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
	metaLatchConflictWaitDurations = metric.Metadata{
		Name:        "kv.concurrency.latch_conflict_wait_durations",
		Help:        "Durations in nanoseconds spent on latch acquisition waiting for conflicts with other latches",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	// Closed timestamp metrics.
	metaClosedTimestampMaxBehindNanos = metric.Metadata{
		Name:        "kv.closed_timestamp.max_behind_nanos",
		Help:        "Largest latency between realtime and replica max closed timestamp",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
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
	// Replica read batch evaluation.
	metaReplicaReadBatchEvaluationLatency = metric.Metadata{
		Name: "kv.replica_read_batch_evaluate.latency",
		Help: `Execution duration for evaluating a BatchRequest on the read-only path after latches have been acquired.

A measurement is recorded regardless of outcome (i.e. also in case of an error). If internal retries occur, each instance is recorded separately.`,
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	// Replica read-write batch evaluation.
	metaReplicaWriteBatchEvaluationLatency = metric.Metadata{
		Name: "kv.replica_write_batch_evaluate.latency",
		Help: `Execution duration for evaluating a BatchRequest on the read-write path after latches have been acquired.

A measurement is recorded regardless of outcome (i.e. also in case of an error). If internal retries occur, each instance is recorded separately.
Note that the measurement does not include the duration for replicating the evaluated command.`,
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaPopularKeyCount = metric.Metadata{
		Name:        "kv.loadsplitter.popularkey",
		Help:        "Load-based splitter could not find a split key and the most popular sampled split key occurs in >= 25% of the samples.",
		Measurement: "Occurrences",
		Unit:        metric.Unit_COUNT,
	}

	metaNoSplitKeyCount = metric.Metadata{
		Name:        "kv.loadsplitter.nosplitkey",
		Help:        "Load-based splitter could not find a split key.",
		Measurement: "Occurrences",
		Unit:        metric.Unit_COUNT,
	}

	metaSplitEstimatedStats = metric.Metadata{
		Name:        "kv.split.estimated_stats",
		Help:        "Number of splits that computed estimated MVCC stats.",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}

	metaSplitEstimatedTotalBytesDiff = metric.Metadata{
		Name:        "kv.split.total_bytes_estimates",
		Help:        "Number of total bytes difference between the pre-split and post-split MVCC stats.",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}

	metaStorageFlushUtilization = metric.Metadata{
		Name:        "storage.flush.utilization",
		Help:        "The percentage of time the storage engine is actively flushing memtables to disk.",
		Measurement: "Flush Utilization",
		Unit:        metric.Unit_PERCENT,
	}
	metaWALBytesWritten = metric.Metadata{
		Name:        "storage.wal.bytes_written",
		Help:        "The number of bytes the storage engine has written to the WAL",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaWALBytesIn = metric.Metadata{
		Name:        "storage.wal.bytes_in",
		Help:        "The number of logical bytes the storage engine has written to the WAL",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaStorageFsyncLatency = metric.Metadata{
		Name:        "storage.wal.fsync.latency",
		Help:        "The write ahead log fsync latency",
		Measurement: "Fsync Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaStorageWALFailoverSwitchCount = metric.Metadata{
		Name: "storage.wal.failover.switch.count",
		Help: "Count of the number of times WAL writing has switched from primary to secondary " +
			"and vice versa.",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaStorageWALFailoverPrimaryDuration = metric.Metadata{
		Name: "storage.wal.failover.primary.duration",
		Help: "Cumulative time spent writing to the primary WAL directory. Only populated " +
			"when WAL failover is configured",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaStorageWALFailoverSecondaryDuration = metric.Metadata{
		Name: "storage.wal.failover.secondary.duration",
		Help: "Cumulative time spent writing to the secondary WAL directory. Only populated " +
			"when WAL failover is configured",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaStorageWALFailoverWriteAndSyncLatency = metric.Metadata{
		Name: "storage.wal.failover.write_and_sync.latency",
		Help: "The observed latency for writing and syncing to the write ahead log. Only populated " +
			"when WAL failover is configured",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaReplicaReadBatchDroppedLatchesBeforeEval = metric.Metadata{
		Name:        "kv.replica_read_batch_evaluate.dropped_latches_before_eval",
		Help:        `Number of times read-only batches dropped latches before evaluation.`,
		Measurement: "Batches",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicaReadBatchWithoutInterleavingIter = metric.Metadata{
		Name:        "kv.replica_read_batch_evaluate.without_interleaving_iter",
		Help:        `Number of read-only batches evaluated without an intent interleaving iter.`,
		Measurement: "Batches",
		Unit:        metric.Unit_COUNT,
	}
	metaDiskReadCount = metric.Metadata{
		Name:        "storage.disk.read.count",
		Unit:        metric.Unit_COUNT,
		Measurement: "Operations",
		Help:        "Disk read operations on the store's disk since this process started (as reported by the OS)",
	}
	metaDiskReadBytes = metric.Metadata{
		Name:        "storage.disk.read.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes read from the store's disk since this process started (as reported by the OS)",
	}
	metaDiskReadTime = metric.Metadata{
		Name:        "storage.disk.read.time",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Time",
		Help:        "Time spent reading from the store's disk since this process started (as reported by the OS)",
	}
	metaDiskWriteCount = metric.Metadata{
		Name:        "storage.disk.write.count",
		Unit:        metric.Unit_COUNT,
		Measurement: "Operations",
		Help:        "Disk write operations on the store's disk since this process started (as reported by the OS)",
	}
	metaDiskWriteBytes = metric.Metadata{
		Name:        "storage.disk.write.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes written to the store's disk since this process started (as reported by the OS)",
	}
	metaDiskWriteTime = metric.Metadata{
		Name:        "storage.disk.write.time",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Time",
		Help:        "Time spent writing to the store's disks since this process started (as reported by the OS)",
	}
	metaDiskIOTime = metric.Metadata{
		Name:        "storage.disk.io.time",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Time",
		Help:        "Time spent reading from or writing to the store's disk since this process started (as reported by the OS)",
	}
	metaDiskWeightedIOTime = metric.Metadata{
		Name:        "storage.disk.weightedio.time",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Time",
		Help:        "Weighted time spent reading from or writing to the store's disk since this process started (as reported by the OS)",
	}
	metaDiskIopsInProgress = metric.Metadata{
		Name:        "storage.disk.iopsinprogress",
		Unit:        metric.Unit_COUNT,
		Measurement: "Operations",
		Help:        "IO operations currently in progress on the store's disk (as reported by the OS)",
	}
	// The disk rate metrics are computed using data sampled on the interval,
	// COCKROACH_DISK_STATS_POLLING_INTERVAL.
	metaDiskReadMaxBytesPerSecond = metric.Metadata{
		Name:        "storage.disk.read-max.bytespersecond",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Maximum rate at which bytes were read from disk (as reported by the OS)",
	}
	metaDiskWriteMaxBytesPerSecond = metric.Metadata{
		Name:        "storage.disk.write-max.bytespersecond",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Maximum rate at which bytes were written to disk (as reported by the OS)",
	}
)

// StoreMetrics is the set of metrics for a given store.
type StoreMetrics struct {
	registry *metric.Registry

	// TenantStorageMetrics stores aggregate metrics for storage usage on a per
	// tenant basis.
	*TenantsStorageMetrics

	// LoadSplitterMetrics stores metrics for load-based splitter split key.
	*split.LoadSplitterMetrics

	// Replica metrics.
	ReplicaCount                  *metric.Gauge // Does not include uninitialized or reserved replicas.
	ReservedReplicaCount          *metric.Gauge
	RaftLeaderCount               *metric.Gauge
	RaftLeaderNotLeaseHolderCount *metric.Gauge
	RaftLeaderNotFortifiedCount   *metric.Gauge
	RaftLeaderInvalidLeaseCount   *metric.Gauge
	LeaseHolderCount              *metric.Gauge
	QuiescentCount                *metric.Gauge
	AsleepCount                   *metric.Gauge
	UninitializedCount            *metric.Gauge
	RaftFlowStateCounts           [tracker.StateCount]*metric.Gauge

	// Range metrics.
	RangeCount                *metric.Gauge
	UnavailableRangeCount     *metric.Gauge
	UnderReplicatedRangeCount *metric.Gauge
	OverReplicatedRangeCount  *metric.Gauge
	DecommissioningRangeCount *metric.Gauge

	// Lease request metrics for successful and failed lease requests. These
	// count proposals (i.e. it does not matter how many replicas apply the
	// lease).
	LeaseRequestSuccessCount       *metric.Counter
	LeaseRequestErrorCount         *metric.Counter
	LeaseRequestLatency            metric.IHistogram
	LeaseTransferSuccessCount      *metric.Counter
	LeaseTransferErrorCount        *metric.Counter
	LeaseTransferLocksWritten      *metric.Counter
	LeaseExpirationCount           *metric.Gauge
	LeaseEpochCount                *metric.Gauge
	LeaseLeaderCount               *metric.Gauge
	LeaseLivenessCount             *metric.Gauge
	LeaseViolatingPreferencesCount *metric.Gauge
	LeaseLessPreferredCount        *metric.Gauge

	// Storage metrics.
	ResolveCommitCount *metric.Counter
	ResolveAbortCount  *metric.Counter
	ResolvePoisonCount *metric.Counter
	Capacity           *metric.Gauge
	Available          *metric.Gauge
	Used               *metric.Gauge
	Reserved           *metric.Gauge

	// Rebalancing metrics.
	AverageQueriesPerSecond    *metric.GaugeFloat64
	AverageWritesPerSecond     *metric.GaugeFloat64
	AverageReadsPerSecond      *metric.GaugeFloat64
	AverageRequestsPerSecond   *metric.GaugeFloat64
	AverageWriteBytesPerSecond *metric.GaugeFloat64
	AverageReadBytesPerSecond  *metric.GaugeFloat64
	AverageCPUNanosPerSecond   *metric.GaugeFloat64
	// NB: Even though we could average the histogram in order to get
	// AverageCPUNanosPerSecond from RecentReplicaCPUNanosPerSecond, we duplicate
	// both for backwards compatibility since the cost of the gauge is small.
	// This includes all replicas, including quiesced ones.
	RecentReplicaCPUNanosPerSecond *metric.ManualWindowHistogram
	RecentReplicaQueriesPerSecond  *metric.ManualWindowHistogram

	// Follower read metrics.
	FollowerReadsCount *metric.Counter

	// Server-side transaction metrics.
	CommitWaitsBeforeCommitTrigger                           *metric.Counter
	WriteEvaluationServerSideRetrySuccess                    *metric.Counter
	WriteEvaluationServerSideRetryFailure                    *metric.Counter
	ReadEvaluationServerSideRetrySuccess                     *metric.Counter
	ReadEvaluationServerSideRetryFailure                     *metric.Counter
	ReadWithinUncertaintyIntervalErrorServerSideRetrySuccess *metric.Counter
	ReadWithinUncertaintyIntervalErrorServerSideRetryFailure *metric.Counter
	OnePhaseCommitSuccess                                    *metric.Counter
	OnePhaseCommitFailure                                    *metric.Counter

	// Storage (pebble) metrics. Some are named RocksDB which is what we used
	// before pebble, and this name is kept for backwards compatibility despite
	// the backing metrics now originating from pebble.
	RdbBlockCacheHits                 *metric.Counter
	RdbBlockCacheMisses               *metric.Counter
	RdbBlockCacheUsage                *metric.Gauge
	RdbBloomFilterPrefixChecked       *metric.Counter
	RdbBloomFilterPrefixUseful        *metric.Counter
	RdbMemtableTotalSize              *metric.Gauge
	RdbFlushes                        *metric.Counter
	RdbFlushedBytes                   *metric.Counter
	RdbCompactions                    *metric.Counter
	RdbIngestedBytes                  *metric.Counter
	RdbCompactedBytesRead             *metric.Counter
	RdbCompactedBytesWritten          *metric.Counter
	RdbTableReadersMemEstimate        *metric.Gauge
	RdbReadAmplification              *metric.Gauge
	RdbNumSSTables                    *metric.Gauge
	RdbPendingCompaction              *metric.Gauge
	RdbMarkedForCompactionFiles       *metric.Gauge
	RdbKeysRangeKeySets               *metric.Gauge
	RdbKeysTombstones                 *metric.Gauge
	RdbL0BytesFlushed                 *metric.Counter
	RdbL0Sublevels                    *metric.Gauge
	RdbL0NumFiles                     *metric.Gauge
	RdbBytesIngested                  [7]*metric.Counter      // idx = level
	RdbLevelSize                      [7]*metric.Gauge        // idx = level
	RdbLevelScore                     [7]*metric.GaugeFloat64 // idx = level
	RdbWriteStalls                    *metric.Gauge
	RdbWriteStallNanos                *metric.Gauge
	SingleDelInvariantViolations      *metric.Counter
	SingleDelIneffectualCount         *metric.Counter
	SharedStorageBytesRead            *metric.Counter
	SharedStorageBytesWritten         *metric.Counter
	BlockLoadsInProgress              *metric.Gauge
	BlockLoadsQueued                  *metric.Counter
	SecondaryCacheSize                *metric.Gauge
	SecondaryCacheCount               *metric.Gauge
	SecondaryCacheTotalReads          *metric.Counter
	SecondaryCacheMultiShardReads     *metric.Counter
	SecondaryCacheMultiBlockReads     *metric.Counter
	SecondaryCacheReadsWithFullHit    *metric.Counter
	SecondaryCacheReadsWithPartialHit *metric.Counter
	SecondaryCacheReadsWithNoHit      *metric.Counter
	SecondaryCacheEvictions           *metric.Counter
	SecondaryCacheWriteBackFails      *metric.Counter
	StorageCompactionsPinnedKeys      *metric.Counter
	StorageCompactionsPinnedBytes     *metric.Counter
	StorageCompactionsCancelledCount  *metric.Counter
	StorageCompactionsCancelledBytes  *metric.Counter
	StorageCompactionsDuration        *metric.Counter
	StorageWriteAmplification         *metric.GaugeFloat64
	IterBlockBytes                    *metric.Counter
	IterBlockBytesInCache             *metric.Counter
	IterBlockReadDuration             *metric.Counter
	IterExternalSeeks                 *metric.Counter
	IterExternalSteps                 *metric.Counter
	IterInternalSeeks                 *metric.Counter
	IterInternalSteps                 *metric.Counter
	FlushableIngestCount              *metric.Counter
	FlushableIngestTableCount         *metric.Counter
	FlushableIngestTableSize          *metric.Counter
	BatchCommitCount                  *metric.Counter
	BatchCommitDuration               *metric.Counter
	BatchCommitSemWaitDuration        *metric.Counter
	BatchCommitWALQWaitDuration       *metric.Counter
	BatchCommitMemStallDuration       *metric.Counter
	BatchCommitL0StallDuration        *metric.Counter
	BatchCommitWALRotWaitDuration     *metric.Counter
	BatchCommitCommitWaitDuration     *metric.Counter
	SSTableZombieBytes                *metric.Gauge
	SSTableCompressionSnappy          *metric.Gauge
	SSTableCompressionZstd            *metric.Gauge
	SSTableCompressionUnknown         *metric.Gauge
	SSTableCompressionNone            *metric.Gauge
	categoryIterMetrics               pebbleCategoryIterMetricsContainer
	categoryDiskWriteMetrics          pebbleCategoryDiskWriteMetricsContainer
	WALBytesWritten                   *metric.Counter
	WALBytesIn                        *metric.Counter
	WALFailoverSwitchCount            *metric.Counter
	WALFailoverPrimaryDuration        *metric.Counter
	WALFailoverSecondaryDuration      *metric.Counter
	WALFailoverWriteAndSyncLatency    *metric.ManualWindowHistogram

	RdbCheckpoints *metric.Gauge

	// Disk health metrics.
	DiskSlow    *metric.Counter
	DiskStalled *metric.Counter

	// TODO(mrtracy): This should be removed as part of #4465. This is only
	// maintained to keep the current structure of NodeStatus; it would be
	// better to convert the Gauges above into counters which are adjusted
	// accordingly.

	// Range event metrics.
	RangeSplits                 *metric.Counter
	RangeMerges                 *metric.Counter
	RangeAdds                   *metric.Counter
	RangeRemoves                *metric.Counter
	RangeRaftLeaderTransfers    *metric.Counter
	RangeRaftLeaderRemovals     *metric.Counter
	RangeLossOfQuorumRecoveries *metric.Counter

	// Range snapshot metrics.
	RangeSnapshotsGenerated                      *metric.Counter
	RangeSnapshotsAppliedByVoters                *metric.Counter
	RangeSnapshotsAppliedForInitialUpreplication *metric.Counter
	RangeSnapshotsAppliedByNonVoters             *metric.Counter
	RangeSnapshotRcvdBytes                       *metric.Counter
	RangeSnapshotSentBytes                       *metric.Counter
	RangeSnapshotUnknownRcvdBytes                *metric.Counter
	RangeSnapshotUnknownSentBytes                *metric.Counter
	RangeSnapshotRecoveryRcvdBytes               *metric.Counter
	RangeSnapshotRecoverySentBytes               *metric.Counter
	RangeSnapshotUpreplicationRcvdBytes          *metric.Counter
	RangeSnapshotUpreplicationSentBytes          *metric.Counter
	RangeSnapshotRebalancingRcvdBytes            *metric.Counter
	RangeSnapshotRebalancingSentBytes            *metric.Counter
	RangeSnapshotRecvFailed                      *metric.Counter
	RangeSnapshotRecvUnusable                    *metric.Counter
	RangeSnapShotCrossRegionSentBytes            *metric.Counter
	RangeSnapShotCrossRegionRcvdBytes            *metric.Counter
	RangeSnapShotCrossZoneSentBytes              *metric.Counter
	RangeSnapShotCrossZoneRcvdBytes              *metric.Counter

	// Range snapshot queue metrics.
	RangeSnapshotSendQueueLength     *metric.Gauge
	RangeSnapshotRecvQueueLength     *metric.Gauge
	RangeSnapshotSendInProgress      *metric.Gauge
	RangeSnapshotRecvInProgress      *metric.Gauge
	RangeSnapshotSendTotalInProgress *metric.Gauge
	RangeSnapshotRecvTotalInProgress *metric.Gauge
	RangeSnapshotSendQueueSize       *metric.Gauge
	RangeSnapshotRecvQueueSize       *metric.Gauge

	// Delegate snapshot metrics. These don't count self-delegated snapshots.
	DelegateSnapshotSendBytes  *metric.Counter
	DelegateSnapshotSuccesses  *metric.Counter
	DelegateSnapshotFailures   *metric.Counter
	DelegateSnapshotInProgress *metric.Gauge

	// Raft processing metrics.
	RaftTicks                  *metric.Counter
	RaftProposalsDropped       *metric.Counter
	RaftProposalsDroppedLeader *metric.Counter
	RaftQuotaPoolPercentUsed   metric.IHistogram
	RaftLoadedEntriesBytes     *metric.Gauge
	RaftWorkingDurationNanos   *metric.Counter
	RaftTickingDurationNanos   *metric.Counter
	RaftCommandsProposed       *metric.Counter
	RaftCommandsReproposed     *metric.Counter
	RaftCommandsReproposedLAI  *metric.Counter
	RaftCommandsPending        *metric.Gauge
	RaftCommandsApplied        *metric.Counter
	RaftLogCommitLatency       metric.IHistogram
	RaftCommandCommitLatency   metric.IHistogram
	RaftHandleReadyLatency     metric.IHistogram
	RaftApplyCommittedLatency  metric.IHistogram
	RaftReplicationLatency     metric.IHistogram
	RaftSchedulerLatency       metric.IHistogram
	RaftTimeoutCampaign        *metric.Counter
	RaftStorageReadBytes       *metric.Counter
	RaftStorageError           *metric.Counter

	// Raft message metrics.
	//
	// An array for conveniently finding the appropriate metric.
	RaftRcvdMessages         [maxRaftMsgType + 1]*metric.Counter
	RaftRcvdDropped          *metric.Counter
	RaftRcvdDroppedBytes     *metric.Counter
	RaftRcvdQueuedBytes      *metric.Gauge
	RaftRcvdSteppedBytes     *metric.Counter
	RaftRcvdBytes            *metric.Counter
	RaftRcvdCrossRegionBytes *metric.Counter
	RaftRcvdCrossZoneBytes   *metric.Counter
	RaftSentBytes            *metric.Counter
	RaftSentCrossRegionBytes *metric.Counter
	RaftSentCrossZoneBytes   *metric.Counter

	// Raft log metrics.
	RaftLogFollowerBehindCount *metric.Gauge
	RaftLogTruncated           *metric.Counter

	RaftPausedFollowerCount       *metric.Gauge
	RaftPausedFollowerDroppedMsgs *metric.Counter
	IOOverload                    *metric.GaugeFloat64

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
	LeaseQueueSuccesses                       *metric.Counter
	LeaseQueueFailures                        *metric.Counter
	LeaseQueuePending                         *metric.Gauge
	LeaseQueueProcessingNanos                 *metric.Counter
	LeaseQueuePurgatory                       *metric.Gauge
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
	GCNumRangeKeysAffected       *metric.Counter
	GCIntentsConsidered          *metric.Counter
	GCIntentTxns                 *metric.Counter
	GCTransactionSpanScanned     *metric.Counter
	GCTransactionSpanGCAborted   *metric.Counter
	GCTransactionSpanGCCommitted *metric.Counter
	GCTransactionSpanGCStaging   *metric.Counter
	GCTransactionSpanGCPending   *metric.Counter
	GCTransactionSpanGCPrepared  *metric.Counter
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
	GCUsedClearRange          *metric.Counter
	GCFailedClearRange        *metric.Counter
	GCEnqueueHighPriority     *metric.Counter

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
	LatchWaitDurations             metric.IHistogram

	// Ingestion metrics
	IngestCount *metric.Gauge

	// Closed timestamp metrics.
	ClosedTimestampMaxBehindNanos *metric.Gauge

	// Replica circuit breaker.
	ReplicaCircuitBreakerCurTripped *metric.Gauge
	ReplicaCircuitBreakerCumTripped *metric.Counter

	// Replica batch evaluation metrics.
	ReplicaReadBatchEvaluationLatency  metric.IHistogram
	ReplicaWriteBatchEvaluationLatency metric.IHistogram

	ReplicaReadBatchDroppedLatchesBeforeEval *metric.Counter
	ReplicaReadBatchWithoutInterleavingIter  *metric.Counter

	SplitsWithEstimatedStats     *metric.Counter
	SplitEstimatedTotalBytesDiff *metric.Counter

	FlushUtilization *metric.GaugeFloat64
	FsyncLatency     *metric.ManualWindowHistogram

	// Disk metrics
	DiskReadBytes              *metric.Counter
	DiskReadCount              *metric.Counter
	DiskReadTime               *metric.Counter
	DiskWriteBytes             *metric.Counter
	DiskWriteCount             *metric.Counter
	DiskWriteTime              *metric.Counter
	DiskIOTime                 *metric.Counter
	DiskWeightedIOTime         *metric.Counter
	DiskIopsInProgress         *metric.Gauge
	DiskReadMaxBytesPerSecond  *metric.Gauge
	DiskWriteMaxBytesPerSecond *metric.Gauge
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
		debugutil.SafeStack
	}
}

func (ref *tenantMetricsRef) assert(ctx context.Context) {
	if atomic.LoadInt32(&ref._state) != 0 {
		ref._stack.Lock()
		defer ref._stack.Unlock()
		log.FatalfDepth(ctx, 1, "tenantMetricsRef already finalized in:\n%s", ref._stack.SafeStack)
	}
}

// TenantsStorageMetrics are metrics which are aggregated over all tenants
// present on the server. The struct maintains child metrics used by each
// tenant to track their individual values. The struct expects that children
// call acquire and release to properly reference count the metrics for
// individual tenants.
type TenantsStorageMetrics struct {
	// NB: If adding more metrics to this struct, be sure to
	// also update tenantsStorageMetricsSet().
	LiveBytes      *aggmetric.AggGauge
	KeyBytes       *aggmetric.AggGauge
	ValBytes       *aggmetric.AggGauge
	RangeKeyBytes  *aggmetric.AggGauge
	RangeValBytes  *aggmetric.AggGauge
	TotalBytes     *aggmetric.AggGauge
	IntentBytes    *aggmetric.AggGauge
	LockBytes      *aggmetric.AggGauge
	LiveCount      *aggmetric.AggGauge
	KeyCount       *aggmetric.AggGauge
	ValCount       *aggmetric.AggGauge
	RangeKeyCount  *aggmetric.AggGauge
	RangeValCount  *aggmetric.AggGauge
	IntentCount    *aggmetric.AggGauge
	LockCount      *aggmetric.AggGauge
	LockAge        *aggmetric.AggGauge
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
	tenants syncutil.Map[roachpb.TenantID, tenantStorageMetrics]
}

// tenantsStorageMetricsSet returns the set of all metric names contained
// within TenantsStorageMetrics.
//
// see kvbase.TenantsStorageMetricsSet for public access. Assigned in init().
func tenantsStorageMetricsSet() map[string]struct{} {
	return map[string]struct{}{
		metaLiveBytes.Name:      {},
		metaKeyBytes.Name:       {},
		metaValBytes.Name:       {},
		metaRangeKeyBytes.Name:  {},
		metaRangeValBytes.Name:  {},
		metaTotalBytes.Name:     {},
		metaIntentBytes.Name:    {},
		metaLiveCount.Name:      {},
		metaKeyCount.Name:       {},
		metaValCount.Name:       {},
		metaRangeKeyCount.Name:  {},
		metaRangeValCount.Name:  {},
		metaIntentCount.Name:    {},
		metaLockAge.Name:        {},
		metaGcBytesAge.Name:     {},
		metaSysBytes.Name:       {},
		metaSysCount.Name:       {},
		metaAbortSpanBytes.Name: {},
	}
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
	for {
		if m, ok := sm.tenants.Load(tenantID); ok {
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
			_, loaded := sm.tenants.LoadOrStore(tenantID, m)
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
			m.RangeKeyBytes = sm.RangeKeyBytes.AddChild(tenantIDStr)
			m.RangeValBytes = sm.RangeValBytes.AddChild(tenantIDStr)
			m.TotalBytes = sm.TotalBytes.AddChild(tenantIDStr)
			m.IntentBytes = sm.IntentBytes.AddChild(tenantIDStr)
			m.LockBytes = sm.LockBytes.AddChild(tenantIDStr)
			m.LiveCount = sm.LiveCount.AddChild(tenantIDStr)
			m.KeyCount = sm.KeyCount.AddChild(tenantIDStr)
			m.ValCount = sm.ValCount.AddChild(tenantIDStr)
			m.RangeKeyCount = sm.RangeKeyCount.AddChild(tenantIDStr)
			m.RangeValCount = sm.RangeValCount.AddChild(tenantIDStr)
			m.IntentCount = sm.IntentCount.AddChild(tenantIDStr)
			m.LockCount = sm.LockCount.AddChild(tenantIDStr)
			m.LockAge = sm.LockAge.AddChild(tenantIDStr)
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
	ref._stack.SafeStack = debugutil.Stack()
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
	for _, gptr := range []**aggmetric.Gauge{
		&m.LiveBytes,
		&m.KeyBytes,
		&m.ValBytes,
		&m.RangeKeyBytes,
		&m.RangeValBytes,
		&m.TotalBytes,
		&m.IntentBytes,
		&m.LockBytes,
		&m.LiveCount,
		&m.KeyCount,
		&m.ValCount,
		&m.RangeKeyCount,
		&m.RangeValCount,
		&m.IntentCount,
		&m.LockCount,
		&m.LockAge,
		&m.GcBytesAge,
		&m.SysBytes,
		&m.SysCount,
		&m.AbortSpanBytes,
	} {
		// Reset before unlinking, see Unlink.
		(*gptr).Update(0)
		(*gptr).Unlink()
		*gptr = nil
	}
	sm.tenants.Delete(ref._tenantID)
}

// getTenant is a helper method used to retrieve the metrics for a tenant. The
// call will log fatally if no such tenant has been previously acquired.
func (sm *TenantsStorageMetrics) getTenant(
	ctx context.Context, ref *tenantMetricsRef,
) *tenantStorageMetrics {
	ref.assert(ctx)
	m, ok := sm.tenants.Load(ref._tenantID)
	if !ok {
		log.Fatalf(ctx, "no metrics exist for tenant %v", ref._tenantID)
	}
	return m
}

type tenantStorageMetrics struct {
	mu struct {
		syncutil.Mutex
		refCount int
	}

	LiveBytes      *aggmetric.Gauge
	KeyBytes       *aggmetric.Gauge
	ValBytes       *aggmetric.Gauge
	RangeKeyBytes  *aggmetric.Gauge
	RangeValBytes  *aggmetric.Gauge
	TotalBytes     *aggmetric.Gauge
	IntentBytes    *aggmetric.Gauge
	LockBytes      *aggmetric.Gauge
	LiveCount      *aggmetric.Gauge
	KeyCount       *aggmetric.Gauge
	ValCount       *aggmetric.Gauge
	RangeKeyCount  *aggmetric.Gauge
	RangeValCount  *aggmetric.Gauge
	IntentCount    *aggmetric.Gauge
	LockCount      *aggmetric.Gauge
	LockAge        *aggmetric.Gauge
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
		RangeKeyBytes:  b.Gauge(metaRangeKeyBytes),
		RangeValBytes:  b.Gauge(metaRangeValBytes),
		TotalBytes:     b.Gauge(metaTotalBytes),
		IntentBytes:    b.Gauge(metaIntentBytes),
		LockBytes:      b.Gauge(metaLockBytes),
		LiveCount:      b.Gauge(metaLiveCount),
		KeyCount:       b.Gauge(metaKeyCount),
		ValCount:       b.Gauge(metaValCount),
		RangeKeyCount:  b.Gauge(metaRangeKeyCount),
		RangeValCount:  b.Gauge(metaRangeValCount),
		IntentCount:    b.Gauge(metaIntentCount),
		LockCount:      b.Gauge(metaLockCount),
		LockAge:        b.Gauge(metaLockAge),
		GcBytesAge:     b.Gauge(metaGcBytesAge),
		SysBytes:       b.Gauge(metaSysBytes),
		SysCount:       b.Gauge(metaSysCount),
		AbortSpanBytes: b.Gauge(metaAbortSpanBytes),
	}
	return sm
}

func newStoreMetrics(histogramWindow time.Duration) *StoreMetrics {
	storeRegistry := metric.NewRegistry()
	rdbBytesIngested := storageLevelCounterSlice(metaRdbBytesIngested)
	rdbLevelSize := storageLevelGaugeSlice(metaRdbLevelSize)
	rdbLevelScore := storageLevelGaugeFloat64Slice(metaRdbLevelScores)

	sm := &StoreMetrics{
		registry:              storeRegistry,
		TenantsStorageMetrics: newTenantsStorageMetrics(),
		LoadSplitterMetrics: &split.LoadSplitterMetrics{
			PopularKeyCount: metric.NewCounter(metaPopularKeyCount),
			NoSplitKeyCount: metric.NewCounter(metaNoSplitKeyCount),
		},

		// Replica metrics.
		ReplicaCount:                  metric.NewGauge(metaReplicaCount),
		ReservedReplicaCount:          metric.NewGauge(metaReservedReplicaCount),
		RaftLeaderCount:               metric.NewGauge(metaRaftLeaderCount),
		RaftLeaderNotLeaseHolderCount: metric.NewGauge(metaRaftLeaderNotLeaseHolderCount),
		RaftLeaderNotFortifiedCount:   metric.NewGauge(metaRaftLeaderNotFortifiedCount),
		RaftLeaderInvalidLeaseCount:   metric.NewGauge(metaRaftLeaderInvalidLeaseCount),
		LeaseHolderCount:              metric.NewGauge(metaLeaseHolderCount),
		QuiescentCount:                metric.NewGauge(metaQuiescentCount),
		AsleepCount:                   metric.NewGauge(metaAsleepCount),
		UninitializedCount:            metric.NewGauge(metaUninitializedCount),
		RaftFlowStateCounts:           raftFlowStateGaugeSlice(),

		// Range metrics.
		RangeCount:                metric.NewGauge(metaRangeCount),
		UnavailableRangeCount:     metric.NewGauge(metaUnavailableRangeCount),
		UnderReplicatedRangeCount: metric.NewGauge(metaUnderReplicatedRangeCount),
		OverReplicatedRangeCount:  metric.NewGauge(metaOverReplicatedRangeCount),
		DecommissioningRangeCount: metric.NewGauge(metaDecommissioningRangeCount),

		// Lease request metrics.
		LeaseRequestSuccessCount: metric.NewCounter(metaLeaseRequestSuccessCount),
		LeaseRequestErrorCount:   metric.NewCounter(metaLeaseRequestErrorCount),
		LeaseRequestLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     metaLeaseRequestLatency,
			Duration:     histogramWindow,
			BucketConfig: metric.IOLatencyBuckets,
		}),
		LeaseTransferSuccessCount:      metric.NewCounter(metaLeaseTransferSuccessCount),
		LeaseTransferErrorCount:        metric.NewCounter(metaLeaseTransferErrorCount),
		LeaseTransferLocksWritten:      metric.NewCounter(metaLeaseTransferLocksWrittenCount),
		LeaseExpirationCount:           metric.NewGauge(metaLeaseExpirationCount),
		LeaseEpochCount:                metric.NewGauge(metaLeaseEpochCount),
		LeaseLeaderCount:               metric.NewGauge(metaLeaseLeaderCount),
		LeaseLivenessCount:             metric.NewGauge(metaLeaseLivenessCount),
		LeaseViolatingPreferencesCount: metric.NewGauge(metaLeaseViolatingPreferencesCount),
		LeaseLessPreferredCount:        metric.NewGauge(metaLeaseLessPreferredCount),

		// Intent resolution metrics.
		ResolveCommitCount: metric.NewCounter(metaResolveCommit),
		ResolveAbortCount:  metric.NewCounter(metaResolveAbort),
		ResolvePoisonCount: metric.NewCounter(metaResolvePoison),

		Capacity:  metric.NewGauge(metaCapacity),
		Available: metric.NewGauge(metaAvailable),
		Used:      metric.NewGauge(metaUsed),
		Reserved:  metric.NewGauge(metaReserved),

		// Rebalancing metrics.
		AverageQueriesPerSecond:    metric.NewGaugeFloat64(metaAverageQueriesPerSecond),
		AverageWritesPerSecond:     metric.NewGaugeFloat64(metaAverageWritesPerSecond),
		AverageRequestsPerSecond:   metric.NewGaugeFloat64(metaAverageRequestsPerSecond),
		AverageReadsPerSecond:      metric.NewGaugeFloat64(metaAverageReadsPerSecond),
		AverageWriteBytesPerSecond: metric.NewGaugeFloat64(metaAverageWriteBytesPerSecond),
		AverageReadBytesPerSecond:  metric.NewGaugeFloat64(metaAverageReadBytesPerSecond),
		AverageCPUNanosPerSecond:   metric.NewGaugeFloat64(metaAverageCPUNanosPerSecond),
		RecentReplicaCPUNanosPerSecond: metric.NewManualWindowHistogram(
			metaRecentReplicaCPUNanosPerSecond,
			metric.ReplicaCPUTimeBuckets.
				GetBucketsFromBucketConfig(),
			true, /* withRotate */
		),
		RecentReplicaQueriesPerSecond: metric.NewManualWindowHistogram(
			metaRecentReplicaQueriesPerSecond,
			metric.ReplicaBatchRequestCountBuckets.
				GetBucketsFromBucketConfig(),
			true, /* withRotate */
		),

		// Follower reads metrics.
		FollowerReadsCount: metric.NewCounter(metaFollowerReadsCount),

		// Server-side transaction metrics.
		CommitWaitsBeforeCommitTrigger:                           metric.NewCounter(metaCommitWaitBeforeCommitTriggerCount),
		WriteEvaluationServerSideRetrySuccess:                    metric.NewCounter(metaWriteEvaluationServerSideRetrySuccess),
		WriteEvaluationServerSideRetryFailure:                    metric.NewCounter(metaWriteEvaluationServerSideRetryFailure),
		ReadEvaluationServerSideRetrySuccess:                     metric.NewCounter(metaReadEvaluationServerSideRetrySuccess),
		ReadEvaluationServerSideRetryFailure:                     metric.NewCounter(metaReadEvaluationServerSideRetryFailure),
		ReadWithinUncertaintyIntervalErrorServerSideRetrySuccess: metric.NewCounter(metaReadWithinUncertaintyIntervalErrorServerSideRetrySuccess),
		ReadWithinUncertaintyIntervalErrorServerSideRetryFailure: metric.NewCounter(metaReadWithinUncertaintyIntervalErrorServerSideRetryFailure),
		OnePhaseCommitSuccess:                                    metric.NewCounter(metaOnePhaseCommitSuccess),
		OnePhaseCommitFailure:                                    metric.NewCounter(metaOnePhaseCommitFailure),

		// Pebble metrics.
		//
		// These are all gauges today because almost all of them are cumulative
		// metrics recorded within Pebble. Internally within Pebble some of
		// these are monotonically increasing counters. There's a bit of a
		// semantic mismatch here because the mechanism of updating the metric
		// is a gauge (eg, we're reading the current value, not incrementing)
		// but the meaning of the metric itself is a counter.
		// TODO(jackson): Reconcile this mismatch so that metrics that are
		// semantically counters are exported as such to Prometheus. See #99922.
		RdbBlockCacheHits:                 metric.NewCounter(metaRdbBlockCacheHits),
		RdbBlockCacheMisses:               metric.NewCounter(metaRdbBlockCacheMisses),
		RdbBlockCacheUsage:                metric.NewGauge(metaRdbBlockCacheUsage),
		RdbBloomFilterPrefixChecked:       metric.NewCounter(metaRdbBloomFilterPrefixChecked),
		RdbBloomFilterPrefixUseful:        metric.NewCounter(metaRdbBloomFilterPrefixUseful),
		RdbMemtableTotalSize:              metric.NewGauge(metaRdbMemtableTotalSize),
		RdbFlushes:                        metric.NewCounter(metaRdbFlushes),
		RdbFlushedBytes:                   metric.NewCounter(metaRdbFlushedBytes),
		RdbCompactions:                    metric.NewCounter(metaRdbCompactions),
		RdbIngestedBytes:                  metric.NewCounter(metaRdbIngestedBytes),
		RdbCompactedBytesRead:             metric.NewCounter(metaRdbCompactedBytesRead),
		RdbCompactedBytesWritten:          metric.NewCounter(metaRdbCompactedBytesWritten),
		RdbTableReadersMemEstimate:        metric.NewGauge(metaRdbTableReadersMemEstimate),
		RdbReadAmplification:              metric.NewGauge(metaRdbReadAmplification),
		RdbNumSSTables:                    metric.NewGauge(metaRdbNumSSTables),
		RdbPendingCompaction:              metric.NewGauge(metaRdbPendingCompaction),
		RdbMarkedForCompactionFiles:       metric.NewGauge(metaRdbMarkedForCompactionFiles),
		RdbKeysRangeKeySets:               metric.NewGauge(metaRdbKeysRangeKeySets),
		RdbKeysTombstones:                 metric.NewGauge(metaRdbKeysTombstones),
		RdbL0BytesFlushed:                 metric.NewCounter(metaRdbL0BytesFlushed),
		RdbL0Sublevels:                    metric.NewGauge(metaRdbL0Sublevels),
		RdbL0NumFiles:                     metric.NewGauge(metaRdbL0NumFiles),
		RdbBytesIngested:                  rdbBytesIngested,
		RdbLevelSize:                      rdbLevelSize,
		RdbLevelScore:                     rdbLevelScore,
		RdbWriteStalls:                    metric.NewGauge(metaRdbWriteStalls),
		RdbWriteStallNanos:                metric.NewGauge(metaRdbWriteStallNanos),
		IterBlockBytes:                    metric.NewCounter(metaBlockBytes),
		IterBlockBytesInCache:             metric.NewCounter(metaBlockBytesInCache),
		IterBlockReadDuration:             metric.NewCounter(metaBlockReadDuration),
		IterExternalSeeks:                 metric.NewCounter(metaIterExternalSeeks),
		IterExternalSteps:                 metric.NewCounter(metaIterExternalSteps),
		IterInternalSeeks:                 metric.NewCounter(metaIterInternalSeeks),
		IterInternalSteps:                 metric.NewCounter(metaIterInternalSteps),
		SingleDelInvariantViolations:      metric.NewCounter(metaStorageSingleDelInvariantViolationCount),
		SingleDelIneffectualCount:         metric.NewCounter(metaStorageSingleDelIneffectualCount),
		SharedStorageBytesRead:            metric.NewCounter(metaSharedStorageBytesRead),
		SharedStorageBytesWritten:         metric.NewCounter(metaSharedStorageBytesWritten),
		BlockLoadsInProgress:              metric.NewGauge(metaBlockLoadsInProgress),
		BlockLoadsQueued:                  metric.NewCounter(metaBlockLoadsQueued),
		SecondaryCacheSize:                metric.NewGauge(metaSecondaryCacheSize),
		SecondaryCacheCount:               metric.NewGauge(metaSecondaryCacheCount),
		SecondaryCacheTotalReads:          metric.NewCounter(metaSecondaryCacheTotalReads),
		SecondaryCacheMultiShardReads:     metric.NewCounter(metaSecondaryCacheMultiShardReads),
		SecondaryCacheMultiBlockReads:     metric.NewCounter(metaSecondaryCacheMultiBlockReads),
		SecondaryCacheReadsWithFullHit:    metric.NewCounter(metaSecondaryCacheReadsWithFullHit),
		SecondaryCacheReadsWithPartialHit: metric.NewCounter(metaSecondaryCacheReadsWithPartialHit),
		SecondaryCacheReadsWithNoHit:      metric.NewCounter(metaSecondaryCacheReadsWithNoHit),
		SecondaryCacheEvictions:           metric.NewCounter(metaSecondaryCacheEvictions),
		SecondaryCacheWriteBackFails:      metric.NewCounter(metaSecondaryCacheWriteBackFailures),
		StorageCompactionsPinnedKeys:      metric.NewCounter(metaStorageCompactionsKeysPinnedCount),
		StorageCompactionsPinnedBytes:     metric.NewCounter(metaStorageCompactionsKeysPinnedBytes),
		StorageCompactionsCancelledCount:  metric.NewCounter(metaStorageCompactionsCancelledCount),
		StorageCompactionsCancelledBytes:  metric.NewCounter(metaStorageCompactionsCancelledBytes),
		StorageCompactionsDuration:        metric.NewCounter(metaStorageCompactionsDuration),
		StorageWriteAmplification:         metric.NewGaugeFloat64(metaStorageWriteAmplification),
		FlushableIngestCount:              metric.NewCounter(metaFlushableIngestCount),
		FlushableIngestTableCount:         metric.NewCounter(metaFlushableIngestTableCount),
		FlushableIngestTableSize:          metric.NewCounter(metaFlushableIngestTableBytes),
		BatchCommitCount:                  metric.NewCounter(metaBatchCommitCount),
		BatchCommitDuration:               metric.NewCounter(metaBatchCommitDuration),
		BatchCommitSemWaitDuration:        metric.NewCounter(metaBatchCommitSemWaitDuration),
		BatchCommitWALQWaitDuration:       metric.NewCounter(metaBatchCommitWALQWaitDuration),
		BatchCommitMemStallDuration:       metric.NewCounter(metaBatchCommitMemStallDuration),
		BatchCommitL0StallDuration:        metric.NewCounter(metaBatchCommitL0StallDuration),
		BatchCommitWALRotWaitDuration:     metric.NewCounter(metaBatchCommitWALRotDuration),
		BatchCommitCommitWaitDuration:     metric.NewCounter(metaBatchCommitCommitWaitDuration),
		SSTableZombieBytes:                metric.NewGauge(metaSSTableZombieBytes),
		SSTableCompressionSnappy:          metric.NewGauge(metaSSTableCompressionSnappy),
		SSTableCompressionZstd:            metric.NewGauge(metaSSTableCompressionZstd),
		SSTableCompressionUnknown:         metric.NewGauge(metaSSTableCompressionUnknown),
		SSTableCompressionNone:            metric.NewGauge(metaSSTableCompressionNone),
		categoryDiskWriteMetrics: pebbleCategoryDiskWriteMetricsContainer{
			registry: storeRegistry,
		},
		WALBytesWritten:              metric.NewCounter(metaWALBytesWritten),
		WALBytesIn:                   metric.NewCounter(metaWALBytesIn),
		WALFailoverSwitchCount:       metric.NewCounter(metaStorageWALFailoverSwitchCount),
		WALFailoverPrimaryDuration:   metric.NewCounter(metaStorageWALFailoverPrimaryDuration),
		WALFailoverSecondaryDuration: metric.NewCounter(metaStorageWALFailoverSecondaryDuration),
		WALFailoverWriteAndSyncLatency: metric.NewManualWindowHistogram(
			metaStorageWALFailoverWriteAndSyncLatency,
			pebble.FsyncLatencyBuckets,
			false, /* withRotate */
		),

		// Ingestion metrics
		IngestCount: metric.NewGauge(metaIngestCount),

		RdbCheckpoints: metric.NewGauge(metaRdbCheckpoints),

		// Disk health metrics.
		DiskSlow:    metric.NewCounter(metaDiskSlow),
		DiskStalled: metric.NewCounter(metaDiskStalled),

		// Range event metrics.
		RangeSplits:                   metric.NewCounter(metaRangeSplits),
		RangeMerges:                   metric.NewCounter(metaRangeMerges),
		RangeAdds:                     metric.NewCounter(metaRangeAdds),
		RangeRemoves:                  metric.NewCounter(metaRangeRemoves),
		RangeSnapshotsGenerated:       metric.NewCounter(metaRangeSnapshotsGenerated),
		RangeSnapshotsAppliedByVoters: metric.NewCounter(metaRangeSnapshotsAppliedByVoters),
		RangeSnapshotsAppliedForInitialUpreplication: metric.NewCounter(metaRangeSnapshotsAppliedForInitialUpreplication),
		RangeSnapshotsAppliedByNonVoters:             metric.NewCounter(metaRangeSnapshotsAppliedByNonVoter),
		RangeSnapshotRcvdBytes:                       metric.NewCounter(metaRangeSnapshotRcvdBytes),
		RangeSnapshotSentBytes:                       metric.NewCounter(metaRangeSnapshotSentBytes),
		RangeSnapshotUnknownRcvdBytes:                metric.NewCounter(metaRangeSnapshotUnknownRcvdBytes),
		RangeSnapshotUnknownSentBytes:                metric.NewCounter(metaRangeSnapshotUnknownSentBytes),
		RangeSnapshotRecoveryRcvdBytes:               metric.NewCounter(metaRangeSnapshotRecoveryRcvdBytes),
		RangeSnapshotRecoverySentBytes:               metric.NewCounter(metaRangeSnapshotRecoverySentBytes),
		RangeSnapshotUpreplicationRcvdBytes:          metric.NewCounter(metaRangeSnapshotUpreplicationRcvdBytes),
		RangeSnapshotUpreplicationSentBytes:          metric.NewCounter(metaRangeSnapshotUpreplicationSentBytes),
		RangeSnapshotRebalancingRcvdBytes:            metric.NewCounter(metaRangeSnapshotRebalancingRcvdBytes),
		RangeSnapshotRebalancingSentBytes:            metric.NewCounter(metaRangeSnapshotRebalancingSentBytes),
		RangeSnapshotRecvFailed:                      metric.NewCounter(metaRangeSnapshotRecvFailed),
		RangeSnapshotRecvUnusable:                    metric.NewCounter(metaRangeSnapshotRecvUnusable),
		RangeSnapShotCrossRegionSentBytes:            metric.NewCounter(metaRangeSnapShotCrossRegionSentBytes),
		RangeSnapShotCrossRegionRcvdBytes:            metric.NewCounter(metaRangeSnapShotCrossRegionRcvdBytes),
		RangeSnapShotCrossZoneSentBytes:              metric.NewCounter(metaRangeSnapShotCrossZoneSentBytes),
		RangeSnapShotCrossZoneRcvdBytes:              metric.NewCounter(metaRangeSnapShotCrossZoneRcvdBytes),
		RangeSnapshotSendQueueLength:                 metric.NewGauge(metaRangeSnapshotSendQueueLength),
		RangeSnapshotRecvQueueLength:                 metric.NewGauge(metaRangeSnapshotRecvQueueLength),
		RangeSnapshotSendInProgress:                  metric.NewGauge(metaRangeSnapshotSendInProgress),
		RangeSnapshotRecvInProgress:                  metric.NewGauge(metaRangeSnapshotRecvInProgress),
		RangeSnapshotSendTotalInProgress:             metric.NewGauge(metaRangeSnapshotSendTotalInProgress),
		RangeSnapshotRecvTotalInProgress:             metric.NewGauge(metaRangeSnapshotRecvTotalInProgress),
		RangeSnapshotSendQueueSize:                   metric.NewGauge(metaRangeSnapshotSendQueueSize),
		RangeSnapshotRecvQueueSize:                   metric.NewGauge(metaRangeSnapshotRecvQueueSize),
		RangeRaftLeaderTransfers:                     metric.NewCounter(metaRangeRaftLeaderTransfers),
		RangeRaftLeaderRemovals:                      metric.NewCounter(metaRangeRaftLeaderRemovals),
		RangeLossOfQuorumRecoveries:                  metric.NewCounter(metaRangeLossOfQuorumRecoveries),
		DelegateSnapshotSendBytes:                    metric.NewCounter(metaDelegateSnapshotSendBytes),
		DelegateSnapshotSuccesses:                    metric.NewCounter(metaDelegateSnapshotSuccesses),
		DelegateSnapshotFailures:                     metric.NewCounter(metaDelegateSnapshotFailures),
		DelegateSnapshotInProgress:                   metric.NewGauge(metaDelegateSnapshotInProgress),

		// Raft processing metrics.
		RaftTicks:                  metric.NewCounter(metaRaftTicks),
		RaftProposalsDropped:       metric.NewCounter(metaRaftProposalsDropped),
		RaftProposalsDroppedLeader: metric.NewCounter(metaRaftProposalsDroppedLeader),
		RaftQuotaPoolPercentUsed: metric.NewHistogram(metric.HistogramOptions{
			Metadata:     metaRaftQuotaPoolPercentUsed,
			Duration:     histogramWindow,
			MaxVal:       100,
			SigFigs:      1,
			BucketConfig: metric.Percent100Buckets,
		}),
		RaftLoadedEntriesBytes:    metric.NewGauge(metaRaftLoadedEntriesBytes),
		RaftWorkingDurationNanos:  metric.NewCounter(metaRaftWorkingDurationNanos),
		RaftTickingDurationNanos:  metric.NewCounter(metaRaftTickingDurationNanos),
		RaftCommandsProposed:      metric.NewCounter(metaRaftCommandsProposed),
		RaftCommandsReproposed:    metric.NewCounter(metaRaftCommandsReproposed),
		RaftCommandsReproposedLAI: metric.NewCounter(metaRaftCommandsReproposedLAI),
		RaftCommandsPending:       metric.NewGauge(metaRaftCommandsPending),
		RaftCommandsApplied:       metric.NewCounter(metaRaftCommandsApplied),
		RaftLogCommitLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     metaRaftLogCommitLatency,
			Duration:     histogramWindow,
			BucketConfig: metric.IOLatencyBuckets,
		}),
		RaftCommandCommitLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     metaRaftCommandCommitLatency,
			Duration:     histogramWindow,
			BucketConfig: metric.IOLatencyBuckets,
		}),
		RaftHandleReadyLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     metaRaftHandleReadyLatency,
			Duration:     histogramWindow,
			BucketConfig: metric.IOLatencyBuckets,
		}),
		RaftApplyCommittedLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     metaRaftApplyCommittedLatency,
			Duration:     histogramWindow,
			BucketConfig: metric.IOLatencyBuckets,
		}),
		RaftReplicationLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaRaftReplicationLatency,
			Duration:     histogramWindow,
			BucketConfig: metric.IOLatencyBuckets,
		}),
		RaftSchedulerLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     metaRaftSchedulerLatency,
			Duration:     histogramWindow,
			BucketConfig: metric.IOLatencyBuckets,
		}),
		RaftTimeoutCampaign:  metric.NewCounter(metaRaftTimeoutCampaign),
		RaftStorageReadBytes: metric.NewCounter(metaRaftStorageReadBytes),
		RaftStorageError:     metric.NewCounter(metaRaftStorageError),

		// Raft message metrics.
		RaftRcvdMessages: [maxRaftMsgType + 1]*metric.Counter{
			raftpb.MsgProp:              metric.NewCounter(metaRaftRcvdProp),
			raftpb.MsgApp:               metric.NewCounter(metaRaftRcvdApp),
			raftpb.MsgAppResp:           metric.NewCounter(metaRaftRcvdAppResp),
			raftpb.MsgVote:              metric.NewCounter(metaRaftRcvdVote),
			raftpb.MsgVoteResp:          metric.NewCounter(metaRaftRcvdVoteResp),
			raftpb.MsgPreVote:           metric.NewCounter(metaRaftRcvdPreVote),
			raftpb.MsgPreVoteResp:       metric.NewCounter(metaRaftRcvdPreVoteResp),
			raftpb.MsgSnap:              metric.NewCounter(metaRaftRcvdSnap),
			raftpb.MsgHeartbeat:         metric.NewCounter(metaRaftRcvdHeartbeat),
			raftpb.MsgHeartbeatResp:     metric.NewCounter(metaRaftRcvdHeartbeatResp),
			raftpb.MsgTransferLeader:    metric.NewCounter(metaRaftRcvdTransferLeader),
			raftpb.MsgTimeoutNow:        metric.NewCounter(metaRaftRcvdTimeoutNow),
			raftpb.MsgFortifyLeader:     metric.NewCounter(metaRaftRcvdFortifyLeader),
			raftpb.MsgFortifyLeaderResp: metric.NewCounter(metaRaftRcvdFortifyLeaderResp),
			raftpb.MsgDeFortifyLeader:   metric.NewCounter(metaRaftRcvdDeFortifyLeader),
		},
		RaftRcvdDropped:          metric.NewCounter(metaRaftRcvdDropped),
		RaftRcvdDroppedBytes:     metric.NewCounter(metaRaftRcvdDroppedBytes),
		RaftRcvdQueuedBytes:      metric.NewGauge(metaRaftRcvdQueuedBytes),
		RaftRcvdSteppedBytes:     metric.NewCounter(metaRaftRcvdSteppedBytes),
		RaftRcvdBytes:            metric.NewCounter(metaRaftRcvdBytes),
		RaftRcvdCrossRegionBytes: metric.NewCounter(metaRaftRcvdCrossRegionBytes),
		RaftRcvdCrossZoneBytes:   metric.NewCounter(metaRaftRcvdCrossZoneBytes),
		RaftSentBytes:            metric.NewCounter(metaRaftSentBytes),
		RaftSentCrossRegionBytes: metric.NewCounter(metaRaftSentCrossRegionBytes),
		RaftSentCrossZoneBytes:   metric.NewCounter(metaRaftSentCrossZoneBytes),

		// Raft log metrics.
		RaftLogFollowerBehindCount: metric.NewGauge(metaRaftLogFollowerBehindCount),
		RaftLogTruncated:           metric.NewCounter(metaRaftLogTruncated),

		RaftPausedFollowerCount:       metric.NewGauge(metaRaftFollowerPaused),
		RaftPausedFollowerDroppedMsgs: metric.NewCounter(metaRaftPausedFollowerDroppedMsgs),
		IOOverload:                    metric.NewGaugeFloat64(metaIOOverload),

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
		LeaseQueueSuccesses:                       metric.NewCounter(metaLeaseQueueSuccesses),
		LeaseQueueFailures:                        metric.NewCounter(metaLeaseQueueFailures),
		LeaseQueuePending:                         metric.NewGauge(metaLeaseQueuePending),
		LeaseQueueProcessingNanos:                 metric.NewCounter(metaLeaseQueueProcessingNanos),
		LeaseQueuePurgatory:                       metric.NewGauge(metaLeaseQueuePurgatory),
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
		GCNumRangeKeysAffected:       metric.NewCounter(metaGCNumRangeKeysAffected),
		GCIntentsConsidered:          metric.NewCounter(metaGCIntentsConsidered),
		GCIntentTxns:                 metric.NewCounter(metaGCIntentTxns),
		GCTransactionSpanScanned:     metric.NewCounter(metaGCTransactionSpanScanned),
		GCTransactionSpanGCAborted:   metric.NewCounter(metaGCTransactionSpanGCAborted),
		GCTransactionSpanGCCommitted: metric.NewCounter(metaGCTransactionSpanGCCommitted),
		GCTransactionSpanGCStaging:   metric.NewCounter(metaGCTransactionSpanGCStaging),
		GCTransactionSpanGCPending:   metric.NewCounter(metaGCTransactionSpanGCPending),
		GCTransactionSpanGCPrepared:  metric.NewCounter(metaGCTransactionSpanGCPrepared),
		GCAbortSpanScanned:           metric.NewCounter(metaGCAbortSpanScanned),
		GCAbortSpanConsidered:        metric.NewCounter(metaGCAbortSpanConsidered),
		GCAbortSpanGCNum:             metric.NewCounter(metaGCAbortSpanGCNum),
		GCPushTxn:                    metric.NewCounter(metaGCPushTxn),
		GCResolveTotal:               metric.NewCounter(metaGCResolveTotal),
		GCResolveSuccess:             metric.NewCounter(metaGCResolveSuccess),
		GCResolveFailed:              metric.NewCounter(metaGCResolveFailed),
		GCTxnIntentsResolveFailed:    metric.NewCounter(metaGCTxnIntentsResolveFailed),
		GCUsedClearRange:             metric.NewCounter(metaGCUsedClearRange),
		GCFailedClearRange:           metric.NewCounter(metaGCFailedClearRange),
		GCEnqueueHighPriority:        metric.NewCounter(metaGCEnqueueHighPriority),

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
		LatchWaitDurations: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     metaLatchConflictWaitDurations,
			Duration:     histogramWindow,
			BucketConfig: metric.IOLatencyBuckets,
		}),

		// Closed timestamp metrics.
		ClosedTimestampMaxBehindNanos: metric.NewGauge(metaClosedTimestampMaxBehindNanos),

		// Replica circuit breaker.
		ReplicaCircuitBreakerCurTripped: metric.NewGauge(metaReplicaCircuitBreakerCurTripped),
		ReplicaCircuitBreakerCumTripped: metric.NewCounter(metaReplicaCircuitBreakerCumTripped),

		// Replica batch evaluation.
		ReplicaReadBatchEvaluationLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     metaReplicaReadBatchEvaluationLatency,
			Duration:     histogramWindow,
			BucketConfig: metric.IOLatencyBuckets,
		}),
		ReplicaWriteBatchEvaluationLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     metaReplicaWriteBatchEvaluationLatency,
			Duration:     histogramWindow,
			BucketConfig: metric.IOLatencyBuckets,
		}),
		FlushUtilization: metric.NewGaugeFloat64(metaStorageFlushUtilization),
		FsyncLatency: metric.NewManualWindowHistogram(
			metaStorageFsyncLatency,
			pebble.FsyncLatencyBuckets,
			false, /* withRotate */
		),

		ReplicaReadBatchDroppedLatchesBeforeEval: metric.NewCounter(metaReplicaReadBatchDroppedLatchesBeforeEval),
		ReplicaReadBatchWithoutInterleavingIter:  metric.NewCounter(metaReplicaReadBatchWithoutInterleavingIter),

		DiskReadBytes:              metric.NewCounter(metaDiskReadBytes),
		DiskReadCount:              metric.NewCounter(metaDiskReadCount),
		DiskReadTime:               metric.NewCounter(metaDiskReadTime),
		DiskWriteBytes:             metric.NewCounter(metaDiskWriteBytes),
		DiskWriteCount:             metric.NewCounter(metaDiskWriteCount),
		DiskWriteTime:              metric.NewCounter(metaDiskWriteTime),
		DiskIOTime:                 metric.NewCounter(metaDiskIOTime),
		DiskWeightedIOTime:         metric.NewCounter(metaDiskWeightedIOTime),
		DiskIopsInProgress:         metric.NewGauge(metaDiskIopsInProgress),
		DiskReadMaxBytesPerSecond:  metric.NewGauge(metaDiskReadMaxBytesPerSecond),
		DiskWriteMaxBytesPerSecond: metric.NewGauge(metaDiskWriteMaxBytesPerSecond),

		// Estimated MVCC stats in split.
		SplitsWithEstimatedStats:     metric.NewCounter(metaSplitEstimatedStats),
		SplitEstimatedTotalBytesDiff: metric.NewCounter(metaSplitEstimatedTotalBytesDiff),
	}
	sm.categoryIterMetrics.init(storeRegistry)

	storeRegistry.AddMetricStruct(sm)
	storeRegistry.AddMetricStruct(sm.LoadSplitterMetrics)
	for i := range sm.categoryIterMetrics.metrics {
		storeRegistry.AddMetricStruct(&sm.categoryIterMetrics.metrics[i])
	}

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
	tm.RangeKeyBytes.Inc(delta.RangeKeyBytes)
	tm.RangeValBytes.Inc(delta.RangeValBytes)
	tm.TotalBytes.Inc(delta.Total())
	tm.IntentBytes.Inc(delta.IntentBytes)
	tm.LockBytes.Inc(delta.LockBytes)
	tm.LiveCount.Inc(delta.LiveCount)
	tm.KeyCount.Inc(delta.KeyCount)
	tm.ValCount.Inc(delta.ValCount)
	tm.RangeKeyCount.Inc(delta.RangeKeyCount)
	tm.RangeValCount.Inc(delta.RangeValCount)
	tm.IntentCount.Inc(delta.IntentCount)
	tm.LockCount.Inc(delta.LockCount)
	tm.LockAge.Inc(delta.LockAge)
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
	sm.RdbTableReadersMemEstimate.Update(m.FileCache.Size)
	sm.RdbReadAmplification.Update(int64(m.ReadAmp()))
	sm.RdbPendingCompaction.Update(int64(m.Compact.EstimatedDebt))
	sm.RdbMarkedForCompactionFiles.Update(int64(m.Compact.MarkedFiles))
	sm.RdbKeysRangeKeySets.Update(int64(m.Keys.RangeKeySetsCount))
	sm.RdbKeysTombstones.Update(int64(m.Keys.TombstoneCount))
	sm.RdbNumSSTables.Update(m.NumSSTables())
	sm.RdbWriteStalls.Update(m.WriteStallCount)
	sm.RdbWriteStallNanos.Update(m.WriteStallDuration.Nanoseconds())
	sm.DiskSlow.Update(m.DiskSlowCount)
	sm.DiskStalled.Update(m.DiskStallCount)
	sm.IterBlockBytes.Update(int64(m.Iterator.BlockBytes))
	sm.IterBlockBytesInCache.Update(int64(m.Iterator.BlockBytesInCache))
	sm.IterBlockReadDuration.Update(int64(m.Iterator.BlockReadDuration))
	sm.IterExternalSeeks.Update(int64(m.Iterator.ExternalSeeks))
	sm.IterExternalSteps.Update(int64(m.Iterator.ExternalSteps))
	sm.IterInternalSeeks.Update(int64(m.Iterator.InternalSeeks))
	sm.IterInternalSteps.Update(int64(m.Iterator.InternalSteps))
	sm.StorageCompactionsPinnedKeys.Update(int64(m.Snapshots.PinnedKeys))
	sm.StorageCompactionsPinnedBytes.Update(int64(m.Snapshots.PinnedSize))
	sm.StorageCompactionsCancelledCount.Update(m.Compact.CancelledCount)
	sm.StorageCompactionsCancelledBytes.Update(m.Compact.CancelledBytes)
	sm.StorageCompactionsDuration.Update(int64(m.Compact.Duration))
	sm.SingleDelInvariantViolations.Update(m.SingleDelInvariantViolationCount)
	sm.SingleDelIneffectualCount.Update(m.SingleDelIneffectualCount)
	sm.SharedStorageBytesRead.Update(m.SharedStorageReadBytes)
	sm.SharedStorageBytesWritten.Update(m.SharedStorageWriteBytes)
	sm.BlockLoadsInProgress.Update(m.BlockLoadsInProgress)
	sm.BlockLoadsQueued.Update(m.BlockLoadsQueued)
	sm.SecondaryCacheSize.Update(m.SecondaryCacheMetrics.Size)
	sm.SecondaryCacheCount.Update(m.SecondaryCacheMetrics.Count)
	sm.SecondaryCacheTotalReads.Update(m.SecondaryCacheMetrics.TotalReads)
	sm.SecondaryCacheMultiShardReads.Update(m.SecondaryCacheMetrics.MultiShardReads)
	sm.SecondaryCacheMultiBlockReads.Update(m.SecondaryCacheMetrics.MultiBlockReads)
	sm.SecondaryCacheReadsWithFullHit.Update(m.SecondaryCacheMetrics.ReadsWithFullHit)
	sm.SecondaryCacheReadsWithPartialHit.Update(m.SecondaryCacheMetrics.ReadsWithPartialHit)
	sm.SecondaryCacheReadsWithNoHit.Update(m.SecondaryCacheMetrics.ReadsWithNoHit)
	sm.SecondaryCacheEvictions.Update(m.SecondaryCacheMetrics.Evictions)
	sm.SecondaryCacheWriteBackFails.Update(m.SecondaryCacheMetrics.WriteBackFailures)
	sm.RdbL0Sublevels.Update(int64(m.Levels[0].Sublevels))
	sm.RdbL0NumFiles.Update(m.Levels[0].NumFiles)
	sm.RdbL0BytesFlushed.Update(int64(m.Levels[0].BytesFlushed))
	sm.FlushableIngestCount.Update(int64(m.Flush.AsIngestCount))
	sm.FlushableIngestTableCount.Update(int64(m.Flush.AsIngestTableCount))
	sm.FlushableIngestTableSize.Update(int64(m.Flush.AsIngestBytes))
	sm.IngestCount.Update(int64(m.Ingest.Count))
	// NB: `UpdateIfHigher` is used here since there is a race in pebble where
	// sometimes the WAL is rotated but metrics are retrieved prior to the update
	// to BytesIn to account for the previous WAL.
	sm.WALBytesWritten.UpdateIfHigher(int64(m.WAL.BytesWritten))
	sm.WALBytesIn.Update(int64(m.WAL.BytesIn))
	sm.WALFailoverSwitchCount.Update(m.WAL.Failover.DirSwitchCount)
	sm.WALFailoverPrimaryDuration.Update(m.WAL.Failover.PrimaryWriteDuration.Nanoseconds())
	sm.WALFailoverSecondaryDuration.Update(m.WAL.Failover.SecondaryWriteDuration.Nanoseconds())
	sm.BatchCommitCount.Update(int64(m.BatchCommitStats.Count))
	sm.BatchCommitDuration.Update(int64(m.BatchCommitStats.TotalDuration))
	sm.BatchCommitSemWaitDuration.Update(int64(m.BatchCommitStats.SemaphoreWaitDuration))
	sm.BatchCommitWALQWaitDuration.Update(int64(m.BatchCommitStats.WALQueueWaitDuration))
	sm.BatchCommitMemStallDuration.Update(int64(m.BatchCommitStats.MemTableWriteStallDuration))
	sm.BatchCommitL0StallDuration.Update(int64(m.BatchCommitStats.L0ReadAmpWriteStallDuration))
	sm.BatchCommitWALRotWaitDuration.Update(int64(m.BatchCommitStats.WALRotationDuration))
	sm.BatchCommitCommitWaitDuration.Update(int64(m.BatchCommitStats.CommitWaitDuration))
	sm.SSTableZombieBytes.Update(int64(m.Table.ZombieSize))
	sm.SSTableCompressionSnappy.Update(m.Table.CompressedCountSnappy)
	sm.SSTableCompressionZstd.Update(m.Table.CompressedCountZstd)
	sm.SSTableCompressionUnknown.Update(m.Table.CompressedCountUnknown)
	sm.SSTableCompressionNone.Update(m.Table.CompressedCountNone)
	sm.categoryIterMetrics.update(m.CategoryStats)
	sm.categoryDiskWriteMetrics.update(m.DiskWriteStats)

	totalWriteAmp := float64(0)
	for level, stats := range m.Levels {
		sm.RdbBytesIngested[level].Update(int64(stats.BytesIngested))
		sm.RdbLevelSize[level].Update(stats.Size)
		sm.RdbLevelScore[level].Update(stats.Score)
		totalWriteAmp += stats.WriteAmp()
	}
	sm.StorageWriteAmplification.Update(totalWriteAmp)
}

// updateCrossLocalityMetricsOnSnapshotSent updates cross-locality related store
// metrics when outgoing snapshots are sent to the outgoingSnapshotStream. The
// metrics being updated include 1. cross-region metrics, which monitor
// activities across different regions, and 2. cross-zone metrics, which monitor
// activities across different zones within the same region or in cases where
// region tiers are not configured.
func (sm *StoreMetrics) updateCrossLocalityMetricsOnSnapshotSent(
	comparisonResult roachpb.LocalityComparisonType, inc int64,
) {
	switch comparisonResult {
	case roachpb.LocalityComparisonType_CROSS_REGION:
		sm.RangeSnapShotCrossRegionSentBytes.Inc(inc)
	case roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE:
		sm.RangeSnapShotCrossZoneSentBytes.Inc(inc)
	}
}

// updateCrossLocalityMetricsOnSnapshotRcvd updates cross-locality related store
// metrics when receiving SnapshotRequests through streaming and constructing
// incoming snapshots. The metrics being updated include 1. cross-region
// metrics, which monitor activities across different regions, and 2. cross-zone
// metrics, which monitor activities across different zones within the same
// region or in cases where region tiers are not configured.
func (sm *StoreMetrics) updateCrossLocalityMetricsOnSnapshotRcvd(
	comparisonResult roachpb.LocalityComparisonType, inc int64,
) {
	switch comparisonResult {
	case roachpb.LocalityComparisonType_CROSS_REGION:
		sm.RangeSnapShotCrossRegionRcvdBytes.Inc(inc)
	case roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE:
		sm.RangeSnapShotCrossZoneRcvdBytes.Inc(inc)
	}
}

// updateCrossLocalityMetricsOnIncomingRaftMsg updates store metrics for raft
// messages that have been received via HandleRaftRequest. In the cases of
// messages containing heartbeats or heartbeat_resps, they capture the byte
// count of requests with coalesced heartbeats before any uncoalescing happens.
// The metrics being updated include 1. total byte count of messages received 2.
// cross-region metrics, which monitor activities across different regions, and
// 3. cross-zone metrics, which monitor activities across different zones within
// the same region or in cases where region tiers are not configured.
func (sm *StoreMetrics) updateCrossLocalityMetricsOnIncomingRaftMsg(
	comparisonResult roachpb.LocalityComparisonType, msgSize int64,
) {
	sm.RaftRcvdBytes.Inc(msgSize)
	switch comparisonResult {
	case roachpb.LocalityComparisonType_CROSS_REGION:
		sm.RaftRcvdCrossRegionBytes.Inc(msgSize)
	case roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE:
		sm.RaftRcvdCrossZoneBytes.Inc(msgSize)
	}
}

// updateCrossLocalityMetricsOnOutgoingRaftMsg updates store metrics for raft
// messages that are about to be sent via raftSendQueue. In the cases of
// messages containing heartbeats or heartbeat_resps, they capture the byte
// count of requests with coalesced heartbeats. The metrics being updated
// include 1. total byte count of messages sent 2. cross-region metrics, which
// monitor activities across different regions, and 3. cross-zone metrics, which
// monitor activities across different zones within the same region or in cases
// where region tiers are not configured. Note that these metrics may include
// messages that get dropped by `SendAsync` due to a full outgoing queue.
func (sm *StoreMetrics) updateCrossLocalityMetricsOnOutgoingRaftMsg(
	comparisonResult roachpb.LocalityComparisonType, msgSize int64,
) {
	sm.RaftSentBytes.Inc(msgSize)
	switch comparisonResult {
	case roachpb.LocalityComparisonType_CROSS_REGION:
		sm.RaftSentCrossRegionBytes.Inc(msgSize)
	case roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE:
		sm.RaftSentCrossZoneBytes.Inc(msgSize)
	}
}

func (sm *StoreMetrics) updateEnvStats(stats fs.EnvStats) {
	sm.EncryptionAlgorithm.Update(int64(stats.EncryptionType))
}

func (sm *StoreMetrics) updateDiskStats(
	ctx context.Context,
	rollingStats disk.StatsWindow,
	cumulativeStats disk.Stats,
	cumulativeStatsErr error,
) {
	if cumulativeStatsErr == nil {
		sm.DiskReadCount.Update(int64(cumulativeStats.ReadsCount))
		sm.DiskReadBytes.Update(int64(cumulativeStats.BytesRead()))
		sm.DiskReadTime.Update(int64(cumulativeStats.ReadsDuration))
		sm.DiskWriteCount.Update(int64(cumulativeStats.WritesCount))
		sm.DiskWriteBytes.Update(int64(cumulativeStats.BytesWritten()))
		sm.DiskWriteTime.Update(int64(cumulativeStats.WritesDuration))
		sm.DiskIOTime.Update(int64(cumulativeStats.CumulativeDuration))
		sm.DiskWeightedIOTime.Update(int64(cumulativeStats.WeightedIODuration))
		sm.DiskIopsInProgress.Update(int64(cumulativeStats.InProgressCount))
	} else {
		// Don't update cumulative stats to the useless zero value.
		log.Errorf(ctx, "not updating cumulative stats due to %s", cumulativeStatsErr)
	}
	maxRollingStats := rollingStats.Max()
	// maxRollingStats is computed as the change in stats every 100ms, so we
	// scale them to represent the change in stats every 1s.
	perSecondMultiplier := int(time.Second / disk.DefaultDiskStatsPollingInterval)
	sm.DiskReadMaxBytesPerSecond.Update(int64(maxRollingStats.BytesRead() * perSecondMultiplier))
	sm.DiskWriteMaxBytesPerSecond.Update(int64(maxRollingStats.BytesWritten() * perSecondMultiplier))
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
	sm.LeaseTransferLocksWritten.Inc(int64(metric.LeaseTransferLocksWritten))
	metric.LeaseTransferLocksWritten = 0

	sm.ResolveCommitCount.Inc(int64(metric.ResolveCommit))
	metric.ResolveCommit = 0
	sm.ResolveAbortCount.Inc(int64(metric.ResolveAbort))
	metric.ResolveAbort = 0
	sm.ResolvePoisonCount.Inc(int64(metric.ResolvePoison))
	metric.ResolvePoison = 0

	sm.AddSSTableAsWrites.Inc(int64(metric.AddSSTableAsWrites))
	metric.AddSSTableAsWrites = 0

	sm.SplitsWithEstimatedStats.Inc(int64(metric.SplitsWithEstimatedStats))
	metric.SplitsWithEstimatedStats = 0

	sm.SplitEstimatedTotalBytesDiff.Inc(int64(metric.SplitEstimatedTotalBytesDiff))
	metric.SplitEstimatedTotalBytesDiff = 0

	if metric != (result.Metrics{}) {
		log.Fatalf(ctx, "unhandled fields in metrics result: %+v", metric)
	}
}

func raftFlowStateGaugeSlice() [tracker.StateCount]*metric.Gauge {
	// NB: explicitly initialize each index so that this does not depend on int
	// values of StateProbe, StateReplicate and StateSnapshot.
	var gauges [tracker.StateCount]*metric.Gauge
	gauges[tracker.StateProbe] = metric.NewGauge(metaRaftFlowsProbe)
	gauges[tracker.StateReplicate] = metric.NewGauge(metaRaftFlowsReplicate)
	gauges[tracker.StateSnapshot] = metric.NewGauge(metaRaftFlowsSnapshot)
	return gauges
}

func storageLevelMetricMetadata(
	name, helpTpl, measurement string, unit metric.Unit,
) [7]metric.Metadata {
	var sl [7]metric.Metadata
	for i := range sl {
		sl[i] = metric.Metadata{
			Name:        fmt.Sprintf("storage.l%d-%s", i, name),
			Help:        fmt.Sprintf(helpTpl, i),
			Measurement: measurement,
			Unit:        unit,
		}
	}
	return sl
}

func storageLevelGaugeSlice(sl [7]metric.Metadata) [7]*metric.Gauge {
	var gs [7]*metric.Gauge
	for i := range sl {
		gs[i] = metric.NewGauge(sl[i])
	}
	return gs
}

func storageLevelGaugeFloat64Slice(sl [7]metric.Metadata) [7]*metric.GaugeFloat64 {
	var gs [7]*metric.GaugeFloat64
	for i := range sl {
		gs[i] = metric.NewGaugeFloat64(sl[i])
	}
	return gs
}

func storageLevelCounterSlice(sl [7]metric.Metadata) [7]*metric.Counter {
	var gs [7]*metric.Counter
	for i := range sl {
		gs[i] = metric.NewCounter(sl[i])
	}
	return gs
}

func (sm *StoreMetrics) getCounterForRangeLogEventType(
	eventType kvserverpb.RangeLogEventType,
) *metric.Counter {
	switch eventType {
	case kvserverpb.RangeLogEventType_split:
		return sm.RangeSplits
	case kvserverpb.RangeLogEventType_merge:
		return sm.RangeMerges
	case kvserverpb.RangeLogEventType_add_voter:
		return sm.RangeAdds
	case kvserverpb.RangeLogEventType_remove_voter:
		return sm.RangeRemoves
	default:
		return nil
	}
}

type pebbleCategoryIterMetrics struct {
	IterBlockBytes          *metric.Counter
	IterBlockBytesInCache   *metric.Counter
	IterBlockReadLatencySum *metric.Counter
}

func makePebbleCategorizedIterMetrics(category block.Category) pebbleCategoryIterMetrics {
	metaBlockBytes := metric.Metadata{
		Name:        fmt.Sprintf("storage.iterator.category-%s.block-load.bytes", category),
		Help:        "Bytes loaded by storage sstable iterators (possibly cached).",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaBlockBytesInCache := metric.Metadata{
		Name:        fmt.Sprintf("storage.iterator.category-%s.block-load.cached-bytes", category),
		Help:        "Bytes loaded by storage sstable iterators from the block cache",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaBlockReadLatencySum := metric.Metadata{
		Name:        fmt.Sprintf("storage.iterator.category-%s.block-load.latency-sum", category),
		Help:        "Cumulative latency for loading bytes not in the block cache, by storage sstable iterators",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	return pebbleCategoryIterMetrics{
		IterBlockBytes:          metric.NewCounter(metaBlockBytes),
		IterBlockBytesInCache:   metric.NewCounter(metaBlockBytesInCache),
		IterBlockReadLatencySum: metric.NewCounter(metaBlockReadLatencySum),
	}
}

// MetricStruct implements the metric.Struct interface.
func (m *pebbleCategoryIterMetrics) MetricStruct() {}

func (m *pebbleCategoryIterMetrics) update(stats block.CategoryStats) {
	m.IterBlockBytes.Update(int64(stats.BlockBytes))
	m.IterBlockBytesInCache.Update(int64(stats.BlockBytesInCache))
	m.IterBlockReadLatencySum.Update(int64(stats.BlockReadDuration))
}

type pebbleCategoryIterMetricsContainer struct {
	registry *metric.Registry
	// metrics slice for all categories; can be directly indexed by block.Category.
	metrics []pebbleCategoryIterMetrics
}

func (m *pebbleCategoryIterMetricsContainer) init(registry *metric.Registry) {
	m.registry = registry
	categories := block.Categories()
	m.metrics = make([]pebbleCategoryIterMetrics, len(categories))
	for _, c := range categories {
		m.metrics[c] = makePebbleCategorizedIterMetrics(c)
	}
}

func (m *pebbleCategoryIterMetricsContainer) update(stats []block.CategoryStatsAggregate) {
	for _, s := range stats {
		m.metrics[s.Category].update(s.CategoryStats)
	}
}

// MetricStruct implements metrics.Struct.
func (m *pebbleCategoryIterMetricsContainer) MetricStruct() {}

type pebbleCategoryDiskWriteMetrics struct {
	BytesWritten *metric.Counter
}

func makePebbleCategorizedWriteMetrics(
	category vfs.DiskWriteCategory,
) *pebbleCategoryDiskWriteMetrics {
	metaDiskBytesWritten := metric.Metadata{
		Name:        fmt.Sprintf("storage.category-%s.bytes-written", category),
		Help:        "Bytes written to disk",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	return &pebbleCategoryDiskWriteMetrics{BytesWritten: metric.NewCounter(metaDiskBytesWritten)}
}

// MetricStruct implements the metric.Struct interface.
func (m *pebbleCategoryDiskWriteMetrics) MetricStruct() {}

func (m *pebbleCategoryDiskWriteMetrics) update(stats vfs.DiskWriteStatsAggregate) {
	m.BytesWritten.Update(int64(stats.BytesWritten))
}

type pebbleCategoryDiskWriteMetricsContainer struct {
	registry   *metric.Registry
	metricsMap syncutil.Map[vfs.DiskWriteCategory, pebbleCategoryDiskWriteMetrics]
}

func (m *pebbleCategoryDiskWriteMetricsContainer) update(stats []vfs.DiskWriteStatsAggregate) {
	for _, s := range stats {
		cm, ok := m.metricsMap.Load(s.Category)
		if !ok {
			cm, ok = m.metricsMap.LoadOrStore(s.Category, makePebbleCategorizedWriteMetrics(s.Category))
			if !ok {
				m.registry.AddMetricStruct(cm)
			}
		}
		cm.update(s)
	}
}
