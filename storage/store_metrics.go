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
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/syncutil"
)

var (
	metaReplicaCount                 = metric.Metadata{Name: "replicas"}
	metaReservedReplicaCount         = metric.Metadata{Name: "replicas.reserved"}
	metaLeaderRangeCount             = metric.Metadata{Name: "ranges.leader"}
	metaReplicatedRangeCount         = metric.Metadata{Name: "ranges.replicated"}
	metaReplicationPendingRangeCount = metric.Metadata{Name: "ranges.replication-pending"}
	metaAvailableRangeCount          = metric.Metadata{Name: "ranges.available"}
	metaLeaseRequestSuccessCount     = metric.Metadata{Name: "leases.success"}
	metaLeaseRequestErrorCount       = metric.Metadata{Name: "leases.error"}
	metaLiveBytes                    = metric.Metadata{Name: "livebytes"}
	metaKeyBytes                     = metric.Metadata{Name: "keybytes"}
	metaValBytes                     = metric.Metadata{Name: "valbytes"}
	metaIntentBytes                  = metric.Metadata{Name: "intentbytes"}
	metaLiveCount                    = metric.Metadata{Name: "livecount"}
	metaKeyCount                     = metric.Metadata{Name: "keycount"}
	metaValCount                     = metric.Metadata{Name: "valcount"}
	metaIntentCount                  = metric.Metadata{Name: "intentcount"}
	metaIntentAge                    = metric.Metadata{Name: "intentage"}
	metaGcBytesAge                   = metric.Metadata{Name: "gcbytesage"}
	metaLastUpdateNanos              = metric.Metadata{Name: "lastupdatenanos"}
	metaCapacity                     = metric.Metadata{Name: "capacity"}
	metaAvailable                    = metric.Metadata{Name: "capacity.available"}
	metaReserved                     = metric.Metadata{Name: "capacity.reserved"}
	metaSysBytes                     = metric.Metadata{Name: "sysbytes"}
	metaSysCount                     = metric.Metadata{Name: "syscount"}

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

	// Range event metrics.
	metaRangeSplits                     = metric.Metadata{Name: "range.splits"}
	metaRangeAdds                       = metric.Metadata{Name: "range.adds"}
	metaRangeRemoves                    = metric.Metadata{Name: "range.removes"}
	metaRangeSnapshotsGenerated         = metric.Metadata{Name: "range.snapshots.generated"}
	metaRangeSnapshotsNormalApplied     = metric.Metadata{Name: "range.snapshots.normal-applied"}
	metaRangeSnapshotsPreemptiveApplied = metric.Metadata{Name: "range.snapshots.preemptive-applied"}

	// Raft processing metrics.
	metaRaftSelectDurationNanos  = metric.Metadata{Name: "process-raft.waitingnanos"}
	metaRaftWorkingDurationNanos = metric.Metadata{Name: "process-raft.workingnanos"}
	metaRaftTickingDurationNanos = metric.Metadata{Name: "process-raft.tickingnanos"}
)

// StoreMetrics is the set of metrics for a given store.
type StoreMetrics struct {
	registry *metric.Registry

	// Range data metrics.
	ReplicaCount                 *metric.Counter // Does not include reserved replicas.
	ReservedReplicaCount         *metric.Counter
	LeaderRangeCount             *metric.Gauge
	ReplicatedRangeCount         *metric.Gauge
	ReplicationPendingRangeCount *metric.Gauge
	AvailableRangeCount          *metric.Gauge

	// Lease data metrics.
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

	// Range event metrics.
	RangeSplits                     *metric.Counter
	RangeAdds                       *metric.Counter
	RangeRemoves                    *metric.Counter
	RangeSnapshotsGenerated         *metric.Counter
	RangeSnapshotsNormalApplied     *metric.Counter
	RangeSnapshotsPreemptiveApplied *metric.Counter

	// Raft processing metrics.
	RaftSelectDurationNanos  *metric.Counter
	RaftWorkingDurationNanos *metric.Counter
	RaftTickingDurationNanos *metric.Counter

	// Stats for efficient merges.
	// TODO(mrtracy): This should be removed as part of #4465. This is only
	// maintained to keep the current structure of StatusSummaries; it would be
	// better to convert the Gauges above into counters which are adjusted
	// accordingly.
	mu    syncutil.Mutex
	stats enginepb.MVCCStats
}

func newStoreMetrics() *StoreMetrics {
	storeRegistry := metric.NewRegistry()
	sm := &StoreMetrics{
		registry:                     storeRegistry,
		ReplicaCount:                 metric.NewCounter(metaReplicaCount),
		ReservedReplicaCount:         metric.NewCounter(metaReservedReplicaCount),
		LeaderRangeCount:             metric.NewGauge(metaLeaderRangeCount),
		ReplicatedRangeCount:         metric.NewGauge(metaReplicatedRangeCount),
		ReplicationPendingRangeCount: metric.NewGauge(metaReplicationPendingRangeCount),
		AvailableRangeCount:          metric.NewGauge(metaAvailableRangeCount),
		LeaseRequestSuccessCount:     metric.NewCounter(metaLeaseRequestSuccessCount),
		LeaseRequestErrorCount:       metric.NewCounter(metaLeaseRequestErrorCount),
		LiveBytes:                    metric.NewGauge(metaLiveBytes),
		KeyBytes:                     metric.NewGauge(metaKeyBytes),
		ValBytes:                     metric.NewGauge(metaValBytes),
		IntentBytes:                  metric.NewGauge(metaIntentBytes),
		LiveCount:                    metric.NewGauge(metaLiveCount),
		KeyCount:                     metric.NewGauge(metaKeyCount),
		ValCount:                     metric.NewGauge(metaValCount),
		IntentCount:                  metric.NewGauge(metaIntentCount),
		IntentAge:                    metric.NewGauge(metaIntentAge),
		GcBytesAge:                   metric.NewGauge(metaGcBytesAge),
		LastUpdateNanos:              metric.NewGauge(metaLastUpdateNanos),
		Capacity:                     metric.NewGauge(metaCapacity),
		Available:                    metric.NewGauge(metaAvailable),
		Reserved:                     metric.NewCounter(metaReserved),
		SysBytes:                     metric.NewGauge(metaSysBytes),
		SysCount:                     metric.NewGauge(metaSysCount),

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

		// Range event metrics.
		RangeSplits:                     metric.NewCounter(metaRangeSplits),
		RangeAdds:                       metric.NewCounter(metaRangeAdds),
		RangeRemoves:                    metric.NewCounter(metaRangeRemoves),
		RangeSnapshotsGenerated:         metric.NewCounter(metaRangeSnapshotsGenerated),
		RangeSnapshotsNormalApplied:     metric.NewCounter(metaRangeSnapshotsNormalApplied),
		RangeSnapshotsPreemptiveApplied: metric.NewCounter(metaRangeSnapshotsPreemptiveApplied),

		// Raft processing metrics.
		RaftSelectDurationNanos:  metric.NewCounter(metaRaftSelectDurationNanos),
		RaftWorkingDurationNanos: metric.NewCounter(metaRaftWorkingDurationNanos),
		RaftTickingDurationNanos: metric.NewCounter(metaRaftTickingDurationNanos),
	}

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
	sm.LiveBytes.Update(sm.stats.LiveBytes)
	sm.KeyBytes.Update(sm.stats.KeyBytes)
	sm.ValBytes.Update(sm.stats.ValBytes)
	sm.IntentBytes.Update(sm.stats.IntentBytes)
	sm.LiveCount.Update(sm.stats.LiveCount)
	sm.KeyCount.Update(sm.stats.KeyCount)
	sm.ValCount.Update(sm.stats.ValCount)
	sm.IntentCount.Update(sm.stats.IntentCount)
	sm.IntentAge.Update(sm.stats.IntentAge)
	sm.GcBytesAge.Update(sm.stats.GCBytesAge)
	sm.LastUpdateNanos.Update(sm.stats.LastUpdateNanos)
	sm.SysBytes.Update(sm.stats.SysBytes)
	sm.SysCount.Update(sm.stats.SysCount)
}

func (sm *StoreMetrics) updateCapacityGauges(capacity roachpb.StoreCapacity) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.Capacity.Update(capacity.Capacity)
	sm.Available.Update(capacity.Available)
}

func (sm *StoreMetrics) updateReplicationGauges(leaders, replicated, pending, available int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.LeaderRangeCount.Update(leaders)
	sm.ReplicatedRangeCount.Update(replicated)
	sm.ReplicationPendingRangeCount.Update(pending)
	sm.AvailableRangeCount.Update(available)
}

func (sm *StoreMetrics) addMVCCStats(stats enginepb.MVCCStats) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.stats.Add(stats)
	sm.updateMVCCGaugesLocked()
}

func (sm *StoreMetrics) subtractMVCCStats(stats enginepb.MVCCStats) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.stats.Subtract(stats)
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
