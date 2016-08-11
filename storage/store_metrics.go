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
	metaReplicaCount                 = metric.MetricMetadata{"replicas", ""}
	metaReservedReplicaCount         = metric.MetricMetadata{"replicas.reserved", ""}
	metaLeaderRangeCount             = metric.MetricMetadata{"ranges.leader", ""}
	metaReplicatedRangeCount         = metric.MetricMetadata{"ranges.replicated", ""}
	metaReplicationPendingRangeCount = metric.MetricMetadata{"ranges.replication-pending", ""}
	metaAvailableRangeCount          = metric.MetricMetadata{"ranges.available", ""}
	metaLeaseRequestSuccessCount     = metric.MetricMetadata{"leases.success", ""}
	metaLeaseRequestErrorCount       = metric.MetricMetadata{"leases.error", ""}
	metaLiveBytes                    = metric.MetricMetadata{"livebytes", ""}
	metaKeyBytes                     = metric.MetricMetadata{"keybytes", ""}
	metaValBytes                     = metric.MetricMetadata{"valbytes", ""}
	metaIntentBytes                  = metric.MetricMetadata{"intentbytes", ""}
	metaLiveCount                    = metric.MetricMetadata{"livecount", ""}
	metaKeyCount                     = metric.MetricMetadata{"keycount", ""}
	metaValCount                     = metric.MetricMetadata{"valcount", ""}
	metaIntentCount                  = metric.MetricMetadata{"intentcount", ""}
	metaIntentAge                    = metric.MetricMetadata{"intentage", ""}
	metaGcBytesAge                   = metric.MetricMetadata{"gcbytesage", ""}
	metaLastUpdateNanos              = metric.MetricMetadata{"lastupdatenanos", ""}
	metaCapacity                     = metric.MetricMetadata{"capacity", ""}
	metaAvailable                    = metric.MetricMetadata{"capacity.available", ""}
	metaReserved                     = metric.MetricMetadata{"capacity.reserved", ""}
	metaSysBytes                     = metric.MetricMetadata{"sysbytes", ""}
	metaSysCount                     = metric.MetricMetadata{"syscount", ""}

	// RocksDB metrics.
	metaRdbBlockCacheHits           = metric.MetricMetadata{"rocksdb.block.cache.hits", ""}
	metaRdbBlockCacheMisses         = metric.MetricMetadata{"rocksdb.block.cache.misses", ""}
	metaRdbBlockCacheUsage          = metric.MetricMetadata{"rocksdb.block.cache.usage", ""}
	metaRdbBlockCachePinnedUsage    = metric.MetricMetadata{"rocksdb.block.cache.pinned-usage", ""}
	metaRdbBloomFilterPrefixChecked = metric.MetricMetadata{"rocksdb.bloom.filter.prefix.checked", ""}
	metaRdbBloomFilterPrefixUseful  = metric.MetricMetadata{"rocksdb.bloom.filter.prefix.useful", ""}
	metaRdbMemtableHits             = metric.MetricMetadata{"rocksdb.memtable.hits", ""}
	metaRdbMemtableMisses           = metric.MetricMetadata{"rocksdb.memtable.misses", ""}
	metaRdbMemtableTotalSize        = metric.MetricMetadata{"rocksdb.memtable.total-size", ""}
	metaRdbFlushes                  = metric.MetricMetadata{"rocksdb.flushes", ""}
	metaRdbCompactions              = metric.MetricMetadata{"rocksdb.compactions", ""}
	metaRdbTableReadersMemEstimate  = metric.MetricMetadata{"rocksdb.table-readers-mem-estimate", ""}
	metaRdbReadAmplification        = metric.MetricMetadata{"rocksdb.read-amplification", ""}

	// Range event metrics.
	metaRangeSplits                     = metric.MetricMetadata{"range.splits", ""}
	metaRangeAdds                       = metric.MetricMetadata{"range.adds", ""}
	metaRangeRemoves                    = metric.MetricMetadata{"range.removes", ""}
	metaRangeSnapshotsGenerated         = metric.MetricMetadata{"range.snapshots.generated", ""}
	metaRangeSnapshotsNormalApplied     = metric.MetricMetadata{"range.snapshots.normal-applied", ""}
	metaRangeSnapshotsPreemptiveApplied = metric.MetricMetadata{"range.snapshots.preemptive-applied", ""}

	// Raft processing metrics.
	metaRaftSelectDurationNanos  = metric.MetricMetadata{"process-raft.waitingnanos", ""}
	metaRaftWorkingDurationNanos = metric.MetricMetadata{"process-raft.workingnanos", ""}
	metaRaftTickingDurationNanos = metric.MetricMetadata{"process-raft.tickingnanos", ""}
)

type storeMetrics struct {
	registry *metric.Registry

	// Range data metrics.
	replicaCount                 *metric.Counter // Does not include reserved replicas.
	reservedReplicaCount         *metric.Counter
	leaderRangeCount             *metric.Gauge
	replicatedRangeCount         *metric.Gauge
	replicationPendingRangeCount *metric.Gauge
	availableRangeCount          *metric.Gauge

	// Lease data metrics.
	leaseRequestSuccessCount *metric.Counter
	leaseRequestErrorCount   *metric.Counter

	// Storage metrics.
	liveBytes       *metric.Gauge
	keyBytes        *metric.Gauge
	valBytes        *metric.Gauge
	intentBytes     *metric.Gauge
	liveCount       *metric.Gauge
	keyCount        *metric.Gauge
	valCount        *metric.Gauge
	intentCount     *metric.Gauge
	intentAge       *metric.Gauge
	gcBytesAge      *metric.Gauge
	lastUpdateNanos *metric.Gauge
	capacity        *metric.Gauge
	available       *metric.Gauge
	reserved        *metric.Counter
	sysBytes        *metric.Gauge
	sysCount        *metric.Gauge

	// RocksDB metrics.
	rdbBlockCacheHits           *metric.Gauge
	rdbBlockCacheMisses         *metric.Gauge
	rdbBlockCacheUsage          *metric.Gauge
	rdbBlockCachePinnedUsage    *metric.Gauge
	rdbBloomFilterPrefixChecked *metric.Gauge
	rdbBloomFilterPrefixUseful  *metric.Gauge
	rdbMemtableHits             *metric.Gauge
	rdbMemtableMisses           *metric.Gauge
	rdbMemtableTotalSize        *metric.Gauge
	rdbFlushes                  *metric.Gauge
	rdbCompactions              *metric.Gauge
	rdbTableReadersMemEstimate  *metric.Gauge
	rdbReadAmplification        *metric.Gauge

	// Range event metrics.
	rangeSplits                     *metric.Counter
	rangeAdds                       *metric.Counter
	rangeRemoves                    *metric.Counter
	rangeSnapshotsGenerated         *metric.Counter
	rangeSnapshotsNormalApplied     *metric.Counter
	rangeSnapshotsPreemptiveApplied *metric.Counter

	// Raft processing metrics.
	raftSelectDurationNanos  *metric.Counter
	raftWorkingDurationNanos *metric.Counter
	raftTickingDurationNanos *metric.Counter

	// Stats for efficient merges.
	// TODO(mrtracy): This should be removed as part of #4465. This is only
	// maintained to keep the current structure of StatusSummaries; it would be
	// better to convert the Gauges above into counters which are adjusted
	// accordingly.
	mu    syncutil.Mutex
	stats enginepb.MVCCStats
}

func newStoreMetrics() *storeMetrics {
	storeRegistry := metric.NewRegistry()
	sm := &storeMetrics{
		registry:                     storeRegistry,
		replicaCount:                 metric.NewCounter(metaReplicaCount),
		reservedReplicaCount:         metric.NewCounter(metaReservedReplicaCount),
		leaderRangeCount:             metric.NewGauge(metaLeaderRangeCount),
		replicatedRangeCount:         metric.NewGauge(metaReplicatedRangeCount),
		replicationPendingRangeCount: metric.NewGauge(metaReplicationPendingRangeCount),
		availableRangeCount:          metric.NewGauge(metaAvailableRangeCount),
		leaseRequestSuccessCount:     metric.NewCounter(metaLeaseRequestSuccessCount),
		leaseRequestErrorCount:       metric.NewCounter(metaLeaseRequestErrorCount),
		liveBytes:                    metric.NewGauge(metaLiveBytes),
		keyBytes:                     metric.NewGauge(metaKeyBytes),
		valBytes:                     metric.NewGauge(metaValBytes),
		intentBytes:                  metric.NewGauge(metaIntentBytes),
		liveCount:                    metric.NewGauge(metaLiveCount),
		keyCount:                     metric.NewGauge(metaKeyCount),
		valCount:                     metric.NewGauge(metaValCount),
		intentCount:                  metric.NewGauge(metaIntentCount),
		intentAge:                    metric.NewGauge(metaIntentAge),
		gcBytesAge:                   metric.NewGauge(metaGcBytesAge),
		lastUpdateNanos:              metric.NewGauge(metaLastUpdateNanos),
		capacity:                     metric.NewGauge(metaCapacity),
		available:                    metric.NewGauge(metaAvailable),
		reserved:                     metric.NewCounter(metaReserved),
		sysBytes:                     metric.NewGauge(metaSysBytes),
		sysCount:                     metric.NewGauge(metaSysCount),

		// RocksDB metrics.
		rdbBlockCacheHits:           metric.NewGauge(metaRdbBlockCacheHits),
		rdbBlockCacheMisses:         metric.NewGauge(metaRdbBlockCacheMisses),
		rdbBlockCacheUsage:          metric.NewGauge(metaRdbBlockCacheUsage),
		rdbBlockCachePinnedUsage:    metric.NewGauge(metaRdbBlockCachePinnedUsage),
		rdbBloomFilterPrefixChecked: metric.NewGauge(metaRdbBloomFilterPrefixChecked),
		rdbBloomFilterPrefixUseful:  metric.NewGauge(metaRdbBloomFilterPrefixUseful),
		rdbMemtableHits:             metric.NewGauge(metaRdbMemtableHits),
		rdbMemtableMisses:           metric.NewGauge(metaRdbMemtableMisses),
		rdbMemtableTotalSize:        metric.NewGauge(metaRdbMemtableTotalSize),
		rdbFlushes:                  metric.NewGauge(metaRdbFlushes),
		rdbCompactions:              metric.NewGauge(metaRdbCompactions),
		rdbTableReadersMemEstimate:  metric.NewGauge(metaRdbTableReadersMemEstimate),
		rdbReadAmplification:        metric.NewGauge(metaRdbReadAmplification),

		// Range event metrics.
		rangeSplits:                     metric.NewCounter(metaRangeSplits),
		rangeAdds:                       metric.NewCounter(metaRangeAdds),
		rangeRemoves:                    metric.NewCounter(metaRangeRemoves),
		rangeSnapshotsGenerated:         metric.NewCounter(metaRangeSnapshotsGenerated),
		rangeSnapshotsNormalApplied:     metric.NewCounter(metaRangeSnapshotsNormalApplied),
		rangeSnapshotsPreemptiveApplied: metric.NewCounter(metaRangeSnapshotsPreemptiveApplied),

		// Raft processing metrics.
		raftSelectDurationNanos:  metric.NewCounter(metaRaftSelectDurationNanos),
		raftWorkingDurationNanos: metric.NewCounter(metaRaftWorkingDurationNanos),
		raftTickingDurationNanos: metric.NewCounter(metaRaftTickingDurationNanos),
	}

	storeRegistry.AddMetric(sm.replicaCount)
	storeRegistry.AddMetric(sm.reservedReplicaCount)
	storeRegistry.AddMetric(sm.leaderRangeCount)
	storeRegistry.AddMetric(sm.replicatedRangeCount)
	storeRegistry.AddMetric(sm.replicationPendingRangeCount)
	storeRegistry.AddMetric(sm.availableRangeCount)
	storeRegistry.AddMetric(sm.leaseRequestSuccessCount)
	storeRegistry.AddMetric(sm.leaseRequestErrorCount)
	storeRegistry.AddMetric(sm.liveBytes)
	storeRegistry.AddMetric(sm.keyBytes)
	storeRegistry.AddMetric(sm.valBytes)
	storeRegistry.AddMetric(sm.intentBytes)
	storeRegistry.AddMetric(sm.liveCount)
	storeRegistry.AddMetric(sm.keyCount)
	storeRegistry.AddMetric(sm.valCount)
	storeRegistry.AddMetric(sm.intentCount)
	storeRegistry.AddMetric(sm.intentAge)
	storeRegistry.AddMetric(sm.gcBytesAge)
	storeRegistry.AddMetric(sm.lastUpdateNanos)
	storeRegistry.AddMetric(sm.capacity)
	storeRegistry.AddMetric(sm.available)
	storeRegistry.AddMetric(sm.reserved)
	storeRegistry.AddMetric(sm.sysBytes)
	storeRegistry.AddMetric(sm.sysCount)
	storeRegistry.AddMetric(sm.rdbBlockCacheHits)
	storeRegistry.AddMetric(sm.rdbBlockCacheMisses)
	storeRegistry.AddMetric(sm.rdbBlockCacheUsage)
	storeRegistry.AddMetric(sm.rdbBlockCachePinnedUsage)
	storeRegistry.AddMetric(sm.rdbBloomFilterPrefixChecked)
	storeRegistry.AddMetric(sm.rdbBloomFilterPrefixUseful)
	storeRegistry.AddMetric(sm.rdbMemtableHits)
	storeRegistry.AddMetric(sm.rdbMemtableMisses)
	storeRegistry.AddMetric(sm.rdbMemtableTotalSize)
	storeRegistry.AddMetric(sm.rdbFlushes)
	storeRegistry.AddMetric(sm.rdbCompactions)
	storeRegistry.AddMetric(sm.rdbTableReadersMemEstimate)
	storeRegistry.AddMetric(sm.rdbReadAmplification)
	storeRegistry.AddMetric(sm.rangeSplits)
	storeRegistry.AddMetric(sm.rangeAdds)
	storeRegistry.AddMetric(sm.rangeRemoves)
	storeRegistry.AddMetric(sm.rangeSnapshotsGenerated)
	storeRegistry.AddMetric(sm.rangeSnapshotsNormalApplied)
	storeRegistry.AddMetric(sm.rangeSnapshotsPreemptiveApplied)
	storeRegistry.AddMetric(sm.raftSelectDurationNanos)
	storeRegistry.AddMetric(sm.raftWorkingDurationNanos)
	storeRegistry.AddMetric(sm.raftTickingDurationNanos)

	return sm
}

// updateGaugesLocked breaks out individual metrics from the MVCCStats object.
// This process should be locked with each stat application to ensure that all
// gauges increase/decrease in step with the application of updates. However,
// this locking is not exposed to the registry level, and therefore a single
// snapshot of these gauges in the registry might mix the values of two
// subsequent updates.
func (sm *storeMetrics) updateMVCCGaugesLocked() {
	sm.liveBytes.Update(sm.stats.LiveBytes)
	sm.keyBytes.Update(sm.stats.KeyBytes)
	sm.valBytes.Update(sm.stats.ValBytes)
	sm.intentBytes.Update(sm.stats.IntentBytes)
	sm.liveCount.Update(sm.stats.LiveCount)
	sm.keyCount.Update(sm.stats.KeyCount)
	sm.valCount.Update(sm.stats.ValCount)
	sm.intentCount.Update(sm.stats.IntentCount)
	sm.intentAge.Update(sm.stats.IntentAge)
	sm.gcBytesAge.Update(sm.stats.GCBytesAge)
	sm.lastUpdateNanos.Update(sm.stats.LastUpdateNanos)
	sm.sysBytes.Update(sm.stats.SysBytes)
	sm.sysCount.Update(sm.stats.SysCount)
}

func (sm *storeMetrics) updateCapacityGauges(capacity roachpb.StoreCapacity) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.capacity.Update(capacity.Capacity)
	sm.available.Update(capacity.Available)
}

func (sm *storeMetrics) updateReplicationGauges(leaders, replicated, pending, available int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.leaderRangeCount.Update(leaders)
	sm.replicatedRangeCount.Update(replicated)
	sm.replicationPendingRangeCount.Update(pending)
	sm.availableRangeCount.Update(available)
}

func (sm *storeMetrics) addMVCCStats(stats enginepb.MVCCStats) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.stats.Add(stats)
	sm.updateMVCCGaugesLocked()
}

func (sm *storeMetrics) subtractMVCCStats(stats enginepb.MVCCStats) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.stats.Subtract(stats)
	sm.updateMVCCGaugesLocked()
}

func (sm *storeMetrics) updateRocksDBStats(stats engine.Stats) {
	// We do not grab a lock here, because it's not possible to get a point-in-
	// time snapshot of RocksDB stats. Retrieving RocksDB stats doesn't grab any
	// locks, and there's no way to retrieve multiple stats in a single operation.
	sm.rdbBlockCacheHits.Update(stats.BlockCacheHits)
	sm.rdbBlockCacheMisses.Update(stats.BlockCacheMisses)
	sm.rdbBlockCacheUsage.Update(stats.BlockCacheUsage)
	sm.rdbBlockCachePinnedUsage.Update(stats.BlockCachePinnedUsage)
	sm.rdbBloomFilterPrefixUseful.Update(stats.BloomFilterPrefixUseful)
	sm.rdbBloomFilterPrefixChecked.Update(stats.BloomFilterPrefixChecked)
	sm.rdbMemtableHits.Update(stats.MemtableHits)
	sm.rdbMemtableMisses.Update(stats.MemtableMisses)
	sm.rdbMemtableTotalSize.Update(stats.MemtableTotalSize)
	sm.rdbFlushes.Update(stats.Flushes)
	sm.rdbCompactions.Update(stats.Compactions)
	sm.rdbTableReadersMemEstimate.Update(stats.TableReadersMemEstimate)
}

func (sm *storeMetrics) leaseRequestComplete(success bool) {
	if success {
		sm.leaseRequestSuccessCount.Inc(1)
	} else {
		sm.leaseRequestErrorCount.Inc(1)
	}
}
