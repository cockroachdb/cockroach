// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execstats

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// ShouldCollectStats is a helper function used to determine if a processor
// should collect stats. The two requirements are that tracing must be enabled
// (to be able to output the stats somewhere), and that the collectStats is true
// (flowCtx.CollectStats flag set by the gateway node).
func ShouldCollectStats(ctx context.Context, collectStats bool) bool {
	return collectStats && tracing.SpanFromContext(ctx) != nil
}

// ContentionEventsListener calculates the cumulative contention time across all
// kvpb.ContentionEvents seen by the listener.
type ContentionEventsListener struct {
	cumulativeContentionTime int64 // atomic
}

var _ tracing.EventListener = &ContentionEventsListener{}

// Notify is part of the tracing.EventListener interface.
func (c *ContentionEventsListener) Notify(event tracing.Structured) tracing.EventConsumptionStatus {
	ce, ok := event.(protoutil.Message).(*kvpb.ContentionEvent)
	if !ok {
		return tracing.EventNotConsumed
	}
	atomic.AddInt64(&c.cumulativeContentionTime, int64(ce.Duration))
	return tracing.EventConsumed
}

// GetContentionTime returns the cumulative contention time this listener has
// seen so far.
func (c *ContentionEventsListener) GetContentionTime() time.Duration {
	return time.Duration(atomic.LoadInt64(&c.cumulativeContentionTime))
}

// ScanStatsListener aggregates all kvpb.ScanStats objects into a single
// ScanStats object. It additionally looks for kvpb.UsedFollowerRead objects.
type ScanStatsListener struct {
	mu struct {
		syncutil.Mutex
		ScanStats
	}
}

var _ tracing.EventListener = &ScanStatsListener{}

// Notify is part of the tracing.EventListener interface.
func (l *ScanStatsListener) Notify(event tracing.Structured) tracing.EventConsumptionStatus {
	var ss *kvpb.ScanStats
	switch t := event.(type) {
	case *kvpb.ScanStats:
		ss = t
	case *kvpb.UsedFollowerRead:
		l.mu.Lock()
		defer l.mu.Unlock()
		l.mu.ScanStats.usedFollowerRead = true
		return tracing.EventConsumed
	default:
		return tracing.EventNotConsumed
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mu.ScanStats.numInterfaceSteps += ss.NumInterfaceSteps
	l.mu.ScanStats.numInternalSteps += ss.NumInternalSteps
	l.mu.ScanStats.numInterfaceSeeks += ss.NumInterfaceSeeks
	l.mu.ScanStats.numInternalSeeks += ss.NumInternalSeeks
	l.mu.ScanStats.blockBytes += ss.BlockBytes
	l.mu.ScanStats.blockBytesInCache += ss.BlockBytesInCache
	l.mu.ScanStats.keyBytes += ss.KeyBytes
	l.mu.ScanStats.valueBytes += ss.ValueBytes
	l.mu.ScanStats.pointCount += ss.PointCount
	l.mu.ScanStats.pointsCoveredByRangeTombstones += ss.PointsCoveredByRangeTombstones
	l.mu.ScanStats.rangeKeyCount += ss.RangeKeyCount
	l.mu.ScanStats.rangeKeyContainedPoints += ss.RangeKeyContainedPoints
	l.mu.ScanStats.rangeKeySkippedPoints += ss.RangeKeySkippedPoints
	l.mu.ScanStats.separatedPointCount += ss.SeparatedPointCount
	l.mu.ScanStats.separatedPointValueBytes += ss.SeparatedPointValueBytes
	l.mu.ScanStats.separatedPointValueBytesFetched += ss.SeparatedPointValueBytesFetched
	l.mu.ScanStats.numGets += ss.NumGets
	l.mu.ScanStats.numScans += ss.NumScans
	l.mu.ScanStats.numReverseScans += ss.NumReverseScans
	l.mu.ScanStats.nodeIDs = util.InsertUnique(l.mu.ScanStats.nodeIDs, int32(ss.NodeID))
	if ss.Region != "" {
		l.mu.ScanStats.regions = util.InsertUnique(l.mu.ScanStats.regions, ss.Region)
	}
	return tracing.EventConsumed
}

// GetScanStats returns all ScanStats the listener has accumulated so far.
func (l *ScanStatsListener) GetScanStats() ScanStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.mu.ScanStats
}

// TenantConsumptionListener aggregates consumed RUs from all
// kvpb.TenantConsumption events seen by the listener.
type TenantConsumptionListener struct {
	consumedRU uint64 // atomic
}

var _ tracing.EventListener = &TenantConsumptionListener{}

// Notify is part of the tracing.EventListener interface.
func (l *TenantConsumptionListener) Notify(
	event tracing.Structured,
) tracing.EventConsumptionStatus {
	tc, ok := event.(protoutil.Message).(*kvpb.TenantConsumption)
	if !ok {
		return tracing.EventNotConsumed
	}
	atomic.AddUint64(&l.consumedRU, uint64(tc.RU))
	return tracing.EventConsumed
}

// GetConsumedRU returns all consumed RUs that this listener has seen so far.
func (l *TenantConsumptionListener) GetConsumedRU() uint64 {
	return atomic.LoadUint64(&l.consumedRU)
}

// ScanStats contains statistics on the internal MVCC operators used to satisfy
// a scan. See storage/engine.go for a more thorough discussion of the meaning
// of each stat.
// TODO(sql-observability): include other fields that are in roachpb.ScanStats,
// here and in execinfrapb.KVStats.
type ScanStats struct {
	// numInterfaceSteps is the number of times the MVCC step function was called
	// to satisfy a scan.
	numInterfaceSteps uint64
	// numInternalSteps is the number of times that MVCC step was invoked
	// internally, including to step over internal, uncompacted Pebble versions.
	numInternalSteps uint64
	// numInterfaceSeeks is the number of times the MVCC seek function was called
	// to satisfy a scan.
	numInterfaceSeeks uint64
	// numInternalSeeks is the number of times that MVCC seek was invoked
	// internally, including to step over internal, uncompacted Pebble versions.
	numInternalSeeks                uint64
	blockBytes                      uint64
	blockBytesInCache               uint64
	keyBytes                        uint64
	valueBytes                      uint64
	pointCount                      uint64
	pointsCoveredByRangeTombstones  uint64
	rangeKeyCount                   uint64
	rangeKeyContainedPoints         uint64
	rangeKeySkippedPoints           uint64
	separatedPointCount             uint64
	separatedPointValueBytes        uint64
	separatedPointValueBytesFetched uint64
	numGets                         uint64
	numScans                        uint64
	numReverseScans                 uint64
	// nodeIDs stores the ordered list of all KV nodes that were used to
	// evaluate the KV requests.
	nodeIDs []int32
	// regions stores the ordered list of all regions that KV nodes used to
	// evaluate the KV requests reside in.
	regions []string
	// usedFollowerRead indicates whether at least some reads were served by the
	// follower replicas.
	usedFollowerRead bool
}

// PopulateKVMVCCStats adds data from the input ScanStats to the input KVStats.
func PopulateKVMVCCStats(kvStats *execinfrapb.KVStats, ss *ScanStats) {
	kvStats.NumInterfaceSteps = optional.MakeUint(ss.numInterfaceSteps)
	kvStats.NumInternalSteps = optional.MakeUint(ss.numInternalSteps)
	kvStats.NumInterfaceSeeks = optional.MakeUint(ss.numInterfaceSeeks)
	kvStats.NumInternalSeeks = optional.MakeUint(ss.numInternalSeeks)
	kvStats.BlockBytes = optional.MakeUint(ss.blockBytes)
	kvStats.BlockBytesInCache = optional.MakeUint(ss.blockBytesInCache)
	kvStats.KeyBytes = optional.MakeUint(ss.keyBytes)
	kvStats.ValueBytes = optional.MakeUint(ss.valueBytes)
	kvStats.PointCount = optional.MakeUint(ss.pointCount)
	kvStats.PointsCoveredByRangeTombstones = optional.MakeUint(ss.pointsCoveredByRangeTombstones)
	kvStats.RangeKeyCount = optional.MakeUint(ss.rangeKeyCount)
	kvStats.RangeKeyContainedPoints = optional.MakeUint(ss.rangeKeyContainedPoints)
	kvStats.RangeKeySkippedPoints = optional.MakeUint(ss.rangeKeySkippedPoints)
	kvStats.NumGets = optional.MakeUint(ss.numGets)
	kvStats.NumScans = optional.MakeUint(ss.numScans)
	kvStats.NumReverseScans = optional.MakeUint(ss.numReverseScans)
	kvStats.NodeIDs = ss.nodeIDs
	kvStats.Regions = ss.regions
	kvStats.UsedFollowerRead = ss.usedFollowerRead
}
