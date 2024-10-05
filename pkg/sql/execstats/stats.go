// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execstats

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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
	CumulativeContentionTime time.Duration
}

var _ tracing.EventListener = &ContentionEventsListener{}

// Notify is part of the tracing.EventListener interface.
func (c *ContentionEventsListener) Notify(event tracing.Structured) tracing.EventConsumptionStatus {
	ce, ok := event.(protoutil.Message).(*kvpb.ContentionEvent)
	if !ok {
		return tracing.EventNotConsumed
	}
	c.CumulativeContentionTime += ce.Duration
	return tracing.EventConsumed
}

// ScanStatsListener aggregates all kvpb.ScanStats objects into a single
// ScanStats object.
type ScanStatsListener struct {
	ScanStats
}

var _ tracing.EventListener = &ScanStatsListener{}

// Notify is part of the tracing.EventListener interface.
func (l *ScanStatsListener) Notify(event tracing.Structured) tracing.EventConsumptionStatus {
	ss, ok := event.(protoutil.Message).(*kvpb.ScanStats)
	if !ok {
		return tracing.EventNotConsumed
	}
	l.ScanStats.NumInterfaceSteps += ss.NumInterfaceSteps
	l.ScanStats.NumInternalSteps += ss.NumInternalSteps
	l.ScanStats.NumInterfaceSeeks += ss.NumInterfaceSeeks
	l.ScanStats.NumInternalSeeks += ss.NumInternalSeeks
	l.ScanStats.BlockBytes += ss.BlockBytes
	l.ScanStats.BlockBytesInCache += ss.BlockBytesInCache
	l.ScanStats.KeyBytes += ss.KeyBytes
	l.ScanStats.ValueBytes += ss.ValueBytes
	l.ScanStats.PointCount += ss.PointCount
	l.ScanStats.PointsCoveredByRangeTombstones += ss.PointsCoveredByRangeTombstones
	l.ScanStats.RangeKeyCount += ss.RangeKeyCount
	l.ScanStats.RangeKeyContainedPoints += ss.RangeKeyContainedPoints
	l.ScanStats.RangeKeySkippedPoints += ss.RangeKeySkippedPoints
	l.ScanStats.SeparatedPointCount += ss.SeparatedPointCount
	l.ScanStats.SeparatedPointValueBytes += ss.SeparatedPointValueBytes
	l.ScanStats.SeparatedPointValueBytesFetched += ss.SeparatedPointValueBytesFetched
	l.ScanStats.NumGets += ss.NumGets
	l.ScanStats.NumScans += ss.NumScans
	l.ScanStats.NumReverseScans += ss.NumReverseScans
	return tracing.EventConsumed
}

// TenantConsumptionListener aggregates consumed RUs from all
// kvpb.TenantConsumption events seen by the listener.
type TenantConsumptionListener struct {
	ConsumedRU uint64
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
	l.ConsumedRU += uint64(tc.RU)
	return tracing.EventConsumed
}

// ScanStats contains statistics on the internal MVCC operators used to satisfy
// a scan. See storage/engine.go for a more thorough discussion of the meaning
// of each stat.
// TODO(sql-observability): include other fields that are in roachpb.ScanStats,
// here and in execinfrapb.KVStats.
type ScanStats struct {
	// NumInterfaceSteps is the number of times the MVCC step function was called
	// to satisfy a scan.
	NumInterfaceSteps uint64
	// NumInternalSteps is the number of times that MVCC step was invoked
	// internally, including to step over internal, uncompacted Pebble versions.
	NumInternalSteps uint64
	// NumInterfaceSeeks is the number of times the MVCC seek function was called
	// to satisfy a scan.
	NumInterfaceSeeks uint64
	// NumInternalSeeks is the number of times that MVCC seek was invoked
	// internally, including to step over internal, uncompacted Pebble versions.
	NumInternalSeeks                uint64
	BlockBytes                      uint64
	BlockBytesInCache               uint64
	KeyBytes                        uint64
	ValueBytes                      uint64
	PointCount                      uint64
	PointsCoveredByRangeTombstones  uint64
	RangeKeyCount                   uint64
	RangeKeyContainedPoints         uint64
	RangeKeySkippedPoints           uint64
	SeparatedPointCount             uint64
	SeparatedPointValueBytes        uint64
	SeparatedPointValueBytesFetched uint64
	NumGets                         uint64
	NumScans                        uint64
	NumReverseScans                 uint64
}

// PopulateKVMVCCStats adds data from the input ScanStats to the input KVStats.
func PopulateKVMVCCStats(kvStats *execinfrapb.KVStats, ss *ScanStats) {
	kvStats.NumInterfaceSteps = optional.MakeUint(ss.NumInterfaceSteps)
	kvStats.NumInternalSteps = optional.MakeUint(ss.NumInternalSteps)
	kvStats.NumInterfaceSeeks = optional.MakeUint(ss.NumInterfaceSeeks)
	kvStats.NumInternalSeeks = optional.MakeUint(ss.NumInternalSeeks)
	kvStats.BlockBytes = optional.MakeUint(ss.BlockBytes)
	kvStats.BlockBytesInCache = optional.MakeUint(ss.BlockBytesInCache)
	kvStats.KeyBytes = optional.MakeUint(ss.KeyBytes)
	kvStats.ValueBytes = optional.MakeUint(ss.ValueBytes)
	kvStats.PointCount = optional.MakeUint(ss.PointCount)
	kvStats.PointsCoveredByRangeTombstones = optional.MakeUint(ss.PointsCoveredByRangeTombstones)
	kvStats.RangeKeyCount = optional.MakeUint(ss.RangeKeyCount)
	kvStats.RangeKeyContainedPoints = optional.MakeUint(ss.RangeKeyContainedPoints)
	kvStats.RangeKeySkippedPoints = optional.MakeUint(ss.RangeKeySkippedPoints)
	kvStats.NumGets = optional.MakeUint(ss.NumGets)
	kvStats.NumScans = optional.MakeUint(ss.NumScans)
	kvStats.NumReverseScans = optional.MakeUint(ss.NumReverseScans)
}
