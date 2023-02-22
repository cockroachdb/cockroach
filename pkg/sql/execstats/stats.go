// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execstats

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	pbtypes "github.com/gogo/protobuf/types"
)

// ShouldCollectStats is a helper function used to determine if a processor
// should collect stats. The two requirements are that tracing must be enabled
// (to be able to output the stats somewhere), and that the collectStats is true
// (flowCtx.CollectStats flag set by the gateway node).
func ShouldCollectStats(ctx context.Context, collectStats bool) bool {
	return collectStats && tracing.SpanFromContext(ctx) != nil
}

// GetCumulativeContentionTime is a helper function to return all the contention
// events from trace and the cumulative contention time. It calculates the
// cumulative contention time from the given recording or, if the recording is
// nil, from the tracing span from the context. All contention events found in
// the trace are included.
func GetCumulativeContentionTime(
	ctx context.Context, recording tracingpb.Recording,
) (time.Duration, []kvpb.ContentionEvent) {
	var cumulativeContentionTime time.Duration
	if recording == nil {
		recording = tracing.SpanFromContext(ctx).GetConfiguredRecording()
	}

	var contentionEvents []kvpb.ContentionEvent
	var ev kvpb.ContentionEvent
	for i := range recording {
		recording[i].Structured(func(any *pbtypes.Any, _ time.Time) {
			if !pbtypes.Is(any, &ev) {
				return
			}
			if err := pbtypes.UnmarshalAny(any, &ev); err != nil {
				return
			}
			cumulativeContentionTime += ev.Duration
			contentionEvents = append(contentionEvents, ev)
		})
	}
	return cumulativeContentionTime, contentionEvents
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
	// ConsumedRU is the number of RUs that were consumed during the course of a
	// scan.
	ConsumedRU      uint64
	NumGets         uint64
	NumScans        uint64
	NumReverseScans uint64
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

// GetScanStats is a helper function to calculate scan stats from the given
// recording or, if the recording is nil, from the tracing span from the
// context.
func GetScanStats(ctx context.Context, recording tracingpb.Recording) (scanStats ScanStats) {
	if recording == nil {
		recording = tracing.SpanFromContext(ctx).GetRecording(tracingpb.RecordingStructured)
	}
	var ss kvpb.ScanStats
	var tc kvpb.TenantConsumption
	for i := range recording {
		recording[i].Structured(func(any *pbtypes.Any, _ time.Time) {
			if pbtypes.Is(any, &ss) {
				if err := pbtypes.UnmarshalAny(any, &ss); err != nil {
					return
				}
				scanStats.NumInterfaceSteps += ss.NumInterfaceSteps
				scanStats.NumInternalSteps += ss.NumInternalSteps
				scanStats.NumInterfaceSeeks += ss.NumInterfaceSeeks
				scanStats.NumInternalSeeks += ss.NumInternalSeeks
				scanStats.BlockBytes += ss.BlockBytes
				scanStats.BlockBytesInCache += ss.BlockBytesInCache
				scanStats.KeyBytes += ss.KeyBytes
				scanStats.ValueBytes += ss.ValueBytes
				scanStats.PointCount += ss.PointCount
				scanStats.PointsCoveredByRangeTombstones += ss.PointsCoveredByRangeTombstones
				scanStats.RangeKeyCount += ss.RangeKeyCount
				scanStats.RangeKeyContainedPoints += ss.RangeKeyContainedPoints
				scanStats.RangeKeySkippedPoints += ss.RangeKeySkippedPoints
				scanStats.SeparatedPointCount += ss.SeparatedPointCount
				scanStats.SeparatedPointValueBytes += ss.SeparatedPointValueBytes
				scanStats.SeparatedPointValueBytesFetched += ss.SeparatedPointValueBytesFetched
				scanStats.NumGets += ss.NumGets
				scanStats.NumScans += ss.NumScans
				scanStats.NumReverseScans += ss.NumReverseScans
			} else if pbtypes.Is(any, &tc) {
				if err := pbtypes.UnmarshalAny(any, &tc); err != nil {
					return
				}
				scanStats.ConsumedRU += uint64(tc.RU)
			}
		})
	}
	return scanStats
}
