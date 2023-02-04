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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

// GetCumulativeContentionInfo is a helper function to return all the contention
// events from the trace and the cumulative contention time. It calculates the
// cumulative contention time from the given recording or, if the recording is
// nil, from the tracing span from the context. All contention events found in
// the span (up to the tracing barrier) are included.
func GetCumulativeContentionInfo(
	ctx context.Context, recordingUpToBarrier tracingpb.Recording,
) (time.Duration, []roachpb.ContentionEvent) {
	var cumulativeContentionTime time.Duration
	if recordingUpToBarrier == nil {
		recordingUpToBarrier = tracing.SpanFromContext(ctx).GetStructuredRecordingUpToBarrier()
	}

	var contentionEvents []roachpb.ContentionEvent
	var ev roachpb.ContentionEvent
	for i := range recordingUpToBarrier {
		recordingUpToBarrier[i].Structured(func(any *pbtypes.Any, _ time.Time) {
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
	NumInternalSeeks uint64
	// ConsumedRU is the number of RUs that were consumed during the course of a
	// scan.
	ConsumedRU uint64
}

// PopulateKVMVCCStats adds data from the input ScanStats to the input KVStats.
func PopulateKVMVCCStats(kvStats *execinfrapb.KVStats, ss *ScanStats) {
	kvStats.NumInterfaceSteps = optional.MakeUint(ss.NumInterfaceSteps)
	kvStats.NumInternalSteps = optional.MakeUint(ss.NumInternalSteps)
	kvStats.NumInterfaceSeeks = optional.MakeUint(ss.NumInterfaceSeeks)
	kvStats.NumInternalSeeks = optional.MakeUint(ss.NumInternalSeeks)
}

// GetScanStats is a helper function to calculate scan stats from the given
// recording or, if the recording is nil, from the tracing span (up to the
// tracing barrier) from the context.
func GetScanStats(
	ctx context.Context, recordingUpToBarrier tracingpb.Recording,
) (scanStats ScanStats) {
	if recordingUpToBarrier == nil {
		recordingUpToBarrier = tracing.SpanFromContext(ctx).GetStructuredRecordingUpToBarrier()
	}
	var ss roachpb.ScanStats
	var tc roachpb.TenantConsumption
	for i := range recordingUpToBarrier {
		recordingUpToBarrier[i].Structured(func(any *pbtypes.Any, _ time.Time) {
			if pbtypes.Is(any, &ss) {
				if err := pbtypes.UnmarshalAny(any, &ss); err != nil {
					return
				}
				scanStats.NumInterfaceSteps += ss.NumInterfaceSteps
				scanStats.NumInternalSteps += ss.NumInternalSteps
				scanStats.NumInterfaceSeeks += ss.NumInterfaceSeeks
				scanStats.NumInternalSeeks += ss.NumInternalSeeks
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
