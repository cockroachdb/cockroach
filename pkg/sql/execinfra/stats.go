// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	pbtypes "github.com/gogo/protobuf/types"
)

// ShouldCollectStats is a helper function used to determine if a processor
// should collect stats. The two requirements are that tracing must be enabled
// (to be able to output the stats somewhere), and that the flowCtx.CollectStats
// flag was set by the gateway node.
func ShouldCollectStats(ctx context.Context, flowCtx *FlowCtx) bool {
	return tracing.SpanFromContext(ctx) != nil && flowCtx.CollectStats
}

// GetCumulativeContentionTime is a helper function to calculate the cumulative
// contention time from the tracing span from the context. All contention events
// found in the trace are included.
func GetCumulativeContentionTime(ctx context.Context) time.Duration {
	var cumulativeContentionTime time.Duration
	recording := GetTraceData(ctx)
	if recording == nil {
		return cumulativeContentionTime
	}
	var ev roachpb.ContentionEvent
	for i := range recording {
		recording[i].Structured(func(any *pbtypes.Any) {
			if !pbtypes.Is(any, &ev) {
				return
			}
			if err := pbtypes.UnmarshalAny(any, &ev); err != nil {
				return
			}
			cumulativeContentionTime += ev.Duration
		})
	}
	return cumulativeContentionTime
}

type ScanStats struct {
	StepCount         uint64
	InternalStepCount uint64
	SeekCount         uint64
	InternalSeekCount uint64
}

// PopulateKVStats adds data from the input ScanStats to the input KVStats.
func PopulateKVStats(kvStats *execinfrapb.KVStats, ss *ScanStats) {
	kvStats.StepCount = optional.MakeUint(ss.StepCount)
	kvStats.InternalStepCount = optional.MakeUint(ss.InternalStepCount)
	kvStats.SeekCount = optional.MakeUint(ss.SeekCount)
	kvStats.InternalSeekCount = optional.MakeUint(ss.InternalSeekCount)
}

// GetScanStats is a helper function to calculate scan stats from the tracing
// span from the context.
func GetScanStats(ctx context.Context) (ss ScanStats) {
	recording := GetTraceData(ctx)
	if recording == nil {
		return ScanStats{}
	}
	var ev roachpb.ScanStats
	for i := range recording {
		recording[i].Structured(func(any *pbtypes.Any) {
			if !pbtypes.Is(any, &ev) {
				return
			}
			if err := pbtypes.UnmarshalAny(any, &ev); err != nil {
				return
			}

			ss.StepCount += ev.StepCount
			ss.InternalStepCount += ev.InternalStepCount
			ss.SeekCount += ev.SeekCount
			ss.InternalSeekCount += ev.InternalSeekCount
		})
	}
	return ss
}
