// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocator

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/state"
)

// RangeUsageInfo contains usage information (sizes and traffic) needed by the
// allocator to make rebalancing decisions for a given range.
type RangeUsageInfo struct {
	LogicalBytes     int64
	QueriesPerSecond float64
	WritesPerSecond  float64
	CPUTimePerSecond float64
	RequestLocality  *RangeRequestLocalityInfo
}

// RangeRequestLocalityInfo is the same as PerLocalityCounts and is used for
// tracking the request counts from each unique locality. It tracks the
// duration over which the request were recoreded.
type RangeRequestLocalityInfo struct {
	Counts   map[string]float64
	Duration time.Duration
}

// Load returns a load dimension representation of the range usage.
func (r RangeUsageInfo) Load() state.Load {
	dims := state.StaticDimensionContainer{}
	dims[state.RangeCountDimension] = float64(1)
	dims[state.QueriesDimension] = r.QueriesPerSecond
	dims[state.WriteKeysDimension] = r.WritesPerSecond
	dims[state.StorageDimension] = float64(r.LogicalBytes)
	dims[state.CPUTimeDimension] = r.CPUTimePerSecond
	return dims
}
