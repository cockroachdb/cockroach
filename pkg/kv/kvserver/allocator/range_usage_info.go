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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/load"
)

// RangeUsageInfo contains usage information (sizes and traffic) needed by the
// allocator to make rebalancing decisions for a given range.
type RangeUsageInfo struct {
	LogicalBytes             int64
	QueriesPerSecond         float64
	WritesPerSecond          float64
	ReadsPerSecond           float64
	WriteBytesPerSecond      float64
	ReadBytesPerSecond       float64
	RequestCPUNanosPerSecond float64
	RequestsPerSecond        float64
	RaftCPUNanosPerSecond    float64
	RequestLocality          *RangeRequestLocalityInfo
}

// RangeRequestLocalityInfo is the same as PerLocalityCounts and is used for
// tracking the request counts from each unique locality. It tracks the
// duration over which the request were recoreded.
type RangeRequestLocalityInfo struct {
	Counts   map[string]float64
	Duration time.Duration
}

// Load returns a load dimension representation of the range usage.
func (r RangeUsageInfo) Load() load.Load {
	dims := load.Vector{}
	dims[load.Queries] = r.QueriesPerSecond
	dims[load.CPU] = r.RequestCPUNanosPerSecond + r.RaftCPUNanosPerSecond
	return dims
}

// TransferImpact returns the impact of transferring the lease for the range,
// given the usage information. The impact is assumed to be symmetric, e.g. the
// receiving store of the transfer will have load = prev_load(recv) + impact
// after the transfer, whilst the sending side will have load =
// prev_load(sender) - impact after the transfer.
func (r RangeUsageInfo) TransferImpact() load.Load {
	dims := load.Vector{}
	dims[load.Queries] = r.QueriesPerSecond
	// Only use the request recorded cpu. This assumes that all replicas will
	// use the same amount of raft cpu - which may be dubious.
	//
	// TODO(kvoli): Look to separate out leaseholder vs replica cpu usage in
	// accounting to account for follower reads if able.
	dims[load.CPU] = r.RequestCPUNanosPerSecond
	return dims
}
