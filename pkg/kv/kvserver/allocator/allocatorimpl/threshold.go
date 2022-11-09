// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocatorimpl

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/state"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
)

const (
	// disableRebalancingThreshold declares a rebalancing threshold that
	// tolerates significant imbalance.
	disableRebalancingThreshold = 0
)

func getLoadThreshold(dim state.LoadDimension, sv *settings.Values) float64 {
	switch dim {
	case state.RangeCountDimension:
		return RangeRebalanceThreshold.Get(sv)
	case state.LeaseCountDimension:
		return LeaseRebalanceThreshold
	case state.QueriesDimension:
		return allocator.QPSRebalanceThreshold.Get(sv)
	case state.WriteKeysDimension:
		panic("load threshold not implemented for write keys")
	case state.StorageDimension:
		panic("load threshold not implemented for storage")
	case state.CPUTimeDimension:
		return CPURebalanceThreshold.Get(sv)
	default:
		panic(errors.AssertionFailedf("Unkown load dimension %d", dim))
	}
}

// LoadThresholds returns the load threshold values for the passed in
// dimensions.
func LoadThresholds(sv *settings.Values, dims ...state.LoadDimension) state.Load {
	thresholds := state.StaticDimensionContainer{}
	// Initially set each threshold to be disabled.
	for i := range thresholds {
		thresholds[i] = disableRebalancingThreshold
	}

	for _, dim := range dims {
		thresholds[dim] = getLoadThreshold(dim, sv)
	}
	return thresholds
}

func getLoadMinThreshold(dim state.LoadDimension) float64 {
	switch dim {
	case state.RangeCountDimension:
		return minRangeRebalanceThreshold
	case state.LeaseCountDimension:
		return LeaseRebalanceThresholdMin
	case state.QueriesDimension:
		return allocator.MinQPSThresholdDifference
	case state.WriteKeysDimension:
		panic("load min threshold not implemented for write keys")
	case state.StorageDimension:
		panic("load min threshold not implemented for storage")
	case state.CPUTimeDimension:
		return float64(minCPURebalanceThreshold)
	default:
		panic(errors.AssertionFailedf("Unkown load dimension %d", dim))
	}
}

// LoadMinThresholds returns the minimum absoute load amounts by which a
// candidate must differ from the mean before it is considered under or
// overfull.
func LoadMinThresholds(dims ...state.LoadDimension) state.Load {
	diffs := state.StaticDimensionContainer{}
	// Initially set each threshold to be disabled.
	for i := range diffs {
		diffs[i] = disableRebalancingThreshold
	}

	for _, dim := range dims {
		diffs[dim] = getLoadMinThreshold(dim)
	}
	return diffs
}

func getLoadRebalanceMinRequiredDiff(dim state.LoadDimension, sv *settings.Values) float64 {
	switch dim {
	case state.RangeCountDimension:
		panic("load min required diff not implemented for range count")
	case state.LeaseCountDimension:
		panic("load min required diff not implemented for lease count")
	case state.QueriesDimension:
		return allocator.MinQPSDifferenceForTransfers.Get(sv)
	case state.WriteKeysDimension:
		panic("load min required diff not implemented for write keys")
	case state.StorageDimension:
		panic("load min required diff not implemented for storage keys")
	case state.CPUTimeDimension:
		return float64(minCPURebalanceThreshold * 2)
	default:
		panic(errors.AssertionFailedf("Unkown load dimension %d", dim))
	}
}

// LoadRebalanceRequiredMinDiff returns the minimum absolute difference between
// an existing (store) and another candidate that must be met before a
// rebalance or transfer is allowed. This setting is purposed to prevent
// thrashing.
func LoadRebalanceRequiredMinDiff(sv *settings.Values, dims ...state.LoadDimension) state.Load {
	diffs := state.StaticDimensionContainer{}
	// Initially set each threshold to be disabled.
	for i := range diffs {
		diffs[i] = disableRebalancingThreshold
	}

	for _, dim := range dims {
		diffs[dim] = getLoadRebalanceMinRequiredDiff(dim, sv)
	}
	return diffs
}

// OverfullLoadThresholds returns the overfull load threshold for each load
// dimension.
func OverfullLoadThresholds(means, thresholds, minThresholds state.Load) state.Load {
	return means.Add(means.Mul(thresholds).Max(minThresholds))
}

// UnderfullLoadThresholds returns the underfull load threshold for each load
// dimension.
func UnderfullLoadThresholds(means, thresholds, minThresholds state.Load) state.Load {
	return means.Sub(means.Mul(thresholds).Max(minThresholds))
}

// MakeQPSOnlyDim returns a load dimension with only QPS filled in with the
// value given.
func MakeQPSOnlyDim(v float64) state.Load {
	dims := state.StaticDimensionContainer{}
	dims[state.QueriesDimension] = v
	return dims
}
