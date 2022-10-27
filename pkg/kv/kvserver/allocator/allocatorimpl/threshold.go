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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/load"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
)

const (
	// disableRebalancingThreshold declares a rebalancing threshold that
	// tolerates significant imbalance.
	disableRebalancingThreshold = 0
)

func getLoadThreshold(dim load.Dimension, sv *settings.Values) float64 {
	switch dim {
	case load.Queries:
		return allocator.QPSRebalanceThreshold.Get(sv)
	default:
		panic(errors.AssertionFailedf("Unkown load dimension %d", dim))
	}
}

// LoadThresholds returns the load threshold values for the passed in
// dimensions.
func LoadThresholds(sv *settings.Values, dims ...load.Dimension) load.Load {
	thresholds := load.Vector{}
	// Initially set each threshold to be disabled.
	for i := range thresholds {
		thresholds[i] = disableRebalancingThreshold
	}

	for _, dim := range dims {
		thresholds[dim] = getLoadThreshold(dim, sv)
	}
	return thresholds
}

func getLoadMinThreshold(dim load.Dimension) float64 {
	switch dim {
	case load.Queries:
		return allocator.MinQPSThresholdDifference
	default:
		panic(errors.AssertionFailedf("Unkown load dimension %d", dim))
	}
}

// LoadMinThresholds returns the minimum absoute load amounts by which a
// candidate must differ from the mean before it is considered under or
// overfull.
func LoadMinThresholds(dims ...load.Dimension) load.Load {
	diffs := load.Vector{}
	// Initially set each threshold to be disabled.
	for i := range diffs {
		diffs[i] = disableRebalancingThreshold
	}

	for _, dim := range dims {
		diffs[dim] = getLoadMinThreshold(dim)
	}
	return diffs
}

func getLoadRebalanceMinRequiredDiff(dim load.Dimension, sv *settings.Values) float64 {
	switch dim {
	case load.Queries:
		return allocator.MinQPSDifferenceForTransfers.Get(sv)
	default:
		panic(errors.AssertionFailedf("Unkown load dimension %d", dim))
	}
}

// LoadRebalanceRequiredMinDiff returns the minimum absolute difference between
// an existing (store) and another candidate that must be met before a
// rebalance or transfer is allowed. This setting is purposed to prevent
// thrashing.
func LoadRebalanceRequiredMinDiff(sv *settings.Values, dims ...load.Dimension) load.Load {
	diffs := load.Vector{}
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
func OverfullLoadThresholds(means, thresholds, minThresholds load.Load) load.Load {
	return means.Add(means.Mul(thresholds).Max(minThresholds))
}

// UnderfullLoadThresholds returns the underfull load threshold for each load
// dimension.
func UnderfullLoadThresholds(means, thresholds, minThresholds load.Load) load.Load {
	return means.Sub(means.Mul(thresholds).Max(minThresholds))
}

// MakeQPSOnlyDim returns a load dimension with only QPS filled in with the
// value given.
func MakeQPSOnlyDim(v float64) load.Load {
	dims := load.Vector{}
	dims[load.Queries] = v
	return dims
}
