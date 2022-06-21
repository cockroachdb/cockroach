// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package asim

import "time"

const (
	defaultReplicaChangeBaseDelay  = 100 * time.Millisecond
	defaultReplicaAddDelayFactor   = 16
	defaultSplitQueueDelay         = 100 * time.Millisecond
	defaultRangeSizeSplitThreshold = 512
	defaultRangeRebalanceThreshold = 0.05
	defaultPacerLoopInterval       = 10 * time.Minute
	defaultPacerMinIterInterval    = 10 * time.Millisecond
	defaultPacerMaxIterIterval     = 1 * time.Second
	defaultStateExchangeInterval   = 10 * time.Second
	defaultStateExchangeDelay      = 500 * time.Millisecond
)

// SimulationSettings controls
// WIP: Thread these settings through to each of the sim parts.
type SimulationSettings struct {
	// ReplicaChangeBaseDelay is the base delay for all replica movements
	// (add,remove). It accounts for a fixed overhead of initiating a replica
	// movement.
	ReplicaChangeBaseDelay time.Duration
	// ReplicaAddDelayFactor is the factor applied to the range size (MB) when
	// calculating how long a replica addition will take for a given range
	// size. For adding a replica to a new store, the delay is calculated as
	// ReplicaChangeBaseDelay + (RangeSize(MB)  * ReplicaAddDelayFactor)
	// milliseconds.
	ReplicaAddDelayFactor float64
	// SplitQueueDelay is the delay that range splits take to complete.
	SplitQueueDelay time.Duration
	// RangeSizeSplitThreshold is the threshold below which ranges will not
	// attempted to be split due to size.
	RangeSizeSplitThreshold int64
	// RangeRebalanceThreshold is the minimum ratio of a store's range count to
	// the mean range count at which that store is considered overfull or underfull
	// of ranges.
	RangeRebalanceThreshold float64
	// PacerLoopInterval is the period over which the pacer will visit every
	// replica e.g. if the period is 10 minutes, the pacer will attempt to
	// visit every replica on the store 10 minute window, so if there are 1000
	// replicas, 100 replicas per minute.
	PacerLoopInterval time.Duration
	// PacerMinIterInterval is the minimum amount of time the pacer may wait
	// between visiting replicas.
	PacerMinIterInterval time.Duration
	// PacerMaxIterIterval is the maximum amount of time the pacer may wait
	// between visiting replicas.
	PacerMaxIterIterval time.Duration
	// StateExchangeInterval is the interval at which state updates will be
	// broadcast to other stores.
	StateExchangeInterval time.Duration
	// StateExchangeDelay is the delay between sending a state update and all
	// other stores receiving the update.
	StateExchangeDelay time.Duration
}

// DefaultSimulationSettings returns a set of default settings for simulation.
func DefaultSimulationSettings() SimulationSettings {
	return SimulationSettings{
		ReplicaChangeBaseDelay:  defaultReplicaChangeBaseDelay,
		ReplicaAddDelayFactor:   defaultReplicaAddDelayFactor,
		SplitQueueDelay:         defaultSplitQueueDelay,
		RangeSizeSplitThreshold: defaultRangeSizeSplitThreshold,
		RangeRebalanceThreshold: defaultRangeRebalanceThreshold,
		PacerLoopInterval:       defaultPacerLoopInterval,
		PacerMinIterInterval:    defaultPacerMinIterInterval,
		PacerMaxIterIterval:     defaultPacerMaxIterIterval,
		StateExchangeInterval:   defaultStateExchangeInterval,
		StateExchangeDelay:      defaultStateExchangeDelay,
	}
}

// ReplicaChangeDelayFn returns a function which calculates the delay for
// adding a replica based on the range size.
func ReplicaChangeDelayFn(
	settings SimulationSettings,
) func(rangeSize int64, add bool) time.Duration {
	return func(rangeSize int64, add bool) time.Duration {
		delay := settings.ReplicaChangeBaseDelay
		if add {
			delay += (time.Duration(rangeSize/(1024*1024)) / time.Duration(settings.ReplicaAddDelayFactor))
		}
		return delay
	}
}

// RangeSplitDelayFn returns a function which calculates the delay for
// splitting a range.
func RangeSplitDelayFn(settings SimulationSettings) func() time.Duration {
	return func() time.Duration {
		return settings.SplitQueueDelay
	}
}
