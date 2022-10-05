// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package config

import "time"

const (
	defaultReplicaChangeBaseDelay    = 200 * time.Millisecond
	defaultReplicaAddDelayFactor     = 16
	defaultSplitQueueDelay           = 200 * time.Millisecond
	defaultRangeSizeSplitThreshold   = 512 * 1024 * 1024 // 512mb
	defaultRangeRebalanceThreshold   = 0.05
	defaultPacerLoopInterval         = 10 * time.Minute
	defaultPacerMinIterInterval      = 10 * time.Millisecond
	defaultPacerMaxIterIterval       = 1 * time.Second
	defaultStateExchangeInterval     = 10 * time.Second
	defaultStateExchangeDelay        = 200 * time.Millisecond
	defaultSplitQPSThreshold         = 2500
	defaultSplitQPSRetention         = 10 * time.Minute
	defaultSeed                      = 42
	defaultLBRebalancingMode         = 2 // Leases and replicas.
	defaultLBRebalancingInterval     = time.Minute
	defaultLBRebalanceQPSThreshold   = 0.1
	defaultLBMinRequiredQPSDiff      = 200
	defaultReplQueueEnabled          = true
	defaultSpiltQueueEnabled         = true
	defaultReplQueueTransfersEnabled = true
	defaultSystemRanges              = 44
	defaultInterval                  = 500 * time.Millisecond
	defaultBackgroundInterval        = 10 * time.Second
	defaultDuration                  = 30 * time.Minute
)

var (
	defaultStartTime = time.Date(2022, 03, 21, 11, 0, 0, 0, time.UTC)
)

// SimulationSettings declares the settings for controlling simulation and
// cluster settings used in cockroach.
type SimulationSettings struct {
	*SimulationControlSettings
	*SimulationClusterSettings
}

// SimulationControlSettings declares the settings that control the simulation
// loop and components. These settings do not exist in the real cluster.
type SimulationControlSettings struct {
	// Seed is the random source that will be used for any simulator components
	// that accept a seed.
	Seed int64
	// ReplicaChangeBaseDelay is the base delay for all replica movements
	// (add,remove). It accounts for a fixed overhead of initiating a replica
	// movement.
	ReplicaChangeBaseDelay time.Duration
	// ReplicaAddRate is the factor applied to the range size (MB) when
	// calculating how long a replica addition will take for a given range
	// size. For adding a replica to a new store, the delay is calculated as
	// ReplicaChangeBaseDelay + (RangeSize(MB)  * ReplicaAddRate) milliseconds.
	// This is analogous to the rate at which a store will ingest snapshots for
	// up replication.
	ReplicaAddRate float64
	// SplitQueueDelay is the delay that range splits take to complete.
	SplitQueueDelay time.Duration
	// PacerLoopInterval is the period over which the pacer will visit every
	// replica e.g. If the period is 10 minutes, the pacer will attempt to
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
	// SystemRanges declares the number of ranges that exist outside of the
	// workload keyspace.
	SystemRanges int
	// Interval is the step between ticks for active simulaton components, such
	// as the queues, store rebalancer and state changers. It should be set
	// lower than the bgInterval, as updated occur more frequently.
	Interval time.Duration
	// BackgroundInterval controls the step between ticks for background
	// simulation components, such as the metrics, state exchange and workload
	// generators. It should be set higher than the interval, as it is more
	// costly and requires less frequent updates.
	BackgroundInterval time.Duration
	// Duration controls the amount of time the simulation runs for.
	Duration time.Duration
	// Start controls the start point, in virtual time that the simulation
	// will begin ticking from.
	Start time.Time
}

// SimulationClusterSettings declares the settings that map to cockroach
// cluster settings.
type SimulationClusterSettings struct {
	// RangeSizeSplitThreshold is the threshold in MB, below which ranges will
	// not attempted to be split due to size.
	RangeSizeSplitThreshold int64
	// RangeRebalanceThreshold is the minimum ratio of a store's range count to
	// the mean range count at which that store is considered overfull or underfull
	// of ranges.
	RangeRebalanceThreshold float64
	// SplitQPSThreshold is the threshold above which a range will be a
	// candidate for load based splitting.
	SplitQPSThreshold float64
	// SplitQPSRetention is the duration which recorded load will be retained
	// and factored into load based splitting decisions.
	SplitQPSRetention time.Duration
	// LBRebalancingMode controls if and when we do store-level rebalancing
	// based on load. It maps to kvserver.LBRebalancingMode.
	LBRebalancingMode int64
	// LBRebalancingInterval controls how often the store rebalancer will
	// consider opportunities for rebalancing.
	LBRebalancingInterval time.Duration
	// LBRebalanceQPSThreshold is the fraction above or below the mean store QPS,
	// that a store is considered overfull or underfull.
	LBRebalanceQPSThreshold float64
	// LBMinQPSDifferenceForTransfers is the minimum QPS difference that the store
	// rebalancer would care to reconcile (via lease or replica rebalancing) between
	// any two stores.
	LBMinRequiredQPSDiff float64
	// ReplicateQueueEnabled is a flag declaring whether the replicate queue is
	// enabled.
	ReplicateQueueEnabled bool
	// SplitQueueEnabled is a flag declaring whether the split queue is
	// enabled.
	SplitQueueEnabled bool
	// ReplQueueTransfersEnabled is a flag declaring whether the replicate
	// queue will attempt to transfer leases in addition to moving replicas.
	ReplQueueTransfersEnabled bool
}

// DefaultSimulationSettings returns the default settings for the simulation.
func DefaultSimulationSettings() *SimulationSettings {
	return &SimulationSettings{
		SimulationControlSettings: DefaultSimulationControlSettings(),
		SimulationClusterSettings: DefaultSimulationClusterSettings(),
	}
}

// DefaultSimulationControlSettings returns a set of default control settings for
// simulation.
func DefaultSimulationControlSettings() *SimulationControlSettings {
	return &SimulationControlSettings{
		Seed:                   defaultSeed,
		ReplicaChangeBaseDelay: defaultReplicaChangeBaseDelay,
		ReplicaAddRate:         defaultReplicaAddDelayFactor,
		SplitQueueDelay:        defaultSplitQueueDelay,
		PacerLoopInterval:      defaultPacerLoopInterval,
		PacerMinIterInterval:   defaultPacerMinIterInterval,
		PacerMaxIterIterval:    defaultPacerMaxIterIterval,
		StateExchangeInterval:  defaultStateExchangeInterval,
		StateExchangeDelay:     defaultStateExchangeDelay,
		SystemRanges:           defaultSystemRanges,
		Interval:               defaultInterval,
		BackgroundInterval:     defaultBackgroundInterval,
		Duration:               defaultDuration,
		Start:                  defaultStartTime,
	}
}

// DefaultSimulationClusterSettings retunrs a set of default cluster settings
// for simulation.
func DefaultSimulationClusterSettings() *SimulationClusterSettings {
	return &SimulationClusterSettings{
		RangeSizeSplitThreshold:   defaultRangeSizeSplitThreshold,
		RangeRebalanceThreshold:   defaultRangeRebalanceThreshold,
		SplitQPSThreshold:         defaultSplitQPSThreshold,
		SplitQPSRetention:         defaultSplitQPSRetention,
		LBRebalancingMode:         defaultLBRebalancingMode,
		LBRebalancingInterval:     defaultLBRebalancingInterval,
		LBRebalanceQPSThreshold:   defaultLBRebalanceQPSThreshold,
		LBMinRequiredQPSDiff:      defaultLBMinRequiredQPSDiff,
		ReplicateQueueEnabled:     defaultReplQueueEnabled,
		SplitQueueEnabled:         defaultSpiltQueueEnabled,
		ReplQueueTransfersEnabled: defaultReplQueueTransfersEnabled,
	}
}

// ReplicaChangeDelayFn returns a function which calculates the delay for
// adding a replica based on the range size.
func (s *SimulationControlSettings) ReplicaChangeDelayFn() func(rangeSize int64, add bool) time.Duration {
	return func(rangeSize int64, add bool) time.Duration {
		delay := s.ReplicaChangeBaseDelay
		if add {
			delay += (time.Duration(rangeSize/(1024*1024)) / time.Duration(s.ReplicaAddRate))
		}
		return delay
	}
}

// RangeSplitDelayFn returns a function which calculates the delay for
// splitting a range.
func (s *SimulationControlSettings) RangeSplitDelayFn() func() time.Duration {
	return func() time.Duration {
		return s.SplitQueueDelay
	}
}

// SplitQPSThresholdFn returns a function that returns the current QPS split
// threshold for load based splitting of a range.
func (s *SimulationClusterSettings) SplitQPSThresholdFn() func() float64 {
	return func() float64 {
		return s.SplitQPSThreshold
	}
}

// SplitQPSRetentionFn returns a function that returns the current QPS
// retention duration for load recorded against a range, used in load based
// split decisions.
func (s *SimulationClusterSettings) SplitQPSRetentionFn() func() time.Duration {
	return func() time.Duration {
		return s.SplitQPSRetention
	}
}
