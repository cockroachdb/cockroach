// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import "time"

// Options includes all Store Liveness durations needed by the SupportManager.
// TODO(mira): make sure these are initialized correctly as part of #125066.
type Options struct {
	// LivenessInterval determines the Store Liveness support expiration time.
	LivenessInterval time.Duration
	// HeartbeatInterval determines how often Store Liveness sends heartbeats.
	HeartbeatInterval time.Duration
	// SupportExpiryInterval determines how often Store Liveness checks if support
	// should be withdrawn.
	SupportExpiryInterval time.Duration
	// IdleSupportFromInterval determines how ofter Store Liveness checks if any
	// stores have not appeared in a SupportFrom call recently.
	IdleSupportFromInterval time.Duration
	// SupportWithdrawalGracePeriod determines how long Store Liveness should
	// wait after restart before withdrawing support. It helps prevent support
	// churn until the first heartbeats are delivered.
	SupportWithdrawalGracePeriod time.Duration
}

// NewOptions instantiates the Store Liveness Options.
func NewOptions(
	livenessInterval time.Duration,
	heartbeatInterval time.Duration,
	supportWithdrawalGracePeriod time.Duration,
) Options {
	return Options{
		LivenessInterval:             livenessInterval,
		HeartbeatInterval:            heartbeatInterval,
		SupportExpiryInterval:        100 * time.Millisecond,
		IdleSupportFromInterval:      1 * time.Minute,
		SupportWithdrawalGracePeriod: supportWithdrawalGracePeriod,
	}
}
