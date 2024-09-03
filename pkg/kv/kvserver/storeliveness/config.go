// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storeliveness

import (
	"time"
)

// Options includes all Store Liveness durations needed by the SupportManager.
type Options struct {
	// HeartbeatInterval determines how often Store Liveness sends heartbeats.
	HeartbeatInterval time.Duration
	// LivenessInterval determines the Store Liveness support expiration time.
	LivenessInterval time.Duration
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
	heartbeatInterval time.Duration,
	livenessInterval time.Duration,
	supportWithdrawalGracePeriod time.Duration,
) Options {
	return Options{
		HeartbeatInterval:            heartbeatInterval,
		LivenessInterval:             livenessInterval,
		SupportExpiryInterval:        1 * time.Second,
		IdleSupportFromInterval:      1 * time.Minute,
		SupportWithdrawalGracePeriod: supportWithdrawalGracePeriod,
	}
}
