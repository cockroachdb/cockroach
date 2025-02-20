// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

var (
	// defaultSupportExpiryInterval is the default value for SupportExpiryInterval.
	defaultSupportExpiryInterval = envutil.EnvOrDefaultDuration(
		"COCKROACH_STORE_LIVENESS_SUPPORT_EXPIRY_INTERVAL", 100*time.Millisecond,
	)

	// defaultIdleSupportFromInterval is the default value for
	// IdleSupportFromInterval.
	defaultIdleSupportFromInterval = envutil.EnvOrDefaultDuration(
		"COCKROACH_STORE_LIVENESS_IDLE_SUPPORT_FROM_INTERVAL", time.Minute,
	)
)

// Options includes all Store Liveness durations needed by the SupportManager.
type Options struct {
	// SupportDuration determines the Store Liveness support expiration time.
	SupportDuration time.Duration
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
	supportDuration time.Duration,
	heartbeatInterval time.Duration,
	supportWithdrawalGracePeriod time.Duration,
) Options {
	return Options{
		SupportDuration:              supportDuration,
		HeartbeatInterval:            heartbeatInterval,
		SupportExpiryInterval:        defaultSupportExpiryInterval,
		IdleSupportFromInterval:      defaultIdleSupportFromInterval,
		SupportWithdrawalGracePeriod: supportWithdrawalGracePeriod,
	}
}

// TransportKnobs includes all knobs that facilitate testing Transport.
type TransportKnobs struct {
	// OverrideIdleTimeout overrides the idleTimeout, which controls how
	// long until an instance of processQueue winds down after not observing any
	// messages.
	OverrideIdleTimeout func() time.Duration
}

// SupportManagerKnobs includes all knobs that facilitate testing the
// SupportManager. It is convenient to pass the same knobs to the entire node,
// and distinguish which store they are intended for within the knobs. E.g.
// TestEngine has a storeID field; DisableHeartbeats points to a store ID.
type SupportManagerKnobs struct {
	// TestEngine is a test engine to be used instead of a real one.
	TestEngine *TestEngine
	// DisableHeartbeats denotes the ID of a store whose heartbeats should be
	// stopped. Setting DisableHeartbeats to nil will re-enable heartbeats.
	DisableHeartbeats *atomic.Value // slpb.StoreIdent
	// DisableAllHeartbeats disables all store liveness heartbeats regardless of
	// the store ID.
	DisableAllHeartbeats *atomic.Bool
}

// TestingKnobs is a wrapper around TransportKnobs and SupportManagerKnobs.
type TestingKnobs struct {
	TransportKnobs
	SupportManagerKnobs
}
