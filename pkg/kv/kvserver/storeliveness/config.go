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

	"github.com/cockroachdb/cockroach/pkg/base"
)

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

func NewOptions(
	rangeLeaseDuration time.Duration,
	rangeLeaseRenewalDuration time.Duration,
	rpcHeartbeatInterval time.Duration,
) Options {
	heartbeatInterval := rangeLeaseRenewalDuration
	// It is possible that rangeLeaseRenewalDuration is 0 if
	// RangeLeaseRenewalFraction is set to -1 (indicating that leases should
	// not be renewed eagerly). Store Liveness should always heartbeat unless
	// it's disabled, so use DefaultRangeLeaseRenewalFraction to compute the
	// renewal duration in those cases.
	if heartbeatInterval <= 0 {
		heartbeatInterval = time.Duration(
			float64(rangeLeaseDuration) * base.DefaultRangeLeaseRenewalFraction,
		)
	}
	// DefaultRPCHeartbeatTimeout ensures the remote store probes the RPC
	// connection to the local store. DialTimeout ensures the remote store
	// has enough time to dial the local store, and NetworkTimeout ensures
	// the remote stores' heartbeat is received by the local store.
	// TODO(mira): Does this value make sense?
	supportWithdrawalGracePeriod := rpcHeartbeatInterval + base.DialTimeout + base.NetworkTimeout
	return Options{
		HeartbeatInterval:            heartbeatInterval,
		LivenessInterval:             rangeLeaseDuration,
		SupportExpiryInterval:        1 * time.Second,
		IdleSupportFromInterval:      1 * time.Minute,
		SupportWithdrawalGracePeriod: supportWithdrawalGracePeriod,
	}
}
