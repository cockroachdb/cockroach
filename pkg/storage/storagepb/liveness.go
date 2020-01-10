// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storagepb

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// IsLive returns whether the node is considered live at the given time with the
// given clock offset.
func (l *Liveness) IsLive(now hlc.Timestamp) bool {
	expiration := hlc.Timestamp(l.Expiration)
	return now.Less(expiration)
}

// IsDead returns true if the liveness expired more than threshold ago.
func (l *Liveness) IsDead(now hlc.Timestamp, threshold time.Duration) bool {
	deadAsOf := hlc.Timestamp(l.Expiration).GoTime().Add(threshold)
	return !now.GoTime().Before(deadAsOf)
}

// LivenessStatus returns a NodeLivenessStatus enumeration value for this liveness
// based on the provided timestamp and threshold.
//
// The timeline of the states that a liveness goes through as time passes after
// the respective liveness record is written is the following:
//
// ------|-------LIVE---|------UNAVAILABLE---|------DEAD------------> time
//       tWrite         tExp                 tExp+threshold
//
// Explanation:
// - Let's say a node write its liveness record at tWrite. It sets the
//   Expiration field of the record as tExp=tWrite+livenessThreshold.
//   The node is considered LIVE (or DECOMISSIONING or UNAVAILABLE if draining).
// - At tExp, the IsLive() method starts returning false. The state becomes
//	 UNAVAILABLE (or stays DECOMISSIONING or UNAVAILABLE if draining).
// - Once threshold passes, the node is considered DEAD (or DECOMISSIONED).
func (l *Liveness) LivenessStatus(now time.Time, threshold time.Duration) NodeLivenessStatus {
	nowHlc := hlc.Timestamp{WallTime: now.UnixNano()}
	if l.IsDead(nowHlc, threshold) {
		if l.Decommissioning {
			return NodeLivenessStatus_DECOMMISSIONED
		}
		return NodeLivenessStatus_DEAD
	}
	if l.Decommissioning {
		return NodeLivenessStatus_DECOMMISSIONING
	}
	if l.Draining {
		return NodeLivenessStatus_UNAVAILABLE
	}
	if l.IsLive(nowHlc) {
		return NodeLivenessStatus_LIVE
	}
	return NodeLivenessStatus_UNAVAILABLE
}
