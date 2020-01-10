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
