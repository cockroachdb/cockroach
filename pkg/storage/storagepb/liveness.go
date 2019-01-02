// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storagepb

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// IsLive returns whether the node is considered live at the given time with the
// given clock offset.
func (l *Liveness) IsLive(now hlc.Timestamp, maxOffset time.Duration) bool {
	if maxOffset == timeutil.ClocklessMaxOffset {
		// When using clockless reads, we're live without a buffer period.
		maxOffset = 0
	}
	expiration := hlc.Timestamp(l.Expiration).Add(-maxOffset.Nanoseconds(), 0)
	return now.Less(expiration)
}

// IsDead returns whether the node is considered dead at the given time with the
// given threshold.
func (l *Liveness) IsDead(now hlc.Timestamp, threshold time.Duration) bool {
	deadAsOf := hlc.Timestamp(l.Expiration).GoTime().Add(threshold)
	return !now.GoTime().Before(deadAsOf)
}

// LivenessStatus returns a NodeLivenessStatus enumeration value for this liveness
// based on the provided timestamp, threshold, and clock max offset.
func (l *Liveness) LivenessStatus(
	now time.Time, threshold, maxOffset time.Duration,
) NodeLivenessStatus {
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
	if l.IsLive(nowHlc, maxOffset) {
		return NodeLivenessStatus_LIVE
	}
	return NodeLivenessStatus_UNAVAILABLE
}
