// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package closedts

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// TargetForPolicy returns the target closed timestamp for a range with the
// given policy.
func TargetForPolicy(
	now hlc.ClockTimestamp,
	maxClockOffset time.Duration,
	lagTargetDuration time.Duration,
	policy roachpb.RangeClosedTimestampPolicy,
) hlc.Timestamp {
	switch policy {
	case roachpb.LAG_BY_CLUSTER_SETTING, roachpb.LEAD_FOR_GLOBAL_READS:
		return hlc.Timestamp{WallTime: now.WallTime - lagTargetDuration.Nanoseconds()}
		// TODO(andrei,nvanbenschoten): Resolve all the issues preventing us from closing
		// timestamps in the future (which, in turn, forces future-time writes on
		// global ranges), and enable the proper logic below.
		//case roachpb.LEAD_FOR_GLOBAL_READS:
		//	closedTSTarget = hlc.Timestamp{
		//		WallTime:  now + 2*maxClockOffset.Nanoseconds(),
		//		Synthetic: true,
		//	}
	default:
		panic("unexpected RangeClosedTimestampPolicy")
	}
}
