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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTargetForPolicy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const nowNanos = 100
	const maxOffsetNanos = 20
	const lagTargetNanos = 10

	for _, tc := range []struct {
		rangePolicy       roachpb.RangeClosedTimestampPolicy
		expClosedTSTarget hlc.Timestamp
	}{
		{
			rangePolicy:       roachpb.LAG_BY_CLUSTER_SETTING,
			expClosedTSTarget: hlc.Timestamp{WallTime: nowNanos - lagTargetNanos},
		},
		{
			rangePolicy:       roachpb.LEAD_FOR_GLOBAL_READS,
			expClosedTSTarget: hlc.Timestamp{WallTime: nowNanos - lagTargetNanos},
			// TODO(andrei, nvanbenschoten): What we should be expecting here is the following, once
			// the propBuf starts properly implementing this timestamp closing policy:
			// expClosedTSTarget: hlc.Timestamp{WallTime: nowNanos + 2*maxOffsetNanos, Synthetic: true},
		},
	} {
		t.Run(tc.rangePolicy.String(), func(t *testing.T) {
			now := hlc.ClockTimestamp{WallTime: nowNanos}
			target := TargetForPolicy(now, maxOffsetNanos, lagTargetNanos, tc.rangePolicy)
			require.Equal(t, tc.expClosedTSTarget, target)
		})
	}
}
