package kvserver

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestClosedTimestampTargetByPolicy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const nowNanos = 100
	const maxOffsetNanos = 20
	manualClock := hlc.NewManualClock(nowNanos)
	clock := hlc.NewClock(manualClock.UnixNano, maxOffsetNanos)
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
			require.Equal(t, tc.expClosedTSTarget, closedTimestampTargetByPolicy(clock.NowAsClockTimestamp(), tc.rangePolicy, lagTargetNanos))
		})
	}
}
