package kvserver

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/tscache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// Test that, when applying the read summary for the range containing the
// beginning of the key space to the timestamp cache, the local keyspace is not
// generally bumped. The first range is special in that its descriptor declares
// that it includes the local keyspace (\x01...), except that key space is
// special and is not included in any range. applyReadToTimestampCache has
// special provisions for this.
func TestReadSummaryForR1(t *testing.T) {
	defer leaktest.AfterTest(t)
	defer log.Scope(t).Close(t)

	baseTS := hlc.Timestamp{WallTime: 123}
	manual := hlc.NewManualClock(baseTS.WallTime)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	tc := tscache.New(clock)

	r1desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
	}
	ts1 := hlc.Timestamp{WallTime: 1000}
	summary := rspb.ReadSummary{
		Local:  rspb.Segment{LowWater: ts1},
		Global: rspb.Segment{LowWater: ts1},
	}
	applyReadSummaryToTimestampCache(tc, &r1desc, summary)
	tc.GetMax(keys.LocalPrefix, nil)

	// Make sure that updating the tscache did something, so the test is not
	// fooling itself.
	ts, _ := tc.GetMax(roachpb.Key("a"), nil)
	require.Equal(t, ts1, ts)

	// Check that the local keyspace was no affected.
	ts, _ = tc.GetMax(keys.LocalPrefix, nil)
	require.Equal(t, baseTS, ts)

	// Check that the range-local keyspace for the range in question was affected.
	ts, _ = tc.GetMax(keys.MakeRangeKeyPrefix(r1desc.StartKey), nil)
	require.Equal(t, ts1, ts)
}
