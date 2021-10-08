// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// Test that, when applying the read summary for the range containing the
// beginning of the key space to the timestamp cache, the local keyspace is not
// generally bumped. The first range is special in that its descriptor declares
// that it includes the local keyspace (\x01...), except that key space is
// special and is not included in any range. applyReadToTimestampCache has
// special provisions for this.
func TestReadSummaryApplyForR1(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	// Check that the local keyspace was not affected.
	ts, _ = tc.GetMax(keys.LocalPrefix, nil)
	require.Equal(t, baseTS, ts)

	// Check that the range-local keyspace for the range in question was affected.
	ts, _ = tc.GetMax(keys.MakeRangeKeyPrefix(r1desc.StartKey), nil)
	require.Equal(t, ts1, ts)
}

// This is the counter-part to TestReadSummaryApplyForR1, checking that the
// summary collection for first range has special logic avoiding the range-local
// keyspace.
func TestReadSummaryCollectForR1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	baseTS := hlc.Timestamp{WallTime: 123}
	manual := hlc.NewManualClock(baseTS.WallTime)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	tc := tscache.New(clock)

	r1desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKey("a"),
	}
	r2desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKeyMax,
	}
	// Populate the timestamp cache for a range-local key for r2.
	tc.Add(keys.MakeRangeKeyPrefix(r2desc.StartKey), nil, hlc.Timestamp{WallTime: 1000}, uuid.Nil)

	// Assert that r1's summary was not influenced by the r2 range-local key we
	// set above.
	summary := collectReadSummaryFromTimestampCache(tc, &r1desc)
	require.Equal(t, baseTS, summary.Global.LowWater)
	require.Equal(t, baseTS, summary.Local.LowWater)
}
