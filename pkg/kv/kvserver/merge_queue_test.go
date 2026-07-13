// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestMergeQueueShouldQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testCtx := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tsc := TestStoreConfig(nil)
	testCtx.StartWithStoreConfig(ctx, t, stopper, tsc)

	mq := newMergeQueue(testCtx.store, testCtx.store.DB())
	kvserverbase.MergeQueueEnabled.Override(ctx, &testCtx.store.ClusterSettings().SV, true)

	tableKey := func(offset uint32) []byte {
		return keys.SystemSQLCodec.TablePrefix(bootstrap.TestingUserDescID(offset))
	}

	config.TestingSetZoneConfig(config.ObjectID(bootstrap.TestingUserDescID(0)), *zonepb.NewZoneConfig())
	config.TestingSetZoneConfig(config.ObjectID(bootstrap.TestingUserDescID(1)), *zonepb.NewZoneConfig())

	type testCase struct {
		startKey, endKey []byte
		minBytes         int64
		bytes            int64
		expShouldQ       bool
		expPriority      float64
	}

	testCases := []testCase{
		// The last range of table 1 should not be mergeable because table 2 exists.
		{
			startKey: tableKey(0),
			endKey:   tableKey(1),
			minBytes: 1,
		},
		{
			startKey: append(tableKey(0), 'z'),
			endKey:   tableKey(1),
			minBytes: 1,
		},

		// Unlike the last range of table 1, the last range of table 2 is mergeable
		// because there is no table that follows. (In this test, the system only
		// knows about tables on which TestingSetZoneConfig has been called.)
		{
			startKey:    tableKey(1),
			endKey:      tableKey(2),
			minBytes:    1,
			expShouldQ:  true,
			expPriority: 1,
		},
		{
			startKey:    append(tableKey(1), 'z'),
			endKey:      tableKey(2),
			minBytes:    1,
			expShouldQ:  true,
			expPriority: 1,
		},

		// The last range is never mergeable.
		{
			startKey: tableKey(2),
			endKey:   roachpb.KeyMax,
			minBytes: 1,
		},
		{
			startKey: append(tableKey(2), 'z'),
			endKey:   roachpb.KeyMax,
			minBytes: 1,
		},

		// An interior range of a table is not mergeable if it meets or exceeds the
		// minimum byte threshold.
		{
			startKey:    tableKey(0),
			endKey:      append(tableKey(0), 'a'),
			minBytes:    1024,
			bytes:       1024,
			expShouldQ:  false,
			expPriority: 0,
		},
		{
			startKey:    tableKey(0),
			endKey:      append(tableKey(0), 'a'),
			minBytes:    1024,
			bytes:       1024,
			expShouldQ:  false,
			expPriority: 0,
		},
		// Edge case: a minimum byte threshold of zero. This effectively disables
		// the threshold, as an empty range is no longer considered mergeable.
		{
			startKey:    tableKey(0),
			endKey:      append(tableKey(0), 'a'),
			minBytes:    0,
			bytes:       0,
			expShouldQ:  false,
			expPriority: 0,
		},

		// An interior range of a table is mergeable if it does not meet the minimum
		// byte threshold. Its priority is inversely related to its size.
		{
			startKey:    tableKey(0),
			endKey:      append(tableKey(0), 'a'),
			minBytes:    1024,
			bytes:       0,
			expShouldQ:  true,
			expPriority: 1,
		},
		{
			startKey:    tableKey(0),
			endKey:      append(tableKey(0), 'a'),
			minBytes:    1024,
			bytes:       768,
			expShouldQ:  true,
			expPriority: 0.25,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			repl := &Replica{store: testCtx.store}
			repl.shMu.state.Desc = &roachpb.RangeDescriptor{StartKey: tc.startKey, EndKey: tc.endKey}
			repl.shMu.state.Stats = &enginepb.MVCCStats{KeyBytes: tc.bytes}
			zoneConfig := zonepb.DefaultZoneConfigRef()
			zoneConfig.RangeMinBytes = proto.Int64(tc.minBytes)
			repl.SetSpanConfig(zoneConfig.AsSpanConfig())
			shouldQ, priority := mq.shouldQueue(ctx, hlc.ClockTimestamp{}, repl, config.NewSystemConfig(zoneConfig))
			if tc.expShouldQ != shouldQ {
				t.Errorf("incorrect shouldQ: expected %v but got %v", tc.expShouldQ, shouldQ)
			}
			if tc.expPriority != priority {
				t.Errorf("incorrect priority: expected %v but got %v", tc.expPriority, priority)
			}
		})
	}

	// A range that recently failed a merge attempt must not be re-offered until
	// the cooldown elapses, regardless of how mergeable it otherwise looks. See
	// MergeQueueCooldown and Replica.mergeQueueCooldownMu.
	t.Run("cooldown", func(t *testing.T) {
		MergeQueueCooldown.Override(ctx, &testCtx.store.ClusterSettings().SV, time.Minute)
		defer MergeQueueCooldown.Override(ctx, &testCtx.store.ClusterSettings().SV, MergeQueueCooldown.Default())

		// A mergeable interior range of table 0, identical in shape to the
		// expShouldQ=true cases above.
		repl := &Replica{store: testCtx.store}
		repl.shMu.state.Desc = &roachpb.RangeDescriptor{
			StartKey: tableKey(0), EndKey: append(tableKey(0), 'a'),
		}
		repl.shMu.state.Stats = &enginepb.MVCCStats{KeyBytes: 0}
		zoneConfig := zonepb.DefaultZoneConfigRef()
		zoneConfig.RangeMinBytes = proto.Int64(1024)
		repl.SetSpanConfig(zoneConfig.AsSpanConfig())
		confReader := config.NewSystemConfig(zoneConfig)

		now := hlc.ClockTimestamp{WallTime: timeutil.Now().UnixNano()}
		armed := func(ago time.Duration) {
			repl.setMergeCooldown(hlc.Timestamp{WallTime: now.WallTime - ago.Nanoseconds()})
		}

		// No cooldown armed: should queue.
		if should, _ := mq.shouldQueue(ctx, now, repl, confReader); !should {
			t.Fatal("expected shouldQueue=true with no cooldown armed")
		}

		// Armed 10s ago, within the 1m window: should not queue.
		armed(10 * time.Second)
		if should, _ := mq.shouldQueue(ctx, now, repl, confReader); should {
			t.Fatal("expected shouldQueue=false within cooldown window")
		}

		// Armed 2m ago, past the 1m window: should queue again.
		armed(2 * time.Minute)
		if should, _ := mq.shouldQueue(ctx, now, repl, confReader); !should {
			t.Fatal("expected shouldQueue=true after cooldown elapsed")
		}

		// Cooldown disabled (0): should queue even within the would-be window.
		MergeQueueCooldown.Override(ctx, &testCtx.store.ClusterSettings().SV, 0)
		armed(10 * time.Second)
		if should, _ := mq.shouldQueue(ctx, now, repl, confReader); !should {
			t.Fatal("expected shouldQueue=true when cooldown disabled")
		}
	})
}

// TestMergeQueueCooldown verifies that when the merge queue processes a
// replica whose right-hand neighbor cannot be merged away (here: a permanent
// sticky bit), it arms the in-memory cooldown, and that shouldQueue declines
// to re-offer the replica until kv.range_merge.cooldown has elapsed. Without
// this gate the merge queue busy-loops at top priority on a range it cannot
// merge (issue #171648).
//
// The test runs against a single Store (no test cluster) and drives the merge
// queue's shouldQueue and process methods directly. Since shouldQueue takes
// the current time as a parameter, the cooldown expiry is exercised without
// any clock manipulation.
func TestMergeQueueCooldown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testCtx := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testCtx.StartWithStoreConfig(ctx, t, stopper, TestStoreConfig(nil))
	store := testCtx.store

	mq := newMergeQueue(store, store.DB())

	// The cooldown defaults to 0 (disabled) on release branches; enable it so
	// the gate under test is active.
	MergeQueueCooldown.Override(ctx, &store.ClusterSettings().SV, time.Hour)

	// Register a (hydrated) zone config for table 0 so that the conf reader can
	// compute split boundaries and the store can resolve span configs for the
	// ranges created by the splits below; the LHS is an interior range of that
	// table and thus has no mandatory split point. The low RangeMinBytes makes
	// the empty LHS mergeable by size.
	zoneConfig := zonepb.DefaultZoneConfig()
	zoneConfig.RangeMinBytes = proto.Int64(1 << 10)
	config.TestingSetZoneConfig(
		config.ObjectID(bootstrap.TestingUserDescID(0)), zoneConfig)
	tableStart := keys.SystemSQLCodec.TablePrefix(bootstrap.TestingUserDescID(0))
	rhsStart := append(tableStart.Clone(), 'a')

	split := func(key roachpb.Key, expiration hlc.Timestamp) {
		t.Helper()
		_, pErr := store.LookupReplica(roachpb.RKey(key)).AdminSplit(ctx, kvpb.AdminSplitRequest{
			RequestHeader:  kvpb.RequestHeader{Key: key},
			SplitKey:       key,
			ExpirationTime: expiration,
		}, "test")
		require.NoError(t, pErr.GoError())
	}
	// Carve out the empty left-hand side [tableStart, rhsStart). Its right-hand
	// neighbor [rhsStart, Max) gets a permanent sticky bit and can thus never
	// be merged away.
	split(tableStart, hlc.Timestamp{} /* expiration */)
	split(rhsStart, hlc.MaxTimestamp /* expiration */)

	lhsRepl := store.LookupReplica(roachpb.RKey(tableStart))
	require.NotNil(t, lhsRepl)
	require.Equal(t, roachpb.RKey(rhsStart), lhsRepl.Desc().EndKey)
	confReader := config.NewSystemConfig(&zoneConfig)

	shouldQ := func(now hlc.ClockTimestamp) bool {
		t.Helper()
		should, _ := mq.shouldQueue(ctx, now, lhsRepl, confReader)
		return should
	}
	cooldownCount := func() int64 { return store.metrics.MergeQueueCooldown.Count() }

	// With no cooldown armed, the replica is offered to the queue: it is below
	// the minimum size threshold and shouldQueue cannot see the RHS sticky bit.
	require.True(t, shouldQ(store.Clock().NowAsClockTimestamp()))

	// Processing discovers the sticky bit, skips the merge, and arms the
	// cooldown.
	before := cooldownCount()
	processed, err := mq.process(ctx, lhsRepl, confReader, 0 /* priority */)
	require.NoError(t, err)
	require.False(t, processed)
	require.Equal(t, before+1, cooldownCount())
	armedAt := lhsRepl.getMergeCooldown()
	require.False(t, armedAt.IsEmpty())
	// The ranges did not merge.
	require.Equal(t, roachpb.RKey(rhsStart), lhsRepl.Desc().EndKey)

	// Within the cooldown window, shouldQueue declines to re-offer the replica.
	// (Without the gate, the queue would re-process it on every scanner cycle
	// and on every write.)
	cd := MergeQueueCooldown.Get(&store.ClusterSettings().SV)
	within := hlc.ClockTimestamp{WallTime: armedAt.WallTime + cd.Nanoseconds() - 1}
	require.False(t, shouldQ(within))

	// Once the cooldown has elapsed, the replica is offered again, and
	// re-processing it (still unmergeable) re-arms the cooldown.
	after := hlc.ClockTimestamp{WallTime: armedAt.WallTime + cd.Nanoseconds() + 1}
	require.True(t, shouldQ(after))
	processed, err = mq.process(ctx, lhsRepl, confReader, 0 /* priority */)
	require.NoError(t, err)
	require.False(t, processed)
	require.Equal(t, before+2, cooldownCount())
}
