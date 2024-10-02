// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestCanServeFollowerRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// The clock needs to be higher than the closed-timestamp lag. Otherwise,
	// trying to close timestamps below zero results in not closing anything.
	manual := timeutil.NewManualTime(timeutil.Unix(0, 5))
	clock := hlc.NewClockForTesting(manual)
	tsc := TestStoreConfig(clock)
	const closedTimestampLag = time.Second
	closedts.TargetDuration.Override(ctx, &tsc.Settings.SV, closedTimestampLag)

	nowNs := clock.Now().WallTime
	tsBelowClosedTimestamp := hlc.Timestamp{
		WallTime: nowNs - closedTimestampLag.Nanoseconds() - clock.MaxOffset().Nanoseconds(),
		Logical:  0,
	}

	type test struct {
		// The timestamp at which we'll read. Reading below the closed timestamp
		// should result in canServeFollowerRead returning true; reading above the
		// closed timestamp should result in a false.
		readTimestamp           hlc.Timestamp
		expCanServeFollowerRead bool
	}
	for _, test := range []test{
		{
			readTimestamp:           tsBelowClosedTimestamp,
			expCanServeFollowerRead: true,
		},
		{
			readTimestamp:           clock.Now(),
			expCanServeFollowerRead: false,
		},
	} {
		t.Run("", func(t *testing.T) {
			tc := testContext{manualClock: manual}
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			tc.StartWithStoreConfig(ctx, t, stopper, tsc)

			key := roachpb.Key("a")

			// Perform a write in order to close a timestamp. The range starts with no
			// closed timestamp.
			{
				write := putArgs(key, []byte("foo"))
				_, pErr := tc.SendWrapped(&write)
				require.NoError(t, pErr.GoError())
			}

			gArgs := getArgs(key)
			txn := roachpb.MakeTransaction(
				"test",
				key,
				isolation.Serializable,
				roachpb.NormalUserPriority,
				test.readTimestamp,
				clock.MaxOffset().Nanoseconds(),
				0, // coordinatorNodeID
				0,
				false, // omitInRangefeeds
			)

			ba := &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: &txn}
			ba.Add(&gArgs)
			r := tc.repl
			r.mu.RLock()
			defer r.mu.RUnlock()
			require.Equal(t, test.expCanServeFollowerRead, r.canServeFollowerReadRLocked(ctx, ba))
		})
	}
}

// Test that follower reads are permitted when the replica's lease is invalid.
func TestCheckExecutionCanProceedAllowsFollowerReadWithInvalidLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// The clock needs to be higher than the closed-timestamp lag. Otherwise,
	// trying to close timestamps below zero results in not closing anything.
	manual := timeutil.NewManualTime(timeutil.Unix(5, 0))
	clock := hlc.NewClockForTesting(manual)
	tsc := TestStoreConfig(clock)
	tsc.TestingKnobs.DisableCanAckBeforeApplication = true
	// Permit only one lease attempt. The test is flaky if we allow the lease to
	// be renewed by background processes.
	var leaseOnce sync.Once
	tsc.TestingKnobs.LeaseRequestEvent = func(ts hlc.Timestamp, _ roachpb.StoreID, _ roachpb.RangeID) *kvpb.Error {
		admitted := false
		leaseOnce.Do(func() {
			admitted = true
		})
		if admitted {
			return nil
		}
		return kvpb.NewErrorf("boom")
	}
	const closedTimestampLag = time.Second
	closedts.TargetDuration.Override(ctx, &tsc.Settings.SV, closedTimestampLag)

	nowNs := clock.Now().WallTime
	tsBelowClosedTimestamp := hlc.Timestamp{
		WallTime: nowNs - closedTimestampLag.Nanoseconds() - clock.MaxOffset().Nanoseconds(),
		Logical:  0,
	}

	tc := testContext{manualClock: manual}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

	key := roachpb.Key("a")

	// Perform a write in order to close a timestamp. The range starts with no
	// closed timestamp.
	{
		write := putArgs(key, []byte("foo"))
		_, pErr := tc.SendWrapped(&write)
		require.NoError(t, pErr.GoError())
	}

	r := tc.repl
	// Sanity check - lease should be valid.
	ls := r.CurrentLeaseStatus(ctx)
	require.True(t, ls.IsValid())

	manual.Advance(100 * time.Second)
	// Sanity check - lease should now no longer be valid.
	ls = r.CurrentLeaseStatus(ctx)
	require.False(t, ls.IsValid())

	gArgs := getArgs(key)
	txn := roachpb.MakeTransaction(
		"test",
		key,
		isolation.Serializable,
		roachpb.NormalUserPriority,
		tsBelowClosedTimestamp,
		clock.MaxOffset().Nanoseconds(),
		0, // coordinatorNodeID
		0,
		false, // omitInRangefeeds
	)

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{
		Txn:       &txn,
		Timestamp: txn.ReadTimestamp,
	}
	ba.Add(&gArgs)

	ls, err := r.checkExecutionCanProceedBeforeStorageSnapshot(ctx, ba, nil /* g */)
	require.NoError(t, err)
	require.Empty(t, ls)

	// Sanity check - lease should not have been renewed, so it's still invalid.
	ls = r.CurrentLeaseStatus(ctx)
	require.False(t, ls.IsValid())
}
