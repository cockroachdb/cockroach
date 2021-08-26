// Copyright 2021 The Cockroach Authors.
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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestCanServeFollowerRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// The clock needs to be higher than the closed-timestamp lag. Otherwise,
	// trying to close timestamps below zero results in not closing anything.
	manual := hlc.NewManualClock(5 * time.Second.Nanoseconds())
	clock := hlc.NewClock(manual.UnixNano, 1)
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
			tc.StartWithStoreConfig(t, stopper, tsc)

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
				"test", key, roachpb.NormalUserPriority,
				test.readTimestamp,
				clock.MaxOffset().Nanoseconds(),
			)

			ba := &roachpb.BatchRequest{}
			ba.Header = roachpb.Header{Txn: &txn}
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
	manual := hlc.NewManualClock(5 * time.Second.Nanoseconds())
	clock := hlc.NewClock(manual.UnixNano, 1)
	tsc := TestStoreConfig(clock)
	// Permit only one lease attempt. The test is flaky if we allow the lease to
	// be renewed by background processes.
	var leaseOnce sync.Once
	tsc.TestingKnobs.LeaseRequestEvent = func(ts hlc.Timestamp, _ roachpb.StoreID, _ roachpb.RangeID) *roachpb.Error {
		admitted := false
		leaseOnce.Do(func() {
			admitted = true
		})
		if admitted {
			return nil
		}
		return roachpb.NewErrorf("boom")
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
	tc.StartWithStoreConfig(t, stopper, tsc)

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

	manual.Increment(100 * time.Second.Nanoseconds())
	// Sanity check - lease should now no longer be valid.
	ls = r.CurrentLeaseStatus(ctx)
	require.False(t, ls.IsValid())

	gArgs := getArgs(key)
	txn := roachpb.MakeTransaction(
		"test", key, roachpb.NormalUserPriority,
		tsBelowClosedTimestamp,
		clock.MaxOffset().Nanoseconds(),
	)

	ba := &roachpb.BatchRequest{}
	ba.Header = roachpb.Header{
		Txn:       &txn,
		Timestamp: txn.ReadTimestamp,
	}
	ba.Add(&gArgs)

	ls, err := r.checkExecutionCanProceed(ctx, ba, nil /* g */)
	require.NoError(t, err)
	require.Empty(t, ls)

	// Sanity check - lease should not have been renewed, so it's still invalid.
	ls = r.CurrentLeaseStatus(ctx)
	require.False(t, ls.IsValid())
}
