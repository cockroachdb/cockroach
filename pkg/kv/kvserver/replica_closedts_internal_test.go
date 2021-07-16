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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestSideTransportClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	ts3 := hlc.Timestamp{WallTime: 3}

	tests := []struct {
		name           string
		applied        ctpb.LAI
		localClosed    hlc.Timestamp
		localLAI       ctpb.LAI
		receiverClosed hlc.Timestamp
		receiverLAI    ctpb.LAI
		sufficient     hlc.Timestamp

		expectedLocalUpdate bool
		expectedClosed      hlc.Timestamp
	}{
		{
			name:                "all empty",
			expectedClosed:      hlc.Timestamp{},
			expectedLocalUpdate: false,
		},
		{
			name:           "only local",
			applied:        100,
			localClosed:    ts1,
			localLAI:       1,
			expectedClosed: ts1,
		},
		{
			name:                "only receiver",
			applied:             100,
			receiverClosed:      ts1,
			receiverLAI:         1,
			expectedClosed:      ts1,
			expectedLocalUpdate: true,
		},
		{
			name:           "local sufficient",
			applied:        100,
			localClosed:    ts1,
			localLAI:       1,
			receiverClosed: ts2,
			receiverLAI:    2,
			// The caller won't need a closed timestamp > ts1, so we expect the
			// receiver to not be consulted.
			sufficient:          ts1,
			expectedClosed:      ts1,
			expectedLocalUpdate: false,
		},
		{
			name:                "local insufficient",
			applied:             100,
			localClosed:         ts1,
			localLAI:            1,
			receiverClosed:      ts2,
			receiverLAI:         2,
			sufficient:          ts3,
			expectedClosed:      ts2,
			expectedLocalUpdate: true,
		},
		{
			name:           "replication not caught up",
			applied:        0,
			localClosed:    ts1,
			localLAI:       1,
			receiverClosed: ts2,
			receiverLAI:    2,
			sufficient:     ts3,
			// We expect no usable closed timestamp to be returned. And also we expect
			// the local state to not be updated because the LAI from the receiver has
			// not been applied by the replica.
			expectedClosed:      hlc.Timestamp{},
			expectedLocalUpdate: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := mockReceiver{
				closed: tc.receiverClosed,
				lai:    tc.receiverLAI,
			}
			var s sidetransportAccess
			s.receiver = &r
			s.mu.closedTimestamp = tc.localClosed
			s.mu.lai = tc.localLAI
			closed := s.get(ctx, roachpb.NodeID(1), tc.applied, tc.sufficient)
			require.Equal(t, tc.expectedClosed, closed)
			if tc.expectedLocalUpdate {
				require.Equal(t, tc.receiverClosed, s.mu.closedTimestamp)
				require.Equal(t, tc.receiverLAI, s.mu.lai)
			} else {
				require.Equal(t, tc.localClosed, s.mu.closedTimestamp)
				require.Equal(t, tc.localLAI, s.mu.lai)
			}
		})
	}
}

type mockReceiver struct {
	closed hlc.Timestamp
	lai    ctpb.LAI
}

var _ sidetransportReceiver = &mockReceiver{}

// GetClosedTimestamp is part of the sidetransportReceiver interface.
func (r *mockReceiver) GetClosedTimestamp(
	ctx context.Context, rangeID roachpb.RangeID, leaseholderNode roachpb.NodeID,
) (hlc.Timestamp, ctpb.LAI) {
	return r.closed, r.lai
}

// HTML is part of the sidetransportReceiver interface.
func (r *mockReceiver) HTML() string {
	return ""
}

// Test that r.GetClosedTimestampV2() mixes its sources of information correctly.
func TestReplicaClosedTimestampV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}

	for _, test := range []struct {
		name                string
		applied             ctpb.LAI
		raftClosed          hlc.Timestamp
		sidetransportClosed hlc.Timestamp
		sidetransportLAI    ctpb.LAI
		expClosed           hlc.Timestamp
	}{
		{
			name:                "raft closed ahead",
			applied:             10,
			raftClosed:          ts2,
			sidetransportClosed: ts1,
			sidetransportLAI:    5,
			expClosed:           ts2,
		},
		{
			name:                "sidetrans closed ahead",
			applied:             10,
			raftClosed:          ts1,
			sidetransportClosed: ts2,
			sidetransportLAI:    5,
			expClosed:           ts2,
		},
		{
			name:                "sidetrans ahead but replication behind",
			applied:             10,
			raftClosed:          ts1,
			sidetransportClosed: ts2,
			sidetransportLAI:    11,
			expClosed:           ts1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			receiver := &mockReceiver{
				closed: test.sidetransportClosed,
				lai:    test.sidetransportLAI,
			}
			var tc testContext
			tc.manualClock = hlc.NewManualClock(123) // required by StartWithStoreConfig
			cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
			cfg.TestingKnobs.DontCloseTimestamps = true
			cfg.ClosedTimestampReceiver = receiver
			tc.StartWithStoreConfig(t, stopper, cfg)
			tc.repl.mu.Lock()
			tc.repl.mu.state.RaftClosedTimestamp = test.raftClosed
			tc.repl.mu.state.LeaseAppliedIndex = uint64(test.applied)
			tc.repl.mu.Unlock()
			require.Equal(t, test.expClosed, tc.repl.GetClosedTimestampV2(ctx))
		})
	}
}

// TestQueryResolvedTimestamp verifies that QueryResolvedTimestamp requests
// behave as expected.
func TestQueryResolvedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	ts10 := hlc.Timestamp{WallTime: 10}
	ts20 := hlc.Timestamp{WallTime: 20}
	ts30 := hlc.Timestamp{WallTime: 30}
	intentKey := roachpb.Key("b")
	intentTS := ts20

	for _, test := range []struct {
		name          string
		span          [2]string
		closedTS      hlc.Timestamp
		expResolvedTS hlc.Timestamp
	}{
		{
			name:          "closed timestamp before earliest intent",
			span:          [2]string{"b", "d"},
			closedTS:      ts10,
			expResolvedTS: ts10,
		},
		{
			name:          "closed timestamp equal to earliest intent",
			span:          [2]string{"b", "d"},
			closedTS:      ts20,
			expResolvedTS: ts20.Prev(),
		},
		{
			name:          "closed timestamp after earliest intent",
			span:          [2]string{"b", "d"},
			closedTS:      ts30,
			expResolvedTS: ts20.Prev(),
		},
		{
			name:          "closed timestamp before non-overlapping intent",
			span:          [2]string{"c", "d"},
			closedTS:      ts10,
			expResolvedTS: ts10,
		},
		{
			name:          "closed timestamp equal to non-overlapping intent",
			span:          [2]string{"c", "d"},
			closedTS:      ts20,
			expResolvedTS: ts20,
		},
		{
			name:          "closed timestamp after non-overlapping intent",
			span:          [2]string{"c", "d"},
			closedTS:      ts30,
			expResolvedTS: ts30,
		},
		{
			name:          "closed timestamp before intent at end key",
			span:          [2]string{"a", "b"},
			closedTS:      ts10,
			expResolvedTS: ts10,
		},
		{
			name:          "closed timestamp equal to intent at end key",
			span:          [2]string{"a", "b"},
			closedTS:      ts20,
			expResolvedTS: ts20,
		},
		{
			name:          "closed timestamp after intent at end key",
			span:          [2]string{"a", "b"},
			closedTS:      ts30,
			expResolvedTS: ts30,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			// Create a single range.
			var tc testContext
			tc.manualClock = hlc.NewManualClock(1) // required by StartWithStoreConfig
			cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, 100*time.Nanosecond))
			cfg.TestingKnobs.DontCloseTimestamps = true
			tc.StartWithStoreConfig(t, stopper, cfg)

			// Write an intent.
			txn := roachpb.MakeTransaction("test", intentKey, 0, intentTS, 0)
			pArgs := putArgs(intentKey, []byte("val"))
			assignSeqNumsForReqs(&txn, &pArgs)
			_, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{Txn: &txn}, &pArgs)
			require.Nil(t, pErr)

			// Inject a closed timestamp.
			tc.repl.mu.Lock()
			tc.repl.mu.state.RaftClosedTimestamp = test.closedTS
			tc.repl.mu.Unlock()

			// Issue a QueryResolvedTimestamp request.
			resTS, err := tc.store.DB().QueryResolvedTimestamp(ctx, test.span[0], test.span[1], true)
			require.NoError(t, err)
			require.Equal(t, test.expResolvedTS, resTS)
		})
	}
}

// TestQueryResolvedTimestampResolvesAbandonedIntents verifies that
// QueryResolvedTimestamp requests attempt to asynchronously resolve intents
// that they encounter once the encountered intents are sufficiently stale.
func TestQueryResolvedTimestampResolvesAbandonedIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	ts10 := hlc.Timestamp{WallTime: 10}
	ts20 := hlc.Timestamp{WallTime: 20}

	// Create a single range.
	var tc testContext
	tc.manualClock = hlc.NewManualClock(1) // required by StartWithStoreConfig
	cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, 100*time.Nanosecond))
	cfg.TestingKnobs.DontCloseTimestamps = true
	tc.StartWithStoreConfig(t, stopper, cfg)

	// Write an intent.
	key := roachpb.Key("a")
	txn := roachpb.MakeTransaction("test", key, 0, ts10, 0)
	pArgs := putArgs(key, []byte("val"))
	assignSeqNumsForReqs(&txn, &pArgs)
	_, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{Txn: &txn}, &pArgs)
	require.Nil(t, pErr)

	intentExists := func() bool {
		t.Helper()
		gArgs := getArgs(key)
		assignSeqNumsForReqs(&txn, &gArgs)
		resp, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{Txn: &txn}, &gArgs)

		abortErr, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError)
		if ok && abortErr.Reason == roachpb.ABORT_REASON_ABORT_SPAN {
			// When the intent is resolved, it will be replaced by an abort span entry.
			return false
		}
		require.Nil(t, pErr)
		require.NotNil(t, resp.(*roachpb.GetResponse).Value)
		return true
	}
	require.True(t, intentExists())

	// Abort the txn, but don't resolve the intent (by not attaching lock spans).
	et, etH := endTxnArgs(&txn, false /* commit */)
	_, pErr = kv.SendWrappedWith(ctx, tc.Sender(), etH, &et)
	require.Nil(t, pErr)
	require.True(t, intentExists())

	// Inject a closed timestamp.
	tc.repl.mu.Lock()
	tc.repl.mu.state.RaftClosedTimestamp = ts20
	tc.repl.mu.Unlock()

	// Issue a QueryResolvedTimestamp request. Should return resolved timestamp
	// derived from the intent's write timestamp (which precedes the closed
	// timestamp). Should not trigger async intent resolution, because the intent
	// is not old enough.
	resTS, err := tc.store.DB().QueryResolvedTimestamp(ctx, "a", "c", true)
	require.NoError(t, err)
	require.Equal(t, ts10.Prev(), resTS)
	require.True(t, intentExists())

	// Drop kv.query_resolved_timestamp.intent_cleanup_age, then re-issue
	// QueryResolvedTimestamp request. Should return the same result, but
	// this time it should trigger async intent resolution.
	batcheval.QueryResolvedTimestampIntentCleanupAge.Override(ctx, &tc.store.ClusterSettings().SV, 0)
	resTS, err = tc.store.DB().QueryResolvedTimestamp(ctx, "a", "c", true)
	require.NoError(t, err)
	require.Equal(t, ts10.Prev(), resTS)
	require.Eventually(t, func() bool {
		return !intentExists()
	}, testutils.DefaultSucceedsSoonDuration, 10*time.Millisecond)

	// Now that the intent is removed, the resolved timestamp should have
	// advanced.
	resTS, err = tc.store.DB().QueryResolvedTimestamp(ctx, "a", "c", true)
	require.NoError(t, err)
	require.Equal(t, ts20, resTS)
}
