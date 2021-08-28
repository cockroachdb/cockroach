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
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestSideTransportClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// cur < next < rec
	cur := closedTimestamp{hlc.Timestamp{WallTime: 1}, 1}
	next := closedTimestamp{hlc.Timestamp{WallTime: 2}, 2}
	rec := closedTimestamp{hlc.Timestamp{WallTime: 3}, 3}

	tests := []struct {
		name       string
		curSet     bool
		nextSet    bool
		recSet     bool
		applied    ctpb.LAI
		sufficient hlc.Timestamp

		expClosed          hlc.Timestamp
		expCurUpdateToNext bool
		expCurUpdateToRec  bool
		expNextCleared     bool
		expNextUpdateToRec bool
	}{
		{
			name:      "all empty",
			expClosed: hlc.Timestamp{},
		},
		{
			name:      "current set",
			curSet:    true,
			applied:   cur.lai,
			expClosed: cur.ts,
		},
		{
			name:      "next set, next not reached",
			nextSet:   true,
			applied:   cur.lai,
			expClosed: hlc.Timestamp{},
		},
		{
			name:               "next set, next reached",
			nextSet:            true,
			applied:            next.lai,
			expClosed:          next.ts,
			expCurUpdateToNext: true,
			expNextCleared:     true,
		},
		{
			name:      "current + next set, next not reached",
			curSet:    true,
			nextSet:   true,
			applied:   cur.lai,
			expClosed: cur.ts,
		},
		{
			name:               "current + next set, next reached",
			curSet:             true,
			nextSet:            true,
			applied:            next.lai,
			expClosed:          next.ts,
			expCurUpdateToNext: true,
			expNextCleared:     true,
		},
		{
			name:               "receiver set, receiver not reached",
			recSet:             true,
			applied:            next.lai,
			expClosed:          hlc.Timestamp{},
			expNextUpdateToRec: true,
		},
		{
			name:              "receiver set, receiver reached",
			recSet:            true,
			applied:           rec.lai,
			expClosed:         rec.ts,
			expCurUpdateToRec: true,
		},
		{
			name:               "current + receiver set, receiver not reached",
			curSet:             true,
			recSet:             true,
			applied:            next.lai,
			expClosed:          cur.ts,
			expNextUpdateToRec: true,
		},
		{
			name:              "current + receiver set, receiver reached",
			curSet:            true,
			recSet:            true,
			applied:           rec.lai,
			expClosed:         rec.ts,
			expCurUpdateToRec: true,
		},
		{
			name:      "next + receiver, next not reached, receiver not reached",
			nextSet:   true,
			recSet:    true,
			applied:   cur.lai,
			expClosed: hlc.Timestamp{},
		},
		{
			name:               "next + receiver, next reached, receiver not reached",
			nextSet:            true,
			recSet:             true,
			applied:            next.lai,
			expClosed:          next.ts,
			expCurUpdateToNext: true,
			expNextUpdateToRec: true,
		},
		{
			name:              "next + receiver, next reached, receiver reached",
			nextSet:           true,
			recSet:            true,
			applied:           rec.lai,
			expClosed:         rec.ts,
			expCurUpdateToRec: true,
			expNextCleared:    true,
		},
		{
			name:      "current + next + receiver set, next not reached, receiver not reached",
			curSet:    true,
			nextSet:   true,
			recSet:    true,
			applied:   cur.lai,
			expClosed: cur.ts,
		},
		{
			name:               "current + next + receiver set, next reached, receiver not reached",
			curSet:             true,
			nextSet:            true,
			recSet:             true,
			applied:            next.lai,
			expClosed:          next.ts,
			expCurUpdateToNext: true,
			expNextUpdateToRec: true,
		},
		{
			name:              "current + next + receiver set, next reached, receiver reached",
			curSet:            true,
			nextSet:           true,
			recSet:            true,
			applied:           rec.lai,
			expClosed:         rec.ts,
			expCurUpdateToRec: true,
			expNextCleared:    true,
		},
		// Cases where current is sufficient.
		{
			name:       "current set, current sufficient",
			curSet:     true,
			applied:    cur.lai,
			sufficient: cur.ts,
			expClosed:  cur.ts,
		},
		{
			name:       "current + next set, next not reached, current sufficient",
			curSet:     true,
			nextSet:    true,
			applied:    cur.lai,
			sufficient: cur.ts,
			expClosed:  cur.ts,
		},
		{
			name:       "current + next set, next reached, current sufficient",
			curSet:     true,
			nextSet:    true,
			applied:    next.lai,
			sufficient: cur.ts,
			expClosed:  cur.ts,
		},
		{
			name:       "current + receiver set, receiver not reached, current sufficient",
			curSet:     true,
			recSet:     true,
			applied:    next.lai,
			sufficient: cur.ts,
			expClosed:  cur.ts,
		},
		{
			name:       "current + receiver set, receiver reached, current sufficient",
			curSet:     true,
			nextSet:    true,
			recSet:     true,
			applied:    rec.lai,
			sufficient: cur.ts,
			expClosed:  cur.ts,
		},
		{
			name:       "current + next + receiver set, next not reached, receiver not reached, current sufficient",
			curSet:     true,
			nextSet:    true,
			recSet:     true,
			applied:    cur.lai,
			sufficient: cur.ts,
			expClosed:  cur.ts,
		},
		{
			name:       "current + next + receiver set, next reached, receiver not reached, current sufficient",
			curSet:     true,
			nextSet:    true,
			recSet:     true,
			applied:    next.lai,
			sufficient: cur.ts,
			expClosed:  cur.ts,
		},
		{
			name:       "current + next + receiver set, next reached, receiver reached, current sufficient",
			curSet:     true,
			nextSet:    true,
			recSet:     true,
			applied:    rec.lai,
			sufficient: cur.ts,
			expClosed:  cur.ts,
		},
		// Cases where next is sufficient.
		{
			name:       "next set, next not reached, next sufficient",
			nextSet:    true,
			applied:    cur.lai,
			sufficient: next.ts,
			expClosed:  hlc.Timestamp{},
		},
		{
			name:               "next set, next reached, next sufficient",
			nextSet:            true,
			applied:            next.lai,
			sufficient:         next.ts,
			expClosed:          next.ts,
			expCurUpdateToNext: true,
			expNextCleared:     true,
		},
		{
			name:       "current + next set, next not reached, next sufficient",
			curSet:     true,
			nextSet:    true,
			applied:    cur.lai,
			sufficient: next.ts,
			expClosed:  cur.ts,
		},
		{
			name:               "current + next set, next reached, next sufficient",
			curSet:             true,
			nextSet:            true,
			applied:            next.lai,
			sufficient:         next.ts,
			expClosed:          next.ts,
			expCurUpdateToNext: true,
			expNextCleared:     true,
		},
		{
			name:       "next + receiver, next not reached, receiver not reached, next sufficient",
			nextSet:    true,
			recSet:     true,
			applied:    cur.lai,
			sufficient: next.ts,
			expClosed:  hlc.Timestamp{},
		},
		{
			name:               "next + receiver, next reached, receiver not reached, next sufficient",
			nextSet:            true,
			recSet:             true,
			applied:            next.lai,
			sufficient:         next.ts,
			expClosed:          next.ts,
			expCurUpdateToNext: true,
			expNextCleared:     true,
		},
		{
			name:               "next + receiver, next reached, receiver reached, next sufficient",
			nextSet:            true,
			recSet:             true,
			applied:            rec.lai,
			sufficient:         next.ts,
			expClosed:          next.ts,
			expCurUpdateToNext: true,
			expNextCleared:     true,
		},
		{
			name:       "current + next + receiver set, next not reached, receiver not reached, next sufficient",
			curSet:     true,
			nextSet:    true,
			recSet:     true,
			applied:    cur.lai,
			sufficient: next.ts,
			expClosed:  cur.ts,
		},
		{
			name:               "current + next + receiver set, next reached, receiver not reached, next sufficient",
			curSet:             true,
			nextSet:            true,
			recSet:             true,
			applied:            next.lai,
			sufficient:         next.ts,
			expClosed:          next.ts,
			expCurUpdateToNext: true,
			expNextCleared:     true,
		},
		{
			name:               "current + next + receiver set, next reached, receiver reached, next sufficient",
			curSet:             true,
			nextSet:            true,
			recSet:             true,
			applied:            rec.lai,
			sufficient:         next.ts,
			expClosed:          next.ts,
			expCurUpdateToNext: true,
			expNextCleared:     true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.False(t, tc.expCurUpdateToNext && tc.expCurUpdateToRec)
			require.False(t, tc.expNextCleared && tc.expNextUpdateToRec)

			var r mockReceiver
			var s sidetransportAccess
			s.receiver = &r
			if tc.curSet {
				s.mu.cur = cur
			}
			if tc.nextSet {
				s.mu.next = next
			}
			if tc.recSet {
				r.closedTimestamp = rec
			}
			curOrig, nextOrig := s.mu.cur, s.mu.next
			closed := s.get(ctx, roachpb.NodeID(1), tc.applied, tc.sufficient)
			require.Equal(t, tc.expClosed, closed)

			expCur, expNext := curOrig, nextOrig
			if tc.expCurUpdateToNext {
				expCur = next
			} else if tc.expCurUpdateToRec {
				expCur = rec
			}
			if tc.expNextCleared {
				expNext = closedTimestamp{}
			} else if tc.expNextUpdateToRec {
				expNext = rec
			}
			require.Equal(t, expCur, s.mu.cur)
			require.Equal(t, expNext, s.mu.next)
		})
	}
}

// TestSideTransportClosedMonotonic tests that sequential calls to
// sidetransportAccess.get return monotonically increasing timestamps.
func TestSideTransportClosedMonotonic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const testTime = 1 * time.Second
	ctx := context.Background()

	var truth struct {
		syncutil.Mutex
		closedTimestamp
	}
	var r mockReceiver
	var s sidetransportAccess
	s.receiver = &r

	var g errgroup.Group
	var done int32

	// Receiver goroutine: periodically modify the true closed timestamp and the
	// closed timestamp stored in the side transport receiver.
	g.Go(func() error {
		for atomic.LoadInt32(&done) == 0 {
			// Update the truth.
			truth.Lock()
			truth.ts = truth.ts.Next()
			if rand.Intn(2) == 0 {
				truth.lai++
			}
			cur := truth.closedTimestamp
			truth.Unlock()

			// Optionally update receiver.
			if rand.Intn(2) == 0 {
				r.Lock()
				r.closedTimestamp = cur
				r.Unlock()
			}

			// Rarely flush and clear receiver.
			if rand.Intn(10) == 0 {
				knownApplied := rand.Intn(2) == 0
				r.Lock()
				s.forward(ctx, r.ts, r.lai, knownApplied)
				r.closedTimestamp = closedTimestamp{}
				r.Unlock()
			}
		}
		return nil
	})

	// Observer goroutines: periodically read the closed timestamp from the side
	// transport access, with small variations in the parameters provided to get.
	// Regardless of what's provided, two sequential calls should never observe a
	// regression in the returned timestamp.
	const observers = 3
	for i := 0; i < observers; i++ {
		g.Go(func() error {
			var lastTS hlc.Timestamp
			var lastLAI ctpb.LAI
			for atomic.LoadInt32(&done) == 0 {
				// Determine which lease applied index to use.
				var lai ctpb.LAI
				switch rand.Intn(3) {
				case 0:
					lai = lastLAI
				case 1:
					lai = lastLAI - 1
				case 2:
					truth.Lock()
					lai = truth.lai
					truth.Unlock()
				}

				// Optionally provide a sufficient timestamp.
				var sufficient hlc.Timestamp
				if !lastTS.IsEmpty() {
					switch rand.Intn(4) {
					case 0:
					// No sufficient timestamp.
					case 1:
						sufficient = lastTS.Prev()
					case 2:
						sufficient = lastTS
					case 3:
						sufficient = lastTS.Next()
					}
				}

				curTS := s.get(ctx, roachpb.NodeID(1), lai, sufficient)
				if curTS.Less(lastTS) {
					return errors.Errorf("closed timestamp regression: %s -> %s", lastTS, curTS)
				}

				lastTS = curTS
				lastLAI = lai
			}
			return nil
		})
	}

	time.Sleep(testTime)
	atomic.StoreInt32(&done, 1)
	require.NoError(t, g.Wait())
}

type mockReceiver struct {
	syncutil.Mutex
	closedTimestamp
}

var _ sidetransportReceiver = &mockReceiver{}

// GetClosedTimestamp is part of the sidetransportReceiver interface.
func (r *mockReceiver) GetClosedTimestamp(
	ctx context.Context, rangeID roachpb.RangeID, leaseholderNode roachpb.NodeID,
) (hlc.Timestamp, ctpb.LAI) {
	r.Lock()
	defer r.Unlock()
	return r.ts, r.lai
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

			var r mockReceiver
			r.ts = test.sidetransportClosed
			r.lai = test.sidetransportLAI
			var tc testContext
			tc.manualClock = hlc.NewManualClock(123) // required by StartWithStoreConfig
			cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
			cfg.TestingKnobs.DontCloseTimestamps = true
			cfg.ClosedTimestampReceiver = &r
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

// TestServerSideBoundedStalenessNegotiation verifies that the server-side
// bounded staleness negotiation fast-path behaves as expected. For details,
// see (*Store).executeServerSideBoundedStalenessNegotiation.
func TestServerSideBoundedStalenessNegotiation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	ts10 := hlc.Timestamp{WallTime: 10}
	ts20 := hlc.Timestamp{WallTime: 20}
	ts30 := hlc.Timestamp{WallTime: 30}
	ts40 := hlc.Timestamp{WallTime: 40}
	emptyKey := roachpb.Key("a")
	intentKey := roachpb.Key("b")
	intentTS := ts20
	closedTS := ts30
	getEmptyKey := getArgs(emptyKey)
	getIntentKey := getArgs(intentKey)

	testutils.RunTrueAndFalse(t, "strict", func(t *testing.T, strict bool) {
		ifStrict := func(a, b string) string {
			if strict {
				return a
			}
			return b
		}

		for _, test := range []struct {
			name           string
			reqs           []roachpb.Request
			minTSBound     hlc.Timestamp
			maxTSBound     hlc.Timestamp
			withTS         bool // error case
			withTxn        bool // error case
			withWrongRange bool // error case

			expRespTS hlc.Timestamp
			expErr    string
		}{
			{
				name:       "empty key, min bound below closed ts",
				reqs:       []roachpb.Request{&getEmptyKey},
				minTSBound: ts20,
				expRespTS:  ts30,
			},
			{
				name:       "empty key, min bound equal to closed ts",
				reqs:       []roachpb.Request{&getEmptyKey},
				minTSBound: ts30,
				expRespTS:  ts30,
			},
			{
				name:       "empty key, min bound above closed ts",
				reqs:       []roachpb.Request{&getEmptyKey},
				minTSBound: ts40,
				expRespTS:  ts40, // for !strict case
				expErr: ifStrict(
					"bounded staleness read .* could not be satisfied",
					"", // no error, batch evaluated above closed timestamp on leaseholder
				),
			},
			{
				name:       "intent key, min bound below intent ts, min bound below closed ts",
				reqs:       []roachpb.Request{&getIntentKey},
				minTSBound: ts10,
				expRespTS:  ts20.Prev(),
			},
			{
				name:       "intent key, min bound equal to intent ts, min bound below closed ts",
				reqs:       []roachpb.Request{&getIntentKey},
				minTSBound: ts20,
				expErr: ifStrict(
					"bounded staleness read .* could not be satisfied",
					"conflicting intents on .*",
				),
			},
			{
				name:       "intent key, min bound above intent ts, min bound equal to closed ts",
				reqs:       []roachpb.Request{&getIntentKey},
				minTSBound: ts30,
				expErr: ifStrict(
					"bounded staleness read .* could not be satisfied",
					"conflicting intents on .*",
				),
			},
			{
				name:       "intent key, min bound above intent ts, min bound above closed ts",
				reqs:       []roachpb.Request{&getIntentKey},
				minTSBound: ts40,
				expErr: ifStrict(
					"bounded staleness read .* could not be satisfied",
					"conflicting intents on .*",
				),
			},
			{
				name:       "empty and intent key, min bound below intent ts, min bound below closed ts",
				reqs:       []roachpb.Request{&getEmptyKey, &getIntentKey},
				minTSBound: ts10,
				expRespTS:  ts20.Prev(),
			},
			{
				name:       "empty and intent key, min bound equal to intent ts, min bound below closed ts",
				reqs:       []roachpb.Request{&getEmptyKey, &getIntentKey},
				minTSBound: ts20,
				expErr: ifStrict(
					"bounded staleness read .* could not be satisfied",
					"conflicting intents on .*",
				),
			},
			{
				name:       "empty and intent key, min bound above intent ts, min bound equal to closed ts",
				reqs:       []roachpb.Request{&getEmptyKey, &getIntentKey},
				minTSBound: ts30,
				expErr: ifStrict(
					"bounded staleness read .* could not be satisfied",
					"conflicting intents on .*",
				),
			},
			{
				name:       "empty and intent key, min bound above intent ts, min bound above closed ts",
				reqs:       []roachpb.Request{&getEmptyKey, &getIntentKey},
				minTSBound: ts40,
				expErr: ifStrict(
					"bounded staleness read .* could not be satisfied",
					"conflicting intents on .*",
				),
			},
			{
				name:       "empty key, min and max bound below closed ts",
				reqs:       []roachpb.Request{&getEmptyKey},
				minTSBound: ts10,
				maxTSBound: ts20.Next(),
				expRespTS:  ts20,
			},
			{
				name:       "intent key, min and max bound below intent ts, min and max bound below closed ts",
				reqs:       []roachpb.Request{&getIntentKey},
				minTSBound: ts10,
				maxTSBound: ts10.Next(),
				expRespTS:  ts10,
			},
			{
				name:       "empty and intent key, min and max bound below intent ts, min and max bound below closed ts",
				reqs:       []roachpb.Request{&getEmptyKey, &getIntentKey},
				minTSBound: ts10,
				maxTSBound: ts10.Next(),
				expRespTS:  ts10,
			},
			{
				name:   "req without min_timestamp_bound",
				reqs:   []roachpb.Request{&getEmptyKey},
				expErr: "MinTimestampBound must be set in batch",
			},
			{
				name:       "req with equal min_timestamp_bound and max_timestamp_bound",
				reqs:       []roachpb.Request{&getEmptyKey},
				minTSBound: ts10,
				maxTSBound: ts10,
				expErr:     "MaxTimestampBound, if set in batch, must be greater than MinTimestampBound",
			},
			{
				name:       "req with inverted min_timestamp_bound and max_timestamp_bound",
				reqs:       []roachpb.Request{&getEmptyKey},
				minTSBound: ts20,
				maxTSBound: ts10,
				expErr:     "MaxTimestampBound, if set in batch, must be greater than MinTimestampBound",
			},
			{
				name:       "req with timestamp",
				reqs:       []roachpb.Request{&getEmptyKey},
				minTSBound: ts20,
				withTS:     true,
				expErr:     "MinTimestampBound and Timestamp cannot both be set in batch",
			},
			{
				name:       "req with transaction",
				reqs:       []roachpb.Request{&getEmptyKey},
				minTSBound: ts20,
				withTxn:    true,
				expErr:     "MinTimestampBound and Txn cannot both be set in batch",
			},
			{
				name:           "req with wrong range",
				reqs:           []roachpb.Request{&getEmptyKey},
				minTSBound:     ts20,
				withWrongRange: true,
				expErr:         "r2 was not found on s1",
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
				tc.repl.mu.state.RaftClosedTimestamp = closedTS
				tc.repl.mu.Unlock()

				// Construct and issue the request.
				var ba roachpb.BatchRequest
				ba.RangeID = tc.rangeID
				ba.BoundedStaleness = &roachpb.BoundedStalenessHeader{
					MinTimestampBound:       test.minTSBound,
					MinTimestampBoundStrict: strict,
					MaxTimestampBound:       test.maxTSBound,
				}
				ba.WaitPolicy = lock.WaitPolicy_Error
				if test.withTS {
					ba.Timestamp = ts20
				}
				if test.withTxn {
					ba.Txn = &txn
				}
				if test.withWrongRange {
					ba.RangeID++
				}
				ba.Add(test.reqs...)

				br, pErr := tc.store.Send(ctx, ba)
				if test.expErr == "" {
					require.Nil(t, pErr)
					require.NotNil(t, br)
					require.Equal(t, test.expRespTS, br.Timestamp)
					require.Equal(t, len(test.reqs), len(br.Responses))
				} else {
					require.Nil(t, br)
					require.NotNil(t, pErr)
					require.Regexp(t, test.expErr, pErr)
				}
			})
		}
	})
}
