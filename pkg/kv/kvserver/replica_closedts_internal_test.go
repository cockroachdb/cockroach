// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
		applied    kvpb.LeaseAppliedIndex
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
			var lastLAI kvpb.LeaseAppliedIndex
			for atomic.LoadInt32(&done) == 0 {
				// Determine which lease applied index to use.
				var lai kvpb.LeaseAppliedIndex
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
) (hlc.Timestamp, kvpb.LeaseAppliedIndex) {
	r.Lock()
	defer r.Unlock()
	return r.ts, r.lai
}

// HTML is part of the sidetransportReceiver interface.
func (r *mockReceiver) HTML() string {
	return ""
}

// Test that r.GetCurrentClosedTimestamp() mixes its sources of information
// correctly.
func TestReplicaClosedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}

	for _, test := range []struct {
		name                string
		applied             kvpb.LeaseAppliedIndex
		raftClosed          hlc.Timestamp
		sidetransportClosed hlc.Timestamp
		sidetransportLAI    kvpb.LeaseAppliedIndex
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
			tc.manualClock = timeutil.NewManualTime(timeutil.Unix(0, 123)) // required by StartWithStoreConfig
			cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
			cfg.TestingKnobs.DontCloseTimestamps = true
			cfg.ClosedTimestampReceiver = &r
			tc.StartWithStoreConfig(ctx, t, stopper, cfg)
			tc.repl.raftMu.Lock()
			tc.repl.mu.Lock()
			defer tc.repl.mu.Unlock()
			tc.repl.shMu.state.RaftClosedTimestamp = test.raftClosed
			tc.repl.shMu.state.LeaseAppliedIndex = test.applied
			tc.repl.raftMu.Unlock()
			// NB: don't release the mutex to make this test a bit more resilient to
			// problems that could arise should something propose a command to this
			// replica whose LeaseAppliedIndex we've mutated.
			require.Equal(t, test.expClosed, tc.repl.getCurrentClosedTimestampLocked(ctx, hlc.Timestamp{} /* sufficient */))
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
			tc.manualClock = timeutil.NewManualTime(timeutil.Unix(0, 1)) // required by StartWithStoreConfig
			cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
			cfg.TestingKnobs.DontCloseTimestamps = true
			// Make sure commands are visible by the time they are applied. Otherwise
			// this test can be flaky because we might have a lease applied index
			// assigned to a command that is committed but not applied yet. When we
			// then "commit" a command out of band, and the stored command gets
			// applied, their indexes will clash and cause a fatal error.
			cfg.TestingKnobs.DisableCanAckBeforeApplication = true
			tc.StartWithStoreConfig(ctx, t, stopper, cfg)

			// Write an intent.
			txn := roachpb.MakeTransaction("test", intentKey, 0, 0, intentTS, 0, 0, 0, false /* omitInRangefeeds */)
			{
				pArgs := putArgs(intentKey, []byte("val"))
				assignSeqNumsForReqs(&txn, &pArgs)
				_, pErr := kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{Txn: &txn}, &pArgs)
				require.Nil(t, pErr)
			}

			// NB: the put is now visible, in particular it has applied, thanks
			// to the testing knobs in this test.

			// Inject a closed timestamp.
			tc.repl.raftMu.Lock()
			tc.repl.mu.Lock()
			tc.repl.shMu.state.RaftClosedTimestamp = test.closedTS
			tc.repl.raftMu.Unlock()
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
	tc.manualClock = timeutil.NewManualTime(timeutil.Unix(0, 1)) // required by StartWithStoreConfig
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	cfg.TestingKnobs.DontCloseTimestamps = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	// Write an intent.
	key := roachpb.Key("a")
	txn := roachpb.MakeTransaction("test", key, 0, 0, ts10, 0, 0, 0, false /* omitInRangefeeds */)
	pArgs := putArgs(key, []byte("val"))
	assignSeqNumsForReqs(&txn, &pArgs)
	_, pErr := kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{Txn: &txn}, &pArgs)
	require.Nil(t, pErr)

	intentExists := func() bool {
		t.Helper()
		gArgs := getArgs(key)
		assignSeqNumsForReqs(&txn, &gArgs)
		resp, pErr := kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{Txn: &txn}, &gArgs)

		abortErr, ok := pErr.GetDetail().(*kvpb.TransactionAbortedError)
		if ok && abortErr.Reason == kvpb.ABORT_REASON_ABORT_SPAN {
			// When the intent is resolved, it will be replaced by an abort span entry.
			return false
		}
		require.Nil(t, pErr)
		require.NotNil(t, resp.(*kvpb.GetResponse).Value)
		return true
	}
	require.True(t, intentExists())

	// Abort the txn, but don't resolve the intent (by not attaching lock spans).
	et, etH := endTxnArgs(&txn, false /* commit */)
	_, pErr = kv.SendWrappedWith(ctx, tc.Sender(), etH, &et)
	require.Nil(t, pErr)
	require.True(t, intentExists())

	// Bump the clock and inject a closed timestamp.
	tc.manualClock.AdvanceTo(ts20.GoTime())
	tc.repl.raftMu.Lock()
	tc.repl.mu.Lock()
	tc.repl.shMu.state.RaftClosedTimestamp = ts20
	tc.repl.raftMu.Unlock()
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
			reqs           []kvpb.Request
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
				reqs:       []kvpb.Request{&getEmptyKey},
				minTSBound: ts20,
				expRespTS:  ts30,
			},
			{
				name:       "empty key, min bound equal to closed ts",
				reqs:       []kvpb.Request{&getEmptyKey},
				minTSBound: ts30,
				expRespTS:  ts30,
			},
			{
				name:       "empty key, min bound above closed ts",
				reqs:       []kvpb.Request{&getEmptyKey},
				minTSBound: ts40,
				expRespTS:  ts40, // for !strict case
				expErr: ifStrict(
					"bounded staleness read .* could not be satisfied",
					"", // no error, batch evaluated above closed timestamp on leaseholder
				),
			},
			{
				name:       "intent key, min bound below intent ts, min bound below closed ts",
				reqs:       []kvpb.Request{&getIntentKey},
				minTSBound: ts10,
				expRespTS:  ts20.Prev(),
			},
			{
				name:       "intent key, min bound equal to intent ts, min bound below closed ts",
				reqs:       []kvpb.Request{&getIntentKey},
				minTSBound: ts20,
				expErr: ifStrict(
					"bounded staleness read .* could not be satisfied",
					"conflicting locks on .*",
				),
			},
			{
				name:       "intent key, min bound above intent ts, min bound equal to closed ts",
				reqs:       []kvpb.Request{&getIntentKey},
				minTSBound: ts30,
				expErr: ifStrict(
					"bounded staleness read .* could not be satisfied",
					"conflicting locks on .*",
				),
			},
			{
				name:       "intent key, min bound above intent ts, min bound above closed ts",
				reqs:       []kvpb.Request{&getIntentKey},
				minTSBound: ts40,
				expErr: ifStrict(
					"bounded staleness read .* could not be satisfied",
					"conflicting locks on .*",
				),
			},
			{
				name:       "empty and intent key, min bound below intent ts, min bound below closed ts",
				reqs:       []kvpb.Request{&getEmptyKey, &getIntentKey},
				minTSBound: ts10,
				expRespTS:  ts20.Prev(),
			},
			{
				name:       "empty and intent key, min bound equal to intent ts, min bound below closed ts",
				reqs:       []kvpb.Request{&getEmptyKey, &getIntentKey},
				minTSBound: ts20,
				expErr: ifStrict(
					"bounded staleness read .* could not be satisfied",
					"conflicting locks on .*",
				),
			},
			{
				name:       "empty and intent key, min bound above intent ts, min bound equal to closed ts",
				reqs:       []kvpb.Request{&getEmptyKey, &getIntentKey},
				minTSBound: ts30,
				expErr: ifStrict(
					"bounded staleness read .* could not be satisfied",
					"conflicting locks on .*",
				),
			},
			{
				name:       "empty and intent key, min bound above intent ts, min bound above closed ts",
				reqs:       []kvpb.Request{&getEmptyKey, &getIntentKey},
				minTSBound: ts40,
				expErr: ifStrict(
					"bounded staleness read .* could not be satisfied",
					"conflicting locks on .*",
				),
			},
			{
				name:       "empty key, min and max bound below closed ts",
				reqs:       []kvpb.Request{&getEmptyKey},
				minTSBound: ts10,
				maxTSBound: ts20.Next(),
				expRespTS:  ts20,
			},
			{
				name:       "intent key, min and max bound below intent ts, min and max bound below closed ts",
				reqs:       []kvpb.Request{&getIntentKey},
				minTSBound: ts10,
				maxTSBound: ts10.Next(),
				expRespTS:  ts10,
			},
			{
				name:       "empty and intent key, min and max bound below intent ts, min and max bound below closed ts",
				reqs:       []kvpb.Request{&getEmptyKey, &getIntentKey},
				minTSBound: ts10,
				maxTSBound: ts10.Next(),
				expRespTS:  ts10,
			},
			{
				name:   "req without min_timestamp_bound",
				reqs:   []kvpb.Request{&getEmptyKey},
				expErr: "MinTimestampBound must be set in batch",
			},
			{
				name:       "req with equal min_timestamp_bound and max_timestamp_bound",
				reqs:       []kvpb.Request{&getEmptyKey},
				minTSBound: ts10,
				maxTSBound: ts10,
				expErr:     "MaxTimestampBound, if set in batch, must be greater than MinTimestampBound",
			},
			{
				name:       "req with inverted min_timestamp_bound and max_timestamp_bound",
				reqs:       []kvpb.Request{&getEmptyKey},
				minTSBound: ts20,
				maxTSBound: ts10,
				expErr:     "MaxTimestampBound, if set in batch, must be greater than MinTimestampBound",
			},
			{
				name:       "req with timestamp",
				reqs:       []kvpb.Request{&getEmptyKey},
				minTSBound: ts20,
				withTS:     true,
				expErr:     "MinTimestampBound and Timestamp cannot both be set in batch",
			},
			{
				name:       "req with transaction",
				reqs:       []kvpb.Request{&getEmptyKey},
				minTSBound: ts20,
				withTxn:    true,
				expErr:     "MinTimestampBound and Txn cannot both be set in batch",
			},
			{
				name:           "req with wrong range",
				reqs:           []kvpb.Request{&getEmptyKey},
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
				tc.manualClock = timeutil.NewManualTime(timeutil.Unix(0, 1)) // required by StartWithStoreConfig
				cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
				cfg.TestingKnobs.DontCloseTimestamps = true
				cfg.TestingKnobs.DisableCanAckBeforeApplication = true
				tc.StartWithStoreConfig(ctx, t, stopper, cfg)

				// Write an intent.
				txn := roachpb.MakeTransaction("test", intentKey, 0, 0, intentTS, 0, 0, 0, false /* omitInRangefeeds */)
				pArgs := putArgs(intentKey, []byte("val"))
				assignSeqNumsForReqs(&txn, &pArgs)
				_, pErr := kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{Txn: &txn}, &pArgs)
				require.Nil(t, pErr)

				// Inject a closed timestamp.
				tc.repl.raftMu.Lock()
				tc.repl.mu.Lock()
				tc.repl.shMu.state.RaftClosedTimestamp = closedTS
				tc.repl.raftMu.Unlock()
				tc.repl.mu.Unlock()

				// Construct and issue the request.
				ba := &kvpb.BatchRequest{}
				ba.RangeID = tc.rangeID
				ba.BoundedStaleness = &kvpb.BoundedStalenessHeader{
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

// TestServerSideBoundedStalenessNegotiationWithResumeSpan verifies that the
// server-side bounded staleness negotiation fast-path negotiates a timestamp
// over a batch's entire set of read spans even if it only performs reads and
// returns results from part of them due to a key/byte limit.
func TestServerSideBoundedStalenessNegotiationWithResumeSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Setup:
	//
	//  a: value  @ 9
	//  b: value  @ 20 // ts above res ts, ignored by reads
	//  c: value  @ 5
	//  d: intent @ 20
	//  e: value  @ 10 // ts above res ts, ignored by reads
	//  f: value  @ 6
	//  g: intent @ 10
	//  h: value  @ 7
	//
	//  closed_timestamp: 30
	//
	//  expected_resolved_timestamp: 10.Prev()
	//
	makeTS := func(ts int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: ts}
	}
	setup := func(t *testing.T, tc *testContext) hlc.Timestamp {
		// Write values and intents.
		val := []byte("val")
		send := func(h kvpb.Header, args kvpb.Request) {
			_, pErr := kv.SendWrappedWith(ctx, tc.Sender(), h, args)
			require.Nil(t, pErr)
		}
		writeValue := func(k string, ts int64) {
			pArgs := putArgs(roachpb.Key(k), val)
			send(kvpb.Header{Timestamp: makeTS(ts)}, &pArgs)
		}
		writeIntent := func(k string, ts int64) {
			txn := roachpb.MakeTransaction("test", roachpb.Key(k), 0, 0, makeTS(ts), 0, 0, 0, false /* omitInRangefeeds */)
			pArgs := putArgs(roachpb.Key(k), val)
			assignSeqNumsForReqs(&txn, &pArgs)
			send(kvpb.Header{Txn: &txn}, &pArgs)
		}
		writeValue("a", 9)
		writeValue("b", 20)
		writeValue("c", 5)
		writeIntent("d", 20)
		writeValue("e", 10)
		writeValue("f", 6)
		writeIntent("g", 10)
		writeValue("h", 7)

		// Inject a closed timestamp.
		tc.repl.raftMu.Lock()
		tc.repl.mu.Lock()
		tc.repl.shMu.state.RaftClosedTimestamp = makeTS(30)
		tc.repl.raftMu.Unlock()
		tc.repl.mu.Unlock()

		// Return the timestamp of the earliest intent.
		return makeTS(10)
	}

	// Reqs:
	//
	//  scan: [a, f)
	//  get:  [f]
	//  get:  [g]
	//  get:  [h]
	//
	makeReq := func(maxKeys int) *kvpb.BatchRequest {
		ba := &kvpb.BatchRequest{}
		ba.BoundedStaleness = &kvpb.BoundedStalenessHeader{
			MinTimestampBound: makeTS(5),
		}
		ba.WaitPolicy = lock.WaitPolicy_Error
		ba.MaxSpanRequestKeys = int64(maxKeys)

		scan := scanArgsString("a", "f")
		get1 := getArgsString("f")
		get2 := getArgsString("g")
		get3 := getArgsString("h")
		ba.Add(scan, get1, get2, get3)
		return ba
	}

	for _, test := range []struct {
		maxKeys int

		expRespKeys   []string
		expResumeSpan string
	}{
		{
			maxKeys:     0, // no limit
			expRespKeys: []string{"a", "c", "f", "h"},
		},
		{
			maxKeys:       1,
			expRespKeys:   []string{"a"},
			expResumeSpan: "b",
		},
		{
			maxKeys:       2,
			expRespKeys:   []string{"a", "c"},
			expResumeSpan: "d",
		},
		{
			maxKeys:       3,
			expRespKeys:   []string{"a", "c", "f"},
			expResumeSpan: "g",
		},
		{
			maxKeys:     4,
			expRespKeys: []string{"a", "c", "f", "h"},
		},
		{
			maxKeys:     5,
			expRespKeys: []string{"a", "c", "f", "h"},
		},
	} {
		t.Run(fmt.Sprintf("maxKeys=%d", test.maxKeys), func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			// Create a single range.
			var tc testContext
			tc.manualClock = timeutil.NewManualTime(timeutil.Unix(0, 1)) // required by StartWithStoreConfig
			cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
			cfg.TestingKnobs.DontCloseTimestamps = true
			cfg.TestingKnobs.DisableCanAckBeforeApplication = true
			tc.StartWithStoreConfig(ctx, t, stopper, cfg)

			// Set up the test.
			earliestIntentTS := setup(t, &tc)

			// Construct the request.
			ba := makeReq(test.maxKeys)
			ba.RangeID = tc.rangeID

			// Regardless of the keys returned, we expect negotiation to decide on the
			// timestamp immediately preceding the earliest intent. This is the
			// primary focus of this test. It would be incorrect for the negotiated
			// spans to be limited to only the spans read before the key limit was
			// reached.
			expRespTS := earliestIntentTS.Prev()

			// Issue the request.
			br, pErr := tc.store.Send(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)
			require.Equal(t, expRespTS, br.Timestamp)
			require.Equal(t, len(ba.Requests), len(br.Responses))

			// Validate response keys and resume span.
			var respKeys []string
			var resumeStart roachpb.Key
			for i, ru := range br.Responses {
				req := ru.GetInner()
				switch v := req.(type) {
				case *kvpb.ScanResponse:
					for _, kv := range v.Rows {
						respKeys = append(respKeys, string(kv.Key))
					}
				case *kvpb.GetResponse:
					if v.Value.IsPresent() {
						respKeys = append(respKeys, string(ba.Requests[i].GetGet().Key))
					}
				default:
					t.Fatalf("unexpected response type %T", req)
				}
				if resume := req.Header().ResumeSpan; resume != nil {
					if resumeStart == nil || resume.Key.Compare(resumeStart) < 0 {
						resumeStart = resume.Key
					}
				}
			}
			require.Equal(t, test.expRespKeys, respKeys)
			if test.expResumeSpan == "" {
				require.Nil(t, resumeStart)
			} else {
				require.NotNil(t, resumeStart)
				require.Equal(t, test.expResumeSpan, string(resumeStart))
			}
		})
	}
}
