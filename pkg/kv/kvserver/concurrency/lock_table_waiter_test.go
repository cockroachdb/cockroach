// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package concurrency

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/stretchr/testify/require"
)

type mockIntentResolver struct {
	pushTxn        func(context.Context, *enginepb.TxnMeta, kvpb.Header, kvpb.PushTxnType) (*roachpb.Transaction, bool, *Error)
	resolveIntent  func(context.Context, roachpb.LockUpdate, intentresolver.ResolveOptions) *Error
	resolveIntents func(context.Context, []roachpb.LockUpdate, intentresolver.ResolveOptions) *Error
}

// mockIntentResolver implements the IntentResolver interface.
func (m *mockIntentResolver) PushTransaction(
	ctx context.Context, txn *enginepb.TxnMeta, h kvpb.Header, pushType kvpb.PushTxnType,
) (*roachpb.Transaction, bool, *Error) {
	return m.pushTxn(ctx, txn, h, pushType)
}

func (m *mockIntentResolver) ResolveIntent(
	ctx context.Context, intent roachpb.LockUpdate, opts intentresolver.ResolveOptions,
) *Error {
	return m.resolveIntent(ctx, intent, opts)
}

func (m *mockIntentResolver) ResolveIntents(
	ctx context.Context, intents []roachpb.LockUpdate, opts intentresolver.ResolveOptions,
) *Error {
	return m.resolveIntents(ctx, intents, opts)
}

type mockLockTableGuard struct {
	state         waitingState
	signal        chan struct{}
	stateObserved chan struct{}
	toResolve     []roachpb.LockUpdate
}

var _ lockTableGuard = &mockLockTableGuard{}

// mockLockTableGuard implements the lockTableGuard interface.
func (g *mockLockTableGuard) ShouldWait() bool            { return true }
func (g *mockLockTableGuard) NewStateChan() chan struct{} { return g.signal }
func (g *mockLockTableGuard) CurState() (waitingState, error) {
	s := g.state
	if g.stateObserved != nil {
		g.stateObserved <- struct{}{}
	}
	return s, nil
}
func (g *mockLockTableGuard) ResolveBeforeScanning() []roachpb.LockUpdate {
	return g.toResolve
}
func (g *mockLockTableGuard) CheckOptimisticNoConflicts(*lockspanset.LockSpanSet) (ok bool) {
	return true
}
func (g *mockLockTableGuard) IsKeyLockedByConflictingTxn(
	context.Context, roachpb.Key, lock.Strength,
) (bool, *enginepb.TxnMeta, error) {
	panic("unimplemented")
}
func (g *mockLockTableGuard) notify() { g.signal <- struct{}{} }

// mockLockTable overrides TransactionIsFinalized, which is the only LockTable
// method that should be called in this test.
type mockLockTable struct {
	lockTableImpl
	txnFinalizedFn func(txn *roachpb.Transaction)
}

func (lt *mockLockTable) TransactionUpdated(txn *roachpb.Transaction) {
	lt.txnFinalizedFn(txn)
}

var lockTableWaiterTestClock = hlc.Timestamp{WallTime: 12}

func setupLockTableWaiterTest() (
	*lockTableWaiterImpl,
	*mockIntentResolver,
	*mockLockTableGuard,
	*timeutil.ManualTime,
) {
	ir := &mockIntentResolver{}
	st := cluster.MakeTestingClusterSettings()
	LockTableDeadlockOrLivenessDetectionPushDelay.Override(context.Background(), &st.SV, 0)
	manual := timeutil.NewManualTime(lockTableWaiterTestClock.GoTime())
	guard := &mockLockTableGuard{
		signal: make(chan struct{}, 1),
	}
	w := &lockTableWaiterImpl{
		nodeDesc: &roachpb.NodeDescriptor{NodeID: 1},
		st:       st,
		clock:    hlc.NewClockForTesting(manual),
		stopper:  stop.NewStopper(),
		ir:       ir,
		lt:       &mockLockTable{},
	}
	return w, ir, guard, manual
}

func makeTxnProto(name string) roachpb.Transaction {
	return roachpb.MakeTransaction(name, []byte("key"), 0, 0, hlc.Timestamp{WallTime: 10}, 0, 6, 0, false /* omitInRangefeeds */)
}

// TestLockTableWaiterWithTxn tests the lockTableWaiter's behavior under
// different waiting states while a transactional request is waiting.
func TestLockTableWaiterWithTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	uncertaintyLimit := hlc.Timestamp{WallTime: 15}
	makeReq := func() Request {
		txn := makeTxnProto("request")
		txn.GlobalUncertaintyLimit = uncertaintyLimit
		ba := &kvpb.BatchRequest{}
		ba.Txn = &txn
		ba.Timestamp = txn.ReadTimestamp
		return Request{
			Txn:       &txn,
			Timestamp: ba.Timestamp,
			BaFmt:     ba,
		}
	}

	expPushTS := func() hlc.Timestamp {
		// The waiter uses the local clock to bound its push timestamp, with the
		// assumption that it will be able to use its local uncertainty limit to
		// ignore the intent. For more, see lockTableWaiterImpl.pushHeader.
		//
		// NOTE: lockTableWaiterTestClock < uncertaintyLimit
		return lockTableWaiterTestClock
	}

	t.Run("state", func(t *testing.T) {
		t.Run("waitFor", func(t *testing.T) {
			testWaitPush(t, waitFor, makeReq, expPushTS())
		})

		t.Run("waitElsewhere", func(t *testing.T) {
			testWaitPush(t, waitElsewhere, makeReq, expPushTS())
		})

		t.Run("waitSelf", func(t *testing.T) {
			testWaitNoopUntilDone(t, waitSelf, makeReq)
		})

		t.Run("waitQueueMaxLengthExceeded", func(t *testing.T) {
			testErrorWaitPush(t, waitQueueMaxLengthExceeded, makeReq, dontExpectPush, reasonWaitQueueMaxLengthExceeded)
		})

		t.Run("doneWaiting", func(t *testing.T) {
			w, _, g, _ := setupLockTableWaiterTest()
			defer w.stopper.Stop(ctx)

			g.state = waitingState{kind: doneWaiting}
			g.notify()

			err := w.WaitOn(ctx, makeReq(), g)
			require.Nil(t, err)
		})
	})

	t.Run("ctx done", func(t *testing.T) {
		w, _, g, _ := setupLockTableWaiterTest()
		defer w.stopper.Stop(ctx)

		ctxWithCancel, cancel := context.WithCancel(ctx)
		go cancel()

		err := w.WaitOn(ctxWithCancel, makeReq(), g)
		require.NotNil(t, err)
		require.Equal(t, context.Canceled.Error(), err.GoError().Error())
	})

	t.Run("stopper quiesce", func(t *testing.T) {
		w, _, g, _ := setupLockTableWaiterTest()
		defer w.stopper.Stop(ctx)

		go func() {
			w.stopper.Quiesce(ctx)
		}()

		err := w.WaitOn(ctx, makeReq(), g)
		require.NotNil(t, err)
		require.IsType(t, &kvpb.NodeUnavailableError{}, err.GetDetail())
	})
}

// TestLockTableWaiterWithNonTxn tests the lockTableWaiter's behavior under
// different waiting states while a non-transactional request is waiting.
func TestLockTableWaiterWithNonTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	reqHeaderTS := hlc.Timestamp{WallTime: 10}
	makeReq := func() Request {
		ba := &kvpb.BatchRequest{}
		ba.Timestamp = reqHeaderTS
		ba.UserPriority = roachpb.NormalUserPriority
		return Request{
			Timestamp:      ba.Timestamp,
			NonTxnPriority: ba.UserPriority,
			BaFmt:          ba,
		}
	}

	t.Run("state", func(t *testing.T) {
		t.Run("waitFor", func(t *testing.T) {
			testWaitPush(t, waitFor, makeReq, reqHeaderTS)
		})

		t.Run("waitElsewhere", func(t *testing.T) {
			testWaitPush(t, waitElsewhere, makeReq, reqHeaderTS)
		})

		t.Run("waitSelf", func(t *testing.T) {
			t.Log("waitSelf is not possible for non-transactional request")
		})

		t.Run("waitQueueMaxLengthExceeded", func(t *testing.T) {
			testErrorWaitPush(t, waitQueueMaxLengthExceeded, makeReq, dontExpectPush, reasonWaitQueueMaxLengthExceeded)
		})

		t.Run("doneWaiting", func(t *testing.T) {
			w, _, g, _ := setupLockTableWaiterTest()
			defer w.stopper.Stop(ctx)

			g.state = waitingState{kind: doneWaiting}
			g.notify()

			err := w.WaitOn(ctx, makeReq(), g)
			require.Nil(t, err)
		})
	})

	t.Run("ctx done", func(t *testing.T) {
		w, _, g, _ := setupLockTableWaiterTest()
		defer w.stopper.Stop(ctx)

		ctxWithCancel, cancel := context.WithCancel(ctx)
		go cancel()

		err := w.WaitOn(ctxWithCancel, makeReq(), g)
		require.NotNil(t, err)
		require.Equal(t, context.Canceled.Error(), err.GoError().Error())
	})

	t.Run("stopper quiesce", func(t *testing.T) {
		w, _, g, _ := setupLockTableWaiterTest()
		defer w.stopper.Stop(ctx)

		go func() {
			w.stopper.Quiesce(ctx)
		}()

		err := w.WaitOn(ctx, makeReq(), g)
		require.NotNil(t, err)
		require.IsType(t, &kvpb.NodeUnavailableError{}, err.GetDetail())
	})
}

func testWaitPush(t *testing.T, k waitKind, makeReq func() Request, expPushTS hlc.Timestamp) {
	ctx := context.Background()
	keyA := roachpb.Key("keyA")
	testutils.RunTrueAndFalse(t, "lockHeld", func(t *testing.T, lockHeld bool) {
		testutils.RunTrueAndFalse(t, "waitAsWrite", func(t *testing.T, waitAsWrite bool) {
			w, ir, g, _ := setupLockTableWaiterTest()
			defer w.stopper.Stop(ctx)
			pusheeTxn := makeTxnProto("pushee")

			req := makeReq()
			g.state = waitingState{
				kind:          k,
				txn:           &pusheeTxn.TxnMeta,
				key:           keyA,
				held:          lockHeld,
				guardStrength: lock.None,
			}
			if waitAsWrite {
				g.state.guardStrength = lock.Intent
			}
			g.notify()

			// waitElsewhere does not cause a push if the lock is not held.
			// It returns immediately.
			if k == waitElsewhere && !lockHeld {
				err := w.WaitOn(ctx, req, g)
				require.Nil(t, err)
				return
			}

			// Non-transactional requests without a timeout do not push transactions
			// that have claimed a lock; they only push lock holders. Instead, they
			// wait for doneWaiting.
			if req.Txn == nil && !lockHeld {
				defer notifyUntilDone(t, g)()
				err := w.WaitOn(ctx, req, g)
				require.Nil(t, err)
				return
			}

			ir.pushTxn = func(
				_ context.Context,
				pusheeArg *enginepb.TxnMeta,
				h kvpb.Header,
				pushType kvpb.PushTxnType,
			) (*roachpb.Transaction, bool, *Error) {
				require.Equal(t, &pusheeTxn.TxnMeta, pusheeArg)
				require.Equal(t, req.Txn, h.Txn)
				require.Equal(t, expPushTS, h.Timestamp)
				if waitAsWrite || !lockHeld {
					require.Equal(t, kvpb.PUSH_ABORT, pushType)
				} else {
					require.Equal(t, kvpb.PUSH_TIMESTAMP, pushType)
				}

				resp := &roachpb.Transaction{TxnMeta: *pusheeArg, Status: roachpb.ABORTED}

				// If the lock is held, we'll try to resolve it now that
				// we know the holder is ABORTED. Otherwise, immediately
				// tell the request to stop waiting.
				if lockHeld {
					w.lt.(*mockLockTable).txnFinalizedFn = func(txn *roachpb.Transaction) {
						require.Equal(t, pusheeTxn.ID, txn.ID)
						require.Equal(t, roachpb.ABORTED, txn.Status)
					}
					ir.resolveIntent = func(_ context.Context, intent roachpb.LockUpdate, opts intentresolver.ResolveOptions) *Error {
						require.Equal(t, keyA, intent.Key)
						require.Equal(t, pusheeTxn.ID, intent.Txn.ID)
						require.Equal(t, roachpb.ABORTED, intent.Status)
						require.Zero(t, intent.ClockWhilePending)
						require.True(t, opts.Poison)
						require.Equal(t, hlc.Timestamp{}, opts.MinTimestamp)
						g.state = waitingState{kind: doneWaiting}
						g.notify()
						return nil
					}
				} else {
					g.state = waitingState{kind: doneWaiting}
					g.notify()
				}
				return resp, false, nil
			}

			err := w.WaitOn(ctx, req, g)
			require.Nil(t, err)
		})
	})
}

func testWaitNoopUntilDone(t *testing.T, k waitKind, makeReq func() Request) {
	ctx := context.Background()
	w, _, g, _ := setupLockTableWaiterTest()
	defer w.stopper.Stop(ctx)

	txn := makeTxnProto("noop-wait-txn")
	g.state = waitingState{
		kind: k,
		txn:  &txn.TxnMeta,
	}
	g.notify()
	defer notifyUntilDone(t, g)()

	err := w.WaitOn(ctx, makeReq(), g)
	require.Nil(t, err)
}

func notifyUntilDone(t *testing.T, g *mockLockTableGuard) func() {
	// Set up an observer channel to detect when the current
	// waiting state is observed.
	g.stateObserved = make(chan struct{})
	done := make(chan struct{})
	go func() {
		<-g.stateObserved
		g.notify()
		<-g.stateObserved
		g.state = waitingState{kind: doneWaiting}
		g.notify()
		<-g.stateObserved
		close(done)
	}()
	return func() { <-done }
}

// TestLockTableWaiterWithErrorWaitPolicy tests the lockTableWaiter's behavior
// under different waiting states with an Error wait policy.
func TestLockTableWaiterWithErrorWaitPolicy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	uncertaintyLimit := hlc.Timestamp{WallTime: 15}
	makeReq := func() Request {
		txn := makeTxnProto("request")
		txn.GlobalUncertaintyLimit = uncertaintyLimit
		ba := &kvpb.BatchRequest{}
		ba.Txn = &txn
		ba.Timestamp = txn.ReadTimestamp
		ba.WaitPolicy = lock.WaitPolicy_Error
		return Request{
			Txn:        &txn,
			Timestamp:  ba.Timestamp,
			WaitPolicy: ba.WaitPolicy,
			BaFmt:      ba,
		}
	}
	makeHighPriReq := func() Request {
		req := makeReq()
		req.Txn.Priority = enginepb.MaxTxnPriority
		return req
	}

	// NOTE: lockTableWaiterTestClock < uncertaintyLimit
	expPushTS := lockTableWaiterTestClock

	t.Run("state", func(t *testing.T) {
		t.Run("waitFor", func(t *testing.T) {
			testErrorWaitPush(t, waitFor, makeReq, expPushTS, reasonWaitPolicy)
		})

		t.Run("waitFor high priority", func(t *testing.T) {
			testErrorWaitPush(t, waitFor, makeHighPriReq, expPushTS, reasonWaitPolicy)
		})

		t.Run("waitElsewhere", func(t *testing.T) {
			testErrorWaitPush(t, waitElsewhere, makeReq, expPushTS, reasonWaitPolicy)
		})

		t.Run("waitSelf", func(t *testing.T) {
			testWaitNoopUntilDone(t, waitSelf, makeReq)
		})

		t.Run("waitQueueMaxLengthExceeded", func(t *testing.T) {
			testErrorWaitPush(t, waitQueueMaxLengthExceeded, makeReq, dontExpectPush, reasonWaitQueueMaxLengthExceeded)
		})

		t.Run("doneWaiting", func(t *testing.T) {
			w, _, g, _ := setupLockTableWaiterTest()
			defer w.stopper.Stop(ctx)

			g.state = waitingState{kind: doneWaiting}
			g.notify()

			err := w.WaitOn(ctx, makeReq(), g)
			require.Nil(t, err)
		})
	})
}

var dontExpectPush = hlc.Timestamp{}

func testErrorWaitPush(
	t *testing.T,
	k waitKind,
	makeReq func() Request,
	expPushTS hlc.Timestamp,
	errReason kvpb.WriteIntentError_Reason,
) {
	ctx := context.Background()
	keyA := roachpb.Key("keyA")
	testutils.RunTrueAndFalse(t, "lockHeld", func(t *testing.T, lockHeld bool) {
		testutils.RunTrueAndFalse(t, "pusheeActive", func(t *testing.T, pusheeActive bool) {
			if !lockHeld && !pusheeActive {
				// !lockHeld means a lock claim, so is only possible when
				// pusheeActive is true.
				skip.IgnoreLint(t, "incompatible params")
			}

			w, ir, g, _ := setupLockTableWaiterTest()
			defer w.stopper.Stop(ctx)
			pusheeTxn := makeTxnProto("pushee")

			req := makeReq()
			g.state = waitingState{
				kind:          k,
				txn:           &pusheeTxn.TxnMeta,
				key:           keyA,
				held:          lockHeld,
				guardStrength: lock.None,
			}
			g.notify()

			// If expPushTS is empty, expect an error immediately.
			if expPushTS == dontExpectPush {
				err := w.WaitOn(ctx, req, g)
				require.NotNil(t, err)
				wiErr := new(kvpb.WriteIntentError)
				require.True(t, errors.As(err.GoError(), &wiErr))
				require.Equal(t, errReason, wiErr.Reason)
				return
			}

			// waitElsewhere does not cause a push if the lock is not held.
			// It returns immediately.
			if k == waitElsewhere && !lockHeld {
				err := w.WaitOn(ctx, req, g)
				require.Nil(t, err)
				return
			}

			ir.pushTxn = func(
				_ context.Context,
				pusheeArg *enginepb.TxnMeta,
				h kvpb.Header,
				pushType kvpb.PushTxnType,
			) (*roachpb.Transaction, bool, *Error) {
				require.Equal(t, &pusheeTxn.TxnMeta, pusheeArg)
				require.Equal(t, req.Txn, h.Txn)
				require.Equal(t, expPushTS, h.Timestamp)
				require.Equal(t, kvpb.PUSH_TIMESTAMP, pushType)

				resp := &roachpb.Transaction{TxnMeta: *pusheeArg, Status: roachpb.PENDING}
				if pusheeActive {
					return nil, false, kvpb.NewError(&kvpb.TransactionPushError{
						PusheeTxn: *resp,
					})
				}

				// Next, we'll try to resolve the lock now that we know the
				// holder is ABORTED.
				w.lt.(*mockLockTable).txnFinalizedFn = func(txn *roachpb.Transaction) {
					require.Equal(t, pusheeTxn.ID, txn.ID)
					require.Equal(t, roachpb.ABORTED, txn.Status)
				}
				ir.resolveIntent = func(_ context.Context, intent roachpb.LockUpdate, opts intentresolver.ResolveOptions) *Error {
					require.Equal(t, keyA, intent.Key)
					require.Equal(t, pusheeTxn.ID, intent.Txn.ID)
					require.Equal(t, roachpb.ABORTED, intent.Status)
					require.Zero(t, intent.ClockWhilePending)
					require.True(t, opts.Poison)
					require.Equal(t, hlc.Timestamp{}, opts.MinTimestamp)
					g.state = waitingState{kind: doneWaiting}
					g.notify()
					return nil
				}
				resp.Status = roachpb.ABORTED
				return resp, false, nil
			}

			err := w.WaitOn(ctx, req, g)
			if pusheeActive {
				require.NotNil(t, err)
				wiErr := new(kvpb.WriteIntentError)
				require.True(t, errors.As(err.GoError(), &wiErr))
				require.Equal(t, errReason, wiErr.Reason)
			} else {
				require.Nil(t, err)
			}
		})
	})
}

// TestLockTableWaiterWithLockTimeout tests the lockTableWaiter's behavior under
// different waiting states with a lock timeout.
func TestLockTableWaiterWithLockTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testutils.RunTrueAndFalse(t, "txn", func(t *testing.T, txn bool) {
		const lockTimeout = 1 * time.Millisecond
		makeReq := func() Request {
			txn := makeTxnProto("request")
			ba := &kvpb.BatchRequest{}
			ba.Txn = &txn
			ba.Timestamp = txn.ReadTimestamp
			ba.LockTimeout = lockTimeout
			return Request{
				Txn:         ba.Txn,
				Timestamp:   ba.Timestamp,
				LockTimeout: ba.LockTimeout,
				BaFmt:       ba,
			}
		}
		if !txn {
			makeReq = func() Request {
				ba := &kvpb.BatchRequest{}
				ba.Timestamp = hlc.Timestamp{WallTime: 10}
				ba.LockTimeout = lockTimeout
				return Request{
					Timestamp:   ba.Timestamp,
					LockTimeout: ba.LockTimeout,
					BaFmt:       ba,
				}
			}
		}

		t.Run("state", func(t *testing.T) {
			t.Run("waitFor", func(t *testing.T) {
				testWaitPushWithTimeout(t, waitFor, makeReq)
			})

			t.Run("waitElsewhere", func(t *testing.T) {
				testWaitPushWithTimeout(t, waitElsewhere, makeReq)
			})

			t.Run("waitSelf", func(t *testing.T) {
				testWaitNoopUntilDone(t, waitSelf, makeReq)
			})

			t.Run("waitQueueMaxLengthExceeded", func(t *testing.T) {
				testErrorWaitPush(t, waitQueueMaxLengthExceeded, makeReq, dontExpectPush, reasonWaitQueueMaxLengthExceeded)
			})

			t.Run("doneWaiting", func(t *testing.T) {
				w, _, g, _ := setupLockTableWaiterTest()
				defer w.stopper.Stop(ctx)

				g.state = waitingState{kind: doneWaiting}
				g.notify()

				err := w.WaitOn(ctx, makeReq(), g)
				require.Nil(t, err)
			})
		})
	})
}

func testWaitPushWithTimeout(t *testing.T, k waitKind, makeReq func() Request) {
	ctx := context.Background()
	keyA := roachpb.Key("keyA")
	testutils.RunTrueAndFalse(t, "lockHeld", func(t *testing.T, lockHeld bool) {
		testutils.RunTrueAndFalse(t, "pusheeActive", func(t *testing.T, pusheeActive bool) {
			testutils.RunTrueAndFalse(t, "timeoutBeforePush", func(t *testing.T, timeoutBeforePush bool) {
				if k == waitElsewhere && timeoutBeforePush {
					// waitElsewhere pushes immediately, so timeoutBeforePush is
					// irrelevant.
					skip.IgnoreLint(t, "incompatible params")
				}
				if !lockHeld && !pusheeActive {
					// !lockHeld means a lock is claimed, so is only possible when
					// pusheeActive is true.
					skip.IgnoreLint(t, "incompatible params")
				}

				w, ir, g, manual := setupLockTableWaiterTest()
				defer w.stopper.Stop(ctx)
				pusheeTxn := makeTxnProto("pushee")

				req := makeReq()
				g.state = waitingState{
					kind:          k,
					txn:           &pusheeTxn.TxnMeta,
					key:           keyA,
					held:          lockHeld,
					guardStrength: lock.Intent,
				}
				g.notify()

				// If the timeout should already be expired by the time that the
				// push is initiated, install a hook to manipulate the clock.
				if timeoutBeforePush {
					w.onPushTimer = func() {
						manual.Advance(req.LockTimeout)
					}
				}

				// waitElsewhere does not cause a push if the lock is not held.
				// It returns immediately.
				if !lockHeld && k == waitElsewhere {
					err := w.WaitOn(ctx, req, g)
					require.Nil(t, err)
					return
				}

				expBlockingPush := !timeoutBeforePush
				sawBlockingPush := false
				sawNonBlockingPush := false
				ir.pushTxn = func(
					ctx context.Context,
					pusheeArg *enginepb.TxnMeta,
					h kvpb.Header,
					pushType kvpb.PushTxnType,
				) (*roachpb.Transaction, bool, *Error) {
					require.Equal(t, &pusheeTxn.TxnMeta, pusheeArg)
					require.Equal(t, req.Txn, h.Txn)

					if expBlockingPush {
						require.Equal(t, kvpb.PUSH_ABORT, pushType)
						_, hasDeadline := ctx.Deadline()
						require.True(t, hasDeadline)
						sawBlockingPush = true
						expBlockingPush = false

						// Wait for the context to hit its timeout.
						<-ctx.Done()
						return nil, false, kvpb.NewError(ctx.Err())
					}

					require.Equal(t, kvpb.PUSH_ABORT, pushType)
					_, hasDeadline := ctx.Deadline()
					require.False(t, hasDeadline)
					sawNonBlockingPush = true
					require.Equal(t, lock.WaitPolicy_Error, h.WaitPolicy)

					resp := &roachpb.Transaction{TxnMeta: *pusheeArg, Status: roachpb.PENDING}
					if pusheeActive {
						return nil, false, kvpb.NewError(&kvpb.TransactionPushError{
							PusheeTxn: *resp,
						})
					}

					// Next, we'll try to resolve the lock now that we know the
					// holder is ABORTED.
					w.lt.(*mockLockTable).txnFinalizedFn = func(txn *roachpb.Transaction) {
						require.Equal(t, pusheeTxn.ID, txn.ID)
						require.Equal(t, roachpb.ABORTED, txn.Status)
					}
					ir.resolveIntent = func(_ context.Context, intent roachpb.LockUpdate, opts intentresolver.ResolveOptions) *Error {
						require.Equal(t, keyA, intent.Key)
						require.Equal(t, pusheeTxn.ID, intent.Txn.ID)
						require.Equal(t, roachpb.ABORTED, intent.Status)
						require.Zero(t, intent.ClockWhilePending)
						require.True(t, opts.Poison)
						require.Equal(t, hlc.Timestamp{}, opts.MinTimestamp)
						g.state = waitingState{kind: doneWaiting}
						g.notify()
						return nil
					}
					resp.Status = roachpb.ABORTED
					return resp, false, nil
				}

				err := w.WaitOn(ctx, req, g)
				if pusheeActive {
					require.NotNil(t, err)
					wiErr := new(kvpb.WriteIntentError)
					require.True(t, errors.As(err.GoError(), &wiErr))
					require.Equal(t, reasonLockTimeout, wiErr.Reason)
				} else {
					require.Nil(t, err)
				}
				require.Equal(t, !timeoutBeforePush, sawBlockingPush)
				require.True(t, sawNonBlockingPush)
			})
		})
	})
}

// TestLockTableWaiterIntentResolverError tests that the lockTableWaiter
// propagates errors from its intent resolver when it pushes transactions
// or resolves their intents.
func TestLockTableWaiterIntentResolverError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	w, ir, g, _ := setupLockTableWaiterTest()
	defer w.stopper.Stop(ctx)

	err1 := kvpb.NewErrorf("error1")
	err2 := kvpb.NewErrorf("error2")

	txn := makeTxnProto("request")
	ba := &kvpb.BatchRequest{}
	ba.Txn = &txn
	ba.Timestamp = txn.ReadTimestamp
	req := Request{
		Txn:       ba.Txn,
		Timestamp: ba.Timestamp,
		BaFmt:     ba,
	}

	// Test with both synchronous and asynchronous pushes.
	// See the comments on pushLockTxn and pushRequestTxn.
	testutils.RunTrueAndFalse(t, "sync", func(t *testing.T, sync bool) {
		keyA := roachpb.Key("keyA")
		pusheeTxn := makeTxnProto("pushee")
		lockHeld := sync
		g.state = waitingState{
			kind:          waitFor,
			txn:           &pusheeTxn.TxnMeta,
			key:           keyA,
			held:          lockHeld,
			guardStrength: lock.Intent,
		}

		// Errors are propagated when observed while pushing transactions.
		g.notify()
		ir.pushTxn = func(
			_ context.Context, _ *enginepb.TxnMeta, _ kvpb.Header, _ kvpb.PushTxnType,
		) (*roachpb.Transaction, bool, *Error) {
			return nil, false, err1
		}
		err := w.WaitOn(ctx, req, g)
		require.Equal(t, err1, err)

		if lockHeld {
			// Errors are propagated when observed while resolving intents.
			g.notify()
			ir.pushTxn = func(
				_ context.Context, _ *enginepb.TxnMeta, _ kvpb.Header, _ kvpb.PushTxnType,
			) (*roachpb.Transaction, bool, *Error) {
				return &pusheeTxn, false, nil
			}
			ir.resolveIntent = func(_ context.Context, intent roachpb.LockUpdate, opts intentresolver.ResolveOptions) *Error {
				return err2
			}
			err = w.WaitOn(ctx, req, g)
			require.Equal(t, err2, err)
		}
	})
}

// TestLockTableWaiterDeferredIntentResolverError tests that the lockTableWaiter
// propagates errors from its intent resolver when it resolves intent batches.
func TestLockTableWaiterDeferredIntentResolverError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	w, ir, g, _ := setupLockTableWaiterTest()
	defer w.stopper.Stop(ctx)

	txn := makeTxnProto("request")
	ba := &kvpb.BatchRequest{}
	ba.Txn = &txn
	ba.Timestamp = txn.ReadTimestamp
	req := Request{
		Txn:       ba.Txn,
		Timestamp: ba.Timestamp,
		BaFmt:     ba,
	}
	keyA := roachpb.Key("keyA")
	pusheeTxn := makeTxnProto("pushee")
	// Make the pusheeTxn ABORTED so that the request avoids the transaction
	// record push and defers the intent resolution.
	pusheeTxn.Status = roachpb.ABORTED

	g.state = waitingState{
		kind:          doneWaiting,
		guardStrength: lock.Intent,
	}
	g.toResolve = []roachpb.LockUpdate{
		roachpb.MakeLockUpdate(&pusheeTxn, roachpb.Span{Key: keyA}),
	}
	g.notify()

	// Errors are propagated when observed while resolving batches of intents.
	err1 := kvpb.NewErrorf("error1")
	ir.resolveIntents = func(_ context.Context, intents []roachpb.LockUpdate, opts intentresolver.ResolveOptions) *Error {
		require.Len(t, intents, 1)
		require.Equal(t, keyA, intents[0].Key)
		require.Equal(t, pusheeTxn.ID, intents[0].Txn.ID)
		require.Equal(t, roachpb.ABORTED, intents[0].Status)
		require.Zero(t, intents[0].ClockWhilePending)
		require.True(t, opts.Poison)
		require.Equal(t, hlc.Timestamp{}, opts.MinTimestamp)
		return err1
	}
	err := w.WaitOn(ctx, req, g)
	require.Equal(t, err1, err)
}

func TestTxnCache(t *testing.T) {
	var c txnCache
	const overflow = 4
	var txns [len(c.txns) + overflow]roachpb.Transaction
	for i := range txns {
		txns[i] = makeTxnProto(fmt.Sprintf("txn %d", i))
	}

	// Add each txn to the cache. Observe LRU eviction policy.
	for i := range txns {
		txn := &txns[i]
		c.add(txn)
		for j, txnInCache := range c.txns {
			if j <= i {
				require.Equal(t, &txns[i-j], txnInCache)
			} else {
				require.Nil(t, txnInCache)
			}
		}
	}

	// Access each txn in the cache in reverse order.
	// Should reverse the order of the cache because of LRU policy.
	for i := len(txns) - 1; i >= 0; i-- {
		txn := &txns[i]
		txnInCache, ok := c.get(txn.ID)
		if i < overflow {
			// Expect overflow.
			require.Nil(t, txnInCache)
			require.False(t, ok)
		} else {
			// Should be in cache.
			require.Equal(t, txn, txnInCache)
			require.True(t, ok)
		}
	}

	// Cache should be in order again.
	for i, txnInCache := range c.txns {
		require.Equal(t, &txns[i+overflow], txnInCache)
	}
}

func TestTxnCacheUpdatesTxn(t *testing.T) {
	var c txnCache

	// Add txn to cache.
	txnOrig := makeTxnProto("txn")
	c.add(txnOrig.Clone())
	txnInCache, ok := c.get(txnOrig.ID)
	require.True(t, ok)
	require.Equal(t, txnOrig.WriteTimestamp, txnInCache.WriteTimestamp)

	// Add pushed txn with higher write timestamp.
	txnPushed := txnOrig.Clone()
	txnPushed.WriteTimestamp.Forward(txnPushed.WriteTimestamp.Add(1, 0))
	c.add(txnPushed)
	txnInCache, ok = c.get(txnOrig.ID)
	require.True(t, ok)
	require.NotEqual(t, txnOrig.WriteTimestamp, txnInCache.WriteTimestamp)
	require.Equal(t, txnPushed.WriteTimestamp, txnInCache.WriteTimestamp)

	// Re-add txn with lower timestamp. Timestamp should not regress.
	c.add(txnOrig.Clone())
	txnInCache, ok = c.get(txnOrig.ID)
	require.True(t, ok)
	require.NotEqual(t, txnOrig.WriteTimestamp, txnInCache.WriteTimestamp)
	require.Equal(t, txnPushed.WriteTimestamp, txnInCache.WriteTimestamp)
}

func BenchmarkTxnCache(b *testing.B) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	var c txnCache
	var txns [len(c.txns) + 4]roachpb.Transaction
	for i := range txns {
		txns[i] = makeTxnProto(fmt.Sprintf("txn %d", i))
	}
	txnOps := make([]*roachpb.Transaction, b.N)
	for i := range txnOps {
		txnOps[i] = &txns[rng.Intn(len(txns))]
	}
	b.ResetTimer()
	for i, txnOp := range txnOps {
		if i%2 == 0 {
			c.add(txnOp)
		} else {
			_, _ = c.get(txnOp.ID)
		}
	}
}

func TestContentionEventTracer(t *testing.T) {
	tr := tracing.NewTracer()
	ctx, sp := tr.StartSpanCtx(context.Background(), "foo", tracing.WithRecording(tracingpb.RecordingVerbose))
	defer sp.Finish()
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	clock := hlc.NewClockForTesting(manual)

	txn1 := makeTxnProto("foo")
	txn2 := makeTxnProto("bar")

	var events []*kvpb.ContentionEvent

	h := newContentionEventTracer(sp, clock)
	h.SetOnContentionEvent(func(ev *kvpb.ContentionEvent) {
		events = append(events, ev)
	})
	h.notify(ctx, waitingState{
		kind: waitFor,
		key:  roachpb.Key("a"),
		txn:  &txn1.TxnMeta,
	})
	require.Zero(t, h.tag.mu.lockWait)
	require.NotZero(t, h.tag.mu.waitStart)
	require.Empty(t, events)
	rec := sp.GetRecording(tracingpb.RecordingVerbose)
	lockTagGroup := rec[0].FindTagGroup(tagContentionTracer)
	require.NotNil(t, lockTagGroup)
	val, ok := lockTagGroup.FindTag(tagNumLocks)
	require.True(t, ok)
	require.Equal(t, "1", val)
	_, ok = lockTagGroup.FindTag(tagWaited)
	require.False(t, ok)
	val, ok = lockTagGroup.FindTag(tagWaitKey)
	require.True(t, ok)
	require.Equal(t, "\"a\"", val)
	val, ok = lockTagGroup.FindTag(tagWaitStart)
	require.True(t, ok)
	require.Equal(t, "00:00:00.1112", val)
	val, ok = lockTagGroup.FindTag(tagLockHolderTxn)
	require.True(t, ok)
	require.Equal(t, txn1.ID.String(), val)

	// Another event for the same txn/key should not mutate
	// or emit an event.
	prevNumLocks := h.tag.mu.numLocks
	manual.Advance(10)
	h.notify(ctx, waitingState{
		kind: waitFor,
		key:  roachpb.Key("a"),
		txn:  &txn1.TxnMeta,
	})
	require.Empty(t, events)
	require.Zero(t, h.tag.mu.lockWait)
	require.Equal(t, prevNumLocks, h.tag.mu.numLocks)

	// Another event for the same key but different txn should
	// emitLocked an event.
	manual.Advance(11)
	h.notify(ctx, waitingState{
		kind: waitFor,
		key:  roachpb.Key("a"),
		txn:  &txn2.TxnMeta,
	})
	require.Equal(t, time.Duration(21) /* 10+11 */, h.tag.mu.lockWait)
	require.Len(t, events, 1)
	ev := events[0]
	require.Equal(t, txn1.TxnMeta, ev.TxnMeta)
	require.Equal(t, roachpb.Key("a"), ev.Key)
	require.Equal(t, time.Duration(21) /* 10+11 */, ev.Duration)

	// Another event for the same txn but different key should
	// emit an event.
	manual.Advance(12)
	h.notify(ctx, waitingState{
		kind: waitFor,
		key:  roachpb.Key("b"),
		txn:  &txn2.TxnMeta,
	})
	require.Equal(t, time.Duration(33) /* 10+11+12 */, h.tag.mu.lockWait)
	require.Len(t, events, 2)
	ev = events[1]
	require.Equal(t, txn2.TxnMeta, ev.TxnMeta)
	require.Equal(t, roachpb.Key("a"), ev.Key)
	require.Equal(t, time.Duration(12), ev.Duration)

	manual.Advance(13)
	h.notify(ctx, waitingState{kind: doneWaiting})
	require.Equal(t, time.Duration(46) /* 10+11+12+13 */, h.tag.mu.lockWait)
	require.Len(t, events, 3)

	h.notify(ctx, waitingState{
		kind: waitFor,
		key:  roachpb.Key("b"),
		txn:  &txn1.TxnMeta,
	})
	manual.Advance(14)
	h.notify(ctx, waitingState{
		kind: doneWaiting,
	})
	require.Equal(t, time.Duration(60) /* 10+11+12+13+14 */, h.tag.mu.lockWait)
	require.Len(t, events, 4)
	rec = sp.GetRecording(tracingpb.RecordingVerbose)
	lockTagGroup = rec[0].FindTagGroup(tagContentionTracer)
	require.NotNil(t, lockTagGroup)
	val, ok = lockTagGroup.FindTag(tagNumLocks)
	require.True(t, ok)
	require.Equal(t, "4", val)
	_, ok = lockTagGroup.FindTag(tagWaited)
	require.True(t, ok)
	_, ok = lockTagGroup.FindTag(tagWaitKey)
	require.False(t, ok)
	_, ok = lockTagGroup.FindTag(tagWaitStart)
	require.False(t, ok)
	_, ok = lockTagGroup.FindTag(tagLockHolderTxn)
	require.False(t, ok)

	// Create a new tracer on the same span and check that the new tracer
	// incorporates the info of the old one.
	anotherTracer := newContentionEventTracer(sp, clock)
	require.NotZero(t, anotherTracer.tag.mu.numLocks)
	require.Equal(t, h.tag.mu.numLocks, anotherTracer.tag.mu.numLocks)
	require.Equal(t, h.tag.mu.lockWait, anotherTracer.tag.mu.lockWait)
}
