// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

type mockIntentResolver struct {
	pushTxn        func(context.Context, *enginepb.TxnMeta, roachpb.Header, roachpb.PushTxnType) (*roachpb.Transaction, *Error)
	resolveIntent  func(context.Context, roachpb.LockUpdate) *Error
	resolveIntents func(context.Context, []roachpb.LockUpdate) *Error
}

// mockIntentResolver implements the IntentResolver interface.
func (m *mockIntentResolver) PushTransaction(
	ctx context.Context, txn *enginepb.TxnMeta, h roachpb.Header, pushType roachpb.PushTxnType,
) (*roachpb.Transaction, *Error) {
	return m.pushTxn(ctx, txn, h, pushType)
}

func (m *mockIntentResolver) ResolveIntent(
	ctx context.Context, intent roachpb.LockUpdate, _ intentresolver.ResolveOptions,
) *Error {
	return m.resolveIntent(ctx, intent)
}

func (m *mockIntentResolver) ResolveIntents(
	ctx context.Context, intents []roachpb.LockUpdate, opts intentresolver.ResolveOptions,
) *Error {
	return m.resolveIntents(ctx, intents)
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
func (g *mockLockTableGuard) CurState() waitingState {
	s := g.state
	if g.stateObserved != nil {
		g.stateObserved <- struct{}{}
	}
	return s
}
func (g *mockLockTableGuard) ResolveBeforeScanning() []roachpb.LockUpdate {
	return g.toResolve
}
func (g *mockLockTableGuard) CheckOptimisticNoConflicts(*spanset.SpanSet) (ok bool) {
	return true
}
func (g *mockLockTableGuard) notify() { g.signal <- struct{}{} }

// mockLockTable overrides TransactionIsFinalized, which is the only LockTable
// method that should be called in this test.
type mockLockTable struct {
	lockTableImpl
	txnFinalizedFn func(txn *roachpb.Transaction)
}

func (lt *mockLockTable) TransactionIsFinalized(txn *roachpb.Transaction) {
	lt.txnFinalizedFn(txn)
}

var lockTableWaiterTestClock = hlc.Timestamp{WallTime: 12}

func setupLockTableWaiterTest() (*lockTableWaiterImpl, *mockIntentResolver, *mockLockTableGuard) {
	ir := &mockIntentResolver{}
	st := cluster.MakeTestingClusterSettings()
	LockTableLivenessPushDelay.Override(context.Background(), &st.SV, 0)
	LockTableDeadlockDetectionPushDelay.Override(context.Background(), &st.SV, 0)
	manual := hlc.NewManualClock(lockTableWaiterTestClock.WallTime)
	guard := &mockLockTableGuard{
		signal: make(chan struct{}, 1),
	}
	w := &lockTableWaiterImpl{
		st:                                  st,
		clock:                               hlc.NewClock(manual.UnixNano, time.Nanosecond),
		stopper:                             stop.NewStopper(),
		ir:                                  ir,
		lt:                                  &mockLockTable{},
		conflictingIntentsResolveRejections: metric.NewCounter(metric.Metadata{}),
	}
	return w, ir, guard
}

func makeTxnProto(name string) roachpb.Transaction {
	return roachpb.MakeTransaction(name, []byte("key"), 0, hlc.Timestamp{WallTime: 10}, 0)
}

// TestLockTableWaiterWithTxn tests the lockTableWaiter's behavior under
// different waiting states while a transactional request is waiting.
func TestLockTableWaiterWithTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testutils.RunTrueAndFalse(t, "synthetic", func(t *testing.T, synthetic bool) {
		uncertaintyLimit := hlc.Timestamp{WallTime: 15}
		makeReq := func() Request {
			txn := makeTxnProto("request")
			txn.GlobalUncertaintyLimit = uncertaintyLimit
			if synthetic {
				txn.ReadTimestamp = txn.ReadTimestamp.WithSynthetic(true)
			}
			return Request{
				Txn:       &txn,
				Timestamp: txn.ReadTimestamp,
			}
		}

		expPushTS := func() hlc.Timestamp {
			// If the waiter has a synthetic timestamp, it pushes all the way up to
			// its global uncertainty limit, because it won't be able to use a local
			// uncertainty limit to ignore a synthetic intent. If the waiter does not
			// have a synthetic timestamp, it uses the local clock to bound its push
			// timestamp, with the assumption that it will be able to use its local
			// uncertainty limit to ignore a non-synthetic intent. For more, see
			// lockTableWaiterImpl.pushHeader.
			if synthetic {
				return uncertaintyLimit.WithSynthetic(true)
			}
			// NOTE: lockTableWaiterTestClock < uncertaintyLimit
			return lockTableWaiterTestClock
		}

		t.Run("state", func(t *testing.T) {
			t.Run("waitFor", func(t *testing.T) {
				testWaitPush(t, waitFor, makeReq, expPushTS())
			})

			t.Run("waitForDistinguished", func(t *testing.T) {
				testWaitPush(t, waitForDistinguished, makeReq, expPushTS())
			})

			t.Run("waitElsewhere", func(t *testing.T) {
				testWaitPush(t, waitElsewhere, makeReq, expPushTS())
			})

			t.Run("waitSelf", func(t *testing.T) {
				testWaitNoopUntilDone(t, waitSelf, makeReq)
			})

			t.Run("doneWaiting", func(t *testing.T) {
				w, _, g := setupLockTableWaiterTest()
				defer w.stopper.Stop(ctx)

				g.state = waitingState{kind: doneWaiting}
				g.notify()

				err := w.WaitOn(ctx, makeReq(), g)
				require.Nil(t, err)
			})
		})

		t.Run("ctx done", func(t *testing.T) {
			w, _, g := setupLockTableWaiterTest()
			defer w.stopper.Stop(ctx)

			ctxWithCancel, cancel := context.WithCancel(ctx)
			go cancel()

			err := w.WaitOn(ctxWithCancel, makeReq(), g)
			require.NotNil(t, err)
			require.Equal(t, context.Canceled.Error(), err.GoError().Error())
		})

		t.Run("stopper quiesce", func(t *testing.T) {
			w, _, g := setupLockTableWaiterTest()
			defer w.stopper.Stop(ctx)

			go func() {
				w.stopper.Quiesce(ctx)
			}()

			err := w.WaitOn(ctx, makeReq(), g)
			require.NotNil(t, err)
			require.IsType(t, &roachpb.NodeUnavailableError{}, err.GetDetail())
		})
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
		return Request{
			Timestamp: reqHeaderTS,
			Priority:  roachpb.NormalUserPriority,
		}
	}

	t.Run("state", func(t *testing.T) {
		t.Run("waitFor", func(t *testing.T) {
			t.Log("waitFor does not cause non-transactional requests to push")
			testWaitNoopUntilDone(t, waitFor, makeReq)
		})

		t.Run("waitForDistinguished", func(t *testing.T) {
			testWaitPush(t, waitForDistinguished, makeReq, reqHeaderTS)
		})

		t.Run("waitElsewhere", func(t *testing.T) {
			testWaitPush(t, waitElsewhere, makeReq, reqHeaderTS)
		})

		t.Run("waitSelf", func(t *testing.T) {
			t.Log("waitSelf is not possible for non-transactional request")
		})

		t.Run("doneWaiting", func(t *testing.T) {
			w, _, g := setupLockTableWaiterTest()
			defer w.stopper.Stop(ctx)

			g.state = waitingState{kind: doneWaiting}
			g.notify()

			err := w.WaitOn(ctx, makeReq(), g)
			require.Nil(t, err)
		})
	})

	t.Run("ctx done", func(t *testing.T) {
		w, _, g := setupLockTableWaiterTest()
		defer w.stopper.Stop(ctx)

		ctxWithCancel, cancel := context.WithCancel(ctx)
		go cancel()

		err := w.WaitOn(ctxWithCancel, makeReq(), g)
		require.NotNil(t, err)
		require.Equal(t, context.Canceled.Error(), err.GoError().Error())
	})

	t.Run("stopper quiesce", func(t *testing.T) {
		w, _, g := setupLockTableWaiterTest()
		defer w.stopper.Stop(ctx)

		go func() {
			w.stopper.Quiesce(ctx)
		}()

		err := w.WaitOn(ctx, makeReq(), g)
		require.NotNil(t, err)
		require.IsType(t, &roachpb.NodeUnavailableError{}, err.GetDetail())
	})
}

func testWaitPush(t *testing.T, k waitKind, makeReq func() Request, expPushTS hlc.Timestamp) {
	ctx := context.Background()
	keyA := roachpb.Key("keyA")
	testutils.RunTrueAndFalse(t, "lockHeld", func(t *testing.T, lockHeld bool) {
		testutils.RunTrueAndFalse(t, "waitAsWrite", func(t *testing.T, waitAsWrite bool) {
			w, ir, g := setupLockTableWaiterTest()
			defer w.stopper.Stop(ctx)
			pusheeTxn := makeTxnProto("pushee")

			req := makeReq()
			g.state = waitingState{
				kind:        k,
				txn:         &pusheeTxn.TxnMeta,
				key:         keyA,
				held:        lockHeld,
				guardAccess: spanset.SpanReadOnly,
			}
			if waitAsWrite {
				g.state.guardAccess = spanset.SpanReadWrite
			}
			g.notify()

			// waitElsewhere does not cause a push if the lock is not held.
			// It returns immediately.
			if k == waitElsewhere && !lockHeld {
				err := w.WaitOn(ctx, req, g)
				require.Nil(t, err)
				return
			}

			// Non-transactional requests do not push reservations, only locks.
			// They wait for doneWaiting.
			if req.Txn == nil && !lockHeld {
				defer notifyUntilDone(t, g)()
				err := w.WaitOn(ctx, req, g)
				require.Nil(t, err)
				return
			}

			ir.pushTxn = func(
				_ context.Context,
				pusheeArg *enginepb.TxnMeta,
				h roachpb.Header,
				pushType roachpb.PushTxnType,
			) (*roachpb.Transaction, *Error) {
				require.Equal(t, &pusheeTxn.TxnMeta, pusheeArg)
				require.Equal(t, req.Txn, h.Txn)
				require.Equal(t, expPushTS, h.Timestamp)
				if waitAsWrite || !lockHeld {
					require.Equal(t, roachpb.PUSH_ABORT, pushType)
				} else {
					require.Equal(t, roachpb.PUSH_TIMESTAMP, pushType)
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
					ir.resolveIntent = func(_ context.Context, intent roachpb.LockUpdate) *Error {
						require.Equal(t, keyA, intent.Key)
						require.Equal(t, pusheeTxn.ID, intent.Txn.ID)
						require.Equal(t, roachpb.ABORTED, intent.Status)
						g.state = waitingState{kind: doneWaiting}
						g.notify()
						return nil
					}
				} else {
					g.state = waitingState{kind: doneWaiting}
					g.notify()
				}
				return resp, nil
			}

			err := w.WaitOn(ctx, req, g)
			require.Nil(t, err)
		})
	})
}

func testWaitNoopUntilDone(t *testing.T, k waitKind, makeReq func() Request) {
	ctx := context.Background()
	w, _, g := setupLockTableWaiterTest()
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

func TestLockTableWaiterWithErrorWaitPolicy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	uncertaintyLimit := hlc.Timestamp{WallTime: 15}
	makeReq := func() Request {
		txn := makeTxnProto("request")
		txn.GlobalUncertaintyLimit = uncertaintyLimit
		return Request{
			Txn:        &txn,
			Timestamp:  txn.ReadTimestamp,
			WaitPolicy: lock.WaitPolicy_Error,
		}
	}

	// NOTE: lockTableWaiterTestClock < uncertaintyLimit
	expPushTS := lockTableWaiterTestClock

	t.Run("state", func(t *testing.T) {
		t.Run("waitFor", func(t *testing.T) {
			testErrorWaitPush(t, waitFor, makeReq, expPushTS)
		})

		t.Run("waitForDistinguished", func(t *testing.T) {
			testErrorWaitPush(t, waitForDistinguished, makeReq, expPushTS)
		})

		t.Run("waitElsewhere", func(t *testing.T) {
			testErrorWaitPush(t, waitElsewhere, makeReq, expPushTS)
		})

		t.Run("waitSelf", func(t *testing.T) {
			testWaitNoopUntilDone(t, waitSelf, makeReq)
		})

		t.Run("doneWaiting", func(t *testing.T) {
			w, _, g := setupLockTableWaiterTest()
			defer w.stopper.Stop(ctx)

			g.state = waitingState{kind: doneWaiting}
			g.notify()

			err := w.WaitOn(ctx, makeReq(), g)
			require.Nil(t, err)
		})
	})
}

func testErrorWaitPush(t *testing.T, k waitKind, makeReq func() Request, expPushTS hlc.Timestamp) {
	ctx := context.Background()
	keyA := roachpb.Key("keyA")
	testutils.RunTrueAndFalse(t, "lockHeld", func(t *testing.T, lockHeld bool) {
		w, ir, g := setupLockTableWaiterTest()
		defer w.stopper.Stop(ctx)
		pusheeTxn := makeTxnProto("pushee")

		req := makeReq()
		g.state = waitingState{
			kind:        k,
			txn:         &pusheeTxn.TxnMeta,
			key:         keyA,
			held:        lockHeld,
			guardAccess: spanset.SpanReadOnly,
		}
		g.notify()

		// If the lock is not held, expect an error immediately. The one
		// exception to this is waitElsewhere, which expects no error.
		if !lockHeld {
			err := w.WaitOn(ctx, req, g)
			if k == waitElsewhere {
				require.Nil(t, err)
			} else {
				require.NotNil(t, err)
				require.Regexp(t, "conflicting intents", err)
			}
			return
		}

		ir.pushTxn = func(
			_ context.Context,
			pusheeArg *enginepb.TxnMeta,
			h roachpb.Header,
			pushType roachpb.PushTxnType,
		) (*roachpb.Transaction, *Error) {
			require.Equal(t, &pusheeTxn.TxnMeta, pusheeArg)
			require.Equal(t, req.Txn, h.Txn)
			require.Equal(t, expPushTS, h.Timestamp)
			require.Equal(t, roachpb.PUSH_TOUCH, pushType)

			resp := &roachpb.Transaction{TxnMeta: *pusheeArg, Status: roachpb.ABORTED}

			// Next, we'll try to resolve the lock now that we know the
			// holder is ABORTED.
			w.lt.(*mockLockTable).txnFinalizedFn = func(txn *roachpb.Transaction) {
				require.Equal(t, pusheeTxn.ID, txn.ID)
				require.Equal(t, roachpb.ABORTED, txn.Status)
			}
			ir.resolveIntent = func(_ context.Context, intent roachpb.LockUpdate) *Error {
				require.Equal(t, keyA, intent.Key)
				require.Equal(t, pusheeTxn.ID, intent.Txn.ID)
				require.Equal(t, roachpb.ABORTED, intent.Status)
				g.state = waitingState{kind: doneWaiting}
				g.notify()
				return nil
			}
			return resp, nil
		}

		err := w.WaitOn(ctx, req, g)
		require.Nil(t, err)
	})
}

// TestLockTableWaiterIntentResolverError tests that the lockTableWaiter
// propagates errors from its intent resolver when it pushes transactions
// or resolves their intents.
func TestLockTableWaiterIntentResolverError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	w, ir, g := setupLockTableWaiterTest()
	defer w.stopper.Stop(ctx)

	err1 := roachpb.NewErrorf("error1")
	err2 := roachpb.NewErrorf("error2")

	txn := makeTxnProto("request")
	req := Request{
		Txn:       &txn,
		Timestamp: txn.ReadTimestamp,
	}

	// Test with both synchronous and asynchronous pushes.
	// See the comments on pushLockTxn and pushRequestTxn.
	testutils.RunTrueAndFalse(t, "sync", func(t *testing.T, sync bool) {
		keyA := roachpb.Key("keyA")
		pusheeTxn := makeTxnProto("pushee")
		lockHeld := sync
		g.state = waitingState{
			kind:        waitForDistinguished,
			txn:         &pusheeTxn.TxnMeta,
			key:         keyA,
			held:        lockHeld,
			guardAccess: spanset.SpanReadWrite,
		}

		// Errors are propagated when observed while pushing transactions.
		g.notify()
		ir.pushTxn = func(
			_ context.Context, _ *enginepb.TxnMeta, _ roachpb.Header, _ roachpb.PushTxnType,
		) (*roachpb.Transaction, *Error) {
			return nil, err1
		}
		err := w.WaitOn(ctx, req, g)
		require.Equal(t, err1, err)

		if lockHeld {
			// Errors are propagated when observed while resolving intents.
			g.notify()
			ir.pushTxn = func(
				_ context.Context, _ *enginepb.TxnMeta, _ roachpb.Header, _ roachpb.PushTxnType,
			) (*roachpb.Transaction, *Error) {
				return &pusheeTxn, nil
			}
			ir.resolveIntent = func(_ context.Context, intent roachpb.LockUpdate) *Error {
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
	w, ir, g := setupLockTableWaiterTest()
	defer w.stopper.Stop(ctx)

	txn := makeTxnProto("request")
	req := Request{
		Txn:       &txn,
		Timestamp: txn.ReadTimestamp,
	}
	keyA := roachpb.Key("keyA")
	pusheeTxn := makeTxnProto("pushee")
	// Make the pusheeTxn ABORTED so that the request avoids the transaction
	// record push and defers the intent resolution.
	pusheeTxn.Status = roachpb.ABORTED

	g.state = waitingState{
		kind:        doneWaiting,
		guardAccess: spanset.SpanReadWrite,
	}
	g.toResolve = []roachpb.LockUpdate{
		roachpb.MakeLockUpdate(&pusheeTxn, roachpb.Span{Key: keyA}),
	}
	g.notify()

	// Errors are propagated when observed while resolving batches of intents.
	err1 := roachpb.NewErrorf("error1")
	ir.resolveIntents = func(_ context.Context, intents []roachpb.LockUpdate) *Error {
		require.Len(t, intents, 1)
		require.Equal(t, keyA, intents[0].Key)
		require.Equal(t, pusheeTxn.ID, intents[0].Txn.ID)
		require.Equal(t, roachpb.ABORTED, intents[0].Status)
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

func TestContentionEventHelper(t *testing.T) {
	// This is mostly a regression test that ensures that we don't
	// accidentally update tBegin when continuing to handle the same event.
	// General coverage of the helper results from TestConcurrencyManagerBasic.

	tr := tracing.NewTracer()
	sp := tr.StartSpan("foo", tracing.WithForceRealSpan())

	var sl []*roachpb.ContentionEvent
	h := contentionEventHelper{
		sp: sp,
		onEvent: func(ev *roachpb.ContentionEvent) {
			sl = append(sl, ev)
		},
	}
	txn := makeTxnProto("foo")
	h.emitAndInit(waitingState{
		kind: waitForDistinguished,
		key:  roachpb.Key("a"),
		txn:  &txn.TxnMeta,
	})
	require.Empty(t, sl)
	require.NotZero(t, h.tBegin)
	tBegin := h.tBegin

	// Another event for the same txn/key should not mutate tBegin
	// or emit an event.
	h.emitAndInit(waitingState{
		kind: waitFor,
		key:  roachpb.Key("a"),
		txn:  &txn.TxnMeta,
	})
	require.Empty(t, sl)
	require.Equal(t, tBegin, h.tBegin)

	h.emitAndInit(waitingState{
		kind: waitForDistinguished,
		key:  roachpb.Key("b"),
		txn:  &txn.TxnMeta,
	})
	require.Len(t, sl, 1)
	require.Equal(t, txn.TxnMeta, sl[0].TxnMeta)
	require.Equal(t, roachpb.Key("a"), sl[0].Key)
	require.NotZero(t, sl[0].Duration)
}
