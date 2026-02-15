// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnwait

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestShouldPushImmediately(t *testing.T) {
	defer leaktest.AfterTest(t)()

	min := enginepb.MinTxnPriority
	max := enginepb.MaxTxnPriority
	mid1 := enginepb.TxnPriority(1)
	mid2 := enginepb.TxnPriority(2)
	testCases := []struct {
		typ        kvpb.PushTxnType
		pusherPri  enginepb.TxnPriority
		pusheePri  enginepb.TxnPriority
		shouldPush bool
	}{
		{kvpb.PUSH_ABORT, min, min, false},
		{kvpb.PUSH_ABORT, min, mid1, false},
		{kvpb.PUSH_ABORT, min, mid2, false},
		{kvpb.PUSH_ABORT, min, max, false},
		{kvpb.PUSH_ABORT, mid1, min, true},
		{kvpb.PUSH_ABORT, mid1, mid1, false},
		{kvpb.PUSH_ABORT, mid1, mid2, false},
		{kvpb.PUSH_ABORT, mid1, max, false},
		{kvpb.PUSH_ABORT, mid2, min, true},
		{kvpb.PUSH_ABORT, mid2, mid1, false},
		{kvpb.PUSH_ABORT, mid2, mid2, false},
		{kvpb.PUSH_ABORT, mid2, max, false},
		{kvpb.PUSH_ABORT, max, min, true},
		{kvpb.PUSH_ABORT, max, mid1, true},
		{kvpb.PUSH_ABORT, max, mid2, true},
		{kvpb.PUSH_ABORT, max, max, false},
		{kvpb.PUSH_TIMESTAMP, min, min, false},
		{kvpb.PUSH_TIMESTAMP, min, mid1, false},
		{kvpb.PUSH_TIMESTAMP, min, mid2, false},
		{kvpb.PUSH_TIMESTAMP, min, max, false},
		{kvpb.PUSH_TIMESTAMP, mid1, min, true},
		{kvpb.PUSH_TIMESTAMP, mid1, mid1, false},
		{kvpb.PUSH_TIMESTAMP, mid1, mid2, false},
		{kvpb.PUSH_TIMESTAMP, mid1, max, false},
		{kvpb.PUSH_TIMESTAMP, mid2, min, true},
		{kvpb.PUSH_TIMESTAMP, mid2, mid1, false},
		{kvpb.PUSH_TIMESTAMP, mid2, mid2, false},
		{kvpb.PUSH_TIMESTAMP, mid2, max, false},
		{kvpb.PUSH_TIMESTAMP, max, min, true},
		{kvpb.PUSH_TIMESTAMP, max, mid1, true},
		{kvpb.PUSH_TIMESTAMP, max, mid2, true},
		{kvpb.PUSH_TIMESTAMP, max, max, false},
		{kvpb.PUSH_TOUCH, min, min, true},
		{kvpb.PUSH_TOUCH, min, mid1, true},
		{kvpb.PUSH_TOUCH, min, mid2, true},
		{kvpb.PUSH_TOUCH, min, max, true},
		{kvpb.PUSH_TOUCH, mid1, min, true},
		{kvpb.PUSH_TOUCH, mid1, mid1, true},
		{kvpb.PUSH_TOUCH, mid1, mid2, true},
		{kvpb.PUSH_TOUCH, mid1, max, true},
		{kvpb.PUSH_TOUCH, mid2, min, true},
		{kvpb.PUSH_TOUCH, mid2, mid1, true},
		{kvpb.PUSH_TOUCH, mid2, mid2, true},
		{kvpb.PUSH_TOUCH, mid2, max, true},
		{kvpb.PUSH_TOUCH, max, min, true},
		{kvpb.PUSH_TOUCH, max, mid1, true},
		{kvpb.PUSH_TOUCH, max, mid2, true},
		{kvpb.PUSH_TOUCH, max, max, true},
	}
	for _, test := range testCases {
		testutils.RunTrueAndFalse(t, "forcePush", func(t *testing.T, forcePush bool) {
			testutils.RunTrueAndFalse(t, "waitPolicyError", func(t *testing.T, waitPolicyError bool) {
				req := kvpb.PushTxnRequest{
					Force:    forcePush,
					PushType: test.typ,
					PusherTxn: roachpb.Transaction{
						TxnMeta: enginepb.TxnMeta{
							// NOTE: different pusher isolation levels tested below.
							IsoLevel: isolation.Serializable,
							Priority: test.pusherPri,
						},
					},
					PusheeTxn: enginepb.TxnMeta{
						// NOTE: different pushee isolation levels tested below.
						IsoLevel: isolation.Serializable,
						Priority: test.pusheePri,
					},
				}
				var wp lock.WaitPolicy
				if waitPolicyError {
					wp = lock.WaitPolicy_Error
				} else {
					wp = lock.WaitPolicy_Block
				}
				pusheeStatus := roachpb.PENDING
				shouldPush := ShouldPushImmediately(&req, pusheeStatus, wp)
				if waitPolicyError || forcePush {
					require.True(t, shouldPush)
				} else {
					require.Equal(t, test.shouldPush, shouldPush)
				}
			})
		})
	}
}

func TestCanPushWithPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run(kvpb.PUSH_ABORT.String(), testCanPushWithPriorityPushAbort)
	t.Run(kvpb.PUSH_TIMESTAMP.String(), testCanPushWithPriorityPushTimestamp)
	t.Run(kvpb.PUSH_TOUCH.String(), testCanPushWithPriorityPushTouch)
}

func testCanPushWithPriorityPushAbort(t *testing.T) {
	min := enginepb.MinTxnPriority
	max := enginepb.MaxTxnPriority
	mid1 := enginepb.TxnPriority(1)
	mid2 := enginepb.TxnPriority(2)
	statuses := []roachpb.TransactionStatus{roachpb.PENDING, roachpb.STAGING, roachpb.PREPARED}
	testCases := []struct {
		pusherPri enginepb.TxnPriority
		pusheePri enginepb.TxnPriority
		exp       bool
	}{
		{min, min, false},
		{min, mid1, false},
		{min, mid2, false},
		{min, max, false},
		{mid1, min, true},
		{mid1, mid1, false},
		{mid1, mid2, false},
		{mid1, max, false},
		{mid2, min, true},
		{mid2, mid1, false},
		{mid2, mid2, false},
		{mid2, max, false},
		{max, min, true},
		{max, mid1, true},
		{max, mid2, true},
		{max, max, false},
	}
	for _, pusheeStatus := range statuses {
		// NOTE: the behavior of PUSH_ABORT pushes is agnostic to isolation levels.
		for _, pusherIso := range isolation.Levels() {
			for _, pusheeIso := range isolation.Levels() {
				for _, test := range testCases {
					name := fmt.Sprintf("pusheeStatus=%s/pusherIso=%s/pusheeIso=%s/pusherPri=%d/pusheePri=%d",
						pusheeStatus, pusherIso, pusheeIso, test.pusherPri, test.pusheePri)
					t.Run(name, func(t *testing.T) {
						exp := test.exp
						if pusheeStatus == roachpb.PREPARED {
							// When the pushee is PREPARED, a PUSH_ABORT never succeeds.
							exp = false
						}
						canPush := CanPushWithPriority(
							kvpb.PUSH_ABORT, pusherIso, pusheeIso, test.pusherPri, test.pusheePri, pusheeStatus)
						require.Equal(t, exp, canPush)
					})
				}
			}
		}
	}
}

func testCanPushWithPriorityPushTimestamp(t *testing.T) {
	t.Run("pusheeStatus="+roachpb.PENDING.String(), testCanPushWithPriorityPushTimestampPusheePending)
	t.Run("pusheeStatus="+roachpb.PREPARED.String(), testCanPushWithPriorityPushTimestampPusheePrepared)
	t.Run("pusheeStatus="+roachpb.STAGING.String(), testCanPushWithPriorityPushTimestampPusheeStaging)
}

func testCanPushWithPriorityPushTimestampPusheePending(t *testing.T) {
	SSI := isolation.Serializable
	SI := isolation.Snapshot
	RC := isolation.ReadCommitted
	min := enginepb.MinTxnPriority
	max := enginepb.MaxTxnPriority
	mid1 := enginepb.TxnPriority(1)
	mid2 := enginepb.TxnPriority(2)
	testCases := []struct {
		pusherIso isolation.Level
		pusheeIso isolation.Level
		pusherPri enginepb.TxnPriority
		pusheePri enginepb.TxnPriority
		exp       bool
	}{
		// SSI pushing SSI
		{SSI, SSI, min, min, false},
		{SSI, SSI, min, mid1, false},
		{SSI, SSI, min, mid2, false},
		{SSI, SSI, min, max, false},
		{SSI, SSI, mid1, min, true},
		{SSI, SSI, mid1, mid1, false},
		{SSI, SSI, mid1, mid2, false},
		{SSI, SSI, mid1, max, false},
		{SSI, SSI, mid2, min, true},
		{SSI, SSI, mid2, mid1, false},
		{SSI, SSI, mid2, mid2, false},
		{SSI, SSI, mid2, max, false},
		{SSI, SSI, max, min, true},
		{SSI, SSI, max, mid1, true},
		{SSI, SSI, max, mid2, true},
		{SSI, SSI, max, max, false},
		// SSI pushing SI
		{SSI, SI, min, min, true},
		{SSI, SI, min, mid1, true},
		{SSI, SI, min, mid2, true},
		{SSI, SI, min, max, true},
		{SSI, SI, mid1, min, true},
		{SSI, SI, mid1, mid1, true},
		{SSI, SI, mid1, mid2, true},
		{SSI, SI, mid1, max, true},
		{SSI, SI, mid2, min, true},
		{SSI, SI, mid2, mid1, true},
		{SSI, SI, mid2, mid2, true},
		{SSI, SI, mid2, max, true},
		{SSI, SI, max, min, true},
		{SSI, SI, max, mid1, true},
		{SSI, SI, max, mid2, true},
		{SSI, SI, max, max, true},
		// SSI pushing RC
		{SSI, RC, min, min, true},
		{SSI, RC, min, mid1, true},
		{SSI, RC, min, mid2, true},
		{SSI, RC, min, max, true},
		{SSI, RC, mid1, min, true},
		{SSI, RC, mid1, mid1, true},
		{SSI, RC, mid1, mid2, true},
		{SSI, RC, mid1, max, true},
		{SSI, RC, mid2, min, true},
		{SSI, RC, mid2, mid1, true},
		{SSI, RC, mid2, mid2, true},
		{SSI, RC, mid2, max, true},
		{SSI, RC, max, min, true},
		{SSI, RC, max, mid1, true},
		{SSI, RC, max, mid2, true},
		{SSI, RC, max, max, true},
		// SI pushing SSI
		{SI, SSI, min, min, true},
		{SI, SSI, min, mid1, false},
		{SI, SSI, min, mid2, false},
		{SI, SSI, min, max, false},
		{SI, SSI, mid1, min, true},
		{SI, SSI, mid1, mid1, true},
		{SI, SSI, mid1, mid2, true},
		{SI, SSI, mid1, max, false},
		{SI, SSI, mid2, min, true},
		{SI, SSI, mid2, mid1, true},
		{SI, SSI, mid2, mid2, true},
		{SI, SSI, mid2, max, false},
		{SI, SSI, max, min, true},
		{SI, SSI, max, mid1, true},
		{SI, SSI, max, mid2, true},
		{SI, SSI, max, max, true},
		// SI pushing SI
		{SI, SI, min, min, true},
		{SI, SI, min, mid1, true},
		{SI, SI, min, mid2, true},
		{SI, SI, min, max, true},
		{SI, SI, mid1, min, true},
		{SI, SI, mid1, mid1, true},
		{SI, SI, mid1, mid2, true},
		{SI, SI, mid1, max, true},
		{SI, SI, mid2, min, true},
		{SI, SI, mid2, mid1, true},
		{SI, SI, mid2, mid2, true},
		{SI, SI, mid2, max, true},
		{SI, SI, max, min, true},
		{SI, SI, max, mid1, true},
		{SI, SI, max, mid2, true},
		{SI, SI, max, max, true},
		// SI pushing RC
		{SI, RC, min, min, true},
		{SI, RC, min, mid1, true},
		{SI, RC, min, mid2, true},
		{SI, RC, min, max, true},
		{SI, RC, mid1, min, true},
		{SI, RC, mid1, mid1, true},
		{SI, RC, mid1, mid2, true},
		{SI, RC, mid1, max, true},
		{SI, RC, mid2, min, true},
		{SI, RC, mid2, mid1, true},
		{SI, RC, mid2, mid2, true},
		{SI, RC, mid2, max, true},
		{SI, RC, max, min, true},
		{SI, RC, max, mid1, true},
		{SI, RC, max, mid2, true},
		{SI, RC, max, max, true},
		// RC pushing SSI
		{RC, SSI, min, min, true},
		{RC, SSI, min, mid1, false},
		{RC, SSI, min, mid2, false},
		{RC, SSI, min, max, false},
		{RC, SSI, mid1, min, true},
		{RC, SSI, mid1, mid1, true},
		{RC, SSI, mid1, mid2, true},
		{RC, SSI, mid1, max, false},
		{RC, SSI, mid2, min, true},
		{RC, SSI, mid2, mid1, true},
		{RC, SSI, mid2, mid2, true},
		{RC, SSI, mid2, max, false},
		{RC, SSI, max, min, true},
		{RC, SSI, max, mid1, true},
		{RC, SSI, max, mid2, true},
		{RC, SSI, max, max, true},
		// RC pushing SI
		{RC, SI, min, min, true},
		{RC, SI, min, mid1, true},
		{RC, SI, min, mid2, true},
		{RC, SI, min, max, true},
		{RC, SI, mid1, min, true},
		{RC, SI, mid1, mid1, true},
		{RC, SI, mid1, mid2, true},
		{RC, SI, mid1, max, true},
		{RC, SI, mid2, min, true},
		{RC, SI, mid2, mid1, true},
		{RC, SI, mid2, mid2, true},
		{RC, SI, mid2, max, true},
		{RC, SI, max, min, true},
		{RC, SI, max, mid1, true},
		{RC, SI, max, mid2, true},
		{RC, SI, max, max, true},
		// RC pushing RC
		{RC, RC, min, min, true},
		{RC, RC, min, mid1, true},
		{RC, RC, min, mid2, true},
		{RC, RC, min, max, true},
		{RC, RC, mid1, min, true},
		{RC, RC, mid1, mid1, true},
		{RC, RC, mid1, mid2, true},
		{RC, RC, mid1, max, true},
		{RC, RC, mid2, min, true},
		{RC, RC, mid2, mid1, true},
		{RC, RC, mid2, mid2, true},
		{RC, RC, mid2, max, true},
		{RC, RC, max, min, true},
		{RC, RC, max, mid1, true},
		{RC, RC, max, mid2, true},
		{RC, RC, max, max, true},
	}
	for _, test := range testCases {
		name := fmt.Sprintf("pusherIso=%s/pusheeIso=%s/pusherPri=%d/pusheePri=%d",
			test.pusherIso, test.pusheeIso, test.pusherPri, test.pusheePri)
		t.Run(name, func(t *testing.T) {
			canPush := CanPushWithPriority(
				kvpb.PUSH_TIMESTAMP, test.pusherIso, test.pusheeIso, test.pusherPri, test.pusheePri, roachpb.PENDING)
			require.Equal(t, test.exp, canPush)
		})
	}
}

func testCanPushWithPriorityPushTimestampPusheePrepared(t *testing.T) {
	min := enginepb.MinTxnPriority
	max := enginepb.MaxTxnPriority
	mid1 := enginepb.TxnPriority(1)
	mid2 := enginepb.TxnPriority(2)
	priorities := []enginepb.TxnPriority{min, mid1, mid2, max}
	// NOTE: the behavior of PUSH_TIMESTAMP pushes is agnostic to isolation levels
	// when the pushee transaction is PREPARED.
	for _, pusherIso := range isolation.Levels() {
		for _, pusheeIso := range isolation.Levels() {
			// NOTE: the behavior of PUSH_TIMESTAMP pushes is agnostic to priorities
			// when the pushee transaction is PREPARED.
			for _, pusherPri := range priorities {
				for _, pusheePri := range priorities {
					name := fmt.Sprintf("pusherIso=%s/pusheeIso=%s/pusherPri=%d/pusheePri=%d",
						pusherIso, pusheeIso, pusherPri, pusheePri)
					t.Run(name, func(t *testing.T) {
						canPush := CanPushWithPriority(
							kvpb.PUSH_TIMESTAMP, pusherIso, pusheeIso, pusherPri, pusheePri, roachpb.PREPARED)
						require.False(t, canPush)
					})
				}
			}
		}
	}
}

func testCanPushWithPriorityPushTimestampPusheeStaging(t *testing.T) {
	min := enginepb.MinTxnPriority
	max := enginepb.MaxTxnPriority
	mid1 := enginepb.TxnPriority(1)
	mid2 := enginepb.TxnPriority(2)
	testCases := []struct {
		pusherPri enginepb.TxnPriority
		pusheePri enginepb.TxnPriority
		exp       bool
	}{
		{min, min, false},
		{min, mid1, false},
		{min, mid2, false},
		{min, max, false},
		{mid1, min, true},
		{mid1, mid1, false},
		{mid1, mid2, false},
		{mid1, max, false},
		{mid2, min, true},
		{mid2, mid1, false},
		{mid2, mid2, false},
		{mid2, max, false},
		{max, min, true},
		{max, mid1, true},
		{max, mid2, true},
		{max, max, false},
	}
	// NOTE: the behavior of PUSH_TIMESTAMP pushes is agnostic to isolation levels
	// when the pushee transaction is STAGING.
	for _, pusherIso := range isolation.Levels() {
		for _, pusheeIso := range isolation.Levels() {
			for _, test := range testCases {
				name := fmt.Sprintf("pusherIso=%s/pusheeIso=%s/pusherPri=%d/pusheePri=%d",
					pusherIso, pusheeIso, test.pusherPri, test.pusheePri)
				t.Run(name, func(t *testing.T) {
					canPush := CanPushWithPriority(
						kvpb.PUSH_TIMESTAMP, pusherIso, pusheeIso, test.pusherPri, test.pusheePri, roachpb.STAGING)
					require.Equal(t, test.exp, canPush)
				})
			}
		}
	}
}

func testCanPushWithPriorityPushTouch(t *testing.T) {
	min := enginepb.MinTxnPriority
	max := enginepb.MaxTxnPriority
	mid1 := enginepb.TxnPriority(1)
	mid2 := enginepb.TxnPriority(2)
	priorities := []enginepb.TxnPriority{min, mid1, mid2, max}
	statuses := []roachpb.TransactionStatus{roachpb.PENDING, roachpb.STAGING, roachpb.PREPARED}
	// NOTE: the behavior of PUSH_TOUCH pushes is agnostic to pushee status.
	for _, pusheeStatus := range statuses {
		// NOTE: the behavior of PUSH_TOUCH pushes is agnostic to isolation levels.
		for _, pusherIso := range isolation.Levels() {
			for _, pusheeIso := range isolation.Levels() {
				// NOTE: the behavior of PUSH_TOUCH pushes is agnostic to txn priorities.
				for _, pusherPri := range priorities {
					for _, pusheePri := range priorities {
						name := fmt.Sprintf("pusheeStatus=%s/pusherIso=%s/pusheeIso=%s/pusherPri=%d/pusheePri=%d",
							pusheeStatus, pusherIso, pusheeIso, pusherPri, pusheePri)
						t.Run(name, func(t *testing.T) {
							canPush := CanPushWithPriority(
								kvpb.PUSH_TOUCH, pusherIso, pusheeIso, pusherPri, pusheePri, pusheeStatus)
							require.True(t, canPush)
						})
					}
				}
			}
		}
	}
}

func makeTS(w int64, l int32) hlc.Timestamp {
	return hlc.Timestamp{WallTime: w, Logical: l}
}

func TestIsPushed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		typ          kvpb.PushTxnType
		pushTo       hlc.Timestamp
		txnStatus    roachpb.TransactionStatus
		txnTimestamp hlc.Timestamp
		isPushed     bool
	}{
		{kvpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.PENDING, hlc.Timestamp{}, false},
		{kvpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.STAGING, hlc.Timestamp{}, false},
		{kvpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.ABORTED, hlc.Timestamp{}, true},
		{kvpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.COMMITTED, hlc.Timestamp{}, true},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, hlc.Timestamp{}, false},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.STAGING, hlc.Timestamp{}, false},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.ABORTED, hlc.Timestamp{}, true},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.COMMITTED, hlc.Timestamp{}, true},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 0), false},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 1), true},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 2), true},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.STAGING, makeTS(10, 0), false},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.STAGING, makeTS(10, 1), true},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.STAGING, makeTS(10, 2), true},
	}
	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			req := kvpb.PushTxnRequest{
				PushType: test.typ,
				PushTo:   test.pushTo,
			}
			txn := roachpb.Transaction{
				Status: test.txnStatus,
				TxnMeta: enginepb.TxnMeta{
					WriteTimestamp: test.txnTimestamp,
				},
			}
			isPushed := isPushed(&req, &txn)
			require.Equal(t, test.isPushed, isPushed)
		})
	}
}

func makeConfig(s kv.SenderFunc, stopper *stop.Stopper) Config {
	var cfg Config
	cfg.RangeDesc = &roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin, EndKey: roachpb.RKeyMax,
	}
	cfg.Clock = hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 123)))
	cfg.Stopper = stopper
	cfg.Metrics = NewMetrics(time.Minute)
	if s != nil {
		factory := kv.NonTransactionalFactoryFunc(s)
		cfg.DB = kv.NewDB(log.MakeTestingAmbientCtxWithNewTracer(), factory, cfg.Clock, stopper)
	}
	return cfg
}

// TestMaybeWaitForPushWithContextCancellation adds a new waiting push to the
// queue and cancels its context. It then verifies that the push was cleaned up
// and that this was properly reflected in the metrics.
func TestMaybeWaitForPushWithContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	var mockSender kv.SenderFunc
	cfg := makeConfig(func(
		ctx context.Context, ba *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, *kvpb.Error) {
		return mockSender(ctx, ba)
	}, stopper)
	q := NewQueue(cfg)
	q.Enable(1 /* leaseSeq */)

	// Enqueue pushee transaction in the queue.
	txn := roachpb.MakeTransaction("test", nil, 0, 0, cfg.Clock.Now(), 0, 0, 0, false /* omitInRangefeeds */)
	q.EnqueueTxn(&txn)

	// Mock out responses to any QueryTxn requests.
	mockSender = func(
		ctx context.Context, ba *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, *kvpb.Error) {
		br := ba.CreateReply()
		resp := br.Responses[0].GetInner().(*kvpb.QueryTxnResponse)
		resp.QueriedTxn = txn
		return br, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	waitingRes := make(chan *kvpb.Error)
	go func() {
		req := kvpb.PushTxnRequest{PusheeTxn: txn.TxnMeta, PushType: kvpb.PUSH_ABORT}
		_, err := q.MaybeWaitForPush(ctx, &req, lock.WaitPolicy_Block)
		waitingRes <- err
	}()

	cancel()
	pErr := <-waitingRes
	require.NotNil(t, pErr)
	s := pErr.String()
	_ = s
	require.Regexp(t, context.Canceled.Error(), pErr)
	require.Equal(t, 0, q.mu.txns[txn.ID].waitingPushes.Len())

	m := cfg.Metrics
	require.Equal(t, int64(1), m.PusheeWaiting.Value())
	require.Equal(t, int64(0), m.PusherWaiting.Value())
	require.Equal(t, int64(0), m.QueryWaiting.Value())
	require.Equal(t, int64(0), m.PusherSlow.Value())
}

// TestMaybeWaitForQueryWithContextCancellation adds a new waiting query to the
// queue and cancels its context. It then verifies that the query was cleaned up
// and that this was properly reflected in the metrics. Regression test against
// #28849, before which the waiting query would leak.
func TestMaybeWaitForQueryWithContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := makeConfig(nil, stopper)
	q := NewQueue(cfg)
	q.Enable(1 /* leaseSeq */)

	ctx, cancel := context.WithCancel(context.Background())
	waitingRes := make(chan *kvpb.Error)
	go func() {
		req := &kvpb.QueryTxnRequest{WaitForUpdate: true}
		waitingRes <- q.MaybeWaitForQuery(ctx, req)
	}()

	cancel()
	pErr := <-waitingRes
	require.NotNil(t, pErr)
	require.Regexp(t, context.Canceled.Error(), pErr)
	require.Equal(t, 0, len(q.mu.queries))

	m := cfg.Metrics
	require.Equal(t, int64(0), m.PusheeWaiting.Value())
	require.Equal(t, int64(0), m.PusherWaiting.Value())
	require.Equal(t, int64(0), m.QueryWaiting.Value())
	require.Equal(t, int64(0), m.PusherSlow.Value())
}

// TestPushersReleasedAfterAnyQueryTxnFindsAbortedTxn tests that if any
// QueryTxn on a pushee txn returns an aborted transaction status, all
// pushees of that transaction are informed of the aborted status and
// released.
func TestPushersReleasedAfterAnyQueryTxnFindsAbortedTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	var mockSender kv.SenderFunc
	cfg := makeConfig(func(
		ctx context.Context, ba *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, *kvpb.Error) {
		return mockSender(ctx, ba)
	}, stopper)
	q := NewQueue(cfg)
	q.Enable(1 /* leaseSeq */)

	// Set an extremely high transaction liveness threshold so that the pushee
	// is only queried once per pusher.
	defer TestingOverrideTxnLivenessThreshold(time.Hour)()

	// Enqueue pushee transaction in the queue.
	txn := roachpb.MakeTransaction("test", nil, 0, 0, cfg.Clock.Now(), 0, 0, 0, false /* omitInRangefeeds */)
	q.EnqueueTxn(&txn)

	const numPushees = 3
	var queryTxnCount int32
	mockSender = func(
		ctx context.Context, ba *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, *kvpb.Error) {
		br := ba.CreateReply()
		resp := br.Responses[0].GetInner().(*kvpb.QueryTxnResponse)
		resp.QueriedTxn = txn
		if atomic.AddInt32(&queryTxnCount, 1) == numPushees {
			// Only the last pusher's query observes an ABORTED transaction. As
			// mentioned in the corresponding comment in MaybeWaitForPush, this
			// isn't expected without an associated update to the pushee's
			// transaction record. However, it is possible if the pushee hasn't
			// written a transaction record yet and the timestamp cache loses
			// resolution due to memory pressure. While rare, we need to handle
			// this case correctly.
			resp.QueriedTxn.Status = roachpb.ABORTED
		}
		return br, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < numPushees; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			req := kvpb.PushTxnRequest{PusheeTxn: txn.TxnMeta, PushType: kvpb.PUSH_ABORT}
			res, err := q.MaybeWaitForPush(ctx, &req, lock.WaitPolicy_Block)
			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, roachpb.ABORTED, res.PusheeTxn.Status)
		}()
	}
	wg.Wait()
}
