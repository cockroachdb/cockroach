// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package txnrecovery

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/assert"
)

func makeManager(s *kv.Sender) (Manager, *hlc.Clock, *stop.Stopper) {
	ac := log.AmbientContext{Tracer: tracing.NewTracer()}
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	stopper := stop.NewStopper()
	db := kv.NewDB(ac, kv.NonTransactionalFactoryFunc(func(
		ctx context.Context, ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error) {
		return (*s).Send(ctx, ba)
	}), clock, stopper)
	return NewManager(ac, clock, db, stopper), clock, stopper
}

func makeStagingTransaction(clock *hlc.Clock) roachpb.Transaction {
	now := clock.Now()
	offset := clock.MaxOffset().Nanoseconds()
	txn := roachpb.MakeTransaction("test", roachpb.Key("a"), 0, now, offset)
	txn.Status = roachpb.STAGING
	return txn
}

type metricVals struct {
	attemptsPending      int64
	attempts             int64
	successesAsCommitted int64
	successesAsAborted   int64
	successesAsPending   int64
	failures             int64
}

func (v metricVals) merge(o metricVals) metricVals {
	v.attemptsPending += o.attemptsPending
	v.attempts += o.attempts
	v.successesAsCommitted += o.successesAsCommitted
	v.successesAsAborted += o.successesAsAborted
	v.successesAsPending += o.successesAsPending
	v.failures += o.failures
	return v
}

func assertMetrics(t *testing.T, m Manager, v metricVals) {
	assert.Equal(t, v.attemptsPending, m.Metrics().AttemptsPending.Value())
	assert.Equal(t, v.attempts, m.Metrics().Attempts.Count())
	assert.Equal(t, v.successesAsCommitted, m.Metrics().SuccessesAsCommitted.Count())
	assert.Equal(t, v.successesAsAborted, m.Metrics().SuccessesAsAborted.Count())
	assert.Equal(t, v.successesAsPending, m.Metrics().SuccessesAsPending.Count())
	assert.Equal(t, v.failures, m.Metrics().Failures.Count())
}

// TestResolveIndeterminateCommit tests successful indeterminate commit
// resolution attempts. It tests the case where an intent is prevented
// and the case where an intent is not prevented.
func TestResolveIndeterminateCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "prevent", func(t *testing.T, prevent bool) {
		var mockSender kv.Sender
		m, clock, stopper := makeManager(&mockSender)
		defer stopper.Stop(context.Background())

		txn := makeStagingTransaction(clock)
		txn.InFlightWrites = []roachpb.SequencedWrite{
			{Key: roachpb.Key("a"), Sequence: 1},
			{Key: roachpb.Key("b"), Sequence: 2},
		}

		mockSender = kv.SenderFunc(func(
			_ context.Context, ba roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			// Probing Phase.
			assertMetrics(t, m, metricVals{attemptsPending: 1, attempts: 1})

			assert.Equal(t, 3, len(ba.Requests))
			assert.IsType(t, &roachpb.QueryTxnRequest{}, ba.Requests[0].GetInner())
			assert.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[1].GetInner())
			assert.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[2].GetInner())

			assert.Equal(t, roachpb.Key(txn.Key), ba.Requests[0].GetInner().Header().Key)
			assert.Equal(t, roachpb.Key("a"), ba.Requests[1].GetInner().Header().Key)
			assert.Equal(t, roachpb.Key("b"), ba.Requests[2].GetInner().Header().Key)

			br := ba.CreateReply()
			br.Responses[0].GetInner().(*roachpb.QueryTxnResponse).QueriedTxn = txn
			br.Responses[1].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
			br.Responses[2].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = !prevent

			mockSender = kv.SenderFunc(func(
				_ context.Context, ba roachpb.BatchRequest,
			) (*roachpb.BatchResponse, *roachpb.Error) {
				// Recovery Phase.
				assertMetrics(t, m, metricVals{attemptsPending: 1, attempts: 1})

				assert.Equal(t, 1, len(ba.Requests))
				assert.IsType(t, &roachpb.RecoverTxnRequest{}, ba.Requests[0].GetInner())

				recTxnReq := ba.Requests[0].GetInner().(*roachpb.RecoverTxnRequest)
				assert.Equal(t, roachpb.Key(txn.Key), recTxnReq.Key)
				assert.Equal(t, txn.TxnMeta, recTxnReq.Txn)
				assert.Equal(t, !prevent, recTxnReq.ImplicitlyCommitted)

				br2 := ba.CreateReply()
				recTxnResp := br2.Responses[0].GetInner().(*roachpb.RecoverTxnResponse)
				recTxnResp.RecoveredTxn = txn
				if !prevent {
					recTxnResp.RecoveredTxn.Status = roachpb.COMMITTED
				} else {
					recTxnResp.RecoveredTxn.Status = roachpb.ABORTED
				}
				return br2, nil
			})
			return br, nil
		})

		assertMetrics(t, m, metricVals{})
		iceErr := roachpb.NewIndeterminateCommitError(txn)
		resTxn, err := m.ResolveIndeterminateCommit(context.Background(), iceErr)
		assert.NotNil(t, resTxn)
		assert.Nil(t, err)

		if !prevent {
			assert.Equal(t, roachpb.COMMITTED, resTxn.Status)
			assertMetrics(t, m, metricVals{attempts: 1, successesAsCommitted: 1})
		} else {
			assert.Equal(t, roachpb.ABORTED, resTxn.Status)
			assertMetrics(t, m, metricVals{attempts: 1, successesAsAborted: 1})
		}
	})
}

// TestResolveIndeterminateCommitTxnChanges tests indeterminate commit
// resolution attempts where the transaction record being recovered changes in
// the middle of the process, either due to an active transaction coordinator or
// due to a concurrent recovery.
func TestResolveIndeterminateCommitTxnChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var mockSender kv.Sender
	m, clock, stopper := makeManager(&mockSender)
	defer stopper.Stop(context.Background())

	txn := makeStagingTransaction(clock)
	txn.InFlightWrites = []roachpb.SequencedWrite{
		{Key: roachpb.Key("a"), Sequence: 1},
		{Key: roachpb.Key("b"), Sequence: 2},
	}

	// Maintain an expected aggregation of metric updates.
	var expMetrics metricVals
	assertMetrics(t, m, expMetrics)

	testCases := []struct {
		name          string
		duringProbing bool
		changedTxn    roachpb.Transaction
		metricImpact  metricVals
	}{
		{
			name:          "transaction commit during probe",
			duringProbing: true,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.Status = roachpb.COMMITTED
				txnCopy.InFlightWrites = nil
				return txnCopy
			}(),
			metricImpact: metricVals{attempts: 1, successesAsCommitted: 1},
		},
		{
			name:          "transaction abort during probe",
			duringProbing: true,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.Status = roachpb.ABORTED
				txnCopy.InFlightWrites = nil
				return txnCopy
			}(),
			metricImpact: metricVals{attempts: 1, successesAsAborted: 1},
		},
		{
			name:          "transaction restart during probe",
			duringProbing: true,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.BumpEpoch()
				return txnCopy
			}(),
			metricImpact: metricVals{attempts: 1, successesAsPending: 1},
		},
		{
			name:          "transaction timestamp increase during probe",
			duringProbing: true,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.WriteTimestamp = txnCopy.WriteTimestamp.Add(1, 0)
				return txnCopy
			}(),
			metricImpact: metricVals{attempts: 1, successesAsPending: 1},
		},
		{
			name:          "transaction commit during recovery",
			duringProbing: false,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.Status = roachpb.COMMITTED
				txnCopy.InFlightWrites = nil
				return txnCopy
			}(),
			metricImpact: metricVals{attempts: 1, successesAsCommitted: 1},
		},
		{
			name:          "transaction abort during recovery",
			duringProbing: false,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.Status = roachpb.ABORTED
				txnCopy.InFlightWrites = nil
				return txnCopy
			}(),
			metricImpact: metricVals{attempts: 1, successesAsAborted: 1},
		},
		{
			name:          "transaction restart during recovery",
			duringProbing: false,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.BumpEpoch()
				return txnCopy
			}(),
			metricImpact: metricVals{attempts: 1, successesAsPending: 1},
		},
		{
			name:          "transaction timestamp increase during recovery",
			duringProbing: false,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.WriteTimestamp = txnCopy.WriteTimestamp.Add(1, 0)
				return txnCopy
			}(),
			metricImpact: metricVals{attempts: 1, successesAsPending: 1},
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			mockSender = kv.SenderFunc(func(
				_ context.Context, ba roachpb.BatchRequest,
			) (*roachpb.BatchResponse, *roachpb.Error) {
				// Probing Phase.
				assertMetrics(t, m, expMetrics.merge(metricVals{attemptsPending: 1, attempts: 1}))

				assert.Equal(t, 3, len(ba.Requests))
				assert.IsType(t, &roachpb.QueryTxnRequest{}, ba.Requests[0].GetInner())
				assert.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[1].GetInner())
				assert.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[2].GetInner())

				assert.Equal(t, roachpb.Key(txn.Key), ba.Requests[0].GetInner().Header().Key)
				assert.Equal(t, roachpb.Key("a"), ba.Requests[1].GetInner().Header().Key)
				assert.Equal(t, roachpb.Key("b"), ba.Requests[2].GetInner().Header().Key)

				br := ba.CreateReply()
				if c.duringProbing {
					br.Responses[0].GetInner().(*roachpb.QueryTxnResponse).QueriedTxn = c.changedTxn
				} else {
					br.Responses[0].GetInner().(*roachpb.QueryTxnResponse).QueriedTxn = txn
				}
				br.Responses[1].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
				br.Responses[2].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = false

				mockSender = kv.SenderFunc(func(
					_ context.Context, ba roachpb.BatchRequest,
				) (*roachpb.BatchResponse, *roachpb.Error) {
					// Recovery Phase.
					assert.False(t, c.duringProbing, "the recovery phase should not be run")
					assertMetrics(t, m, expMetrics.merge(metricVals{attemptsPending: 1, attempts: 1}))

					assert.Equal(t, 1, len(ba.Requests))
					assert.IsType(t, &roachpb.RecoverTxnRequest{}, ba.Requests[0].GetInner())

					recTxnReq := ba.Requests[0].GetInner().(*roachpb.RecoverTxnRequest)
					assert.Equal(t, roachpb.Key(txn.Key), recTxnReq.Key)
					assert.Equal(t, txn.TxnMeta, recTxnReq.Txn)
					assert.Equal(t, false, recTxnReq.ImplicitlyCommitted)

					br2 := ba.CreateReply()
					br2.Responses[0].GetInner().(*roachpb.RecoverTxnResponse).RecoveredTxn = c.changedTxn
					return br2, nil
				})
				return br, nil
			})

			iceErr := roachpb.NewIndeterminateCommitError(txn)
			resTxn, err := m.ResolveIndeterminateCommit(context.Background(), iceErr)
			assert.NotNil(t, resTxn)
			assert.Equal(t, c.changedTxn, *resTxn)
			assert.Nil(t, err)

			expMetrics = expMetrics.merge(c.metricImpact)
			assertMetrics(t, m, expMetrics)
		})
	}
}

// TestResolveIndeterminateCommitTxnWithoutInFlightWrites tests that an
// indeterminate commit resolution attempt skips the probing phase entirely
// when a STAGING transaction has no in-flight writes. This shouldn't happen
// in practice because a transaction will move straight to being explicitly
// committed if it doesn't have any concurrent writes at the time that it
// is committing, but it is handled correctly nonetheless.
func TestResolveIndeterminateCommitTxnWithoutInFlightWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var mockSender kv.Sender
	m, clock, stopper := makeManager(&mockSender)
	defer stopper.Stop(context.Background())

	// Create STAGING txn without any in-flight writes.
	txn := makeStagingTransaction(clock)

	mockSender = kv.SenderFunc(func(
		_ context.Context, ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error) {
		// Recovery Phase. Probing phase skipped.
		assert.Equal(t, 1, len(ba.Requests))
		assert.IsType(t, &roachpb.RecoverTxnRequest{}, ba.Requests[0].GetInner())

		recTxnReq := ba.Requests[0].GetInner().(*roachpb.RecoverTxnRequest)
		assert.Equal(t, roachpb.Key(txn.Key), recTxnReq.Key)
		assert.Equal(t, txn.TxnMeta, recTxnReq.Txn)
		assert.Equal(t, true, recTxnReq.ImplicitlyCommitted)

		br := ba.CreateReply()
		recTxnResp := br.Responses[0].GetInner().(*roachpb.RecoverTxnResponse)
		recTxnResp.RecoveredTxn = txn
		recTxnResp.RecoveredTxn.Status = roachpb.COMMITTED
		return br, nil
	})

	iceErr := roachpb.NewIndeterminateCommitError(txn)
	resTxn, err := m.ResolveIndeterminateCommit(context.Background(), iceErr)
	assert.NotNil(t, resTxn)
	assert.Equal(t, roachpb.COMMITTED, resTxn.Status)
	assert.Nil(t, err)
}
