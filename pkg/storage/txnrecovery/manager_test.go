// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless assertd by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package txnrecovery

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/assert"
)

func makeManager(s *client.Sender) (Manager, *hlc.Clock, *stop.Stopper) {
	ac := log.AmbientContext{Tracer: tracing.NewTracer()}
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	stopper := stop.NewStopper()
	db := client.NewDB(ac, client.NonTransactionalFactoryFunc(func(
		ctx context.Context, ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error) {
		return (*s).Send(ctx, ba)
	}), clock)
	return NewManager(ac, clock, db, stopper), clock, stopper
}

func makeStagingTransaction(clock *hlc.Clock) roachpb.Transaction {
	now := clock.Now()
	offset := clock.MaxOffset().Nanoseconds()
	txn := roachpb.MakeTransaction("test", roachpb.Key("a"), 0, now, offset)
	txn.Status = roachpb.STAGING
	return txn
}

// TestResolveIndeterminateCommit tests successful indeterminate commit
// resolution attempts. It tests the case where an intent is prevented
// and the case where an intent is not prevented.
func TestResolveIndeterminateCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var mockSender client.Sender
	m, clock, stopper := makeManager(&mockSender)
	defer stopper.Stop(context.Background())

	txn := makeStagingTransaction(clock)
	txn.InFlightWrites = []roachpb.SequencedWrite{
		{Key: roachpb.Key("a"), Sequence: 1},
		{Key: roachpb.Key("b"), Sequence: 2},
	}

	testutils.RunTrueAndFalse(t, "prevent", func(t *testing.T, prevent bool) {
		mockSender = client.SenderFunc(func(
			_ context.Context, ba roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			// Probing Phase.
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

			mockSender = client.SenderFunc(func(
				_ context.Context, ba roachpb.BatchRequest,
			) (*roachpb.BatchResponse, *roachpb.Error) {
				// Recovery Phase.
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

		iceErr := roachpb.NewIndeterminateCommitError(txn)
		resTxn, err := m.ResolveIndeterminateCommit(context.Background(), iceErr)
		assert.NotNil(t, resTxn)
		assert.Nil(t, err)

		if !prevent {
			assert.Equal(t, roachpb.COMMITTED, resTxn.Status)
		} else {
			assert.Equal(t, roachpb.ABORTED, resTxn.Status)
		}
	})
}

// TestResolveIndeterminateCommitTxnChanges tests indeterminate commit
// resolution attempts where the transaction record being recovered changes in
// the middle of the process, either due to an active transaction coordinator or
// due to a concurrent recovery.
func TestResolveIndeterminateCommitTxnChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var mockSender client.Sender
	m, clock, stopper := makeManager(&mockSender)
	defer stopper.Stop(context.Background())

	txn := makeStagingTransaction(clock)
	txn.InFlightWrites = []roachpb.SequencedWrite{
		{Key: roachpb.Key("a"), Sequence: 1},
		{Key: roachpb.Key("b"), Sequence: 2},
	}

	testCases := []struct {
		name          string
		duringProbing bool
		changedTxn    roachpb.Transaction
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
		},
		{
			name:          "transaction restart during probe",
			duringProbing: true,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.BumpEpoch()
				return txnCopy
			}(),
		},
		{
			name:          "transaction timestamp increase during probe",
			duringProbing: true,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.Timestamp = txnCopy.Timestamp.Add(1, 0)
				return txnCopy
			}(),
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
		},
		{
			name:          "transaction restart during recovery",
			duringProbing: false,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.BumpEpoch()
				return txnCopy
			}(),
		},
		{
			name:          "transaction timestamp increase during recovery",
			duringProbing: false,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.Timestamp = txnCopy.Timestamp.Add(1, 0)
				return txnCopy
			}(),
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			mockSender = client.SenderFunc(func(
				_ context.Context, ba roachpb.BatchRequest,
			) (*roachpb.BatchResponse, *roachpb.Error) {
				// Probing Phase.
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

				mockSender = client.SenderFunc(func(
					_ context.Context, ba roachpb.BatchRequest,
				) (*roachpb.BatchResponse, *roachpb.Error) {
					// Recovery Phase.
					assert.False(t, c.duringProbing, "the recovery phase should not be run")

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

	var mockSender client.Sender
	m, clock, stopper := makeManager(&mockSender)
	defer stopper.Stop(context.Background())

	// Create STAGING txn without any in-flight writes.
	txn := makeStagingTransaction(clock)

	mockSender = client.SenderFunc(func(
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
