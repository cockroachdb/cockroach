// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package kv

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func makeMockTxnCommitter() (txnCommitter, *mockLockedSender) {
	mockSender := &mockLockedSender{}
	return txnCommitter{
		wrapped: mockSender,
	}, mockSender
}

// TestTxnCommitterAttachesTxnKey tests that the txnCommitter attaches the
// transaction key to committing and aborting EndTransaction requests.
func TestTxnCommitterAttachesTxnKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc, mockSender := makeMockTxnCommitter()

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	// Attach IntentSpans to each EndTransaction request to avoid the elided
	// EndTransaction optimization.
	intents := []roachpb.Span{{Key: keyA}}

	// Verify that the txn key is attached to committing EndTransaction requests.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.EndTransactionRequest{Commit: true, IntentSpans: intents})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.Equal(t, keyA, ba.Requests[0].GetInner().Header().Key)
		require.Equal(t, roachpb.Key(txn.Key), ba.Requests[1].GetInner().Header().Key)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.COMMITTED
		return br, nil
	})

	br, pErr := tc.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)

	// Verify that the txn key is attached to aborting EndTransaction requests.
	ba.Requests = nil
	ba.Add(&roachpb.EndTransactionRequest{Commit: false, IntentSpans: intents})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.Equal(t, roachpb.Key(txn.Key), ba.Requests[0].GetInner().Header().Key)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.ABORTED
		return br, nil
	})

	br, pErr = tc.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
}

// TestTxnCommitterElideEndTransaction tests that EndTransaction requests for
// read-only transactions are removed from their batches because they are not
// necessary. The test verifies the case where the EndTransaction request is
// part of a batch with other requests and the case where it is alone in its
// batch.
func TestTxnCommitterElideEndTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc, mockSender := makeMockTxnCommitter()

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	// Test with both commits and rollbacks.
	testutils.RunTrueAndFalse(t, "commit", func(t *testing.T, commit bool) {
		expStatus := roachpb.COMMITTED
		if !commit {
			expStatus = roachpb.ABORTED
		}

		// Test the case where the EndTransaction request is part of a larger
		// batch of requests.
		var ba roachpb.BatchRequest
		ba.Header = roachpb.Header{Txn: &txn}
		ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
		ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
		ba.Add(&roachpb.EndTransactionRequest{Commit: commit, IntentSpans: nil})

		mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			require.Len(t, ba.Requests, 2)
			require.IsType(t, &roachpb.GetRequest{}, ba.Requests[0].GetInner())
			require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

			br := ba.CreateReply()
			br.Txn = ba.Txn
			// The Sender did not receive an EndTransaction request, so it keeps
			// the Txn status as PENDING.
			br.Txn.Status = roachpb.PENDING
			return br, nil
		})

		br, pErr := tc.SendLocked(ctx, ba)
		require.NotNil(t, br)
		require.Nil(t, pErr)
		require.NotNil(t, br.Txn)
		require.Equal(t, expStatus, br.Txn.Status)

		// Test the case where the EndTransaction request is alone.
		ba.Requests = nil
		ba.Add(&roachpb.EndTransactionRequest{Commit: commit, IntentSpans: nil})

		mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			require.Fail(t, "should not have issued batch request", ba)
			return nil, nil
		})

		br, pErr = tc.SendLocked(ctx, ba)
		require.NotNil(t, br)
		require.Nil(t, pErr)
		require.NotNil(t, br.Txn)
		require.Equal(t, expStatus, br.Txn.Status)
	})
}
