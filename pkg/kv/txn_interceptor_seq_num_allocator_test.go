// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func makeMockTxnSeqNumAllocator() (txnSeqNumAllocator, *mockLockedSender) {
	mockSender := &mockLockedSender{}
	return txnSeqNumAllocator{
		wrapped: mockSender,
	}, mockSender
}

// TestSequenceNumberAllocation tests the basics of sequence number allocation.
// It verifies that read-only requests are assigned the current largest sequence
// number and that write requests are assigned a sequence number larger than any
// previously allocated.
func TestSequenceNumberAllocation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, mockSender := makeMockTxnSeqNumAllocator()

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")

	// Read-only requests are not given unique sequence numbers.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.Equal(t, enginepb.TxnSeq(0), ba.Requests[0].GetInner().Header().Sequence)
		require.Equal(t, enginepb.TxnSeq(0), ba.Requests[1].GetInner().Header().Sequence)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := s.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Write requests each get a unique sequence number.
	ba.Requests = nil
	ba.Add(&roachpb.ConditionalPutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.InitPutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 4)
		require.Equal(t, enginepb.TxnSeq(1), ba.Requests[0].GetInner().Header().Sequence)
		require.Equal(t, enginepb.TxnSeq(1), ba.Requests[1].GetInner().Header().Sequence)
		require.Equal(t, enginepb.TxnSeq(2), ba.Requests[2].GetInner().Header().Sequence)
		require.Equal(t, enginepb.TxnSeq(2), ba.Requests[3].GetInner().Header().Sequence)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = s.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// EndTransaction requests also get a unique sequence number.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}})
	ba.Add(&roachpb.EndTransactionRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.Equal(t, enginepb.TxnSeq(3), ba.Requests[0].GetInner().Header().Sequence)
		require.Equal(t, enginepb.TxnSeq(3), ba.Requests[1].GetInner().Header().Sequence)
		require.Equal(t, enginepb.TxnSeq(4), ba.Requests[2].GetInner().Header().Sequence)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = s.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
}

// TestSequenceNumberAllocationTxnRequests tests sequence number allocation's
// interaction with transaction state requests (HeartbeatTxn and EndTxn). Only
// EndTxn requests should be assigned unique sequence numbers.
func TestSequenceNumberAllocationTxnRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, mockSender := makeMockTxnSeqNumAllocator()

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.HeartbeatTxnRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.EndTransactionRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.Equal(t, enginepb.TxnSeq(0), ba.Requests[0].GetInner().Header().Sequence)
		require.Equal(t, enginepb.TxnSeq(1), ba.Requests[1].GetInner().Header().Sequence)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := s.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
}

// TestSequenceNumberAllocationAfterEpochBump tests that sequence number
// allocation resets to zero after an transaction epoch bump.
func TestSequenceNumberAllocationAfterEpochBump(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, mockSender := makeMockTxnSeqNumAllocator()

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")

	// Perform a few writes to increase the sequence number counter.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.ConditionalPutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.InitPutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.Equal(t, enginepb.TxnSeq(1), ba.Requests[0].GetInner().Header().Sequence)
		require.Equal(t, enginepb.TxnSeq(1), ba.Requests[1].GetInner().Header().Sequence)
		require.Equal(t, enginepb.TxnSeq(2), ba.Requests[2].GetInner().Header().Sequence)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := s.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Bump the transaction's epoch.
	s.epochBumpedLocked()

	// Perform a few more writes. The sequence numbers assigned to requests
	// should have started back at zero again.
	ba.Requests = nil
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.ConditionalPutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}})
	ba.Add(&roachpb.InitPutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 4)
		require.Equal(t, enginepb.TxnSeq(0), ba.Requests[0].GetInner().Header().Sequence)
		require.Equal(t, enginepb.TxnSeq(1), ba.Requests[1].GetInner().Header().Sequence)
		require.Equal(t, enginepb.TxnSeq(1), ba.Requests[2].GetInner().Header().Sequence)
		require.Equal(t, enginepb.TxnSeq(2), ba.Requests[3].GetInner().Header().Sequence)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = s.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
}

// TestSequenceNumberAllocationAfterAugmentation tests that the sequence number
// allocator updates its sequence counter based on the provided TxnCoordMeta.
func TestSequenceNumberAllocationAfterAugmentation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, mockSender := makeMockTxnSeqNumAllocator()

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	// Create a TxnCoordMeta object. This simulates the interceptor living
	// on a Leaf transaction coordinator and being initialized by the Root
	// coordinator.
	var inMeta roachpb.TxnCoordMeta
	inMeta.Txn.Sequence = 4
	s.augmentMetaLocked(inMeta)

	// Ensure that the update round-trips.
	var outMeta roachpb.TxnCoordMeta
	s.populateMetaLocked(&outMeta)
	require.Equal(t, enginepb.TxnSeq(4), outMeta.Txn.Sequence)

	// Perform a few reads and writes. The sequence numbers assigned should
	// start at the sequence number provided in the TxnCoordMeta.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.ConditionalPutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.InitPutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 4)
		require.Equal(t, enginepb.TxnSeq(4), ba.Requests[0].GetInner().Header().Sequence)
		require.Equal(t, enginepb.TxnSeq(5), ba.Requests[1].GetInner().Header().Sequence)
		require.Equal(t, enginepb.TxnSeq(5), ba.Requests[2].GetInner().Header().Sequence)
		require.Equal(t, enginepb.TxnSeq(6), ba.Requests[3].GetInner().Header().Sequence)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := s.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Ensure that the updated sequence counter is reflected in a TxnCoordMeta.
	outMeta = roachpb.TxnCoordMeta{}
	s.populateMetaLocked(&outMeta)
	require.Equal(t, enginepb.TxnSeq(6), outMeta.Txn.Sequence)
}
