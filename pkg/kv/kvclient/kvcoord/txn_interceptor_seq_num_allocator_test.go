// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	defer log.Scope(t).Close(t)
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

	// EndTxn requests also get a unique sequence number.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}})
	ba.Add(&roachpb.EndTxnRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

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

// TestSequenceNumberAllocationWithStep tests the basics of sequence number allocation.
// It verifies that read-only requests are assigned the last step sequence number
// and that write requests are assigned a sequence number larger than any
// previously allocated.
func TestSequenceNumberAllocationWithStep(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, mockSender := makeMockTxnSeqNumAllocator()

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")

	s.configureSteppingLocked(true /* enabled */)

	for i := 1; i <= 3; i++ {
		if err := s.stepLocked(ctx); err != nil {
			t.Fatal(err)
		}
		if s.writeSeq != s.readSeq {
			t.Fatalf("mismatched read seqnum: got %d, expected %d", s.readSeq, s.writeSeq)
		}

		t.Run(fmt.Sprintf("step %d", i), func(t *testing.T) {
			currentStepSeqNum := s.writeSeq

			var ba roachpb.BatchRequest
			ba.Header = roachpb.Header{Txn: &txn}
			ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
			ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}})

			mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Len(t, ba.Requests, 2)
				require.Equal(t, currentStepSeqNum, ba.Requests[0].GetInner().Header().Sequence)
				require.Equal(t, currentStepSeqNum, ba.Requests[1].GetInner().Header().Sequence)

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			})

			br, pErr := s.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)

			// Write requests each get a unique sequence number. The read-only requests
			// remain at the last step seqnum.
			ba.Requests = nil
			ba.Add(&roachpb.ConditionalPutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
			ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
			ba.Add(&roachpb.InitPutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
			ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}})

			mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Len(t, ba.Requests, 4)
				require.Equal(t, currentStepSeqNum+1, ba.Requests[0].GetInner().Header().Sequence)
				require.Equal(t, currentStepSeqNum, ba.Requests[1].GetInner().Header().Sequence)
				require.Equal(t, currentStepSeqNum+2, ba.Requests[2].GetInner().Header().Sequence)
				require.Equal(t, currentStepSeqNum, ba.Requests[3].GetInner().Header().Sequence)

				br = ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			})

			br, pErr = s.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)

			// EndTxn requests also get a unique sequence number. Meanwhile read-only
			// requests remain at the last step.
			ba.Requests = nil
			ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
			ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}})
			ba.Add(&roachpb.EndTxnRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

			mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Len(t, ba.Requests, 3)
				require.Equal(t, currentStepSeqNum+3, ba.Requests[0].GetInner().Header().Sequence)
				require.Equal(t, currentStepSeqNum, ba.Requests[1].GetInner().Header().Sequence)
				require.Equal(t, currentStepSeqNum+4, ba.Requests[2].GetInner().Header().Sequence)

				br = ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			})

			br, pErr = s.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)
		})
	}

	// Check that step-wise execution is disabled by ConfigureStepping(SteppingDisabled).
	s.configureSteppingLocked(false /* enabled */)
	currentStepSeqNum := s.writeSeq

	var ba roachpb.BatchRequest
	ba.Requests = nil
	ba.Add(&roachpb.ConditionalPutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.InitPutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 4)
		require.Equal(t, currentStepSeqNum+1, ba.Requests[0].GetInner().Header().Sequence)
		require.Equal(t, currentStepSeqNum+1, ba.Requests[1].GetInner().Header().Sequence)
		require.Equal(t, currentStepSeqNum+2, ba.Requests[2].GetInner().Header().Sequence)
		require.Equal(t, currentStepSeqNum+2, ba.Requests[3].GetInner().Header().Sequence)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := s.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
}

// TestModifyReadSeqNum tests that it's possible to switch the read seq num
// inside of a transaction to perform reads that do not include writes that
// occurred at higher seq nums in the transaction. This is a unit test of the
// functionality that SQL uses to implement CURSOR.
func TestModifyReadSeqNum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, mockSender := makeMockTxnSeqNumAllocator()

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	s.configureSteppingLocked(true /* enabled */)
	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	// The test plan is as follows:
	// 1. do a mutation.
	// 2. pretend to create a cursor by grabbing the current read seq num.
	// 3. do another mutation.
	// 4. ensure that we can perform a read at the old seq num and that nothing
	//    blows up, and that we can perform a read at the current seq num as well
	//    afterwards.
	// 5. do another mutation.
	// 6. repeat step 4.

	// First, do a mutation.
	if err := s.stepLocked(ctx); err != nil {
		t.Fatal(err)
	}
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	br, pErr := s.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Grab the current seq num.
	cursorSeqNum := s.readSeq

	// Perform another mutation.
	if err := s.stepLocked(ctx); err != nil {
		t.Fatal(err)
	}
	ba = roachpb.BatchRequest{}
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	br, pErr = s.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Ensure that sending a read at the old seq num doesn't do anything bad and
	// also passes through the correct seq num.
	curReadSeq := s.readSeq
	if err := s.stepLocked(ctx); err != nil {
		t.Fatal(err)
	}
	s.readSeq = cursorSeqNum
	ba = roachpb.BatchRequest{}
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, cursorSeqNum, ba.Requests[0].GetGet().RequestHeader.Sequence)
		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})
	s.readSeq = curReadSeq

	// Ensure that doing another read after resetting the seq num back to the
	// newer seq num also doesn't do anything bad.
	if err := s.stepLocked(ctx); err != nil {
		t.Fatal(err)
	}
	ba = roachpb.BatchRequest{}
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, curReadSeq, ba.Requests[0].GetGet().RequestHeader.Sequence)
		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	// Do another mutation after messing with the read seq nums so that we can
	// be sure things are still groovy.
	if err := s.stepLocked(ctx); err != nil {
		t.Fatal(err)
	}
	ba = roachpb.BatchRequest{}
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, s.writeSeq, ba.Requests[0].GetGet().RequestHeader.Sequence)
		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	// Ensure that sending another read at the old seq num doesn't do anything bad
	// and also passes through the correct seq num.
	curReadSeq = s.readSeq
	if err := s.stepLocked(ctx); err != nil {
		t.Fatal(err)
	}
	s.readSeq = cursorSeqNum
	ba = roachpb.BatchRequest{}
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, cursorSeqNum, ba.Requests[0].GetGet().RequestHeader.Sequence)
		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})
	s.readSeq = curReadSeq

	// Ensure that doing another read after resetting the seq num back to the
	// newer seq num also doesn't do anything bad.
	if err := s.stepLocked(ctx); err != nil {
		t.Fatal(err)
	}
	ba = roachpb.BatchRequest{}
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, curReadSeq, ba.Requests[0].GetGet().RequestHeader.Sequence)
		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})
}

// TestSequenceNumberAllocationTxnRequests tests sequence number allocation's
// interaction with transaction state requests (HeartbeatTxn and EndTxn). Only
// EndTxn requests should be assigned unique sequence numbers.
func TestSequenceNumberAllocationTxnRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, mockSender := makeMockTxnSeqNumAllocator()

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.HeartbeatTxnRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.EndTxnRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

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
	defer log.Scope(t).Close(t)
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

// TestSequenceNumberAllocationAfterLeafInitialization tests that the sequence number
// allocator updates its sequence counter based on the provided LeafTxnInitialState
func TestSequenceNumberAllocationAfterLeafInitialization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, mockSender := makeMockTxnSeqNumAllocator()

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	tis := roachpb.LeafTxnInputState{
		SteppingModeEnabled: true,
		ReadSeqNum:          4,
	}
	s.initializeLeaf(&tis)

	// Perform a few reads and writes. The sequence numbers assigned should
	// start at the sequence number provided in the LeafTxnInputState.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.Equal(t, enginepb.TxnSeq(4), ba.Requests[0].GetInner().Header().Sequence)
		require.Equal(t, enginepb.TxnSeq(4), ba.Requests[1].GetInner().Header().Sequence)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := s.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
}

// TestSequenceNumberAllocationSavepoint tests that the allocator populates a
// savepoint with the cur seq num.
func TestSequenceNumberAllocationSavepoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, mockSender := makeMockTxnSeqNumAllocator()
	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")

	// Perform a few writes to increase the sequence number counter.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyB}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})
	br, pErr := s.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, enginepb.TxnSeq(2), s.writeSeq)

	sp := &savepoint{}
	s.createSavepointLocked(ctx, sp)
	require.Equal(t, enginepb.TxnSeq(2), sp.seqNum)
}
