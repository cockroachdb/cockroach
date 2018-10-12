// Copyright 2018 The Cockroach Authors.
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
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/google/btree"
	"github.com/stretchr/testify/require"
)

// mockLockedSender implements the lockedSender interface and provides a way to
// mock out and adjust the SendLocked method.
type mockLockedSender struct {
	mockFn func(roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)
}

func (m *mockLockedSender) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	return m.mockFn(ba)
}

func (m *mockLockedSender) MockSend(
	fn func(roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error),
) {
	m.mockFn = fn
}

func makeMockTxnPipeliner() (txnPipeliner, *mockLockedSender) {
	mockSender := &mockLockedSender{}
	return txnPipeliner{
		st:      cluster.MakeTestingClusterSettings(),
		wrapped: mockSender,
	}, mockSender
}

func makeTxnProto() roachpb.Transaction {
	return roachpb.MakeTransaction("test", []byte("key"), 0, 0, hlc.Timestamp{}, 0)
}

// TestTxnPipeliner1PCTransaction tests that 1PC transactions pass through the
// txnPipeliner untouched.
func TestTxnPipeliner1PCTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.BeginTransactionRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.EndTransactionRequest{Commit: true})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 3, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.BeginTransactionRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.EndTransactionRequest{}, ba.Requests[2].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.COMMITTED
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 0, tp.outstandingWritesLen())
}

// TestTxnPipelinerTrackOutstandingWrites tests that txnPipeliner tracks writes
// that were performed with async consensus. It also tests that these writes are
// proved as requests are chained onto them. Finally, it tests that EndTxn
// requests chain on to all existing requests.
func TestTxnPipelinerTrackOutstandingWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.BeginTransactionRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	putArgs.Sequence = 2
	ba.Add(&putArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.BeginTransactionRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 1, tp.outstandingWritesLen())

	w := tp.outstandingWrites.Min().(*outstandingWrite)
	require.Equal(t, putArgs.Key, w.Key)
	require.Equal(t, putArgs.Sequence, w.Sequence)

	// More writes, one that replaces the other's sequence number.
	keyB, keyC := roachpb.Key("b"), roachpb.Key("c")
	ba.Requests = nil
	cputArgs := roachpb.ConditionalPutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	cputArgs.Sequence = 3
	ba.Add(&cputArgs)
	initPutArgs := roachpb.InitPutRequest{RequestHeader: roachpb.RequestHeader{Key: keyB}}
	initPutArgs.Sequence = 4
	ba.Add(&initPutArgs)
	incArgs := roachpb.IncrementRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}}
	incArgs.Sequence = 5
	ba.Add(&incArgs)
	// Write at the same key as another write in the same batch. Will only
	// result in a single outstanding write, at the larger sequence number.
	delArgs := roachpb.DeleteRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}}
	delArgs.Sequence = 6
	ba.Add(&delArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 5, len(ba.Requests))
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.ConditionalPutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.InitPutRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &roachpb.IncrementRequest{}, ba.Requests[3].GetInner())
		require.IsType(t, &roachpb.DeleteRequest{}, ba.Requests[4].GetInner())

		qiReq := ba.Requests[0].GetInner().(*roachpb.QueryIntentRequest)
		require.Equal(t, keyA, qiReq.Key)
		require.Equal(t, txn.ID, qiReq.Txn.ID)
		require.Equal(t, txn.Timestamp, qiReq.Txn.Timestamp)
		require.Equal(t, int32(2), qiReq.Txn.Sequence)
		require.Equal(t, roachpb.QueryIntentRequest_RETURN_ERROR, qiReq.IfMissing)

		// No outstanding writes have been proved yet.
		require.Equal(t, 1, tp.outstandingWritesLen())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Equal(t, 4, len(br.Responses)) // QueryIntent response stripped
	require.IsType(t, &roachpb.ConditionalPutResponse{}, br.Responses[0].GetInner())
	require.IsType(t, &roachpb.InitPutResponse{}, br.Responses[1].GetInner())
	require.IsType(t, &roachpb.IncrementResponse{}, br.Responses[2].GetInner())
	require.IsType(t, &roachpb.DeleteResponse{}, br.Responses[3].GetInner())
	require.Nil(t, pErr)
	require.Equal(t, 3, tp.outstandingWritesLen())

	wMin := tp.outstandingWrites.Min().(*outstandingWrite)
	require.Equal(t, cputArgs.Key, wMin.Key)
	require.Equal(t, cputArgs.Sequence, wMin.Sequence)
	wMax := tp.outstandingWrites.Max().(*outstandingWrite)
	require.Equal(t, delArgs.Key, wMax.Key)
	require.Equal(t, delArgs.Sequence, wMax.Sequence)

	// Send a final write, along with an EndTransaction request. Should attempt
	// to prove all outstanding writes. Should NOT use async consensus.
	keyD := roachpb.Key("d")
	ba.Requests = nil
	putArgs2 := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyD}}
	putArgs2.Sequence = 7
	ba.Add(&putArgs2)
	ba.Add(&roachpb.EndTransactionRequest{Commit: true})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 5, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[3].GetInner())
		require.IsType(t, &roachpb.EndTransactionRequest{}, ba.Requests[4].GetInner())

		qiReq1 := ba.Requests[1].GetInner().(*roachpb.QueryIntentRequest)
		qiReq2 := ba.Requests[2].GetInner().(*roachpb.QueryIntentRequest)
		qiReq3 := ba.Requests[3].GetInner().(*roachpb.QueryIntentRequest)
		require.Equal(t, keyA, qiReq1.Key)
		require.Equal(t, keyB, qiReq2.Key)
		require.Equal(t, keyC, qiReq3.Key)
		require.Equal(t, int32(3), qiReq1.Txn.Sequence)
		require.Equal(t, int32(4), qiReq2.Txn.Sequence)
		require.Equal(t, int32(6), qiReq3.Txn.Sequence)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.COMMITTED
		br.Responses[1].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		br.Responses[2].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		br.Responses[3].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Equal(t, 2, len(br.Responses)) // QueryIntent response stripped
	require.Nil(t, pErr)
	require.Equal(t, 0, tp.outstandingWritesLen())
}

// TestTxnPipelinerReads tests that txnPipeliner will never instruct batches
// with reads in them to use async consensus. It also tests that these reading
// batches will still chain on to outstanding writers, if necessary.
func TestTxnPipelinerReads(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	txn := makeTxnProto()
	keyA, keyC := roachpb.Key("a"), roachpb.Key("c")

	// Read-only.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 1, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.GetRequest{}, ba.Requests[0].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)

	// Read before write.
	ba.Requests = nil
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.GetRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)

	// Read after write.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}})
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.GetRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)

	// Add a key into the outstanding writes set.
	tp.maybeInsertOutstandingWriteLocked(keyA, 10)
	require.Equal(t, 1, tp.outstandingWritesLen())

	// Read-only with conflicting outstanding write.
	ba.Requests = nil
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.GetRequest{}, ba.Requests[1].GetInner())

		qiReq := ba.Requests[0].GetInner().(*roachpb.QueryIntentRequest)
		require.Equal(t, keyA, qiReq.Key)
		require.Equal(t, int32(10), qiReq.Txn.Sequence)

		// No outstanding writes have been proved yet.
		require.Equal(t, 1, tp.outstandingWritesLen())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 0, tp.outstandingWritesLen())
}

// TestTxnPipelinerRangedWrites tests that txnPipeliner will never perform
// ranged write operations using async consensus. It also tests that ranged
// writes will correctly chain on to existing outstanding writes.
func TestTxnPipelinerRangedWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	txn := makeTxnProto()
	keyA, keyD := roachpb.Key("a"), roachpb.Key("d")

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.DeleteRangeRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyD}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.DeleteRangeRequest{}, ba.Requests[1].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	// The PutRequest was not run asynchronously, so it is not outstanding.
	require.Equal(t, 0, tp.outstandingWritesLen())

	// Add five keys into the outstanding writes set, one of which overlaps with
	// the Put request and two others which also overlap with the DeleteRange
	// request. Send the batch again and assert that the Put chains onto the
	// first outstanding write and the DeleteRange chains onto the second and
	// third outstanding write.
	tp.maybeInsertOutstandingWriteLocked(roachpb.Key("a"), 10)
	tp.maybeInsertOutstandingWriteLocked(roachpb.Key("b"), 11)
	tp.maybeInsertOutstandingWriteLocked(roachpb.Key("c"), 12)
	tp.maybeInsertOutstandingWriteLocked(roachpb.Key("d"), 13)
	tp.maybeInsertOutstandingWriteLocked(roachpb.Key("e"), 13)
	require.Equal(t, 5, tp.outstandingWritesLen())

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 5, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[3].GetInner())
		require.IsType(t, &roachpb.DeleteRangeRequest{}, ba.Requests[4].GetInner())

		qiReq1 := ba.Requests[0].GetInner().(*roachpb.QueryIntentRequest)
		qiReq2 := ba.Requests[2].GetInner().(*roachpb.QueryIntentRequest)
		qiReq3 := ba.Requests[3].GetInner().(*roachpb.QueryIntentRequest)
		require.Equal(t, roachpb.Key("a"), qiReq1.Key)
		require.Equal(t, roachpb.Key("b"), qiReq2.Key)
		require.Equal(t, roachpb.Key("c"), qiReq3.Key)
		require.Equal(t, txn.ID, qiReq1.Txn.ID)
		require.Equal(t, txn.ID, qiReq2.Txn.ID)
		require.Equal(t, txn.ID, qiReq3.Txn.ID)
		require.Equal(t, int32(10), qiReq1.Txn.Sequence)
		require.Equal(t, int32(11), qiReq2.Txn.Sequence)
		require.Equal(t, int32(12), qiReq3.Txn.Sequence)

		// No outstanding writes have been proved yet.
		require.Equal(t, 5, tp.outstandingWritesLen())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		br.Responses[2].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		br.Responses[3].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 2, tp.outstandingWritesLen())
}

// TestTxnPipelinerNonTransactionalRequests tests that non-transaction requests
// cause the txnPipeliner to stall its entire pipeline.
func TestTxnPipelinerNonTransactionalRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	txn := makeTxnProto()
	keyA, keyC := roachpb.Key("a"), roachpb.Key("c")

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 2, tp.outstandingWritesLen())

	// Send a non-transactional request. Should stall pipeline and chain onto
	// all outstanding writes, even if its header doesn't imply any interaction.
	keyRangeDesc := roachpb.Key("rangeDesc")
	ba.Requests = nil
	ba.Add(&roachpb.SubsumeRequest{
		RequestHeader: roachpb.RequestHeader{Key: keyRangeDesc},
	})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 3, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.SubsumeRequest{}, ba.Requests[2].GetInner())

		qiReq1 := ba.Requests[0].GetInner().(*roachpb.QueryIntentRequest)
		qiReq2 := ba.Requests[1].GetInner().(*roachpb.QueryIntentRequest)
		require.Equal(t, keyA, qiReq1.Key)
		require.Equal(t, keyC, qiReq2.Key)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		br.Responses[1].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 0, tp.outstandingWritesLen())
}

// TestTxnPipelinerManyWrites tests that a txnPipeliner behaves correctly even
// when its outstanding write tree grows to a very large size.
func TestTxnPipelinerManyWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	// Disable maxBatchSize limit.
	pipelinedWritesMaxBatchSize.Override(&tp.st.SV, 0)

	const writes = 2048
	keyBuf := roachpb.Key(strings.Repeat("a", writes+1))
	makeKey := func(i int) roachpb.Key { return keyBuf[:i+1] }
	makeSeq := func(i int) int32 { return int32(i) + 1 }

	txn := makeTxnProto()
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	for i := 0; i < writes; i++ {
		key := makeKey(i)
		if i == 0 {
			ba.Add(&roachpb.BeginTransactionRequest{RequestHeader: roachpb.RequestHeader{Key: key}})
		}
		putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: key}}
		putArgs.Sequence = makeSeq(i)
		ba.Add(&putArgs)
	}

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, writes+1, len(ba.Requests))
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.BeginTransactionRequest{}, ba.Requests[0].GetInner())
		for i := 0; i < writes; i++ {
			require.IsType(t, &roachpb.PutRequest{}, ba.Requests[i+1].GetInner())
		}

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, writes, tp.outstandingWritesLen())

	// Query every other write.
	ba.Requests = nil
	for i := 0; i < writes; i++ {
		if i%2 == 0 {
			key := makeKey(i)
			ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: key}})
		}
	}

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, writes, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		for i := 0; i < writes; i++ {
			if i%2 == 0 {
				key := makeKey(i)
				require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[i].GetInner())
				require.IsType(t, &roachpb.GetRequest{}, ba.Requests[i+1].GetInner())

				qiReq := ba.Requests[i].GetInner().(*roachpb.QueryIntentRequest)
				require.Equal(t, key, qiReq.Key)
				require.Equal(t, txn.ID, qiReq.Txn.ID)
				require.Equal(t, makeSeq(i), qiReq.Txn.Sequence)

				getReq := ba.Requests[i+1].GetInner().(*roachpb.GetRequest)
				require.Equal(t, key, getReq.Key)
			}
		}

		br = ba.CreateReply()
		br.Txn = ba.Txn
		for i := 0; i < writes; i++ {
			if i%2 == 0 {
				br.Responses[i].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
			}
		}
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, writes/2, tp.outstandingWritesLen())

	// Make sure the correct writes are still outstanding.
	expIdx := 1
	tp.outstandingWrites.Ascend(func(i btree.Item) bool {
		w := i.(*outstandingWrite)
		require.Equal(t, makeKey(expIdx), w.Key)
		require.Equal(t, makeSeq(expIdx), w.Sequence)
		expIdx += 2
		return true
	})
}

// TestTxnPipelinerTransactionAbort tests that a txnPipeliner allows an aborting
// EndTransactionRequest to proceed without attempting to prove all outstanding
// writes.
func TestTxnPipelinerTransactionAbort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.BeginTransactionRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	putArgs.Sequence = 2
	ba.Add(&putArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.BeginTransactionRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 1, tp.outstandingWritesLen())

	// Send an EndTransaction request with commit=false. Should NOT attempt
	// to prove all outstanding writes because its attempting to abort the
	// txn anyway. Should NOT use async consensus.
	//
	// We'll unrealistically return a PENDING transaction, which won't allow
	// the txnPipeliner to clean up.
	ba.Requests = nil
	ba.Add(&roachpb.EndTransactionRequest{Commit: false})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 1, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.EndTransactionRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.PENDING // keep at PENDING
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 1, tp.outstandingWritesLen()) // nothing proven

	// Send EndTransaction request with commit=false again. Same deal. This
	// time, return ABORTED transaction. This will allow the txnPipeliner to
	// remove all outstanding writes because they are now uncommittable.
	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 1, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.EndTransactionRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.ABORTED
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 0, tp.outstandingWritesLen())
}

// TestTxnPipelinerEpochIncrement tests that a txnPipeliner's outstanding write
// set is reset on an epoch increment.
func TestTxnPipelinerEpochIncrement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tp, _ := makeMockTxnPipeliner()

	tp.maybeInsertOutstandingWriteLocked(roachpb.Key("b"), 10)
	tp.maybeInsertOutstandingWriteLocked(roachpb.Key("d"), 11)
	require.Equal(t, 2, tp.outstandingWritesLen())

	tp.epochBumpedLocked()
	require.Equal(t, 0, tp.outstandingWritesLen())
}

// TestTxnPipelinerIntentMissingError tests that a txnPipeliner transforms an
// IntentMissingError into a TransactionRetryError. It also ensures that it
// fixes the errors index.
func TestTxnPipelinerIntentMissingError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.DeleteRangeRequest{RequestHeader: roachpb.RequestHeader{Key: keyB, EndKey: keyD}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyD}})

	// Insert outstanding writes into the outstanding write set so that each
	// request will need to chain on with a QueryIntent.
	tp.maybeInsertOutstandingWriteLocked(keyA, 1)
	tp.maybeInsertOutstandingWriteLocked(keyB, 2)
	tp.maybeInsertOutstandingWriteLocked(keyC, 3)
	tp.maybeInsertOutstandingWriteLocked(keyD, 4)

	for errIdx, resErrIdx := range map[int32]int32{
		0: 0, // intent on key "a" missing
		2: 1, // intent on key "b" missing
		3: 1, // intent on key "c" missing
		5: 2, // intent on key "d" missing
	} {
		t.Run(fmt.Sprintf("errIdx=%d", errIdx), func(t *testing.T) {
			mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Equal(t, 7, len(ba.Requests))
				require.False(t, ba.AsyncConsensus)
				require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())
				require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[2].GetInner())
				require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[3].GetInner())
				require.IsType(t, &roachpb.DeleteRangeRequest{}, ba.Requests[4].GetInner())
				require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[5].GetInner())
				require.IsType(t, &roachpb.PutRequest{}, ba.Requests[6].GetInner())

				err := roachpb.NewIntentMissingError(nil)
				pErr := roachpb.NewErrorWithTxn(err, &txn)
				pErr.SetErrorIndex(errIdx)
				return nil, pErr
			})

			br, pErr := tp.SendLocked(ctx, ba)
			require.Nil(t, br)
			require.NotNil(t, pErr)
			require.Equal(t, &txn, pErr.GetTxn())
			require.Equal(t, resErrIdx, pErr.Index.Index)
			require.IsType(t, &roachpb.TransactionRetryError{}, pErr.GetDetail())
			require.Equal(t, roachpb.RETRY_ASYNC_WRITE_FAILURE, pErr.GetDetail().(*roachpb.TransactionRetryError).Reason)
		})
	}
}

// TestTxnPipelinerEnableDisableMixTxn tests that the txnPipeliner behaves
// correctly if pipelining is enabled or disabled midway through a transaction.
func TestTxnPipelinerEnableDisableMixTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	// Start with pipelining disabled. Should NOT use async consensus.
	pipelinedWritesEnabled.Override(&tp.st.SV, false)

	txn := makeTxnProto()
	keyA, keyC := roachpb.Key("a"), roachpb.Key("c")

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.BeginTransactionRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	putArgs.Sequence = 1
	ba.Add(&putArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.BeginTransactionRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 0, tp.outstandingWritesLen())

	// Enable pipelining. Should use async consensus.
	pipelinedWritesEnabled.Override(&tp.st.SV, true)

	ba.Requests = nil
	putArgs2 := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	putArgs2.Sequence = 2
	ba.Add(&putArgs2)
	putArgs3 := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}}
	putArgs3.Sequence = 3
	ba.Add(&putArgs3)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 2, tp.outstandingWritesLen())

	// Disable pipelining again. Should NOT use async consensus but should still
	// make sure to chain on to any overlapping outstanding writes.
	pipelinedWritesEnabled.Override(&tp.st.SV, false)

	ba.Requests = nil
	putArgs4 := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	putArgs4.Sequence = 4
	ba.Add(&putArgs4)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

		qiReq := ba.Requests[0].GetInner().(*roachpb.QueryIntentRequest)
		require.Equal(t, keyA, qiReq.Key)
		require.Equal(t, int32(2), qiReq.Txn.Sequence)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 1, tp.outstandingWritesLen())

	// Commit the txn. Again with pipeling disabled. Again, outstanding writes
	// should be proven first.
	ba.Requests = nil
	ba.Add(&roachpb.EndTransactionRequest{Commit: true})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.EndTransactionRequest{}, ba.Requests[1].GetInner())

		qiReq := ba.Requests[0].GetInner().(*roachpb.QueryIntentRequest)
		require.Equal(t, keyC, qiReq.Key)
		require.Equal(t, int32(3), qiReq.Txn.Sequence)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.COMMITTED
		br.Responses[0].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 0, tp.outstandingWritesLen())
}

// TestTxnPipelinerMaxBatchSize tests that batches that contain more requests
// than allowed by the maxBatchSize setting will not be pipelined.
func TestTxnPipelinerMaxBatchSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	// Set maxBatchSize limit to 1.
	pipelinedWritesMaxBatchSize.Override(&tp.st.SV, 1)

	txn := makeTxnProto()
	keyA, keyC := roachpb.Key("a"), roachpb.Key("c")

	// Batch below limit.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.BeginTransactionRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.BeginTransactionRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 1, tp.outstandingWritesLen())

	// Batch above limit.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 3, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[2].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 0, tp.outstandingWritesLen())

	// Increase maxBatchSize limit to 2.
	pipelinedWritesMaxBatchSize.Override(&tp.st.SV, 2)

	// Same batch now below limit.
	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 2, tp.outstandingWritesLen())
}
