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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
	tp, mockSender := makeMockTxnPipeliner()

	txn := makeTxnProto()
	key := roachpb.Key("a")

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.BeginTransactionRequest{RequestHeader: roachpb.RequestHeader{Key: key}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: key}})
	ba.Add(&roachpb.EndTransactionRequest{Commit: true})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 3, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.BeginTransactionRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.EndTransactionRequest{}, ba.Requests[2].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(context.Background(), ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 0, tp.outstandingWritesLen())
}

// TestTxnPipelinerTrackOutstandingWrites tests that txnPipeliner tracks writes
// that were performed with async consensus. It also tests that these writes are
// resolved as requests are chained onto them. Finally, it tests that EndTxn
// requests chain on to all existing requests.
func TestTxnPipelinerTrackOutstandingWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tp, mockSender := makeMockTxnPipeliner()

	txn := makeTxnProto()
	key := roachpb.Key("a")

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.BeginTransactionRequest{RequestHeader: roachpb.RequestHeader{Key: key}})
	putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: key}}
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

	br, pErr := tp.SendLocked(context.Background(), ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 1, tp.outstandingWritesLen())

	w := tp.outstandingWrites.Min().(*outstandingWrite)
	require.Equal(t, putArgs.Key, w.Key)
	require.Equal(t, putArgs.Sequence, w.Sequence)

	// More writes, one that replaces the other's sequence number.
	key2, key3 := roachpb.Key("b"), roachpb.Key("c")
	ba.Requests = nil
	cputArgs := roachpb.ConditionalPutRequest{RequestHeader: roachpb.RequestHeader{Key: key}}
	cputArgs.Sequence = 3
	ba.Add(&cputArgs)
	initPutArgs := roachpb.InitPutRequest{RequestHeader: roachpb.RequestHeader{Key: key2}}
	initPutArgs.Sequence = 4
	ba.Add(&initPutArgs)
	incArgs := roachpb.IncrementRequest{RequestHeader: roachpb.RequestHeader{Key: key3}}
	incArgs.Sequence = 5
	ba.Add(&incArgs)
	// Write at the same key as another write in the same batch. Will only
	// result in a single outstanding write, at the larger sequence number.
	delArgs := roachpb.DeleteRequest{RequestHeader: roachpb.RequestHeader{Key: key3}}
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
		require.Equal(t, key, qiReq.Key)
		require.Equal(t, txn.ID, qiReq.Txn.ID)
		require.Equal(t, txn.Timestamp, qiReq.Txn.Timestamp)
		require.Equal(t, int32(2), qiReq.Txn.Sequence)
		require.Equal(t, roachpb.QueryIntentRequest_RETURN_ERROR, qiReq.IfMissing)

		// No outstanding writes have been resolved yet.
		require.Equal(t, 1, tp.outstandingWritesLen())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(context.Background(), ba)
	require.NotNil(t, br)
	require.Equal(t, 4, len(br.Responses)) // QueryIntent response stripped
	require.Nil(t, pErr)
	require.Equal(t, 3, tp.outstandingWritesLen())

	wMin := tp.outstandingWrites.Min().(*outstandingWrite)
	require.Equal(t, cputArgs.Key, wMin.Key)
	require.Equal(t, cputArgs.Sequence, wMin.Sequence)
	wMax := tp.outstandingWrites.Max().(*outstandingWrite)
	require.Equal(t, delArgs.Key, wMax.Key)
	require.Equal(t, delArgs.Sequence, wMax.Sequence)

	// Send a final write, along with an EndTransaction request. Should attempt
	// to resolve all outstanding writes. Should NOT use async consensus.
	key4 := roachpb.Key("d")
	ba.Requests = nil
	putArgs2 := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: key4}}
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
		require.Equal(t, key, qiReq1.Key)
		require.Equal(t, key2, qiReq2.Key)
		require.Equal(t, key3, qiReq3.Key)
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

	br, pErr = tp.SendLocked(context.Background(), ba)
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
	tp, mockSender := makeMockTxnPipeliner()

	txn := makeTxnProto()
	key, key2 := roachpb.Key("a"), roachpb.Key("c")

	// Read-only.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: key}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 1, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.GetRequest{}, ba.Requests[0].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(context.Background(), ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)

	// Read before write.
	ba.Requests = nil
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: key}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: key2}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.GetRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tp.SendLocked(context.Background(), ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)

	// Read after write.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: key2}})
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: key}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.GetRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tp.SendLocked(context.Background(), ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)

	// Add a key into the outstanding writes set.
	tp.maybeInsertOutstandingWriteLocked(key, 10)
	require.Equal(t, 1, tp.outstandingWritesLen())

	// Read-only with conflicting outstanding write.
	ba.Requests = nil
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: key}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.GetRequest{}, ba.Requests[1].GetInner())

		qiReq := ba.Requests[0].GetInner().(*roachpb.QueryIntentRequest)
		require.Equal(t, key, qiReq.Key)
		require.Equal(t, int32(10), qiReq.Txn.Sequence)

		// No outstanding writes have been resolved yet.
		require.Equal(t, 1, tp.outstandingWritesLen())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(context.Background(), ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 0, tp.outstandingWritesLen())
}

// TestTxnPipelinerRangedWrites tests that txnPipeliner will never perform
// ranged write operations using async consensus. It also tests that ranged
// writes will correctly chain on to existing outstanding writes.
func TestTxnPipelinerRangedWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tp, mockSender := makeMockTxnPipeliner()

	txn := makeTxnProto()
	key, key2 := roachpb.Key("a"), roachpb.Key("c")

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: key}})
	ba.Add(&roachpb.DeleteRangeRequest{RequestHeader: roachpb.RequestHeader{Key: key, EndKey: key2}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.DeleteRangeRequest{}, ba.Requests[1].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(context.Background(), ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	// The PutRequest was not run asynchronously, so it is not outstanding.
	require.Equal(t, 0, tp.outstandingWritesLen())

	// Add two keys into the outstanding writes set, one of which overlaps with
	// the DeleteRange request. Send the batch again and assert that the DeleteRange
	// chains onto the first outstanding write.
	tp.maybeInsertOutstandingWriteLocked(roachpb.Key("b"), 10)
	tp.maybeInsertOutstandingWriteLocked(roachpb.Key("d"), 11)
	require.Equal(t, 2, tp.outstandingWritesLen())

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 3, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.DeleteRangeRequest{}, ba.Requests[2].GetInner())

		qiReq := ba.Requests[1].GetInner().(*roachpb.QueryIntentRequest)
		require.Equal(t, roachpb.Key("b"), qiReq.Key)
		require.Equal(t, txn.ID, qiReq.Txn.ID)
		require.Equal(t, int32(10), qiReq.Txn.Sequence)

		// No outstanding writes have been resolved yet.
		require.Equal(t, 2, tp.outstandingWritesLen())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[1].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(context.Background(), ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 1, tp.outstandingWritesLen())
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

// TestTxnPipelinerMaxBatchSize tests that batches that contain more requests
// than allowed by the maxBatchSize setting will not be pipelined.
func TestTxnPipelinerMaxBatchSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tp, mockSender := makeMockTxnPipeliner()

	// Set maxBatchSize limit to 1.
	pipelinedWritesMaxBatchSize.Override(&tp.st.SV, 1)

	txn := makeTxnProto()
	key, key2 := roachpb.Key("a"), roachpb.Key("c")

	// Batch below limit.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.BeginTransactionRequest{RequestHeader: roachpb.RequestHeader{Key: key}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: key}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 2, len(ba.Requests))
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.BeginTransactionRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(context.Background(), ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 1, tp.outstandingWritesLen())

	// Batch above limit.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: key}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: key2}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Equal(t, 3, len(ba.Requests))
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[2].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetInner().(*roachpb.QueryIntentResponse).FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(context.Background(), ba)
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

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tp.SendLocked(context.Background(), ba)
	require.NotNil(t, br)
	require.Nil(t, pErr)
	require.Equal(t, 2, tp.outstandingWritesLen())
}
