// Copyright 2018 The Cockroach Authors.
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
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// mockLockedSender implements the lockedSender interface and provides a way to
// mock out and adjust the SendLocked method. If no mock function is set, a call
// to SendLocked will return the default successful response.
type mockLockedSender struct {
	mockFn func(roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)
}

func (m *mockLockedSender) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	if m.mockFn == nil {
		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	}
	return m.mockFn(ba)
}

// MockSend sets the mockLockedSender mocking function.
func (m *mockLockedSender) MockSend(
	fn func(roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error),
) {
	m.mockFn = fn
}

// Reset resets the mockLockedSender mocking function to a no-op.
func (m *mockLockedSender) Reset() {
	m.mockFn = nil
}

// ChainMockSend sets a series of mocking functions on the mockLockedSender.
// The provided mocking functions are set in the order that they are provided
// and a given mocking function is set after the previous one has been called.
func (m *mockLockedSender) ChainMockSend(
	fns ...func(roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error),
) {
	for i := range fns {
		i := i
		fn := fns[i]
		fns[i] = func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			if i < len(fns)-1 {
				m.mockFn = fns[i+1]
			}
			return fn(ba)
		}
	}
	m.mockFn = fns[0]
}

func makeMockTxnPipeliner() (txnPipeliner, *mockLockedSender) {
	mockSender := &mockLockedSender{}
	return txnPipeliner{
		st:      cluster.MakeTestingClusterSettings(),
		wrapped: mockSender,
	}, mockSender
}

func makeTxnProto() roachpb.Transaction {
	return roachpb.MakeTransaction("test", []byte("key"), 0, hlc.Timestamp{WallTime: 10}, 0)
}

// TestTxnPipeliner1PCTransaction tests that the writes performed by 1PC
// transactions are not pipelined by the txnPipeliner. It also tests that the
// interceptor attaches any locks that the batch will acquire as lock spans to
// the EndTxn request except for those locks that correspond to point writes,
// which are attached to the EndTxn request separately.
func TestTxnPipeliner1PCTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	scanArgs := roachpb.ScanRequest{
		RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB},
		KeyLocking:    lock.Exclusive,
	}
	ba.Add(&scanArgs)
	putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	putArgs.Sequence = 1
	ba.Add(&putArgs)
	delRngArgs := roachpb.DeleteRangeRequest{
		RequestHeader: roachpb.RequestHeader{Key: keyC, EndKey: keyD},
	}
	delRngArgs.Sequence = 2
	ba.Add(&delRngArgs)
	ba.Add(&roachpb.EndTxnRequest{Commit: true})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 4)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.ScanRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.DeleteRangeRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &roachpb.EndTxnRequest{}, ba.Requests[3].GetInner())

		etReq := ba.Requests[3].GetEndTxn()
		expLocks := []roachpb.Span{
			{Key: keyA, EndKey: keyB},
			{Key: keyC, EndKey: keyD},
		}
		require.Equal(t, expLocks, etReq.LockSpans)
		expInFlight := []roachpb.SequencedWrite{
			{Key: keyA, Sequence: 1},
		}
		require.Equal(t, expInFlight, etReq.InFlightWrites)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.COMMITTED
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 0, tp.ifWrites.len())
}

// TestTxnPipelinerTrackInFlightWrites tests that txnPipeliner tracks writes
// that were performed with async consensus. It also tests that these writes are
// proved as requests are chained onto them. Finally, it tests that EndTxn
// requests chain on to all existing requests.
func TestTxnPipelinerTrackInFlightWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	putArgs.Sequence = 1
	ba.Add(&putArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 1, tp.ifWrites.len())

	w := tp.ifWrites.t.Min().(*inFlightWrite)
	require.Equal(t, putArgs.Key, w.Key)
	require.Equal(t, putArgs.Sequence, w.Sequence)

	// More writes, one that replaces the other's sequence number.
	keyB, keyC := roachpb.Key("b"), roachpb.Key("c")
	ba.Requests = nil
	cputArgs := roachpb.ConditionalPutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	cputArgs.Sequence = 2
	ba.Add(&cputArgs)
	initPutArgs := roachpb.InitPutRequest{RequestHeader: roachpb.RequestHeader{Key: keyB}}
	initPutArgs.Sequence = 3
	ba.Add(&initPutArgs)
	incArgs := roachpb.IncrementRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}}
	incArgs.Sequence = 4
	ba.Add(&incArgs)
	// Write at the same key as another write in the same batch. Will only
	// result in a single in-flight write, at the larger sequence number.
	delArgs := roachpb.DeleteRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}}
	delArgs.Sequence = 5
	ba.Add(&delArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 5)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.ConditionalPutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.InitPutRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &roachpb.IncrementRequest{}, ba.Requests[3].GetInner())
		require.IsType(t, &roachpb.DeleteRequest{}, ba.Requests[4].GetInner())

		qiReq := ba.Requests[0].GetQueryIntent()
		require.Equal(t, keyA, qiReq.Key)
		require.Equal(t, txn.ID, qiReq.Txn.ID)
		require.Equal(t, txn.WriteTimestamp, qiReq.Txn.WriteTimestamp)
		require.Equal(t, enginepb.TxnSeq(1), qiReq.Txn.Sequence)
		require.True(t, qiReq.ErrorIfMissing)

		// No in-flight writes have been proved yet.
		require.Equal(t, 1, tp.ifWrites.len())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetQueryIntent().FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Len(t, br.Responses, 4) // QueryIntent response stripped
	require.IsType(t, &roachpb.ConditionalPutResponse{}, br.Responses[0].GetInner())
	require.IsType(t, &roachpb.InitPutResponse{}, br.Responses[1].GetInner())
	require.IsType(t, &roachpb.IncrementResponse{}, br.Responses[2].GetInner())
	require.IsType(t, &roachpb.DeleteResponse{}, br.Responses[3].GetInner())
	require.Nil(t, pErr)
	require.Equal(t, 3, tp.ifWrites.len())

	wMin := tp.ifWrites.t.Min().(*inFlightWrite)
	require.Equal(t, cputArgs.Key, wMin.Key)
	require.Equal(t, cputArgs.Sequence, wMin.Sequence)
	wMax := tp.ifWrites.t.Max().(*inFlightWrite)
	require.Equal(t, delArgs.Key, wMax.Key)
	require.Equal(t, delArgs.Sequence, wMax.Sequence)

	// Send a final write, along with an EndTxn request. Should attempt to prove
	// all in-flight writes. Should NOT use async consensus.
	keyD := roachpb.Key("d")
	ba.Requests = nil
	putArgs2 := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyD}}
	putArgs2.Sequence = 6
	ba.Add(&putArgs2)
	etArgs := roachpb.EndTxnRequest{Commit: true}
	etArgs.Sequence = 7
	ba.Add(&etArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 5)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[3].GetInner())
		require.IsType(t, &roachpb.EndTxnRequest{}, ba.Requests[4].GetInner())

		qiReq1 := ba.Requests[1].GetQueryIntent()
		qiReq2 := ba.Requests[2].GetQueryIntent()
		qiReq3 := ba.Requests[3].GetQueryIntent()
		require.Equal(t, keyA, qiReq1.Key)
		require.Equal(t, keyB, qiReq2.Key)
		require.Equal(t, keyC, qiReq3.Key)
		require.Equal(t, enginepb.TxnSeq(2), qiReq1.Txn.Sequence)
		require.Equal(t, enginepb.TxnSeq(3), qiReq2.Txn.Sequence)
		require.Equal(t, enginepb.TxnSeq(5), qiReq3.Txn.Sequence)

		etReq := ba.Requests[4].GetEndTxn()
		require.Equal(t, []roachpb.Span{{Key: keyA}}, etReq.LockSpans)
		expInFlight := []roachpb.SequencedWrite{
			{Key: keyA, Sequence: 2},
			{Key: keyB, Sequence: 3},
			{Key: keyC, Sequence: 5},
			{Key: keyD, Sequence: 6},
		}
		require.Equal(t, expInFlight, etReq.InFlightWrites)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.COMMITTED
		br.Responses[1].GetQueryIntent().FoundIntent = true
		br.Responses[2].GetQueryIntent().FoundIntent = true
		br.Responses[3].GetQueryIntent().FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Len(t, br.Responses, 2) // QueryIntent response stripped
	require.Nil(t, pErr)
	require.Equal(t, 0, tp.ifWrites.len())
}

// TestTxnPipelinerReads tests that txnPipeliner will never instruct batches
// with reads in them to use async consensus. It also tests that these reading
// batches will still chain on to in-flight writes, if necessary.
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
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.GetRequest{}, ba.Requests[0].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Read before write.
	ba.Requests = nil
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.GetRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Read after write.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}})
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.GetRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Add a key into the in-flight writes set.
	tp.ifWrites.insert(keyA, 10)
	require.Equal(t, 1, tp.ifWrites.len())

	// Read-only with conflicting in-flight write.
	ba.Requests = nil
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.GetRequest{}, ba.Requests[1].GetInner())

		qiReq := ba.Requests[0].GetQueryIntent()
		require.Equal(t, keyA, qiReq.Key)
		require.Equal(t, enginepb.TxnSeq(10), qiReq.Txn.Sequence)

		// No in-flight writes have been proved yet.
		require.Equal(t, 1, tp.ifWrites.len())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetQueryIntent().FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 0, tp.ifWrites.len())
}

// TestTxnPipelinerRangedWrites tests that txnPipeliner will never perform
// ranged write operations using async consensus. It also tests that ranged
// writes will correctly chain on to existing in-flight writes.
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
		require.Len(t, ba.Requests, 2)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.DeleteRangeRequest{}, ba.Requests[1].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	// The PutRequest was not run asynchronously, so it is not outstanding.
	require.Equal(t, 0, tp.ifWrites.len())

	// Add five keys into the in-flight writes set, one of which overlaps with
	// the Put request and two others which also overlap with the DeleteRange
	// request. Send the batch again and assert that the Put chains onto the
	// first in-flight write and the DeleteRange chains onto the second and
	// third in-flight write.
	tp.ifWrites.insert(roachpb.Key("a"), 10)
	tp.ifWrites.insert(roachpb.Key("b"), 11)
	tp.ifWrites.insert(roachpb.Key("c"), 12)
	tp.ifWrites.insert(roachpb.Key("d"), 13)
	tp.ifWrites.insert(roachpb.Key("e"), 13)
	require.Equal(t, 5, tp.ifWrites.len())

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 5)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[3].GetInner())
		require.IsType(t, &roachpb.DeleteRangeRequest{}, ba.Requests[4].GetInner())

		qiReq1 := ba.Requests[0].GetQueryIntent()
		qiReq2 := ba.Requests[2].GetQueryIntent()
		qiReq3 := ba.Requests[3].GetQueryIntent()
		require.Equal(t, roachpb.Key("a"), qiReq1.Key)
		require.Equal(t, roachpb.Key("b"), qiReq2.Key)
		require.Equal(t, roachpb.Key("c"), qiReq3.Key)
		require.Equal(t, txn.ID, qiReq1.Txn.ID)
		require.Equal(t, txn.ID, qiReq2.Txn.ID)
		require.Equal(t, txn.ID, qiReq3.Txn.ID)
		require.Equal(t, enginepb.TxnSeq(10), qiReq1.Txn.Sequence)
		require.Equal(t, enginepb.TxnSeq(11), qiReq2.Txn.Sequence)
		require.Equal(t, enginepb.TxnSeq(12), qiReq3.Txn.Sequence)

		// No in-flight writes have been proved yet.
		require.Equal(t, 5, tp.ifWrites.len())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetQueryIntent().FoundIntent = true
		br.Responses[2].GetQueryIntent().FoundIntent = true
		br.Responses[3].GetQueryIntent().FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 2, tp.ifWrites.len())
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
		require.Len(t, ba.Requests, 2)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 2, tp.ifWrites.len())

	// Send a non-transactional request. Should stall pipeline and chain onto
	// all in-flight writes, even if its header doesn't imply any interaction.
	keyRangeDesc := roachpb.Key("rangeDesc")
	ba.Requests = nil
	ba.Add(&roachpb.SubsumeRequest{
		RequestHeader: roachpb.RequestHeader{Key: keyRangeDesc},
	})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.SubsumeRequest{}, ba.Requests[2].GetInner())

		qiReq1 := ba.Requests[0].GetQueryIntent()
		qiReq2 := ba.Requests[1].GetQueryIntent()
		require.Equal(t, keyA, qiReq1.Key)
		require.Equal(t, keyC, qiReq2.Key)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetQueryIntent().FoundIntent = true
		br.Responses[1].GetQueryIntent().FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 0, tp.ifWrites.len())
}

// TestTxnPipelinerManyWrites tests that a txnPipeliner behaves correctly even
// when its in-flight write tree grows to a very large size.
func TestTxnPipelinerManyWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	// Disable write_pipelining_max_outstanding_size,
	// write_pipelining_max_batch_size, and max_intents_bytes limits.
	pipelinedWritesMaxInFlightSize.Override(&tp.st.SV, math.MaxInt64)
	pipelinedWritesMaxBatchSize.Override(&tp.st.SV, 0)
	trackedWritesMaxSize.Override(&tp.st.SV, math.MaxInt64)

	const writes = 2048
	keyBuf := roachpb.Key(strings.Repeat("a", writes+1))
	makeKey := func(i int) roachpb.Key { return keyBuf[:i+1] }
	makeSeq := func(i int) enginepb.TxnSeq { return enginepb.TxnSeq(i) + 1 }

	txn := makeTxnProto()
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	for i := 0; i < writes; i++ {
		putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: makeKey(i)}}
		putArgs.Sequence = makeSeq(i)
		ba.Add(&putArgs)
	}

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, writes)
		require.True(t, ba.AsyncConsensus)
		for i := 0; i < writes; i++ {
			require.IsType(t, &roachpb.PutRequest{}, ba.Requests[i].GetInner())
		}

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, writes, tp.ifWrites.len())

	// Query every other write.
	ba.Requests = nil
	for i := 0; i < writes; i++ {
		if i%2 == 0 {
			key := makeKey(i)
			ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: key}})
		}
	}

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, writes)
		require.False(t, ba.AsyncConsensus)
		for i := 0; i < writes; i++ {
			if i%2 == 0 {
				key := makeKey(i)
				require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[i].GetInner())
				require.IsType(t, &roachpb.GetRequest{}, ba.Requests[i+1].GetInner())

				qiReq := ba.Requests[i].GetQueryIntent()
				require.Equal(t, key, qiReq.Key)
				require.Equal(t, txn.ID, qiReq.Txn.ID)
				require.Equal(t, makeSeq(i), qiReq.Txn.Sequence)

				getReq := ba.Requests[i+1].GetGet()
				require.Equal(t, key, getReq.Key)
			}
		}

		br = ba.CreateReply()
		br.Txn = ba.Txn
		for i := 0; i < writes; i++ {
			if i%2 == 0 {
				br.Responses[i].GetQueryIntent().FoundIntent = true
			}
		}
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, writes/2, tp.ifWrites.len())

	// Make sure the correct writes are still in-flight.
	expIdx := 1
	tp.ifWrites.ascend(func(w *inFlightWrite) {
		require.Equal(t, makeKey(expIdx), w.Key)
		require.Equal(t, makeSeq(expIdx), w.Sequence)
		expIdx += 2
	})
}

// TestTxnPipelinerTransactionAbort tests that a txnPipeliner allows an aborting
// EndTxnRequest to proceed without attempting to prove all in-flight writes. It
// also tests that the interceptor attaches lock spans to these EndTxnRequests.
func TestTxnPipelinerTransactionAbort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	putArgs.Sequence = 1
	ba.Add(&putArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 1, tp.ifWrites.len())

	// Send an EndTxn request with commit=false. Should NOT attempt to prove all
	// in-flight writes because its attempting to abort the txn anyway. Should
	// NOT use async consensus.
	//
	// We'll unrealistically return a PENDING transaction, which won't allow the
	// txnPipeliner to clean up.
	ba.Requests = nil
	etArgs := roachpb.EndTxnRequest{Commit: false}
	etArgs.Sequence = 2
	ba.Add(&etArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.EndTxnRequest{}, ba.Requests[0].GetInner())

		etReq := ba.Requests[0].GetEndTxn()
		require.Len(t, etReq.LockSpans, 0)
		require.Equal(t, []roachpb.SequencedWrite{{Key: keyA, Sequence: 1}}, etReq.InFlightWrites)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.PENDING // keep at PENDING
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 1, tp.ifWrites.len()) // nothing proven

	// Send EndTxn request with commit=false again. Same deal. This time, return
	// ABORTED transaction. This will allow the txnPipeliner to remove all
	// in-flight writes because they are now uncommittable.
	ba.Requests = nil
	etArgs = roachpb.EndTxnRequest{Commit: false}
	etArgs.Sequence = 2
	ba.Add(&etArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.EndTxnRequest{}, ba.Requests[0].GetInner())

		etReq := ba.Requests[0].GetEndTxn()
		require.Len(t, etReq.LockSpans, 0)
		require.Equal(t, []roachpb.SequencedWrite{{Key: keyA, Sequence: 1}}, etReq.InFlightWrites)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.ABORTED
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 1, tp.ifWrites.len()) // nothing proven
}

// TestTxnPipelinerEpochIncrement tests that a txnPipeliner's in-flight write
// set is reset on an epoch increment and that all writes in this set are moved
// to the lock footprint so they will be removed when the transaction finishes.
func TestTxnPipelinerEpochIncrement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tp, _ := makeMockTxnPipeliner()

	tp.ifWrites.insert(roachpb.Key("b"), 10)
	tp.ifWrites.insert(roachpb.Key("d"), 11)
	require.Equal(t, 2, tp.ifWrites.len())
	require.Equal(t, 0, len(tp.lockFootprint.asSlice()))

	tp.epochBumpedLocked()
	require.Equal(t, 0, tp.ifWrites.len())
	require.Equal(t, 2, len(tp.lockFootprint.asSlice()))
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

	// Insert in-flight writes into the in-flight write set so that each request
	// will need to chain on with a QueryIntent.
	tp.ifWrites.insert(keyA, 1)
	tp.ifWrites.insert(keyB, 2)
	tp.ifWrites.insert(keyC, 3)
	tp.ifWrites.insert(keyD, 4)

	for errIdx, resErrIdx := range map[int32]int32{
		0: 0, // intent on key "a" missing
		2: 1, // intent on key "b" missing
		3: 1, // intent on key "c" missing
		5: 2, // intent on key "d" missing
	} {
		t.Run(fmt.Sprintf("errIdx=%d", errIdx), func(t *testing.T) {
			mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Len(t, ba.Requests, 7)
				require.False(t, ba.AsyncConsensus)
				require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())
				require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[2].GetInner())
				require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[3].GetInner())
				require.IsType(t, &roachpb.DeleteRangeRequest{}, ba.Requests[4].GetInner())
				require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[5].GetInner())
				require.IsType(t, &roachpb.PutRequest{}, ba.Requests[6].GetInner())

				err := roachpb.NewIntentMissingError(nil /* key */, nil /* intent */)
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
	putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	putArgs.Sequence = 1
	ba.Add(&putArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 0, tp.ifWrites.len())

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
		require.Len(t, ba.Requests, 2)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 2, tp.ifWrites.len())

	// Disable pipelining again. Should NOT use async consensus but should still
	// make sure to chain on to any overlapping in-flight writes.
	pipelinedWritesEnabled.Override(&tp.st.SV, false)

	ba.Requests = nil
	putArgs4 := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	putArgs4.Sequence = 4
	ba.Add(&putArgs4)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

		qiReq := ba.Requests[0].GetQueryIntent()
		require.Equal(t, keyA, qiReq.Key)
		require.Equal(t, enginepb.TxnSeq(2), qiReq.Txn.Sequence)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetQueryIntent().FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 1, tp.ifWrites.len())

	// Commit the txn. Again with pipeling disabled. Again, in-flight writes
	// should be proven first.
	ba.Requests = nil
	etArgs := roachpb.EndTxnRequest{Commit: true}
	etArgs.Sequence = 5
	ba.Add(&etArgs)

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.EndTxnRequest{}, ba.Requests[1].GetInner())

		qiReq := ba.Requests[0].GetQueryIntent()
		require.Equal(t, keyC, qiReq.Key)
		require.Equal(t, enginepb.TxnSeq(3), qiReq.Txn.Sequence)

		etReq := ba.Requests[1].GetEndTxn()
		require.Equal(t, []roachpb.Span{{Key: keyA}}, etReq.LockSpans)
		require.Equal(t, []roachpb.SequencedWrite{{Key: keyC, Sequence: 3}}, etReq.InFlightWrites)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.COMMITTED
		br.Responses[0].GetQueryIntent().FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 0, tp.ifWrites.len())
}

// TestTxnPipelinerMaxInFlightSize tests that batches are not pipelined if
// doing so would push the memory used to track in-flight writes over the
// limit allowed by the kv.transaction.write_pipelining_max_outstanding_size
// setting.
func TestTxnPipelinerMaxInFlightSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	// Set maxInFlightSize limit to 3 bytes.
	pipelinedWritesMaxInFlightSize.Override(&tp.st.SV, 3)

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

	// Send a batch that would exceed the limit.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyB}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyD}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 4)
		require.False(t, ba.AsyncConsensus)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, int64(0), tp.ifWrites.byteSize())

	// Send a batch that is equal to the limit.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyB}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.True(t, ba.AsyncConsensus)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, int64(3), tp.ifWrites.byteSize())

	// Send a batch that would be under the limit if we weren't already at it.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyD}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.AsyncConsensus)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, int64(3), tp.ifWrites.byteSize())

	// Send a batch that proves two of the in-flight writes.
	ba.Requests = nil
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyB}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 4)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.GetRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &roachpb.GetRequest{}, ba.Requests[3].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetQueryIntent().FoundIntent = true
		br.Responses[2].GetQueryIntent().FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, int64(1), tp.ifWrites.byteSize())

	// Now that we're not up against the limit, send a batch that proves one
	// write and immediately writes it again, along with a second write.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyB}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[2].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[1].GetQueryIntent().FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, int64(2), tp.ifWrites.byteSize())

	// Send the same batch again. Even though it would prove two in-flight
	// writes while performing two others, we won't allow it to perform async
	// consensus because the estimation is conservative.
	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 4)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[3].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetQueryIntent().FoundIntent = true
		br.Responses[2].GetQueryIntent().FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, int64(0), tp.ifWrites.byteSize())

	// Increase maxInFlightSize limit to 5 bytes.
	pipelinedWritesMaxInFlightSize.Override(&tp.st.SV, 5)

	// The original batch with 4 writes should succeed.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyB}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyD}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 4)
		require.True(t, ba.AsyncConsensus)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, int64(4), tp.ifWrites.byteSize())

	// Bump the txn epoch. The in-flight bytes counter should reset.
	tp.epochBumpedLocked()
	require.Equal(t, int64(0), tp.ifWrites.byteSize())
}

// TestTxnPipelinerMaxBatchSize tests that batches that contain more requests
// than allowed by the kv.transaction.write_pipelining_max_batch_size setting
// will not be pipelined.
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
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 1, tp.ifWrites.len())

	// Batch above limit.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[2].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetQueryIntent().FoundIntent = true
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 0, tp.ifWrites.len())

	// Increase maxBatchSize limit to 2.
	pipelinedWritesMaxBatchSize.Override(&tp.st.SV, 2)

	// Same batch now below limit.
	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 2, tp.ifWrites.len())
}

// TestTxnPipelinerRecordsLocksOnFailure tests that even when a request returns
// with an ABORTED transaction status or an error, the locks that it attempted
// to acquire are added to the lock footprint.
func TestTxnPipelinerRecordsLocksOnFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	txn := makeTxnProto()
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
	keyD, keyE, keyF := roachpb.Key("d"), roachpb.Key("e"), roachpb.Key("f")

	// Return an error for a point write, a range write, and a range locking
	// read.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.DeleteRangeRequest{RequestHeader: roachpb.RequestHeader{Key: keyB, EndKey: keyB.Next()}})
	ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyC, EndKey: keyC.Next()}, KeyLocking: lock.Exclusive})

	mockPErr := roachpb.NewErrorf("boom")
	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.DeleteRangeRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.ScanRequest{}, ba.Requests[2].GetInner())

		return nil, mockPErr
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.Nil(t, br)
	require.Equal(t, mockPErr, pErr)
	require.Equal(t, 0, tp.ifWrites.len())

	var expLocks []roachpb.Span
	expLocks = append(expLocks, roachpb.Span{Key: keyA})
	expLocks = append(expLocks, roachpb.Span{Key: keyB, EndKey: keyB.Next()})
	expLocks = append(expLocks, roachpb.Span{Key: keyC, EndKey: keyC.Next()})
	require.Equal(t, expLocks, tp.lockFootprint.asSlice())

	// Return an ABORTED transaction record for a point write, a range write,
	// and a range locking read.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyD}})
	ba.Add(&roachpb.DeleteRangeRequest{RequestHeader: roachpb.RequestHeader{Key: keyE, EndKey: keyE.Next()}})
	ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyF, EndKey: keyF.Next()}, KeyLocking: lock.Exclusive})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.DeleteRangeRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &roachpb.ScanRequest{}, ba.Requests[2].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 0, tp.ifWrites.len())

	expLocks = append(expLocks, roachpb.Span{Key: keyD})
	expLocks = append(expLocks, roachpb.Span{Key: keyE, EndKey: keyE.Next()})
	expLocks = append(expLocks, roachpb.Span{Key: keyF, EndKey: keyF.Next()})
	require.Equal(t, expLocks, tp.lockFootprint.asSlice())

	// The lock spans are all attached to the EndTxn request when one is sent.
	ba.Requests = nil
	ba.Add(&roachpb.EndTxnRequest{Commit: false})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.IsType(t, &roachpb.EndTxnRequest{}, ba.Requests[0].GetInner())

		etReq := ba.Requests[0].GetEndTxn()
		require.Equal(t, expLocks, etReq.LockSpans)
		require.Len(t, etReq.InFlightWrites, 0)

		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.ABORTED
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
}

// Test that the pipeliners knows how to save and restore its state.
func TestTxnPipelinerSavepoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner()

	initialSavepoint := savepoint{}
	tp.createSavepointLocked(ctx, &initialSavepoint)

	tp.ifWrites.insert(roachpb.Key("a"), 10)
	tp.ifWrites.insert(roachpb.Key("b"), 11)
	tp.ifWrites.insert(roachpb.Key("c"), 12)
	require.Equal(t, 3, tp.ifWrites.len())

	s := savepoint{seqNum: enginepb.TxnSeq(12), active: true}
	tp.createSavepointLocked(ctx, &s)

	// Some more writes after the savepoint. One of them is on key "c" that is
	// part of the savepoint too, so we'll check that, upon rollback, the savepoint is
	// updated to remove the lower-seq-num write to "c" that it was tracking as in-flight.
	tp.ifWrites.insert(roachpb.Key("c"), 13)
	tp.ifWrites.insert(roachpb.Key("d"), 14)
	require.Empty(t, tp.lockFootprint.asSlice())

	// Now verify one of the writes. When we'll rollback to the savepoint below,
	// we'll check that the verified write stayed verified.
	txn := makeTxnProto()
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: roachpb.Key("a")}})
	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &roachpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &roachpb.GetRequest{}, ba.Requests[1].GetInner())

		qiReq := ba.Requests[0].GetInner().(*roachpb.QueryIntentRequest)
		require.Equal(t, roachpb.Key("a"), qiReq.Key)
		require.Equal(t, enginepb.TxnSeq(10), qiReq.Txn.Sequence)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetQueryIntent().FoundIntent = true
		return br, nil
	})
	br, pErr := tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, []roachpb.Span{{Key: roachpb.Key("a")}}, tp.lockFootprint.asSlice())
	require.Equal(t, 3, tp.ifWrites.len()) // We've verified one out of 4 writes.

	// Now restore the savepoint and check that the in-flight write state has been restored
	// and all rolled-back writes were moved to the lock footprint.
	tp.rollbackToSavepointLocked(ctx, s)

	// Check that the tracked inflight writes were updated correctly. The key that
	// had been verified ("a") should have been taken out of the savepoint. Same
	// for the "c", for which the pipeliner is now tracking a
	// higher-sequence-number (which implies that it must have verified the lower
	// sequence number write).
	var ifWrites []inFlightWrite
	tp.ifWrites.ascend(func(w *inFlightWrite) {
		ifWrites = append(ifWrites, *w)
	})
	require.Equal(t,
		[]inFlightWrite{
			{roachpb.SequencedWrite{Key: roachpb.Key("b"), Sequence: 11}},
		},
		ifWrites)

	// Check that the footprint was updated correctly. In addition to the "a"
	// which it had before, it will also have "d" because it's not part of the
	// savepoint. It will also have "c" since that's not an in-flight write any
	// more (see above).
	require.Equal(t,
		[]roachpb.Span{
			{Key: roachpb.Key("a")},
			{Key: roachpb.Key("c")},
			{Key: roachpb.Key("d")},
		},
		tp.lockFootprint.asSlice())

	// Now rollback to the initial savepoint and check that all in-flight writes are gone.
	tp.rollbackToSavepointLocked(ctx, initialSavepoint)
	require.Empty(t, tp.ifWrites.len())
}
