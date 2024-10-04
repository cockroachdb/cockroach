// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// mockLockedSender implements the lockedSender interface and provides a way to
// mock out and adjust the SendLocked method. If no mock function is set, a call
// to SendLocked will return the default successful response.
type mockLockedSender struct {
	mockFn func(*kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error)
}

func (m *mockLockedSender) SendLocked(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	if m.mockFn == nil {
		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	}
	return m.mockFn(ba)
}

// MockSend sets the mockLockedSender mocking function.
func (m *mockLockedSender) MockSend(
	fn func(*kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error),
) {
	m.mockFn = fn
}

// ChainMockSend sets a series of mocking functions on the mockLockedSender.
// The provided mocking functions are set in the order that they are provided
// and a given mocking function is set after the previous one has been called.
func (m *mockLockedSender) ChainMockSend(
	fns ...func(*kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error),
) {
	for i := range fns {
		i := i
		fn := fns[i]
		fns[i] = func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			if i < len(fns)-1 {
				m.mockFn = fns[i+1]
			}
			return fn(ba)
		}
	}
	m.mockFn = fns[0]
}

// makeMockTxnPipeliner creates a txnPipeliner.
//
// iter is the iterator to use for condensing the lock spans. It can be nil, in
// which case the pipeliner will panic if it ever needs to condense lock spans.
func makeMockTxnPipeliner(iter condensableSpanSetRangeIterator) (txnPipeliner, *mockLockedSender) {
	mockSender := &mockLockedSender{}
	metrics := MakeTxnMetrics(time.Hour)
	everyN := log.Every(time.Hour)
	return txnPipeliner{
		st:                     cluster.MakeTestingClusterSettings(),
		wrapped:                mockSender,
		txnMetrics:             &metrics,
		condensedIntentsEveryN: &everyN,
		riGen: rangeIteratorFactory{
			factory: func() condensableSpanSetRangeIterator {
				return iter
			},
		},
	}, mockSender

}

func makeTxnProto() roachpb.Transaction {
	return roachpb.MakeTransaction("test", []byte("key"), isolation.Serializable, 0,
		hlc.Timestamp{WallTime: 10}, 0 /* maxOffsetNs */, 0 /* coordinatorNodeID */, 0, false /* omitInRangefeeds */)
}

// TestTxnPipeliner1PCTransaction tests that the writes performed by 1PC
// transactions are not pipelined by the txnPipeliner. It also tests that the
// interceptor attaches any locks that the batch will acquire as lock spans to
// the EndTxn request except for those locks that correspond to point writes,
// which are attached to the EndTxn request separately.
func TestTxnPipeliner1PCTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner(nil /* iter */)

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	scanArgs := kvpb.ScanRequest{
		RequestHeader:      kvpb.RequestHeader{Key: keyA, EndKey: keyB},
		KeyLockingStrength: lock.Exclusive,
	}
	ba.Add(&scanArgs)
	putArgs := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}}
	putArgs.Sequence = 1
	ba.Add(&putArgs)
	delRngArgs := kvpb.DeleteRangeRequest{
		RequestHeader: kvpb.RequestHeader{Key: keyC, EndKey: keyD},
	}
	delRngArgs.Sequence = 2
	ba.Add(&delRngArgs)
	ba.Add(&kvpb.EndTxnRequest{Commit: true})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 4)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.DeleteRangeRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[3].GetInner())

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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner(nil /* iter */)

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putArgs := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}}
	putArgs.Sequence = 1
	ba.Add(&putArgs)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

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
	cputArgs := kvpb.ConditionalPutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}}
	cputArgs.Sequence = 2
	ba.Add(&cputArgs)
	initPutArgs := kvpb.InitPutRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}}
	initPutArgs.Sequence = 3
	ba.Add(&initPutArgs)
	incArgs := kvpb.IncrementRequest{RequestHeader: kvpb.RequestHeader{Key: keyC}}
	incArgs.Sequence = 4
	ba.Add(&incArgs)
	// Write at the same key as another write in the same batch. Will only
	// result in a single in-flight write, at the larger sequence number.
	delArgs := kvpb.DeleteRequest{RequestHeader: kvpb.RequestHeader{Key: keyC}}
	delArgs.Sequence = 5
	ba.Add(&delArgs)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 5)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.ConditionalPutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.InitPutRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &kvpb.IncrementRequest{}, ba.Requests[3].GetInner())
		require.IsType(t, &kvpb.DeleteRequest{}, ba.Requests[4].GetInner())

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
	require.IsType(t, &kvpb.ConditionalPutResponse{}, br.Responses[0].GetInner())
	require.IsType(t, &kvpb.InitPutResponse{}, br.Responses[1].GetInner())
	require.IsType(t, &kvpb.IncrementResponse{}, br.Responses[2].GetInner())
	require.IsType(t, &kvpb.DeleteResponse{}, br.Responses[3].GetInner())
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
	putArgs2 := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyD}}
	putArgs2.Sequence = 6
	ba.Add(&putArgs2)
	etArgs := kvpb.EndTxnRequest{Commit: true}
	etArgs.Sequence = 7
	ba.Add(&etArgs)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 5)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[3].GetInner())
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[4].GetInner())

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
		// NOTE: expected response from a v23.1 node.
		// TODO(nvanbenschoten): update this case when v23.1 compatibility is no
		// longer required.
		br.Responses[2].GetQueryIntent().FoundIntent = false
		br.Responses[1].GetQueryIntent().FoundUnpushedIntent = true
		// NOTE: expected responses from a v23.2 node.
		br.Responses[2].GetQueryIntent().FoundIntent = true
		br.Responses[2].GetQueryIntent().FoundUnpushedIntent = true
		br.Responses[3].GetQueryIntent().FoundIntent = true
		br.Responses[2].GetQueryIntent().FoundUnpushedIntent = false
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Len(t, br.Responses, 2) // QueryIntent response stripped
	require.Nil(t, pErr)
	require.Equal(t, 0, tp.ifWrites.len())
}

// TestTxnPipelinerTrackInFlightWritesPaginatedResponse tests that txnPipeliner
// handles cases where a batch is paginated and not all in-flight writes that
// were queried are proven to exist.
func TestTxnPipelinerTrackInFlightWritesPaginatedResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner(nil /* iter */)

	txn := makeTxnProto()
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putArgs1 := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}}
	putArgs1.Sequence = 1
	ba.Add(&putArgs1)
	putArgs2 := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}}
	putArgs2.Sequence = 2
	ba.Add(&putArgs2)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 2, tp.ifWrites.len())

	w := tp.ifWrites.t.Min().(*inFlightWrite)
	require.Equal(t, putArgs1.Key, w.Key)
	require.Equal(t, putArgs1.Sequence, w.Sequence)
	w = tp.ifWrites.t.Max().(*inFlightWrite)
	require.Equal(t, putArgs2.Key, w.Key)
	require.Equal(t, putArgs2.Sequence, w.Sequence)

	// Scan both keys with a key limit.
	ba.Requests = nil
	ba.MaxSpanRequestKeys = 1
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyC}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[2].GetInner())

		qiReq1 := ba.Requests[0].GetQueryIntent()
		require.Equal(t, keyA, qiReq1.Key)
		require.Equal(t, enginepb.TxnSeq(1), qiReq1.Txn.Sequence)
		qiReq2 := ba.Requests[1].GetQueryIntent()
		require.Equal(t, keyB, qiReq2.Key)
		require.Equal(t, enginepb.TxnSeq(2), qiReq2.Txn.Sequence)

		// Assume a range split at key "b". DistSender will split this batch into
		// two partial batches:
		//  part1: {QueryIntent(key: "a"), Scan(key: "a", endKey: "b")}
		//  part2: {QueryIntent(key: "b"), Scan(key: "b", endKey: "c")}
		//
		// If part1 hits the batch-wide key limit. DistSender will construct an
		// empty response for QueryIntent(key: "b") in fillSkippedResponses.
		br = ba.CreateReply()
		br.Txn = ba.Txn
		br.Responses[0].GetQueryIntent().FoundIntent = true
		// br.Responses[1].GetQueryIntent() intentionally left empty.
		br.Responses[2].GetScan().ResumeReason = kvpb.RESUME_KEY_LIMIT
		br.Responses[2].GetScan().ResumeSpan = &roachpb.Span{Key: keyB, EndKey: keyC}
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.NotNil(t, br)
	require.Len(t, br.Responses, 1) // QueryIntent responses stripped
	require.Nil(t, pErr)
	require.Equal(t, 1, tp.ifWrites.len())

	w = tp.ifWrites.t.Min().(*inFlightWrite)
	require.Equal(t, putArgs2.Key, w.Key)
	require.Equal(t, putArgs2.Sequence, w.Sequence)
}

// TestTxnPipelinerReads tests that txnPipeliner will never instruct batches
// with reads in them to use async consensus. It also tests that these reading
// batches will still chain on to in-flight writes, if necessary.
func TestTxnPipelinerReads(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner(nil /* iter */)

	txn := makeTxnProto()
	keyA, keyC := roachpb.Key("a"), roachpb.Key("c")

	// Read-only.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Read before write.
	ba.Requests = nil
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyC}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Read after write.
	ba.Requests = nil
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyC}})
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.GetRequest{}, ba.Requests[1].GetInner())

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
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.GetRequest{}, ba.Requests[1].GetInner())

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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner(nil /* iter */)

	txn := makeTxnProto()
	keyA, keyD := roachpb.Key("a"), roachpb.Key("d")

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})
	ba.Add(&kvpb.DeleteRangeRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyD}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.DeleteRangeRequest{}, ba.Requests[1].GetInner())

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

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 5)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[3].GetInner())
		require.IsType(t, &kvpb.DeleteRangeRequest{}, ba.Requests[4].GetInner())

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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner(nil /* iter */)

	txn := makeTxnProto()
	keyA, keyC := roachpb.Key("a"), roachpb.Key("c")

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyC}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())

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
	ba.Add(&kvpb.SubsumeRequest{
		RequestHeader: kvpb.RequestHeader{Key: keyRangeDesc},
	})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.SubsumeRequest{}, ba.Requests[2].GetInner())

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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner(nil /* iter */)

	// Disable write_pipelining_max_outstanding_size and max_intents_bytes limits.
	pipelinedWritesMaxBatchSize.Override(ctx, &tp.st.SV, 0)
	TrackedWritesMaxSize.Override(ctx, &tp.st.SV, math.MaxInt64)

	const writes = 2048
	keyBuf := roachpb.Key(strings.Repeat("a", writes+1))
	makeKey := func(i int) roachpb.Key { return keyBuf[:i+1] }
	makeSeq := func(i int) enginepb.TxnSeq { return enginepb.TxnSeq(i) + 1 }

	txn := makeTxnProto()
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	for i := 0; i < writes; i++ {
		putArgs := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: makeKey(i)}}
		putArgs.Sequence = makeSeq(i)
		ba.Add(&putArgs)
	}

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, writes)
		require.True(t, ba.AsyncConsensus)
		for i := 0; i < writes; i++ {
			require.IsType(t, &kvpb.PutRequest{}, ba.Requests[i].GetInner())
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
			ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: key}})
		}
	}

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, writes)
		require.False(t, ba.AsyncConsensus)
		for i := 0; i < writes; i++ {
			if i%2 == 0 {
				key := makeKey(i)
				require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[i].GetInner())
				require.IsType(t, &kvpb.GetRequest{}, ba.Requests[i+1].GetInner())

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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner(nil /* iter */)

	txn := makeTxnProto()
	keyA := roachpb.Key("a")

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putArgs := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}}
	putArgs.Sequence = 1
	ba.Add(&putArgs)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

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
	etArgs := kvpb.EndTxnRequest{Commit: false}
	etArgs.Sequence = 2
	ba.Add(&etArgs)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

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
	etArgs = kvpb.EndTxnRequest{Commit: false}
	etArgs.Sequence = 2
	ba.Add(&etArgs)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

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
	defer log.Scope(t).Close(t)
	tp, _ := makeMockTxnPipeliner(nil /* iter */)

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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner(nil /* iter */)

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})
	ba.Add(&kvpb.DeleteRangeRequest{RequestHeader: kvpb.RequestHeader{Key: keyB, EndKey: keyD}})
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyD}})

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
			mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 7)
				require.False(t, ba.AsyncConsensus)
				require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())
				require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[2].GetInner())
				require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[3].GetInner())
				require.IsType(t, &kvpb.DeleteRangeRequest{}, ba.Requests[4].GetInner())
				require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[5].GetInner())
				require.IsType(t, &kvpb.PutRequest{}, ba.Requests[6].GetInner())

				err := kvpb.NewIntentMissingError(nil /* key */, nil /* intent */)
				pErr := kvpb.NewErrorWithTxn(err, &txn)
				pErr.SetErrorIndex(errIdx)
				return nil, pErr
			})

			br, pErr := tp.SendLocked(ctx, ba)
			require.Nil(t, br)
			require.NotNil(t, pErr)
			require.Equal(t, &txn, pErr.GetTxn())
			require.Equal(t, resErrIdx, pErr.Index.Index)
			require.IsType(t, &kvpb.TransactionRetryError{}, pErr.GetDetail())
			require.Equal(t, kvpb.RETRY_ASYNC_WRITE_FAILURE, pErr.GetDetail().(*kvpb.TransactionRetryError).Reason)
		})
	}
}

// TestTxnPipelinerEnableDisableMixTxn tests that the txnPipeliner behaves
// correctly if pipelining is enabled or disabled midway through a transaction.
func TestTxnPipelinerEnableDisableMixTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner(nil /* iter */)

	// Start with pipelining disabled. Should NOT use async consensus.
	PipelinedWritesEnabled.Override(ctx, &tp.st.SV, false)

	txn := makeTxnProto()
	keyA, keyC := roachpb.Key("a"), roachpb.Key("c")

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putArgs := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}}
	putArgs.Sequence = 1
	ba.Add(&putArgs)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, 0, tp.ifWrites.len())

	// Enable pipelining. Should use async consensus.
	PipelinedWritesEnabled.Override(ctx, &tp.st.SV, true)

	ba.Requests = nil
	putArgs2 := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}}
	putArgs2.Sequence = 2
	ba.Add(&putArgs2)
	putArgs3 := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyC}}
	putArgs3.Sequence = 3
	ba.Add(&putArgs3)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())

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
	PipelinedWritesEnabled.Override(ctx, &tp.st.SV, false)

	ba.Requests = nil
	putArgs4 := kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}}
	putArgs4.Sequence = 4
	ba.Add(&putArgs4)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())

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
	etArgs := kvpb.EndTxnRequest{Commit: true}
	etArgs.Sequence = 5
	ba.Add(&etArgs)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[1].GetInner())

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

// TestTxnPipelinerMaxInFlightSize tests that batches are not pipelined if doing
// so would push the memory used to track locks and in-flight writes over the
// limit allowed by the kv.transaction.max_intents_bytes setting.
func TestTxnPipelinerMaxInFlightSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	rangeIter := newDescriptorDBRangeIterator(mockRangeDescriptorDBForDescs(
		roachpb.RangeDescriptor{
			RangeID:  1,
			StartKey: roachpb.RKey("a"),
			EndKey:   roachpb.RKey("z"),
		},
	))
	tp, mockSender := makeMockTxnPipeliner(rangeIter)

	// Set budget limit to 3 bytes.
	TrackedWritesMaxSize.Override(ctx, &tp.st.SV, 3)

	txn := makeTxnProto()
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

	// Send a batch that would exceed the limit.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}})
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyC}})
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyD}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
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
	require.Equal(t, tp.lockFootprint.asSlice(), []roachpb.Span{{Key: keyA, EndKey: keyD.Next()}})

	// Send a batch that is equal to the limit.
	tp.lockFootprint.clear() // Hackily forget about the past.
	ba.Requests = nil
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}})
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyC}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
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
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyD}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
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
	tp.lockFootprint.clear() // hackily disregard the locks
	ba.Requests = nil
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 4)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.GetRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &kvpb.GetRequest{}, ba.Requests[3].GetInner())

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
	tp.lockFootprint.clear() // hackily disregard the locks
	ba.Requests = nil
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}})
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyC}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[2].GetInner())

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
	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 4)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[3].GetInner())

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

	// Increase the budget limit to 5 bytes.
	TrackedWritesMaxSize.Override(ctx, &tp.st.SV, 5)
	tp.lockFootprint.clear() // hackily disregard the locks

	// The original batch with 4 writes should succeed.
	ba.Requests = nil
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}})
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyC}})
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyD}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
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
// than allowed by the kv.transaction.write_pipelining.max_batch_size setting
// will not be pipelined.
func TestTxnPipelinerMaxBatchSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner(nil /* iter */)

	// Set maxBatchSize limit to 1.
	pipelinedWritesMaxBatchSize.Override(ctx, &tp.st.SV, 1)

	txn := makeTxnProto()
	keyA, keyC := roachpb.Key("a"), roachpb.Key("c")

	// Batch below limit.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

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
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyC}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[2].GetInner())

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
	pipelinedWritesMaxBatchSize.Override(ctx, &tp.st.SV, 2)

	// Same batch now below limit.
	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.True(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())

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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner(nil /* iter */)

	txn := makeTxnProto()
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
	keyD, keyE, keyF := roachpb.Key("d"), roachpb.Key("e"), roachpb.Key("f")

	// Return an error for a point write, a range write, and a range locking
	// read.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})
	ba.Add(&kvpb.DeleteRangeRequest{RequestHeader: kvpb.RequestHeader{Key: keyB, EndKey: keyB.Next()}})
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyC, EndKey: keyC.Next()}, KeyLockingStrength: lock.Exclusive})

	mockPErr := kvpb.NewErrorf("boom")
	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.DeleteRangeRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[2].GetInner())

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
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyD}})
	ba.Add(&kvpb.DeleteRangeRequest{RequestHeader: kvpb.RequestHeader{Key: keyE, EndKey: keyE.Next()}})
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyF, EndKey: keyF.Next()}, KeyLockingStrength: lock.Exclusive})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.DeleteRangeRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[2].GetInner())

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
	ba.Add(&kvpb.EndTxnRequest{Commit: false})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

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

// TestTxnPipelinerIgnoresLocksOnUnambiguousFailure tests that when a request
// returns with an unambiguous error, the locks that it attempted to acquire
// from the specific request that hit the error (but not any other in the batch)
// are NOT added to the lock footprint.
func TestTxnPipelinerIgnoresLocksOnUnambiguousFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner(nil /* iter */)

	txn := makeTxnProto()
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
	keyD, keyE, keyF := roachpb.Key("d"), roachpb.Key("e"), roachpb.Key("f")

	// Return a ConditionalFailed error for a CPut. The lock spans correspond to
	// the CPut are not added to the lock footprint, but the lock spans for all
	// other requests in the batch are.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.ConditionalPutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})
	ba.Add(&kvpb.DeleteRangeRequest{RequestHeader: kvpb.RequestHeader{Key: keyB, EndKey: keyB.Next()}})
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyC, EndKey: keyC.Next()}, KeyLockingStrength: lock.Exclusive})

	condFailedErr := kvpb.NewError(&kvpb.ConditionFailedError{})
	condFailedErr.SetErrorIndex(0)
	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.ConditionalPutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.DeleteRangeRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[2].GetInner())

		return nil, condFailedErr
	})

	br, pErr := tp.SendLocked(ctx, ba)
	require.Nil(t, br)
	require.Equal(t, condFailedErr, pErr)
	require.Equal(t, 0, tp.ifWrites.len())

	var expLocks []roachpb.Span
	expLocks = append(expLocks, roachpb.Span{Key: keyB, EndKey: keyB.Next()})
	expLocks = append(expLocks, roachpb.Span{Key: keyC, EndKey: keyC.Next()})
	require.Equal(t, expLocks, tp.lockFootprint.asSlice())

	// Return a WriteIntentError for a Scan. The lock spans correspond to the
	// Scan are not added to the lock footprint, but the lock spans for all
	// other requests in the batch are.
	ba.Requests = nil
	ba.Add(&kvpb.ConditionalPutRequest{RequestHeader: kvpb.RequestHeader{Key: keyD}})
	ba.Add(&kvpb.DeleteRangeRequest{RequestHeader: kvpb.RequestHeader{Key: keyE, EndKey: keyE.Next()}})
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyF, EndKey: keyF.Next()}, KeyLockingStrength: lock.Exclusive})

	writeIntentErr := kvpb.NewError(&kvpb.WriteIntentError{})
	writeIntentErr.SetErrorIndex(2)
	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.ConditionalPutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.DeleteRangeRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[2].GetInner())

		return nil, writeIntentErr
	})

	br, pErr = tp.SendLocked(ctx, ba)
	require.Nil(t, br)
	require.Equal(t, writeIntentErr, pErr)
	require.Equal(t, 0, tp.ifWrites.len())

	expLocks = append(expLocks, roachpb.Span{Key: keyD})
	expLocks = append(expLocks, roachpb.Span{Key: keyE, EndKey: keyE.Next()})
	require.Equal(t, expLocks, tp.lockFootprint.asSlice())
}

// Test that the pipeliners knows how to save and restore its state.
func TestTxnPipelinerSavepoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tp, mockSender := makeMockTxnPipeliner(nil /* iter */)

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
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("a")}})
	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.False(t, ba.AsyncConsensus)
		require.IsType(t, &kvpb.QueryIntentRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.GetRequest{}, ba.Requests[1].GetInner())

		qiReq := ba.Requests[0].GetInner().(*kvpb.QueryIntentRequest)
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

// TestTxnPipelinerCondenseLockSpans2 verifies that lock spans are condensed
// along range boundaries when they exceed the maximum intent bytes threshold.
func TestTxnPipelinerCondenseLockSpans2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	type span struct {
		start, end string
	}

	c30 := "cccccccccccccccccccccccccccccc"

	testCases := []struct {
		name string
		// Pre-existing lock spans and in-flight writes.
		lockSpans []span
		ifWrites  []string
		// The budget.
		maxBytes int64
		// The request that the test sends.
		req *kvpb.BatchRequest
		// The expected state after the request returns.
		expLockSpans []span
		expIfWrites  []string
	}{
		{
			// In this scenario, a request is sent when the pipeliner already had a
			// considerable size of inflight-writes. These cause the pipeliner to
			// exceed its budget and collapse the spans.
			//
			// The in-flight writes are by themselves larger than maxBytes, so the
			// lock span condensing is essentially told that it needs to compact the
			// locks completely.
			name:      "pre-existing large inflight-writes",
			lockSpans: []span{{"a1", "a2"}, {"a3", "a4"}, {"b1", "b2"}, {"b3", "b4"}},
			ifWrites:  []string{c30},
			maxBytes:  20,
			req:       putBatchNoAsyncConsensus(roachpb.Key("b"), nil),
			// We expect the locks to be condensed as aggressively as possible, which
			// means that they're completely condensed at the level of each range.
			// Note that the "b" key from the request is included.
			expLockSpans: []span{{"a1", "a4"}, {"b", "b4"}},
			expIfWrites:  []string{c30}, // The pre-existing key.
		},
		{
			// Like the above, except the large in-flight writes come from the test's
			// request. The request will not be allowed to perform async consensus.
			// Because it runs without async consensus, the request's key will be
			// added to the lock spans on response.
			name:         "new large inflight-writes",
			lockSpans:    []span{{"a1", "a2"}, {"a3", "a4"}, {"b1", "b2"}, {"b3", "b4"}},
			maxBytes:     20,
			req:          putBatch(roachpb.Key(c30), nil),
			expLockSpans: []span{{"a1", "a4"}, {"b1", "b4"}, {c30, ""}},
			expIfWrites:  nil, // The request was not allowed to perform async consensus.
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rangeIter := newDescriptorDBRangeIterator(mockRangeDescriptorDBForDescs(
				roachpb.RangeDescriptor{
					RangeID:  1,
					StartKey: roachpb.RKey("a"),
					EndKey:   roachpb.RKey("b"),
				},
				roachpb.RangeDescriptor{
					RangeID:  2,
					StartKey: roachpb.RKey("b"),
					EndKey:   roachpb.RKey("c"),
				},
				roachpb.RangeDescriptor{
					RangeID:  3,
					StartKey: roachpb.RKey("c"),
					EndKey:   roachpb.RKey("d"),
				}))
			tp, mockSender := makeMockTxnPipeliner(rangeIter)
			TrackedWritesMaxSize.Override(ctx, &tp.st.SV, tc.maxBytes)

			for _, sp := range tc.lockSpans {
				tp.lockFootprint.insert(roachpb.Span{Key: roachpb.Key(sp.start), EndKey: roachpb.Key(sp.end)})
			}

			for _, k := range tc.ifWrites {
				tp.ifWrites.insert(roachpb.Key(k), 1)
			}

			txn := makeTxnProto()
			mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			})

			tc.req.Header = kvpb.Header{Txn: &txn}
			_, pErr := tp.SendLocked(ctx, tc.req)
			require.Nil(t, pErr)

			expLockSpans := make([]roachpb.Span, len(tc.expLockSpans))
			for i, sp := range tc.expLockSpans {
				var endKey roachpb.Key
				if sp.end != "" {
					endKey = roachpb.Key(sp.end)
				}
				expLockSpans[i] = roachpb.Span{Key: roachpb.Key(sp.start), EndKey: endKey}
			}
			require.Equal(t, expLockSpans, tp.lockFootprint.asSortedSlice())

			expIfWrites := make([]roachpb.Key, len(tc.expIfWrites))
			for i, k := range tc.expIfWrites {
				expIfWrites[i] = roachpb.Key(k)
			}
			ifWrites := tp.ifWrites.asSlice()
			ifWriteKeys := make([]roachpb.Key, len(ifWrites))
			for i, k := range ifWrites {
				ifWriteKeys[i] = k.Key
			}
			require.Equal(t, expIfWrites, ifWriteKeys)
		})
	}
}

func putBatch(key roachpb.Key, value []byte) *kvpb.BatchRequest {
	ba := &kvpb.BatchRequest{}
	ba.Add(&kvpb.PutRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes(value),
	})
	return ba
}

// putBatchNoAsyncConsesnsus returns a PutRequest addressed to the default
// replica for the specified key / value. The batch also contains a Get, which
// inhibits the asyncConsensus flag.
func putBatchNoAsyncConsensus(key roachpb.Key, value []byte) *kvpb.BatchRequest {
	ba := putBatch(key, value)
	ba.Add(&kvpb.GetRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
	})
	return ba
}

type descriptorDBRangeIterator struct {
	db      MockRangeDescriptorDB
	curDesc roachpb.RangeDescriptor
}

var _ condensableSpanSetRangeIterator = &descriptorDBRangeIterator{}

func newDescriptorDBRangeIterator(db MockRangeDescriptorDB) *descriptorDBRangeIterator {
	return &descriptorDBRangeIterator{db: db}
}

func (s descriptorDBRangeIterator) Valid() bool {
	return true
}

func (s *descriptorDBRangeIterator) Seek(ctx context.Context, key roachpb.RKey, dir ScanDirection) {
	descs, _, err := s.db.RangeLookup(ctx, key, kvpb.READ_UNCOMMITTED, dir == Descending)
	if err != nil {
		panic(err)
	}
	if len(descs) > 1 {
		panic(fmt.Sprintf("unexpected multiple descriptors for key %s: %s", key, descs))
	}
	s.curDesc = descs[0]
}

func (s descriptorDBRangeIterator) Error() error {
	return nil
}

// Test that the pipeliner rejects requests when the lock span budget is
// exceeded, if configured to do so.
func TestTxnPipelinerRejectAboveBudget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	largeAs := make([]byte, 11)
	for i := 0; i < len(largeAs); i++ {
		largeAs[i] = 'a'
	}
	largeWrite := putBatch(largeAs, nil)
	mediumWrite := putBatch(largeAs[:5], nil)

	delRange := &kvpb.BatchRequest{}
	delRange.Header.MaxSpanRequestKeys = 1
	delRange.Add(&kvpb.DeleteRangeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("b"),
		},
	})
	delRangeResp := delRange.CreateReply()
	delRangeResp.Responses[0].GetInner().(*kvpb.DeleteRangeResponse).ResumeSpan = &roachpb.Span{
		Key:    largeAs,
		EndKey: roachpb.Key("b"),
	}

	testCases := []struct {
		name string
		// The requests to be sent one by one.
		reqs []*kvpb.BatchRequest
		// The responses for reqs. If an entry is nil, a response is automatically
		// generated for it. Requests past the end of the resp array are also
		// generated automatically.
		resp []*kvpb.BatchResponse
		// The 0-based index of the request that's expected to be rejected. -1 if no
		// request is expected to be rejected.
		expRejectIdx int
		maxSize      int64
	}{
		{name: "large request",
			reqs:         []*kvpb.BatchRequest{largeWrite},
			expRejectIdx: 0,
			maxSize:      int64(len(largeAs)) - 1 + roachpb.SpanOverhead,
		},
		{name: "requests that add up",
			reqs: []*kvpb.BatchRequest{
				putBatchNoAsyncConsensus(roachpb.Key("aaaa"), nil),
				putBatchNoAsyncConsensus(roachpb.Key("bbbb"), nil),
				putBatchNoAsyncConsensus(roachpb.Key("cccc"), nil)},
			expRejectIdx: 2,
			// maxSize is such that first two requests fit and the third one
			// goes above the limit.
			maxSize: 9 + 2*roachpb.SpanOverhead,
		},
		{name: "async requests that add up",
			// Like the previous test, but this time the requests run with async
			// consensus. Being tracked as in-flight writes, this test shows that
			// in-flight writes count towards the budget.
			reqs: []*kvpb.BatchRequest{
				putBatch(roachpb.Key("aaaa"), nil),
				putBatch(roachpb.Key("bbbb"), nil),
				putBatch(roachpb.Key("cccc"), nil)},
			expRejectIdx: 2,
			maxSize:      10 + roachpb.SpanOverhead,
		},
		{
			name: "response goes over budget, next request rejected",
			// A request returns a response with a large resume span, which takes up
			// the budget. Then the next request will be rejected.
			reqs:         []*kvpb.BatchRequest{delRange, putBatch(roachpb.Key("a"), nil)},
			resp:         []*kvpb.BatchResponse{delRangeResp},
			expRejectIdx: 1,
			maxSize:      10 + roachpb.SpanOverhead,
		},
		{
			name: "response goes over budget",
			// Like the previous test, except here we don't have a followup request
			// once we're above budget. The test runner will commit the txn, and this
			// test checks that committing is allowed.
			reqs:         []*kvpb.BatchRequest{delRange},
			resp:         []*kvpb.BatchResponse{delRangeResp},
			expRejectIdx: -1,
			maxSize:      10 + roachpb.SpanOverhead,
		},
		{
			// Request keys overlap, so they don't count twice.
			name:         "overlapping requests",
			reqs:         []*kvpb.BatchRequest{mediumWrite, mediumWrite, mediumWrite},
			expRejectIdx: -1,
			// Our estimation logic for rejecting requests based on size
			// consults both the in-flight write set (which doesn't account for
			// the span overhead) as well as the lock footprint (which accounts
			// for the span overhead).
			maxSize: 16 + roachpb.SpanOverhead,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expRejectIdx >= len(tc.reqs) {
				t.Fatalf("invalid test")
			}

			tp, mockSender := makeMockTxnPipeliner(nil /* iter */)
			TrackedWritesMaxSize.Override(ctx, &tp.st.SV, tc.maxSize)
			rejectTxnOverTrackedWritesBudget.Override(ctx, &tp.st.SV, true)

			txn := makeTxnProto()

			var respIdx int
			mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				// Handle rollbacks and commits separately.
				if ba.IsSingleAbortTxnRequest() || ba.IsSingleCommitRequest() {
					br := ba.CreateReply()
					br.Txn = ba.Txn
					return br, nil
				}

				var resp *kvpb.BatchResponse
				if respIdx < len(tc.resp) {
					resp = tc.resp[respIdx]
				}
				respIdx++

				if resp != nil {
					resp.Txn = ba.Txn
					return resp, nil
				}
				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			})

			for i, ba := range tc.reqs {
				ba.Header = kvpb.Header{Txn: &txn}
				_, pErr := tp.SendLocked(ctx, ba)
				if i == tc.expRejectIdx {
					require.NotNil(t, pErr, "expected rejection, but request succeeded")

					budgetErr := (lockSpansOverBudgetError{})
					if !errors.As(pErr.GoError(), &budgetErr) {
						t.Fatalf("expected lockSpansOverBudgetError, got %+v", pErr.GoError())
					}
					require.Equal(t, pgcode.ConfigurationLimitExceeded, pgerror.GetPGCode(pErr.GoError()))
					require.Equal(t, int64(1), tp.txnMetrics.TxnsRejectedByLockSpanBudget.Count())

					// Make sure rolling back the txn works.
					rollback := &kvpb.BatchRequest{}
					rollback.Add(&kvpb.EndTxnRequest{Commit: false})
					rollback.Txn = &txn
					_, pErr = tp.SendLocked(ctx, rollback)
					require.Nil(t, pErr)
				} else {
					require.Nil(t, pErr)

					// Make sure that committing works. This is particularly relevant for
					// testcases where we ended up over budget but we didn't return an
					// error (because we failed to pre-emptively detect that we're going
					// to be over budget and the response surprised us with a large
					// ResumeSpan). Committing in these situations is allowed, since the
					// harm has already been done.
					commit := &kvpb.BatchRequest{}
					commit.Add(&kvpb.EndTxnRequest{Commit: true})
					commit.Txn = &txn
					_, pErr = tp.SendLocked(ctx, commit)
					require.Nil(t, pErr)
				}
			}
		})
	}
}

func (s descriptorDBRangeIterator) Desc() *roachpb.RangeDescriptor {
	return &s.curDesc
}
