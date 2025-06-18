// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func makeMockTxnWriteBuffer(st *cluster.Settings) (txnWriteBuffer, *mockLockedSender) {
	metrics := MakeTxnMetrics(time.Hour)
	mockSender := &mockLockedSender{}
	return txnWriteBuffer{
		enabled:    true,
		wrapped:    mockSender,
		txnMetrics: &metrics,
		st:         st,
	}, mockSender
}

func getArgs(key roachpb.Key) *kvpb.GetRequest {
	return &kvpb.GetRequest{
		RequestHeader: kvpb.RequestHeader{Key: key},
	}
}

func putArgs(key roachpb.Key, value string, seq enginepb.TxnSeq) *kvpb.PutRequest {
	return &kvpb.PutRequest{
		RequestHeader: kvpb.RequestHeader{Key: key, Sequence: seq},
		Value:         roachpb.MakeValueFromString(value),
	}
}

func cputArgs(
	key roachpb.Key, value string, expValue string, seq enginepb.TxnSeq,
) *kvpb.ConditionalPutRequest {
	return &kvpb.ConditionalPutRequest{
		RequestHeader: kvpb.RequestHeader{Key: key, Sequence: seq},
		Value:         roachpb.MakeValueFromString(value),
		ExpBytes:      []byte(expValue),
	}
}

func delArgs(key roachpb.Key, seq enginepb.TxnSeq) *kvpb.DeleteRequest {
	return &kvpb.DeleteRequest{
		RequestHeader: kvpb.RequestHeader{Key: key, Sequence: seq},
	}
}

func delRangeArgs(key, endKey roachpb.Key, seq enginepb.TxnSeq) *kvpb.DeleteRangeRequest {
	return &kvpb.DeleteRangeRequest{
		RequestHeader: kvpb.RequestHeader{Key: key, EndKey: endKey, Sequence: seq},
	}
}

func makeBufferedWrite(key roachpb.Key, vals ...bufferedValue) bufferedWrite {
	return bufferedWrite{key: key, vals: vals}
}

func makeBufferedValue(value string, seq enginepb.TxnSeq) bufferedValue {
	val := roachpb.MakeValueFromString(value)
	if value == "" {
		// Special handling to denote Deletes.
		val = roachpb.Value{}
	}
	return bufferedValue{val: val, seq: seq}
}

// TestTxnWriteBufferBuffersBlindWrites tests that the txnWriteBuffer correctly
// buffers blind writes (Put/Del requests). Other than the basic case, it tests
// that multiple writes to the same key are stored correctly.
func TestTxnWriteBufferBuffersBlindWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	txn := makeTxnProto()
	txn.Sequence = 1
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")

	// Blindly write to some keys.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putA := putArgs(keyA, "valA", txn.Sequence)
	putB := putArgs(keyB, "valB", txn.Sequence)
	delC := delArgs(keyC, txn.Sequence)
	ba.Add(putA, putB, delC)

	numCalled := mockSender.NumCalled()
	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	// All the requests should be buffered and not make it past the
	// txnWriteBuffer.
	require.Equal(t, numCalled, mockSender.NumCalled())
	// Even though the txnWriteBuffer did not send any Put requests to the KV
	// layer above, the responses should still be populated.
	require.Len(t, br.Responses, 3)
	require.Equal(t, br.Responses[0].GetInner(), &kvpb.PutResponse{})
	require.Equal(t, br.Responses[1].GetInner(), &kvpb.PutResponse{})
	require.Equal(t, br.Responses[2].GetInner(), &kvpb.DeleteResponse{})
	require.Equal(t, int64(1), twb.txnMetrics.TxnWriteBufferFullyHandledBatches.Count())

	// Verify the writes were buffered correctly.
	expBufferedWrites := []bufferedWrite{
		makeBufferedWrite(keyA, makeBufferedValue("valA", 1)),
		makeBufferedWrite(keyB, makeBufferedValue("valB", 1)),
		makeBufferedWrite(keyC, makeBufferedValue("", 1)),
	}
	require.Equal(t, expBufferedWrites, twb.testingBufferedWritesAsSlice())

	// Commit the transaction and ensure that the buffer is correctly flushed.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.EndTxnRequest{Commit: true})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 4)

		// We now expect the buffer to be flushed along with the commit.
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.DeleteRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[3].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Even though we flushed the buffer, responses from the blind writes should
	// not be returned.
	require.Len(t, br.Responses, 1)
	require.IsType(t, &kvpb.EndTxnResponse{}, br.Responses[0].GetInner())
}

// TestTxnWriteBufferWritesToSameKey ensures that writes to the same key are
// all buffered as expected, but at commit time, only the final write (the one
// with the highest sequence number) is flushed to KV.
func TestTxnWriteBufferWritesToSameKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	txn := makeTxnProto()
	txn.Sequence = 1
	keyA := roachpb.Key("a")

	// Perform blind writes to keyA multiple times.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putA := putArgs(keyA, "val1", txn.Sequence)
	ba.Add(putA)

	numCalledBefore := mockSender.NumCalled()
	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	// The request shouldn't make it past the txnWriteBuffer as there's only one
	// Put, which should be buffered.
	require.Equal(t, numCalledBefore, mockSender.NumCalled())

	// Even though the txnWriteBuffer did not send any Put requests to the KV
	// layer above, the responses should still be populated.
	require.Len(t, br.Responses, 1)
	require.Equal(t, br.Responses[0].GetInner(), &kvpb.PutResponse{})

	// Verify the write was buffered correctly.
	expBufferedWrites := []bufferedWrite{
		makeBufferedWrite(keyA, makeBufferedValue("val1", 1)),
	}
	require.Equal(t, expBufferedWrites, twb.testingBufferedWritesAsSlice())

	// Write to keyA again at a higher sequence number.
	txn.Sequence++
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putA = putArgs(keyA, "val2", txn.Sequence)
	ba.Add(putA)

	numCalledBefore = mockSender.NumCalled()
	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	// The Put request shouldn't make it past the txnWriteBuffer.
	require.Equal(t, numCalledBefore, mockSender.NumCalled())

	// Again, there should be a response, even though we didn't send a KV request.
	require.Len(t, br.Responses, 1)
	require.Equal(t, br.Responses[0].GetInner(), &kvpb.PutResponse{})

	// Verify the buffer is correctly updated.
	expBufferedWrites = []bufferedWrite{
		makeBufferedWrite(keyA,
			makeBufferedValue("val1", 1), makeBufferedValue("val2", 2),
		),
	}
	require.Equal(t, expBufferedWrites, twb.testingBufferedWritesAsSlice())

	// Commit the transaction and ensure that only the write at sequence number 2
	// is flushed.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.EndTxnRequest{Commit: true})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)

		// We now expect the buffer to be flushed along with the commit. Instead of
		// both the writes to keyA, only the final one should be sent to the KV
		// layer.
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.Equal(t, enginepb.TxnSeq(2), ba.Requests[0].GetInner().(*kvpb.PutRequest).Sequence)
		require.Equal(t, roachpb.MakeValueFromString("val2"), ba.Requests[0].GetInner().(*kvpb.PutRequest).Value)

		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Even though we flushed the buffer, responses from the blind writes should
	// not be returned.
	require.Len(t, br.Responses, 1)
	require.IsType(t, &kvpb.EndTxnResponse{}, br.Responses[0].GetInner())
}

// TestTxnWriteBufferBlindWritesIncludingOtherRequests tests that the
// txnWriteBuffer behaves correctly when a batch request contains both blind
// writes and other requests that will not be transformed. In the future, we may
// want to extend this test to include read-write requests as well.
func TestTxnWriteBufferBlindWritesIncludingOtherRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	txn := makeTxnProto()
	txn.Sequence = 1
	keyA, keyB, keyC, keyD, keyE := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c"),
		roachpb.Key("d"), roachpb.Key("e")

	// Construct a batch that includes both blind writes and other requests.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putA := putArgs(keyA, "val1", txn.Sequence)
	getB := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}}
	delC := delArgs(keyC, txn.Sequence)
	scanDE := &kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyD, EndKey: keyE}}
	queryLocks := &kvpb.QueryLocksRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyE}, IncludeUncontended: true}
	leaseInfo := &kvpb.LeaseInfoRequest{}
	ba.Add(putA)
	ba.Add(getB)
	ba.Add(delC)
	ba.Add(scanDE)
	ba.Add(queryLocks)
	ba.Add(leaseInfo)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 4)
		require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.QueryLocksRequest{}, ba.Requests[2].GetInner())
		require.IsType(t, &kvpb.LeaseInfoRequest{}, ba.Requests[3].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Expect 6 responses, even though only 4 KV requests were sent. Moreover,
	// ensure that the responses are in the correct order and non-nil.
	require.Len(t, br.Responses, 6)
	require.IsType(t, &kvpb.PutResponse{}, br.Responses[0].GetInner())
	require.IsType(t, &kvpb.GetResponse{}, br.Responses[1].GetInner())
	require.IsType(t, &kvpb.DeleteResponse{}, br.Responses[2].GetInner())
	require.IsType(t, &kvpb.ScanResponse{}, br.Responses[3].GetInner())
	require.IsType(t, &kvpb.QueryLocksResponse{}, br.Responses[4].GetInner())
	require.IsType(t, &kvpb.LeaseInfoResponse{}, br.Responses[5].GetInner())

	// Verify the writes were buffered correctly.
	expBufferedWrites := []bufferedWrite{
		makeBufferedWrite(keyA, makeBufferedValue("val1", 1)),
		makeBufferedWrite(keyC, makeBufferedValue("", 1)),
	}
	require.Equal(t, expBufferedWrites, twb.testingBufferedWritesAsSlice())

	// Commit the transaction and ensure that the buffer is correctly flushed.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.EndTxnRequest{Commit: true})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)

		// We now expect the buffer to be flushed along with the commit.
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.DeleteRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[2].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
}

// TestTxnWriteBufferCorrectlyAdjustsFlushErrors ensures that the txnWriteBuffer
// correctly adjusts the index of the error returned upon flushing the buffer.
// In particular, if the error belongs to a flushed write, the index is nil-ed
// out. Otherwise, the index is adjusted to hide the flushed writes.
func TestTxnWriteBufferCorrectlyAdjustsFlushErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for errIdx, resErrIdx := range map[int32]int32{
		0: -1, // points to the Put; -1 to denote nil
		1: -1, // points to the Del; -1 to denote nil
		2: 0,  // points to the Get
		3: 1,  // points to the EndTxn
	} {
		t.Run(fmt.Sprintf("errIdx=%d", errIdx), func(t *testing.T) {
			ctx := context.Background()
			twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

			txn := makeTxnProto()
			txn.Sequence = 1
			keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")

			// Perform some blind writes.
			ba := &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: &txn}
			putA := putArgs(keyA, "val1", txn.Sequence)
			getB := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}}
			delC := delArgs(keyC, txn.Sequence)
			ba.Add(putA)
			ba.Add(getB)
			ba.Add(delC)

			mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			})

			br, pErr := twb.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)

			// Commit the transaction. This should flush the buffer as well.
			ba = &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: &txn}
			ba.Add(getB)
			ba.Add(&kvpb.EndTxnRequest{Commit: true})

			mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 4)
				require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &kvpb.DeleteRequest{}, ba.Requests[1].GetInner())
				require.IsType(t, &kvpb.GetRequest{}, ba.Requests[2].GetInner())
				require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[3].GetInner())

				pErr := kvpb.NewErrorWithTxn(errors.New("boom"), &txn)
				pErr.SetErrorIndex(errIdx)
				return nil, pErr
			})

			br, pErr = twb.SendLocked(ctx, ba)
			require.Nil(t, br)
			require.NotNil(t, pErr)
			require.Equal(t, &txn, pErr.GetTxn())

			if resErrIdx == -1 {
				require.Nil(t, pErr.Index)
			} else {
				require.NotNil(t, pErr.Index)
				require.Equal(t, resErrIdx, pErr.Index.Index)
			}
		})
	}
}

// TestTxnWriteBufferCorrectlyAdjustsErrorsAfterBuffering ensures that the
// txnWriteBuffer correctly adjusts the index of the errors returned when a part
// of the batch is buffered on the client.
func TestTxnWriteBufferCorrectlyAdjustsErrorsAfterBuffering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for errIdx, resErrIdx := range map[int32]*kvpb.ErrPosition{
		0: {Index: 1},  // points to the GetB
		1: {Index: 4},  // points to the ScanE
		2: {Index: 5},  // points to the RevScanF
		3: nil,         // points to the CPutG
		4: {Index: 7},  // points to the QueryLocks
		5: {Index: 8},  // points to the LeaseInfo
		6: nil,         // points to the CPutH
		7: {Index: 11}, // points to the GetJ
	} {
		t.Run(fmt.Sprintf("errIdx=%d", errIdx), func(t *testing.T) {
			ctx := context.Background()
			twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

			txn := makeTxnProto()
			txn.Sequence = 1
			keyA, keyB, keyC, keyD, keyE, keyF, keyG, keyH, keyI, keyJ :=
				roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c"), roachpb.Key("d"),
				roachpb.Key("e"), roachpb.Key("f"), roachpb.Key("g"), roachpb.Key("h"),
				roachpb.Key("i"), roachpb.Key("j")

			// Construct a batch request where some of the requests will be buffered.
			ba := &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: &txn}
			putA := putArgs(keyA, "val1", txn.Sequence)
			getB := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}}
			delC := delArgs(keyC, txn.Sequence)
			putD := putArgs(keyD, "val2", txn.Sequence)
			scanE := &kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyE}}
			revScanF := &kvpb.ReverseScanRequest{RequestHeader: kvpb.RequestHeader{Key: keyF}}
			cputG := &kvpb.ConditionalPutRequest{RequestHeader: kvpb.RequestHeader{Key: keyG}}
			queryLocks := &kvpb.QueryLocksRequest{}
			leaseInfo := &kvpb.LeaseInfoRequest{}
			cputH := &kvpb.ConditionalPutRequest{RequestHeader: kvpb.RequestHeader{Key: keyH}}
			putI := putArgs(keyI, "val3", txn.Sequence)
			getJ := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyJ}}

			ba.Add(putA)
			ba.Add(getB)
			ba.Add(delC)
			ba.Add(putD)
			ba.Add(scanE)
			ba.Add(revScanF)
			ba.Add(cputG)
			ba.Add(queryLocks)
			ba.Add(leaseInfo)
			ba.Add(cputH)
			ba.Add(putI)
			ba.Add(getJ)

			mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 8)
				require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[1].GetInner())
				require.IsType(t, &kvpb.ReverseScanRequest{}, ba.Requests[2].GetInner())
				require.IsType(t, &kvpb.GetRequest{}, ba.Requests[3].GetInner())
				require.IsType(t, &kvpb.QueryLocksRequest{}, ba.Requests[4].GetInner())
				require.IsType(t, &kvpb.LeaseInfoRequest{}, ba.Requests[5].GetInner())
				require.IsType(t, &kvpb.GetRequest{}, ba.Requests[6].GetInner())
				require.IsType(t, &kvpb.GetRequest{}, ba.Requests[7].GetInner())

				pErr := kvpb.NewErrorWithTxn(errors.New("boom"), &txn)
				pErr.SetErrorIndex(errIdx)
				return nil, pErr
			})

			br, pErr := twb.SendLocked(ctx, ba)
			require.Nil(t, br)
			require.NotNil(t, pErr)
			require.Equal(t, &txn, pErr.GetTxn())
			require.Equal(t, resErrIdx, pErr.Index)

			// The batch we sent encountered an error; nothing should have been
			// buffered.
			require.Empty(t, twb.testingBufferedWritesAsSlice())

			// Don't commit transactions that have encountered an error.
			ba = &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: &txn}
			ba.Add(&kvpb.EndTxnRequest{Commit: false})

			mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)

				br = ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			})

			br, pErr = twb.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)
		})
	}
}

// TestTxnWriteBufferServesPointReadsLocally ensures that point reads hoping to
// do read-your-own-writes are correctly served from the buffer.
func TestTxnWriteBufferServesPointReadsLocally(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	putAtSeq := func(key roachpb.Key, val string, seq enginepb.TxnSeq) {
		txn := makeTxnProto()
		txn.Sequence = seq
		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: &txn}
		put := putArgs(key, val, seq)
		ba.Add(put)

		numCalled := mockSender.NumCalled()
		br, pErr := twb.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
		require.Equal(t, numCalled, mockSender.NumCalled())
	}

	txn := makeTxnProto()
	txn.Sequence = 10
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")

	const valA10 = "valA10"
	const valA12 = "valA12"
	const valA14 = "valA14"

	// Blindly write to keys A and B. Leave C as is.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putA := putArgs(keyA, valA10, txn.Sequence)
	delB := delArgs(keyB, txn.Sequence)
	ba.Add(putA, delB)

	numCalled := mockSender.NumCalled()
	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	// All the requests should be buffered and not make it past the
	// txnWriteBuffer.
	require.Equal(t, numCalled, mockSender.NumCalled())
	// Even though the txnWriteBuffer did not send any Put requests to the KV
	// layer above, the responses should still be populated.
	require.Len(t, br.Responses, 2)
	require.Equal(t, br.Responses[0].GetInner(), &kvpb.PutResponse{})
	require.Equal(t, br.Responses[1].GetInner(), &kvpb.DeleteResponse{})

	// Verify the writes were buffered correctly.
	expBufferedWrites := []bufferedWrite{
		makeBufferedWrite(keyA, makeBufferedValue("valA10", 10)),
		makeBufferedWrite(keyB, makeBufferedValue("", 10)),
	}
	require.Equal(t, expBufferedWrites, twb.testingBufferedWritesAsSlice())

	// Add a few more blind writes to keyA at different sequence numbers.
	putAtSeq(keyA, valA12, 12)
	putAtSeq(keyA, valA14, 14)

	// Verify the buffer is what we expect.
	expBufferedWrites = []bufferedWrite{
		makeBufferedWrite(keyA,
			makeBufferedValue(valA10, 10),
			makeBufferedValue(valA12, 12),
			makeBufferedValue(valA14, 14),
		),
		makeBufferedWrite(keyB, makeBufferedValue("", 10)),
	}
	require.Equal(t, expBufferedWrites, twb.testingBufferedWritesAsSlice())

	// First up, perform reads on key A at various sequence numbers and ensure the
	// correct value is served from the buffer.
	for seq, expVal := range map[enginepb.TxnSeq]string{
		10: valA10, 11: valA10, 12: valA12, 13: valA12, 14: valA14, 15: valA14,
	} {
		ba = &kvpb.BatchRequest{}
		txn.Sequence = seq
		ba.Header = kvpb.Header{Txn: &txn}
		getA := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, Sequence: txn.Sequence}}
		ba.Add(getA)

		numCalled = mockSender.NumCalled()
		br, pErr = twb.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
		require.Len(t, br.Responses, 1)
		require.Equal(t, roachpb.MakeValueFromString(expVal), *br.Responses[0].GetInner().(*kvpb.GetResponse).Value)
		// Should be served entirely from the buffer, and nothing should be sent to
		// the KV layer.
		require.Equal(t, numCalled, mockSender.NumCalled())
	}

	// Next, perform a read on keyB using a sequence number that should see the
	// delete. Ensure it's served from the buffer.
	txn.Sequence = 10
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	getB := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyB, Sequence: txn.Sequence}}
	ba.Add(getB)

	numCalled = mockSender.NumCalled()
	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Len(t, br.Responses, 1)
	require.False(t, br.Responses[0].GetInner().(*kvpb.GetResponse).Value.IsPresent())
	// Should be served entirely from the buffer, and nothing should be sent to
	// the KV layer.
	require.Equal(t, numCalled, mockSender.NumCalled())

	// Perform a read on keyC. This should be sent to the KV layer, as no write
	// for this key has been buffered.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	getC := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyC}}
	ba.Add(getC)

	numCalled = mockSender.NumCalled()
	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Len(t, br.Responses, 1)
	// Sanity check that the request did go to the KV layer.
	require.Equal(t, mockSender.NumCalled(), numCalled+1)

	// Finally, perform a read on keyA and keyB at a lower sequence number than
	// the minimum buffered write. This should be served from the KV layer.
	txn.Sequence = 9
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	getA := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, Sequence: txn.Sequence}}
	getB = &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyB, Sequence: txn.Sequence}}
	ba.Add(getA)
	ba.Add(getB)

	numCalled = mockSender.NumCalled()
	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.GetRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})
	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Len(t, br.Responses, 2)
	// Assert that the request was sent to the KV layer.
	require.Equal(t, mockSender.NumCalled(), numCalled+1)

	// Lastly, for completeness, commit the transaction and ensure that the buffer
	// is correctly flushed.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.EndTxnRequest{Commit: true})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)

		// We now expect the buffer to be flushed along with the commit.
		require.IsType(t, &kvpb.DeleteRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[2].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Even though we flushed the buffer, responses from the blind writes should
	// not be returned.
	require.Len(t, br.Responses, 1)
	require.IsType(t, &kvpb.EndTxnResponse{}, br.Responses[0].GetInner())
}

// TestTxnWriteBufferServesPointReadsAfterScan is a regression test
// for a bug in which reused iterator state resulted in the end key of
// a scan affecting subsequent GetRequests.
func TestTxnWriteBufferServesPointReadsAfterScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	txn := makeTxnProto()
	txn.Sequence = 10
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(putArgs(keyA, "valA", txn.Sequence))
	ba.Add(putArgs(keyB, "valB", txn.Sequence))
	ba.Add(putArgs(keyC, "valC", txn.Sequence))
	numCalled := mockSender.NumCalled()
	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	// All writes should be buffered.
	require.Equal(t, numCalled, mockSender.NumCalled())

	// First, read [a, c) via ScanRequest.
	txn.Sequence = 10
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.ScanRequest{
		RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyC, Sequence: txn.Sequence},
	})
	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[0].GetInner())
		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})
	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Len(t, br.Responses, 1)
	require.Equal(t, int64(2), br.Responses[0].GetScan().NumKeys)

	// Perform a read on keyC.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	getC := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyC, Sequence: txn.Sequence}}
	ba.Add(getC)

	numCalled = mockSender.NumCalled()
	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Len(t, br.Responses, 1)
	require.True(t, br.Responses[0].GetGet().Value.IsPresent())
	require.Equal(t, mockSender.NumCalled(), numCalled)
}

// TestTxnWriteBufferServesOverlappingReadsCorrectly ensures that Scan and
// ReverseScan requests that overlap with buffered writes are correctly served
// from the buffer.
func TestTxnWriteBufferServesOverlappingReadsCorrectly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	putAtSeq := func(key roachpb.Key, val string, seq enginepb.TxnSeq) {
		txn := makeTxnProto()
		txn.Sequence = seq
		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: &txn}
		put := putArgs(key, val, seq)
		ba.Add(put)

		numCalled := mockSender.NumCalled()
		br, pErr := twb.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
		require.Equal(t, numCalled, mockSender.NumCalled())
	}

	txn := makeTxnProto()
	txn.Sequence = 10
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")

	const valA10 = "valA10"
	const valA12 = "valA12"
	const valA14 = "valA14"

	// Blindly write to keys A and B. Leave C as is.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putA := putArgs(keyA, valA10, txn.Sequence)
	delB := delArgs(keyB, txn.Sequence)
	ba.Add(putA, delB)

	numCalled := mockSender.NumCalled()
	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	// All the requests should be buffered and not make it past the
	// txnWriteBuffer.
	require.Equal(t, numCalled, mockSender.NumCalled())
	// Even though the txnWriteBuffer did not send any Put requests to the KV
	// layer above, the responses should still be populated.
	require.Len(t, br.Responses, 2)
	require.Equal(t, br.Responses[0].GetInner(), &kvpb.PutResponse{})
	require.Equal(t, br.Responses[1].GetInner(), &kvpb.DeleteResponse{})

	// Verify the writes were buffered correctly.
	expBufferedWrites := []bufferedWrite{
		makeBufferedWrite(keyA, makeBufferedValue(valA10, 10)),
		makeBufferedWrite(keyB, makeBufferedValue("", 10)),
	}
	require.Equal(t, expBufferedWrites, twb.testingBufferedWritesAsSlice())

	// Add a few more blind writes to keyA at different sequence numbers.
	putAtSeq(keyA, valA12, 12)
	putAtSeq(keyA, valA14, 14)

	// Verify the buffer is what we expect.
	expBufferedWrites = []bufferedWrite{
		makeBufferedWrite(keyA,
			makeBufferedValue(valA10, 10),
			makeBufferedValue(valA12, 12),
			makeBufferedValue(valA14, 14),
		),
		makeBufferedWrite(keyB, makeBufferedValue("", 10)),
	}
	require.Equal(t, expBufferedWrites, twb.testingBufferedWritesAsSlice())

	for _, tc := range []struct {
		scanFormat kvpb.ScanFormat
		numKVs     func(rows []roachpb.KeyValue, batchResponses [][]byte) int
		firstKV    func(rows []roachpb.KeyValue, batchResponses [][]byte) (roachpb.Key, roachpb.Value)
	}{
		{
			scanFormat: kvpb.KEY_VALUES,
			numKVs: func(rows []roachpb.KeyValue, _ [][]byte) int {
				return len(rows)
			},
			firstKV: func(rows []roachpb.KeyValue, _ [][]byte) (roachpb.Key, roachpb.Value) {
				return rows[0].Key, rows[0].Value
			},
		},
		{
			scanFormat: kvpb.BATCH_RESPONSE,
			numKVs: func(_ []roachpb.KeyValue, batchResponses [][]byte) int {
				var numKVs int
				err := enginepb.ScanDecodeKeyValues(batchResponses, func([]byte, hlc.Timestamp, []byte) error {
					numKVs++
					return nil
				})
				require.NoError(t, err)
				return numKVs
			},
			firstKV: func(_ []roachpb.KeyValue, batchResponses [][]byte) (roachpb.Key, roachpb.Value) {
				key, rawBytes, _, err := enginepb.ScanDecodeKeyValueNoTS(batchResponses[0])
				require.NoError(t, err)
				return key, roachpb.Value{RawBytes: rawBytes}
			},
		},
	} {
		// First up, perform reads on key A at various sequence numbers and
		// ensure the correct value is served from the buffer.
		for seq, expVal := range map[enginepb.TxnSeq]string{
			10: valA10, 11: valA10, 12: valA12, 13: valA12, 14: valA14, 15: valA14,
		} {
			ba = &kvpb.BatchRequest{}
			txn.Sequence = seq
			ba.Header = kvpb.Header{Txn: &txn}
			scan := &kvpb.ScanRequest{
				RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyC, Sequence: txn.Sequence},
				ScanFormat:    tc.scanFormat,
			}
			reverseScan := &kvpb.ReverseScanRequest{
				RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyC, Sequence: txn.Sequence},
				ScanFormat:    tc.scanFormat,
			}
			ba.Add(scan)
			ba.Add(reverseScan)

			numCalled = mockSender.NumCalled()
			mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 2)
				require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &kvpb.ReverseScanRequest{}, ba.Requests[1].GetInner())

				br = ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			})

			br, pErr = twb.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)
			require.Len(t, br.Responses, 2)
			// There should only be a single response, for Key A, as Key B was
			// deleted.
			sr := br.Responses[0].GetInner().(*kvpb.ScanResponse)
			require.Equal(t, 1, tc.numKVs(sr.Rows, sr.BatchResponses))
			rsr := br.Responses[1].GetInner().(*kvpb.ReverseScanResponse)
			require.Equal(t, 1, tc.numKVs(rsr.Rows, rsr.BatchResponses))
			sk, sv := tc.firstKV(sr.Rows, sr.BatchResponses)
			require.Equal(t, keyA, sk)
			require.Equal(t, roachpb.MakeValueFromString(expVal), sv)
			rsk, rsv := tc.firstKV(rsr.Rows, rsr.BatchResponses)
			require.Equal(t, keyA, rsk)
			require.Equal(t, roachpb.MakeValueFromString(expVal), rsv)
			// Assert that the request was sent to the KV layer.
			require.Equal(t, mockSender.NumCalled(), numCalled+1)
		}

		// Perform a scan at a lower sequence number than the minimum buffered
		// write. This should be sent to the KV layer, like above, but the
		// result shouldn't include any buffered writes.
		txn.Sequence = 9
		ba = &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: &txn}
		scan := &kvpb.ScanRequest{
			RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyC, Sequence: txn.Sequence},
			ScanFormat:    tc.scanFormat,
		}
		reverseScan := &kvpb.ReverseScanRequest{
			RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyC, Sequence: txn.Sequence},
			ScanFormat:    tc.scanFormat,
		}
		ba.Add(scan)
		ba.Add(reverseScan)

		numCalled = mockSender.NumCalled()
		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 2)
			require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[0].GetInner())
			require.IsType(t, &kvpb.ReverseScanRequest{}, ba.Requests[1].GetInner())

			br = ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})
		br, pErr = twb.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
		require.Len(t, br.Responses, 2)
		// Assert that no buffered write was returned.
		sr := br.Responses[0].GetInner().(*kvpb.ScanResponse)
		require.Equal(t, 0, tc.numKVs(sr.Rows, sr.BatchResponses))
		rsr := br.Responses[1].GetInner().(*kvpb.ReverseScanResponse)
		require.Equal(t, 0, tc.numKVs(rsr.Rows, rsr.BatchResponses))
		// Assert that the request was sent to the KV layer.
		require.Equal(t, mockSender.NumCalled(), numCalled+1)
	}

	// Lastly, for completeness, commit the transaction and ensure that the buffer
	// is correctly flushed.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.EndTxnRequest{Commit: true})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)

		// We now expect the buffer to be flushed along with the commit.
		require.IsType(t, &kvpb.DeleteRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[2].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Even though we flushed the buffer, responses from the blind writes should
	// not be returned.
	require.Len(t, br.Responses, 1)
	require.IsType(t, &kvpb.EndTxnResponse{}, br.Responses[0].GetInner())
}

// TestTxnWriteBufferLockingGetRequests ensures that locking get requests are
// handled appropriately -- they're sent to KV, to acquire a lock, but the
// read is served from the buffer (upholding read-your-own-write semantics).
func TestTxnWriteBufferLockingGetRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	txn := makeTxnProto()
	txn.Sequence = 10
	keyA := roachpb.Key("a")
	valA := "val"

	// Blindly write to key A.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putA := putArgs(keyA, valA, txn.Sequence)
	ba.Add(putA)

	numCalled := mockSender.NumCalled()
	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	// All the requests should be buffered and not make it past the
	// txnWriteBuffer.
	require.Equal(t, numCalled, mockSender.NumCalled())
	// Even though the txnWriteBuffer did not send any Put requests to the KV
	// layer above, the responses should still be populated.
	require.Len(t, br.Responses, 1)
	require.Equal(t, br.Responses[0].GetInner(), &kvpb.PutResponse{})

	// Verify the writes were buffered correctly.
	expBufferedWrites := []bufferedWrite{
		makeBufferedWrite(keyA, makeBufferedValue(valA, 10)),
	}
	require.Equal(t, expBufferedWrites, twb.testingBufferedWritesAsSlice())

	// Perform a locking read on keyA. Ensure a request is sent to the KV layer,
	// but the response is served from the buffer.
	testutils.RunValues(t, "str", []lock.Strength{lock.None, lock.Shared, lock.Exclusive, lock.Update}, func(t *testing.T, strength lock.Strength) {
		txn.Sequence = 11
		ba = &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: &txn}
		getA := &kvpb.GetRequest{
			RequestHeader:      kvpb.RequestHeader{Key: keyA, Sequence: txn.Sequence},
			KeyLockingStrength: strength,
		}
		ba.Add(getA)
		numCalled = mockSender.NumCalled()

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())
			require.Equal(t, strength, ba.Requests[0].GetInner().(*kvpb.GetRequest).KeyLockingStrength)
			require.True(t, strength != lock.None) // non-locking Gets aren't sent to KV

			br = ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})

		br, pErr = twb.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
		require.Len(t, br.Responses, 1)
		require.Equal(t, roachpb.MakeValueFromString(valA), *br.Responses[0].GetInner().(*kvpb.GetResponse).Value)

		var expNumCalled int
		if strength == lock.None {
			expNumCalled = numCalled // nothing should be sent to KV
		} else {
			expNumCalled = numCalled + 1 // a locking request should still be sent to KV
		}
		require.Equal(t, expNumCalled, mockSender.NumCalled())
	})

	// Lastly, for completeness, commit the transaction and ensure that the buffer
	// is correctly flushed.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.EndTxnRequest{Commit: true})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)

		// We now expect the buffer to be flushed along with the commit.
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Even though we flushed the buffer, responses from the blind writes should
	// not be returned.
	require.Len(t, br.Responses, 1)
	require.IsType(t, &kvpb.EndTxnResponse{}, br.Responses[0].GetInner())
}

// TestTxnWriteBufferDecomposesConditionalPuts verifies that conditional puts
// are decomposed into a locking Get, which is sent to KV, and a Put, which is
// buffered locally. Additionally, we also test that the Put is only buffered, and
// flushed to KV, if the condition evaluates successfully.
func TestTxnWriteBufferDecomposesConditionalPuts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "condEvalSuccessful", func(t *testing.T, condEvalSuccessful bool) {
		ctx := context.Background()
		twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())
		twb.testingOverrideCPutEvalFn = func(expBytes []byte, actVal *roachpb.Value, actValPresent bool, allowNoExisting bool) *kvpb.ConditionFailedError {
			if condEvalSuccessful {
				return nil
			}
			return &kvpb.ConditionFailedError{}
		}

		txn := makeTxnProto()
		txn.Sequence = 10
		keyA := roachpb.Key("a")
		valAStr := "valA"

		// Blindly write to keys A.
		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: &txn}
		cputA := cputArgs(keyA, valAStr, valAStr, txn.Sequence)
		ba.Add(cputA)

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())
			getReq := ba.Requests[0].GetInner().(*kvpb.GetRequest)
			require.Equal(t, keyA, getReq.Key)
			require.Equal(t, txn.Sequence, getReq.Sequence)
			require.Equal(t, lock.Exclusive, getReq.KeyLockingStrength)
			br := ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})

		br, pErr := twb.SendLocked(ctx, ba)
		if condEvalSuccessful {
			require.Nil(t, pErr)
			require.NotNil(t, br)
		} else {
			require.NotNil(t, pErr)
			require.IsType(t, &kvpb.ConditionFailedError{}, pErr.GoError())
		}

		// Lastly, commit the transaction. A put should only be flushed if the
		// condition evaluated successfully.
		ba = &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: &txn}
		ba.Add(&kvpb.EndTxnRequest{Commit: true})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			if condEvalSuccessful {
				require.Len(t, ba.Requests, 2)
				require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[1].GetInner())
			} else {
				require.Len(t, ba.Requests, 1)
				require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())
			}

			br = ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})

		br, pErr = twb.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		// Even though we may have flushed the buffer, responses from the blind writes should
		// not be returned.
		require.Len(t, br.Responses, 1)
		require.IsType(t, &kvpb.EndTxnResponse{}, br.Responses[0].GetInner())
	})
}

// TestTxnWriteBufferDecomposesConditionalPutsExpectingNoRow verifies
// that conditional puts are decomposed into a locking Get with
// LockNonExisting set when the expected ConditionalPut expects no
// existing row.
func TestTxnWriteBufferDecomposesConditionalPutsExpectingNoRow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	twb.testingOverrideCPutEvalFn = func(expBytes []byte, actVal *roachpb.Value, actValPresent bool, allowNoExisting bool) *kvpb.ConditionFailedError {
		return nil
	}

	txn := makeTxnProto()
	txn.Sequence = 10
	keyA := roachpb.Key("a")

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(cputArgs(keyA, "val", "", txn.Sequence))

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())
		getReq := ba.Requests[0].GetInner().(*kvpb.GetRequest)
		require.Equal(t, keyA, getReq.Key)
		require.Equal(t, txn.Sequence, getReq.Sequence)
		require.Equal(t, lock.Exclusive, getReq.KeyLockingStrength)
		require.True(t, getReq.LockNonExisting)
		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.EndTxnRequest{Commit: true})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Even though we may have flushed the buffer, responses from the blind writes should
	// not be returned.
	require.Len(t, br.Responses, 1)
	require.IsType(t, &kvpb.EndTxnResponse{}, br.Responses[0].GetInner())
}

// TestTxnWriteBufferRespectsMustAcquireExclusiveLock verifies that Put and
// Delete requests that have the MustAcquireExclusiveLock are decomposed into a
// locking Get and a buffered write.
func TestTxnWriteBufferRespectsMustAcquireExclusiveLock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	txn := makeTxnProto()
	txn.Sequence = 10
	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	val := "val"

	// Blindly write to keys A and B.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	delA := delArgs(keyA, txn.Sequence)
	putB := putArgs(keyB, val, txn.Sequence)

	ba.Add(delA)
	ba.Add(putB)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())
		getReq := ba.Requests[0].GetInner().(*kvpb.GetRequest)
		require.Equal(t, keyA, getReq.Key)
		require.Equal(t, txn.Sequence, getReq.Sequence)
		require.Equal(t, lock.Exclusive, getReq.KeyLockingStrength)
		require.True(t, getReq.LockNonExisting)

		require.IsType(t, &kvpb.GetRequest{}, ba.Requests[1].GetInner())
		getReq = ba.Requests[1].GetInner().(*kvpb.GetRequest)
		require.Equal(t, keyB, getReq.Key)
		require.Equal(t, txn.Sequence, getReq.Sequence)
		require.Equal(t, lock.Exclusive, getReq.KeyLockingStrength)
		require.True(t, getReq.LockNonExisting)
		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Lastly, commit the transaction.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.EndTxnRequest{Commit: true})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		require.IsType(t, &kvpb.DeleteRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[2].GetInner())
		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Even though we may have flushed the buffer, responses from the blind writes should
	// not be returned.
	require.Len(t, br.Responses, 1)
	require.IsType(t, &kvpb.EndTxnResponse{}, br.Responses[0].GetInner())
}

// TestTxnWriteBufferMustSortBatchesBySequenceNumber verifies that flushed
// batches are sorted in sequence number order, as currently required by the txn
// pipeliner interceptor.
func TestTxnWriteBufferMustSortBatchesBySequenceNumber(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	txn := makeTxnProto()
	txn.Sequence = 10
	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	keyC := roachpb.Key("b")
	val := "val"

	// Send a batch that should be completely buffered.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(putArgs(keyC, val, txn.Sequence))
	ba.Add(putArgs(keyB, val, txn.Sequence+1))
	ba.Add(putArgs(keyA, val, txn.Sequence+2))

	prevNumCalled := mockSender.numCalled
	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, prevNumCalled, mockSender.numCalled, "batch sent unexpectedly")

	// Commit the batch to flush the buffer.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.EndTxnRequest{
		RequestHeader: kvpb.RequestHeader{Sequence: txn.Sequence + 4},
		Commit:        true,
	})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		lastSeq := enginepb.TxnSeq(0)
		for _, r := range ba.Requests {
			seq := r.GetInner().Header().Sequence
			if seq >= lastSeq {
				lastSeq = seq
			} else {
				t.Fatal("expected batch to be sorted by sequence number")
			}
		}
		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
}

// TestTxnWriteBufferEstimateSize is a unit test for
// txnWriteBuffer.estimateSize().
func TestTxnWriteBufferEstimateSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	twb, _ := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	st := cluster.MakeTestingClusterSettings()
	twb.st = st

	txn := makeTxnProto()
	txn.Sequence = 10
	keyA := roachpb.Key("a")
	valAStr := "valA"
	valA := roachpb.MakeValueFromString(valAStr)
	keyLarge := roachpb.Key("a" + strings.Repeat("A", 1000))
	valLargeStr := "val" + strings.Repeat("A", 1000)
	valLarge := roachpb.MakeValueFromString(valLargeStr)

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putA := putArgs(keyA, valAStr, txn.Sequence)
	ba.Add(putA)

	require.Equal(t,
		int64(len(keyA)+len(valA.RawBytes))+bufferedWriteStructOverhead+bufferedValueStructOverhead,
		twb.estimateSize(ba),
	)

	ba = &kvpb.BatchRequest{}
	cputLarge := cputArgs(keyLarge, valLargeStr, "", txn.Sequence)
	ba.Add(cputLarge)

	require.Equal(t,
		int64(len(keyLarge)+len(valLarge.RawBytes))+bufferedWriteStructOverhead+bufferedValueStructOverhead,
		twb.estimateSize(ba),
	)

	ba = &kvpb.BatchRequest{}
	delA := delArgs(keyA, txn.Sequence)
	ba.Add(delA)

	// NB: note that we're overcounting here, as we're deleting a key that's
	// already present in the buffer. But that's what estimating is about.
	require.Equal(t,
		int64(len(keyA))+bufferedWriteStructOverhead+bufferedValueStructOverhead,
		twb.estimateSize(ba),
	)
}

// TestTxnWriteBufferFlushesWhenOverBudget verifies that the txnWriteBuffer
// flushes the buffer on receiving a batch request that'll run it over budget.
// It also ensures subsequent batches are not buffered.
func TestTxnWriteBufferFlushesWhenOverBudget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	twb, mockSender := makeMockTxnWriteBuffer(st)

	txn := makeTxnProto()
	txn.Sequence = 10
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
	valAStr := "valA"
	valA := roachpb.MakeValueFromString(valAStr)

	putAEstimate := int64(len(keyA)+valA.Size()) + bufferedWriteStructOverhead + bufferedValueStructOverhead

	bufferedWritesMaxBufferSize.Override(ctx, &st.SV, putAEstimate)

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putA := putArgs(keyA, valAStr, txn.Sequence)
	ba.Add(putA)

	numCalled := mockSender.NumCalled()
	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	// All the requests should be buffered and not make it past the
	// txnWriteBuffer. The response returned should be indistinguishable.
	require.Equal(t, numCalled, mockSender.NumCalled())
	require.Len(t, br.Responses, 1)
	require.IsType(t, &kvpb.PutResponse{}, br.Responses[0].GetInner())
	// Verify the Put was buffered correctly.
	expBufferedWrites := []bufferedWrite{
		makeBufferedWrite(keyA, makeBufferedValue("valA", 10)),
	}
	require.Equal(t, expBufferedWrites, twb.testingBufferedWritesAsSlice())

	// The next batch will contain a write request that'll run us over budget.
	// The buffer should be flushed.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	delB := delArgs(keyB, txn.Sequence)
	ba.Add(delB)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)

		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.DeleteRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	// Even though we flushed the Put, it shouldn't make it back to the response.
	require.Len(t, br.Responses, 1)
	require.IsType(t, &kvpb.DeleteResponse{}, br.Responses[0].GetInner())
	require.Equal(t, int64(1), twb.txnMetrics.TxnWriteBufferDisabledAfterBuffering.Count())
	require.Equal(t, int64(1), twb.txnMetrics.TxnWriteBufferMemoryLimitExceeded.Count())

	// Ensure the buffer is empty at this point.
	require.Equal(t, 0, len(twb.testingBufferedWritesAsSlice()))

	// Subsequent batches should not be buffered.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putC := putArgs(keyC, valAStr, txn.Sequence)
	ba.Add(putC)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)

		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Len(t, br.Responses, 1)
	require.IsType(t, &kvpb.PutResponse{}, br.Responses[0].GetInner())

	// Commit the transaction. We flushed the buffer already, and no subsequent
	// writes were buffered, so the buffer should be empty. As such, no write
	// requests should be added to the batch.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.EndTxnRequest{Commit: true})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Len(t, br.Responses, 1)
	require.IsType(t, &kvpb.EndTxnResponse{}, br.Responses[0].GetInner())
	// Sanity check the metrics remain the same (and don't increase).
	require.Equal(t, int64(1), twb.txnMetrics.TxnWriteBufferDisabledAfterBuffering.Count())
	require.Equal(t, int64(1), twb.txnMetrics.TxnWriteBufferMemoryLimitExceeded.Count())
}

// TestTxnWriteBufferFlushesIfBatchRequiresFlushing ensures that the
// txnWriteBuffer correctly handles requests that aren't currently
// supported by the txnWriteBuffer by flushing the buffer before
// processing the request.
func TestTxnWriteBufferFlushesIfBatchRequiresFlushing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
	valA := "valA"

	type batchSendMock func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error)
	type testCase struct {
		name         string
		ba           func(*kvpb.BatchRequest)
		baSender     func(*testing.T) batchSendMock
		validateResp func(*testing.T, *kvpb.BatchResponse, *kvpb.Error)
	}

	testCases := []testCase{
		{
			name: "DeleteRange",
			ba: func(b *kvpb.BatchRequest) {
				b.Add(delRangeArgs(keyA, keyB, b.Txn.Sequence))
			},
			baSender: func(t *testing.T) batchSendMock {
				return func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
					require.Len(t, ba.Requests, 3)
					require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
					require.IsType(t, &kvpb.DeleteRequest{}, ba.Requests[1].GetInner())
					require.IsType(t, &kvpb.DeleteRangeRequest{}, ba.Requests[2].GetInner())

					br := ba.CreateReply()
					br.Txn = ba.Txn
					return br, nil
				}
			},
			validateResp: func(t *testing.T, br *kvpb.BatchResponse, pErr *kvpb.Error) {
				require.Nil(t, pErr)
				require.NotNil(t, br)
				require.Len(t, br.Responses, 1)
				require.IsType(t, &kvpb.DeleteRangeResponse{}, br.Responses[0].GetInner())
			},
		},
		{
			name: "Increment",
			ba: func(b *kvpb.BatchRequest) {
				b.Add(&kvpb.IncrementRequest{
					RequestHeader: kvpb.RequestHeader{Key: keyA},
					Increment:     1,
				})
			},
			baSender: func(t *testing.T) batchSendMock {
				return func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
					require.Len(t, ba.Requests, 3)
					require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
					require.IsType(t, &kvpb.DeleteRequest{}, ba.Requests[1].GetInner())
					require.IsType(t, &kvpb.IncrementRequest{}, ba.Requests[2].GetInner())

					br := ba.CreateReply()
					br.Txn = ba.Txn
					return br, nil
				}
			},
			validateResp: func(t *testing.T, br *kvpb.BatchResponse, pErr *kvpb.Error) {
				require.Nil(t, pErr)
				require.NotNil(t, br)
				require.Len(t, br.Responses, 1)
				require.IsType(t, &kvpb.IncrementResponse{}, br.Responses[0].GetInner())
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

			txn := makeTxnProto()
			txn.Sequence = 10
			ba := &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: &txn}
			putA := putArgs(keyA, valA, txn.Sequence)
			delC := delArgs(keyC, txn.Sequence)
			ba.Add(putA)
			ba.Add(delC)

			numCalled := mockSender.NumCalled()
			br, pErr := twb.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)
			// All the requests should be buffered and not make it past the
			// txnWriteBuffer. The response returned should be indistinguishable.
			require.Equal(t, numCalled, mockSender.NumCalled())
			require.Len(t, br.Responses, 2)
			require.IsType(t, &kvpb.PutResponse{}, br.Responses[0].GetInner())
			// Verify the Put was buffered correctly.
			expBufferedWrites := []bufferedWrite{
				makeBufferedWrite(keyA, makeBufferedValue("valA", 10)),
				makeBufferedWrite(keyC, makeBufferedValue("", 10)),
			}
			require.Equal(t, expBufferedWrites, twb.testingBufferedWritesAsSlice())

			// Send the batch that should require a flush
			ba = &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: &txn}
			tc.ba(ba)
			mockSender.MockSend(tc.baSender(t))
			br, pErr = twb.SendLocked(ctx, ba)
			tc.validateResp(t, br, pErr)
			// Ensure the buffer is empty at this point.
			require.Equal(t, 0, len(twb.testingBufferedWritesAsSlice()))
			require.Equal(t, int64(1), twb.txnMetrics.TxnWriteBufferDisabledAfterBuffering.Count())

			// Subsequent batches should not be buffered.
			ba = &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: &txn}
			putC := putArgs(keyC, valA, txn.Sequence)
			ba.Add(putC)

			mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)

				require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

				br = ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			})

			br, pErr = twb.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)
			require.Len(t, br.Responses, 1)
			require.IsType(t, &kvpb.PutResponse{}, br.Responses[0].GetInner())

			// Commit the transaction. We flushed the buffer already, and no subsequent
			// writes were buffered, so the buffer should be empty. As such, no write
			// requests should be added to the batch.
			ba = &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: &txn}
			ba.Add(&kvpb.EndTxnRequest{Commit: true})

			mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

				br = ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			})

			br, pErr = twb.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)
			require.Len(t, br.Responses, 1)
			require.IsType(t, &kvpb.EndTxnResponse{}, br.Responses[0].GetInner())
		})
	}
}

// TestTxnWriteBufferRollbackToSavepoint tests the savepoint rollback logic.
func TestTxnWriteBufferRollbackToSavepoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	txn := makeTxnProto()
	txn.Sequence = 10
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
	valA, valA2, valB := "valA", "valA2", "valB"

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putA := putArgs(keyA, valA, txn.Sequence)
	txn.Sequence++
	delC := delArgs(keyC, txn.Sequence)
	ba.Add(putA)
	ba.Add(delC)

	numCalled := mockSender.NumCalled()
	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	// All the requests should be buffered and not make it past the
	// txnWriteBuffer. The response returned should be indistinguishable.
	require.Equal(t, numCalled, mockSender.NumCalled())
	require.Len(t, br.Responses, 2)
	require.IsType(t, &kvpb.PutResponse{}, br.Responses[0].GetInner())
	require.IsType(t, &kvpb.DeleteResponse{}, br.Responses[1].GetInner())
	// Verify the
	expBufferedWrites := []bufferedWrite{
		makeBufferedWrite(keyA, makeBufferedValue("valA", 10)),
		makeBufferedWrite(keyC, makeBufferedValue("", 11)),
	}
	require.Equal(t, expBufferedWrites, twb.testingBufferedWritesAsSlice())

	// Create a savepoint. This is inclusive of the buffered Delete on keyC.
	savepoint := &savepoint{seqNum: txn.Sequence}
	twb.createSavepointLocked(ctx, savepoint)

	// Add some new writes. A second write to keyA and a new one to keyB.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	txn.Sequence++
	putA2 := putArgs(keyA, valA2, txn.Sequence)
	ba.Add(putA2)
	txn.Sequence++
	putB := putArgs(keyB, valB, txn.Sequence)
	ba.Add(putB)

	// All these writes should be buffered as well, with no KV requests sent yet.
	numCalled = mockSender.NumCalled()
	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	require.Equal(t, numCalled, mockSender.NumCalled())
	require.Len(t, br.Responses, 2)
	require.IsType(t, &kvpb.PutResponse{}, br.Responses[0].GetInner())
	require.IsType(t, &kvpb.PutResponse{}, br.Responses[1].GetInner())
	// Verify the state of the write buffer.
	expBufferedWrites = []bufferedWrite{
		makeBufferedWrite(keyA, makeBufferedValue("valA", 10), makeBufferedValue("valA2", 12)),
		makeBufferedWrite(keyB, makeBufferedValue("valB", 13)),
		makeBufferedWrite(keyC, makeBufferedValue("", 11)),
	}
	require.Equal(t, expBufferedWrites, twb.testingBufferedWritesAsSlice())

	// Now, Rollback to the savepoint. This should leave just one write in the
	// buffer, that on keyA at seqnum 10.
	twb.rollbackToSavepointLocked(ctx, *savepoint)
	expBufferedWrites = []bufferedWrite{
		makeBufferedWrite(keyA, makeBufferedValue("valA", 10)),
	}
	require.Equal(t, expBufferedWrites, twb.testingBufferedWritesAsSlice())

	// Commit the transaction.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.EndTxnRequest{Commit: true})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[1].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Len(t, br.Responses, 1)
	require.IsType(t, &kvpb.EndTxnResponse{}, br.Responses[0].GetInner())
}

// TestTxnWriteBufferRollbackToSavepointMidTxn tests the savepoint rollback
// logic in the presence of explicit savepoints.
func TestTxnWriteBufferRollbackToSavepointMidTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	sendPut := func(t *testing.T, twb *txnWriteBuffer, mockSender *mockLockedSender, txn *roachpb.Transaction) {
		txn.Sequence++
		keyA := roachpb.Key("a")
		valA := fmt.Sprintf("valA@%d", txn.Sequence)
		putA := putArgs(keyA, valA, txn.Sequence)

		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn}
		ba.Add(putA)

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			br := ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})

		numCalled := mockSender.NumCalled()
		br, pErr := twb.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
		require.Equal(t, numCalled, mockSender.NumCalled())
	}

	delRangeBatch := func(txn *roachpb.Transaction) *kvpb.BatchRequest {
		txn.Sequence++
		keyB := roachpb.Key("b")
		keyC := roachpb.Key("c")
		delRangeReq := delRangeArgs(keyB, keyC, txn.Sequence)

		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn}
		ba.Add(delRangeReq)
		return ba
	}

	savepoint := func(twb *txnWriteBuffer, txn *roachpb.Transaction) *savepoint {
		txn.Sequence++
		savepoint := &savepoint{seqNum: txn.Sequence}
		twb.createSavepointLocked(ctx, savepoint)
		return savepoint
	}

	t.Run("flush with no savepoint sends latest", func(t *testing.T) {
		twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())
		txn := makeTxnProto()
		// Send 4 requests to the buffer
		sendPut(t, &twb, mockSender, &txn)
		sendPut(t, &twb, mockSender, &txn)
		sendPut(t, &twb, mockSender, &txn)
		sendPut(t, &twb, mockSender, &txn)
		ba := delRangeBatch(&txn)

		// Expect 1 Put and 1 DelRange.
		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 2)
			require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
			require.IsType(t, &kvpb.DeleteRangeRequest{}, ba.Requests[1].GetInner())

			br := ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})
		br, pErr := twb.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
	})

	t.Run("flush with savepoint still elides unnecessary writes under savepoint", func(t *testing.T) {
		twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())
		txn := makeTxnProto()
		sendPut(t, &twb, mockSender, &txn) // should be elided
		sendPut(t, &twb, mockSender, &txn)
		_ = savepoint(&twb, &txn)
		sendPut(t, &twb, mockSender, &txn)
		sendPut(t, &twb, mockSender, &txn)
		ba := delRangeBatch(&txn)

		// Expect 3 Put and 1 DelRange.
		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 4)
			require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
			require.IsType(t, &kvpb.PutRequest{}, ba.Requests[1].GetInner())
			require.IsType(t, &kvpb.PutRequest{}, ba.Requests[2].GetInner())
			require.IsType(t, &kvpb.DeleteRangeRequest{}, ba.Requests[3].GetInner())

			br := ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})
		br, pErr := twb.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
	})

	t.Run("flush after release of earliest savepoint only sends latest", func(t *testing.T) {
		twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())
		txn := makeTxnProto()
		sendPut(t, &twb, mockSender, &txn)
		sendPut(t, &twb, mockSender, &txn)
		sendPut(t, &twb, mockSender, &txn)
		sp := savepoint(&twb, &txn)
		sendPut(t, &twb, mockSender, &txn)
		twb.releaseSavepointLocked(ctx, sp)

		ba := delRangeBatch(&txn)
		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 2)
			require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
			require.IsType(t, &kvpb.DeleteRangeRequest{}, ba.Requests[1].GetInner())

			br := ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})
		br, pErr := twb.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
	})
}

// TestTxnWriteBufferFlushesAfterDisabling verifies that the txnWriteBuffer
// flushes on the next batch after it is disabled if it buffered any writes.
func TestTxnWriteBufferFlushesAfterDisabling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	txn := makeTxnProto()
	txn.Sequence = 1
	keyA := roachpb.Key("a")

	// Write to keyA.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putA := putArgs(keyA, "val1", txn.Sequence)
	ba.Add(putA)

	numCalledBefore := mockSender.NumCalled()
	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	// The request shouldn't make it past the txnWriteBuffer as there's only one
	// Put, which should be buffered.
	require.Equal(t, numCalledBefore, mockSender.NumCalled())

	// Even though the txnWriteBuffer did not send any Put requests to the KV
	// layer above, the responses should still be populated.
	require.Len(t, br.Responses, 1)
	require.Equal(t, br.Responses[0].GetInner(), &kvpb.PutResponse{})

	// Verify the write was buffered correctly.
	expBufferedWrites := []bufferedWrite{
		makeBufferedWrite(keyA, makeBufferedValue("val1", 1)),
	}
	require.Equal(t, expBufferedWrites, twb.testingBufferedWritesAsSlice())

	// Disable write buffering.
	twb.setEnabled(false)

	// Delete keyA at a higher sequence number.
	txn.Sequence++
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	delA := delArgs(keyA, txn.Sequence)
	ba.Add(delA)

	numCalledBefore = mockSender.NumCalled()
	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// The buffer should be flushed and disabled.
	require.False(t, twb.enabled)
	require.False(t, twb.flushOnNextBatch)
	require.Equal(t, int64(1), twb.txnMetrics.TxnWriteBufferDisabledAfterBuffering.Count())

	// Both Put and Del should make it to the server in a single bath.
	require.Equal(t, numCalledBefore+1, mockSender.NumCalled())

	// Even though we flushed the Put, it shouldn't make it back to the response.
	require.Len(t, br.Responses, 1)
	require.IsType(t, &kvpb.DeleteResponse{}, br.Responses[0].GetInner())

	// Ensure the buffer is empty at this point.
	require.Equal(t, 0, len(twb.testingBufferedWritesAsSlice()))

	// Subsequent batches should not be buffered.
	txn.Sequence++
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putA = putArgs(keyA, "val2", txn.Sequence)
	ba.Add(putA)

	numCalledBefore = mockSender.NumCalled()
	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Ensure the buffer is still empty and the batch made it to the server.
	require.Equal(t, 0, len(twb.testingBufferedWritesAsSlice()))
	require.Equal(t, numCalledBefore+1, mockSender.NumCalled())

	// Commit the transaction. We flushed the buffer already, and no subsequent
	// writes were buffered, so the buffer should be empty. As such, no write
	// requests should be added to the batch.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.EndTxnRequest{Commit: true})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Len(t, br.Responses, 1)
	require.IsType(t, &kvpb.EndTxnResponse{}, br.Responses[0].GetInner())
	// Sanity check the metric remains the same (and doesn't increase).
	require.Equal(t, int64(1), twb.txnMetrics.TxnWriteBufferDisabledAfterBuffering.Count())
}

// TestTxnWriteBufferClearsBufferOnEpochBump tests that the txnWriteBuffer
// clears its buffer whenever the epoch is bumped.
func TestTxnWriteBufferClearsBufferOnEpochBump(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	txn := makeTxnProto()
	txn.Sequence = 1

	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")

	// Blindly write to some keys that should all be buffered.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putA := putArgs(keyA, "valA", txn.Sequence)
	putB := putArgs(keyB, "valB", txn.Sequence)
	delC := delArgs(keyC, txn.Sequence)
	ba.Add(putA, putB, delC)

	numCalled := mockSender.NumCalled()
	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, numCalled, mockSender.NumCalled())

	// The buffer should be cleared after epoch bump.
	twb.epochBumpedLocked()
	require.Equal(t, 0, len(twb.testingBufferedWritesAsSlice()))
	require.Equal(t, 0, int(twb.bufferSize))
	require.Equal(t, numCalled, mockSender.NumCalled())
}

// TestTxnWriteBufferBatchRequestValidation verifies that the txnWriteBuffer
// rejects requests that it doesn't know how to support.
func TestTxnWriteBufferBatchRequestValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	type testCase struct {
		name string
		ba   func() *kvpb.BatchRequest
	}

	txn := makeTxnProto()
	txn.Sequence = 1
	keyA, keyC := roachpb.Key("a"), roachpb.Key("c")

	tests := []testCase{
		{
			name: "batch with OriginTimestamp in WriteOptions",
			ba: func() *kvpb.BatchRequest {
				header := kvpb.Header{
					Txn: &txn,
					WriteOptions: &kvpb.WriteOptions{
						OriginTimestamp: hlc.Timestamp{WallTime: 1},
					},
				}
				return &kvpb.BatchRequest{Header: header}
			},
		},
		{
			name: "batch with OriginID in WriteOptions",
			ba: func() *kvpb.BatchRequest {
				header := kvpb.Header{
					Txn: &txn,
					WriteOptions: &kvpb.WriteOptions{
						OriginID: 1,
					},
				}
				return &kvpb.BatchRequest{Header: header}
			},
		},
		{
			name: "batch with OriginTimestamp in ConditionalPutRequest",
			ba: func() *kvpb.BatchRequest {
				header := kvpb.Header{
					Txn: &txn,
				}
				b := &kvpb.BatchRequest{Header: header}
				r := &kvpb.ConditionalPutRequest{
					OriginTimestamp: hlc.Timestamp{WallTime: 1},
				}
				b.Add(r)
				return b
			},
		},
		{
			name: "batch with ReturnRawMVCCValues Scan",
			ba: func() *kvpb.BatchRequest {
				b := &kvpb.BatchRequest{Header: kvpb.Header{Txn: &txn}}
				r := &kvpb.ScanRequest{
					ReturnRawMVCCValues: true,
					RequestHeader:       kvpb.RequestHeader{Key: keyA, EndKey: keyC, Sequence: txn.Sequence},
				}
				b.Add(r)
				return b
			},
		},
		{
			name: "batch with ReturnRawMVCCValues ReverseScan",
			ba: func() *kvpb.BatchRequest {
				b := &kvpb.BatchRequest{Header: kvpb.Header{Txn: &txn}}
				r := &kvpb.ReverseScanRequest{
					ReturnRawMVCCValues: true,
					RequestHeader:       kvpb.RequestHeader{Key: keyA, EndKey: keyC, Sequence: txn.Sequence},
				}
				b.Add(r)
				return b
			},
		},
		{
			name: "batch with ReturnRawMVCCValues Get",
			ba: func() *kvpb.BatchRequest {
				b := &kvpb.BatchRequest{Header: kvpb.Header{Txn: &txn}}
				r := &kvpb.GetRequest{
					ReturnRawMVCCValues: true,
					RequestHeader:       kvpb.RequestHeader{Key: keyA, Sequence: txn.Sequence},
				}
				b.Add(r)
				return b
			},
		},
		{
			name: "batch with COL_BATCH_RESPONSE Scan",
			ba: func() *kvpb.BatchRequest {
				b := &kvpb.BatchRequest{Header: kvpb.Header{Txn: &txn}}
				r := &kvpb.ScanRequest{
					ScanFormat:    kvpb.COL_BATCH_RESPONSE,
					RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyC, Sequence: txn.Sequence},
				}
				b.Add(r)
				return b
			},
		},
		{
			name: "batch with COL_BATCH_RESPONSE ReverseScan",
			ba: func() *kvpb.BatchRequest {
				b := &kvpb.BatchRequest{Header: kvpb.Header{Txn: &txn}}
				r := &kvpb.ReverseScanRequest{
					ScanFormat:    kvpb.COL_BATCH_RESPONSE,
					RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyC, Sequence: txn.Sequence},
				}
				b.Add(r)
				return b
			},
		},
		{
			name: "batch with unsupported request",
			ba: func() *kvpb.BatchRequest {
				b := &kvpb.BatchRequest{Header: kvpb.Header{Txn: &txn}}
				r := &kvpb.TruncateLogRequest{}
				b.Add(r)
				return b
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			numCalledBefore := mockSender.NumCalled()
			_, pErr := twb.SendLocked(ctx, tc.ba())
			require.NotNil(t, pErr)
			require.Equal(t, numCalledBefore, mockSender.NumCalled())

		})
	}
}

// TestTxnWriteBufferHasBufferedAllPrecedingWrites verifies that the
// txnWriteBuffer correctly sets the HasBufferedAllPrecedingWrites flag.
func TestTxnWriteBufferHasBufferedAllPrecedingWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	txn := makeTxnProto()
	txn.Sequence = 1
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")

	for _, tc := range []struct {
		name                             string
		setup                            func(*txnWriteBuffer)
		ba                               func(ba *kvpb.BatchRequest)
		mockSend                         func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error)
		expHasBufferedAllPrecedingWrites bool
	}{
		{
			name: "batch with two Get requests",
			ba: func(ba *kvpb.BatchRequest) {
				getA := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyA, Sequence: txn.Sequence}}
				getB := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyB, Sequence: txn.Sequence}}
				ba.Add(getA, getB)
			},
			mockSend: func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 2)
				require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &kvpb.GetRequest{}, ba.Requests[1].GetInner())

				require.True(t, ba.HasBufferedAllPrecedingWrites)

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			},
			expHasBufferedAllPrecedingWrites: true,
		},
		{
			name: "batch with one Put and one Get request",
			ba: func(ba *kvpb.BatchRequest) {
				putA := putArgs(keyA, "valA", txn.Sequence)
				getB := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyB, Sequence: txn.Sequence}}
				ba.Add(putA, getB)
			},
			mockSend: func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())

				require.True(t, ba.HasBufferedAllPrecedingWrites)

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			},
			expHasBufferedAllPrecedingWrites: true,
		},
		{
			name: "batch with one Put, one Get, and one Delete request",
			ba: func(ba *kvpb.BatchRequest) {
				putA := putArgs(keyA, "valA", txn.Sequence)
				getB := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyB, Sequence: txn.Sequence}}
				delC := delArgs(keyC, txn.Sequence)

				ba.Add(putA, getB, delC)
			},
			mockSend: func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())

				require.True(t, ba.HasBufferedAllPrecedingWrites)

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			},
			expHasBufferedAllPrecedingWrites: true,
		},
		{
			name: "batch with one DeleteRange and one Get request",
			ba: func(ba *kvpb.BatchRequest) {
				delRange := delRangeArgs(keyA, keyB, txn.Sequence)
				getB := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyB, Sequence: txn.Sequence}}

				ba.Add(delRange, getB)
			},
			mockSend: func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 2)
				require.IsType(t, &kvpb.DeleteRangeRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &kvpb.GetRequest{}, ba.Requests[1].GetInner())

				require.True(t, ba.HasBufferedAllPrecedingWrites)

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			},
			expHasBufferedAllPrecedingWrites: false,
		},
		{
			name: "flushed due to size limit",
			setup: func(twb *txnWriteBuffer) {
				bufferedWritesMaxBufferSize.Override(context.Background(), &twb.st.SV, 1)
			},
			ba: func(ba *kvpb.BatchRequest) {
				putA := putArgs(keyA, "valA", txn.Sequence)

				ba.Add(putA)
			},
			mockSend: func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

				require.True(t, ba.HasBufferedAllPrecedingWrites)

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			},
			expHasBufferedAllPrecedingWrites: false,
		},
		{
			name: "write buffering disabled",
			setup: func(twb *txnWriteBuffer) {
				twb.setEnabled(false)
			},
			ba: func(ba *kvpb.BatchRequest) {
				putA := putArgs(keyA, "valA", txn.Sequence)

				ba.Add(putA)
			},
			mockSend: func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

				// NB: Should never be set if write buffering is disabled
				require.False(t, ba.HasBufferedAllPrecedingWrites)

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			},
			expHasBufferedAllPrecedingWrites: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := cluster.MakeTestingClusterSettings()
			twb, mockSender := makeMockTxnWriteBuffer(st)

			if tc.setup != nil {
				tc.setup(&twb)
			}

			ba := &kvpb.BatchRequest{Header: kvpb.Header{Txn: &txn}}
			tc.ba(ba)
			mockSender.MockSend(tc.mockSend)

			br, pErr := twb.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)

			// Go to commit the transaction and ensure HasBufferedAllPrecedingWrites
			// is set correctly.
			mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Equal(t, tc.expHasBufferedAllPrecedingWrites, ba.HasBufferedAllPrecedingWrites)

				br = ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			})

			ba = &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: &txn}
			ba.Add(&kvpb.EndTxnRequest{Commit: true})

			br, pErr = twb.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)
			require.Len(t, br.Responses, 1)
			require.IsType(t, &kvpb.EndTxnResponse{}, br.Responses[0].GetInner())
		})
	}
}

// BenchmarkTxnWriteBuffer benchmarks the txnWriteBuffer. The test sets up a
// transaction with an existing buffer and runs a single batch through
// SendLocked and flushBufferAndSendBatch. The test varies the state of the
// buffer, the size of the keys and values, the fraction of reads in the batch,
// as well as the fraction of  reads served from the buffer.
// TODO(mira): Should we test more cases?
//   - Batches with requests other than Get and Put. Notably, CPut.
//   - Batches that exercise error paths.
func BenchmarkTxnWriteBuffer(b *testing.B) {
	defer leaktest.AfterTest(b)()
	ctx := context.Background()

	makeKey := func(i int, kvSize int) roachpb.Key {
		// The keys are kvSize bytes.
		keyPrefix := strings.Repeat("a", kvSize-1)
		return roachpb.Key(fmt.Sprintf("%s%d", keyPrefix, i))
	}
	makeValue := func(kvSize int) string {
		// The values are kvSize KiB.
		return strings.Repeat("a", kvSize*1024)
	}
	makeBuffer := func(kvSize int, txn *roachpb.Transaction, numWrites int) txnWriteBuffer {
		twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())
		twb.setEnabled(true)
		sendFunc := func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			br := ba.CreateReply()
			br.Txn = ba.Txn
			var resps []kvpb.ResponseUnion
			resp := kvpb.ResponseUnion{}
			// All requests get responses. Gets also have a return value.
			for _, req := range ba.Requests {
				switch req.GetInner().(type) {
				case *kvpb.GetRequest:
					resp.Value = &kvpb.ResponseUnion_Get{
						Get: &kvpb.GetResponse{
							Value: &roachpb.Value{RawBytes: []byte(makeValue(kvSize))},
						},
					}
				}
				resps = append(resps, resp)
			}
			br.Responses = resps
			return br, nil
		}
		mockSender.MockSend(sendFunc)

		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn}
		// Write to the keys that will later be served from the buffer but
		// not from the benchmarked batch.
		for i := 0; i < numWrites; i++ {
			ba.Add(putArgs(makeKey(i, kvSize), makeValue(kvSize), enginepb.TxnSeq(i)))
		}
		_, pErr := twb.SendLocked(ctx, ba)
		if pErr != nil {
			b.Fatal(pErr)
		}
		return twb
	}

	numRequests := 100
	// A size X denotes a key of size X bytes and a value of size X KiB. We don't
	// want these to push the buffer past its max size. There's a separate test
	// below for handling flushing the buffer.
	kvSizes := []int{8, 32}
	// The fraction of reads in the benchmarked batch.
	fractionsReads := []float64{0.0, 0.5, 1.0}
	// The fraction of the reads in the batch to be served from the buffer.
	fractionsFromBuffer := []float64{0.0, 0.5, 1.0}
	// The fraction of reads served from the buffer that come from the same batch.
	fractionsFromBufferSameBatch := []float64{0.0, 0.5, 1.0}
	for _, kvSize := range kvSizes {
		for _, fractionReads := range fractionsReads {
			for _, fractionFromBuffer := range fractionsFromBuffer {
				for _, fractionFromBufferSameBatch := range fractionsFromBufferSameBatch {
					name := fmt.Sprintf(
						"SendLocked/size=%v/reads=%2.2f/from_buffer=%2.2f/from_batch=%2.2f", kvSize,
						fractionReads*100, fractionFromBuffer*100, fractionFromBufferSameBatch*100,
					)
					b.Run(
						name, func(b *testing.B) {
							// The total number of requests in the batch being benchmarked are broken down
							// into five groups, executed in the order below.
							// 0. Not included in the batch: previous writes by the same transaction.
							// 1. Reads served from the buffer (same keys as 0).
							// 2. Writes in the same transaction and the same batch.
							// 3. Reads served from the buffer from the same batch (same keys as 2).
							// 4. Reads not served from the buffer.
							// 5. Writes not seen by any reads in the batch.
							numReads := int(fractionReads * float64(numRequests))
							numWrites := numRequests - numReads
							readsFromBuffer := int(fractionFromBuffer * float64(numReads))
							readsFromBufferSameBatch := int(fractionFromBufferSameBatch * float64(readsFromBuffer))
							readsFromPrevBatch := readsFromBuffer - readsFromBufferSameBatch

							// Create the benchmarked batch.
							txn := makeTxnProto()
							ba := &kvpb.BatchRequest{}
							ba.Header = kvpb.Header{Txn: &txn}

							// Read from the keys that were written while setting up the
							// buffer (same transaction, previous batch).
							for i := 0; i < readsFromPrevBatch; i++ {
								ba.Add(getArgs(makeKey(i, kvSize)))
							}
							// Write and then read the keys that are served from the buffer
							// and are in the benchmarked batch.
							for i := readsFromPrevBatch; i < readsFromPrevBatch+readsFromBufferSameBatch; i++ {
								// Half of these puts acquire exclusive locks.
								args := putArgs(makeKey(i, kvSize), makeValue(kvSize), enginepb.TxnSeq(i))
								if i%2 == 0 {
									args.MustAcquireExclusiveLock = true
								}
								ba.Add(args)
								ba.Add(getArgs(makeKey(i, kvSize)))
							}
							// Add any remaining reads, not served from the buffer.
							for i := readsFromPrevBatch + readsFromBufferSameBatch; i < numReads; i++ {
								ba.Add(getArgs(makeKey(i, kvSize)))
							}
							// Add any remaining writes, not observed by any reads.
							for i := readsFromPrevBatch + readsFromBufferSameBatch; i < numWrites; i++ {
								// Half of these puts acquire exclusive locks.
								args := putArgs(makeKey(i, kvSize), makeValue(kvSize), enginepb.TxnSeq(i))
								if i%2 == 0 {
									args.MustAcquireExclusiveLock = true
								}
							}

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								b.StopTimer()
								twb := makeBuffer(kvSize, &txn, readsFromPrevBatch)
								b.StartTimer()
								_, pErr := twb.SendLocked(ctx, ba)
								if pErr != nil {
									b.Fatal(pErr)
								}
							}
						},
					)
				}
			}
		}

		name := fmt.Sprintf("flushBufferAndSendBatch/size=%v", kvSize)
		b.Run(name, func(b *testing.B) {
			// Create the benchmarked batch. It's just a single Get as we're
			// interested in the work to flush the buffer.
			txn := makeTxnProto()
			ba := &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: &txn}
			ba.Add(getArgs(makeKey(0, kvSize)))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// All requests correspond to writes that will be stored in the buffer.
				twb := makeBuffer(kvSize, &txn, numRequests)
				twb.flushOnNextBatch = true
				_, pErr := twb.flushBufferAndSendBatch(ctx, ba)
				if pErr != nil {
					b.Fatal(pErr)
				}
			}
		},
		)
	}
}

// TestTxnWriteBufferChecksForExclusionLoss verifies that decomposed
// writes attach an exclusion timestamp to their final batch.
func TestTxnWriteBufferChecksForExclusionLoss(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	txn := makeTxnProto()
	txn.Sequence = 10
	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	keyC := roachpb.Key("c")

	valStr := "val"

	// Requests that require an exclusion timestamp:
	//
	// - ConditionalPut
	// - PutMustAcquireExclusiveLock
	// - DeleteMustAcquireExclusiveLock
	//

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(cputArgs(keyA, valStr, "", txn.Sequence))

	putReq := putArgs(keyB, valStr, txn.Sequence)
	putReq.MustAcquireExclusiveLock = true
	ba.Add(putReq)

	delReq := delArgs(keyC, txn.Sequence)
	delReq.MustAcquireExclusiveLock = true
	ba.Add(delReq)

	initialReadTimestamp := txn.ReadTimestamp
	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)
		resp := ba.CreateReply()
		resp.Txn = ba.Txn
		return resp, nil
	})

	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Another write on keyB
	txn.BumpReadTimestamp(initialReadTimestamp.Next())
	txn.Sequence++
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putReq = putArgs(keyB, valStr, txn.Sequence)
	putReq.MustAcquireExclusiveLock = true
	ba.Add(putReq)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		resp := ba.CreateReply()
		resp.Txn = ba.Txn
		return resp, nil
	})
	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Commit the transaction and verify that the request has the expected exclusion timestamp.
	txn.BumpReadTimestamp(initialReadTimestamp.Next())
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.EndTxnRequest{Commit: true})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 4)

		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		putReq := ba.Requests[0].GetInner().(*kvpb.PutRequest)
		require.Equal(t, keyA, putReq.Key)
		require.Equal(t, initialReadTimestamp, putReq.ExpectExclusionSince)

		require.IsType(t, &kvpb.DeleteRequest{}, ba.Requests[1].GetInner())
		delReq := ba.Requests[1].GetInner().(*kvpb.DeleteRequest)
		require.Equal(t, keyC, delReq.Key)
		require.Equal(t, initialReadTimestamp, delReq.ExpectExclusionSince)

		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[2].GetInner())
		putReq = ba.Requests[2].GetInner().(*kvpb.PutRequest)
		require.Equal(t, keyB, putReq.Key)
		require.Equal(t, initialReadTimestamp, putReq.ExpectExclusionSince)

		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[3].GetInner())

		resp := ba.CreateReply()
		resp.Txn = ba.Txn
		return resp, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
}

// TestTxnWriteBufferCorrectlyRollsbackExclusionTimestamp verifies that
// decomposed writes don't attach an exclusion timestamp that was established at
// a sequence number that was subsequently rolled back.
func TestTxnWriteBufferCorrectlyRollsbackExclusionTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	txn := makeTxnProto()
	txn.Sequence = 10
	initialReadTimestamp := txn.ReadTimestamp

	keyA := roachpb.Key("a")
	valStr := "val"

	savepoint := &savepoint{seqNum: txn.Sequence}
	twb.createSavepointLocked(ctx, savepoint)

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putReq := putArgs(keyA, valStr, txn.Sequence)
	putReq.MustAcquireExclusiveLock = true
	ba.Add(putReq)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		resp := ba.CreateReply()
		resp.Txn = ba.Txn
		return resp, nil
	})

	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	twb.rollbackToSavepointLocked(ctx, *savepoint)
	txn.Sequence++

	// Another write on keyA
	nextReadTimestamp := initialReadTimestamp.Next()
	txn.BumpReadTimestamp(nextReadTimestamp)
	txn.Sequence++
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putReq = putArgs(keyA, valStr, txn.Sequence)
	putReq.MustAcquireExclusiveLock = true
	ba.Add(putReq)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 1)
		resp := ba.CreateReply()
		resp.Txn = ba.Txn
		return resp, nil
	})
	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Commit the transaction and verify that the request has the expected exclusion timestamp.
	txn.BumpReadTimestamp(nextReadTimestamp.Next())
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&kvpb.EndTxnRequest{Commit: true})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)

		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		putReq := ba.Requests[0].GetInner().(*kvpb.PutRequest)
		require.Equal(t, keyA, putReq.Key)
		require.Equal(t, nextReadTimestamp, putReq.ExpectExclusionSince)

		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[1].GetInner())

		resp := ba.CreateReply()
		resp.Txn = ba.Txn
		return resp, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
}

func TestLockKeyInfo(t *testing.T) {
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}

	t.Run("held", func(t *testing.T) {
		lki := newLockedKeyInfo(lock.Exclusive, 1, ts1)
		require.True(t, lki.held(lock.Exclusive))
		require.False(t, lki.held(lock.Shared))

		lki = newLockedKeyInfo(lock.Shared, 1, ts1)
		require.True(t, lki.held(lock.Shared))
		require.False(t, lki.held(lock.Exclusive))
	})
	t.Run("heldGE", func(t *testing.T) {
		lki := newLockedKeyInfo(lock.Exclusive, 1, ts1)
		require.True(t, lki.heldGE(lock.Exclusive))
		require.True(t, lki.heldGE(lock.Shared))

		lki = newLockedKeyInfo(lock.Shared, 1, ts1)
		require.False(t, lki.heldGE(lock.Exclusive))
		require.True(t, lki.heldGE(lock.Shared))
	})
	t.Run("heldStr", func(t *testing.T) {
		lki := newLockedKeyInfo(lock.Exclusive, 2, ts1)
		require.Equal(t, lock.None, lki.heldStr(1))
		require.Equal(t, lock.Exclusive, lki.heldStr(2))
		require.Equal(t, lock.Exclusive, lki.heldStr(3))

		lki = newLockedKeyInfo(lock.Shared, 2, ts1)
		require.Equal(t, lock.None, lki.heldStr(1))
		require.Equal(t, lock.Shared, lki.heldStr(2))
		require.Equal(t, lock.Shared, lki.heldStr(3))

		lki = newLockedKeyInfo(lock.Shared, 2, ts1)
		lki.acquireLock(lock.Exclusive, 2, ts1)
		require.Equal(t, lock.None, lki.heldStr(1))
		require.Equal(t, lock.Exclusive, lki.heldStr(2))
		require.Equal(t, lock.Exclusive, lki.heldStr(3))

		lki = newLockedKeyInfo(lock.Shared, 2, ts1)
		lki.acquireLock(lock.Exclusive, 3, ts1)
		require.Equal(t, lock.None, lki.heldStr(1))
		require.Equal(t, lock.Shared, lki.heldStr(2))
		require.Equal(t, lock.Exclusive, lki.heldStr(3))
		require.Equal(t, lock.Exclusive, lki.heldStr(4))
	})
	t.Run("acquireLock", func(t *testing.T) {
		lki := newLockedKeyInfo(lock.Exclusive, 1, ts1)
		lki.acquireLock(lock.Shared, 1, ts2)
		require.Equal(t, ts1, lki.ts)
		require.True(t, lki.held(lock.Exclusive))
		require.True(t, lki.held(lock.Shared))

		lki = newLockedKeyInfo(lock.Shared, 1, ts1)
		lki.acquireLock(lock.Exclusive, 1, ts2)
		require.Equal(t, ts1, lki.ts)
		require.True(t, lki.held(lock.Exclusive))
		require.True(t, lki.held(lock.Shared))
	})
	t.Run("rollbackSequence", func(t *testing.T) {
		lki := newLockedKeyInfo(lock.Shared, 2, ts1)
		lki.acquireLock(lock.Exclusive, 2, ts2)
		require.False(t, lki.rollbackSequence(1))
		require.False(t, lki.ts.IsSet())

		// Also test rollback with only one lock type acquired.
		lki = newLockedKeyInfo(lock.Shared, 2, ts1)
		require.False(t, lki.rollbackSequence(1))
		require.False(t, lki.ts.IsSet())

		lki = newLockedKeyInfo(lock.Exclusive, 2, ts1)
		require.False(t, lki.rollbackSequence(1))
		require.False(t, lki.ts.IsSet())

		lki = newLockedKeyInfo(lock.Shared, 2, ts1)
		lki.acquireLock(lock.Exclusive, 3, ts2)
		require.True(t, lki.rollbackSequence(3))
		require.Equal(t, ts1, lki.ts)
		require.True(t, lki.held(lock.Shared))
		require.False(t, lki.held(lock.Exclusive))
	})
}

// TestTxnWriteBufferElidesUnnecessaryLockingRequests tests that if the
// txnWriteBuffer already holds a lock, it elides subsequent locking requests.
func TestTxnWriteBufferElidesUnnecessaryLockingRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Each test case will send two locking requests to the txnWriteBuffer and,
	// when appropriate, make an assertion that the second request is not sent to
	// the underlying sender because it is completely served from the buffer.
	type lockingRequests struct {
		name string

		// requiresValue, when true, indicates that this request requires a value to
		// correctly generate a response.
		requiresValue bool
		// buffersValue, when true, indicates that this request buffers a value that
		// can be later read.
		buffersValue bool
		// buffersLock, when true, indicates that this request records the fact that
		// it locks. Every place where this is false represents a missed opportunity
		// to avoid a locking request.
		buffersLock bool
		// isFullyCovered, when true, indicates that this request can be served
		// completely from the buffer if a previous request locked the value. This
		// is different from requiresValue as it indicates that requests that aren't
		// fully covered must always be sent to KV since there is no way to know if
		// the buffer has all required required values.
		isFullyCovered bool

		generateRequest             func(t *testing.T, ba *kvpb.BatchRequest, txn *roachpb.Transaction)
		optionalGenSecondRequest    func(t *testing.T, ba *kvpb.BatchRequest, txn *roachpb.Transaction)
		validateAndGenerateResponse func(t *testing.T, ba *kvpb.BatchRequest) *kvpb.BatchResponse
	}

	// All point requests are assumed to be on A. All span requests are on [a, c).
	keyA := roachpb.Key("a")
	valueAStr := "valueA"
	valueA := roachpb.MakeValueFromString(valueAStr)
	keyC := roachpb.Key("c")

	// A number of the requests expect a single locking get to be produced. This
	// validate function is shared across them.
	validateLockingGet := func(t *testing.T, ba *kvpb.BatchRequest) *kvpb.BatchResponse {
		getReq := ba.Requests[0].GetGet()
		require.NotNil(t, getReq)
		require.Equal(t, lock.Exclusive, getReq.KeyLockingStrength)
		require.Equal(t, lock.Unreplicated, getReq.KeyLockingDurability)
		resp := ba.CreateReply()
		resp.Txn = ba.Txn
		return resp
	}

	reqs := []lockingRequests{
		{
			name:           "ReplicatedLockingScanScanFormat=KEY_VALUES",
			buffersLock:    false,
			buffersValue:   false,
			requiresValue:  true,
			isFullyCovered: false,
			generateRequest: func(t *testing.T, ba *kvpb.BatchRequest, txn *roachpb.Transaction) {
				ba.Add(&kvpb.ScanRequest{
					KeyLockingStrength:   lock.Exclusive,
					KeyLockingDurability: lock.Replicated,
					ScanFormat:           kvpb.KEY_VALUES,
					RequestHeader:        kvpb.RequestHeader{Key: keyA, EndKey: keyC, Sequence: txn.Sequence},
				})
			},
			validateAndGenerateResponse: func(t *testing.T, ba *kvpb.BatchRequest) *kvpb.BatchResponse {
				scanReq := ba.Requests[0].GetScan()
				require.NotNil(t, scanReq)
				require.Equal(t, lock.Exclusive, scanReq.KeyLockingStrength)
				resp := ba.CreateReply()
				resp.Txn = ba.Txn
				resp.Responses[0].MustSetInner(&kvpb.ScanResponse{
					Rows: []roachpb.KeyValue{{Key: keyA, Value: valueA}},
				})
				return resp
			},
		},
		{
			name:           "ReplicatedLockingScanScanFormat=BATCH_RESPONSE",
			buffersLock:    false,
			buffersValue:   false,
			requiresValue:  true,
			isFullyCovered: false,
			generateRequest: func(t *testing.T, ba *kvpb.BatchRequest, txn *roachpb.Transaction) {
				ba.Add(&kvpb.ScanRequest{
					KeyLockingStrength:   lock.Exclusive,
					KeyLockingDurability: lock.Replicated,
					ScanFormat:           kvpb.BATCH_RESPONSE,
					RequestHeader:        kvpb.RequestHeader{Key: keyA, EndKey: keyC, Sequence: txn.Sequence},
				})
			},
			validateAndGenerateResponse: func(t *testing.T, ba *kvpb.BatchRequest) *kvpb.BatchResponse {
				scanReq := ba.Requests[0].GetScan()
				require.NotNil(t, scanReq)
				require.Equal(t, lock.Exclusive, scanReq.KeyLockingStrength)
				resp := ba.CreateReply()
				resp.Txn = ba.Txn
				// Encode to BATCH_RESPONSE FORMAT.
				kvLen, _ := encKVLength(keyA, &valueA)
				repr := make([]byte, 0, kvLen)
				appendKV(repr, keyA, &valueA)
				resp.Responses[0].MustSetInner(&kvpb.ScanResponse{
					BatchResponses: [][]byte{repr},
				})
				return resp
			},
		},
		{
			name:           "ReplicatedLockingReverseScan",
			buffersLock:    false,
			buffersValue:   false,
			requiresValue:  true,
			isFullyCovered: false,
			generateRequest: func(t *testing.T, ba *kvpb.BatchRequest, txn *roachpb.Transaction) {
				ba.Add(&kvpb.ReverseScanRequest{
					KeyLockingStrength:   lock.Exclusive,
					KeyLockingDurability: lock.Replicated,
					ScanFormat:           kvpb.KEY_VALUES,
					RequestHeader:        kvpb.RequestHeader{Key: keyA, EndKey: keyC, Sequence: txn.Sequence},
				})
			},
			validateAndGenerateResponse: func(t *testing.T, ba *kvpb.BatchRequest) *kvpb.BatchResponse {
				scanReq := ba.Requests[0].GetReverseScan()
				require.NotNil(t, scanReq)
				require.Equal(t, lock.Exclusive, scanReq.KeyLockingStrength)

				resp := ba.CreateReply()
				resp.Txn = ba.Txn
				resp.Responses[0].MustSetInner(&kvpb.ReverseScanResponse{
					Rows: []roachpb.KeyValue{{Key: keyA, Value: valueA}},
				})
				return resp
			},
		},
		{
			name:           "ReplicatedLockingGet",
			buffersLock:    false,
			buffersValue:   false,
			requiresValue:  true,
			isFullyCovered: true,
			generateRequest: func(t *testing.T, ba *kvpb.BatchRequest, txn *roachpb.Transaction) {
				getReq := &kvpb.GetRequest{
					RequestHeader: kvpb.RequestHeader{Key: keyA, Sequence: txn.Sequence},
				}
				getReq.KeyLockingDurability = lock.Replicated
				getReq.KeyLockingStrength = lock.Exclusive
				ba.Add(getReq)
			},
			validateAndGenerateResponse: validateLockingGet,
		},
		{
			name:           "PutMustAcquireExclusive",
			buffersLock:    true,
			buffersValue:   true,
			requiresValue:  false,
			isFullyCovered: true,
			generateRequest: func(t *testing.T, ba *kvpb.BatchRequest, txn *roachpb.Transaction) {
				putReq := putArgs(keyA, valueAStr, txn.Sequence)
				putReq.MustAcquireExclusiveLock = true
				ba.Add(putReq)
			},
			validateAndGenerateResponse: validateLockingGet,
		},
		{
			name:         "DeleteMustAcquireExclusive",
			buffersLock:  true,
			buffersValue: true,
			// Delete requires a value to correctly return the number of deleted row.
			requiresValue:  true,
			isFullyCovered: true,
			generateRequest: func(t *testing.T, ba *kvpb.BatchRequest, txn *roachpb.Transaction) {
				delReq := delArgs(keyA, txn.Sequence)
				delReq.MustAcquireExclusiveLock = true
				ba.Add(delReq)
			},
			validateAndGenerateResponse: validateLockingGet,
		},
		{
			name:         "ConditionalPut",
			buffersLock:  true,
			buffersValue: true,
			// ConditionalPut requires a value to evaluate its expected bytes condition.
			requiresValue:  true,
			isFullyCovered: true,
			generateRequest: func(t *testing.T, ba *kvpb.BatchRequest, txn *roachpb.Transaction) {
				ba.Add(cputArgs(keyA, valueAStr, "", txn.Sequence))
			},
			// All of the current examples produce a buffered value of A. When issued
			// as a second request, assert this value matches if it exists.
			optionalGenSecondRequest: func(t *testing.T, ba *kvpb.BatchRequest, txn *roachpb.Transaction) {
				cput := cputArgs(keyA, valueAStr, string(valueA.TagAndDataBytes()), txn.Sequence)
				cput.AllowIfDoesNotExist = true
				ba.Add(cput)
			},
			validateAndGenerateResponse: validateLockingGet,
		},
	}

	for _, firstReq := range reqs {
		for _, secondReq := range reqs {
			t.Run(fmt.Sprintf("%s followed by %s", firstReq.name, secondReq.name), func(t *testing.T) {
				if !firstReq.buffersLock {
					skip.WithIssue(t, 142977, "%s does not buffer its lock", firstReq.name)
				}
				if secondReq.requiresValue && !firstReq.buffersValue {
					skip.WithIssue(t, 142977, "%s requires a value but %s does not buffer its response", firstReq.name, secondReq.name)
				}

				twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())
				txn := makeTxnProto()
				txn.Sequence = 10
				// Send first request and run firstRequest validation
				ba := &kvpb.BatchRequest{}
				ba.Header = kvpb.Header{Txn: &txn}
				firstReq.generateRequest(t, ba, &txn)

				mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
					resp := firstReq.validateAndGenerateResponse(t, ba)
					return resp, nil
				})

				br, pErr := twb.SendLocked(ctx, ba)
				require.NotNil(t, br)
				require.Nil(t, pErr)

				// Send second request and expect nothing to be sent.
				ba = &kvpb.BatchRequest{}
				txn.Sequence++
				ba.Header = kvpb.Header{Txn: &txn}
				if secondReq.optionalGenSecondRequest != nil {
					secondReq.optionalGenSecondRequest(t, ba, &txn)
				} else {
					secondReq.generateRequest(t, ba, &txn)
				}

				mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
					if secondReq.isFullyCovered {
						resp := ba.CreateReply()
						resp.Txn = ba.Txn
						return resp, nil
					} else {
						resp := secondReq.validateAndGenerateResponse(t, ba)
						return resp, nil
					}
				})

				expectedCalls := 0
				if !secondReq.isFullyCovered {
					expectedCalls = 1
				}
				numCalled := mockSender.NumCalled()
				br, pErr = twb.SendLocked(ctx, ba)
				require.Nil(t, pErr)
				require.NotNil(t, br)
				require.Len(t, br.Responses, 1)
				require.Equal(t, numCalled+expectedCalls, mockSender.NumCalled())
			})
		}
	}
}
