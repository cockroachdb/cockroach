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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func makeMockTxnWriteBuffer(st *cluster.Settings) (txnWriteBuffer, *mockLockedSender) {
	mockSender := &mockLockedSender{}
	return txnWriteBuffer{
		enabled: true,
		wrapped: mockSender,
		st:      st,
	}, mockSender
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
	ba.Add(putA)
	ba.Add(getB)
	ba.Add(delC)
	ba.Add(scanDE)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.ScanRequest{}, ba.Requests[1].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Expect 4 responses, even though only 2 KV requests were sent. Moreover,
	// ensure that the responses are in the correct order.
	require.Len(t, br.Responses, 4)
	require.Equal(t, br.Responses[0].GetInner(), &kvpb.PutResponse{})
	require.Equal(t, br.Responses[1].GetInner(), &kvpb.GetResponse{})
	require.Equal(t, br.Responses[2].GetInner(), &kvpb.DeleteResponse{})
	require.Equal(t, br.Responses[3].GetInner(), &kvpb.ScanResponse{})

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
//
// TODO(arul): extend this test to transformations as well once we start
// transforming read-write requests into separate bits.
func TestTxnWriteBufferCorrectlyAdjustsErrorsAfterBuffering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for errIdx, resErrIdx := range map[int32]int32{
		0: 1, // points to the GetB
		1: 4, // points to the GetE
		2: 5, // points to the GetF
	} {
		t.Run(fmt.Sprintf("errIdx=%d", errIdx), func(t *testing.T) {
			ctx := context.Background()
			twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

			txn := makeTxnProto()
			txn.Sequence = 1
			keyA, keyB, keyC, keyD, keyE, keyF := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c"),
				roachpb.Key("d"), roachpb.Key("e"), roachpb.Key("f")

			// Construct a batch request where some of the requests will be buffered.
			ba := &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: &txn}
			putA := putArgs(keyA, "val1", txn.Sequence)
			getB := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}}
			delC := delArgs(keyC, txn.Sequence)
			putD := putArgs(keyD, "val2", txn.Sequence)
			getE := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyE}}
			getF := &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyF}}

			ba.Add(putA)
			ba.Add(getB)
			ba.Add(delC)
			ba.Add(putD)
			ba.Add(getE)
			ba.Add(getF)

			mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 3)
				require.IsType(t, &kvpb.GetRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &kvpb.GetRequest{}, ba.Requests[1].GetInner())
				require.IsType(t, &kvpb.GetRequest{}, ba.Requests[2].GetInner())

				pErr := kvpb.NewErrorWithTxn(errors.New("boom"), &txn)
				pErr.SetErrorIndex(errIdx)
				return nil, pErr
			})

			br, pErr := twb.SendLocked(ctx, ba)
			require.Nil(t, br)
			require.NotNil(t, pErr)
			require.Equal(t, &txn, pErr.GetTxn())

			require.NotNil(t, pErr.Index)
			require.Equal(t, resErrIdx, pErr.Index.Index)

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

			return ba.CreateReply(), nil
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
		return ba.CreateReply(), nil
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
		return ba.CreateReply(), nil
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
}

// TestTxnWriteBufferDeleteRange ensures that the txnWriteBuffer correctly
// handles DeleteRange requests. In particular, whenever we see a batch with a
// DeleteRange request, the write buffer is flushed and write buffering is
// turned off for subsequent requests.
func TestTxnWriteBufferDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer(cluster.MakeClusterSettings())

	txn := makeTxnProto()
	txn.Sequence = 10
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
	valA := "valA"

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

	// Send a DeleteRange request. This should result in the entire buffer
	// being flushed. Note that we're flushing the delete to key C as well, even
	// though it doesn't overlap with the DeleteRange request.
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	delRange := delRangeArgs(keyA, keyB, txn.Sequence)
	ba.Add(delRange)

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 3)

		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.DeleteRequest{}, ba.Requests[1].GetInner())
		require.IsType(t, &kvpb.DeleteRangeRequest{}, ba.Requests[2].GetInner())

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	// Even though we flushed some writes, it shouldn't make it back to the response.
	require.Len(t, br.Responses, 1)
	require.IsType(t, &kvpb.DeleteRangeResponse{}, br.Responses[0].GetInner())

	// Ensure the buffer is empty at this point.
	require.Equal(t, 0, len(twb.testingBufferedWritesAsSlice()))

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
			name: "batch with OriginTimestamp",
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
			name: "batch with OriginID",
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
			name: "batch with InitPut",
			ba: func() *kvpb.BatchRequest {
				b := &kvpb.BatchRequest{Header: kvpb.Header{Txn: &txn}}
				b.Add(&kvpb.InitPutRequest{
					RequestHeader: kvpb.RequestHeader{Key: keyA, Sequence: txn.Sequence},
					Value:         roachpb.Value{},
				})
				return b
			},
		},
		{
			name: "batch with Increment",
			ba: func() *kvpb.BatchRequest {
				b := &kvpb.BatchRequest{Header: kvpb.Header{Txn: &txn}}
				b.Add(&kvpb.IncrementRequest{
					RequestHeader: kvpb.RequestHeader{Key: keyA, Sequence: txn.Sequence},
				})
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
