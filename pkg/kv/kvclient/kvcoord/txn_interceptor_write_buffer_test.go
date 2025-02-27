// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func makeMockTxnWriteBuffer() (txnWriteBuffer, *mockLockedSender) {
	mockSender := &mockLockedSender{}
	return txnWriteBuffer{
		enabled: true,
		wrapped: mockSender,
	}, mockSender
}

func putArgs(key roachpb.Key, value string, seq enginepb.TxnSeq) *kvpb.PutRequest {
	return &kvpb.PutRequest{
		RequestHeader: kvpb.RequestHeader{Key: key, Sequence: seq},
		Value:         roachpb.MakeValueFromString(value),
	}
}

func delArgs(key roachpb.Key, seq enginepb.TxnSeq) *kvpb.DeleteRequest {
	return &kvpb.DeleteRequest{
		RequestHeader: kvpb.RequestHeader{Key: key, Sequence: seq},
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
	twb, mockSender := makeMockTxnWriteBuffer()

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
	twb, mockSender := makeMockTxnWriteBuffer()

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
	twb, mockSender := makeMockTxnWriteBuffer()

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

	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer()

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

	for errIdx, resErrIdx := range map[int32]int32{
		0: -1, // points to the Put; -1 to denote nil
		1: -1, // points to the Del; -1 to denote nil
		2: 0,  // points to the Get
		3: 1,  // points to the EndTxn
	} {
		t.Run(fmt.Sprintf("errIdx=%d", errIdx), func(t *testing.T) {
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

			br, pErr := twb.SendLocked(ctx, ba)
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

	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer()

	txn := makeTxnProto()
	txn.Sequence = 1
	keyA, keyB, keyC, keyD, keyE, keyF := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c"),
		roachpb.Key("d"), roachpb.Key("e"), roachpb.Key("f")

	for errIdx, resErrIdx := range map[int32]int32{
		0: 1, // points to the GetB
		1: 4, // points to the GetE
		2: 5, // points to the GetF
	} {
		t.Run(fmt.Sprintf("errIdx=%d", errIdx), func(t *testing.T) {
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
		})

		// Finish off the test by commiting the transaction and sanity checking the
		// buffer is flushed as expected.
		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: &txn}
		ba.Add(&kvpb.EndTxnRequest{Commit: true})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 4)

			// We now expect the buffer to be flushed along with the commit.
			require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
			require.IsType(t, &kvpb.DeleteRequest{}, ba.Requests[1].GetInner())
			require.IsType(t, &kvpb.PutRequest{}, ba.Requests[2].GetInner())
			require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[3].GetInner())

			br := ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})

		br, pErr := twb.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
	}
}

// TestTxnWriteBufferServesPointReadsLocally ensures that point reads hoping to
// do read-your-own-writes are correctly served from the buffer.
func TestTxnWriteBufferServesPointReadsLocally(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer()

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

	// Even though we flushed the buffer, responses from the blind writes should
	// not be returned.
	require.Len(t, br.Responses, 1)
	require.IsType(t, &kvpb.EndTxnResponse{}, br.Responses[0].GetInner())
}

// TestTxnWriteBufferServesOverlappingReadsCorrectly ensures that Scan requests
// that overlap with buffered writes are correctly served from the buffer.
func TestTxnWriteBufferServesOverlappingReadsCorrectly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer()

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
		scan := &kvpb.ScanRequest{
			RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyC, Sequence: txn.Sequence},
			ScanFormat:    kvpb.KEY_VALUES,
		}
		ba.Add(scan)

		numCalled = mockSender.NumCalled()
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
		// There should only be a single response, for Key A, as Key B was deleted.
		require.Len(t, br.Responses[0].GetInner().(*kvpb.ScanResponse).Rows, 1)
		require.Equal(t, keyA, br.Responses[0].GetInner().(*kvpb.ScanResponse).Rows[0].Key)
		require.Equal(t, roachpb.MakeValueFromString(expVal), br.Responses[0].GetInner().(*kvpb.ScanResponse).Rows[0].Value)
		// Assert that the request was sent to the KV layer.
		require.Equal(t, mockSender.NumCalled(), numCalled+1)
	}

	// Perform a scan at a lower sequence number than the minimum buffered write.
	// This should be sent to the KV layer, like above, but the result shouldn't
	// include any buffered writes.
	txn.Sequence = 9
	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	scan := &kvpb.ScanRequest{
		RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyC, Sequence: txn.Sequence},
		ScanFormat:    kvpb.KEY_VALUES,
	}
	ba.Add(scan)

	numCalled = mockSender.NumCalled()
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
	// Assert that no buffered write was returned.
	require.Len(t, br.Responses[0].GetInner().(*kvpb.ScanResponse).Rows, 0)
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
	twb, mockSender := makeMockTxnWriteBuffer()

	txn := makeTxnProto()
	txn.Sequence = 10
	keyA := roachpb.Key("a")
	valA := "val"

	// Blindly write to keys A.
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
