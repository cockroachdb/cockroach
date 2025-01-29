// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 0)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
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

		br := ba.CreateReply()
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

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 0)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

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

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 0)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

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

		br := ba.CreateReply()
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

// TestTxnWriteBufferBlindWritesIncludingOtherRequests tests that the txnWriteBuffer
// behaves correctly when a batch request contains both blind writes and other
// requests that will not be transformed. In the future, we may want to extend this
// test to include read-write requests as well.
func TestTxnWriteBufferBlindWritesIncludingOtherRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	twb, mockSender := makeMockTxnWriteBuffer()

	txn := makeTxnProto()
	txn.Sequence = 1
	keyA, keyB, keyC, keyD, keyE := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c"),
		roachpb.Key("d"), roachpb.Key("e")

	// Perform blind writes to keyA multiple times.
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

	// Verify the write was buffered correctly.
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

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
}
