// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

type callbackFlowStreamServer struct {
	server flowStreamServer
	sendCb func()
	recvCb func()
}

func (s callbackFlowStreamServer) Send(cs *distsqlpb.ConsumerSignal) error {
	if s.sendCb != nil {
		s.sendCb()
	}
	return s.server.Send(cs)
}

func (s callbackFlowStreamServer) Recv() (*distsqlpb.ProducerMessage, error) {
	if s.recvCb != nil {
		s.recvCb()
	}
	return s.server.Recv()
}

var _ flowStreamServer = callbackFlowStreamServer{}

func TestInboxCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	typs := []types.T{types.Int64}
	t.Run("ReaderWaitingForStreamHandler", func(t *testing.T) {
		inbox, err := NewInbox(typs)
		require.NoError(t, err)
		ctx, cancelFn := context.WithCancel(context.Background())
		// Cancel the context.
		cancelFn()
		// Next should not block if the context is canceled.
		err = exec.CatchVectorizedRuntimeError(func() { inbox.Next(ctx) })
		require.True(t, testutils.IsError(err, "context canceled"), err)
		// Now, the remote stream arrives.
		err = inbox.RunWithStream(context.Background(), mockFlowStreamServer{})
		require.True(t, testutils.IsError(err, "while waiting for stream"), err)
	})

	t.Run("DuringRecv", func(t *testing.T) {
		rpcLayer := makeMockFlowStreamRPCLayer()
		inbox, err := NewInbox(typs)
		require.NoError(t, err)
		ctx, cancelFn := context.WithCancel(context.Background())

		// Setup reader and stream.
		go func() {
			inbox.Next(ctx)
		}()
		recvCalled := make(chan struct{})
		streamHandlerErrCh := handleStream(context.Background(), inbox, callbackFlowStreamServer{
			server: rpcLayer.server,
			recvCb: func() {
				recvCalled <- struct{}{}
			},
		}, func() { close(rpcLayer.server.csChan) })

		// Now wait for the Inbox to call Recv on the stream.
		<-recvCalled

		// Cancel the context.
		cancelFn()
		err = <-streamHandlerErrCh
		require.True(t, testutils.IsError(err, "readerCtx in Inbox stream handler"), err)

		// The mock RPC layer does not unblock the Recv for us on the server side,
		// so manually send an io.EOF to the reader goroutine.
		close(rpcLayer.server.pmChan)
	})

	t.Run("StreamHandlerWaitingForReader", func(t *testing.T) {
		rpcLayer := makeMockFlowStreamRPCLayer()
		inbox, err := NewInbox(typs)
		require.NoError(t, err)

		ctx, cancelFn := context.WithCancel(context.Background())

		cancelFn()
		// A stream arrives but there is no reader.
		err = <-handleStream(ctx, inbox, rpcLayer.server, func() { close(rpcLayer.client.csChan) })
		require.True(t, testutils.IsError(err, "while waiting for reader"), err)
	})
}

// TestInboxNextPanicDoesntLeakGoroutines verifies that goroutines that are
// spawned as part of an Inbox's normal operation are cleaned up even on a
// panic.
func TestInboxNextPanicDoesntLeakGoroutines(t *testing.T) {
	defer leaktest.AfterTest(t)()

	inbox, err := NewInbox([]types.T{types.Int64})
	require.NoError(t, err)

	rpcLayer := makeMockFlowStreamRPCLayer()
	streamHandlerErrCh := handleStream(context.Background(), inbox, rpcLayer.server, func() { close(rpcLayer.client.csChan) })

	m := &distsqlpb.ProducerMessage{}
	m.Data.RawBytes = []byte("garbage")

	go func() {
		_ = rpcLayer.client.Send(m)
	}()

	// inbox.Next should panic given that the deserializer will encounter garbage
	// data.
	require.Panics(t, func() { inbox.Next(context.Background()) })

	// We require no error from the stream handler as nothing was canceled. The
	// panic is bubbled up through the Next chain on the Inbox's host.
	require.NoError(t, <-streamHandlerErrCh)
}

func TestInboxTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	inbox, err := NewInbox([]types.T{types.Int64})
	require.NoError(t, err)

	var (
		ctx         = context.Background()
		readerErrCh = make(chan error)
		rpcLayer    = makeMockFlowStreamRPCLayer()
	)
	go func() {
		readerErrCh <- exec.CatchVectorizedRuntimeError(func() { inbox.Next(ctx) })
	}()

	// Timeout the inbox.
	const timeoutErr = "timeout error"
	inbox.Timeout(errors.New(timeoutErr))

	// Ensure that the reader gets the error.
	readerErr := <-readerErrCh
	require.True(t, testutils.IsError(readerErr, timeoutErr), readerErr)

	// And now the stream arrives.
	streamHandlerErrCh := handleStream(ctx, inbox, rpcLayer.server, nil /* doneFn */)
	streamErr := <-streamHandlerErrCh
	require.True(t, testutils.IsError(streamErr, "stream arrived too late"), streamErr)
}
