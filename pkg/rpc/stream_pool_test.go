// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type mockBatchStreamConstructor struct {
	stream        BatchStreamClient
	streamErr     error
	streamCount   int
	lastStreamCtx *context.Context
}

func (m *mockBatchStreamConstructor) newStream(
	ctx context.Context, conn *grpc.ClientConn,
) (BatchStreamClient, error) {
	m.streamCount++
	if m.lastStreamCtx != nil {
		if m.streamCount != 1 {
			panic("unexpected stream creation with non-nil lastStreamCtx")
		}
		*m.lastStreamCtx = ctx
	}
	return m.stream, m.streamErr
}

func TestStreamPool_Basic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stream := NewMockBatchStreamClient(ctrl)
	stream.EXPECT().Send(gomock.Any()).Return(nil).Times(2)
	stream.EXPECT().Recv().Return(&kvpb.BatchResponse{}, nil).Times(2)
	conn := &mockBatchStreamConstructor{stream: stream}
	p := makeStreamPool(stopper, conn.newStream)
	p.Bind(ctx, new(grpc.ClientConn))
	defer p.Close()

	// Exercise the pool.
	resp, err := p.Send(ctx, &kvpb.BatchRequest{})
	require.NotNil(t, resp)
	require.NoError(t, err)
	require.Equal(t, 1, conn.streamCount)
	require.Equal(t, 1, stopper.NumTasks())
	require.Len(t, p.streams.s, 1)

	// Exercise the pool again. Should re-use the same connection.
	resp, err = p.Send(ctx, &kvpb.BatchRequest{})
	require.NotNil(t, resp)
	require.NoError(t, err)
	require.Equal(t, 1, conn.streamCount)
	require.Equal(t, 1, stopper.NumTasks())
	require.Len(t, p.streams.s, 1)
}

func TestStreamPool_Multi(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const num = 3
	sendC := make(chan struct{})
	recvC := make(chan struct{})
	stream := NewMockBatchStreamClient(ctrl)
	stream.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(_ *kvpb.BatchRequest) error {
			sendC <- struct{}{}
			return nil
		}).
		Times(num)
	stream.EXPECT().
		Recv().
		DoAndReturn(func() (*kvpb.BatchResponse, error) {
			<-recvC
			return &kvpb.BatchResponse{}, nil
		}).
		Times(num)
	conn := &mockBatchStreamConstructor{stream: stream}
	p := makeStreamPool(stopper, conn.newStream)
	p.Bind(ctx, new(grpc.ClientConn))
	defer p.Close()

	// Exercise the pool with concurrent requests, pausing between each one to
	// wait for its request to have been sent.
	var g errgroup.Group
	for range num {
		g.Go(func() error {
			_, err := p.Send(ctx, &kvpb.BatchRequest{})
			return err
		})
		<-sendC
	}

	// Assert that all requests have been sent and are waiting for responses. The
	// pool is empty at this point, as all streams are in use.
	require.Equal(t, num, conn.streamCount)
	require.Equal(t, num, stopper.NumTasks())
	require.Len(t, p.streams.s, 0)

	// Allow all requests to complete.
	for range num {
		recvC <- struct{}{}
	}
	require.NoError(t, g.Wait())

	// All three streams should be returned to the pool.
	require.Len(t, p.streams.s, num)
}

func TestStreamPool_SendBeforeBind(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stream := NewMockBatchStreamClient(ctrl)
	stream.EXPECT().Send(gomock.Any()).Times(0)
	stream.EXPECT().Recv().Times(0)
	conn := &mockBatchStreamConstructor{stream: stream}
	p := makeStreamPool(stopper, conn.newStream)

	// Exercise the pool before it is bound to a gRPC connection.
	resp, err := p.Send(ctx, &kvpb.BatchRequest{})
	require.Nil(t, resp)
	require.Error(t, err)
	require.Regexp(t, err, "streamPool not bound to a client conn")
	require.Equal(t, 0, conn.streamCount)
	require.Len(t, p.streams.s, 0)
}

func TestStreamPool_SendError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sendErr := errors.New("test error")
	stream := NewMockBatchStreamClient(ctrl)
	stream.EXPECT().Send(gomock.Any()).Return(sendErr).Times(1)
	stream.EXPECT().Recv().Times(0)
	conn := &mockBatchStreamConstructor{stream: stream}
	p := makeStreamPool(stopper, conn.newStream)
	p.Bind(ctx, new(grpc.ClientConn))

	// Exercise the pool and observe the error.
	resp, err := p.Send(ctx, &kvpb.BatchRequest{})
	require.Nil(t, resp)
	require.Error(t, err)
	require.ErrorIs(t, err, sendErr)

	// The stream should not be returned to the pool.
	require.Len(t, p.streams.s, 0)
}

func TestStreamPool_RecvError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	recvErr := errors.New("test error")
	stream := NewMockBatchStreamClient(ctrl)
	stream.EXPECT().Send(gomock.Any()).Return(nil).Times(1)
	stream.EXPECT().Recv().Return(nil, recvErr).Times(1)
	conn := &mockBatchStreamConstructor{stream: stream}
	p := makeStreamPool(stopper, conn.newStream)
	p.Bind(ctx, new(grpc.ClientConn))

	// Exercise the pool and observe the error.
	resp, err := p.Send(ctx, &kvpb.BatchRequest{})
	require.Nil(t, resp)
	require.Error(t, err)
	require.ErrorIs(t, err, recvErr)

	// The stream should not be returned to the pool.
	require.Len(t, p.streams.s, 0)
}

func TestStreamPool_NewStreamError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	streamErr := errors.New("test error")
	conn := &mockBatchStreamConstructor{streamErr: streamErr}
	p := makeStreamPool(stopper, conn.newStream)
	p.Bind(ctx, new(grpc.ClientConn))

	// Exercise the pool and observe the error.
	resp, err := p.Send(ctx, &kvpb.BatchRequest{})
	require.Nil(t, resp)
	require.Error(t, err)
	require.ErrorIs(t, err, streamErr)

	// The stream should not be placed in the pool.
	require.Len(t, p.streams.s, 0)
}

func TestStreamPool_Cancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sendC := make(chan struct{})
	var streamCtx context.Context
	stream := NewMockBatchStreamClient(ctrl)
	stream.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(_ *kvpb.BatchRequest) error {
			sendC <- struct{}{}
			return nil
		}).
		Times(1)
	stream.EXPECT().
		Recv().
		DoAndReturn(func() (*kvpb.BatchResponse, error) {
			<-streamCtx.Done()
			return nil, streamCtx.Err()
		}).
		Times(1)
	conn := &mockBatchStreamConstructor{stream: stream, lastStreamCtx: &streamCtx}
	p := makeStreamPool(stopper, conn.newStream)
	p.Bind(ctx, new(grpc.ClientConn))

	// Exercise the pool and observe the request get stuck.
	reqCtx, cancel := context.WithCancel(ctx)
	type result struct {
		resp *kvpb.BatchResponse
		err  error
	}
	resC := make(chan result)
	go func() {
		resp, err := p.Send(reqCtx, &kvpb.BatchRequest{})
		resC <- result{resp: resp, err: err}
	}()
	<-sendC
	select {
	case <-resC:
		t.Fatal("unexpected result")
	case <-time.After(10 * time.Millisecond):
	}

	// Cancel the request and observe the result.
	cancel()
	res := <-resC
	require.Nil(t, res.resp)
	require.Error(t, res.err)
	require.ErrorIs(t, res.err, context.Canceled)

	// The stream should not be returned to the pool.
	require.Len(t, p.streams.s, 0)
}

func TestStreamPool_Quiesce(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stream := NewMockBatchStreamClient(ctrl)
	stream.EXPECT().Send(gomock.Any()).Return(nil).Times(1)
	stream.EXPECT().Recv().Return(&kvpb.BatchResponse{}, nil).Times(1)
	conn := &mockBatchStreamConstructor{stream: stream}
	p := makeStreamPool(stopper, conn.newStream)
	stopperCtx, _ := stopper.WithCancelOnQuiesce(ctx)
	p.Bind(stopperCtx, new(grpc.ClientConn))

	// Exercise the pool to create a worker goroutine.
	resp, err := p.Send(ctx, &kvpb.BatchRequest{})
	require.NotNil(t, resp)
	require.NoError(t, err)
	require.Equal(t, 1, conn.streamCount)
	require.Equal(t, 1, stopper.NumTasks())
	require.Len(t, p.streams.s, 1)

	// Stop the stopper, which closes the pool.
	stopper.Stop(ctx)
	require.Len(t, p.streams.s, 0)
}

func TestStreamPool_QuiesceDuringSend(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sendC := make(chan struct{})
	var streamCtx context.Context
	stream := NewMockBatchStreamClient(ctrl)
	stream.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(_ *kvpb.BatchRequest) error {
			sendC <- struct{}{}
			return nil
		}).
		Times(1)
	stream.EXPECT().
		Recv().
		DoAndReturn(func() (*kvpb.BatchResponse, error) {
			<-streamCtx.Done()
			return nil, streamCtx.Err()
		}).
		Times(1)
	conn := &mockBatchStreamConstructor{stream: stream, lastStreamCtx: &streamCtx}
	p := makeStreamPool(stopper, conn.newStream)
	stopperCtx, _ := stopper.WithCancelOnQuiesce(ctx)
	p.Bind(stopperCtx, new(grpc.ClientConn))

	// Exercise the pool and observe the request get stuck.
	type result struct {
		resp *kvpb.BatchResponse
		err  error
	}
	resC := make(chan result)
	go func() {
		resp, err := p.Send(ctx, &kvpb.BatchRequest{})
		resC <- result{resp: resp, err: err}
	}()
	<-sendC
	select {
	case <-resC:
		t.Fatal("unexpected result")
	case <-time.After(10 * time.Millisecond):
	}

	// Stop the stopper, which cancels the request and closes the pool.
	stopper.Stop(ctx)
	require.Len(t, p.streams.s, 0)
	res := <-resC
	require.Nil(t, res.resp)
	require.Error(t, res.err)
	require.ErrorIs(t, res.err, context.Canceled)
}

func TestStreamPool_IdleTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stream := NewMockBatchStreamClient(ctrl)
	stream.EXPECT().Send(gomock.Any()).Return(nil).Times(1)
	stream.EXPECT().Recv().Return(&kvpb.BatchResponse{}, nil).Times(1)
	conn := &mockBatchStreamConstructor{stream: stream}
	p := makeStreamPool(stopper, conn.newStream)
	p.Bind(ctx, new(grpc.ClientConn))
	p.idleTimeout = 10 * time.Millisecond

	// Exercise the pool to create a worker goroutine.
	resp, err := p.Send(ctx, &kvpb.BatchRequest{})
	require.NotNil(t, resp)
	require.NoError(t, err)
	require.Equal(t, 1, conn.streamCount)

	// Eventually the worker should be stopped due to the idle timeout.
	testutils.SucceedsSoon(t, func() error {
		if stopper.NumTasks() != 0 {
			return errors.New("worker not stopped")
		}
		return nil
	})

	// Once the worker is stopped, the pool should be empty.
	require.Len(t, p.streams.s, 0)
}
