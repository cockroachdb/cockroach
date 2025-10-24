// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"crypto/tls"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"storj.io/drpc"
)

// dummyStream is a minimal implementation of drpc.Stream used for testing the
// stream interceptor.
type dummyStream struct {
	ctx context.Context
}

func (d dummyStream) Context() context.Context                  { return d.ctx }
func (d dummyStream) MsgSend(drpc.Message, drpc.Encoding) error { return nil }
func (d dummyStream) MsgRecv(drpc.Message, drpc.Encoding) error { return nil }
func (d dummyStream) CloseSend() error                          { return nil }
func (d dummyStream) Close() error                              { return nil }

// TestMakeStopperInterceptors verifies that the stopper interceptors allow RPCs
// to run before the stopper quiesces and reject them afterward.
func TestMakeStopperInterceptors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	rpcCtx := &Context{ContextOptions: ContextOptions{Stopper: stopper}}

	unaryInterceptor, streamInterceptor := makeStopperInterceptors(rpcCtx)

	// Before quiesce runs.
	called := false
	_, err := unaryInterceptor(ctx, nil, "test", func(ctx context.Context, req interface{}) (interface{}, error) {
		called = true
		return nil, nil
	})
	require.NoError(t, err)
	require.True(t, called)

	called = false
	_, err = streamInterceptor(dummyStream{ctx: ctx}, "test", func(stream drpc.Stream) (interface{}, error) {
		called = true
		return nil, nil
	})
	require.NoError(t, err)
	require.True(t, called)

	// After quiesce, RPCs are rejected.
	stopper.Quiesce(ctx)

	called = false
	_, err = unaryInterceptor(ctx, nil, "test", func(ctx context.Context, req interface{}) (interface{}, error) {
		called = true
		return nil, nil
	})
	require.ErrorIs(t, err, stop.ErrUnavailable)
	require.False(t, called)

	called = false
	_, err = streamInterceptor(dummyStream{ctx: ctx}, "test", func(stream drpc.Stream) (interface{}, error) {
		called = true
		return nil, nil
	})
	require.ErrorIs(t, err, stop.ErrUnavailable)
	require.False(t, called)
}

func TestGatewayRequestDRPCRecoveryInterceptor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// With gateway metadata - should recover from panic
	t.Run("with gateway metadata", func(t *testing.T) {
		ctx := MarkDRPCGatewayRequest(context.Background())

		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			panic("test panic")
		}

		resp, err := drpcGatewayRequestRecoveryInterceptor(ctx, nil, "test", handler)

		require.Nil(t, resp)
		require.ErrorContains(t, err, "unexpected error occurred")
	})

	// Without gateway metadata - should not recover from panic
	t.Run("without gateway metadata", func(t *testing.T) {
		ctx := context.Background()

		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			panic("test panic")
		}

		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic to propagate, got none")
			}
		}()

		_, _ = drpcGatewayRequestRecoveryInterceptor(ctx, nil, "test", handler)
	})

	// With gateway metadata but no panic - should pass through normally
	t.Run("with gateway metadata no panic", func(t *testing.T) {
		ctx := MarkDRPCGatewayRequest(context.Background())

		expectedResp := "success"
		expectedErr := errors.New("expected error")
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return expectedResp, expectedErr
		}

		resp, err := drpcGatewayRequestRecoveryInterceptor(ctx, nil, "test", handler)

		require.Equal(t, expectedResp, resp)
		require.ErrorIs(t, err, expectedErr)
	})
}

// TestDRPCServerWithInterceptor verifies that configured interceptors are
// invoked.
func TestDRPCServerWithInterceptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	loopbackL := netutil.NewLoopbackListener(ctx, stopper)

	serverCtx := newTestContext(uuid.MakeV4(), timeutil.NewManualTime(timeutil.Unix(0, 1)), 0, stopper)
	serverCtx.NodeID.Set(ctx, 1)
	serverCtx.SetLoopbackDRPCDialer(loopbackL.Connect)
	serverCtx.AdvertiseAddr = "127.0.0.1:1"
	serverCtx.RPCHeartbeatInterval = 1 * time.Hour

	var shouldBlock atomic.Bool
	blockedErr := errors.New("RPC blocked by interceptor")
	drpcServer, err := NewDRPCServer(ctx, serverCtx, WithInterceptor(func(rpcName string) error {
		if shouldBlock.Load() && rpcName == "/cockroach.rpc.Heartbeat/Ping" {
			return blockedErr
		}
		return nil
	}))
	require.NoError(t, err)
	require.NoError(t, DRPCRegisterHeartbeat(drpcServer, serverCtx.NewHeartbeatService()))

	tlsConfig, err := serverCtx.GetServerTLSConfig()
	require.NoError(t, err)
	tlsListener := tls.NewListener(loopbackL, tlsConfig)
	require.NoError(t, stopper.RunAsyncTask(ctx, "drpc-server", func(ctx context.Context) {
		netutil.FatalIfUnexpected(drpcServer.Serve(ctx, tlsListener))
	}))

	conn, err := serverCtx.DRPCDialNode("127.0.0.1:1", 1, roachpb.Locality{}, rpcbase.DefaultClass).Connect(ctx)
	require.NoError(t, err)
	client := NewDRPCHeartbeatClient(conn)
	shouldBlock.Store(true)
	_, err = client.Ping(ctx, &PingRequest{})
	require.Contains(t, err.Error(), "RPC blocked by interceptor")
}
