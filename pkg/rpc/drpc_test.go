// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"storj.io/drpc"
	"storj.io/drpc/drpcmigrate"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
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
func (d dummyStream) Kind() drpc.StreamKind                     { return drpc.StreamKindUnknown }

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
		md := metadata.New(map[string]string{
			gwRequestKey: "test",
		})
		// Create a context with the gateway metadata
		ctx := metadata.NewIncomingContext(context.Background(), md)

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
		// Create a context with the gateway metadata
		md := metadata.New(map[string]string{
			gwRequestKey: "test",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

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

// dropDRPCHeaderListener strips the DRPC migration header from accepted
// connections. This is needed for raw drpcserver instances that don't use
// CockroachDB's multiplexed listener.
type dropDRPCHeaderListener struct {
	net.Listener
}

func (ln *dropDRPCHeaderListener) Accept() (net.Conn, error) {
	conn, err := ln.Listener.Accept()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, len(drpcmigrate.DRPCHeader))
	if _, err := io.ReadFull(conn, buf); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// fakeHeartbeatServer implements DRPCHeartbeatServer with a configurable Pong
// value, allowing tests to identify which server handled a request.
type fakeHeartbeatServer struct {
	pong string
}

func (f *fakeHeartbeatServer) Ping(_ context.Context, req *PingRequest) (*PingResponse, error) {
	return &PingResponse{
		Pong:          f.pong,
		ServerTime:    timeutil.Now().UnixNano(),
		ServerVersion: clusterversion.Latest.Version(),
	}, nil
}

// startFakeDRPCServer starts a minimal dRPC server with a fake heartbeat
// service on the given TCP address. Returns the listener and a cleanup
// function that stops the server and waits for it to finish.
func startFakeDRPCServer(t *testing.T, addr string, pong string) (net.Listener, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	drpcL := &dropDRPCHeaderListener{Listener: ln}
	mux := drpcmux.New()
	srv := drpcserver.New(mux)
	require.NoError(t, DRPCRegisterHeartbeat(mux, &fakeHeartbeatServer{pong: pong}))

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = srv.Serve(context.Background(), drpcL)
	}()

	return ln, func() {
		_ = ln.Close()
		<-serveDone
	}
}

// TestDRPCPoolConnTransparentRedial demonstrates that the drpcpool underneath a
// peer connection can transparently re-dial to a different server after TCP port
// reuse, bypassing CockroachDB's heartbeat-based connection validation.
//
// This is the root cause of flakes like #168792: when a TestCluster tears down
// and another test's node binds to the same port, the surviving node's poolConn
// silently connects to the new node and delivers phantom raft messages.
func TestDRPCPoolConnTransparentRedial(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start server A on a random port.
	lnA, cleanupA := startFakeDRPCServer(t, "127.0.0.1:0", "server-A")
	addr := lnA.Addr().String()
	port := lnA.Addr().(*net.TCPAddr).Port

	// Create a client rpc.Context with a very long heartbeat interval. This
	// ensures no subsequent heartbeat fires between the server swap, isolating
	// the test to show that the poolConn re-dials without any re-validation.
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	opts := DefaultContextOptions()
	opts.Insecure = true
	opts.Clock = &timeutil.DefaultTimeSource{}
	opts.ToleratedOffset = time.Nanosecond
	opts.Stopper = stopper
	opts.Settings = cluster.MakeTestingClusterSettings()
	opts.RPCHeartbeatInterval = time.Hour
	opts.RPCHeartbeatTimeout = 10 * time.Second
	clientCtx := NewContext(ctx, opts)
	clientCtx.StorageClusterID.Set(ctx, uuid.MakeV4())
	clientCtx.NodeID.Set(ctx, 99)

	// Establish a peer connection. Connect() blocks until the initial heartbeat
	// succeeds, which validates the remote server's identity.
	conn, err := clientCtx.DRPCDialNode(
		addr, 1, roachpb.Locality{}, rpcbase.DefaultClass,
	).Connect(ctx)
	require.NoError(t, err)

	// Confirm we can reach server A.
	client := NewDRPCHeartbeatClient(conn)
	resp, err := client.Ping(ctx, &PingRequest{})
	require.NoError(t, err)
	require.Equal(t, "server-A", resp.Pong)

	// Kill server A. The underlying TCP connection will receive a FIN/RST.
	// The poolConn's Closed() channel does NOT fire (it only fires on explicit
	// Close), so the heartbeat loop remains blissfully unaware — it's waiting
	// on the 1-hour timer.
	cleanupA()

	// Start server B on the SAME port. This simulates another test's node
	// binding to a recently freed port.
	lnB, cleanupB := startFakeDRPCServer(
		t, fmt.Sprintf("127.0.0.1:%d", port), "server-B",
	)
	_ = lnB
	defer cleanupB()

	// Ping again on the SAME conn. The poolConn detects the cached TCP
	// connection is dead (via Closed() on the underlying drpcconn.Conn) and
	// transparently re-dials to port P — which is now server B. No heartbeat
	// validates this new connection.
	testutils.SucceedsSoon(t, func() error {
		resp, err := client.Ping(ctx, &PingRequest{})
		if err != nil {
			return err
		}
		if resp.Pong != "server-B" {
			return errors.Newf("got pong %q, want %q", resp.Pong, "server-B")
		}
		return nil
	})
}
