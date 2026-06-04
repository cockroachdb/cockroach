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
	"strings"
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
	"storj.io/drpc/drpcclient"
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

// TestDRPCValidateOnDial verifies that validateOnDial rejects freshly
// dialed drpc conns whose remote cluster identity (cluster ID, node ID)
// does not match what the client expects, and accepts conns that match.
func TestDRPCValidateOnDial(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	const serverNodeID roachpb.NodeID = 7
	const advertiseAddr = "127.0.0.1:1"
	serverClusterID := uuid.MakeV4()
	clock := timeutil.NewManualTime(timeutil.Unix(0, 1))

	loopbackL := netutil.NewLoopbackListener(ctx, stopper)
	serverCtx := newTestContext(serverClusterID, clock, 0, stopper)
	serverCtx.NodeID.Set(ctx, serverNodeID)
	serverCtx.SetLoopbackDRPCDialer(loopbackL.Connect)
	serverCtx.AdvertiseAddr = advertiseAddr

	drpcServer, err := NewDRPCServer(ctx, serverCtx)
	require.NoError(t, err)
	require.NoError(t, DRPCRegisterHeartbeat(drpcServer, serverCtx.NewHeartbeatService()))

	tlsConfig, err := serverCtx.GetServerTLSConfig()
	require.NoError(t, err)
	tlsListener := tls.NewListener(loopbackL, tlsConfig)
	require.NoError(t, stopper.RunAsyncTask(ctx, "drpc-server", func(ctx context.Context) {
		netutil.FatalIfUnexpected(drpcServer.Serve(ctx, tlsListener))
	}))

	// newClientCtx returns an rpcCtx that dials the test server via the
	// shared loopback listener. clusterID controls which cluster identity
	// the client claims, allowing tests to exercise mismatched-identity
	// cases.
	newClientCtx := func(clusterID uuid.UUID) *Context {
		c := newTestContext(clusterID, clock, 0, stopper)
		c.SetLoopbackDRPCDialer(loopbackL.Connect)
		c.AdvertiseAddr = advertiseAddr
		return c
	}

	// dialAndValidate bare-dials the server through the loopback listener
	// and runs validateOnDial with the provided expected NodeID, returning
	// the validation error.
	dialAndValidate := func(t *testing.T, clientCtx *Context, expectedNodeID roachpb.NodeID) error {
		opts, err := clientCtx.drpcDialOptionsInternal(ctx, advertiseAddr, rpcbase.DefaultClass, loopbackTransport)
		require.NoError(t, err)
		conn, err := drpcclient.DialContext(ctx, advertiseAddr, opts...)
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()
		return validateOnDial(ctx, clientCtx, conn, advertiseAddr, expectedNodeID, rpcbase.DefaultClass, opts)
	}

	tests := []struct {
		name        string
		clientUUID  uuid.UUID
		nodeID      roachpb.NodeID
		expectedErr string
	}{
		{
			name:       "matching identity",
			clientUUID: serverClusterID,
			nodeID:     serverNodeID,
		},
		{
			name:        "mismatched node ID",
			clientUUID:  serverClusterID,
			nodeID:      serverNodeID + 1,
			expectedErr: "node ID",
		},
		{
			name:        "mismatched cluster ID",
			clientUUID:  uuid.MakeV4(),
			nodeID:      serverNodeID,
			expectedErr: "cluster ID",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := dialAndValidate(t, newClientCtx(tc.clientUUID), tc.nodeID)
			if tc.expectedErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.expectedErr)
		})
	}
}

// dropDRPCHeaderListener strips the DRPC migration header from accepted
// connections. Production servers strip the header inside cmux; raw
// drpcserver instances used in tests do not, so connections from a real
// drpc client (which prefixes the header via drpcmigrate.DialWithHeader)
// would otherwise fail at the protocol layer.
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
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// fakeHeartbeatServer is a minimal HeartbeatServer that lets a test
// pin a remote identity (cluster name) and an echoed Pong value
// without standing up a full HeartbeatService.
type fakeHeartbeatServer struct {
	pong        string
	clusterName string
}

func (f *fakeHeartbeatServer) Ping(_ context.Context, req *PingRequest) (*PingResponse, error) {
	return &PingResponse{
		Pong:          f.pong,
		ServerTime:    timeutil.Now().UnixNano(),
		ServerVersion: clusterversion.Latest.Version(),
		ClusterName:   f.clusterName,
	}, nil
}

// startFakeDRPCServer starts a minimal insecure dRPC server with a fake
// heartbeat service on the given TCP address. Returns the listener and a
// cleanup function that stops the server and waits for it to exit.
func startFakeDRPCServer(
	t *testing.T, addr string, pong string, clusterName string,
) (net.Listener, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	mux := drpcmux.New()
	require.NoError(t, DRPCRegisterHeartbeat(mux, &fakeHeartbeatServer{
		pong: pong, clusterName: clusterName,
	}))
	srv := drpcserver.New(mux)

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = srv.Serve(context.Background(), &dropDRPCHeaderListener{Listener: ln})
	}()
	return ln, func() {
		_ = ln.Close()
		<-serveDone
	}
}

// TestDRPCPoolConnRevalidatesAfterPortReuse is the regression test for
// #168792. It exercises the exact race that produced "phantom" raft
// messages in CI: a peer connection is established to server A; A is
// killed; another process binds to the same TCP port; the next RPC on
// the existing peer conn forces drpcpool to re-dial. With dial-time
// validation, the new conn fails its identity handshake against the
// expected cluster and is rejected before any RPC can ride it.
//
// Adapted from the reproducer in #169068. The original test asserted
// the buggy behavior (silent re-dial succeeding); this version asserts
// the fix (re-dial blocked at the cluster-identity handshake).
func TestDRPCPoolConnRevalidatesAfterPortReuse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const clusterA = "cluster-A"
	const clusterB = "cluster-B"

	// Server A listens on a random port. The client expects to be
	// talking to clusterA throughout the test.
	lnA, cleanupA := startFakeDRPCServer(t, "127.0.0.1:0", "server-A", clusterA)
	addr := lnA.Addr().String()
	port := lnA.Addr().(*net.TCPAddr).Port

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// The very long heartbeat interval ensures no peer-level Ping fires
	// between the server swap and the next RPC on the cached conn,
	// isolating the test to dial-time validation.
	opts := DefaultContextOptions()
	opts.Insecure = true
	opts.Clock = &timeutil.DefaultTimeSource{}
	opts.ToleratedOffset = time.Nanosecond
	opts.Stopper = stopper
	opts.Settings = cluster.MakeTestingClusterSettings()
	opts.RPCHeartbeatInterval = time.Hour
	opts.RPCHeartbeatTimeout = 10 * time.Second
	opts.ClusterName = clusterA
	clientCtx := NewContext(ctx, opts)
	clientCtx.StorageClusterID.Set(ctx, uuid.MakeV4())
	clientCtx.NodeID.Set(ctx, 99)

	// Connect to server A. The peer-level initial heartbeat validates
	// identity once.
	conn, err := clientCtx.DRPCDialNode(
		addr, 1, roachpb.Locality{}, rpcbase.DefaultClass,
	).Connect(ctx)
	require.NoError(t, err)

	client := NewDRPCHeartbeatClient(conn)
	resp, err := client.Ping(ctx, &PingRequest{})
	require.NoError(t, err)
	require.Equal(t, "server-A", resp.Pong)

	// Kill server A. The underlying TCP conn dies but drpcpool's wrapper
	// stays "open" from the peer layer's point of view, so connClosedCh
	// (peer.go:556) does not fire.
	cleanupA()

	// Bind server B to the freed port. This is the production hazard:
	// some other process grabs the IP:port the original peer was using.
	_, cleanupB := startFakeDRPCServer(
		t, fmt.Sprintf("127.0.0.1:%d", port), "server-B", clusterB,
	)
	defer cleanupB()

	// The next Ping on the existing peer conn forces drpcpool to evict
	// the dead cached conn and dial fresh. validateOnDial runs against
	// server B, sees the cluster name does not match clusterA, and
	// rejects the conn. The Ping surfaces that error rather than
	// silently returning "server-B".
	testutils.SucceedsSoon(t, func() error {
		resp, err := client.Ping(ctx, &PingRequest{})
		if err == nil {
			return errors.Newf("expected dial-time validation to reject the re-dial, got pong %q", resp.Pong)
		}
		if !strings.Contains(err.Error(), "cluster name") {
			return errors.Wrapf(err, "unexpected error shape; want cluster-name rejection")
		}
		return nil
	})
}
