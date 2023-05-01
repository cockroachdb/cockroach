// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestingConnHealth returns nil if we have an open connection to the given
// target with DefaultClass that succeeded on its most recent heartbeat.
// Otherwise, it kicks off a connection attempt (unless one is already in
// progress or we are in a backoff state) and returns an error (typically
// ErrNotHeartbeated). This is a conservative/pessimistic indicator:
// if we have not attempted to talk to the target node recently, an
// error will be returned. This method should therefore be used to
// prioritize among a list of candidate nodes, but not to filter out
// "unhealthy" nodes.
//
// This is used in tests only; in clusters use (*Dialer).ConnHealth()
// instead which automates the address resolution.
//
// TODO(knz): remove this altogether. Use the dialer in all cases.
func (rpcCtx *Context) TestingConnHealth(target string, nodeID roachpb.NodeID) error {
	if rpcCtx.GetLocalInternalClientForAddr(nodeID) != nil {
		// The local server is always considered healthy.
		return nil
	}
	conn := rpcCtx.GRPCDialNode(target, nodeID, DefaultClass)
	return conn.Health()
}

// AddTestingDialOpts adds extra dialing options to the rpc Context. This should
// be done before GRPCDial is called.
func (rpcCtx *Context) AddTestingDialOpts(opts ...grpc.DialOption) {
	rpcCtx.testingDialOpts = append(rpcCtx.testingDialOpts, opts...)
}

func newTestServer(t testing.TB, ctx *Context, extraOpts ...grpc.ServerOption) *grpc.Server {
	tlsConfig, err := ctx.GetServerTLSConfig()
	require.NoError(t, err)
	opts := []grpc.ServerOption{
		grpc.Creds(credentials.NewTLS(tlsConfig)),
	}
	opts = append(opts, extraOpts...)
	return grpc.NewServer(opts...)
}

func newTestContextWithKnobs(
	clock hlc.WallClock, maxOffset time.Duration, stopper *stop.Stopper, knobs ContextTestingKnobs,
) *Context {
	return NewContext(context.Background(), ContextOptions{
		TenantID:        roachpb.SystemTenantID,
		Config:          testutils.NewNodeTestBaseContext(),
		Clock:           clock,
		ToleratedOffset: maxOffset,
		Stopper:         stopper,
		Settings:        cluster.MakeTestingClusterSettings(),
		Knobs:           knobs,
	})
}

func newTestContext(
	storageClusterID uuid.UUID, clock hlc.WallClock, maxOffset time.Duration, stopper *stop.Stopper,
) *Context {
	return newTestContextWithKnobs(clock, maxOffset, stopper, ContextTestingKnobs{
		StorageClusterID: &storageClusterID,
	})
}

func TestHeartbeatCB(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "compression", func(t *testing.T, compression bool) {
		stopper := stop.NewStopper()
		defer stopper.Stop(context.Background())

		// Shared cluster ID by all RPC peers (this ensures that the peers
		// don't talk to servers from unrelated tests by accident).
		clusterID := uuid.MakeV4()

		clock := timeutil.NewManualTime(timeutil.Unix(0, 20))
		maxOffset := time.Duration(0)
		serverCtx := newTestContext(clusterID, clock, maxOffset, stopper)
		serverCtx.rpcCompression = compression
		const serverNodeID = 1
		serverCtx.NodeID.Set(context.Background(), serverNodeID)
		s := newTestServer(t, serverCtx)
		RegisterHeartbeatServer(s, &HeartbeatService{
			clock:              clock,
			remoteClockMonitor: serverCtx.RemoteClocks,
			clusterID:          serverCtx.StorageClusterID,
			nodeID:             serverCtx.NodeID,
			version:            serverCtx.Settings.Version,
		})

		ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
		if err != nil {
			t.Fatal(err)
		}
		remoteAddr := ln.Addr().String()

		// Clocks don't matter in this test.
		clientCtx := newTestContext(clusterID, clock, maxOffset, stopper)
		clientCtx.rpcCompression = compression

		var once sync.Once
		ch := make(chan struct{})

		clientCtx.HeartbeatCB = func() {
			once.Do(func() {
				close(ch)
			})
		}

		if _, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, DefaultClass).Connect(context.Background()); err != nil {
			t.Fatal(err)
		}

		<-ch
	})
}

// TestPingInterceptors checks that OnOutgoingPing and OnIncomingPing can inject errors.
func TestPingInterceptors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // avoid hang on failure
	defer cancel()

	const (
		blockedTargetNodeID = 5
		blockedOriginNodeID = 123
	)

	errBoomSend := errors.Handled(errors.New("boom due to onSendPing"))
	recvMsg := "boom due to onHandlePing"
	errBoomRecv := status.Error(codes.FailedPrecondition, recvMsg)
	opts := ContextOptions{
		TenantID:        roachpb.SystemTenantID,
		Config:          testutils.NewNodeTestBaseContext(),
		Clock:           &timeutil.DefaultTimeSource{},
		ToleratedOffset: 500 * time.Millisecond,
		Stopper:         stop.NewStopper(),
		Settings:        cluster.MakeTestingClusterSettings(),
		OnOutgoingPing: func(ctx context.Context, req *PingRequest) error {
			if req.TargetNodeID == blockedTargetNodeID {
				return errBoomSend
			}
			return nil
		},
		OnIncomingPing: func(ctx context.Context, req *PingRequest, resp *PingResponse) error {
			if req.OriginNodeID == blockedOriginNodeID {
				return errBoomRecv
			}
			return nil
		},
	}
	defer opts.Stopper.Stop(ctx)

	rpcCtx := NewContext(ctx, opts)
	{
		_, err := rpcCtx.GRPCDialNode("unused:1234", 5, SystemClass).Connect(ctx)
		require.Equal(t, errBoomSend, errors.Cause(err))
	}

	s := newTestServer(t, rpcCtx)
	RegisterHeartbeatServer(s, rpcCtx.NewHeartbeatService())
	rpcCtx.NodeID.Set(ctx, blockedOriginNodeID)
	ln, err := netutil.ListenAndServeGRPC(rpcCtx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	remoteAddr := ln.Addr().String()
	{
		_, err := rpcCtx.GRPCDialNode(remoteAddr, blockedOriginNodeID, SystemClass).Connect(ctx)
		require.True(t, errors.HasType(err, errBoomRecv))
		status, ok := status.FromError(errors.UnwrapAll(err))
		require.True(t, ok)
		require.Equal(t, codes.FailedPrecondition, status.Code())
		require.Equal(t, recvMsg, status.Message())
	}
}

// TestClockOffsetInPingRequest ensures that all ping requests
// after the first one have a non-zero offset.
// (Regression test for issue #84027.)
func TestClockOffsetInPingRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testClockOffsetInPingRequestInternal(t, false /* clientOnly */)
}

// TestClockOffsetInclientPingRequest ensures that all ping requests
// have a zero offset and there is no Remotelocks to update.
// (Regression test for issue #84017.)
func TestClockOffsetInClientPingRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testClockOffsetInPingRequestInternal(t, true /* clientOnly */)
}

func testClockOffsetInPingRequestInternal(t *testing.T, clientOnly bool) {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	pings := make(chan PingRequest, 5)
	done := make(chan struct{})
	defer func() { close(done) }()

	// Build a minimal server.
	opts := ContextOptions{
		TenantID:        roachpb.SystemTenantID,
		Config:          testutils.NewNodeTestBaseContext(),
		Clock:           &timeutil.DefaultTimeSource{},
		ToleratedOffset: 500 * time.Millisecond,
		Stopper:         stopper,
		Settings:        cluster.MakeTestingClusterSettings(),
	}
	rpcCtxServer := NewContext(ctx, opts)

	clientOpts := opts
	clientOpts.Config = testutils.NewNodeTestBaseContext()
	// Experimentally, values below 50ms seem to incur flakiness.
	clientOpts.Config.RPCHeartbeatInterval = 100 * time.Millisecond
	clientOpts.Config.RPCHeartbeatTimeout = 100 * time.Millisecond
	clientOpts.ClientOnly = clientOnly
	clientOpts.OnOutgoingPing = func(ctx context.Context, req *PingRequest) error {
		select {
		case <-done:
		case pings <- *req:
		}
		return nil
	}
	rpcCtxClient := NewContext(ctx, clientOpts)

	require.NotNil(t, rpcCtxServer.RemoteClocks)
	if !clientOnly {
		require.NotNil(t, rpcCtxClient.RemoteClocks)
	} else {
		require.Nil(t, rpcCtxClient.RemoteClocks)
	}

	t.Logf("server listen")
	s := newTestServer(t, rpcCtxServer)
	RegisterHeartbeatServer(s, rpcCtxServer.NewHeartbeatService())
	rpcCtxServer.NodeID.Set(ctx, 1)
	ln, err := netutil.ListenAndServeGRPC(stopper, s, util.TestAddr)
	require.NoError(t, err)

	t.Logf("client dial")
	// Dial: this causes the heartbeats to start.
	remoteAddr := ln.Addr().String()
	_, err = rpcCtxClient.GRPCDialNode(remoteAddr, 1, SystemClass).Connect(ctx)
	require.NoError(t, err)

	t.Logf("first ping check")
	firstPing := <-pings
	require.Zero(t, firstPing.Offset.Offset)
	require.Zero(t, firstPing.Offset.Uncertainty)
	require.Zero(t, firstPing.Offset.MeasuredAt)
	for i := 1; i < 3; i++ {
		t.Logf("ping %d check", i)
		nextPing := <-pings
		if !clientOnly {
			require.NotZero(t, nextPing.Offset.Offset, i)
			require.NotZero(t, nextPing.Offset.Uncertainty, i)
			require.NotZero(t, nextPing.Offset.MeasuredAt, i)
		} else {
			require.Zero(t, nextPing.Offset.Offset, i)
			require.Zero(t, nextPing.Offset.Uncertainty, i)
			require.Zero(t, nextPing.Offset.MeasuredAt, i)
		}
	}
}

var _ kvpb.InternalServer = &internalServer{}

type internalServer struct {
	// rangeFeedEvents are returned on RangeFeed() calls.
	rangeFeedEvents   []kvpb.RangeFeedEvent
	rfServerStream    kvpb.Internal_RangeFeedServer
	muxRfServerStream kvpb.Internal_MuxRangeFeedServer
}

type rangefeedEventSink struct {
	ctx    context.Context
	stream kvpb.Internal_MuxRangeFeedServer
}

var _ kvpb.RangeFeedEventSink = (*rangefeedEventSink)(nil)

func (s *rangefeedEventSink) Context() context.Context {
	return s.ctx
}

func (s *rangefeedEventSink) Send(event *kvpb.RangeFeedEvent) error {
	return s.stream.Send(&kvpb.MuxRangeFeedEvent{RangeFeedEvent: *event})
}

func (s *internalServer) MuxRangeFeed(stream kvpb.Internal_MuxRangeFeedServer) error {
	s.muxRfServerStream = stream
	_, err := stream.Recv()
	if err != nil {
		return err
	}
	sink := &rangefeedEventSink{ctx: stream.Context(), stream: stream}
	return s.singleRangeFeed(sink)
}

func (*internalServer) Batch(context.Context, *kvpb.BatchRequest) (*kvpb.BatchResponse, error) {
	return nil, nil
}

func (*internalServer) RangeLookup(
	context.Context, *kvpb.RangeLookupRequest,
) (*kvpb.RangeLookupResponse, error) {
	panic("unimplemented")
}

func (s *internalServer) RangeFeed(
	_ *kvpb.RangeFeedRequest, stream kvpb.Internal_RangeFeedServer,
) error {
	s.rfServerStream = stream
	err := s.singleRangeFeed(stream)
	if err != nil {
		return err
	}
	return nil
}

func (s *internalServer) singleRangeFeed(sink kvpb.RangeFeedEventSink) error {
	for _, ev := range s.rangeFeedEvents {
		evCpy := ev
		if err := sink.Send(&evCpy); err != nil {
			return err
		}
	}
	return nil
}

func (*internalServer) GossipSubscription(
	*kvpb.GossipSubscriptionRequest, kvpb.Internal_GossipSubscriptionServer,
) error {
	panic("unimplemented")
}

func (*internalServer) ResetQuorum(
	context.Context, *kvpb.ResetQuorumRequest,
) (*kvpb.ResetQuorumResponse, error) {
	panic("unimplemented")
}

func (*internalServer) Join(
	context.Context, *kvpb.JoinNodeRequest,
) (*kvpb.JoinNodeResponse, error) {
	panic("unimplemented")
}

func (*internalServer) TokenBucket(
	ctx context.Context, in *kvpb.TokenBucketRequest,
) (*kvpb.TokenBucketResponse, error) {
	panic("unimplemented")
}

func (*internalServer) GetSpanConfigs(
	context.Context, *roachpb.GetSpanConfigsRequest,
) (*roachpb.GetSpanConfigsResponse, error) {
	panic("unimplemented")
}

func (*internalServer) GetAllSystemSpanConfigsThatApply(
	context.Context, *roachpb.GetAllSystemSpanConfigsThatApplyRequest,
) (*roachpb.GetAllSystemSpanConfigsThatApplyResponse, error) {
	panic("unimplemented")
}

func (*internalServer) UpdateSpanConfigs(
	context.Context, *roachpb.UpdateSpanConfigsRequest,
) (*roachpb.UpdateSpanConfigsResponse, error) {
	panic("unimplemented")
}

func (s *internalServer) SpanConfigConformance(
	context.Context, *roachpb.SpanConfigConformanceRequest,
) (*roachpb.SpanConfigConformanceResponse, error) {
	panic("unimplemented")
}

func (*internalServer) TenantSettings(
	*kvpb.TenantSettingsRequest, kvpb.Internal_TenantSettingsServer,
) error {
	panic("unimplemented")
}

func (n *internalServer) GetRangeDescriptors(
	*kvpb.GetRangeDescriptorsRequest, kvpb.Internal_GetRangeDescriptorsServer,
) error {
	panic("unimplemented")
}

// TestInternalServerAddress verifies that RPCContext uses AdvertiseAddr, not Addr, to
// determine whether to apply the local server optimization.
//
// Prevents regression of https://github.com/cockroachdb/cockroach/issues/19991.
func TestInternalServerAddress(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := timeutil.NewManualTime(timeutil.Unix(0, 1))
	maxOffset := time.Duration(0)

	serverCtx := newTestContext(uuid.MakeV4(), clock, maxOffset, stopper)
	serverCtx.Config.Addr = "127.0.0.1:9999"
	serverCtx.Config.AdvertiseAddr = "127.0.0.1:8888"
	serverCtx.NodeID.Set(context.Background(), 1)

	internal := &internalServer{}
	serverCtx.SetLocalInternalServer(
		internal,
		ServerInterceptorInfo{}, ClientInterceptorInfo{})

	ic := serverCtx.GetLocalInternalClientForAddr(1)
	lic, ok := ic.(internalClientAdapter)
	require.True(t, ok)
	require.Equal(t, internal, lic.server)
}

// Test that the internalClientAdapter runs the gRPC interceptors.
func TestInternalClientAdapterRunsInterceptors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	stopper.SetTracer(tracing.NewTracer())

	clock := timeutil.NewManualTime(timeutil.Unix(0, 1))
	maxOffset := time.Duration(0)

	serverCtx := newTestContext(uuid.MakeV4(), clock, maxOffset, stopper)
	serverCtx.Config.Addr = "127.0.0.1:9999"
	serverCtx.Config.AdvertiseAddr = "127.0.0.1:8888"
	serverCtx.NodeID.Set(context.Background(), 1)

	_ /* server */, serverInterceptors, err := NewServerEx(serverCtx)
	require.NoError(t, err)

	// Pile on one more interceptor to make sure it's called.
	var serverUnaryInterceptor1Called, serverUnaryInterceptor2Called bool
	interceptor1 := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		serverUnaryInterceptor1Called = true
		return handler(ctx, req)
	}
	serverInterceptors.UnaryInterceptors = append([]grpc.UnaryServerInterceptor{interceptor1}, serverInterceptors.UnaryInterceptors...)
	serverInterceptors.UnaryInterceptors = append(serverInterceptors.UnaryInterceptors, func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		serverUnaryInterceptor2Called = true
		return handler(ctx, req)
	})

	serverStreamInterceptor1Called := false
	serverInterceptors.StreamInterceptors = append(serverInterceptors.StreamInterceptors, func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		serverStreamInterceptor1Called = true
		return handler(srv, stream)
	})
	serverStreamInterceptor2Called := false
	serverInterceptors.StreamInterceptors = append(serverInterceptors.StreamInterceptors, func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		serverStreamInterceptor2Called = true
		return handler(srv, stream)
	})

	var clientInterceptors ClientInterceptorInfo
	var clientUnaryInterceptor1Called, clientUnaryInterceptor2Called bool
	var clientStreamInterceptor1Called, clientStreamInterceptor2Called bool
	clientInterceptors.UnaryInterceptors = append(clientInterceptors.UnaryInterceptors,
		func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			clientUnaryInterceptor1Called = true
			return invoker(ctx, method, req, reply, cc, opts...)
		},
		func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			clientUnaryInterceptor2Called = true
			return invoker(ctx, method, req, reply, cc, opts...)
		})
	clientInterceptors.StreamInterceptors = append(clientInterceptors.StreamInterceptors,
		func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			clientStreamInterceptor1Called = true
			return streamer(ctx, desc, cc, method, opts...)
		},
		func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			clientStreamInterceptor2Called = true
			return streamer(ctx, desc, cc, method, opts...)
		})

	internal := &internalServer{}
	serverCtx.SetLocalInternalServer(
		internal,
		serverInterceptors, clientInterceptors)
	ic := serverCtx.GetLocalInternalClientForAddr(1)
	lic, ok := ic.(internalClientAdapter)
	require.True(t, ok)
	require.Equal(t, internal, lic.server)

	for i := 0; i < 2; i++ {
		clientUnaryInterceptor1Called, clientUnaryInterceptor2Called = false, false
		serverUnaryInterceptor1Called, serverUnaryInterceptor2Called = false, false
		ba := &kvpb.BatchRequest{}
		_, err := lic.Batch(ctx, ba)
		require.NoError(t, err)
		require.True(t, serverUnaryInterceptor1Called)
		require.True(t, serverUnaryInterceptor2Called)
		require.True(t, clientUnaryInterceptor1Called)
		require.True(t, clientUnaryInterceptor2Called)
	}

	for i := 0; i < 2; i++ {
		serverStreamInterceptor1Called, serverStreamInterceptor2Called = false, false
		clientStreamInterceptor1Called, clientStreamInterceptor2Called = false, false
		stream, err := lic.RangeFeed(ctx, &kvpb.RangeFeedRequest{})
		require.NoError(t, err)
		_, err = stream.Recv()
		require.ErrorIs(t, err, io.EOF)
		require.True(t, clientStreamInterceptor1Called)
		require.True(t, clientStreamInterceptor2Called)
		require.True(t, serverStreamInterceptor1Called)
		require.True(t, serverStreamInterceptor2Called)
	}
}

// Test that a client stream interceptor can wrap the ClientStream when the
// internalClientAdapter is used.
func TestInternalClientAdapterWithClientStreamInterceptors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	stopper.SetTracer(tracing.NewTracer())

	clock := timeutil.NewManualTime(timeutil.Unix(0, 1))
	maxOffset := time.Duration(0)

	serverCtx := newTestContext(uuid.MakeV4(), clock, maxOffset, stopper)
	serverCtx.Config.Addr = "127.0.0.1:9999"
	serverCtx.Config.AdvertiseAddr = "127.0.0.1:8888"
	serverCtx.NodeID.Set(context.Background(), 1)

	_ /* server */, serverInterceptors, err := NewServerEx(serverCtx)
	require.NoError(t, err)

	testutils.RunTrueAndFalse(t, "use_mux_rangefeed", func(t *testing.T, useMux bool) {
		var clientInterceptors ClientInterceptorInfo
		var s *testClientStream
		clientInterceptors.StreamInterceptors = append(clientInterceptors.StreamInterceptors,
			func(
				ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
				method string, streamer grpc.Streamer, opts ...grpc.CallOption,
			) (grpc.ClientStream, error) {
				clientStream, err := streamer(ctx, desc, cc, method, opts...)
				if err != nil {
					return nil, err
				}
				s = &testClientStream{inner: clientStream}
				return s, nil
			})

		internal := &internalServer{rangeFeedEvents: []kvpb.RangeFeedEvent{{}, {}}}
		serverCtx.SetLocalInternalServer(
			internal,
			serverInterceptors, clientInterceptors)
		ic := serverCtx.GetLocalInternalClientForAddr(1)
		lic, ok := ic.(internalClientAdapter)
		require.True(t, ok)
		require.Equal(t, internal, lic.server)

		var receiveEvent func() error
		if useMux {
			stream, err := lic.MuxRangeFeed(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&kvpb.RangeFeedRequest{}))
			receiveEvent = func() error {
				e, err := stream.Recv()
				_ = e
				return err
			}
		} else {
			stream, err := lic.RangeFeed(ctx, &kvpb.RangeFeedRequest{})
			require.NoError(t, err)
			receiveEvent = func() error {
				_, err := stream.Recv()
				return err
			}
		}
		// Consume the stream.
		for {
			err := receiveEvent()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
		require.Equal(t, len(internal.rangeFeedEvents)+1, s.recvCount)
	})
}

// Test that a server stream interceptor can wrap the ServerStream when the
// internalClientAdapter is used.
func TestInternalClientAdapterWithServerStreamInterceptors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	stopper.SetTracer(tracing.NewTracer())

	clock := timeutil.NewManualTime(timeutil.Unix(0, 1))
	maxOffset := time.Duration(0)

	serverCtx := newTestContext(uuid.MakeV4(), clock, maxOffset, stopper)
	serverCtx.Config.Addr = "127.0.0.1:9999"
	serverCtx.Config.AdvertiseAddr = "127.0.0.1:8888"
	serverCtx.NodeID.Set(context.Background(), 1)

	_ /* server */, serverInterceptors, err := NewServerEx(serverCtx)
	require.NoError(t, err)

	testutils.RunTrueAndFalse(t, "use_mux_rangefeed", func(t *testing.T, useMux bool) {
		const int1Name = "interceptor 1"
		serverInterceptors.StreamInterceptors = append(serverInterceptors.StreamInterceptors,
			func(
				srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
			) error {
				serverStream := &testServerStream{name: "interceptor 1", inner: ss}
				return handler(srv, serverStream)
			})
		var secondInterceptorWrapped grpc.ServerStream
		const int2Name = "interceptor 2"
		serverInterceptors.StreamInterceptors = append(serverInterceptors.StreamInterceptors,
			func(
				srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
			) error {
				secondInterceptorWrapped = ss
				serverStream := &testServerStream{name: int2Name, inner: ss}
				return handler(srv, serverStream)
			})

		internal := &internalServer{rangeFeedEvents: []kvpb.RangeFeedEvent{{}, {}}}
		serverCtx.SetLocalInternalServer(
			internal,
			serverInterceptors, ClientInterceptorInfo{})
		ic := serverCtx.GetLocalInternalClientForAddr(1)
		lic, ok := ic.(internalClientAdapter)
		require.True(t, ok)
		require.Equal(t, internal, lic.server)

		var receiveEvent func() error
		if useMux {
			stream, err := lic.MuxRangeFeed(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&kvpb.RangeFeedRequest{}))
			receiveEvent = func() error {
				_, err := stream.Recv()
				return err
			}
		} else {
			stream, err := lic.RangeFeed(ctx, &kvpb.RangeFeedRequest{})
			require.NoError(t, err)
			receiveEvent = func() error {
				_, err := stream.Recv()
				return err
			}
		}

		// Consume the stream. This will synchronize with the server RPC handler
		// goroutine, ensuring that the server-side interceptors run.
		for {
			err := receiveEvent()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}

		require.IsType(t, &testServerStream{}, secondInterceptorWrapped)

		require.Equal(t, int1Name, secondInterceptorWrapped.(*testServerStream).name)
		var ss grpc.ServerStream
		if useMux {
			require.IsType(t, muxRangeFeedServerAdapter{}, internal.muxRfServerStream)
			ss = internal.muxRfServerStream.(muxRangeFeedServerAdapter).ServerStream
		} else {
			require.IsType(t, rangeFeedServerAdapter{}, internal.rfServerStream)
			ss = internal.rfServerStream.(rangeFeedServerAdapter).ServerStream
		}
		require.IsType(t, &testServerStream{}, ss)
		topStream := ss.(*testServerStream)
		require.Equal(t, int2Name, topStream.name)
		require.IsType(t, &testServerStream{}, topStream.inner)
		bottomStream := topStream.inner.(*testServerStream)
		require.Equal(t, int1Name, bottomStream.name)
	})
}

type testClientStream struct {
	inner     grpc.ClientStream
	recvCount int
}

var _ grpc.ClientStream = &testClientStream{}

func (t *testClientStream) Header() (metadata.MD, error) {
	return t.inner.Header()
}

func (t *testClientStream) Trailer() metadata.MD {
	return t.inner.Trailer()
}

func (t *testClientStream) CloseSend() error {
	return t.inner.CloseSend()
}

func (t *testClientStream) Context() context.Context {
	return t.inner.Context()
}

func (t *testClientStream) SendMsg(m interface{}) error {
	return t.inner.SendMsg(m)
}

func (t *testClientStream) RecvMsg(m interface{}) error {
	t.recvCount++
	return t.inner.RecvMsg(m)
}

type testServerStream struct {
	name  string
	inner grpc.ServerStream
}

var _ grpc.ServerStream = &testServerStream{}

func (t testServerStream) SetHeader(md metadata.MD) error {
	return t.inner.SetHeader(md)
}

func (t testServerStream) SendHeader(md metadata.MD) error {
	return t.inner.SendHeader(md)
}

func (t testServerStream) SetTrailer(md metadata.MD) {
	t.inner.SetTrailer(md)
}

func (t testServerStream) Context() context.Context {
	return t.inner.Context()
}

func (t testServerStream) SendMsg(m interface{}) error {
	return t.inner.SendMsg(m)
}

func (t testServerStream) RecvMsg(m interface{}) error {
	return t.inner.RecvMsg(m)
}

func BenchmarkInternalClientAdapter(b *testing.B) {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := timeutil.NewManualTime(timeutil.Unix(0, 1))
	maxOffset := time.Duration(0)
	serverCtx := newTestContext(uuid.MakeV4(), clock, maxOffset, stopper)
	serverCtx.Config.Addr = "127.0.0.1:9999"
	serverCtx.Config.AdvertiseAddr = "127.0.0.1:8888"
	serverCtx.NodeID.Set(context.Background(), 1)

	_, interceptors, err := NewServerEx(serverCtx)
	require.NoError(b, err)

	internal := &internalServer{}
	serverCtx.SetLocalInternalServer(
		internal,
		interceptors, ClientInterceptorInfo{})
	ic := serverCtx.GetLocalInternalClientForAddr(roachpb.NodeID(1))
	lic, ok := ic.(internalClientAdapter)
	require.True(b, ok)
	require.Equal(b, internal, lic.server)
	ba := &kvpb.BatchRequest{}
	_, err = lic.Batch(ctx, ba)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = lic.Batch(ctx, ba)
	}
}

// TestHeartbeatHealth verifies that the health status changes after
// heartbeats succeed or fail.
func TestHeartbeatHealth(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := timeutil.NewManualTime(timeutil.Unix(0, 1))
	maxOffset := time.Duration(0)

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	const serverNodeID = 1
	const clientNodeID = 2

	ctx := context.Background()
	serverCtx := newTestContext(clusterID, clock, maxOffset, stop.NewStopper())
	defer serverCtx.Stopper.Stop(ctx)
	serverCtx.NodeID.Set(context.Background(), serverNodeID)
	s := newTestServer(t, serverCtx)

	heartbeat := &ManualHeartbeatService{
		ready:              make(chan error),
		stopper:            stopper,
		clock:              clock,
		maxOffset:          maxOffset,
		remoteClockMonitor: serverCtx.RemoteClocks,
		version:            serverCtx.Settings.Version,
		nodeID:             serverCtx.NodeID,
	}
	RegisterHeartbeatServer(s, heartbeat)

	errFailedHeartbeat := errors.New("failed heartbeat")

	var hbSuccess atomic.Value
	hbSuccess.Store(true)

	go func() {
		for {
			var err error
			if !hbSuccess.Load().(bool) {
				err = errFailedHeartbeat
			}

			select {
			case <-stopper.ShouldQuiesce():
				return
			case heartbeat.ready <- err:
			}
		}
	}()

	clientCtx := newTestContext(clusterID, clock, maxOffset, stopper)
	clientCtx.NodeID.Set(context.Background(), clientNodeID)

	lisNotLocalServer, err := net.Listen("tcp", "127.0.0.1:0")
	t.Logf("lisNotLocal: %s", lisNotLocalServer.Addr())
	require.NoError(t, err)
	defer func() {
		netutil.FatalIfUnexpected(lisNotLocalServer.Close())
	}()
	lisLocalServer, err := net.Listen("tcp", "127.0.0.1:0")
	t.Logf("lisLocalServer: %s", lisLocalServer.Addr())
	require.NoError(t, err)
	require.NoError(t, err)
	defer func() {
		netutil.FatalIfUnexpected(lisLocalServer.Close())
	}()

	clientCtx.Config.Addr = lisNotLocalServer.Addr().String()
	clientCtx.Config.AdvertiseAddr = lisLocalServer.Addr().String()

	// Make the interval shorter to speed up the test.
	clientCtx.Config.RPCHeartbeatInterval = 1 * time.Millisecond
	clientCtx.Config.RPCHeartbeatTimeout = 1 * time.Millisecond

	m := clientCtx.Metrics()

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	require.NoError(t, err)
	remoteAddr := ln.Addr().String()
	t.Logf("remoteAddr: %s [serves grpc on serverCtx]", ln.Addr())
	if _, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, DefaultClass).Connect(ctx); err != nil {
		t.Fatal(err)
	}

	requireNominal := func(t *testing.T, exp int64) {
		t.Helper()
		require.Equal(t, exp, m.HeartbeatsNominal.Value())
	}

	// Wait for the connection.
	testutils.SucceedsSoon(t, func() error {
		err := clientCtx.TestingConnHealth(remoteAddr, serverNodeID)
		if err != nil && !errors.Is(err, ErrNotHeartbeated) {
			t.Fatal(err)
		}
		return err
	})
	requireNominal(t, 1)

	// Should not get a healthy connection while heartbeats fail; nominal
	// heartbeats should soon drop to zero as the heartbeat loop fails (which
	// destroys the connection).
	hbSuccess.Store(false)
	testutils.SucceedsSoon(t, func() error {
		n := clientCtx.Metrics().HeartbeatsNominal.Value()
		if n != 0 {
			return errors.Errorf("%d nominal heartbeats", n)
		}
		// It might take a brief moment for ConnHealth to come back
		// as ErrNotHeartbeated, but it should be non-nil immediately.
		err := clientCtx.TestingConnHealth(remoteAddr, serverNodeID)
		require.Error(t, err)
		if !errors.Is(err, ErrNotHeartbeated) {
			return err
		}
		return nil
	})

	// Should become healthy in the presence of successful heartbeats.
	hbSuccess.Store(true)
	testutils.SucceedsSoon(t, func() error {
		return clientCtx.TestingConnHealth(remoteAddr, serverNodeID)
	})
	requireNominal(t, 1)

	// Should become unhealthy again in the presence of failing heartbeats.
	hbSuccess.Store(false)
	testutils.SucceedsSoon(t, func() error {
		if n := m.HeartbeatsNominal.Value(); n > 0 {
			return errors.Errorf("%d nominal heartbeats", n)
		}
		// See above for rationale.
		err := clientCtx.TestingConnHealth(remoteAddr, serverNodeID)
		require.Error(t, err)
		if !errors.Is(err, ErrNotHeartbeated) {
			return err
		}
		return nil
	})

	// Should become healthy in the presence of successful heartbeats.
	hbSuccess.Store(true)
	testutils.SucceedsSoon(t, func() error {
		return clientCtx.TestingConnHealth(remoteAddr, serverNodeID)
	})
	requireNominal(t, 1)

	// Ensure that non-existing connections return ErrNotHeartbeated.
	lisNonExistentConnection, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		netutil.FatalIfUnexpected(lisNonExistentConnection.Close())
	}()
	t.Logf("lisNonExistent: %s", lisNonExistentConnection.Addr())
	if err := clientCtx.TestingConnHealth(lisNonExistentConnection.Addr().String(), 3); !errors.Is(err, ErrNotHeartbeated) {
		t.Errorf("wanted ErrNotHeartbeated, not %v", err)
	}
	// The connection to Node 3 on the lisNonExistentConnection should be
	// initializing and the server connection should be nominal.
	testutils.SucceedsSoon(t, func() error {
		return checkNominal(m, 1)
	})

	// Ensure that there's no error connecting to the local node when an internal
	// server has been registered.
	clientCtx.SetLocalInternalServer(
		&internalServer{},
		ServerInterceptorInfo{}, ClientInterceptorInfo{})
	require.NoError(t, clientCtx.TestingConnHealth(clientCtx.Config.AdvertiseAddr, clientNodeID))

	// Connections should shut down again and now that we're nearing the test it's
	// a good opportunity to check that what came up must go down.
	hbSuccess.Store(false)
	// Need to close these or we eat a 20s timeout.
	netutil.FatalIfUnexpected(lisNonExistentConnection.Close())
	netutil.FatalIfUnexpected(lisNotLocalServer.Close())
	testutils.SucceedsSoon(t, func() error {
		started, exited := m.HeartbeatLoopsStarted.Count(), m.HeartbeatLoopsExited.Count()
		if started != exited {
			return errors.Errorf("started(%d) != exited(%d)", started, exited)
		}
		return nil
	})
}

func checkNominal(m *Metrics, exp int64) error {
	if n := m.HeartbeatsNominal.Value(); n != exp {
		return errors.Errorf("%d nominal, want %d", n, exp)
	}
	return nil
}

// TestConnectionRemoveNodeIDZero verifies that when a connection initiated via
// GRPCDialNode fails, we also clean up the connection returned by
// GRPCUnvalidatedDial.
//
// See #37200.
func TestConnectionRemoveNodeIDZero(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := &timeutil.DefaultTimeSource{}
	maxOffset := time.Nanosecond
	clientCtx := newTestContext(uuid.MakeV4(), clock, maxOffset, stopper)
	// Provoke an error.
	_, err := clientCtx.GRPCDialNode("127.0.0.1:notaport", 1, DefaultClass).Connect(context.Background())
	if err == nil {
		t.Fatal("expected some kind of error, got nil")
	}

	// NB: this takes a moment because GRPCDialRaw only gives up on the initial
	// connection after 1s (more precisely, the redialChan gets closed only after
	// 1s), which seems difficult to configure ad-hoc.
	testutils.SucceedsSoon(t, func() error {
		var keys []connKey
		clientCtx.m.mu.RLock()
		defer clientCtx.m.mu.RUnlock()
		for k := range clientCtx.m.mu.m {
			keys = append(keys, k)
		}
		if len(keys) > 0 {
			return errors.Errorf("still have connections %v", keys)
		}
		return nil
	})
}

type interceptingListener struct {
	net.Listener
	connCB func(net.Conn)
}

func (ln *interceptingListener) Accept() (net.Conn, error) {
	conn, err := ln.Listener.Accept()
	if err == nil {
		ln.connCB(conn)
	}
	return conn, err
}

// TestHeartbeatHealth verifies that the health status changes after
// heartbeats succeed or fail due to transport failures.
func TestHeartbeatHealthTransport(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	ctx := context.Background()

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	clock := timeutil.NewManualTime(timeutil.Unix(0, 1))
	maxOffset := time.Duration(0)
	serverCtx := newTestContext(clusterID, clock, maxOffset, stopper)
	const serverNodeID = 1
	serverCtx.NodeID.Set(context.Background(), serverNodeID)
	// newTestServer with a custom listener.
	tlsConfig, err := serverCtx.GetServerTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		clusterID:          serverCtx.StorageClusterID,
		nodeID:             serverCtx.NodeID,
		version:            serverCtx.Settings.Version,
	})

	mu := struct {
		syncutil.Mutex
		conns     []net.Conn
		autoClose bool
	}{}
	ln := func() *interceptingListener {
		ln, err := net.Listen("tcp", util.TestAddr.String())
		if err != nil {
			t.Fatal(err)
		}
		return &interceptingListener{
			Listener: ln,
			connCB: func(conn net.Conn) {
				mu.Lock()
				if mu.autoClose {
					_ = conn.Close()
				} else {
					mu.conns = append(mu.conns, conn)
				}
				mu.Unlock()
			}}
	}()

	_ = stopper.RunAsyncTask(ctx, "wait-quiesce", func(context.Context) {
		<-stopper.ShouldQuiesce()
		netutil.FatalIfUnexpected(ln.Close())
		<-stopper.ShouldQuiesce()
		s.Stop()
	})

	_ = stopper.RunAsyncTask(ctx, "serve", func(context.Context) {
		netutil.FatalIfUnexpected(s.Serve(ln))
	})

	remoteAddr := ln.Addr().String()

	clientCtx := newTestContext(clusterID, clock, maxOffset, stopper)
	// Make the interval shorter to speed up the test.
	clientCtx.Config.RPCHeartbeatInterval = 1 * time.Millisecond
	clientCtx.Config.RPCHeartbeatTimeout = 1 * time.Millisecond
	if _, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, DefaultClass).Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Everything is normal; should become healthy.
	testutils.SucceedsSoon(t, func() error {
		return clientCtx.TestingConnHealth(remoteAddr, serverNodeID)
	})

	closeConns := func() (numClosed int, _ error) {
		mu.Lock()
		defer mu.Unlock()
		n := len(mu.conns)
		for i := n - 1; i >= 0; i-- {
			// This can spuriously return ErrClosed since the listener is closed
			// before us.
			if err := mu.conns[i].Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				return 0, err
			}
			mu.conns = mu.conns[:i]
		}
		return n, nil
	}

	isUnhealthy := func(err error) bool {
		// Most of the time, an unhealthy connection will get
		// ErrNotHeartbeated, but there are brief periods during which we
		// could get one of the grpc errors below (while the old
		// connection is in the middle of closing).
		if errors.Is(err, ErrNotHeartbeated) {
			return true
		}
		// The expected code here is Unavailable, but at least on OSX you can also get
		//
		// rpc error: code = Internal desc = connection error: desc = "transport: authentication
		// handshake failed: write tcp 127.0.0.1:53936->127.0.0.1:53934: write: broken pipe".
		code := status.Code(errors.UnwrapAll(err))
		return code == codes.Unavailable || code == codes.Internal
	}

	// Close all the connections until we see a failure on the main goroutine.
	done := make(chan struct{})
	if err := stopper.RunAsyncTask(ctx, "busyloop-closer", func(ctx context.Context) {
		for {
			if _, err := closeConns(); err != nil {
				log.Warningf(ctx, "%v", err)
			}
			select {
			case <-done:
				return
			default:
			}
		}
	}); err != nil {
		t.Fatal(err)
	}

	// We don't use SucceedsSoon because that internally uses doubling backoffs, and
	// it doesn't need too much bad luck to run into the time limit.
	for then := timeutil.Now(); ; {
		err := func() error {
			if err := clientCtx.TestingConnHealth(remoteAddr, serverNodeID); !isUnhealthy(err) {
				return errors.Errorf("unexpected error: %v", err) // nolint:errwrap
			}
			return nil
		}()
		if err == nil {
			break
		}
		if timeutil.Since(then) > 45*time.Second {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(done)

	// We can reconnect and the connection becomes healthy again.
	testutils.SucceedsSoon(t, func() error {
		if _, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, DefaultClass).Connect(context.Background()); err != nil {
			return err
		}
		return clientCtx.TestingConnHealth(remoteAddr, serverNodeID)
	})

	// Close the listener and all the connections. Note that if we
	// only closed the listener, recently-accepted-but-not-yet-handled
	// connections could sneak in and randomly make the target healthy
	// again. To avoid this, we flip the boolean below which is used in
	// our handler callback to eagerly close any stragglers.
	mu.Lock()
	mu.autoClose = true
	mu.Unlock()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	// Also terminate any existing connections.
	if _, err := closeConns(); err != nil {
		t.Fatal(err)
	}

	// Should become unhealthy again now that the connection was closed.
	testutils.SucceedsSoon(t, func() error {
		err := clientCtx.TestingConnHealth(remoteAddr, serverNodeID)

		if !isUnhealthy(err) {
			return errors.Errorf("unexpected error: %v", err) // nolint:errwrap
		}
		return nil
	})

	// TODO(baptist): Better understand when this happens. It appears we can get
	// spurious connections to other tests on a stress run. This has been
	// happening for a while, but only comes out rarely when this package is
	// stressed. This test is very aggressive since it is calling GRPCDialNode in
	// a busy loop for 50ms.
	expectedCluster := "doesn't match server cluster ID"
	expectedNode := "doesn't match server node ID"
	// Should stay unhealthy despite reconnection attempts.
	for then := timeutil.Now(); timeutil.Since(then) < 50*clientCtx.Config.RPCHeartbeatTimeout; {
		err := clientCtx.TestingConnHealth(remoteAddr, serverNodeID)
		if !isUnhealthy(err) && !testutils.IsError(err, expectedCluster) && !testutils.IsError(err, expectedNode) {
			t.Fatal(err)
		}
	}
}

func TestOffsetMeasurement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()
	ctx := context.Background()

	serverClock := timeutil.NewManualTime(timeutil.Unix(0, 20))
	maxOffset := time.Duration(0)
	serverCtx := newTestContext(clusterID, serverClock, maxOffset, stopper)
	const serverNodeID = 1
	serverCtx.NodeID.Set(ctx, serverNodeID)
	s := newTestServer(t, serverCtx)
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		clusterID:          serverCtx.StorageClusterID,
		nodeID:             serverCtx.NodeID,
		version:            serverCtx.Settings.Version,
	})

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	remoteAddr := ln.Addr().String()

	// Create a client clock that is behind the server clock.
	clientClock := &AdvancingClock{time: timeutil.Unix(0, 10)}
	clientMaxOffset := time.Duration(0)
	clientCtx := newTestContext(clusterID, clientClock, clientMaxOffset, stopper)
	// Make the interval shorter to speed up the test.
	clientCtx.Config.RPCHeartbeatInterval = 1 * time.Millisecond
	clientCtx.Config.RPCHeartbeatTimeout = 1 * time.Millisecond
	clientCtx.RemoteClocks.offsetTTL = 5 * clientClock.getAdvancementInterval()
	if _, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, DefaultClass).Connect(ctx); err != nil {
		t.Fatal(err)
	}

	expectedOffset := RemoteOffset{Offset: 10, Uncertainty: 0, MeasuredAt: 10}
	testutils.SucceedsSoon(t, func() error {
		clientCtx.RemoteClocks.mu.Lock()
		defer clientCtx.RemoteClocks.mu.Unlock()

		if o, ok := clientCtx.RemoteClocks.mu.offsets[serverNodeID]; !ok {
			return errors.Errorf("expected offset of %d to be initialized, but it was not", serverNodeID)
		} else if o != expectedOffset {
			return errors.Errorf("expected:\n%v\nactual:\n%v", expectedOffset, o)
		}
		return nil
	})

	// Change the client such that it receives a heartbeat right after the
	// maximum clock reading delay.
	clientClock.setAdvancementInterval(
		maximumPingDurationMult*clientMaxOffset + 1*time.Nanosecond)

	testutils.SucceedsSoon(t, func() error {
		clientCtx.RemoteClocks.mu.Lock()
		defer clientCtx.RemoteClocks.mu.Unlock()

		if o, ok := clientCtx.RemoteClocks.mu.offsets[serverNodeID]; ok {
			return errors.Errorf("expected offset to have been cleared, but found %s", o)
		}
		return nil
	})
}

func TestFailedOffsetMeasurement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	clock := timeutil.NewManualTime(timeutil.Unix(0, 1))
	maxOffset := time.Duration(0)

	serverCtx := newTestContext(clusterID, clock, maxOffset, stopper)
	const serverNodeID = 1
	serverCtx.NodeID.Set(context.Background(), serverNodeID)
	s := newTestServer(t, serverCtx)
	heartbeat := &ManualHeartbeatService{
		clock:              clock,
		maxOffset:          maxOffset,
		remoteClockMonitor: serverCtx.RemoteClocks,
		ready:              make(chan error),
		stopper:            stopper,
		version:            serverCtx.Settings.Version,
		nodeID:             serverCtx.NodeID,
	}
	RegisterHeartbeatServer(s, heartbeat)

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	remoteAddr := ln.Addr().String()

	// Create a client that never receives a heartbeat after the first.
	clientCtx := newTestContext(clusterID, clock, maxOffset, stopper)
	// Remove the timeout so that failure arises from exceeding the maximum
	// clock reading delay, not the timeout.
	clientCtx.heartbeatTimeout = 0
	go func() { heartbeat.ready <- nil }() // Allow one heartbeat for initialization.
	if _, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, DefaultClass).Connect(ctx); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		clientCtx.RemoteClocks.mu.Lock()
		defer clientCtx.RemoteClocks.mu.Unlock()

		if _, ok := clientCtx.RemoteClocks.mu.offsets[serverNodeID]; !ok {
			return errors.Errorf("expected offset of %s to be initialized, but it was not", remoteAddr)
		}
		return nil
	})

	testutils.SucceedsSoon(t, func() error {
		serverCtx.RemoteClocks.mu.Lock()
		defer serverCtx.RemoteClocks.mu.Unlock()

		if o, ok := serverCtx.RemoteClocks.mu.offsets[serverNodeID]; ok {
			return errors.Errorf("expected offset of %s to not be initialized, but it was: %v", remoteAddr, o)
		}
		return nil
	})
}

// TestLatencyInfoCleanup tests that latencyInfo is cleaned up for closed connection
// to avoid reporting stale information.
func TestLatencyInfoCleanupOnClosedConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()
	ctx := context.Background()

	serverClock := timeutil.NewManualTime(timeutil.Unix(0, 20))
	maxOffset := time.Duration(0)
	serverCtx := newTestContext(clusterID, serverClock, maxOffset, stopper)
	const serverNodeID = 1
	serverCtx.NodeID.Set(ctx, serverNodeID)
	s := newTestServer(t, serverCtx)
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              serverClock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		clusterID:          serverCtx.StorageClusterID,
		nodeID:             serverCtx.NodeID,
		version:            serverCtx.Settings.Version,
	})

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	remoteAddr := ln.Addr().String()

	// Create a client clock that is behind the server clock.
	clientClock := &AdvancingClock{time: timeutil.Unix(0, 10)}
	clientMaxOffset := time.Duration(0)
	clientCtx := newTestContext(clusterID, clientClock, clientMaxOffset, stopper)
	// Make the interval shorter to speed up the test.
	clientCtx.Config.RPCHeartbeatInterval = 1 * time.Millisecond
	clientCtx.Config.RPCHeartbeatTimeout = 1 * time.Millisecond

	conn, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, DefaultClass).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	anotherConn, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, SystemClass).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	clientClock.setAdvancementInterval(
		maximumPingDurationMult*clientMaxOffset + 1*time.Nanosecond)

	testutils.SucceedsSoon(t, func() error {
		clientCtx.RemoteClocks.mu.Lock()
		defer clientCtx.RemoteClocks.mu.Unlock()

		if li, ok := clientCtx.RemoteClocks.mu.latencyInfos[serverNodeID]; !ok {
			return errors.Errorf("expected to have latencyInfos %v, but it was not", li)
		}
		return nil
	})

	// Close first connection. It cannot be considered as network disruption yet, since anotherConn still open.
	err = conn.Close() // nolint:grpcconnclose
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		clientCtx.RemoteClocks.mu.Lock()
		defer clientCtx.RemoteClocks.mu.Unlock()
		if _, ok := clientCtx.RemoteClocks.mu.latencyInfos[serverNodeID]; !ok {
			return errors.Errorf("expected to have latencyInfos, but nothing found")
		}
		return nil
	})

	// Close last anotherConn to simulate network disruption.
	err = anotherConn.Close() // nolint:grpcconnclose
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		clientCtx.RemoteClocks.mu.Lock()
		defer clientCtx.RemoteClocks.mu.Unlock()

		if li, ok := clientCtx.RemoteClocks.mu.latencyInfos[serverNodeID]; ok {
			return errors.Errorf("expected to have removed latencyInfos, but found: %v", li)
		}
		return nil
	})
}

type AdvancingClock struct {
	syncutil.Mutex
	time                time.Time
	advancementInterval atomic.Value // time.Duration
}

var _ hlc.WallClock = &AdvancingClock{}

func (ac *AdvancingClock) setAdvancementInterval(d time.Duration) {
	ac.advancementInterval.Store(d)
}

func (ac *AdvancingClock) getAdvancementInterval() time.Duration {
	v := ac.advancementInterval.Load()
	if v == nil {
		return 0
	}
	return v.(time.Duration)
}

func (ac *AdvancingClock) Now() time.Time {
	ac.Lock()
	now := ac.time
	ac.time = now.Add(ac.getAdvancementInterval())
	ac.Unlock()
	return now
}

func TestRemoteOffsetUnhealthy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	const maxOffset = 100 * time.Millisecond

	type nodeContext struct {
		offset  time.Duration
		ctx     *Context
		errChan chan error
	}

	start := time.Date(2012, 12, 07, 0, 0, 0, 0, time.UTC)

	nodeCtxs := []nodeContext{
		{offset: 0},
		{offset: 0},
		{offset: 0},
		// The minimum offset that actually triggers node death.
		{offset: maxOffset + 1},
	}

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	for i := range nodeCtxs {
		clock := timeutil.NewManualTime(timeutil.Unix(0, start.Add(nodeCtxs[i].offset).UnixNano()))
		nodeCtxs[i].errChan = make(chan error, 1)
		nodeCtxs[i].ctx = newTestContext(clusterID, clock, maxOffset, stopper)
		nodeCtxs[i].ctx.Config.RPCHeartbeatInterval = maxOffset
		nodeCtxs[i].ctx.Config.RPCHeartbeatTimeout = maxOffset
		nodeCtxs[i].ctx.NodeID.Set(context.Background(), roachpb.NodeID(i+1))

		s := newTestServer(t, nodeCtxs[i].ctx)
		RegisterHeartbeatServer(s, &HeartbeatService{
			clock:              clock,
			remoteClockMonitor: nodeCtxs[i].ctx.RemoteClocks,
			clusterID:          nodeCtxs[i].ctx.StorageClusterID,
			nodeID:             nodeCtxs[i].ctx.NodeID,
			version:            nodeCtxs[i].ctx.Settings.Version,
		})
		ln, err := netutil.ListenAndServeGRPC(nodeCtxs[i].ctx.Stopper, s, util.TestAddr)
		if err != nil {
			t.Fatal(err)
		}
		nodeCtxs[i].ctx.Config.Addr = ln.Addr().String()
	}

	// Fully connect the nodes.
	for i, clientNodeContext := range nodeCtxs {
		for j, serverNodeContext := range nodeCtxs {
			if i == j {
				continue
			}
			if _, err := clientNodeContext.ctx.GRPCDialNode(serverNodeContext.ctx.Config.Addr, serverNodeContext.ctx.NodeID.Get(), DefaultClass).Connect(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Wait until all nodes are connected to all other nodes.
	for _, nodeCtx := range nodeCtxs {
		testutils.SucceedsSoon(t, func() error {
			nodeCtx.ctx.RemoteClocks.mu.Lock()
			defer nodeCtx.ctx.RemoteClocks.mu.Unlock()

			if a, e := len(nodeCtx.ctx.RemoteClocks.mu.offsets), len(nodeCtxs)-1; a != e {
				return errors.Errorf("not yet fully connected: have %d of %d connections: %v", a, e, nodeCtx.ctx.RemoteClocks.mu.offsets)
			}
			return nil
		})
	}

	for i, nodeCtx := range nodeCtxs {
		if nodeOffset := nodeCtx.offset; nodeOffset > maxOffset {
			if err := nodeCtx.ctx.RemoteClocks.VerifyClockOffset(nodeCtx.ctx.MasterCtx); testutils.IsError(err, errOffsetGreaterThanMaxOffset) {
				t.Logf("max offset: %s - node %d with excessive clock offset of %s returned expected error: %s", maxOffset, i, nodeOffset, err)
			} else {
				t.Errorf("max offset: %s - node %d with excessive clock offset of %s returned unexpected error: %v", maxOffset, i, nodeOffset, err)
			}
		} else {
			if err := nodeCtx.ctx.RemoteClocks.VerifyClockOffset(nodeCtx.ctx.MasterCtx); err != nil {
				t.Errorf("max offset: %s - node %d with acceptable clock offset of %s returned unexpected error: %s", maxOffset, i, nodeOffset, err)
			} else {
				t.Logf("max offset: %s - node %d with acceptable clock offset of %s did not return an error, as expected", maxOffset, i, nodeOffset)
			}
		}
	}
}

// This is a smoketest for gRPC Keepalives: rpc.Context asks gRPC to perform
// periodic pings on the transport to check that it's still alive. If the ping
// doesn't get a pong within a timeout, the transport is supposed to be closed -
// that's what we're testing here. Likewise, serverside keepalive ensures that
// if a ping is not seen within a timeout, the transport will also be closed.
//
// In this test we use a TestingHeartbeatStreamService as opposed to a standard
// HeartbeatService. This is important to test scenarios where the
// client->server connection is partitioned but the server->client connection is
// healthy, because a TestingHeartbeatStreamService will continue to respond on
// its response stream even if it doesn't get any new requests.
func TestGRPCKeepaliveFailureFailsInflightRPCs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, 51800, "Takes too long given https://github.com/grpc/grpc-go/pull/2642")

	sc := log.Scope(t)
	defer sc.Close(t)

	testCases := []grpcKeepaliveTestCase{
		// Keepalive doesn't matter if the network is fine.
		{cKeepalive: false, sKeepalive: false, partitionC2S: false, partitionS2C: false, expClose: false},

		// No keepalive. Never detects network issues.
		{cKeepalive: false, sKeepalive: false, partitionC2S: true, partitionS2C: false, expClose: false},
		{cKeepalive: false, sKeepalive: false, partitionC2S: false, partitionS2C: true, expClose: false},
		{cKeepalive: false, sKeepalive: false, partitionC2S: true, partitionS2C: true, expClose: false},

		// Client-only keepalive. Doesn't detect client->server partition.
		{cKeepalive: true, sKeepalive: false, partitionC2S: true, partitionS2C: false, expClose: false},
		{cKeepalive: true, sKeepalive: false, partitionC2S: false, partitionS2C: true, expClose: true},
		{cKeepalive: true, sKeepalive: false, partitionC2S: true, partitionS2C: true, expClose: true},

		// Server-only keepalive. Only detects server->client partition. The
		// bi-directional partition case (third case) may be is surprising.
		// The reason the client doesn't close the connection is because it
		// does not receive the connection closed message sent by the server.
		// This demonstrates why client keepalive is so important.
		{cKeepalive: false, sKeepalive: true, partitionC2S: true, partitionS2C: false, expClose: true},
		{cKeepalive: false, sKeepalive: true, partitionC2S: false, partitionS2C: true, expClose: false},
		{cKeepalive: false, sKeepalive: true, partitionC2S: true, partitionS2C: true, expClose: false},

		// Client and Server keepalive. Detects all partitions!
		{cKeepalive: true, sKeepalive: true, partitionC2S: true, partitionS2C: false, expClose: true},
		{cKeepalive: true, sKeepalive: true, partitionC2S: false, partitionS2C: true, expClose: true},
		{cKeepalive: true, sKeepalive: true, partitionC2S: true, partitionS2C: true, expClose: true},
	}

	// For consistent spacing in test names.
	fmtBool := func(b bool) string {
		s := strconv.FormatBool(b)
		if b {
			s += " "
		}
		return s
	}
	connIcon := func(partition bool) string {
		if partition {
			return "-X->"
		}
		return "--->"
	}

	// Run all the tests.
	var wg sync.WaitGroup
	wg.Add(len(testCases))
	errCh := make(chan error, len(testCases))
	for testNum, c := range testCases {
		kaName := fmt.Sprintf("clientKeepalive=%s,serverKeepalive=%s", fmtBool(c.cKeepalive), fmtBool(c.sKeepalive))
		pName := fmt.Sprintf("client%sserver,server%sclient", connIcon(c.partitionC2S), connIcon(c.partitionS2C))
		testName := fmt.Sprintf("%d/%s/%s", testNum, kaName, pName)
		ctx := logtags.AddTag(context.Background(), testName, nil)

		log.Infof(ctx, "starting sub-test")
		go func(c grpcKeepaliveTestCase) {
			errCh <- errors.Wrapf(grpcRunKeepaliveTestCase(ctx, c), "%+v", c)
			wg.Done()
		}(c)
	}
	log.Infof(context.Background(), "waiting for sub-tests to complete")
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Errorf("%+v", err)
		}
	}
}

type grpcKeepaliveTestCase struct {
	cKeepalive, sKeepalive     bool
	partitionC2S, partitionS2C bool
	expClose                   bool
}

func grpcRunKeepaliveTestCase(testCtx context.Context, c grpcKeepaliveTestCase) error {
	var cKeepalive keepalive.ClientParameters
	if c.cKeepalive {
		cKeepalive = clientKeepalive
	}
	var sKeepalive keepalive.ServerParameters
	if c.sKeepalive {
		sKeepalive = serverTestingKeepalive
	}

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	ctx, cancel := stopper.WithCancelOnQuiesce(testCtx)
	defer cancel()

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	// Construct server with server-side keepalive.
	log.Infof(ctx, "constructing server")
	clock := timeutil.NewManualTime(timeutil.Unix(0, 20))
	maxOffset := time.Duration(0)
	serverCtx := newTestContext(clusterID, clock, maxOffset, stopper)
	const serverNodeID = 1
	serverCtx.NodeID.Set(context.Background(), serverNodeID)
	tlsConfig, err := serverCtx.GetServerTLSConfig()
	if err != nil {
		return err
	}
	s := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsConfig)),
		grpc.KeepaliveParams(sKeepalive),
	)

	// Create heartbeat service. This service will continuously
	// read on its input stream and send on its output stream.
	log.Infof(ctx, "creating heartbeat service")
	const msgInterval = 10 * time.Millisecond
	hss := &HeartbeatStreamService{
		HeartbeatService: HeartbeatService{
			clock:              clock,
			remoteClockMonitor: serverCtx.RemoteClocks,
			clusterID:          serverCtx.StorageClusterID,
			nodeID:             serverCtx.NodeID,
			version:            serverCtx.Settings.Version,
		},
		interval: msgInterval,
	}
	RegisterHeartbeatServer(s, hss)
	RegisterTestingHeartbeatStreamServer(s, hss)

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	if err != nil {
		return err
	}
	remoteAddr := ln.Addr().String()

	log.Infof(ctx, "setting up client")
	clientCtx := newTestContext(clusterID, clock, maxOffset, stopper)
	// Disable automatic heartbeats. We'll send them by hand.
	clientCtx.Config.RPCHeartbeatInterval = math.MaxInt64
	clientCtx.Config.RPCHeartbeatTimeout = math.MaxInt64

	var firstConn int32 = 1

	// We're going to open RPC transport connections using a dialer that returns
	// PartitionableConns. We'll partition the first opened connection.
	dialerCh := make(chan *testutils.PartitionableConn, 1)
	clientCtx.AddTestingDialOpts(
		grpc.WithContextDialer(
			func(_ context.Context, addr string) (net.Conn, error) {
				if !atomic.CompareAndSwapInt32(&firstConn, 1, 0) {
					// If we allow gRPC to open a 2nd transport connection, then our RPCs
					// might succeed if they're sent on that one. In the spirit of a
					// partition, we'll return errors for the attempt to open a new
					// connection (albeit for a TCP connection the error would come after
					// a socket connect timeout).
					return nil, errors.Errorf("No more connections for you. We're partitioned.")
				}

				conn, err := net.Dial("tcp", addr)
				if err != nil {
					return nil, err
				}
				transportConn := testutils.NewPartitionableConn(conn)
				dialerCh <- transportConn
				return transportConn, nil
			}),
		grpc.WithKeepaliveParams(cKeepalive),
	)
	log.Infof(ctx, "dialing server")
	conn, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, DefaultClass).Connect(ctx)
	if err != nil {
		return err
	}

	// Create the heartbeat client.
	log.Infof(ctx, "starting heartbeat client")
	unlockedHeartbeatClient, err := NewTestingHeartbeatStreamClient(conn).PingStream(ctx)
	if err != nil {
		return err
	}
	heartbeatClient := &lockedPingStreamClient{
		TestingHeartbeatStream_PingStreamClient: unlockedHeartbeatClient,
	}

	// Perform an initial request-response round trip.
	log.Infof(ctx, "first ping")
	request := PingRequest{ServerVersion: clientCtx.Settings.Version.BinaryVersion()}
	if err := heartbeatClient.Send(&request); err != nil {
		return err
	}
	if _, err := heartbeatClient.Recv(); err != nil {
		return err
	}

	// Launch a goroutine to read from the channel continuously and
	// a goroutine to write to the channel continuously. Both will
	// exit when the channel breaks (either because of a partition
	// or because the stopper stops).
	go func() {
		t := time.NewTicker(msgInterval)
		defer t.Stop()
		for {
			<-t.C
			log.Infof(ctx, "client send")
			if err := heartbeatClient.Send(&request); err != nil {
				return
			}
		}
	}()
	go func() {
		for {
			log.Infof(ctx, "client recv")
			if _, err := heartbeatClient.Recv(); err != nil {
				return
			}
		}
	}()

	// Now partition either client->server, server->client, or both, and attempt
	// to perform an RPC. We expect it to fail once the grpc keepalive fails to
	// get a response from the server.
	transportConn := <-dialerCh
	defer transportConn.Finish()
	if c.partitionC2S {
		log.Infof(ctx, "partition C2S")
		transportConn.PartitionC2S()
	}
	if c.partitionS2C {
		log.Infof(ctx, "partition S2C")
		transportConn.PartitionS2C()
	}

	// We want to start a goroutine that keeps trying to send requests and reports
	// the error from the send call. In cases where there are no keep-alives this
	// request may get blocked if flow control blocks it.
	errChan := make(chan error)
	sendCtx, cancel := context.WithCancel(ctx)
	r := retry.StartWithCtx(sendCtx, retry.Options{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     500 * time.Millisecond,
	})
	defer cancel()
	go func() {
		for r.Next() {
			err := heartbeatClient.Send(&request)
			isClosed := err != nil && grpcutil.IsClosedConnection(err)
			log.Infof(ctx, "heartbeat Send got error %+v (closed=%v)", err, isClosed)
			select {
			case errChan <- err:
			case <-sendCtx.Done():
				return
			}
			if isClosed {
				return
			}
		}
	}()
	// Check whether the connection eventually closes. We may need to
	// adjust this duration if the test gets flaky.
	// This unfortunately massive amount of time is required due to gRPC's
	// minimum timeout of 10s and the below issue whereby keepalives are sent
	// at half the expected rate.
	// https://github.com/grpc/grpc-go/issues/2638
	const timeoutDur = 21 * time.Second
	timeout := time.After(timeoutDur)
	// sendErr will hold the last error we saw from an attempt to send a
	// heartbeat. Initialize it with a dummy error which will fail the test if
	// it is not overwritten.
	sendErr := fmt.Errorf("not a real error")
	for done := false; !done; {
		select {
		case <-timeout:
			cancel()
			done = true
		case sendErr = <-errChan:
		}
	}
	if c.expClose {
		if sendErr == nil || !grpcutil.IsClosedConnection(sendErr) {
			newErr := errors.Newf("expected closed connection, found %v", sendErr) // nolint:errwrap
			log.Infof(ctx, "%+v", newErr)
			return newErr
		}
	} else {
		if sendErr != nil {
			newErr := errors.Newf("expected unclosed connection, found %v", sendErr) // nolint:errwrap
			log.Infof(ctx, "%+v", newErr)
			return newErr
		}
	}

	// If the DialOptions we passed to gRPC didn't prevent it from opening new
	// connections, then next RPCs would succeed since gRPC reconnects the
	// transport (and that would succeed here since we've only partitioned one
	// connection). We could further test that the status reported by
	// Context.ConnHealth() for the remote node moves to UNAVAILABLE because of
	// the (application-level) heartbeats performed by rpc.Context, but the
	// behavior of our heartbeats in the face of transport failures is
	// sufficiently tested in TestHeartbeatHealthTransport.
	log.Infof(ctx, "test done")
	return nil
}

// TestGRPCDeadlinePropagation is a smoketest for gRPC deadline propagation.
// When RPC clients issue requests with deadlines/timeouts attached to their
// context, they are guaranteed that not only will remote RPCs respect this
// deadline/timeout if it is reached, but that the remote RPC will be aware of
// the timeout throughout its lifetime. In other words, deadlines/timeouts are
// communicated upfront by the client, not only after they have been reached.
//
// gRPC implements this through its "grpc-timeout" header field, which is
// attached to the header (first) frame of unary and streaming calls.
//
// For more, see https://grpc.io/docs/what-is-grpc/core-concepts/#deadlines,
// which says:
// > gRPC allows clients to specify how long they are willing to wait for an RPC
// > to complete before the RPC is terminated with a DEADLINE_EXCEEDED error. On
// > the server side, the server can query to see if a particular RPC has timed
// > out, or how much time is left to complete the RPC.
func TestGRPCDeadlinePropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clusterID := uuid.MakeV4()
	clock := timeutil.NewManualTime(timeutil.Unix(0, 20))
	maxOffset := time.Duration(0)

	// Construct the server context.
	const serverNodeID = 1
	serverCtx := newTestContext(clusterID, clock, maxOffset, stopper)
	serverCtx.NodeID.Set(ctx, serverNodeID)

	// Register an UnknownServiceHandler that expects a BatchRequest and sends
	// a BatchResponse. Record the context deadline of the request in the handler.
	var serverDeadline time.Time
	s := newTestServer(t, serverCtx, grpc.UnknownServiceHandler(
		func(srv interface{}, stream grpc.ServerStream) error {
			serverDeadline, _ = stream.Context().Deadline()
			var ba kvpb.BatchRequest
			if err := stream.RecvMsg(&ba); err != nil {
				return err
			}
			return stream.SendMsg(&kvpb.BatchResponse{})
		},
	))
	RegisterHeartbeatServer(s, serverCtx.NewHeartbeatService())

	// Begin listening.
	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	require.Nil(t, err)
	remoteAddr := ln.Addr().String()

	// Construct the client context.
	clientCtx := newTestContext(clusterID, clock, maxOffset, stopper)
	defConn, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, DefaultClass).Connect(ctx)
	require.Nil(t, err)

	// Issue an RPC with a deadline far in the future.
	clientDeadline := timeutil.Now().Add(4 * time.Hour)
	ctxWithDeadline, cancel := context.WithDeadline(ctx, clientDeadline)
	defer cancel()

	desc := grpc.StreamDesc{
		StreamName:    "foo",
		ClientStreams: true,
	}
	const method = "/cockroach.rpc.Testing/Foo"
	cs, err := defConn.NewStream(ctxWithDeadline, &desc, method)
	require.Nil(t, err)
	require.Nil(t, cs.SendMsg(&kvpb.BatchRequest{}))
	var br kvpb.BatchResponse
	require.Nil(t, cs.RecvMsg(&br))
	require.Nil(t, cs.CloseSend())

	// The server should have heard about the deadline, and it should be nearly
	// identical to the client-side deadline. The values aren't exactly the same
	// because the deadline (a fixed point in time) passes through a timeout (a
	// duration of time) over the wire. However, we can assert that the client
	// deadline is always earlier than the server deadline, but by no more than a
	// small margin (relative to the 4-hour timeout).
	require.NotZero(t, serverDeadline)
	require.True(t, clientDeadline.Before(serverDeadline))
	require.True(t, serverDeadline.Before(clientDeadline.Add(1*time.Minute)))
}

func TestClusterIDMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := timeutil.NewManualTime(timeutil.Unix(0, 20))
	maxOffset := time.Duration(0)
	serverCtx := newTestContext(uuid.MakeV4(), clock, maxOffset, stopper)
	const serverNodeID = 1
	serverCtx.NodeID.Set(ctx, serverNodeID)
	s := newTestServer(t, serverCtx)
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		clusterID:          serverCtx.StorageClusterID,
		nodeID:             serverCtx.NodeID,
		version:            serverCtx.Settings.Version,
	})

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	remoteAddr := ln.Addr().String()

	// Ensure the client ctx gets a new fresh cluster ID so it becomes
	// different from the server's.
	clientCtx := newTestContext(uuid.MakeV4(), clock, maxOffset, stopper)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			_, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, DefaultClass).Connect(ctx)
			expected := "doesn't match server cluster ID"
			if !testutils.IsError(err, expected) {
				t.Errorf("expected %s error, got %v", expected, err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestClusterNameMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clock := timeutil.NewManualTime(timeutil.Unix(0, 20))
	maxOffset := time.Duration(0)

	testData := []struct {
		serverName             string
		serverDisablePeerCheck bool
		clientName             string
		clientDisablePeerCheck bool
		expectedErr            string
	}{
		{"", false, "", false, ``},
		// The name check is enabled if both the client and server want it.
		{"a", false, "", false, `peer node expects cluster name "a", use --cluster-name to configure`},
		{"", false, "a", false, `peer node does not have a cluster name configured, cannot use --cluster-name`},
		{"a", false, "b", false, `local cluster name "b" does not match peer cluster name "a"`},
		// It's disabled if either doesn't want it.
		// However in any case if the name is not empty it has to match.
		{"a", true, "", false, ``},
		{"", true, "a", false, ``},
		{"a", true, "b", false, ``},
		{"a", false, "", true, ``},
		{"", false, "a", true, ``},
		{"a", false, "b", true, ``},
		{"a", true, "", true, ``},
		{"", true, "a", true, ``},
		{"a", true, "b", true, ``},
	}

	for i, c := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(context.Background())

			serverCtx := newTestContext(uuid.MakeV4(), clock, maxOffset, stopper)
			serverCtx.Config.ClusterName = c.serverName
			serverCtx.Config.DisableClusterNameVerification = c.serverDisablePeerCheck

			s := newTestServer(t, serverCtx)
			RegisterHeartbeatServer(s, &HeartbeatService{
				clock:                          clock,
				remoteClockMonitor:             serverCtx.RemoteClocks,
				clusterID:                      serverCtx.StorageClusterID,
				nodeID:                         serverCtx.NodeID,
				version:                        serverCtx.Settings.Version,
				clusterName:                    serverCtx.Config.ClusterName,
				disableClusterNameVerification: serverCtx.Config.DisableClusterNameVerification,
			})

			ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
			if err != nil {
				t.Fatal(err)
			}
			remoteAddr := ln.Addr().String()

			clientCtx := newTestContext(serverCtx.StorageClusterID.Get(), clock, maxOffset, stopper)
			clientCtx.Config.ClusterName = c.clientName
			clientCtx.Config.DisableClusterNameVerification = c.clientDisablePeerCheck

			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(expectedErr string) {
					_, err := clientCtx.GRPCUnvalidatedDial(remoteAddr).Connect(context.Background())
					if !testutils.IsError(err, expectedErr) {
						t.Errorf("expected %s error, got %v", expectedErr, err)
					}
					wg.Done()
				}(c.expectedErr)
			}
			wg.Wait()
		})
	}
}

func TestNodeIDMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	clock := timeutil.NewManualTime(timeutil.Unix(0, 20))
	maxOffset := time.Duration(0)
	serverCtx := newTestContext(clusterID, clock, maxOffset, stopper)
	serverCtx.NodeID.Set(ctx, 1)
	s := newTestServer(t, serverCtx)
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		clusterID:          serverCtx.StorageClusterID,
		nodeID:             serverCtx.NodeID,
		version:            serverCtx.Settings.Version,
	})

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	remoteAddr := ln.Addr().String()

	clientCtx := newTestContext(clusterID, clock, maxOffset, stopper)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			_, err := clientCtx.GRPCDialNode(remoteAddr, 2, DefaultClass).Connect(ctx)
			expected := "doesn't match server node ID"
			if !testutils.IsError(err, expected) {
				t.Errorf("expected %s error, got %v", expected, err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func setVersion(c *Context, v roachpb.Version) error {
	st := cluster.MakeTestingClusterSettingsWithVersions(v, v, true /* initializeVersion */)
	c.Settings = st
	return nil
}

// Test that GRPCDial fails if there is a version incompatibility in either
// direction (client -> server or server -> client).
func TestVersionCheckBidirectional(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	v1 := roachpb.Version{Major: 1}
	v2 := clusterversion.TestingBinaryVersion

	testData := []struct {
		name          string
		serverVersion roachpb.Version
		clientVersion roachpb.Version
		expectError   bool
	}{
		{"serverVersion == clientVersion", v1, v1, false},
		{"serverVersion < clientVersion", v1, v2, true},
		{"serverVersion > clientVersion", v2, v1, true},
	}

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	for _, td := range testData {
		t.Run(td.name, func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(context.Background())

			clock := timeutil.NewManualTime(timeutil.Unix(0, 20))
			maxOffset := time.Duration(0)
			serverCtx := newTestContext(clusterID, clock, maxOffset, stopper)
			const serverNodeID = 1
			serverCtx.NodeID.Set(context.Background(), serverNodeID)
			if err := setVersion(serverCtx, td.serverVersion); err != nil {
				t.Fatal(err)
			}
			s := newTestServer(t, serverCtx)
			RegisterHeartbeatServer(s, &HeartbeatService{
				clock:              clock,
				remoteClockMonitor: serverCtx.RemoteClocks,
				clusterID:          serverCtx.StorageClusterID,
				nodeID:             serverCtx.NodeID,
				version:            serverCtx.Settings.Version,
			})

			ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
			if err != nil {
				t.Fatal(err)
			}
			remoteAddr := ln.Addr().String()

			clientCtx := newTestContext(clusterID, clock, maxOffset, stopper)
			if err := setVersion(clientCtx, td.clientVersion); err != nil {
				t.Fatal(err)
			}

			_, err = clientCtx.GRPCDialNode(remoteAddr, serverNodeID, DefaultClass).Connect(ctx)

			if td.expectError {
				expected := "cluster requires at least version"
				if !testutils.IsError(err, expected) {
					t.Errorf("expected %s error, got %v", expected, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %s", err)
			}
		})
	}
}

// TestGRPCDialClass ensures that distinct connections are constructed when
// dialing the same target with different classes.
func TestGRPCDialClass(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := timeutil.NewManualTime(timeutil.Unix(0, 20))
	maxOffset := time.Duration(0)
	serverCtx := newTestContext(uuid.MakeV4(), clock, maxOffset, stopper)
	const serverNodeID = 1
	serverCtx.NodeID.Set(context.Background(), serverNodeID)
	s := newTestServer(t, serverCtx)
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		clusterID:          serverCtx.StorageClusterID,
		nodeID:             serverCtx.NodeID,
		version:            serverCtx.Settings.Version,
	})

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	require.Nil(t, err)
	remoteAddr := ln.Addr().String()
	clientCtx := newTestContext(serverCtx.StorageClusterID.Get(), clock, maxOffset, stopper)

	def1 := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, DefaultClass)
	sys1 := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, SystemClass)
	require.False(t, sys1 == def1,
		"expected connections dialed with different classes to the same target to differ")
	defConn1, err := def1.Connect(context.Background())
	require.Nil(t, err, "expected successful connection")
	sysConn1, err := sys1.Connect(context.Background())
	require.Nil(t, err, "expected successful connection")
	require.False(t, sysConn1 == defConn1, "expected connections dialed with "+
		"different classes to the sametarget to have separate underlying gRPC connections")
	def2 := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, DefaultClass)
	require.True(t, def1 == def2, "expected connections dialed with the same "+
		"class to the same target to be the same")
	sys2 := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, SystemClass)
	require.True(t, sys1 == sys2, "expected connections dialed with the same "+
		"class to the same target to be the same")
	for _, c := range []*Connection{def2, sys2} {
		require.Nil(t, c.Health(), "expected connections to be healthy")
	}
}

// TestTestingKnobs ensures that the testing knobs are injected in the proper
// places.
func TestTestingKnobs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clusterID := uuid.MakeV4()

	clock := timeutil.NewManualTime(timeutil.Unix(0, 20))
	maxOffset := time.Duration(0)
	serverCtx := newTestContext(clusterID, clock, maxOffset, stopper)
	const serverNodeID = 1
	serverCtx.NodeID.Set(ctx, serverNodeID)
	// Register an UnknownServiceHandler that expects a BatchRequest and sends
	// a BatchResponse. It will be used both as a unary and stream handler below.
	s := newTestServer(t, serverCtx, grpc.UnknownServiceHandler(
		func(srv interface{}, stream grpc.ServerStream) error {
			var ba kvpb.BatchRequest
			if err := stream.RecvMsg(&ba); err != nil {
				return err
			}
			return stream.SendMsg(&kvpb.BatchResponse{})
		},
	))
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		clusterID:          serverCtx.StorageClusterID,
		nodeID:             serverCtx.NodeID,
		version:            serverCtx.Settings.Version,
	})

	// The test will inject interceptors for both stream and unary calls and then
	// will ensure that these interceptors are properly called by keeping track
	// of all calls.

	// Use these structs to keep track of the number of times the interceptors
	// are called in the seen map below.
	type streamCall struct {
		target string
		class  ConnectionClass
		method string
	}
	seen := make(map[interface{}]int)
	var seenMu syncutil.Mutex
	recordCall := func(call interface{}) {
		seenMu.Lock()
		defer seenMu.Unlock()
		seen[call]++
	}
	clientCtx := newTestContextWithKnobs(clock, maxOffset, stopper, ContextTestingKnobs{
		StorageClusterID: &clusterID,
		StreamClientInterceptor: func(
			target string, class ConnectionClass,
		) grpc.StreamClientInterceptor {
			return func(
				ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
				method string, streamer grpc.Streamer, opts ...grpc.CallOption,
			) (grpc.ClientStream, error) {
				cs, err := streamer(ctx, desc, cc, method, opts...)
				if err != nil {
					return nil, err
				}
				recordCall(streamCall{
					target: target,
					class:  class,
					method: method,
				})
				return cs, nil
			}
		},
	})

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	require.Nil(t, err)
	remoteAddr := ln.Addr().String()
	defConn, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, DefaultClass).Connect(ctx)
	require.Nil(t, err)
	const streamMethod = "/cockroach.rpc.Testing/Bar"
	const numDefStream = 4
	for i := 0; i < numDefStream; i++ {
		desc := grpc.StreamDesc{
			StreamName:    "bar",
			ClientStreams: true,
		}
		cs, err := defConn.NewStream(ctx, &desc, streamMethod)
		require.Nil(t, err)
		require.Nil(t, cs.SendMsg(&kvpb.BatchRequest{}))
		var br kvpb.BatchResponse
		require.Nil(t, cs.RecvMsg(&br))
		require.Nil(t, cs.CloseSend())
	}

	exp := map[interface{}]int{
		streamCall{
			target: remoteAddr,
			class:  DefaultClass,
			method: streamMethod,
		}: numDefStream,
	}
	seenMu.Lock()
	defer seenMu.Unlock()
	for call, num := range exp {
		require.Equal(t, num, seen[call])
	}
}

func BenchmarkGRPCDial(b *testing.B) {
	skip.UnderShort(b, "TODO: fix benchmark")
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)

	clock := &timeutil.DefaultTimeSource{}
	maxOffset := 250 * time.Millisecond
	rpcCtx := newTestContext(uuid.MakeV4(), clock, maxOffset, stopper)
	const serverNodeID = 1
	rpcCtx.NodeID.Set(ctx, serverNodeID)

	s := newTestServer(b, rpcCtx)
	ln, err := netutil.ListenAndServeGRPC(rpcCtx.Stopper, s, util.TestAddr)
	if err != nil {
		b.Fatal(err)
	}
	remoteAddr := ln.Addr().String()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := rpcCtx.grpcDialRaw(ctx, remoteAddr, DefaultClass)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestRejectDialOnQuiesce(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	clusID := uuid.MakeV4()
	clock := &timeutil.DefaultTimeSource{}
	maxOffset := 250 * time.Millisecond
	// Run vanilla server.
	const serverNodeID = 1
	var addr string
	{
		srvStopper := stop.NewStopper()
		defer srvStopper.Stop(ctx)
		serverCtx := newTestContext(clusID, clock, maxOffset, srvStopper)
		serverCtx.NodeID.Set(ctx, serverNodeID)
		s, err := NewServer(serverCtx)
		require.NoError(t, err)
		ln, err := netutil.ListenAndServeGRPC(srvStopper, s, util.TestAddr)
		require.NoError(t, err)
		addr = ln.Addr().String()
	}

	// Set up client.
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	rpcCtx := newTestContext(clusID, clock, maxOffset, stopper)

	// Set up one connection before quiesce to exercise the path in which
	// the connection already exists and is healthy when the node shuts
	// down.
	conn, err := rpcCtx.GRPCDialNode(addr, serverNodeID, SystemClass).Connect(ctx)
	require.NoError(t, err)
	// Check that we can reach the server (though we don't bother to also register
	// an endpoint on it; getting codes.Unimplemented back is proof enough).
	err = conn.Invoke(ctx, "/Does/Not/Exist", &PingRequest{}, &PingResponse{})
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err))

	// Now quiesce.
	stopper.Quiesce(ctx)

	// First, we shouldn't be able to dial again, even though we already have a
	// connection.
	_, err = rpcCtx.GRPCDialNode(addr, serverNodeID, SystemClass).Connect(ctx)
	require.ErrorIs(t, err, errDialRejected)
	require.True(t, grpcutil.IsConnectionRejected(err))
	require.True(t, grpcutil.IsAuthError(err))

	// If we use the existing connection, we'll get an error. It won't be the
	// ideal one - it's an opaque grpc version of `context.Canceled` but it's
	// the best we can hope for, since this is entirely within gRPC code.
	// This is good enough to prevent hangs since callers won't be using
	// this connection going forward; they need to call .Connect(ctx) again
	// which gives the permanent error PermissionDenied.
	err = conn.Invoke(ctx, "/Does/Not/Exist", &PingRequest{}, &PingResponse{})
	require.Error(t, err)
	require.Equal(t, codes.Canceled, status.Code(err))
}

// TestOnlyOnceDialer verifies that onlyOnceDialer prevents gRPC from re-dialing
// on an existing connection. (Instead, gRPC will have to make a new dialer).
func TestOnlyOnceDialer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	ln, err := net.Listen(util.TestAddr.Network(), util.TestAddr.String())
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	ood := &onlyOnceDialer{}
	conn, err := ood.dial(ctx, ln.Addr().String())
	require.NoError(t, err)
	_ = conn.Close()

	for i := 0; i < 5; i++ {
		_, err := ood.dial(ctx, ln.Addr().String())
		if i == 0 {
			require.True(t, errors.Is(err, grpcutil.ErrConnectionInterrupted), "i=%d: %+v", i, err)
		} else {
			require.ErrorContains(t, err, `gRPC connection unexpectedly re-dialed`)
		}
	}
}

type trackingListener struct {
	net.Listener
	mu          syncutil.Mutex
	connections []net.Conn
	closed      bool
}

func (d *trackingListener) Accept() (net.Conn, error) {
	c, err := d.Listener.Accept()

	d.mu.Lock()
	defer d.mu.Unlock()
	// If we get any trailing accepts after we close, just close the connection immediately.
	if err == nil {
		if d.closed {
			_ = c.Close()
		} else {
			d.connections = append(d.connections, c)
		}
	}
	return c, err
}

func (d *trackingListener) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	for _, c := range d.connections {
		_ = c.Close()
	}
	err := d.Listener.Close()
	if err != nil {
		return err
	}
	return nil
}

func newRegisteredServer(
	t testing.TB, stopper *stop.Stopper, clusterID uuid.UUID, nodeID roachpb.NodeID,
) (*Context, string, chan *PingRequest, *trackingListener) {
	clock := timeutil.NewManualTime(timeutil.Unix(0, 1))
	// We don't want to stall sending to this channel.
	pingChan := make(chan *PingRequest, 5)

	opts := ContextOptions{
		TenantID:        roachpb.SystemTenantID,
		Config:          testutils.NewNodeTestBaseContext(),
		Clock:           clock,
		ToleratedOffset: time.Duration(0),
		Stopper:         stopper,
		Settings:        cluster.MakeTestingClusterSettings(),
		NeedsDialback:   true,
		Knobs:           ContextTestingKnobs{NoLoopbackDialer: true},
	}
	// Heartbeat faster so we don't have to wait as long.
	opts.Config.RPCHeartbeatInterval = 10 * time.Millisecond
	opts.Config.RPCHeartbeatTimeout = 100 * time.Millisecond

	rpcCtx := NewContext(context.Background(), opts)
	// This is normally set up inside the server, we want to hold onto all PingRequests that come through.
	rpcCtx.OnIncomingPing = func(ctx context.Context, req *PingRequest, resp *PingResponse) error {
		pingChan <- req
		err := rpcCtx.VerifyDialback(ctx, req, resp, roachpb.Locality{})
		// On success store the ping to the channel for test analysis.
		return err
	}

	rpcCtx.NodeID.Set(context.Background(), nodeID)
	rpcCtx.StorageClusterID.Set(context.Background(), clusterID)
	s := newTestServer(t, rpcCtx)

	RegisterHeartbeatServer(s, rpcCtx.NewHeartbeatService())

	ln, err := net.Listen("tcp", util.TestAddr.String())
	require.Nil(t, err)
	tracker := trackingListener{Listener: ln}
	_ = stopper.RunAsyncTask(context.Background(), "serve", func(context.Context) {
		closeReason := s.Serve(&tracker)
		log.Infof(context.Background(), "Closed listener with reason %v", closeReason)
	})

	addr := ln.Addr().String()
	log.Infof(context.Background(), "Listening on %s", addr)
	// This needs to be set once we know our address so that ping requests have
	// the correct reverse addr in them.
	rpcCtx.Config.AdvertiseAddr = addr
	return rpcCtx, addr, pingChan, &tracker
}

// TestHeartbeatDialer verifies that unidirectional partitions are converted
// into bidirectional partitions. The test sets up two nodes that are pinging each
func TestHeartbeatDialback(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clusterID := uuid.MakeV4()

	ctx1, remoteAddr1, pingChan1, ln1 := newRegisteredServer(t, stopper, clusterID, 1)
	ctx2, remoteAddr2, pingChan2, ln2 := newRegisteredServer(t, stopper, clusterID, 2)
	defer func() { netutil.FatalIfUnexpected(ln1.Close()) }()
	defer func() { netutil.FatalIfUnexpected(ln2.Close()) }()

	// Test an incorrect remoteNodeID, this should fail with a heartbeat error.
	// This invariant is important to make sure we don't try and connect to the
	// wrong node.
	{
		_, err := ctx1.GRPCDialNode(remoteAddr2, 3, DefaultClass).Connect(ctx)
		var respErr *netutil.InitialHeartbeatFailedError
		require.ErrorAs(t, err, &respErr)
		// Verify no heartbeat received in either direction.
		require.Equal(t, 0, len(pingChan1))
		require.Equal(t, 0, len(pingChan2))
	}

	// Initiate connection from node 1 to node 2 which will create a dialback
	// connection back to 1. This will be a blocking connection since there is no
	// reverse connection.
	{
		conn, err := ctx1.GRPCDialNode(remoteAddr2, 2, DefaultClass).Connect(ctx)
		defer func() {
			_ = conn.Close() // nolint:grpcconnclose
		}()
		require.NoError(t, err)
		require.NotNil(t, conn)
		require.Equal(t, 1, len(pingChan2))
		pingReq := <-pingChan2
		require.Equal(t, PingRequest_BLOCKING, pingReq.NeedsDialback)
		require.Equal(t, 0, len(pingChan1))
	}

	//Now connect back in the opposite direction. This should not initiate any
	//dialback since we are already connected.
	{
		conn, err := ctx1.GRPCDialNode(remoteAddr2, 2, DefaultClass).Connect(ctx)
		defer func() {
			_ = conn.Close() // nolint:grpcconnclose
		}()
		require.NoError(t, err)
		require.NotNil(t, conn)
		// The reverse connection was already set up, but we are still blocking.
		pingReq := <-pingChan1
		require.Equal(t, PingRequest_BLOCKING, pingReq.NeedsDialback)

		// At this point, node 1 has a fully established connection to node 2, however node 2 has not yet finished connecting back.
		require.Equal(t, nil, ctx1.ConnHealth(remoteAddr2, 2, DefaultClass))
	}

	// Verify we get non-blocking requests in both directions now.
	require.Equal(t, PingRequest_NON_BLOCKING, (<-pingChan2).NeedsDialback)
	require.Equal(t, PingRequest_NON_BLOCKING, (<-pingChan1).NeedsDialback)

	// Verify we are fully healthy in both directions (note the dialback is on the
	// system class).
	require.Equal(t, nil, ctx1.ConnHealth(remoteAddr2, 2, DefaultClass))
	require.Equal(t, nil, ctx2.ConnHealth(remoteAddr1, 1, SystemClass))

	// Forcibly shut down listener 2 and the connection node1 -> node2.
	// Test the reverse connection also closes within ~RPCHeartbeatTimeout.
	log.Info(ctx, "Closing node 2 listener")
	_ = ln2.Close()

	// Wait for a few more pings to go through to make sure it has a chance to
	// shut down the reverse connection. Normally the connect attempt times out
	// immediately and returns an error, but occasionally it needs to wait for the
	// RPCHeartbeatTimeout (100 ms). Wait until pings have stopped in both
	// directions for at least 1 second before checking health.
	for {
		select {
		case ping := <-pingChan1:
			log.Infof(ctx, "Received %+v", ping)
		case ping := <-pingChan2:
			log.Infof(ctx, "Received %+v", ping)
		case <-time.After(1 * time.Second):
			require.ErrorAs(t, ctx1.ConnHealth(remoteAddr2, 2, DefaultClass), &ErrNoConnection)
			require.ErrorAs(t, ctx2.ConnHealth(remoteAddr1, 1, SystemClass), &ErrNoConnection)
			return
		}
	}
}

// TODO(baptist): Add a test using TestCluster to verify this works in a full
// integration test.
