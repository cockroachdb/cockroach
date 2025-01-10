// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/grpcutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
	gogostatus "github.com/gogo/status"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
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
	conn := rpcCtx.GRPCDialNode(target, nodeID, roachpb.Locality{}, DefaultClass)
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
	opts := DefaultContextOptions()
	opts.Clock = clock
	opts.ToleratedOffset = maxOffset
	opts.Stopper = stopper
	opts.Knobs = knobs
	opts.Settings = cluster.MakeTestingClusterSettings()

	// Make tests faster.
	opts.RPCHeartbeatInterval = 10 * time.Millisecond
	opts.RPCHeartbeatTimeout = 0

	return NewContext(context.Background(), opts)
}

func newTestContext(
	storageClusterID uuid.UUID, clock hlc.WallClock, maxOffset time.Duration, stopper *stop.Stopper,
) *Context {
	return newTestContextWithKnobs(clock, maxOffset, stopper, ContextTestingKnobs{
		StorageClusterID: &storageClusterID,
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
	opts := DefaultContextOptions()
	opts.ToleratedOffset = 500 * time.Millisecond
	opts.Stopper = stop.NewStopper()
	opts.Settings = cluster.MakeTestingClusterSettings()
	opts.OnOutgoingPing = func(ctx context.Context, req *PingRequest) error {
		if req.TargetNodeID == blockedTargetNodeID {
			return errBoomSend
		}
		return nil
	}
	opts.OnIncomingPing = func(ctx context.Context, req *PingRequest, resp *PingResponse) error {
		if req.OriginNodeID == blockedOriginNodeID {
			return errBoomRecv
		}
		return nil
	}

	defer opts.Stopper.Stop(ctx)

	rpcCtx := NewContext(ctx, opts)
	{
		_, err := rpcCtx.GRPCDialNode("unused:1234", 5, roachpb.Locality{}, SystemClass).Connect(ctx)
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
		_, err := rpcCtx.GRPCDialNode(remoteAddr, blockedOriginNodeID, roachpb.Locality{}, SystemClass).Connect(ctx)
		require.True(t, errors.HasType(err, errBoomRecv))
		st, ok := status.FromError(errors.UnwrapAll(err))
		require.True(t, ok)
		require.Equal(t, codes.FailedPrecondition, st.Code())
		require.Equal(t, recvMsg, st.Message())
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
	opts := DefaultContextOptions()
	opts.ToleratedOffset = 500 * time.Millisecond
	opts.Stopper = stopper
	opts.Settings = cluster.MakeTestingClusterSettings()
	rpcCtxServer := NewContext(ctx, opts)

	clientOpts := opts
	// Experimentally, values below 50ms seem to incur flakiness.
	clientOpts.RPCHeartbeatInterval = 100 * time.Millisecond
	clientOpts.RPCHeartbeatTimeout = 200 * time.Millisecond
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
	testutils.SucceedsSoon(t, func() error {
		_, err = rpcCtxClient.GRPCDialNode(
			remoteAddr, 1, roachpb.Locality{}, SystemClass,
		).Connect(ctx)
		return err
	})

	// The first ping establishes the TCP+TLS connection and uses a blocking dialback,
	// so it's usually pretty noisy in terms of detecting clock offsets. The second
	// ping thus has no previous useful offset either, but it returns measurements
	// then used by the third ping.
	t.Logf("first two pings check")
	for i := 0; i < 2; i++ {
		firstPing := <-pings
		require.Zero(t, firstPing.Offset.Offset)
		require.Zero(t, firstPing.Offset.Uncertainty)
		require.Zero(t, firstPing.Offset.MeasuredAt)
	}
	for i := 1; i < 3; i++ {
		t.Logf("ping %d check", i)
		var nextPing PingRequest
		select {
		case nextPing = <-pings:
		case <-time.After(testutils.DefaultSucceedsSoonDuration):
			t.Fatalf("timed out waiting for ping #%d", i)
		}
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
	muxRfServerStream kvpb.Internal_MuxRangeFeedServer
}

type rangefeedEventSink struct {
	ctx    context.Context
	stream kvpb.Internal_MuxRangeFeedServer
}

var _ kvpb.RangeFeedEventSink = (*rangefeedEventSink)(nil)

// Note that SendUnbuffered itself is not thread-safe (grpc stream is not
// thread-safe), but tests were written in a way that sends sequentially,
// ensuring thread-safety for SendUnbuffered.
func (s *rangefeedEventSink) SendUnbufferedIsThreadSafe() {}

func (s *rangefeedEventSink) SendUnbuffered(event *kvpb.RangeFeedEvent) error {
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

func (*internalServer) BatchStream(stream kvpb.Internal_BatchStreamServer) error {
	panic("unimplemented")
}

func (*internalServer) RangeLookup(
	context.Context, *kvpb.RangeLookupRequest,
) (*kvpb.RangeLookupResponse, error) {
	panic("unimplemented")
}

func (s *internalServer) singleRangeFeed(sink kvpb.RangeFeedEventSink) error {
	for _, ev := range s.rangeFeedEvents {
		evCpy := ev
		if err := sink.SendUnbuffered(&evCpy); err != nil {
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
	serverCtx.AdvertiseAddr = "127.0.0.1:8888"
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
	serverCtx.AdvertiseAddr = "127.0.0.1:8888"
	serverCtx.NodeID.Set(context.Background(), 1)

	_ /* gRPC server */, _ /* drpc server */, serverInterceptors, err := NewServerEx(ctx, serverCtx)
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
		stream, err := lic.MuxRangeFeed(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&kvpb.RangeFeedRequest{}))
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
	serverCtx.AdvertiseAddr = "127.0.0.1:8888"
	serverCtx.NodeID.Set(context.Background(), 1)

	_ /* gRPC server */, _ /* drpc server */, serverInterceptors, err := NewServerEx(ctx, serverCtx)
	require.NoError(t, err)
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
	stream, err := lic.MuxRangeFeed(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&kvpb.RangeFeedRequest{}))
	receiveEvent = func() error {
		e, err := stream.Recv()
		_ = e
		return err
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
	serverCtx.AdvertiseAddr = "127.0.0.1:8888"
	serverCtx.NodeID.Set(context.Background(), 1)

	_ /* gRPC server */, _ /* drpc server */, serverInterceptors, err := NewServerEx(ctx, serverCtx)
	require.NoError(t, err)

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
	stream, err := lic.MuxRangeFeed(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&kvpb.RangeFeedRequest{}))
	receiveEvent = func() error {
		_, err := stream.Recv()
		return err
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
	require.IsType(t, muxRangeFeedServerAdapter{}, internal.muxRfServerStream)
	ss = internal.muxRfServerStream.(muxRangeFeedServerAdapter).ServerStream
	require.IsType(t, &testServerStream{}, ss)
	topStream := ss.(*testServerStream)
	require.Equal(t, int2Name, topStream.name)
	require.IsType(t, &testServerStream{}, topStream.inner)
	bottomStream := topStream.inner.(*testServerStream)
	require.Equal(t, int1Name, bottomStream.name)
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
	serverCtx.AdvertiseAddr = "127.0.0.1:8888"
	serverCtx.NodeID.Set(context.Background(), 1)

	_ /* gRPC server */, _ /* drpc server */, interceptors, err := NewServerEx(ctx, serverCtx)
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

// TestConnectLoopback verifies that we correctly go through the
// internal server when dialing the local address.
func TestConnectLoopback(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := timeutil.NewManualTime(timeutil.Unix(0, 1))
	maxOffset := time.Duration(0)

	clusterID := uuid.MakeV4()
	const nodeID = 1

	ctx := context.Background()
	clientCtx := newTestContext(clusterID, clock, maxOffset, stopper)
	clientCtx.NodeID.Set(context.Background(), nodeID)
	loopLn := netutil.NewLoopbackListener(ctx, stopper)
	clientCtx.loopbackDialFn = func(ctx context.Context) (net.Conn, error) {
		return loopLn.Connect(ctx)
	}
	// Ensure that there's no error connecting to the local node when an internal
	// server has been registered.
	clientCtx.SetLocalInternalServer(
		&internalServer{},
		ServerInterceptorInfo{}, ClientInterceptorInfo{})

	sTCP := newTestServer(t, clientCtx)
	sLoopback := newTestServer(t, clientCtx)
	errLoopback := gogostatus.Newf(codes.DataLoss, "loopback!").Err()
	RegisterHeartbeatServer(sLoopback, &ManualHeartbeatService{readyFn: func() error {
		return errLoopback
	}})
	require.NoError(t, stopper.RunAsyncTask(ctx, "listen-server", func(ctx context.Context) {
		netutil.FatalIfUnexpected(sLoopback.Serve(loopLn))
	}))
	ln, err := netutil.ListenAndServeGRPC(stopper, sTCP, util.TestAddr)
	require.NoError(t, err)

	addr := ln.Addr().String()
	clientCtx.AdvertiseAddr = addr

	// Connect and get the error that comes from loopbackLn, proving that we were routed there.
	_, err = clientCtx.GRPCDialNode(addr, nodeID, roachpb.Locality{}, DefaultClass).Connect(ctx)
	require.Equal(t, codes.DataLoss, gogostatus.Code(errors.UnwrapAll(err)), "%+v", err)
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
	clientClock := NewAdvancingClock(timeutil.Unix(0, 10))
	clientMaxOffset := time.Duration(0)
	clientCtx := newTestContext(clusterID, clientClock, clientMaxOffset, stopper)
	clientCtx.RemoteClocks.offsetTTL = 1 * time.Nanosecond
	if _, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, roachpb.Locality{}, DefaultClass).Connect(ctx); err != nil {
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
	clientClock.setAdvancementInterval(1 * time.Nanosecond)

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
	clientCtx.RPCHeartbeatTimeout = 0
	// Allow two heartbeat for initialization. The first ping doesn't report an offset,
	// the second one thus doesn't have an offset to work with, so it's only on the
	// third one that's fully configured.
	go func() {
		heartbeat.ready <- nil
		heartbeat.ready <- nil
	}()
	if _, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, roachpb.Locality{}, DefaultClass).Connect(ctx); err != nil {
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
	clientClock := NewAdvancingClock(timeutil.Unix(0, 10))
	clientMaxOffset := time.Duration(0)
	clientCtx := newTestContext(clusterID, clientClock, clientMaxOffset, stopper)

	var hbDecommission atomic.Value
	hbDecommission.Store(false)
	clientCtx.OnOutgoingPing = func(ctx context.Context, req *PingRequest) error {
		if hbDecommission.Load().(bool) {
			return kvpb.NewDecommissionedStatusErrorf(
				codes.PermissionDenied, "injected decommissioned error for n%d", req.TargetNodeID,
			)
		}
		return nil
	}

	conn, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, roachpb.Locality{}, DefaultClass).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	anotherConn, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, roachpb.Locality{}, SystemClass).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	clientClock.setAdvancementInterval(1 * time.Nanosecond)

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
	hbDecommission.Store(true)
	testutils.SucceedsSoon(t, func() error {
		clientCtx.RemoteClocks.mu.Lock()
		defer clientCtx.RemoteClocks.mu.Unlock()

		if li, ok := clientCtx.RemoteClocks.mu.latencyInfos[serverNodeID]; ok {
			return errors.Errorf("expected to have removed latencyInfos, but found: %v", li)
		}
		return nil
	})
}

// AdvancingClock is a clock that advances by a fixed interval each time it is
// read. The advancement interval starts at 0.
type AdvancingClock struct {
	clock               *timeutil.ManualTime
	advancementInterval atomic.Int64 // time.Duration
}

var _ hlc.WallClock = &AdvancingClock{}

func NewAdvancingClock(initialTime time.Time) *AdvancingClock {
	return &AdvancingClock{
		clock: timeutil.NewManualTime(initialTime),
	}
}

func (ac *AdvancingClock) setAdvancementInterval(d time.Duration) {
	ac.advancementInterval.Store(int64(d))
}

func (ac *AdvancingClock) getAdvancementInterval() time.Duration {
	return time.Duration(ac.advancementInterval.Load())
}

func (ac *AdvancingClock) Now() time.Time {
	ac.clock.Advance(ac.getAdvancementInterval())
	return ac.clock.Now()
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
		// Make the test faster.
		nodeCtxs[i].ctx.RPCHeartbeatInterval = 10 * time.Millisecond
		// Disable RPC heartbeat timeouts to avoid flakiness in the test. If a
		// heartbeat were to time out, its RPC connection would be closed and its
		// clock offset information would be lost.
		nodeCtxs[i].ctx.RPCHeartbeatTimeout = 0
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
		nodeCtxs[i].ctx.AdvertiseAddr = ln.Addr().String()
	}

	// Fully connect the nodes.
	for i, clientNodeContext := range nodeCtxs {
		for j, serverNodeContext := range nodeCtxs {
			if i == j {
				continue
			}
			testutils.SucceedsSoon(t, func() error {
				if _, err := clientNodeContext.ctx.GRPCDialNode(
					serverNodeContext.ctx.AdvertiseAddr,
					serverNodeContext.ctx.NodeID.Get(),
					roachpb.Locality{},
					DefaultClass,
				).Connect(ctx); err != nil {
					return err
				}
				return nil
			})
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
	defConn, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, roachpb.Locality{}, DefaultClass).Connect(ctx)
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
			_, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, roachpb.Locality{}, DefaultClass).Connect(ctx)
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
			serverCtx.ContextOptions.ClusterName = c.serverName
			serverCtx.ContextOptions.DisableClusterNameVerification = c.serverDisablePeerCheck

			s := newTestServer(t, serverCtx)
			RegisterHeartbeatServer(s, &HeartbeatService{
				clock:                          clock,
				remoteClockMonitor:             serverCtx.RemoteClocks,
				clusterID:                      serverCtx.StorageClusterID,
				nodeID:                         serverCtx.NodeID,
				version:                        serverCtx.Settings.Version,
				clusterName:                    serverCtx.ContextOptions.ClusterName,
				disableClusterNameVerification: serverCtx.ContextOptions.DisableClusterNameVerification,
			})

			ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
			if err != nil {
				t.Fatal(err)
			}
			remoteAddr := ln.Addr().String()

			clientCtx := newTestContext(serverCtx.StorageClusterID.Get(), clock, maxOffset, stopper)
			clientCtx.ContextOptions.ClusterName = c.clientName
			clientCtx.ContextOptions.DisableClusterNameVerification = c.clientDisablePeerCheck

			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(expectedErr string) {
					_, err := clientCtx.GRPCUnvalidatedDial(remoteAddr, roachpb.Locality{}).Connect(context.Background())
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
			_, err := clientCtx.GRPCDialNode(remoteAddr, 2, roachpb.Locality{}, DefaultClass).Connect(ctx)
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
	v2 := clusterversion.Latest.Version()

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

			_, err = clientCtx.GRPCDialNode(remoteAddr, serverNodeID, roachpb.Locality{}, DefaultClass).Connect(ctx)

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

	def1 := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, roachpb.Locality{}, DefaultClass)
	sys1 := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, roachpb.Locality{}, SystemClass)
	require.False(t, sys1 == def1,
		"expected connections dialed with different classes to the same target to differ")
	defConn1, err := def1.Connect(context.Background())
	require.Nil(t, err, "expected successful connection")
	sysConn1, err := sys1.Connect(context.Background())
	require.Nil(t, err, "expected successful connection")
	require.False(t, sysConn1 == defConn1, "expected connections dialed with "+
		"different classes to the sametarget to have separate underlying gRPC connections")
	def2 := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, roachpb.Locality{}, DefaultClass)
	require.True(t, def1 == def2, "expected connections dialed with the same "+
		"class to the same target to be the same")
	sys2 := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, roachpb.Locality{}, SystemClass)
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
	defConn, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, roachpb.Locality{}, DefaultClass).Connect(ctx)
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
		s, err := NewServer(ctx, serverCtx)
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
	conn, err := rpcCtx.GRPCDialNode(addr, serverNodeID, roachpb.Locality{}, SystemClass).Connect(ctx)
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
	_, err = rpcCtx.GRPCDialNode(addr, serverNodeID, roachpb.Locality{}, SystemClass).Connect(ctx)
	require.ErrorIs(t, err, errQuiescing)
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
	t testing.TB,
	ctx context.Context,
	stopper *stop.Stopper,
	clusterID uuid.UUID,
	nodeID roachpb.NodeID,
) (*Context, string, *trackingListener) {
	clock := timeutil.NewManualTime(timeutil.Unix(0, 1))
	opts := DefaultContextOptions()
	opts.Clock = clock
	opts.ToleratedOffset = time.Duration(0)
	opts.Stopper = stopper
	opts.Settings = cluster.MakeTestingClusterSettings()
	opts.NeedsDialback = true
	opts.Knobs = ContextTestingKnobs{NoLoopbackDialer: true}

	// Heartbeat faster so we don't have to wait as long.
	opts.RPCHeartbeatInterval = 10 * time.Millisecond
	opts.RPCHeartbeatTimeout = 1000 * time.Millisecond

	rpcCtx := NewContext(context.Background(), opts)
	// This is normally set up inside the server, we want to hold onto all PingRequests that come through.
	rpcCtx.OnIncomingPing = func(ctx context.Context, req *PingRequest, resp *PingResponse) error {
		err := VerifyDialback(ctx, rpcCtx, req, resp, roachpb.Locality{}, &rpcCtx.Settings.SV)
		if err != nil {
			t.Logf("dialback error n%d->n%d @ %s: %s", nodeID, req.OriginNodeID, req.OriginAddr, err)
			return err
		}
		// On success store the ping to the channel for test analysis.
		t.Logf("dialback success n%d->n%d @ %s", nodeID, req.OriginNodeID, req.OriginAddr)
		return nil
	}

	rpcCtx.NodeID.Set(ctx, nodeID)
	rpcCtx.StorageClusterID.Set(ctx, clusterID)
	s := newTestServer(t, rpcCtx)

	RegisterHeartbeatServer(s, rpcCtx.NewHeartbeatService())

	ln, err := net.Listen("tcp", util.TestAddr.String())
	require.Nil(t, err)
	tracker := trackingListener{Listener: ln}
	stopper.OnQuiesce(func() { netutil.FatalIfUnexpected(ln.Close()) })

	rpcCtx.OnOutgoingPing = func(ctx context.Context, req *PingRequest) error { return nil }

	_ = stopper.RunAsyncTask(ctx, "serve", func(context.Context) {
		closeReason := s.Serve(&tracker)
		log.Infof(ctx, "Closed listener with reason %v", closeReason)
	})

	addr := ln.Addr().String()
	log.Infof(ctx, "Listening on %s", addr)
	// This needs to be set once we know our address so that ping requests have
	// the correct reverse addr in them.
	rpcCtx.AdvertiseAddr = addr
	return rpcCtx, addr, &tracker
}

// TestHeartbeatDialer verifies that unidirectional partitions are converted
// into bidirectional partitions. The test sets up two nodes that are pinging each
func TestHeartbeatDialback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clusterID := uuid.MakeV4()

	ctx1, remoteAddr1, _ := newRegisteredServer(t, context.Background(), stopper, clusterID, 1)
	ctx2, remoteAddr2, ln2 := newRegisteredServer(t, context.Background(), stopper, clusterID, 2)

	// Test an incorrect remoteNodeID, this should fail with a heartbeat error.
	// This invariant is important to make sure we don't try and connect to the
	// wrong node.
	{
		_, err := ctx1.GRPCDialNode(remoteAddr2, 3, roachpb.Locality{}, DefaultClass).Connect(ctx)
		var respErr *netutil.InitialHeartbeatFailedError
		require.ErrorAs(t, err, &respErr)
	}

	// Initiate connection from node 1 to node 2 which will create a dialback
	// connection back to 1. This will be a blocking connection since there is no
	// reverse connection.
	{
		conn, err := ctx1.GRPCDialNode(remoteAddr2, 2, roachpb.Locality{}, DefaultClass).Connect(ctx)
		require.NoError(t, err)
		require.NotNil(t, conn)
		defer func() {
			_ = conn.Close() // nolint:grpcconnclose
		}()
	}

	// Now connect back in the opposite direction. This should not initiate any
	// dialback since we are already connected.
	{
		conn, err := ctx1.GRPCDialNode(remoteAddr2, 2, roachpb.Locality{}, DefaultClass).Connect(ctx)
		defer func() {
			_ = conn.Close() // nolint:grpcconnclose
		}()
		require.NoError(t, err)
		require.NotNil(t, conn)
	}

	// The connection we established should be healthy.
	require.NoError(t, ctx1.ConnHealth(remoteAddr2, 2, DefaultClass))
	// The dialback connection (which uses the system class, in reverse direction)
	// becomes healthy "very soon". It may not be healthy immediately since the
	// first blocking heartbeat of the n1->n2 connection used a throwaway connection,
	// which may have completed sooner than the system-class connection which is
	// asynchronous.
	testutils.SucceedsSoon(t, func() error {
		return ctx2.ConnHealth(remoteAddr1, 1, SystemClass)
	})

	// Forcibly shut down listener 2 and the connection node1 -> node2.
	// Test the reverse connection also closes within ~RPCHeartbeatTimeout.
	t.Logf("Closing node 2 listener")
	_ = ln2.Close()

	// n1 -> n2 can not be healthy for much longer since the listener for n2 is
	// closed, and n2 should terminate its connection to n1 within ~100ms
	// (heartbeat interval in this test) since n1 can't dial-back.
	testutils.SucceedsSoon(t, func() error {
		errDef, errSys := ctx1.ConnHealth(remoteAddr2, 2, DefaultClass), ctx2.ConnHealth(remoteAddr1, 1, SystemClass)
		if errors.Is(errDef, ErrNotHeartbeated) {
			errDef = nil
		}
		if errors.Is(errSys, ErrNotHeartbeated) {
			errSys = nil
		}
		if errors.HasType(errDef, (*netutil.InitialHeartbeatFailedError)(nil)) {
			errDef = nil
		}
		if errors.HasType(errSys, (*netutil.InitialHeartbeatFailedError)(nil)) {
			errSys = nil
		}
		return errors.CombineErrors(errDef, errSys)
	})
}

func TestVerifyDialback(t *testing.T) {
	defer leaktest.AfterTest(t)()

	run := func(t *testing.T, name string, f func(t *testing.T, mockRPCCtx *MockDialbacker, sv *settings.Values)) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			f(t, NewMockDialbacker(ctrl), &cluster.MakeTestingClusterSettings().SV)
		})
	}

	mkConn := func() *Connection {
		c := &Connection{
			breakerSignalFn: func() circuit.Signal {
				return &neverTripSignal{}
			},
		}
		c.connFuture.ready = make(chan struct{})
		return c
	}
	ping := func(typ PingRequest_DialbackType) *PingRequest {
		return &PingRequest{
			OriginAddr:    "1.1.1.1",
			TargetNodeID:  1,
			OriginNodeID:  2,
			NeedsDialback: typ,
		}
	}

	run(t, "no-ops", func(t *testing.T, mockRPCCtx *MockDialbacker, sv *settings.Values) {
		// NONE always no-ops, the other modes do if setting is disabled.
		require.NoError(t, VerifyDialback(context.Background(), mockRPCCtx, ping(PingRequest_NONE),
			&PingResponse{}, roachpb.Locality{}, sv))
		enableRPCCircuitBreakers.Override(context.Background(), sv, false)
		require.NoError(t, VerifyDialback(context.Background(), mockRPCCtx, ping(PingRequest_BLOCKING),
			&PingResponse{}, roachpb.Locality{}, sv))
		require.NoError(t, VerifyDialback(context.Background(), mockRPCCtx, ping(PingRequest_NON_BLOCKING),
			&PingResponse{}, roachpb.Locality{}, sv))
	})

	// If reverse system class connection is healthy, VerifyDialback
	// hits the fast-path, returning success.
	for _, typ := range []PingRequest_DialbackType{PingRequest_BLOCKING, PingRequest_NON_BLOCKING} {
		run(t, fmt.Sprintf("fast-path/healthy/%s", typ),
			func(t *testing.T, mockRPCCtx *MockDialbacker, sv *settings.Values) {

				mockRPCCtx.EXPECT().GRPCDialNode("1.1.1.1", roachpb.NodeID(2), roachpb.Locality{}, SystemClass).
					DoAndReturn(func(string, roachpb.NodeID, roachpb.Locality, ConnectionClass) *Connection {
						healthyConn := mkConn()
						close(healthyConn.connFuture.ready)
						return healthyConn
					})

				require.NoError(t, VerifyDialback(context.Background(), mockRPCCtx, &PingRequest{
					OriginAddr:    "1.1.1.1",
					TargetNodeID:  1,
					OriginNodeID:  2,
					NeedsDialback: typ,
				}, &PingResponse{}, roachpb.Locality{}, sv))
			})
	}

	run(t, "fast-path/pending/NON_BLOCKING", func(t *testing.T, mockRPCCtx *MockDialbacker, sv *settings.Values) {
		// If reverse system class connection is not healthy, non-blocking dial attempt
		// interprets this as success.
		mockRPCCtx.EXPECT().GRPCDialNode("1.1.1.1", roachpb.NodeID(2), roachpb.Locality{}, SystemClass).
			DoAndReturn(func(string, roachpb.NodeID, roachpb.Locality, ConnectionClass) *Connection {
				tmpConn := mkConn()
				assert.Equal(t, ErrNotHeartbeated, tmpConn.Health())
				return tmpConn
			})
		require.NoError(t, VerifyDialback(context.Background(), mockRPCCtx, &PingRequest{
			OriginAddr:    "1.1.1.1",
			TargetNodeID:  1,
			OriginNodeID:  2,
			NeedsDialback: PingRequest_NON_BLOCKING,
		}, &PingResponse{}, roachpb.Locality{}, sv))
	})

	for _, dialbackOK := range []bool{true, false} {
		run(t, fmt.Sprintf("fast-path/pending/BLOCKING/success=%t", dialbackOK), func(t *testing.T, mockRPCCtx *MockDialbacker, sv *settings.Values) {
			// If reverse system class connection is not healthy, blocking dial attempt
			// will do a one-off dialback.
			mockRPCCtx.EXPECT().GRPCDialNode("1.1.1.1", roachpb.NodeID(2), roachpb.Locality{}, SystemClass).
				DoAndReturn(func(string, roachpb.NodeID, roachpb.Locality, ConnectionClass) *Connection {
					tmpConn := mkConn()
					assert.Equal(t, ErrNotHeartbeated, tmpConn.Health())
					return tmpConn
				})
			mockRPCCtx.EXPECT().wrapCtx(gomock.Any(), "1.1.1.1", roachpb.NodeID(2), SystemClass).DoAndReturn(func(
				ctx context.Context, _ string, _ roachpb.NodeID, _ ConnectionClass) context.Context {
				return ctx
			})
			mockRPCCtx.EXPECT().grpcDialRaw(gomock.Any() /* ctx */, "1.1.1.1", SystemClass, gomock.Any()).
				DoAndReturn(func(context.Context, string, ConnectionClass, ...grpc.DialOption) (*grpc.ClientConn, error) {
					if dialbackOK {
						return nil, nil
					}
					return nil, errors.New("boom")
				})
			err := VerifyDialback(context.Background(), mockRPCCtx, &PingRequest{
				OriginAddr:    "1.1.1.1",
				TargetNodeID:  1,
				OriginNodeID:  2,
				NeedsDialback: PingRequest_BLOCKING,
			}, &PingResponse{}, roachpb.Locality{}, sv)
			if dialbackOK {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}

	run(t, "without-nodeid", func(t *testing.T, mockRPCCtx *MockDialbacker, sv *settings.Values) {
		// If PingRequest has no OriginNodeID, we'll check the system-level connection using GRPCUnvalidatedDial
		// and still do the one-off dialback (in BLOCKING mode)
		req := ping(PingRequest_BLOCKING)
		req.OriginNodeID = 0
		mockRPCCtx.EXPECT().GRPCUnvalidatedDial("1.1.1.1", roachpb.Locality{}).
			DoAndReturn(func(string, roachpb.Locality) *Connection {
				tmpConn := mkConn()
				assert.Equal(t, ErrNotHeartbeated, tmpConn.Health())
				return tmpConn
			})
		mockRPCCtx.EXPECT().wrapCtx(gomock.Any(), "1.1.1.1", roachpb.NodeID(0), SystemClass).DoAndReturn(func(
			ctx context.Context, _ string, _ roachpb.NodeID, _ ConnectionClass) context.Context {
			return ctx
		})
		mockRPCCtx.EXPECT().grpcDialRaw(gomock.Any() /* ctx */, "1.1.1.1", SystemClass, gomock.Any()).
			DoAndReturn(func(context.Context, string, ConnectionClass, ...grpc.DialOption) (*grpc.ClientConn, error) {
				return nil, nil
			})
		require.NoError(t, VerifyDialback(context.Background(), mockRPCCtx, req, &PingResponse{}, roachpb.Locality{}, sv))
	})
}

// TODO(baptist): Add a test using TestCluster to verify this works in a full
// integration test.

func checkMetrics(m *Metrics, healthy, unhealthy, inactive int64, checkDurations bool) error {
	// NB: peers can be neither healthy nor unhealthy. This happens when
	// a peer is first created and when its deleteAfter field is set.
	if exp, n := healthy, m.ConnectionHealthy.Value(); exp != n {
		return errors.Errorf("ConnectionHealthy = %d", n)
	}
	if exp, n := unhealthy, m.ConnectionUnhealthy.Value(); exp != n {
		return errors.Errorf("ConnectionUnhealthy = %d", n)
	}
	if exp, n := inactive, m.ConnectionInactive.Value(); exp != n {
		return errors.Errorf("ConnectionInactive = %d", n)
	}

	if !checkDurations {
		return nil
	}

	// HealthyFor is nonzero if and only if there is at least one healthy connection, since it's
	// the sum of the connections' HealthyFor, and on a connection this is nonzero if and only
	// if the connection is healthy.
	if hf := m.ConnectionHealthyFor.Value(); (hf > 0) != (healthy > 0) {
		return errors.Errorf("#healthy is %d but ConnectionHealthyFor is %v", healthy, hf)
	}
	// UnhealthyFor is nonzero if and only if there is at least one unhealthy connection, for the
	// same reasons as above.
	if uf := m.ConnectionUnhealthyFor.Value(); (uf > 0) != (unhealthy > 0) {
		return errors.Errorf("#unhealthy is %d but ConnectionUnHealthyFor is %v", unhealthy, uf)
	}

	// Similar to the two above, only healthy connections should maintain the avg round-trip latency.
	if v := m.ConnectionAvgRoundTripLatency.Value(); (v > 0) != (healthy > 0) {
		return errors.Errorf("ConnectionAvgRoundTripLatency = %v", v)
	}

	return nil
}

// TestInitialHeartbeatFailedError tests that InitialHeartbeatFailedError is
// returned for various scenarios. This is important for
// grpcutil.RequestDidNotStart to properly detect unambiguous failures.
func TestInitialHeartbeatFailedError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const maxOffset = 0
	const nodeID = 1

	hbErrType := (*netutil.InitialHeartbeatFailedError)(nil)
	requireHeartbeatError := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		require.True(t, errors.HasType(err, hbErrType), "got %T: %s", err, err)
	}

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := timeutil.NewManualTime(timeutil.Unix(0, 20))
	serverCtx := newTestContext(uuid.MakeV4(), clock, maxOffset, stopper)
	serverCtx.NodeID.Set(ctx, nodeID)
	clientCtx := newTestContext(serverCtx.StorageClusterID.Get(), clock, maxOffset, stopper)
	clientCtx.AddTestingDialOpts(grpc.WithConnectParams(grpc.ConnectParams{
		MinConnectTimeout: time.Second,
	}))

	// Set up a ping handler that can error out.
	var failPing, hangPing atomic.Bool
	onHandlePing := func(ctx context.Context, req *PingRequest, resp *PingResponse) error {
		if failPing.Load() {
			return errors.New("error")
		}
		for hangPing.Load() {
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	}

	// Rejected connection errors with InitialHeartbeatFailedError.
	remoteAddr := "127.0.0.99:64072"
	_, err := clientCtx.GRPCDialNode(remoteAddr, nodeID, roachpb.Locality{}, SystemClass).Connect(ctx)
	requireHeartbeatError(t, err)

	// Hung listener errors with InitialHeartbeatFailedError.
	hungLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		_ = hungLn.Close()
	}()
	remoteAddr = hungLn.Addr().String()

	_, err = clientCtx.GRPCDialNode(remoteAddr, nodeID, roachpb.Locality{}, SystemClass).Connect(ctx)
	requireHeartbeatError(t, err)

	// Start server listener. We set the ping handler to fail initially, to make
	// sure no other actor creates an RPC connection.
	failPing.Store(true)
	s := newTestServer(t, serverCtx)
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		clusterID:          serverCtx.StorageClusterID,
		nodeID:             serverCtx.NodeID,
		version:            serverCtx.Settings.Version,
		onHandlePing:       onHandlePing,
	})

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	require.NoError(t, err)
	remoteAddr = ln.Addr().String()

	// Before connecting, health does not return an InitialHeartbeatFailedError,
	// it returns ErrNotHeartbeated.
	err = clientCtx.GRPCDialNode(remoteAddr, nodeID, roachpb.Locality{}, SystemClass).Health()
	require.Error(t, err)
	require.True(t, errors.HasType(err, ErrNotHeartbeated))
	require.False(t, errors.HasType(err, hbErrType))

	// Ping errors result in InitialHeartbeatFailedError.
	failPing.Store(true)
	_, err = clientCtx.GRPCDialNode(remoteAddr, nodeID, roachpb.Locality{}, SystemClass).Connect(ctx)
	requireHeartbeatError(t, err)

	// Stalled pings result in InitialHeartbeatFailedError. We're careful to
	// enable the hang before disabling the error, to make sure no other
	// actor establishes a connection in the meanwhile.
	hangPing.Store(true)
	failPing.Store(false)
	_, err = clientCtx.GRPCDialNode(remoteAddr, nodeID, roachpb.Locality{}, SystemClass).Connect(ctx)
	requireHeartbeatError(t, err)
	hangPing.Store(false)

	// RPC circuit breakers will now be tripped. They should result in
	// InitialHeartbeatFailedError until we finally recover.
	testutils.SucceedsSoon(t, func() error {
		_, err := clientCtx.GRPCDialNode(remoteAddr, nodeID, roachpb.Locality{}, SystemClass).Connect(ctx)
		if err != nil {
			requireHeartbeatError(t, err)
		}
		return err
	})
}

func BenchmarkGRPCPing(b *testing.B) {
	for _, bytes := range []int{1, 1 << 8, 1 << 10, 1 << 11, 1 << 12, 1 << 13, 1 << 14, 1 << 15, 1 << 16, 1 << 18, 1 << 20} {
		bstr := fmt.Sprintf("%d", bytes)
		bname := strings.Repeat("_", 7-len(bstr)) + bstr
		b.Run("bytes="+bname, func(b *testing.B) {
			stopper := stop.NewStopper()
			ctx := context.Background()
			defer stopper.Stop(ctx)

			clock := &timeutil.DefaultTimeSource{}
			maxOffset := 250 * time.Millisecond
			srvRPCCtx := newTestContext(uuid.MakeV4(), clock, maxOffset, stopper)
			const serverNodeID = 1
			srvRPCCtx.NodeID.Set(ctx, serverNodeID)
			s := newTestServer(b, srvRPCCtx)

			randBytes := make([]byte, bytes)
			_, err := rand.Read(randBytes)
			require.NoError(b, err)

			req := &PingRequest{Ping: string(randBytes)}
			resp := &PingResponse{Pong: string(randBytes)}
			anyreq, err := types.MarshalAny(req)
			require.NoError(b, err)
			anyresp, err := types.MarshalAny(resp)
			require.NoError(b, err)

			require.NoError(b, err)
			b.Logf("marshaled request size: %d bytes (%d bytes of overhead)", req.Size(), req.Size()-bytes)

			tsi := &grpcutils.TestServerImpl{
				UU: func(ctx context.Context, req *types.Any) (*types.Any, error) {
					return anyresp, nil
				},
				SS: func(srv grpcutils.GRPCTest_StreamStreamServer) error {
					for {
						if _, err := srv.Recv(); err != nil {
							return err
						}
						if err := srv.Send(anyresp); err != nil {
							return err
						}
					}
				},
			}

			grpcutils.RegisterGRPCTestServer(s, tsi)

			ln, err := netutil.ListenAndServeGRPC(srvRPCCtx.Stopper, s, util.TestAddr)
			if err != nil {
				b.Fatal(err)
			}
			remoteAddr := ln.Addr().String()

			cliRPCCtx := newTestContext(uuid.MakeV4(), clock, maxOffset, stopper)
			cliRPCCtx.NodeID.Set(ctx, 2)
			cc, err := cliRPCCtx.grpcDialRaw(ctx, remoteAddr, DefaultClass)
			require.NoError(b, err)

			for _, tc := range []struct {
				name   string
				invoke func(c grpcutils.GRPCTestClient, N int) error
			}{
				{"UnaryUnary", func(c grpcutils.GRPCTestClient, N int) error {
					for i := 0; i < N; i++ {
						_, err := c.UnaryUnary(ctx, anyreq)
						if err != nil {
							return err
						}
					}
					return nil
				}},
				{
					"StreamStream", func(c grpcutils.GRPCTestClient, N int) error {
						sc, err := c.StreamStream(ctx)
						if err != nil {
							return err
						}
						for i := 0; i < N; i++ {
							if err := sc.Send(anyreq); err != nil {
								return err
							}
							if _, err := sc.Recv(); err != nil {
								return err
							}
						}
						return nil
					}},
			} {

				b.Run("rpc="+tc.name, func(b *testing.B) {

					c := grpcutils.NewGRPCTestClient(cc)

					b.SetBytes(int64(req.Size() + resp.Size()))
					b.ResetTimer()
					if err := tc.invoke(c, b.N); err != nil {
						b.Fatal(err)
					}
				})
			}
		})
	}
}
