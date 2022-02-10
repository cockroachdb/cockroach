// Copyright 2015 The Cockroach Authors.
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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/growstack"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/syncmap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
)

func init() {
	// Disable GRPC tracing. This retains a subset of messages for
	// display on /debug/requests, which is very expensive for
	// snapshots. Until we can be more selective about what is retained
	// in traces, we must disable tracing entirely.
	// https://github.com/grpc/grpc-go/issues/695
	grpc.EnableTracing = false
}

const (
	// The coefficient by which the maximum offset is multiplied to determine the
	// maximum acceptable measurement latency.
	maximumPingDurationMult = 2
)

const (
	defaultWindowSize = 65535
)

func getWindowSize(name string, c ConnectionClass, defaultSize int) int32 {
	const maxWindowSize = defaultWindowSize * 32
	s := envutil.EnvOrDefaultInt(name, defaultSize)
	if s > maxWindowSize {
		log.Warningf(context.Background(), "%s value too large; trimmed to %d", name, maxWindowSize)
		s = maxWindowSize
	}
	if s <= defaultWindowSize {
		log.Warningf(context.Background(),
			"%s RPC will use dynamic window sizes due to %s value lower than %d", c, name, defaultSize)
	}
	return int32(s)
}

var (
	// for an RPC
	initialWindowSize = getWindowSize(
		"COCKROACH_RPC_INITIAL_WINDOW_SIZE", DefaultClass, defaultWindowSize*32)
	initialConnWindowSize = initialWindowSize * 16 // for a connection

	// for RangeFeed RPC
	rangefeedInitialWindowSize = getWindowSize(
		"COCKROACH_RANGEFEED_RPC_INITIAL_WINDOW_SIZE", RangefeedClass, 2*defaultWindowSize /* 128K */)
)

// GRPC Dialer connection timeout. 20s matches default value that is
// suppressed when backoff config is provided.
const minConnectionTimeout = 20 * time.Second

// errDialRejected is returned from client interceptors when the server's
// stopper is quiescing. The error is constructed to return true in
// `grpcutil.IsConnectionRejected` which prevents infinite retry loops during
// cluster shutdown, especially in unit testing.
var errDialRejected = grpcstatus.Error(codes.PermissionDenied, "refusing to dial; node is quiescing")

// sourceAddr is the environment-provided local address for outgoing
// connections.
var sourceAddr = func() net.Addr {
	const envKey = "COCKROACH_SOURCE_IP_ADDRESS"
	if sourceAddr, ok := envutil.EnvString(envKey, 0); ok {
		sourceIP := net.ParseIP(sourceAddr)
		if sourceIP == nil {
			panic(fmt.Sprintf("unable to parse %s '%s' as IP address", envKey, sourceAddr))
		}
		return &net.TCPAddr{
			IP: sourceIP,
		}
	}
	return nil
}()

var enableRPCCompression = envutil.EnvOrDefaultBool("COCKROACH_ENABLE_RPC_COMPRESSION", true)

type serverOpts struct {
	interceptor func(fullMethod string) error
}

// ServerOption is a configuration option passed to NewServer.
type ServerOption func(*serverOpts)

// WithInterceptor adds an additional interceptor. The interceptor is called before
// streaming and unary RPCs and may inject an error.
func WithInterceptor(f func(fullMethod string) error) ServerOption {
	return func(opts *serverOpts) {
		if opts.interceptor == nil {
			opts.interceptor = f
		} else {
			f := opts.interceptor
			opts.interceptor = func(fullMethod string) error {
				if err := f(fullMethod); err != nil {
					return err
				}
				return f(fullMethod)
			}
		}
	}
}

// NewServer sets up an RPC server. Depending on the ServerOptions, the Server
// either expects incoming connections from KV nodes, or from tenant SQL
// servers.
func NewServer(rpcCtx *Context, opts ...ServerOption) *grpc.Server {
	var o serverOpts
	for _, f := range opts {
		f(&o)
	}
	grpcOpts := []grpc.ServerOption{
		// The limiting factor for lowering the max message size is the fact
		// that a single large kv can be sent over the network in one message.
		// Our maximum kv size is unlimited, so we need this to be very large.
		//
		// TODO(peter,tamird): need tests before lowering.
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		// Adjust the stream and connection window sizes. The gRPC defaults are too
		// low for high latency connections.
		grpc.InitialWindowSize(initialWindowSize),
		grpc.InitialConnWindowSize(initialConnWindowSize),
		// The default number of concurrent streams/requests on a client connection
		// is 100, while the server is unlimited. The client setting can only be
		// controlled by adjusting the server value. Set a very large value for the
		// server value so that we have no fixed limit on the number of concurrent
		// streams/requests on either the client or server.
		grpc.MaxConcurrentStreams(math.MaxInt32),
		grpc.KeepaliveParams(serverKeepalive),
		grpc.KeepaliveEnforcementPolicy(serverEnforcement),
		// A stats handler to measure server network stats.
		grpc.StatsHandler(&rpcCtx.stats),
	}
	if !rpcCtx.Config.Insecure {
		tlsConfig, err := rpcCtx.GetServerTLSConfig()
		if err != nil {
			panic(err)
		}
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	// These interceptors will be called in the order in which they appear, i.e.
	// The last element will wrap the actual handler. The first interceptor
	// guards RPC endpoints for use after Stopper.Drain() by handling the RPC
	// inside a stopper task.
	var unaryInterceptor []grpc.UnaryServerInterceptor
	var streamInterceptor []grpc.StreamServerInterceptor
	unaryInterceptor = append(unaryInterceptor, func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (interface{}, error) {
		var resp interface{}
		if err := rpcCtx.Stopper.RunTaskWithErr(ctx, info.FullMethod, func(ctx context.Context) error {
			var err error
			resp, err = handler(ctx, req)
			return err
		}); err != nil {
			return nil, err
		}
		return resp, nil
	})
	streamInterceptor = append(streamInterceptor, func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return rpcCtx.Stopper.RunTaskWithErr(ss.Context(), info.FullMethod, func(ctx context.Context) error {
			return handler(srv, ss)
		})
	})

	if !rpcCtx.Config.Insecure {
		a := kvAuth{
			tenant: tenantAuthorizer{
				tenantID: rpcCtx.tenID,
			},
		}

		unaryInterceptor = append(unaryInterceptor, a.AuthUnary())
		streamInterceptor = append(streamInterceptor, a.AuthStream())
	}

	if o.interceptor != nil {
		unaryInterceptor = append(unaryInterceptor, func(
			ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
		) (interface{}, error) {
			if err := o.interceptor(info.FullMethod); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		})

		streamInterceptor = append(streamInterceptor, func(
			srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
		) error {
			if err := o.interceptor(info.FullMethod); err != nil {
				return err
			}
			return handler(srv, stream)
		})
	}

	if tracer := rpcCtx.Stopper.Tracer(); tracer != nil {
		unaryInterceptor = append(unaryInterceptor, tracing.ServerInterceptor(tracer))
		streamInterceptor = append(streamInterceptor, tracing.StreamServerInterceptor(tracer))
	}

	grpcOpts = append(grpcOpts, grpc.ChainUnaryInterceptor(unaryInterceptor...))
	grpcOpts = append(grpcOpts, grpc.ChainStreamInterceptor(streamInterceptor...))

	s := grpc.NewServer(grpcOpts...)
	RegisterHeartbeatServer(s, rpcCtx.NewHeartbeatService())
	return s
}

type heartbeatResult struct {
	everSucceeded bool  // true if the heartbeat has ever succeeded
	err           error // heartbeat error, initialized to ErrNotHeartbeated
}

// state is a helper to return the heartbeatState implied by a heartbeatResult.
func (hr heartbeatResult) state() (s heartbeatState) {
	switch {
	case !hr.everSucceeded && hr.err != nil:
		s = heartbeatInitializing
	case hr.everSucceeded && hr.err == nil:
		s = heartbeatNominal
	case hr.everSucceeded && hr.err != nil:
		s = heartbeatFailed
	}
	return s
}

// Connection is a wrapper around grpc.ClientConn. It prevents the underlying
// connection from being used until it has been validated via heartbeat.
type Connection struct {
	grpcConn             *grpc.ClientConn
	dialErr              error         // error while dialing; if set, connection is unusable
	heartbeatResult      atomic.Value  // result of latest heartbeat
	initialHeartbeatDone chan struct{} // closed after first heartbeat
	stopper              *stop.Stopper

	// remoteNodeID implies checking the remote node ID. 0 when unknown,
	// non-zero to check with remote node. This is constant throughout
	// the lifetime of a Connection object.
	remoteNodeID roachpb.NodeID

	initOnce sync.Once
}

func newConnectionToNodeID(stopper *stop.Stopper, remoteNodeID roachpb.NodeID) *Connection {
	c := &Connection{
		initialHeartbeatDone: make(chan struct{}),
		stopper:              stopper,
		remoteNodeID:         remoteNodeID,
	}
	c.heartbeatResult.Store(heartbeatResult{err: ErrNotHeartbeated})
	return c
}

// Connect returns the underlying grpc.ClientConn after it has been validated,
// or an error if dialing or validation fails.
func (c *Connection) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	if c.dialErr != nil {
		return nil, c.dialErr
	}

	// Wait for initial heartbeat.
	select {
	case <-c.initialHeartbeatDone:
	case <-c.stopper.ShouldQuiesce():
		return nil, errors.Errorf("stopped")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// If connection is invalid, return latest heartbeat error.
	h := c.heartbeatResult.Load().(heartbeatResult)
	if !h.everSucceeded {
		// If we've never succeeded, h.err will be ErrNotHeartbeated.
		return nil, netutil.NewInitialHeartBeatFailedError(h.err)
	}
	return c.grpcConn, nil
}

// Health returns an error indicating the success or failure of the
// connection's latest heartbeat. Returns ErrNotHeartbeated if the
// first heartbeat has not completed.
func (c *Connection) Health() error {
	return c.heartbeatResult.Load().(heartbeatResult).err
}

// Context contains the fields required by the rpc framework.
//
// TODO(tbg): rename at the very least the `ctx` receiver, but possibly the whole
// thing.
type Context struct {
	ContextOptions
	SecurityContext

	breakerClock breakerClock
	RemoteClocks *RemoteClockMonitor
	MasterCtx    context.Context

	heartbeatTimeout time.Duration
	HeartbeatCB      func()

	rpcCompression bool

	localInternalClient roachpb.InternalClient

	conns syncmap.Map

	stats StatsHandler

	metrics Metrics

	// For unittesting.
	BreakerFactory  func() *circuit.Breaker
	testingDialOpts []grpc.DialOption

	// For testing. See the comment on the same field in HeartbeatService.
	TestingAllowNamedRPCToAnonymousServer bool
}

// connKey is used as key in the Context.conns map.
// Connections which carry a different class but share a target and nodeID
// will always specify distinct connections. Different remote node IDs get
// distinct *Connection objects to ensure that we don't mis-route RPC
// requests in the face of address reuse. Gossip connections and other
// non-Internal users of the Context are free to dial nodes without
// specifying a node ID (see GRPCUnvalidatedDial()) however later calls to
// Dial with the same target and class with a node ID will create a new
// underlying connection. The inverse however is not true, a connection
// dialed without a node ID will use an existing connection to a matching
// (targetAddr, class) pair.
type connKey struct {
	targetAddr string
	// Note: this ought to be renamed, see:
	// https://github.com/cockroachdb/cockroach/pull/73309
	nodeID roachpb.NodeID
	class  ConnectionClass
}

var _ redact.SafeFormatter = connKey{}

// SafeFormat implements the redact.SafeFormatter interface.
func (c connKey) SafeFormat(p redact.SafePrinter, _ rune) {
	p.Printf("{n%d: %s (%v)}", c.nodeID, c.targetAddr, c.class)
}

// ContextOptions are passed to NewContext to set up a new *Context.
// All pointer fields and TenantID are required.
type ContextOptions struct {
	TenantID roachpb.TenantID
	Config   *base.Config
	Clock    *hlc.Clock
	Stopper  *stop.Stopper
	Settings *cluster.Settings
	// OnIncomingPing is called when handling a PingRequest, after
	// preliminary checks but before recording clock offset information.
	//
	// It can inject an error.
	OnIncomingPing func(context.Context, *PingRequest) error
	// OnOutgoingPing intercepts outgoing PingRequests. It may inject an
	// error.
	OnOutgoingPing func(context.Context, *PingRequest) error
	Knobs          ContextTestingKnobs

	// NodeID is the node ID / SQL instance ID container shared
	// with the remainder of the server. If unset in the options,
	// the RPC context will instantiate its own separate container
	// (this is useful in tests).
	// Note: this ought to be renamed, see:
	// https://github.com/cockroachdb/cockroach/pull/73309
	NodeID *base.NodeIDContainer

	// ClusterID is the cluster ID shared with the remainder of the
	// server. If unset in the options, the RPC context will instantiate
	// its own separate container (this is useful in tests).
	ClusterID *base.ClusterIDContainer

	// ClientOnly indicates that this RPC context is run by a CLI
	// utility, not a server, and thus misses server configuration, a
	// cluster version, a node ID, etc.
	ClientOnly bool
}

func (c ContextOptions) validate() error {
	if c.TenantID == (roachpb.TenantID{}) {
		return errors.New("must specify TenantID")
	}
	if c.Config == nil {
		return errors.New("Config must be set")
	}
	if c.Clock == nil {
		return errors.New("Clock must be set")
	}
	if c.Stopper == nil {
		return errors.New("Stopper must be set")
	}
	if c.Settings == nil {
		return errors.New("Settings must be set")
	}

	// NB: OnOutgoingPing and OnIncomingPing default to noops.
	// This is used both for testing and the cli.
	_, _ = c.OnOutgoingPing, c.OnIncomingPing

	return nil
}

// NewContext creates an rpc.Context with the supplied values.
func NewContext(ctx context.Context, opts ContextOptions) *Context {
	if err := opts.validate(); err != nil {
		panic(err)
	}

	if opts.NodeID == nil {
		// Tests rely on NewContext to generate its own ID container.
		var c base.NodeIDContainer
		opts.NodeID = &c
	}

	if opts.ClusterID == nil {
		// Tests rely on NewContext to generate its own ID container.
		var c base.ClusterIDContainer
		opts.ClusterID = &c
	}

	masterCtx, cancel := context.WithCancel(ctx)

	rpcCtx := &Context{
		ContextOptions:  opts,
		SecurityContext: MakeSecurityContext(opts.Config, security.ClusterTLSSettings(opts.Settings), opts.TenantID),
		breakerClock: breakerClock{
			clock: opts.Clock,
		},
		RemoteClocks: newRemoteClockMonitor(
			opts.Clock, 10*opts.Config.RPCHeartbeatInterval, opts.Config.HistogramWindowInterval()),
		rpcCompression:   enableRPCCompression,
		MasterCtx:        masterCtx,
		metrics:          makeMetrics(),
		heartbeatTimeout: 2 * opts.Config.RPCHeartbeatInterval,
	}
	if id := opts.Knobs.ClusterID; id != nil {
		rpcCtx.ClusterID.Set(masterCtx, *id)
	}

	waitQuiesce := func(context.Context) {
		<-rpcCtx.Stopper.ShouldQuiesce()

		cancel()
		rpcCtx.conns.Range(func(k, v interface{}) bool {
			conn := v.(*Connection)
			conn.initOnce.Do(func() {
				// Make sure initialization is not in progress when we're removing the
				// conn. We need to set the error in case we win the race against the
				// real initialization code.
				if conn.dialErr == nil {
					conn.dialErr = errDialRejected
				}
			})
			rpcCtx.removeConn(conn, k.(connKey))
			return true
		})
	}
	if err := rpcCtx.Stopper.RunAsyncTask(rpcCtx.MasterCtx, "wait-rpcctx-quiesce", waitQuiesce); err != nil {
		waitQuiesce(rpcCtx.MasterCtx)
	}
	return rpcCtx
}

// ClusterName retrieves the configured cluster name.
func (rpcCtx *Context) ClusterName() string {
	if rpcCtx == nil {
		// This is used in tests.
		return "<MISSING RPC CONTEXT>"
	}
	return rpcCtx.Config.ClusterName
}

// GetStatsMap returns a map of network statistics maintained by the
// internal stats handler. The map is from the remote network address
// (in string form) to an rpc.Stats object.
func (rpcCtx *Context) GetStatsMap() *syncmap.Map {
	return &rpcCtx.stats.stats
}

// Metrics returns the Context's Metrics struct.
func (rpcCtx *Context) Metrics() *Metrics {
	return &rpcCtx.metrics
}

// GetLocalInternalClientForAddr returns the context's internal batch client
// for target, if it exists.
// Note: the node ID ought to be retyped, see
// https://github.com/cockroachdb/cockroach/pull/73309
func (rpcCtx *Context) GetLocalInternalClientForAddr(
	target string, nodeID roachpb.NodeID,
) roachpb.InternalClient {
	if target == rpcCtx.Config.AdvertiseAddr && nodeID == rpcCtx.NodeID.Get() {
		return rpcCtx.localInternalClient
	}
	return nil
}

type internalClientAdapter struct {
	server roachpb.InternalServer
}

// Batch implements the roachpb.InternalClient interface.
func (a internalClientAdapter) Batch(
	ctx context.Context, ba *roachpb.BatchRequest, _ ...grpc.CallOption,
) (*roachpb.BatchResponse, error) {
	// Mark this as originating locally, which is useful for the decision about
	// memory allocation tracking.
	ba.AdmissionHeader.SourceLocation = roachpb.AdmissionHeader_LOCAL
	return a.server.Batch(ctx, ba)
}

// RangeLookup implements the roachpb.InternalClient interface.
func (a internalClientAdapter) RangeLookup(
	ctx context.Context, rl *roachpb.RangeLookupRequest, _ ...grpc.CallOption,
) (*roachpb.RangeLookupResponse, error) {
	return a.server.RangeLookup(ctx, rl)
}

// Join implements the roachpb.InternalClient interface.
func (a internalClientAdapter) Join(
	ctx context.Context, req *roachpb.JoinNodeRequest, _ ...grpc.CallOption,
) (*roachpb.JoinNodeResponse, error) {
	return a.server.Join(ctx, req)
}

// ResetQuorum is part of the roachpb.InternalClient interface.
func (a internalClientAdapter) ResetQuorum(
	ctx context.Context, req *roachpb.ResetQuorumRequest, _ ...grpc.CallOption,
) (*roachpb.ResetQuorumResponse, error) {
	return a.server.ResetQuorum(ctx, req)
}

// TokenBucket is part of the roachpb.InternalClient interface.
func (a internalClientAdapter) TokenBucket(
	ctx context.Context, in *roachpb.TokenBucketRequest, opts ...grpc.CallOption,
) (*roachpb.TokenBucketResponse, error) {
	return a.server.TokenBucket(ctx, in)
}

// GetSpanConfigs is part of the roachpb.InternalClient interface.
func (a internalClientAdapter) GetSpanConfigs(
	ctx context.Context, req *roachpb.GetSpanConfigsRequest, _ ...grpc.CallOption,
) (*roachpb.GetSpanConfigsResponse, error) {
	return a.server.GetSpanConfigs(ctx, req)
}

// UpdateSpanConfigs is part of the roachpb.InternalClient interface.
func (a internalClientAdapter) UpdateSpanConfigs(
	ctx context.Context, req *roachpb.UpdateSpanConfigsRequest, _ ...grpc.CallOption,
) (*roachpb.UpdateSpanConfigsResponse, error) {
	return a.server.UpdateSpanConfigs(ctx, req)
}

type respStreamClientAdapter struct {
	ctx   context.Context
	respC chan interface{}
	errC  chan error
}

func makeRespStreamClientAdapter(ctx context.Context) respStreamClientAdapter {
	return respStreamClientAdapter{
		ctx:   ctx,
		respC: make(chan interface{}, 128),
		errC:  make(chan error, 1),
	}
}

// grpc.ClientStream methods.
func (respStreamClientAdapter) Header() (metadata.MD, error) { panic("unimplemented") }
func (respStreamClientAdapter) Trailer() metadata.MD         { panic("unimplemented") }
func (respStreamClientAdapter) CloseSend() error             { panic("unimplemented") }

// grpc.ServerStream methods.
func (respStreamClientAdapter) SetHeader(metadata.MD) error  { panic("unimplemented") }
func (respStreamClientAdapter) SendHeader(metadata.MD) error { panic("unimplemented") }
func (respStreamClientAdapter) SetTrailer(metadata.MD)       { panic("unimplemented") }

// grpc.Stream methods.
func (a respStreamClientAdapter) Context() context.Context  { return a.ctx }
func (respStreamClientAdapter) SendMsg(m interface{}) error { panic("unimplemented") }
func (respStreamClientAdapter) RecvMsg(m interface{}) error { panic("unimplemented") }

func (a respStreamClientAdapter) recvInternal() (interface{}, error) {
	// Prioritize respC. Both channels are buffered and the only guarantee we
	// have is that once an error is sent on errC no other events will be sent
	// on respC again.
	select {
	case e := <-a.respC:
		return e, nil
	case err := <-a.errC:
		select {
		case e := <-a.respC:
			a.errC <- err
			return e, nil
		default:
			return nil, err
		}
	}
}

func (a respStreamClientAdapter) sendInternal(e interface{}) error {
	select {
	case a.respC <- e:
		return nil
	case <-a.ctx.Done():
		return a.ctx.Err()
	}
}

type rangeFeedClientAdapter struct {
	respStreamClientAdapter
}

// roachpb.Internal_RangeFeedServer methods.
func (a rangeFeedClientAdapter) Recv() (*roachpb.RangeFeedEvent, error) {
	e, err := a.recvInternal()
	if err != nil {
		return nil, err
	}
	return e.(*roachpb.RangeFeedEvent), nil
}

// roachpb.Internal_RangeFeedServer methods.
func (a rangeFeedClientAdapter) Send(e *roachpb.RangeFeedEvent) error {
	return a.sendInternal(e)
}

var _ roachpb.Internal_RangeFeedClient = rangeFeedClientAdapter{}
var _ roachpb.Internal_RangeFeedServer = rangeFeedClientAdapter{}

// RangeFeed implements the roachpb.InternalClient interface.
func (a internalClientAdapter) RangeFeed(
	ctx context.Context, args *roachpb.RangeFeedRequest, _ ...grpc.CallOption,
) (roachpb.Internal_RangeFeedClient, error) {
	ctx, cancel := context.WithCancel(ctx)
	ctx, sp := tracing.ChildSpan(ctx, "/cockroach.roachpb.Internal/RangeFeed")
	rfAdapter := rangeFeedClientAdapter{
		respStreamClientAdapter: makeRespStreamClientAdapter(ctx),
	}

	// Mark this as originating locally.
	args.AdmissionHeader.SourceLocation = roachpb.AdmissionHeader_LOCAL
	go func() {
		defer cancel()
		defer sp.Finish()
		err := a.server.RangeFeed(args, rfAdapter)
		if err == nil {
			err = io.EOF
		}
		rfAdapter.errC <- err
	}()

	return rfAdapter, nil
}

type gossipSubscriptionClientAdapter struct {
	respStreamClientAdapter
}

// roachpb.Internal_GossipSubscriptionServer methods.
func (a gossipSubscriptionClientAdapter) Recv() (*roachpb.GossipSubscriptionEvent, error) {
	e, err := a.recvInternal()
	if err != nil {
		return nil, err
	}
	return e.(*roachpb.GossipSubscriptionEvent), nil
}

// roachpb.Internal_GossipSubscriptionServer methods.
func (a gossipSubscriptionClientAdapter) Send(e *roachpb.GossipSubscriptionEvent) error {
	return a.sendInternal(e)
}

var _ roachpb.Internal_GossipSubscriptionClient = gossipSubscriptionClientAdapter{}
var _ roachpb.Internal_GossipSubscriptionServer = gossipSubscriptionClientAdapter{}

// GossipSubscription is part of the roachpb.InternalClient interface.
func (a internalClientAdapter) GossipSubscription(
	ctx context.Context, args *roachpb.GossipSubscriptionRequest, _ ...grpc.CallOption,
) (roachpb.Internal_GossipSubscriptionClient, error) {
	ctx, cancel := context.WithCancel(ctx)
	ctx, sp := tracing.ChildSpan(ctx, "/cockroach.roachpb.Internal/GossipSubscription")
	gsAdapter := gossipSubscriptionClientAdapter{
		respStreamClientAdapter: makeRespStreamClientAdapter(ctx),
	}

	go func() {
		defer cancel()
		defer sp.Finish()
		err := a.server.GossipSubscription(args, gsAdapter)
		if err == nil {
			err = io.EOF
		}
		gsAdapter.errC <- err
	}()

	return gsAdapter, nil
}

type tenantSettingsClientAdapter struct {
	respStreamClientAdapter
}

// roachpb.Internal_TenantSettingsServer methods.
func (a tenantSettingsClientAdapter) Recv() (*roachpb.TenantSettingsEvent, error) {
	e, err := a.recvInternal()
	if err != nil {
		return nil, err
	}
	return e.(*roachpb.TenantSettingsEvent), nil
}

// roachpb.Internal_TenantSettingsServer methods.
func (a tenantSettingsClientAdapter) Send(e *roachpb.TenantSettingsEvent) error {
	return a.sendInternal(e)
}

var _ roachpb.Internal_TenantSettingsClient = tenantSettingsClientAdapter{}
var _ roachpb.Internal_TenantSettingsServer = tenantSettingsClientAdapter{}

// TenantSettings is part of the roachpb.InternalClient interface.
func (a internalClientAdapter) TenantSettings(
	ctx context.Context, args *roachpb.TenantSettingsRequest, _ ...grpc.CallOption,
) (roachpb.Internal_TenantSettingsClient, error) {
	ctx, cancel := context.WithCancel(ctx)
	gsAdapter := tenantSettingsClientAdapter{
		respStreamClientAdapter: makeRespStreamClientAdapter(ctx),
	}

	go func() {
		defer cancel()
		err := a.server.TenantSettings(args, gsAdapter)
		if err == nil {
			err = io.EOF
		}
		gsAdapter.errC <- err
	}()

	return gsAdapter, nil
}

var _ roachpb.InternalClient = internalClientAdapter{}

// IsLocal returns true if the given InternalClient is local.
func IsLocal(iface roachpb.InternalClient) bool {
	_, ok := iface.(internalClientAdapter)
	return ok // internalClientAdapter is used for local connections.
}

// SetLocalInternalServer sets the context's local internal batch server.
func (rpcCtx *Context) SetLocalInternalServer(internalServer roachpb.InternalServer) {
	rpcCtx.localInternalClient = internalClientAdapter{internalServer}
}

// removeConn removes the given connection from the pool. The supplied connKeys
// must represent *all* the keys under among which the connection was shared.
func (rpcCtx *Context) removeConn(conn *Connection, keys ...connKey) {
	for _, key := range keys {
		rpcCtx.conns.Delete(key)
	}
	log.Health.Infof(rpcCtx.MasterCtx, "closing %+v", keys)
	if grpcConn := conn.grpcConn; grpcConn != nil {
		err := grpcConn.Close() // nolint:grpcconnclose
		if err != nil && !grpcutil.IsClosedConnection(err) {
			log.Health.Warningf(rpcCtx.MasterCtx, "failed to close client connection: %v", err)
		}
	}
}

// ConnHealth returns nil if we have an open connection of the request
// class to the given node that succeeded on its most recent heartbeat.
// Note: the node ID ought to be retyped, see
// https://github.com/cockroachdb/cockroach/pull/73309
func (rpcCtx *Context) ConnHealth(
	target string, nodeID roachpb.NodeID, class ConnectionClass,
) error {
	// The local client is always considered healthy.
	if rpcCtx.GetLocalInternalClientForAddr(target, nodeID) != nil {
		return nil
	}
	if value, ok := rpcCtx.conns.Load(connKey{target, nodeID, class}); ok {
		return value.(*Connection).Health()
	}
	return ErrNoConnection
}

// GRPCDialOptions returns the minimal `grpc.DialOption`s necessary to connect
// to a server created with `NewServer`.
//
// At the time of writing, this is being used for making net.Pipe-based
// connections, so only those options that affect semantics are included. In
// particular, performance tuning options are omitted. Decompression is
// necessarily included to support compression-enabled servers, and compression
// is included for symmetry. These choices are admittedly subjective.
func (rpcCtx *Context) GRPCDialOptions() ([]grpc.DialOption, error) {
	return rpcCtx.grpcDialOptions("", DefaultClass)
}

// grpcDialOptions extends GRPCDialOptions to support a connection class for use
// with TestingKnobs.
func (rpcCtx *Context) grpcDialOptions(
	target string, class ConnectionClass,
) ([]grpc.DialOption, error) {
	var dialOpts []grpc.DialOption
	if rpcCtx.Config.Insecure {
		//lint:ignore SA1019 grpc.WithInsecure is deprecated
		dialOpts = append(dialOpts, grpc.WithInsecure())
	} else {
		var tlsConfig *tls.Config
		var err error
		if rpcCtx.tenID == roachpb.SystemTenantID {
			tlsConfig, err = rpcCtx.GetClientTLSConfig()
		} else {
			tlsConfig, err = rpcCtx.GetTenantTLSConfig()
		}
		if err != nil {
			return nil, err
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	// The limiting factor for lowering the max message size is the fact
	// that a single large kv can be sent over the network in one message.
	// Our maximum kv size is unlimited, so we need this to be very large.
	//
	// TODO(peter,tamird): need tests before lowering.
	dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(math.MaxInt32),
		grpc.MaxCallSendMsgSize(math.MaxInt32),
	))

	// Compression is enabled separately from decompression to allow staged
	// rollout.
	if rpcCtx.rpcCompression {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor((snappyCompressor{}).Name())))
	}

	// GRPC uses the HTTPS_PROXY environment variable by default[1]. This is
	// surprising, and likely undesirable for CRDB because it turns the proxy
	// into an availability risk and a throughput bottleneck. We disable the use
	// of proxies by default.
	//
	// [1]: https://github.com/grpc/grpc-go/blob/c0736608/Documentation/proxy.md
	dialOpts = append(dialOpts, grpc.WithNoProxy())

	var unaryInterceptors []grpc.UnaryClientInterceptor
	var streamInterceptors []grpc.StreamClientInterceptor

	if tracer := rpcCtx.Stopper.Tracer(); tracer != nil {
		// TODO(tbg): re-write all of this for our tracer.

		// We use a decorator to set the "node" tag. All other spans get the
		// node tag from context log tags.
		//
		// Unfortunately we cannot use the corresponding interceptor on the
		// server-side of gRPC to set this tag on server spans because that
		// interceptor runs too late - after a traced RPC's recording had
		// already been collected. So, on the server-side, the equivalent code
		// is in setupSpanForIncomingRPC().
		//
		tagger := func(span *tracing.Span) {
			span.SetTag("node", attribute.IntValue(int(rpcCtx.NodeID.Get())))
		}
		compatMode := func(reqCtx context.Context) bool {
			return !rpcCtx.ContextOptions.Settings.Version.IsActive(reqCtx, clusterversion.SelectRPCsTakeTracingInfoInband)
		}

		if rpcCtx.ClientOnly {
			// client-only RPC contexts don't have a node ID to report nor a
			// cluster version to check against.
			tagger = func(span *tracing.Span) {}
			compatMode = func(_ context.Context) bool { return false }
		}

		unaryInterceptors = append(unaryInterceptors,
			tracing.ClientInterceptor(tracer, tagger, compatMode))
		streamInterceptors = append(streamInterceptors,
			tracing.StreamClientInterceptor(tracer, tagger))
	}
	if rpcCtx.Knobs.UnaryClientInterceptor != nil {
		testingUnaryInterceptor := rpcCtx.Knobs.UnaryClientInterceptor(target, class)
		if testingUnaryInterceptor != nil {
			unaryInterceptors = append(unaryInterceptors, testingUnaryInterceptor)
		}
	}
	if rpcCtx.Knobs.StreamClientInterceptor != nil {
		testingStreamInterceptor := rpcCtx.Knobs.StreamClientInterceptor(target, class)
		if testingStreamInterceptor != nil {
			streamInterceptors = append(streamInterceptors, testingStreamInterceptor)
		}
	}
	if rpcCtx.Knobs.ArtificialLatencyMap != nil {
		dialerFunc := func(ctx context.Context, target string) (net.Conn, error) {
			dialer := net.Dialer{
				LocalAddr: sourceAddr,
			}
			return dialer.DialContext(ctx, "tcp", target)
		}
		latency := rpcCtx.Knobs.ArtificialLatencyMap[target]
		log.VEventf(rpcCtx.MasterCtx, 1, "connecting to node %s with simulated latency %dms", target, latency)
		dialer := artificialLatencyDialer{
			dialerFunc: dialerFunc,
			latencyMS:  latency,
		}
		dialerFunc = dialer.dial
		dialOpts = append(dialOpts, grpc.WithContextDialer(dialerFunc))
	}

	if len(unaryInterceptors) > 0 {
		dialOpts = append(dialOpts, grpc.WithChainUnaryInterceptor(unaryInterceptors...))
	}
	if len(streamInterceptors) > 0 {
		dialOpts = append(dialOpts, grpc.WithChainStreamInterceptor(streamInterceptors...))
	}
	return dialOpts, nil
}

// growStackCodec wraps the default grpc/encoding/proto codec to detect
// BatchRequest rpcs and grow the stack prior to Unmarshaling.
type growStackCodec struct {
	encoding.Codec
}

// Unmarshal detects BatchRequests and calls growstack.Grow before calling
// through to the underlying codec.
func (c growStackCodec) Unmarshal(data []byte, v interface{}) error {
	if _, ok := v.(*roachpb.BatchRequest); ok {
		growstack.Grow()
	}
	return c.Codec.Unmarshal(data, v)
}

// Install the growStackCodec over the default proto codec in order to grow the
// stack for BatchRequest RPCs prior to unmarshaling.
func init() {
	encoding.RegisterCodec(growStackCodec{Codec: codec{}})
}

// onlyOnceDialer implements the grpc.WithDialer interface but only
// allows a single connection attempt. If a reconnection is attempted,
// redialChan is closed to signal a higher-level retry loop. This
// ensures that our initial heartbeat (and its version/clusterID
// validation) occurs on every new connection.
type onlyOnceDialer struct {
	syncutil.Mutex
	dialed     bool
	closed     bool
	redialChan chan struct{}
}

func (ood *onlyOnceDialer) dial(ctx context.Context, addr string) (net.Conn, error) {
	ood.Lock()
	defer ood.Unlock()
	if !ood.dialed {
		ood.dialed = true
		dialer := net.Dialer{
			LocalAddr: sourceAddr,
		}
		return dialer.DialContext(ctx, "tcp", addr)
	} else if !ood.closed {
		ood.closed = true
		close(ood.redialChan)
	}
	return nil, grpcutil.ErrCannotReuseClientConn
}

type dialerFunc func(context.Context, string) (net.Conn, error)

type artificialLatencyDialer struct {
	dialerFunc dialerFunc
	latencyMS  int
}

func (ald *artificialLatencyDialer) dial(ctx context.Context, addr string) (net.Conn, error) {
	conn, err := ald.dialerFunc(ctx, addr)
	if err != nil {
		return conn, err
	}
	return &delayingConn{
		Conn:    conn,
		latency: time.Duration(ald.latencyMS) * time.Millisecond,
		readBuf: new(bytes.Buffer),
	}, nil
}

type delayingListener struct {
	net.Listener
}

// NewDelayingListener creates a net.Listener that introduces a set delay on its connections.
func NewDelayingListener(l net.Listener) net.Listener {
	return delayingListener{Listener: l}
}

func (d delayingListener) Accept() (net.Conn, error) {
	c, err := d.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &delayingConn{
		Conn: c,
		// Put a default latency as the server's conn. This value will get populated
		// as packets are exchanged across the delayingConnections.
		latency: time.Duration(0) * time.Millisecond,
		readBuf: new(bytes.Buffer),
	}, nil
}

// delayingConn is a wrapped net.Conn that introduces a fixed delay into all
// writes to the connection. The implementation works by specifying a timestamp
// at which the other end of the connection is allowed to read the data, and
// sending that timestamp across the network in a header packet. On the read
// side, a sleep until the timestamp is introduced after the data is read before
// the data is returned to the consumer.
//
// Note that the fixed latency here is a one-way latency, so if you want to
// simulate a round-trip latency of x milliseconds, you should use a delayingConn
// on both ends with x/2 milliseconds of latency.
type delayingConn struct {
	net.Conn
	latency     time.Duration
	lastSendEnd time.Time
	readBuf     *bytes.Buffer
}

func (d delayingConn) Write(b []byte) (n int, err error) {
	tNow := timeutil.Now()
	if d.lastSendEnd.Before(tNow) {
		d.lastSendEnd = tNow
	}
	hdr := delayingHeader{
		Magic:    magic,
		ReadTime: d.lastSendEnd.Add(d.latency).UnixNano(),
		Sz:       int32(len(b)),
		DelayMS:  int32(d.latency / time.Millisecond),
	}
	if err := binary.Write(d.Conn, binary.BigEndian, hdr); err != nil {
		return n, err
	}
	x, err := d.Conn.Write(b)
	n += x
	return n, err
}

var errMagicNotFound = errors.New("didn't get expected magic bytes header")

func (d *delayingConn) Read(b []byte) (n int, err error) {
	if d.readBuf.Len() == 0 {
		var hdr delayingHeader
		if err := binary.Read(d.Conn, binary.BigEndian, &hdr); err != nil {
			return 0, err
		}
		// If we somehow don't get our expected magic, throw an error.
		if hdr.Magic != magic {
			return 0, errors.WithStack(errMagicNotFound)
		}

		// Once we receive our first packet, we set our delay to the expected
		// delay that was sent on the write side.
		d.latency = time.Duration(hdr.DelayMS) * time.Millisecond
		defer func() {
			time.Sleep(timeutil.Until(timeutil.Unix(0, hdr.ReadTime)))
		}()
		if _, err := io.CopyN(d.readBuf, d.Conn, int64(hdr.Sz)); err != nil {
			return 0, err
		}
	}
	return d.readBuf.Read(b)
}

const magic = 0xfeedfeed

type delayingHeader struct {
	Magic    int64
	ReadTime int64
	Sz       int32
	DelayMS  int32
}

func (rpcCtx *Context) makeDialCtx(
	target string, remoteNodeID roachpb.NodeID, class ConnectionClass,
) context.Context {
	dialCtx := rpcCtx.MasterCtx
	var rnodeID interface{} = remoteNodeID
	if remoteNodeID == 0 {
		rnodeID = '?'
	}
	dialCtx = logtags.AddTag(dialCtx, "rnode", rnodeID)
	dialCtx = logtags.AddTag(dialCtx, "raddr", target)
	dialCtx = logtags.AddTag(dialCtx, "class", class)
	return dialCtx
}

// GRPCDialRaw calls grpc.Dial with options appropriate for the context.
// Unlike GRPCDialNode, it does not start an RPC heartbeat to validate the
// connection. This connection will not be reconnected automatically;
// the returned channel is closed when a reconnection is attempted.
// This method implies a DefaultClass ConnectionClass for the returned
// ClientConn.
func (rpcCtx *Context) GRPCDialRaw(target string) (*grpc.ClientConn, <-chan struct{}, error) {
	ctx := rpcCtx.makeDialCtx(target, 0, DefaultClass)
	return rpcCtx.grpcDialRaw(ctx, target, 0, DefaultClass)
}

// grpcDialRaw connects to the remote node.
// The ctx passed as argument must be derived from rpcCtx.masterCtx, so
// that it respects the same cancellation policy.
func (rpcCtx *Context) grpcDialRaw(
	ctx context.Context, target string, remoteNodeID roachpb.NodeID, class ConnectionClass,
) (*grpc.ClientConn, <-chan struct{}, error) {
	dialOpts, err := rpcCtx.grpcDialOptions(target, class)
	if err != nil {
		return nil, nil, err
	}

	// Add a stats handler to measure client network stats.
	dialOpts = append(dialOpts, grpc.WithStatsHandler(rpcCtx.stats.newClient(target)))

	// Lower the MaxBackoff (which defaults to ~minutes) to something in the
	// ~second range.
	backoffConfig := backoff.DefaultConfig
	backoffConfig.MaxDelay = maxBackoff
	dialOpts = append(dialOpts, grpc.WithConnectParams(grpc.ConnectParams{
		Backoff:           backoffConfig,
		MinConnectTimeout: minConnectionTimeout}))
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(clientKeepalive))
	dialOpts = append(dialOpts, grpc.WithInitialConnWindowSize(initialConnWindowSize))
	if class == RangefeedClass {
		dialOpts = append(dialOpts, grpc.WithInitialWindowSize(rangefeedInitialWindowSize))
	} else {
		dialOpts = append(dialOpts, grpc.WithInitialWindowSize(initialWindowSize))
	}

	dialer := onlyOnceDialer{
		redialChan: make(chan struct{}),
	}
	dialerFunc := dialer.dial
	if rpcCtx.Knobs.ArtificialLatencyMap != nil {
		latency := rpcCtx.Knobs.ArtificialLatencyMap[target]
		log.VEventf(ctx, 1, "connecting with simulated latency %dms",
			latency)
		dialer := artificialLatencyDialer{
			dialerFunc: dialerFunc,
			latencyMS:  latency,
		}
		dialerFunc = dialer.dial
	}
	dialOpts = append(dialOpts, grpc.WithContextDialer(dialerFunc))

	// add testingDialOpts after our dialer because one of our tests
	// uses a custom dialer (this disables the only-one-connection
	// behavior and redialChan will never be closed).
	dialOpts = append(dialOpts, rpcCtx.testingDialOpts...)

	log.Health.Infof(ctx, "dialing")
	conn, err := grpc.DialContext(ctx, target, dialOpts...)
	if err != nil && rpcCtx.MasterCtx.Err() != nil {
		// If the node is draining, discard the error (which is likely gRPC's version
		// of context.Canceled) and return errDialRejected which instructs callers not
		// to retry.
		err = errDialRejected
	}
	return conn, dialer.redialChan, err
}

// GRPCUnvalidatedDial uses GRPCDialNode and disables validation of the
// node ID between client and server. This function should only be
// used with the gossip client and CLI commands which can talk to any
// node. This method implies a SystemClass.
func (rpcCtx *Context) GRPCUnvalidatedDial(target string) *Connection {
	ctx := rpcCtx.makeDialCtx(target, 0, SystemClass)
	return rpcCtx.grpcDialNodeInternal(ctx, target, 0, SystemClass)
}

// GRPCDialNode calls grpc.Dial with options appropriate for the
// context and class (see the comment on ConnectionClass).
//
// The remoteNodeID becomes a constraint on the expected node ID of
// the remote node; this is checked during heartbeats. The caller is
// responsible for ensuring the remote node ID is known prior to using
// this function.
func (rpcCtx *Context) GRPCDialNode(
	target string, remoteNodeID roachpb.NodeID, class ConnectionClass,
) *Connection {
	ctx := rpcCtx.makeDialCtx(target, remoteNodeID, class)
	if remoteNodeID == 0 && !rpcCtx.TestingAllowNamedRPCToAnonymousServer {
		log.Fatalf(ctx, "%v", errors.AssertionFailedf("invalid node ID 0 in GRPCDialNode()"))
	}
	return rpcCtx.grpcDialNodeInternal(ctx, target, remoteNodeID, class)
}

// GRPCDialPod wraps GRPCDialNode and treats the `remoteInstanceID`
// argument as a `NodeID` which it converts. This works because the
// tenant gRPC server is initialized using the `InstanceID` so it
// accepts our connection as matching the ID we're dialing.
//
// Since GRPCDialNode accepts a separate `target` and `NodeID` it
// requires no further modification to work between pods.
func (rpcCtx *Context) GRPCDialPod(
	target string, remoteInstanceID base.SQLInstanceID, class ConnectionClass,
) *Connection {
	return rpcCtx.GRPCDialNode(target, roachpb.NodeID(remoteInstanceID), class)
}

// grpcDialNodeInternal connects to the remote node and sets up the async heartbeater.
// The ctx passed as argument must be derived from rpcCtx.masterCtx, so
// that it respects the same cancellation policy.
func (rpcCtx *Context) grpcDialNodeInternal(
	ctx context.Context, target string, remoteNodeID roachpb.NodeID, class ConnectionClass,
) *Connection {
	thisConnKeys := []connKey{{target, remoteNodeID, class}}
	value, ok := rpcCtx.conns.Load(thisConnKeys[0])
	if !ok {
		value, _ = rpcCtx.conns.LoadOrStore(thisConnKeys[0], newConnectionToNodeID(rpcCtx.Stopper, remoteNodeID))
		if remoteNodeID != 0 {
			// If the first connection established at a target address is
			// for a specific node ID, then we want to reuse that connection
			// also for other dials (eg for gossip) which don't require a
			// specific node ID. (We do this as an optimization to reduce
			// the number of TCP connections alive between nodes. This is
			// not strictly required for correctness.) This LoadOrStore will
			// ensure we're registering the connection we just created for
			// future use by these other dials.
			//
			// We need to be careful to unregister both connKeys when the
			// connection breaks. Otherwise, we leak the entry below which
			// "simulates" a hard network partition for anyone dialing without
			// the nodeID (gossip).
			//
			// See:
			// https://github.com/cockroachdb/cockroach/issues/37200
			otherKey := connKey{target, 0, class}
			if _, loaded := rpcCtx.conns.LoadOrStore(otherKey, value); !loaded {
				thisConnKeys = append(thisConnKeys, otherKey)
			}
		}
	}

	conn := value.(*Connection)
	conn.initOnce.Do(func() {
		// Either we kick off the heartbeat loop (and clean up when it's done),
		// or we clean up the connKey entries immediately.
		var redialChan <-chan struct{}
		conn.grpcConn, redialChan, conn.dialErr = rpcCtx.grpcDialRaw(ctx, target, remoteNodeID, class)
		if conn.dialErr == nil {
			if err := rpcCtx.Stopper.RunAsyncTask(
				logtags.AddTag(ctx, "heartbeat", nil),
				"rpc.Context: grpc heartbeat", func(ctx context.Context) {
					err := rpcCtx.runHeartbeat(ctx, conn, target, redialChan)
					if err != nil && !grpcutil.IsClosedConnection(err) &&
						!grpcutil.IsConnectionRejected(err) {
						log.Health.Errorf(ctx, "removing connection to %s due to error: %v", target, err)
					}
					rpcCtx.removeConn(conn, thisConnKeys...)
				}); err != nil {
				// If node is draining (`err` will always equal stop.ErrUnavailable
				// here), return special error (see its comments).
				_ = err // ignore this error
				conn.dialErr = errDialRejected
			}
		}
		if conn.dialErr != nil {
			rpcCtx.removeConn(conn, thisConnKeys...)
		}
	})

	return conn
}

// NewBreaker creates a new circuit breaker properly configured for RPC
// connections. name is used internally for logging state changes of the
// returned breaker.
func (rpcCtx *Context) NewBreaker(name string) *circuit.Breaker {
	if rpcCtx.BreakerFactory != nil {
		return rpcCtx.BreakerFactory()
	}
	return newBreaker(rpcCtx.MasterCtx, name, &rpcCtx.breakerClock)
}

// ErrNotHeartbeated is returned by ConnHealth when we have not yet performed
// the first heartbeat.
var ErrNotHeartbeated = errors.New("not yet heartbeated")

// ErrNoConnection is returned by ConnHealth when no connection exists to
// the node.
var ErrNoConnection = errors.New("no connection found")

// runHeartbeat runs the heartbeat loop for the given RPC connection.
// The ctx passed as argument must be derived from rpcCtx.masterCtx, so
// that it respects the same cancellation policy.
func (rpcCtx *Context) runHeartbeat(
	ctx context.Context, conn *Connection, target string, redialChan <-chan struct{},
) (retErr error) {
	rpcCtx.metrics.HeartbeatLoopsStarted.Inc(1)
	// setInitialHeartbeatDone is idempotent and is critical to notify Connect
	// callers of the failure in the case where no heartbeat is ever sent.
	state := updateHeartbeatState(&rpcCtx.metrics, heartbeatNotRunning, heartbeatInitializing)
	initialHeartbeatDone := false
	setInitialHeartbeatDone := func() {
		if !initialHeartbeatDone {
			close(conn.initialHeartbeatDone)
			initialHeartbeatDone = true
		}
	}
	defer func() {
		if retErr != nil {
			rpcCtx.metrics.HeartbeatLoopsExited.Inc(1)
		}
		updateHeartbeatState(&rpcCtx.metrics, state, heartbeatNotRunning)
		setInitialHeartbeatDone()
	}()
	maxOffset := rpcCtx.Clock.MaxOffset()
	maxOffsetNanos := maxOffset.Nanoseconds()

	heartbeatClient := NewHeartbeatClient(conn.grpcConn)

	var heartbeatTimer timeutil.Timer
	defer heartbeatTimer.Stop()

	// Give the first iteration a wait-free heartbeat attempt.
	heartbeatTimer.Reset(0)
	everSucceeded := false
	// Both transient and permanent errors can arise here. Transient errors
	// set the `heartbeatResult.err` field but retain the connection.
	// Permanent errors return an error from this method, which means that
	// the connection will be removed. Errors are presumed transient by
	// default, but some - like ClusterID or version mismatches, as well as
	// PermissionDenied errors injected by OnOutgoingPing, are considered permanent.
	returnErr := false
	for {
		select {
		case <-redialChan:
			return grpcutil.ErrCannotReuseClientConn
		case <-rpcCtx.Stopper.ShouldQuiesce():
			return nil
		case <-heartbeatTimer.C:
			heartbeatTimer.Read = true
		}

		if err := rpcCtx.Stopper.RunTaskWithErr(ctx, "rpc heartbeat", func(ctx context.Context) error {
			// We re-mint the PingRequest to pick up any asynchronous update to clusterID.
			clusterID := rpcCtx.ClusterID.Get()
			request := &PingRequest{
				OriginNodeID:         rpcCtx.NodeID.Get(),
				OriginAddr:           rpcCtx.Config.Addr,
				OriginMaxOffsetNanos: maxOffsetNanos,
				ClusterID:            &clusterID,
				TargetNodeID:         conn.remoteNodeID,
				ServerVersion:        rpcCtx.Settings.Version.BinaryVersion(),
			}

			interceptor := func(context.Context, *PingRequest) error { return nil }
			if fn := rpcCtx.OnOutgoingPing; fn != nil {
				interceptor = fn
			}

			var response *PingResponse
			sendTime := rpcCtx.Clock.PhysicalTime()
			ping := func(ctx context.Context) error {
				// NB: We want the request to fail-fast (the default), otherwise we won't
				// be notified of transport failures.
				if err := interceptor(ctx, request); err != nil {
					returnErr = true
					return err
				}
				var err error
				response, err = heartbeatClient.Ping(ctx, request)
				return err
			}
			var err error
			if rpcCtx.heartbeatTimeout > 0 {
				err = contextutil.RunWithTimeout(ctx, "rpc heartbeat", rpcCtx.heartbeatTimeout, ping)
			} else {
				err = ping(ctx)
			}

			if grpcutil.IsConnectionRejected(err) {
				returnErr = true
			}

			if err == nil {
				// We verify the cluster name on the initiator side (instead
				// of the heartbeat service side, as done for the cluster ID
				// and node ID checks) so that the operator who is starting a
				// new node in a cluster and mistakenly joins the wrong
				// cluster gets a chance to see the error message on their
				// management console.
				if !rpcCtx.Config.DisableClusterNameVerification && !response.DisableClusterNameVerification {
					err = errors.Wrap(
						checkClusterName(rpcCtx.Config.ClusterName, response.ClusterName),
						"cluster name check failed on ping response")
					if err != nil {
						returnErr = true
					}
				}
			}

			if err == nil {
				err = errors.Wrap(
					checkVersion(ctx, rpcCtx.Settings, response.ServerVersion),
					"version compatibility check failed on ping response")
				if err != nil {
					returnErr = true
				}
			}

			if err == nil {
				everSucceeded = true
				receiveTime := rpcCtx.Clock.PhysicalTime()

				// Only update the clock offset measurement if we actually got a
				// successful response from the server.
				pingDuration := receiveTime.Sub(sendTime)
				maxOffset := rpcCtx.Clock.MaxOffset()
				if pingDuration > maximumPingDurationMult*maxOffset {
					request.Offset.Reset()
				} else {
					// Offset and error are measured using the remote clock reading
					// technique described in
					// http://se.inf.tu-dresden.de/pubs/papers/SRDS1994.pdf, page 6.
					// However, we assume that drift and min message delay are 0, for
					// now.
					request.Offset.MeasuredAt = receiveTime.UnixNano()
					request.Offset.Uncertainty = (pingDuration / 2).Nanoseconds()
					remoteTimeNow := timeutil.Unix(0, response.ServerTime).Add(pingDuration / 2)
					request.Offset.Offset = remoteTimeNow.Sub(receiveTime).Nanoseconds()
				}
				rpcCtx.RemoteClocks.UpdateOffset(ctx, target, request.Offset, pingDuration)

				if cb := rpcCtx.HeartbeatCB; cb != nil {
					cb()
				}
			}

			hr := heartbeatResult{
				everSucceeded: everSucceeded,
				err:           err,
			}
			state = updateHeartbeatState(&rpcCtx.metrics, state, hr.state())
			conn.heartbeatResult.Store(hr)
			setInitialHeartbeatDone()
			if returnErr {
				return err
			}
			return nil
		}); err != nil {
			return err
		}

		heartbeatTimer.Reset(rpcCtx.Config.RPCHeartbeatInterval)
	}
}

// NewHeartbeatService returns a HeartbeatService initialized from the Context.
func (rpcCtx *Context) NewHeartbeatService() *HeartbeatService {
	return &HeartbeatService{
		clock:                                 rpcCtx.Clock,
		remoteClockMonitor:                    rpcCtx.RemoteClocks,
		clusterName:                           rpcCtx.ClusterName(),
		disableClusterNameVerification:        rpcCtx.Config.DisableClusterNameVerification,
		clusterID:                             rpcCtx.ClusterID,
		nodeID:                                rpcCtx.NodeID,
		settings:                              rpcCtx.Settings,
		onHandlePing:                          rpcCtx.OnIncomingPing,
		testingAllowNamedRPCToAnonymousServer: rpcCtx.TestingAllowNamedRPCToAnonymousServer,
	}
}
