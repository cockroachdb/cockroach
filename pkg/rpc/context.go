// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rpc

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/sync/syncmap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
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
	defaultHeartbeatInterval = 3 * time.Second
	// The coefficient by which the maximum offset is multiplied to determine the
	// maximum acceptable measurement latency.
	maximumPingDurationMult = 2
)

const (
	defaultWindowSize     = 65535
	initialWindowSize     = defaultWindowSize * 32 // for an RPC
	initialConnWindowSize = initialWindowSize * 16 // for a connection
)

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

// spanInclusionFuncForServer is used as a SpanInclusionFunc for the server-side
// of RPCs, deciding for which operations the gRPC opentracing interceptor should
// create a span.
func spanInclusionFuncForServer(
	t *tracing.Tracer, parentSpanCtx opentracing.SpanContext, method string, req, resp interface{},
) bool {
	// Is client tracing?
	return (parentSpanCtx != nil && !tracing.IsNoopContext(parentSpanCtx)) ||
		// Should we trace regardless of the client? This is useful for calls coming
		// through the HTTP->RPC gateway (i.e. the AdminUI), where client is never
		// tracing.
		t.AlwaysTrace()
}

// spanInclusionFuncForClient is used as a SpanInclusionFunc for the client-side
// of RPCs, deciding for which operations the gRPC opentracing interceptor should
// create a span.
func spanInclusionFuncForClient(
	parentSpanCtx opentracing.SpanContext, method string, req, resp interface{},
) bool {
	return parentSpanCtx != nil && !tracing.IsNoopContext(parentSpanCtx)
}

func requireSuperUser(ctx context.Context) error {
	// TODO(marc): grpc's authentication model (which gives credential access in
	// the request handler) doesn't really fit with the current design of the
	// security package (which assumes that TLS state is only given at connection
	// time) - that should be fixed.
	if grpcutil.IsLocalRequestContext(ctx) {
		// This is an in-process request. Bypass authentication check.
	} else if peer, ok := peer.FromContext(ctx); ok {
		if tlsInfo, ok := peer.AuthInfo.(credentials.TLSInfo); ok {
			certUser, err := security.GetCertificateUser(&tlsInfo.State)
			if err != nil {
				return err
			}
			// TODO(benesch): the vast majority of RPCs should be limited to just
			// NodeUser. This is not a security concern, as RootUser has access to
			// read and write all data, merely good hygiene. For example, there is
			// no reason to permit the root user to send raw Raft RPCs.
			if certUser != security.NodeUser && certUser != security.RootUser {
				return errors.Errorf("user %s is not allowed to perform this RPC", certUser)
			}
		}
	} else {
		return errors.New("internal authentication error: TLSInfo is not available in request context")
	}
	return nil
}

// NewServer is a thin wrapper around grpc.NewServer that registers a heartbeat
// service.
func NewServer(ctx *Context) *grpc.Server {
	return NewServerWithInterceptor(ctx, nil)
}

// NewServerWithInterceptor is like NewServer, but accepts an additional
// interceptor which is called before streaming and unary RPCs and may inject an
// error.
func NewServerWithInterceptor(
	ctx *Context, interceptor func(fullMethod string) error,
) *grpc.Server {
	opts := []grpc.ServerOption{
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
		grpc.StatsHandler(&ctx.stats),
	}
	if !ctx.Insecure {
		tlsConfig, err := ctx.GetServerTLSConfig()
		if err != nil {
			panic(err)
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	var unaryInterceptor grpc.UnaryServerInterceptor
	var streamInterceptor grpc.StreamServerInterceptor

	if tracer := ctx.AmbientCtx.Tracer; tracer != nil {
		// We use a SpanInclusionFunc to save a bit of unnecessary work when
		// tracing is disabled.
		unaryInterceptor = otgrpc.OpenTracingServerInterceptor(
			tracer,
			otgrpc.IncludingSpans(otgrpc.SpanInclusionFunc(
				func(
					parentSpanCtx opentracing.SpanContext,
					method string,
					req, resp interface{}) bool {
					// This anonymous func serves to bind the tracer for
					// spanInclusionFuncForServer.
					return spanInclusionFuncForServer(
						tracer.(*tracing.Tracer), parentSpanCtx, method, req, resp)
				})),
		)
		// TODO(tschottdorf): should set up tracing for stream-based RPCs as
		// well. The otgrpc package has no such facility, but there's also this:
		//
		// https://github.com/grpc-ecosystem/go-grpc-middleware/tree/master/tracing/opentracing
	}

	// TODO(tschottdorf): when setting up the interceptors below, could make the
	// functions a wee bit more performant by hoisting some of the nil checks
	// out. Doubt measurements can tell the difference though.

	if interceptor != nil {
		prevUnaryInterceptor := unaryInterceptor
		unaryInterceptor = func(
			ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
		) (interface{}, error) {
			if err := interceptor(info.FullMethod); err != nil {
				return nil, err
			}
			if prevUnaryInterceptor != nil {
				return prevUnaryInterceptor(ctx, req, info, handler)
			}
			return handler(ctx, req)
		}
	}

	if interceptor != nil {
		prevStreamInterceptor := streamInterceptor
		streamInterceptor = func(
			srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
		) error {
			if err := interceptor(info.FullMethod); err != nil {
				return err
			}
			if prevStreamInterceptor != nil {
				return prevStreamInterceptor(srv, stream, info, handler)
			}
			return handler(srv, stream)
		}
	}

	if !ctx.Insecure {
		prevUnaryInterceptor := unaryInterceptor
		unaryInterceptor = func(
			ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
		) (interface{}, error) {
			if err := requireSuperUser(ctx); err != nil {
				return nil, err
			}
			if prevUnaryInterceptor != nil {
				return prevUnaryInterceptor(ctx, req, info, handler)
			}
			return handler(ctx, req)
		}
		prevStreamInterceptor := streamInterceptor
		streamInterceptor = func(
			srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
		) error {
			if err := requireSuperUser(stream.Context()); err != nil {
				return err
			}
			if prevStreamInterceptor != nil {
				return prevStreamInterceptor(srv, stream, info, handler)
			}
			return handler(srv, stream)
		}
	}

	if unaryInterceptor != nil {
		opts = append(opts, grpc.UnaryInterceptor(unaryInterceptor))
	}
	if streamInterceptor != nil {
		opts = append(opts, grpc.StreamInterceptor(streamInterceptor))
	}

	s := grpc.NewServer(opts...)
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              ctx.LocalClock,
		remoteClockMonitor: ctx.RemoteClocks,
		clusterID:          &ctx.ClusterID,
		version:            ctx.version,
	})
	return s
}

type heartbeatResult struct {
	everSucceeded bool  // true if the heartbeat has ever succeeded
	err           error // heartbeat error. should not be nil if everSucceeded is false
}

// Connection is a wrapper around grpc.ClientConn. It prevents the underlying
// connection from being used until it has been validated via heartbeat.
type Connection struct {
	grpcConn             *grpc.ClientConn
	dialErr              error         // error while dialing; if set, connection is unusable
	heartbeatResult      atomic.Value  // result of latest heartbeat
	initialHeartbeatDone chan struct{} // closed after first heartbeat
	stopper              *stop.Stopper

	initOnce      sync.Once
	validatedOnce sync.Once
}

func newConnection(stopper *stop.Stopper) *Connection {
	c := &Connection{
		initialHeartbeatDone: make(chan struct{}),
		stopper:              stopper,
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
	case <-c.stopper.ShouldStop():
		return nil, errors.Errorf("stopped")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// If connection is invalid, return latest heartbeat error.
	h := c.heartbeatResult.Load().(heartbeatResult)
	if !h.everSucceeded {
		return nil, netutil.NewInitialHeartBeatFailedError(h.err)
	}
	return c.grpcConn, nil
}

func (c *Connection) setInitialHeartbeatDone() {
	c.validatedOnce.Do(func() {
		close(c.initialHeartbeatDone)
	})
}

// Health returns an error indicating the success or failure of the
// connection's latest heartbeat. Returns ErrNotHeartbeated if the
// first heartbeat has not completed.
func (c *Connection) Health() error {
	return c.heartbeatResult.Load().(heartbeatResult).err
}

// Context contains the fields required by the rpc framework.
type Context struct {
	*base.Config

	AmbientCtx   log.AmbientContext
	LocalClock   *hlc.Clock
	breakerClock breakerClock
	Stopper      *stop.Stopper
	RemoteClocks *RemoteClockMonitor
	masterCtx    context.Context

	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration
	HeartbeatCB       func()

	rpcCompression bool

	localInternalClient roachpb.InternalClient

	conns syncmap.Map

	stats StatsHandler

	ClusterID base.ClusterIDContainer
	version   *cluster.ExposedClusterVersion

	// For unittesting.
	BreakerFactory  func() *circuit.Breaker
	testingDialOpts []grpc.DialOption
}

// NewContext creates an rpc Context with the supplied values.
func NewContext(
	ambient log.AmbientContext,
	baseCtx *base.Config,
	hlcClock *hlc.Clock,
	stopper *stop.Stopper,
	version *cluster.ExposedClusterVersion,
) *Context {
	if hlcClock == nil {
		panic("nil clock is forbidden")
	}
	ctx := &Context{
		AmbientCtx: ambient,
		Config:     baseCtx,
		LocalClock: hlcClock,
		breakerClock: breakerClock{
			clock: hlcClock,
		},
		rpcCompression: enableRPCCompression,
		version:        version,
	}
	var cancel context.CancelFunc
	ctx.masterCtx, cancel = context.WithCancel(ambient.AnnotateCtx(context.Background()))
	ctx.Stopper = stopper
	ctx.RemoteClocks = newRemoteClockMonitor(
		ctx.LocalClock, 10*defaultHeartbeatInterval, baseCtx.HistogramWindowInterval)
	ctx.heartbeatInterval = defaultHeartbeatInterval
	ctx.heartbeatTimeout = 2 * defaultHeartbeatInterval

	stopper.RunWorker(ctx.masterCtx, func(context.Context) {
		<-stopper.ShouldQuiesce()

		cancel()
		ctx.conns.Range(func(k, v interface{}) bool {
			conn := v.(*Connection)
			conn.initOnce.Do(func() {
				// Make sure initialization is not in progress when we're removing the
				// conn. We need to set the error in case we win the race against the
				// real initialization code.
				if conn.dialErr == nil {
					conn.dialErr = &roachpb.NodeUnavailableError{}
				}
			})
			ctx.removeConn(k.(string), conn)
			return true
		})
	})

	return ctx
}

// GetStatsMap returns a map of network statistics maintained by the
// internal stats handler. The map is from the remote network address
// (in string form) to an rpc.Stats object.
func (ctx *Context) GetStatsMap() *syncmap.Map {
	return &ctx.stats.stats
}

// GetLocalInternalClientForAddr returns the context's internal batch client
// for target, if it exists.
func (ctx *Context) GetLocalInternalClientForAddr(target string) roachpb.InternalClient {
	if target == ctx.AdvertiseAddr {
		return ctx.localInternalClient
	}
	return nil
}

type internalClientAdapter struct {
	roachpb.InternalServer
}

func (a internalClientAdapter) Batch(
	ctx context.Context, ba *roachpb.BatchRequest, _ ...grpc.CallOption,
) (*roachpb.BatchResponse, error) {
	return a.InternalServer.Batch(ctx, ba)
}

type rangeFeedClientAdapter struct {
	ctx    context.Context
	eventC chan *roachpb.RangeFeedEvent
	errC   chan error
}

// roachpb.Internal_RangeFeedServer methods.
func (a rangeFeedClientAdapter) Recv() (*roachpb.RangeFeedEvent, error) {
	// Prioritize eventC. Both channels are buffered and the only guarantee we
	// have is that once an error is sent on errC no other events will be sent
	// on eventC again.
	select {
	case e := <-a.eventC:
		return e, nil
	case err := <-a.errC:
		select {
		case e := <-a.eventC:
			a.errC <- err
			return e, nil
		default:
			return nil, err
		}
	}
}

// roachpb.Internal_RangeFeedServer methods.
func (a rangeFeedClientAdapter) Send(e *roachpb.RangeFeedEvent) error {
	select {
	case a.eventC <- e:
		return nil
	case <-a.ctx.Done():
		return a.ctx.Err()
	}
}

// grpc.ClientStream methods.
func (rangeFeedClientAdapter) Header() (metadata.MD, error) { panic("unimplemented") }
func (rangeFeedClientAdapter) Trailer() metadata.MD         { panic("unimplemented") }
func (rangeFeedClientAdapter) CloseSend() error             { panic("unimplemented") }

// grpc.ServerStream methods.
func (rangeFeedClientAdapter) SetHeader(metadata.MD) error  { panic("unimplemented") }
func (rangeFeedClientAdapter) SendHeader(metadata.MD) error { panic("unimplemented") }
func (rangeFeedClientAdapter) SetTrailer(metadata.MD)       { panic("unimplemented") }

// grpc.Stream methods.
func (a rangeFeedClientAdapter) Context() context.Context  { return a.ctx }
func (rangeFeedClientAdapter) SendMsg(m interface{}) error { panic("unimplemented") }
func (rangeFeedClientAdapter) RecvMsg(m interface{}) error { panic("unimplemented") }

var _ roachpb.Internal_RangeFeedClient = rangeFeedClientAdapter{}
var _ roachpb.Internal_RangeFeedServer = rangeFeedClientAdapter{}

func (a internalClientAdapter) RangeFeed(
	ctx context.Context, args *roachpb.RangeFeedRequest, _ ...grpc.CallOption,
) (roachpb.Internal_RangeFeedClient, error) {
	ctx, cancel := context.WithCancel(ctx)
	rfAdapter := rangeFeedClientAdapter{
		ctx:    ctx,
		eventC: make(chan *roachpb.RangeFeedEvent, 128),
		errC:   make(chan error, 1),
	}

	go func() {
		defer cancel()
		err := a.InternalServer.RangeFeed(args, rfAdapter)
		if err == nil {
			err = io.EOF
		}
		rfAdapter.errC <- err
	}()

	return rfAdapter, nil
}

var _ roachpb.InternalClient = internalClientAdapter{}

// IsLocal returns true if the given InternalClient is local.
func IsLocal(iface roachpb.InternalClient) bool {
	_, ok := iface.(internalClientAdapter)
	return ok // internalClientAdapter is used for local connections.
}

// SetLocalInternalServer sets the context's local internal batch server.
func (ctx *Context) SetLocalInternalServer(internalServer roachpb.InternalServer) {
	ctx.localInternalClient = internalClientAdapter{internalServer}
}

func (ctx *Context) removeConn(key string, conn *Connection) {
	ctx.conns.Delete(key)
	if log.V(1) {
		log.Infof(ctx.masterCtx, "closing %s", key)
	}
	if grpcConn := conn.grpcConn; grpcConn != nil {
		if err := grpcConn.Close(); err != nil && !grpcutil.IsClosedConnection(err) {
			if log.V(1) {
				log.Errorf(ctx.masterCtx, "failed to close client connection: %s", err)
			}
		}
	}
}

// GRPCDialOptions returns the minimal `grpc.DialOption`s necessary to connect
// to a server created with `NewServer`.
//
// At the time of writing, this is being used for making net.Pipe-based
// connections, so only those options that affect semantics are included. In
// particular, performance tuning options are omitted. Decompression is
// necessarily included to support compression-enabled servers, and compression
// is included for symmetry. These choices are admittedly subjective.
func (ctx *Context) GRPCDialOptions() ([]grpc.DialOption, error) {
	var dialOpts []grpc.DialOption
	if ctx.Insecure {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	} else {
		tlsConfig, err := ctx.GetClientTLSConfig()
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
	if ctx.rpcCompression {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor((snappyCompressor{}).Name())))
	}

	if tracer := ctx.AmbientCtx.Tracer; tracer != nil {
		// We use a SpanInclusionFunc to circumvent the interceptor's work when
		// tracing is disabled. Otherwise, the interceptor causes an increase in
		// the number of packets (even with an empty context!). See #17177.
		interceptor := otgrpc.OpenTracingClientInterceptor(
			tracer,
			otgrpc.IncludingSpans(otgrpc.SpanInclusionFunc(spanInclusionFuncForClient)),
		)
		dialOpts = append(dialOpts, grpc.WithUnaryInterceptor(interceptor))
	}

	return dialOpts, nil
}

// onlyOnceDialer implements the grpc.WithDialer interface but only
// allows a single connection attempt. If a reconnection is attempted,
// redialChan is closed to signal a higher-level retry loop. This
// ensures that our initial heartbeat (and its version/clusterID
// validation) occurs on every new connection.
type onlyOnceDialer struct {
	ctx context.Context
	syncutil.Mutex
	dialed     bool
	closed     bool
	redialChan chan struct{}
}

func (ood *onlyOnceDialer) dial(addr string, timeout time.Duration) (net.Conn, error) {
	ood.Lock()
	defer ood.Unlock()
	if !ood.dialed {
		ood.dialed = true
		dialer := net.Dialer{
			Timeout:   timeout,
			LocalAddr: sourceAddr,
		}
		return dialer.DialContext(ood.ctx, "tcp", addr)
	} else if !ood.closed {
		ood.closed = true
		close(ood.redialChan)
	}
	return nil, grpcutil.ErrCannotReuseClientConn
}

// GRPCDialRaw calls grpc.Dial with options appropriate for the context.
// Unlike GRPCDial, it does not start an RPC heartbeat to validate the
// connection. This connection will not be reconnected automatically;
// the returned channel is closed when a reconnection is attempted.
func (ctx *Context) GRPCDialRaw(target string) (*grpc.ClientConn, <-chan struct{}, error) {
	dialOpts, err := ctx.GRPCDialOptions()
	if err != nil {
		return nil, nil, err
	}

	// Add a stats handler to measure client network stats.
	dialOpts = append(dialOpts, grpc.WithStatsHandler(ctx.stats.newClient(target)))

	dialOpts = append(dialOpts, grpc.WithBackoffMaxDelay(maxBackoff))
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(clientKeepalive))
	dialOpts = append(dialOpts,
		grpc.WithInitialWindowSize(initialWindowSize),
		grpc.WithInitialConnWindowSize(initialConnWindowSize))

	dialer := onlyOnceDialer{
		ctx:        ctx.masterCtx,
		redialChan: make(chan struct{}),
	}
	dialOpts = append(dialOpts, grpc.WithDialer(dialer.dial))

	// add testingDialOpts after our dialer because one of our tests
	// uses a custom dialer (this disables the only-one-connection
	// behavior and redialChan will never be closed).
	dialOpts = append(dialOpts, ctx.testingDialOpts...)

	if log.V(1) {
		log.Infof(ctx.masterCtx, "dialing %s", target)
	}
	conn, err := grpc.DialContext(ctx.masterCtx, target, dialOpts...)
	return conn, dialer.redialChan, err
}

// GRPCDial calls grpc.Dial with options appropriate for the context.
func (ctx *Context) GRPCDial(target string) *Connection {
	value, ok := ctx.conns.Load(target)
	if !ok {
		value, _ = ctx.conns.LoadOrStore(target, newConnection(ctx.Stopper))
	}

	conn := value.(*Connection)
	conn.initOnce.Do(func() {
		var redialChan <-chan struct{}
		conn.grpcConn, redialChan, conn.dialErr = ctx.GRPCDialRaw(target)
		if conn.dialErr == nil {
			if err := ctx.Stopper.RunTask(
				ctx.masterCtx, "rpc.Context: grpc heartbeat", func(masterCtx context.Context) {
					ctx.Stopper.RunWorker(masterCtx, func(masterCtx context.Context) {
						err := ctx.runHeartbeat(conn, target, redialChan)
						if err != nil && !grpcutil.IsClosedConnection(err) {
							log.Errorf(masterCtx, "removing connection to %s due to error: %s", target, err)
						}
						ctx.removeConn(target, conn)
					})
				}); err != nil {
				conn.dialErr = err
				ctx.removeConn(target, conn)
			}
		}
	})

	return conn
}

// NewBreaker creates a new circuit breaker properly configured for RPC
// connections. name is used internally for logging state changes of the
// returned breaker.
func (ctx *Context) NewBreaker(name string) *circuit.Breaker {
	if ctx.BreakerFactory != nil {
		return ctx.BreakerFactory()
	}
	return newBreaker(ctx.masterCtx, name, &ctx.breakerClock)
}

// ErrNotHeartbeated is returned by ConnHealth when we have not yet performed
// the first heartbeat.
var ErrNotHeartbeated = errors.New("not yet heartbeated")

// ConnHealth returns nil if we have an open connection to the given
// target that succeeded on its most recent heartbeat. Otherwise, it
// kicks off a connection attempt (unless one is already in progress
// or we are in a backoff state) and returns an error (typically
// ErrNotHeartbeated). This is a conservative/pessimistic indicator:
// if we have not attempted to talk to the target node recently, an
// error will be returned. This method should therefore be used to
// prioritize among a list of candidate nodes, but not to filter out
// "unhealthy" nodes.
func (ctx *Context) ConnHealth(target string) error {
	if ctx.GetLocalInternalClientForAddr(target) != nil {
		// The local server is always considered healthy.
		return nil
	}
	conn := ctx.GRPCDial(target)
	return conn.Health()
}

func (ctx *Context) runHeartbeat(
	conn *Connection, target string, redialChan <-chan struct{},
) error {
	maxOffset := ctx.LocalClock.MaxOffset()
	clusterID := ctx.ClusterID.Get()

	request := PingRequest{
		Addr:           ctx.Addr,
		MaxOffsetNanos: maxOffset.Nanoseconds(),
		ClusterID:      &clusterID,
		ServerVersion:  ctx.version.ServerVersion,
	}
	heartbeatClient := NewHeartbeatClient(conn.grpcConn)

	var heartbeatTimer timeutil.Timer
	defer heartbeatTimer.Stop()

	// Give the first iteration a wait-free heartbeat attempt.
	heartbeatTimer.Reset(0)
	everSucceeded := false
	for {
		select {
		case <-redialChan:
			return grpcutil.ErrCannotReuseClientConn
		case <-ctx.Stopper.ShouldStop():
			return nil
		case <-heartbeatTimer.C:
			heartbeatTimer.Read = true
		}

		var response *PingResponse
		sendTime := ctx.LocalClock.PhysicalTime()
		err := contextutil.RunWithTimeout(ctx.masterCtx, "rpc heartbeat", ctx.heartbeatTimeout,
			func(goCtx context.Context) error {
				// NB: We want the request to fail-fast (the default), otherwise we won't
				// be notified of transport failures.
				var err error
				response, err = heartbeatClient.Ping(goCtx, &request)
				return err
			})

		if err == nil {
			err = errors.Wrap(
				checkVersion(ctx.version, response.ServerVersion),
				"version compatibility check failed on ping response")
		}

		if err == nil {
			everSucceeded = true
			receiveTime := ctx.LocalClock.PhysicalTime()

			// Only update the clock offset measurement if we actually got a
			// successful response from the server.
			pingDuration := receiveTime.Sub(sendTime)
			maxOffset := ctx.LocalClock.MaxOffset()
			if maxOffset != timeutil.ClocklessMaxOffset &&
				pingDuration > maximumPingDurationMult*maxOffset {

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
			ctx.RemoteClocks.UpdateOffset(ctx.masterCtx, target, request.Offset, pingDuration)

			if cb := ctx.HeartbeatCB; cb != nil {
				cb()
			}
		}
		conn.heartbeatResult.Store(heartbeatResult{
			everSucceeded: everSucceeded,
			err:           err,
		})
		conn.setInitialHeartbeatDone()

		heartbeatTimer.Reset(ctx.heartbeatInterval)
	}
}
