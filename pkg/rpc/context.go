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
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/rubyist/circuitbreaker"
	"golang.org/x/sync/syncmap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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

func spanInclusionFunc(
	parentSpanCtx opentracing.SpanContext, method string, req, resp interface{},
) bool {
	return parentSpanCtx != nil && !tracing.IsNoopContext(parentSpanCtx)
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
			otgrpc.IncludingSpans(otgrpc.SpanInclusionFunc(spanInclusionFunc)),
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
		return nil, errors.Wrap(h.err, "initial connection heartbeat failed")
	}
	return c.grpcConn, nil
}

func (c *Connection) setInitialHeartbeatDone() {
	c.validatedOnce.Do(func() {
		close(c.initialHeartbeatDone)
	})
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

	localInternalServer roachpb.InternalServer

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

// GetLocalInternalServerForAddr returns the context's internal batch server
// for target, if it exists.
func (ctx *Context) GetLocalInternalServerForAddr(target string) roachpb.InternalServer {
	if target == ctx.AdvertiseAddr {
		return ctx.localInternalServer
	}
	return nil
}

// SetLocalInternalServer sets the context's local internal batch server.
func (ctx *Context) SetLocalInternalServer(internalServer roachpb.InternalServer) {
	ctx.localInternalServer = internalServer
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
			otgrpc.IncludingSpans(otgrpc.SpanInclusionFunc(spanInclusionFunc)),
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
		return dialer.Dial("tcp", addr)
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
// connections.
func (ctx *Context) NewBreaker() *circuit.Breaker {
	if ctx.BreakerFactory != nil {
		return ctx.BreakerFactory()
	}
	return newBreaker(&ctx.breakerClock)
}

// ErrNotConnected is returned by ConnHealth when there is no connection to the
// host (e.g. GRPCDial was never called for that address, or a connection has
// been closed and not reconnected).
var ErrNotConnected = errors.New("not connected")

// ErrNotHeartbeated is returned by ConnHealth when we have not yet performed
// the first heartbeat.
var ErrNotHeartbeated = errors.New("not yet heartbeated")

// ConnHealth returns whether the most recent heartbeat succeeded or not.
// This should not be used as a definite status of a node's health and just used
// to prioritize healthy nodes over unhealthy ones.
//
// NB: as of #22658, this does not work as you think. We kick
// connections out of the connection pool as soon as they run into an
// error, at which point their ConnHealth will reset to
// ErrNotConnected. ConnHealth does no more return a sane notion of
// "recent connection health". When it returns nil all seems well, but
// if it doesn't then this may mean that the node is simply refusing
// connections (and is thus unconnected most of the time), or that the
// node hasn't been connected to but is perfectly healthy.
//
// See #23829.
func (ctx *Context) ConnHealth(target string) error {
	if ctx.GetLocalInternalServerForAddr(target) != nil {
		// The local server is always considered healthy.
		return nil
	}
	if value, ok := ctx.conns.Load(target); ok {
		return value.(*Connection).heartbeatResult.Load().(heartbeatResult).err
	}
	return ErrNotConnected
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

		goCtx := ctx.masterCtx
		var cancel context.CancelFunc
		if hbTimeout := ctx.heartbeatTimeout; hbTimeout > 0 {
			goCtx, cancel = context.WithTimeout(goCtx, hbTimeout)
		}
		sendTime := ctx.LocalClock.PhysicalTime()
		// NB: We want the request to fail-fast (the default), otherwise we won't
		// be notified of transport failures.
		response, err := heartbeatClient.Ping(goCtx, &request)
		if cancel != nil {
			cancel()
		}

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
