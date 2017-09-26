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
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/rubyist/circuitbreaker"
	"golang.org/x/net/context"
	"golang.org/x/sync/syncmap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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

// SourceAddr provides a way to specify a source/local address for outgoing
// connections. It should only ever be set by testing code, and is not thread
// safe (so it must be initialized before the server starts).
var SourceAddr = func() net.Addr {
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
		grpc.RPCDecompressor(snappyDecompressor{}),
		// By default, gRPC disconnects clients that send "too many" pings,
		// but we don't really care about that, so configure the server to be
		// as permissive as possible.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             time.Nanosecond,
			PermitWithoutStream: true,
		}),
	}
	// Compression is enabled separately from decompression to allow staged
	// rollout.
	if ctx.rpcCompression {
		opts = append(opts, grpc.RPCCompressor(snappyCompressor{}))
	}
	if !ctx.Insecure {
		tlsConfig, err := ctx.GetServerTLSConfig()
		if err != nil {
			panic(err)
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	if tracer := ctx.AmbientCtx.Tracer; tracer != nil {
		// We use a SpanInclusionFunc to save a bit of unnecessary work when
		// tracing is disabled.
		interceptor := otgrpc.OpenTracingServerInterceptor(
			tracer,
			otgrpc.IncludingSpans(otgrpc.SpanInclusionFunc(spanInclusionFunc)),
		)
		opts = append(opts, grpc.UnaryInterceptor(interceptor))
	}
	s := grpc.NewServer(opts...)
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              ctx.LocalClock,
		remoteClockMonitor: ctx.RemoteClocks,
	})
	return s
}

// errValue is used to allow storing an error in an atomic.Value which does not
// support storing a nil interface.
type errValue struct {
	error
}

type connMeta struct {
	sync.Once
	conn         *grpc.ClientConn
	dialErr      error
	heartbeatErr atomic.Value
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

	// For unittesting.
	BreakerFactory func() *circuit.Breaker
}

// NewContext creates an rpc Context with the supplied values.
func NewContext(
	ambient log.AmbientContext, baseCtx *base.Config, hlcClock *hlc.Clock, stopper *stop.Stopper,
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
			meta := v.(*connMeta)
			meta.Do(func() {
				// Make sure initialization is not in progress when we're removing the
				// conn. We need to set the error in case we win the race against the
				// real initialization code.
				if meta.dialErr == nil {
					meta.dialErr = &roachpb.NodeUnavailableError{}
				}
			})
			ctx.removeConn(k.(string), meta)
			return true
		})
	})

	return ctx
}

// GetLocalInternalServerForAddr returns the context's internal batch server
// for target, if it exists.
func (ctx *Context) GetLocalInternalServerForAddr(target string) roachpb.InternalServer {
	if target == ctx.Addr {
		return ctx.localInternalServer
	}
	return nil
}

// SetLocalInternalServer sets the context's local internal batch server.
func (ctx *Context) SetLocalInternalServer(internalServer roachpb.InternalServer) {
	ctx.localInternalServer = internalServer
}

func (ctx *Context) removeConn(key string, meta *connMeta) {
	ctx.conns.Delete(key)
	if log.V(1) {
		log.Infof(ctx.masterCtx, "closing %s", key)
	}
	if conn := meta.conn; conn != nil {
		if err := conn.Close(); err != nil && !grpcutil.IsClosedConnection(err) {
			if log.V(1) {
				log.Errorf(ctx.masterCtx, "failed to close client connection: %s", err)
			}
		}
	}
}

// GRPCDial calls grpc.Dial with the options appropriate for the context.
func (ctx *Context) GRPCDial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	value, ok := ctx.conns.Load(target)
	if !ok {
		meta := &connMeta{}
		meta.heartbeatErr.Store(errValue{ErrNotHeartbeated})
		value, _ = ctx.conns.LoadOrStore(target, meta)
	}

	meta := value.(*connMeta)
	meta.Do(func() {
		var dialOpt grpc.DialOption
		if ctx.Insecure {
			dialOpt = grpc.WithInsecure()
		} else {
			tlsConfig, err := ctx.GetClientTLSConfig()
			if err != nil {
				meta.dialErr = err
				return
			}
			dialOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
		}

		var dialOpts []grpc.DialOption
		dialOpts = append(dialOpts, dialOpt)
		// The limiting factor for lowering the max message size is the fact
		// that a single large kv can be sent over the network in one message.
		// Our maximum kv size is unlimited, so we need this to be very large.
		//
		// TODO(peter,tamird): need tests before lowering.
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32),
		))
		dialOpts = append(dialOpts, grpc.WithBackoffMaxDelay(maxBackoff))
		dialOpts = append(dialOpts, grpc.WithDecompressor(snappyDecompressor{}))
		// Compression is enabled separately from decompression to allow staged
		// rollout.
		if ctx.rpcCompression {
			dialOpts = append(dialOpts, grpc.WithCompressor(snappyCompressor{}))
		}
		dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			// Send periodic pings on the connection.
			Time: base.NetworkTimeout,
			// If the pings don't get a response within the timeout, we might be
			// experiencing a network partition. gRPC will close the transport-level
			// connection and all the pending RPCs (which may not have timeouts) will
			// fail eagerly. gRPC will then reconnect the transport transparently.
			Timeout: base.NetworkTimeout,
			// Do the pings even when there are no ongoing RPCs.
			PermitWithoutStream: true,
		}))
		dialOpts = append(dialOpts,
			grpc.WithInitialWindowSize(initialWindowSize),
			grpc.WithInitialConnWindowSize(initialConnWindowSize))
		dialOpts = append(dialOpts, opts...)

		if SourceAddr != nil {
			dialOpts = append(dialOpts, grpc.WithDialer(
				func(addr string, timeout time.Duration) (net.Conn, error) {
					dialer := net.Dialer{
						Timeout:   timeout,
						LocalAddr: SourceAddr,
					}
					return dialer.Dial("tcp", addr)
				},
			))
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

		if log.V(1) {
			log.Infof(ctx.masterCtx, "dialing %s", target)
		}
		meta.conn, meta.dialErr = grpc.DialContext(ctx.masterCtx, target, dialOpts...)
		if ctx.GetLocalInternalServerForAddr(target) == nil && meta.dialErr == nil {
			if err := ctx.Stopper.RunTask(
				ctx.masterCtx, "rpc.Context: grpc heartbeat", func(masterCtx context.Context) {
					ctx.Stopper.RunWorker(masterCtx, func(masterCtx context.Context) {
						err := ctx.runHeartbeat(meta, target)
						if err != nil && !grpcutil.IsClosedConnection(err) {
							log.Errorf(masterCtx, "removing connection to %s due to error: %s", target, err)
						}
						ctx.removeConn(target, meta)
					})
				}); err != nil {
				meta.dialErr = err
				// removeConn and ctx's cleanup worker both lock ctx.conns. However,
				// to avoid racing with meta's initialization, the cleanup worker
				// blocks on meta.Do while holding ctx.conns. Invoke removeConn
				// asynchronously to avoid deadlock.
				go ctx.removeConn(target, meta)
			}
		}
	})

	return meta.conn, meta.dialErr
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
// host (e.g. GRPCDial was never called for that address).
var ErrNotConnected = errors.New("not connected")

// ErrNotHeartbeated is returned by ConnHealth when we have not yet performed
// the first heartbeat.
var ErrNotHeartbeated = errors.New("not yet heartbeated")

// ConnHealth returns whether the most recent heartbeat succeeded or not.
// This should not be used as a definite status of a node's health and just used
// to prioritize healthy nodes over unhealthy ones.
func (ctx *Context) ConnHealth(target string) error {
	if ctx.GetLocalInternalServerForAddr(target) != nil {
		// The local server is always considered healthy.
		return nil
	}
	if value, ok := ctx.conns.Load(target); ok {
		return value.(*connMeta).heartbeatErr.Load().(errValue).error
	}
	return ErrNotConnected
}

func (ctx *Context) runHeartbeat(meta *connMeta, target string) error {
	maxOffset := ctx.LocalClock.MaxOffset()

	request := PingRequest{
		Addr:           ctx.Addr,
		MaxOffsetNanos: maxOffset.Nanoseconds(),
	}
	heartbeatClient := NewHeartbeatClient(meta.conn)

	var heartbeatTimer timeutil.Timer
	defer heartbeatTimer.Stop()

	// Give the first iteration a wait-free heartbeat attempt.
	heartbeatTimer.Reset(0)
	for {
		select {
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
		meta.heartbeatErr.Store(errValue{err})

		// HACK: work around https://github.com/grpc/grpc-go/issues/1026
		// Getting a "connection refused" error from the "write" system call
		// has confused grpc's error handling and this connection is permanently
		// broken.
		// TODO(bdarnell): remove this when the upstream bug is fixed.
		if err != nil && strings.Contains(err.Error(), "write: connection refused") {
			return nil
		}

		if err == nil {
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

		heartbeatTimer.Reset(ctx.heartbeatInterval)
	}
}
