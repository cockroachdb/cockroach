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
//
// Author: Tamir Duberstein (tamird@gmail.com)

package rpc

import (
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/rubyist/circuitbreaker"
	"golang.org/x/net/context"
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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

var enableRPCCompression = envutil.EnvOrDefaultBool("COCKROACH_ENABLE_RPC_COMPRESSION", false)

// NewServer is a thin wrapper around grpc.NewServer that registers a heartbeat
// service.
func NewServer(ctx *Context) *grpc.Server {
	opts := []grpc.ServerOption{
		// The limiting factor for lowering the max message size is the fact
		// that a single large kv can be sent over the network in one message.
		// Our maximum kv size is unlimited, so we need this to be very large.
		// TODO(peter,tamird): need tests before lowering
		grpc.MaxMsgSize(math.MaxInt32),
		// The default number of concurrent streams/requests on a client connection
		// is 100, while the server is unlimited. The client setting can only be
		// controlled by adjusting the server value. Set a very large value for the
		// server value so that we have no fixed limit on the number of concurrent
		// streams/requests on either the client or server.
		grpc.MaxConcurrentStreams(math.MaxInt32),
		grpc.RPCDecompressor(snappyDecompressor{}),
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
	s := grpc.NewServer(opts...)
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              ctx.LocalClock,
		remoteClockMonitor: ctx.RemoteClocks,
	})
	return s
}

type connMeta struct {
	sync.Once
	conn         *grpc.ClientConn
	dialErr      error
	heartbeatErr error
}

// Context contains the fields required by the rpc framework.
type Context struct {
	*base.Config

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

	conns struct {
		syncutil.Mutex
		cache map[string]*connMeta
	}

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
	ctx.conns.cache = make(map[string]*connMeta)

	stopper.RunWorker(ctx.masterCtx, func(context.Context) {
		<-stopper.ShouldQuiesce()

		cancel()
		ctx.conns.Lock()
		for key, meta := range ctx.conns.cache {
			meta.Do(func() {
				// Make sure initialization is not in progress when we're removing the
				// conn. We need to set the error in case we win the race against the
				// real initialization code.
				if meta.dialErr == nil {
					meta.dialErr = &roachpb.NodeUnavailableError{}
				}
			})
			ctx.removeConnLocked(key, meta)
		}
		ctx.conns.Unlock()
	})

	return ctx
}

// GetLocalInternalServerForAddr returns the context's internal batch server
// for addr, if it exists.
func (ctx *Context) GetLocalInternalServerForAddr(addr string) roachpb.InternalServer {
	if addr == ctx.Addr {
		return ctx.localInternalServer
	}
	return nil
}

// SetLocalInternalServer sets the context's local internal batch server.
func (ctx *Context) SetLocalInternalServer(internalServer roachpb.InternalServer) {
	ctx.localInternalServer = internalServer
}

func (ctx *Context) removeConn(key string, meta *connMeta) {
	ctx.conns.Lock()
	ctx.removeConnLocked(key, meta)
	ctx.conns.Unlock()
}

func (ctx *Context) removeConnLocked(key string, meta *connMeta) {
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
	delete(ctx.conns.cache, key)
}

// GRPCDial calls grpc.Dial with the options appropriate for the context.
func (ctx *Context) GRPCDial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	ctx.conns.Lock()
	meta, ok := ctx.conns.cache[target]
	if !ok {
		meta = &connMeta{
			heartbeatErr: errNotHeartbeated,
		}
		ctx.conns.cache[target] = meta
	}
	ctx.conns.Unlock()

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

		if log.V(1) {
			log.Infof(ctx.masterCtx, "dialing %s", target)
		}
		meta.conn, meta.dialErr = grpc.DialContext(ctx.masterCtx, target, dialOpts...)
		if meta.dialErr == nil {
			if err := ctx.Stopper.RunTask(ctx.masterCtx, func(masterCtx context.Context) {
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

var errNotConnected = errors.New("not connected")
var errNotHeartbeated = errors.New("not yet heartbeated")

// ConnHealth returns whether the most recent heartbeat succeeded or not.
// This should not be used as a definite status of a node's health and just used
// to prioritize healthy nodes over unhealthy ones.
func (ctx *Context) ConnHealth(remoteAddr string) error {
	ctx.conns.Lock()
	defer ctx.conns.Unlock()
	if meta, ok := ctx.conns.cache[remoteAddr]; ok {
		return meta.heartbeatErr
	}
	return errNotConnected
}

func (ctx *Context) runHeartbeat(meta *connMeta, remoteAddr string) error {
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
		ctx.conns.Lock()
		meta.heartbeatErr = err
		ctx.conns.Unlock()

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
			if pingDuration > maximumPingDurationMult*ctx.LocalClock.MaxOffset() {
				request.Offset.Reset()
			} else {
				// Offset and error are measured using the remote clock reading
				// technique described in
				// http://se.inf.tu-dresden.de/pubs/papers/SRDS1994.pdf, page 6.
				// However, we assume that drift and min message delay are 0, for
				// now.
				request.Offset.MeasuredAt = receiveTime.UnixNano()
				request.Offset.Uncertainty = (pingDuration / 2).Nanoseconds()
				remoteTimeNow := time.Unix(0, response.ServerTime).Add(pingDuration / 2)
				request.Offset.Offset = remoteTimeNow.Sub(receiveTime).Nanoseconds()
			}
			ctx.RemoteClocks.UpdateOffset(ctx.masterCtx, remoteAddr, request.Offset, pingDuration)

			if cb := ctx.HeartbeatCB; cb != nil {
				cb()
			}
		}

		heartbeatTimer.Reset(ctx.heartbeatInterval)
	}
}
