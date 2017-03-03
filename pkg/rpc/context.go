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
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/rubyist/circuitbreaker"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
var SourceAddr net.Addr

// NewServer is a thin wrapper around grpc.NewServer that registers a heartbeat
// service.
func NewServer(ctx *Context) *grpc.Server {
	opts := []grpc.ServerOption{
		// The limiting factor for lowering the max message size is the fact
		// that a single large kv can be sent over the network in one message.
		// Our maximum kv size is unlimited, so we need this to be very large.
		// TODO(peter,tamird): need tests before lowering
		grpc.MaxMsgSize(math.MaxInt32),
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
		clock:              ctx.localClock,
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

	localClock   *hlc.Clock
	breakerClock breakerClock
	Stopper      *stop.Stopper
	RemoteClocks *RemoteClockMonitor
	masterCtx    context.Context

	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	HeartbeatCB       func()

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
		localClock: hlcClock,
		breakerClock: breakerClock{
			clock: hlcClock,
		},
	}
	var cancel context.CancelFunc
	ctx.masterCtx, cancel = context.WithCancel(ambient.AnnotateCtx(context.Background()))
	ctx.Stopper = stopper
	ctx.RemoteClocks = newRemoteClockMonitor(
		ctx.localClock, 10*defaultHeartbeatInterval, baseCtx.HistogramWindowInterval)
	ctx.HeartbeatInterval = defaultHeartbeatInterval
	ctx.HeartbeatTimeout = 2 * defaultHeartbeatInterval
	ctx.conns.cache = make(map[string]*connMeta)

	stopper.RunWorker(func() {
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
	defer ctx.conns.Unlock()
	ctx.removeConnLocked(key, meta)
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

		dialOpts := make([]grpc.DialOption, 0, 2+len(opts))
		dialOpts = append(dialOpts, dialOpt)
		dialOpts = append(dialOpts, grpc.WithBackoffMaxDelay(maxBackoff))
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
			if err := ctx.Stopper.RunTask(func() {
				ctx.Stopper.RunWorker(func() {
					err := ctx.runHeartbeat(meta, target)
					if err != nil && !grpcutil.IsClosedConnection(err) {
						log.Error(ctx.masterCtx, err)
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
	request := PingRequest{
		Addr:           ctx.Addr,
		MaxOffsetNanos: ctx.localClock.MaxOffset().Nanoseconds(),
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

		sendTime := ctx.localClock.PhysicalTime()
		response, err := ctx.heartbeat(heartbeatClient, request)
		ctx.conns.Lock()
		meta.heartbeatErr = err
		ctx.conns.Unlock()

		// If we got a timeout, we might be experiencing a network partition. We
		// close the connection so that all other pending RPCs (which may not have
		// timeouts) fail eagerly. Any other error is likely to be noticed by
		// other RPCs, so it's OK to leave the connection open while grpc
		// internally reconnects if necessary.
		if grpc.Code(err) == codes.DeadlineExceeded {
			return err
		}

		// HACK: work around https://github.com/grpc/grpc-go/issues/1026
		// Getting a "connection refused" error from the "write" system call
		// has confused grpc's error handling and this connection is permanently
		// broken.
		// TODO(bdarnell): remove this when the upstream bug is fixed.
		if err != nil && strings.Contains(err.Error(), "write: connection refused") {
			return nil
		}

		if err == nil {
			receiveTime := ctx.localClock.PhysicalTime()

			// Only update the clock offset measurement if we actually got a
			// successful response from the server.
			pingDuration := receiveTime.Sub(sendTime)
			if pingDuration > maximumPingDurationMult*ctx.localClock.MaxOffset() {
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

		heartbeatTimer.Reset(ctx.HeartbeatInterval)
	}
}

func (ctx *Context) heartbeat(
	heartbeatClient HeartbeatClient, request PingRequest,
) (*PingResponse, error) {
	goCtx, cancel := context.WithTimeout(context.TODO(), ctx.HeartbeatTimeout)
	defer cancel()
	// NB: We want the request to fail-fast (the default), otherwise we won't be
	// notified of transport failures.
	return heartbeatClient.Ping(goCtx, &request)
}
