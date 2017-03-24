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
	defaultKeepaliveInterval = 3 * time.Second

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
	if !ctx.cfg.Insecure {
		tlsConfig, err := ctx.cfg.GetServerTLSConfig()
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

// ContextTestingKnobs are the testing knobs for a Context.
type ContextTestingKnobs struct {
	BreakerFactory func() *circuit.Breaker
	// PeriodicClockCheckHook is used to override the default clock offset check
	// on connections. If an error is returned, the node will crash itself.
	PeriodicClockCheckHook func() error
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*ContextTestingKnobs) ModuleTestingKnobs() {
}

var _ base.ModuleTestingKnobs = &ContextTestingKnobs{}

// ContextConfig contains parameters for creating a Context.
type ContextConfig struct {
	// The base.Config used to open gRPC connections.
	*base.Config

	// KeepaliveInterval is the interval at which gRPC is going to perform HTTP2
	// pings to check that the connection is still alive.
	KeepaliveInterval time.Duration
	// KeepaliveTimeout is the timeout for receiving a response to a ping. If no
	// response is received within timeout, the connection is closed and in-flight
	// RPCs will receive an error. gRPC will attempt to reconnect.
	KeepaliveTimeout time.Duration

	// HeartbeatInterval is the period on which the health status of a connection
	// is tested. A Heartbeat RPC is sent periodically and, if it fails or times
	// out, the remote host is considered to be unhealthy (see
	// ConnHealth(remoteAddr)).
	// Set to 0 to disable.
	//
	// Heartbeats are different from the gRPC pings configured through
	// KeepaliveInterval. Those fail in-flight RPCs and cause gRPC to reconnect,
	// but otherwise their failure is not exposed to the application.
	//
	// Heartbeats can also be piggy-backed by clock skew checks; see
	// EnableClockSkewCheck.
	HeartbeatInterval time.Duration
	// HeartbeatTimeout specifies the timeout of the Heartbeat RPCs. If a
	// heartbeat times out, the remote node is considered to be unhealthy.
	HeartbeatTimeout time.Duration

	// EnableClockSkewChecks, if set, makes the Heartbeat RPCs also perform clock
	// skew checks. If an unacceptable clock skew is detected, the node kills
	// itself.
	// If set, HeartbeatInterval must also be set.
	EnableClockSkewChecks bool

	// HLCClock is the clock used for reporting the the node's clock with
	// heartbeats, and also for the circuit breaker.
	HLCClock *hlc.Clock

	TestingKnobs ContextTestingKnobs
}

// Context allows clients to open gRPC connections to remote hosts. It also
// optionally performs health checks on those connections and checks for clock
// skew between nodes.
type Context struct {
	cfg ContextConfig

	LocalClock   *hlc.Clock
	breakerClock breakerClock
	Stopper      *stop.Stopper
	// RemoteClocks is used to perform periodic clock offset checks on all
	// connections.
	RemoteClocks *RemoteClockMonitor
	masterCtx    context.Context

	localInternalServer roachpb.InternalServer

	conns struct {
		syncutil.Mutex
		cache map[string]*connMeta
	}
}

// NewContext creates an rpc Context with the supplied values.
func NewContext(ambient log.AmbientContext, cfg ContextConfig, stopper *stop.Stopper) *Context {
	if cfg.HLCClock == nil {
		panic("nil clock is forbidden")
	}
	ctx := &Context{
		cfg:        cfg,
		LocalClock: cfg.HLCClock,
		breakerClock: breakerClock{
			clock: cfg.HLCClock,
		},
	}
	var cancel context.CancelFunc
	ctx.masterCtx, cancel = context.WithCancel(ambient.AnnotateCtx(context.Background()))
	ctx.Stopper = stopper
	if ctx.cfg.EnableClockSkewChecks {
		if ctx.cfg.HeartbeatInterval == 0 {
			log.Fatal(ctx.masterCtx, "specified EnableClockSkewChecks but not HeartbeatInterval")
		}
		ctx.RemoteClocks = newRemoteClockMonitor(
			ctx.LocalClock, 10*ctx.cfg.HeartbeatInterval /* offsetTTL */, ctx.cfg.HistogramWindowInterval)
	}
	if ctx.cfg.KeepaliveInterval == 0 {
		ctx.cfg.KeepaliveInterval = defaultKeepaliveInterval
	}
	if ctx.cfg.KeepaliveTimeout == 0 {
		ctx.cfg.KeepaliveTimeout = 2 * defaultKeepaliveInterval
	}
	if ctx.cfg.HeartbeatInterval != 0 {
		if ctx.cfg.HeartbeatTimeout == 0 {
			log.Fatal(ctx.masterCtx, "invalid HeartbeatTimeout: 0")
		}
	}
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
	if addr == ctx.cfg.Addr {
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
		if ctx.cfg.Insecure {
			dialOpt = grpc.WithInsecure()
		} else {
			tlsConfig, err := ctx.cfg.GetClientTLSConfig()
			if err != nil {
				meta.dialErr = err
				return
			}
			dialOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
		}

		dialOpts := make([]grpc.DialOption, 0, 2+len(opts))
		dialOpts = append(dialOpts, dialOpt)
		dialOpts = append(dialOpts, grpc.WithBackoffMaxDelay(maxBackoff))
		dialOpts = append(dialOpts, grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				// Send periodic pings on the connection.
				Time: ctx.cfg.KeepaliveInterval,
				// If the pings don't get a response within the timeout, the connection
				// will be closed: if we got a timeout, we might be experiencing a
				// network partition. All the pending RPCs (which may not have timeouts)
				// will fail eagerly.
				Timeout: ctx.cfg.KeepaliveTimeout,
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
		if meta.dialErr == nil && ctx.cfg.HeartbeatInterval != 0 {
			if err := ctx.Stopper.RunTask(func() {
				ctx.Stopper.RunWorker(func() {
					ctx.runPeriodicHeartbeats(meta, target)
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
	if ctx.cfg.TestingKnobs.BreakerFactory != nil {
		return ctx.cfg.TestingKnobs.BreakerFactory()
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

func (ctx *Context) runPeriodicHeartbeats(meta *connMeta, remoteAddr string) {
	if ctx.cfg.HeartbeatInterval == 0 {
		log.Fatal(ctx.masterCtx, "invalid HeartbeatInterval: 0")
	}
	maxOffset := ctx.LocalClock.MaxOffset()

	request := PingRequest{
		Addr:           ctx.cfg.Addr,
		MaxOffsetNanos: maxOffset.Nanoseconds(),
	}
	heartbeatClient := NewHeartbeatClient(meta.conn)

	var clockCheckTimer timeutil.Timer
	defer clockCheckTimer.Stop()

	// Give the first iteration a wait-free heartbeat attempt.
	clockCheckTimer.Reset(0)
	for {
		select {
		case <-ctx.Stopper.ShouldStop():
			return
		case <-clockCheckTimer.C:
			clockCheckTimer.Read = true
		}

		goCtx, _ := context.WithTimeout(ctx.masterCtx, ctx.cfg.HeartbeatTimeout)
		sendTime := ctx.LocalClock.PhysicalTime()
		response, err := heartbeatClient.Ping(goCtx, &request)
		ctx.conns.Lock()
		// Consider the remote node unhealthy in case of an error.
		meta.heartbeatErr = err
		ctx.conns.Unlock()

		// HACK: work around https://github.com/grpc/grpc-go/issues/1026
		// Getting a "connection refused" error from the "write" system call
		// has confused grpc's error handling and this connection is permanently
		// broken.
		// TODO(bdarnell): remove this when the upstream bug is fixed.
		if err != nil && strings.Contains(err.Error(), "write: connection refused") {
			return
		}

		if err == nil && ctx.cfg.EnableClockSkewChecks {
			receiveTime := ctx.LocalClock.PhysicalTime()

			// Only update the clock offset measurement if we actually got a
			// successful response from the server and the measurement didn't take too
			// long.
			pingDuration := receiveTime.Sub(sendTime)
			if pingDuration > maximumPingDurationMult*ctx.LocalClock.MaxOffset() {
				// The empty offset will not be recorded.
				request.Offset.Reset()
			} else {
				// Offset and error are measured using the remote clock reading technique
				// described in http://se.inf.tu-dresden.de/pubs/papers/SRDS1994.pdf, page
				// 6. However, we assume that drift and min message delay are 0, for now.
				request.Offset.MeasuredAt = receiveTime.UnixNano()
				request.Offset.Uncertainty = (pingDuration / 2).Nanoseconds()
				remoteTimeNow := time.Unix(0, response.ServerTime).Add(pingDuration / 2)
				request.Offset.Offset = remoteTimeNow.Sub(receiveTime).Nanoseconds()
			}
			ctx.RemoteClocks.UpdateOffset(ctx.masterCtx, remoteAddr, request.Offset, pingDuration)

			if cb := ctx.cfg.TestingKnobs.PeriodicClockCheckHook; cb != nil {
				if err := cb(); err != nil {
					log.Fatal(ctx.masterCtx, err)
				}
			} else {
				if err := ctx.RemoteClocks.VerifyClockOffset(ctx.masterCtx); err != nil {
					log.Fatal(ctx.masterCtx, err)
				}
			}
		}
		clockCheckTimer.Reset(ctx.cfg.HeartbeatInterval)
	}
}
