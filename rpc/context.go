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
	"math"
	"time"

	"github.com/rubyist/circuitbreaker"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/grpcutil"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

const (
	defaultHeartbeatInterval = 3 * time.Second
	// The coefficient by which the maximum offset is multiplied to determine the
	// maximum acceptable measurement latency.
	maximumPingDurationMult = 2
)

// NewServer is a thin wrapper around grpc.NewServer that registers a heartbeat
// service.
func NewServer(ctx *Context) *grpc.Server {
	opts := []grpc.ServerOption{
		grpc.MaxMsgSize(math.MaxInt32), // TODO(peter,tamird): need tests before lowering
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
	conn    *grpc.ClientConn
	healthy bool
}

// Context contains the fields required by the rpc framework.
type Context struct {
	*base.Context

	localClock   *hlc.Clock
	breakerClock breakerClock
	Stopper      *stop.Stopper
	RemoteClocks *RemoteClockMonitor

	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	HeartbeatCB       func()

	localInternalServer roachpb.InternalServer

	conns struct {
		syncutil.Mutex
		cache map[string]connMeta
	}
}

// NewContext creates an rpc Context with the supplied values.
func NewContext(baseCtx *base.Context, hlcClock *hlc.Clock, stopper *stop.Stopper) *Context {
	ctx := &Context{
		Context: baseCtx,
	}
	if hlcClock != nil {
		ctx.localClock = hlcClock
	} else {
		ctx.localClock = hlc.NewClock(hlc.UnixNano)
	}
	ctx.breakerClock = breakerClock{
		clock: ctx.localClock,
	}
	ctx.Stopper = stopper
	ctx.RemoteClocks = newRemoteClockMonitor(ctx.localClock, 10*defaultHeartbeatInterval)
	ctx.HeartbeatInterval = defaultHeartbeatInterval
	ctx.HeartbeatTimeout = 2 * defaultHeartbeatInterval
	ctx.conns.cache = make(map[string]connMeta)

	stopper.RunWorker(func() {
		<-stopper.ShouldQuiesce()

		ctx.conns.Lock()
		for key, meta := range ctx.conns.cache {
			ctx.removeConn(key, meta.conn)
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

func (ctx *Context) removeConn(key string, conn *grpc.ClientConn) {
	if log.V(1) {
		log.Infof(context.TODO(), "closing %s", key)
	}
	if err := conn.Close(); err != nil && !grpcutil.IsClosedConnection(err) {
		if log.V(1) {
			log.Errorf(context.TODO(), "failed to close client connection: %s", err)
		}
	}
	delete(ctx.conns.cache, key)
}

// GRPCDial calls grpc.Dial with the options appropriate for the context.
func (ctx *Context) GRPCDial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	ctx.conns.Lock()
	defer ctx.conns.Unlock()

	if meta, ok := ctx.conns.cache[target]; ok {
		return meta.conn, nil
	}

	dialOpt, err := ctx.GRPCDialOption()
	if err != nil {
		return nil, err
	}

	dialOpts := make([]grpc.DialOption, 0, 1+len(opts))
	dialOpts = append(dialOpts, dialOpt)
	dialOpts = append(dialOpts, opts...)

	if log.V(1) {
		log.Infof(context.TODO(), "dialing %s", target)
	}
	conn, err := grpc.Dial(target, dialOpts...)
	if err == nil {
		ctx.conns.cache[target] = connMeta{conn: conn}

		if ctx.Stopper.RunTask(func() {
			ctx.Stopper.RunWorker(func() {
				err := ctx.runHeartbeat(conn, target)
				if err != nil && !grpcutil.IsClosedConnection(err) {
					log.Error(context.TODO(), err)
				}
				ctx.conns.Lock()
				ctx.removeConn(target, conn)
				ctx.conns.Unlock()
			})
		}) != nil {
			ctx.removeConn(target, conn)
		}
	}

	return conn, err
}

// GRPCDialOption returns the GRPC dialing option appropriate for the context.
func (ctx *Context) GRPCDialOption() (grpc.DialOption, error) {
	var dialOpt grpc.DialOption
	if ctx.Insecure {
		dialOpt = grpc.WithInsecure()
	} else {
		tlsConfig, err := ctx.GetClientTLSConfig()
		if err != nil {
			return dialOpt, err
		}
		dialOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}
	return dialOpt, nil
}

// NewBreaker creates a new circuit breaker properly configured for RPC
// connections.
func (ctx *Context) NewBreaker() *circuit.Breaker {
	return newBreaker(&ctx.breakerClock)
}

// setConnHealthy sets the health status of the connection.
func (ctx *Context) setConnHealthy(remoteAddr string, healthy bool) {
	ctx.conns.Lock()
	defer ctx.conns.Unlock()

	meta, ok := ctx.conns.cache[remoteAddr]
	if ok {
		meta.healthy = healthy
		ctx.conns.cache[remoteAddr] = meta
	}
}

// IsConnHealthy returns whether the most recent heartbeat succeeded or not.
// This should not be used as a definite status of a nodes health and just used
// to prioritized healthy nodes over unhealthy ones.
func (ctx *Context) IsConnHealthy(remoteAddr string) bool {
	ctx.conns.Lock()
	defer ctx.conns.Unlock()

	return ctx.conns.cache[remoteAddr].healthy
}

func (ctx *Context) runHeartbeat(cc *grpc.ClientConn, remoteAddr string) error {
	request := PingRequest{Addr: ctx.Addr}
	heartbeatClient := NewHeartbeatClient(cc)
	var heartbeatTimer timeutil.Timer
	defer heartbeatTimer.Stop()

	// Give the first iteration a wait-free heartbeat attempt.
	nextHeartbeat := 0 * time.Nanosecond
	for {
		heartbeatTimer.Reset(nextHeartbeat)
		select {
		case <-ctx.Stopper.ShouldStop():
			return nil
		case <-heartbeatTimer.C:
			heartbeatTimer.Read = true
		}

		sendTime := ctx.localClock.PhysicalTime()
		response, err := ctx.heartbeat(heartbeatClient, request)
		ctx.setConnHealthy(remoteAddr, err == nil)
		if err == nil {
			receiveTime := ctx.localClock.PhysicalTime()

			// Only update the clock offset measurement if we actually got a
			// successful response from the server.
			if pingDuration := receiveTime.Sub(sendTime); pingDuration > maximumPingDurationMult*ctx.localClock.MaxOffset() {
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
			ctx.RemoteClocks.UpdateOffset(remoteAddr, request.Offset)

			if cb := ctx.HeartbeatCB; cb != nil {
				cb()
			}
		}

		// If the heartbeat timed out, run the next one immediately. Otherwise,
		// wait out the heartbeat interval on the next iteration.
		if grpc.Code(err) == codes.DeadlineExceeded {
			nextHeartbeat = 0
		} else {
			nextHeartbeat = ctx.HeartbeatInterval
		}
	}
}

func (ctx *Context) heartbeat(heartbeatClient HeartbeatClient, request PingRequest) (*PingResponse, error) {
	goCtx, cancel := context.WithTimeout(context.Background(), ctx.HeartbeatTimeout)
	defer cancel()
	// NB: We want the request to fail-fast (the default), otherwise we won't be
	// notified of transport failures.
	return heartbeatClient.Ping(goCtx, &request)
}
