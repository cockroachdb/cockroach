package rpc

import (
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/grpcutil"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

func init() {
	grpc.EnableTracing = false
}

const (
	defaultHeartbeatInterval = 3 * time.Second
	// The coefficient by which the maximum offset is multiplied to determine the
	// maximum acceptable measurement latency.
	maximumPingDurationMult = 2
)

// NewServer is a thin wrapper around grpc.NewServer that registers a heartbeat
// service.
func NewServer(ctx *Context) *grpc.Server {
	var s *grpc.Server
	if ctx.Insecure {
		s = grpc.NewServer()
	} else {
		tlsConfig, err := ctx.GetServerTLSConfig()
		if err != nil {
			panic(err)
		}
		s = grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	RegisterHeartbeatServer(s, &HeartbeatService{
		clock:              ctx.localClock,
		remoteClockMonitor: ctx.RemoteClocks,
	})
	return s
}

// Context contains the fields required by the rpc framework.
type Context struct {
	*base.Context

	localClock   *hlc.Clock
	Stopper      *stop.Stopper
	RemoteClocks *RemoteClockMonitor

	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	HeartbeatCB       func()

	localInternalServer roachpb.InternalServer
	localAddr           string

	conns struct {
		sync.Mutex
		cache map[string]*grpc.ClientConn
	}
}

// NewContext creates an rpc Context with the supplied values.
func NewContext(baseCtx *base.Context, clock *hlc.Clock, stopper *stop.Stopper) *Context {
	ctx := &Context{
		Context: baseCtx,
	}
	if clock != nil {
		ctx.localClock = clock
	} else {
		ctx.localClock = hlc.NewClock(hlc.UnixNano)
	}
	ctx.Stopper = stopper
	ctx.RemoteClocks = newRemoteClockMonitor(clock, 10*defaultHeartbeatInterval)
	ctx.HeartbeatInterval = defaultHeartbeatInterval
	ctx.HeartbeatTimeout = 2 * defaultHeartbeatInterval

	stopper.RunWorker(func() {
		<-stopper.ShouldDrain()

		ctx.conns.Lock()
		for key, conn := range ctx.conns.cache {
			ctx.removeConn(key, conn)
		}
		ctx.conns.Unlock()
	})

	return ctx
}

// GetLocalInternalServerForAddr returns the context's internal batch server
// for addr, if it exists.
func (ctx *Context) GetLocalInternalServerForAddr(addr string) roachpb.InternalServer {
	if addr == ctx.localAddr {
		return ctx.localInternalServer
	}
	return nil
}

// SetLocalInternalServer sets the context's local internal batch server.
func (ctx *Context) SetLocalInternalServer(internalServer roachpb.InternalServer, addr string) {
	ctx.localInternalServer = internalServer
	ctx.localAddr = addr
}

func (ctx *Context) removeConn(key string, conn *grpc.ClientConn) {
	if err := conn.Close(); err != nil && !grpcutil.IsClosedConnection(err) {
		if log.V(1) {
			log.Errorf("failed to close client connection: %s", err)
		}
	}
	delete(ctx.conns.cache, key)
}

// GRPCDial calls grpc.Dial with the options appropriate for the context.
func (ctx *Context) GRPCDial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	ctx.conns.Lock()
	defer ctx.conns.Unlock()

	if conn, ok := ctx.conns.cache[target]; ok {
		return conn, nil
	}

	var dialOpt grpc.DialOption
	if ctx.Insecure {
		dialOpt = grpc.WithInsecure()
	} else {
		tlsConfig, err := ctx.GetClientTLSConfig()
		if err != nil {
			return nil, err
		}
		dialOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	dialOpts := make([]grpc.DialOption, 0, 2+len(opts))
	dialOpts = append(dialOpts, dialOpt, grpc.WithTimeout(base.NetworkTimeout))
	dialOpts = append(dialOpts, opts...)

	conn, err := grpc.Dial(target, dialOpts...)
	if err == nil {
		if ctx.conns.cache == nil {
			ctx.conns.cache = make(map[string]*grpc.ClientConn)
		}
		ctx.conns.cache[target] = conn

		ctx.Stopper.RunWorker(func() {
			if err := ctx.runHeartbeat(conn, target); err != nil && !grpcutil.IsClosedConnection(err) {
				log.Error(err)
			}
			ctx.conns.Lock()
			ctx.removeConn(target, conn)
			ctx.conns.Unlock()
		})
	}
	return conn, err
}

func (ctx *Context) runHeartbeat(cc *grpc.ClientConn, remoteAddr string) error {
	request := PingRequest{Addr: ctx.localAddr}
	heartbeatClient := NewHeartbeatClient(cc)

	var heartbeatTimer util.Timer
	defer heartbeatTimer.Stop()
	for {
		sendTime := ctx.localClock.PhysicalTime()
		response, err := ctx.heartbeat(heartbeatClient, request)
		if err != nil {
			if grpc.Code(err) == codes.DeadlineExceeded {
				continue
			}
			return err
		}
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

		// Wait after the heartbeat so that the first iteration gets a wait-free
		// heartbeat attempt.
		heartbeatTimer.Reset(ctx.HeartbeatInterval)
		select {
		case <-ctx.Stopper.ShouldStop():
			return nil
		case <-heartbeatTimer.C:
			heartbeatTimer.Read = true
		}
	}
}

func (ctx *Context) heartbeat(heartbeatClient HeartbeatClient, request PingRequest) (*PingResponse, error) {
	goCtx, cancel := context.WithTimeout(context.Background(), ctx.HeartbeatTimeout)
	defer cancel()
	return heartbeatClient.Ping(goCtx, &request)
}
