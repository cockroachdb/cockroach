package rpc

import (
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
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

	// Affects maximum error in reading the clock of the remote. 1.5 seconds is
	// the longest NTP allows for a remote clock reading. After 1.5 seconds, we
	// assume that the offset from the clock is infinite.
	maximumClockReadingDelay = 1500 * time.Millisecond
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
	// Embed the base context.
	base.Context

	localClock   *hlc.Clock
	Stopper      *stop.Stopper
	RemoteClocks *RemoteClockMonitor

	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration

	LocalInternalServer roachpb.InternalServer
	LocalAddr           string

	conns struct {
		sync.Mutex
		cache map[string]*grpc.ClientConn
	}
}

// NewContext creates an rpc Context with the supplied values.
func NewContext(baseCtx *base.Context, clock *hlc.Clock, stopper *stop.Stopper) *Context {
	var ctx *Context
	if baseCtx != nil {
		// TODO(tamird): This form fools `go vet`; `baseCtx` contains several
		// `sync.Mutex`s, and this deference copies them, which is bad. The problem
		// predates this comment, so I'm kicking the can down the road for now, but
		// we should fix this.
		ctx = &Context{
			Context: *baseCtx,
		}
	} else {
		ctx = new(Context)
	}
	if clock != nil {
		ctx.localClock = clock
	} else {
		ctx.localClock = hlc.NewClock(hlc.UnixNano)
	}
	ctx.Stopper = stopper
	ctx.RemoteClocks = newRemoteClockMonitor(clock)
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

// SetLocalInternalServer sets the context's local internal batch server.
func (ctx *Context) SetLocalInternalServer(internalServer roachpb.InternalServer, addr string) {
	ctx.LocalInternalServer = internalServer
	ctx.LocalAddr = addr
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

	conn, err := grpc.Dial(target, append(opts, dialOpt, grpc.WithTimeout(base.NetworkTimeout))...)
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
	var request PingRequest
	heartbeatClient := NewHeartbeatClient(cc)

	var heartbeatTimer util.Timer
	defer heartbeatTimer.Stop()
	for {
		sendTime := ctx.localClock.PhysicalNow()
		goCtx, cancel := context.WithTimeout(context.Background(), ctx.HeartbeatTimeout)
		response, err := heartbeatClient.Ping(goCtx, &request)
		if err != nil {
			cancel()
			return err
		}
		receiveTime := ctx.localClock.PhysicalNow()

		// Only update the clock offset measurement if we actually got a
		// successful response from the server.
		if receiveTime > sendTime+maximumClockReadingDelay.Nanoseconds() {
			request.Offset.Reset()
		} else {
			// Offset and error are measured using the remote clock reading
			// technique described in
			// http://se.inf.tu-dresden.de/pubs/papers/SRDS1994.pdf, page 6.
			// However, we assume that drift and min message delay are 0, for
			// now.
			request.Offset.MeasuredAt = receiveTime
			request.Offset.Uncertainty = (receiveTime - sendTime) / 2
			remoteTimeNow := response.ServerTime + request.Offset.Uncertainty
			request.Offset.Offset = remoteTimeNow - receiveTime
			ctx.RemoteClocks.UpdateOffset(remoteAddr, request.Offset)
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
