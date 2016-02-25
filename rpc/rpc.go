package rpc

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/stop"
)

// Context contains the fields required by the rpc framework.
type Context struct {
	// Embed the base context.
	base.Context

	localClock        *hlc.Clock
	Stopper           *stop.Stopper
	RemoteClocks      *RemoteClockMonitor
	DisableCache      bool // Disable client cache when calling NewClient()
	DisableReconnects bool // Disable client reconnects
	HealthWait        time.Duration

	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration

	LocalServer *Server // Holds the local RPC server handle
	LocalAddr   string
}

// NewContext creates an rpc Context with the supplied values.
func NewContext(context *base.Context, clock *hlc.Clock, stopper *stop.Stopper) *Context {
	ctx := &Context{
		Context:      *context,
		localClock:   clock,
		Stopper:      stopper,
		RemoteClocks: newRemoteClockMonitor(clock),
		HealthWait:   5 * time.Second,
	}
	return ctx
}

// GRPCDial calls grpc.Dial with the options appropriate for the context.
func (c *Context) GRPCDial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	var dialOpt grpc.DialOption
	if c.Insecure {
		dialOpt = grpc.WithInsecure()
	} else {
		tlsConfig, err := c.GetClientTLSConfig()
		if err != nil {
			return nil, err
		}
		dialOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	return grpc.Dial(target, append(opts, dialOpt, grpc.WithTimeout(base.NetworkTimeout))...)
}

// Copy creates a copy of the rpc Context config values, but with a
// new remote clock monitor.
func (c *Context) Copy() *Context {
	copy := *c
	copy.RemoteClocks = newRemoteClockMonitor(c.localClock)
	return &copy
}

// SetLocalServer sets the local RPC server handle to the context.
func (c *Context) SetLocalServer(s *Server, addr string) {
	c.LocalServer = s
	c.LocalAddr = addr
}
