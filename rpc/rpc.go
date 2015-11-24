package rpc

import (
	"time"

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

	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration

	localServer *Server // Holds the local RPC server handle
	localAddr   string
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

// Copy creates a copy of the rpc Context config values, but with a
// new remote clock monitor.
func (c *Context) Copy() *Context {
	copy := *c
	copy.RemoteClocks = newRemoteClockMonitor(c.localClock)
	return &copy
}

// SetLocalServer sets the local RPC server handle to the context.
func (c *Context) SetLocalServer(s *Server, addr string) {
	c.localServer = s
	c.localAddr = addr
}
