package rpc

import (
	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/stop"
)

// Context contains the fields required by the rpc framework.
type Context struct {
	// Embed the base context.
	base.Context

	localClock   *hlc.Clock
	Stopper      *stop.Stopper
	RemoteClocks *RemoteClockMonitor
	DisableCache bool // Disable client cache when calling NewClient()
}

// NewContext creates an rpc Context with the supplied values.
func NewContext(context *base.Context, clock *hlc.Clock, stopper *stop.Stopper) *Context {
	ctx := &Context{
		Context:      *context,
		localClock:   clock,
		Stopper:      stopper,
		RemoteClocks: newRemoteClockMonitor(clock),
	}
	return ctx
}

// Copy creates a copy of the rpc Context config values, but with a
// new remote clock monitor.
func (c *Context) Copy() *Context {
	return &Context{
		Context:      c.Context,
		localClock:   c.localClock,
		Stopper:      c.Stopper,
		RemoteClocks: newRemoteClockMonitor(c.localClock),
		DisableCache: c.DisableCache,
	}
}
