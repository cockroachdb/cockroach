package rpc

import (
	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// Context contains the fields required by the rpc framework.
type Context struct {
	// Embed the base context.
	base.Context

	localClock   *hlc.Clock
	Stopper      *util.Stopper
	RemoteClocks *RemoteClockMonitor
	DisableCache bool // Disable client cache when calling NewClient()
}

// NewContext creates an rpc Context with the supplied values.
func NewContext(context *base.Context, clock *hlc.Clock, stopper *util.Stopper) *Context {
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
