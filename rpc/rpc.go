package rpc

import (
	"crypto/tls"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// Context contains the fields required by the rpc framework.
type Context struct {
	localClock   *hlc.Clock
	tlsConfig    *tls.Config
	Stopper      *util.Stopper
	RemoteClocks *RemoteClockMonitor
	DisableCache bool // Disable client cache when calling NewClient()
}

// NewContext creates an rpc Context with the supplied values.
func NewContext(clock *hlc.Clock, config *tls.Config, stopper *util.Stopper) *Context {
	return &Context{
		localClock:   clock,
		tlsConfig:    config,
		Stopper:      stopper,
		RemoteClocks: newRemoteClockMonitor(clock),
	}
}

// Copy creates a copy of the rpc Context config values, but with a
// new remote clock monitor.
func (c *Context) Copy() *Context {
	return &Context{
		localClock:   c.localClock,
		tlsConfig:    c.tlsConfig,
		Stopper:      c.Stopper,
		RemoteClocks: newRemoteClockMonitor(c.localClock),
		DisableCache: c.DisableCache,
	}
}
