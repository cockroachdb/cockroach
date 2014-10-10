package rpc

import "github.com/cockroachdb/cockroach/util/hlc"

// Context contains the fields required by the rpc framework.
type Context struct {
	localClock   *hlc.Clock
	tlsConfig    *TLSConfig
	remoteClocks *RemoteClockMonitor
}

// NewContext creates an rpc Context with the supplied values.
func NewContext(clock *hlc.Clock, config *TLSConfig) *Context {
	return &Context{
		localClock:   clock,
		tlsConfig:    config,
		remoteClocks: newRemoteClockMonitor(clock.MaxDrift()),
	}
}
