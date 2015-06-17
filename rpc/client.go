// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package rpc

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc/codec"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
)

const (
	defaultHeartbeatInterval = 3 * time.Second // 3s

	// Affects maximum error in reading the clock of the remote. 1.5 seconds is
	// the longest NTP allows for a remote clock reading. After 1.5 seconds, we
	// assume that the offset from the clock is infinite.
	maximumClockReadingDelay = 1500 * time.Millisecond
)

var (
	clientMu          sync.Mutex         // Protects access to the client cache.
	clients           map[string]*Client // Cache of RPC clients by server address.
	heartbeatInterval time.Duration
)

// clientRetryOptions specifies exponential backoff starting
// at 1s and ending at 30s with indefinite retries.
var clientRetryOptions = retry.Options{
	Backoff:     1 * time.Second,  // first backoff at 1s
	MaxBackoff:  30 * time.Second, // max backoff is 30s
	Constant:    2,                // doubles
	MaxAttempts: 0,                // indefinite retries
}

// init creates a new client RPC cache.
func init() {
	clients = map[string]*Client{}
	heartbeatInterval = defaultHeartbeatInterval
}

// Client is a Cockroach-specific RPC client with an embedded go
// rpc.Client struct.
type Client struct {
	Ready  chan struct{} // Closed when client is connected
	Closed chan struct{} // Closed when connection has closed
	closed bool          // True when connection has closed. Protected by clientMu.

	mu           sync.Mutex // Mutex protects the fields below
	*rpc.Client             // Embedded RPC client
	addr         net.Addr   // Remote address of client
	lAddr        net.Addr   // Local address of client
	healthy      bool
	offset       proto.RemoteOffset // Latest measured clock offset from the server
	clock        *hlc.Clock
	remoteClocks *RemoteClockMonitor
	cached       bool
}

// NewClient returns a client RPC stub for the specified address
// (usually a TCP host:port, but for testing may be a unix domain
// socket). The process-wide client RPC cache is consulted first; if
// the requested client is not present, it's created and the cache is
// updated. Specify opts to fine tune client connection behavior or
// nil to use defaults (i.e. indefinite retries with exponential
// backoff).
//
// The Client.Ready channel is closed after the client has connected
// and completed one successful heartbeat. The Closed channel is
// closed if the client fails to connect or if the client's Close()
// method is invoked.
func NewClient(addr net.Addr, opts *retry.Options, context *Context) *Client {
	clientMu.Lock()
	if !context.DisableCache {
		if c, ok := clients[addr.String()]; ok {
			clientMu.Unlock()
			return c
		}
	}
	c := &Client{
		addr:         addr,
		Ready:        make(chan struct{}),
		Closed:       make(chan struct{}),
		clock:        context.localClock,
		remoteClocks: context.RemoteClocks,
		cached:       !context.DisableCache,
	}
	if !context.DisableCache {
		clients[c.Addr().String()] = c
	}
	clientMu.Unlock()

	context.Stopper.RunWorker(func() {
		c.connect(opts, context)
	})
	return c
}

// connect dials the connection in a backoff/retry loop.
func (c *Client) connect(opts *retry.Options, context *Context) {
	// Attempt to dial connection.
	retryOpts := clientRetryOptions
	if opts != nil {
		retryOpts = *opts
	}
	retryOpts.Tag = fmt.Sprintf("client %s connection", c.addr)
	retryOpts.Stopper = context.Stopper

	if err := retry.WithBackoff(retryOpts, func() (retry.Status, error) {
		tlsConfig, err := context.GetClientTLSConfig()
		if err != nil {
			// Problem loading the TLS config. Retrying will not help.
			return retry.Break, err
		}

		conn, err := tlsDialHTTP(c.addr.Network(), c.addr.String(), tlsConfig)
		if err != nil {
			// Retry if the error is temporary, otherwise fail fast.
			if t, ok := err.(net.Error); ok && t.Temporary() {
				return retry.Continue, err
			}
			return retry.Break, err
		}

		c.mu.Lock()
		c.Client = rpc.NewClientWithCodec(codec.NewClientCodec(conn))
		c.lAddr = conn.LocalAddr()
		c.mu.Unlock()

		// Ensure at least one heartbeat succeeds before exiting the
		// retry loop. If it fails, don't retry: The node is probably
		// dead.
		if err = c.heartbeat(); err != nil {
			return retry.Break, err
		}

		// Signal client is ready by closing Ready channel.
		log.Infof("client %s connected", c.addr)
		close(c.Ready)

		return retry.Break, nil
	}); err != nil {
		log.Errorf("client %s failed to connect: %v", c.addr, err)
		c.Close()
	}

	// Launch periodic heartbeat. This blocks until the client is to be closed.
	c.runHeartbeat(context.Stopper)
	c.Close()
}

// IsConnected returns whether the client is connected.
func (c *Client) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Client != nil
}

// IsHealthy returns whether the client is healthy.
func (c *Client) IsHealthy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.healthy
}

// Addr returns remote address of the client.
func (c *Client) Addr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.addr
}

// LocalAddr returns the local address of the client.
func (c *Client) LocalAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lAddr
}

// RemoteOffset returns the most recently measured offset of the client clock
// from the remote server clock.
func (c *Client) RemoteOffset() proto.RemoteOffset {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offset
}

// Close removes the client from the clients map and closes
// the Closed channel.
func (c *Client) Close() {
	clientMu.Lock()
	if !c.closed {
		if c.cached {
			delete(clients, c.Addr().String())
		}
		c.mu.Lock()
		c.healthy = false
		c.closed = true
		if c.Client != nil {
			c.Client.Close()
		}
		c.mu.Unlock()
		close(c.Closed)
	}
	clientMu.Unlock()
}

// runHeartbeat sends periodic heartbeats to client. Closes the
// connection on error. Heartbeats are sent in an infinite loop until
// an error is encountered.
func (c *Client) runHeartbeat(stopper *util.Stopper) {
	if log.V(2) {
		log.Infof("client %s starting heartbeat", c.Addr())
	}

	for {
		select {
		case <-stopper.ShouldStop():
			return
		case <-time.After(heartbeatInterval):
			if err := c.heartbeat(); err != nil {
				log.Infof("client %s heartbeat failed: %v; recycling...", c.Addr(), err)
				return
			}
		}
	}
}

// heartbeat sends a single heartbeat RPC. As part of the heartbeat protocol,
// it measures the clock of the remote to determine the node's clock offset
// from the remote.
func (c *Client) heartbeat() error {
	request := &proto.PingRequest{Offset: c.RemoteOffset(), Addr: c.LocalAddr().String()}
	response := &proto.PingResponse{}
	sendTime := c.clock.PhysicalNow()
	call := c.Go("Heartbeat.Ping", request, response, nil)
	select {
	case <-call.Done:
		receiveTime := c.clock.PhysicalNow()
		if log.V(2) {
			log.Infof("client %s heartbeat: %v", c.Addr(), call.Error)
		}
		if call.Error == nil {
			// Only update the clock offset measurement if we actually got a
			// successful response from the server.
			c.mu.Lock()
			c.healthy = true
			if receiveTime-sendTime > maximumClockReadingDelay.Nanoseconds() {
				c.offset.Reset()
			} else {
				// Offset and error are measured using the remote clock reading
				// technique described in
				// http://se.inf.tu-dresden.de/pubs/papers/SRDS1994.pdf, page 6.
				// However, we assume that drift and min message delay are 0, for
				// now.
				c.offset.MeasuredAt = receiveTime
				c.offset.Uncertainty = (receiveTime - sendTime) / 2
				remoteTimeNow := response.ServerTime + (receiveTime-sendTime)/2
				c.offset.Offset = remoteTimeNow - receiveTime
			}
			offset := c.offset
			c.mu.Unlock()
			if offset.MeasuredAt != 0 {
				c.remoteClocks.UpdateOffset(c.addr.String(), offset)
			}
		}
		return call.Error
	case <-time.After(heartbeatInterval * 2):
		// Allowed twice heartbeat interval.
		c.mu.Lock()
		c.healthy = false
		c.offset.Reset()
		c.mu.Unlock()
		log.Warningf("client %s unhealthy after %s", c.Addr(), heartbeatInterval)
		return util.Errorf("client timeout")
	case <-c.Closed:
		return util.Errorf("client is closed")
	}
}
