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
// implied.  See the License for the specific language governing
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

	"github.com/cockroachdb/cockroach/util"
	"github.com/golang/glog"
)

const (
	defaultHeartbeatInterval = 3 * time.Second // 3s
)

var (
	clientMu          sync.Mutex         // Protects access to the client cache.
	clients           map[string]*Client // Cache of RPC clients by server address.
	heartbeatInterval time.Duration
)

// clientRetryOptions specifies exponential backoff starting
// at 1s and ending at 30s with indefinite retries.
var clientRetryOptions = util.RetryOptions{
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

	mu          sync.RWMutex // Mutex protects the fields below
	*rpc.Client              // Embedded RPC client
	addr        net.Addr     // Remote address of client
	lAddr       net.Addr     // Local address of client
	healthy     bool
	closed      bool
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
func NewClient(addr net.Addr, opts *util.RetryOptions) *Client {
	clientMu.Lock()
	if c, ok := clients[addr.String()]; ok {
		clientMu.Unlock()
		return c
	}
	c := &Client{
		addr:   addr,
		Ready:  make(chan struct{}),
		Closed: make(chan struct{}),
	}
	clients[c.Addr().String()] = c
	clientMu.Unlock()

	// Attempt to dial connection.
	retryOpts := clientRetryOptions
	if opts != nil {
		retryOpts = *opts
	}
	retryOpts.Tag = fmt.Sprintf("client %s connection", addr)

	go func() {
		err := util.RetryWithBackoff(retryOpts, func() (bool, error) {
			// TODO(spencer): use crypto.tls.
			conn, err := net.Dial(addr.Network(), addr.String())
			if err != nil {
				glog.Info(err)
				return false, nil
			}
			c.mu.Lock()
			c.Client = rpc.NewClient(conn)
			c.lAddr = conn.LocalAddr()
			c.mu.Unlock()

			// Ensure at least one heartbeat succeeds before exiting the
			// retry loop.
			if err = c.heartbeat(); err != nil {
				c.Close()
				return false, err
			}

			// Signal client is ready by closing Ready channel.
			glog.Infof("client %s connected", addr)
			close(c.Ready)

			// Launch periodic heartbeat.
			go c.startHeartbeat()

			return true, nil
		})
		if err != nil {
			glog.Errorf("client %s failed to connect", addr)
			c.Close()
		}
	}()

	return c
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
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.addr
}

// LocalAddr returns the local address of the client.
func (c *Client) LocalAddr() net.Addr {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lAddr
}

// Close removes the client from the clients map and closes
// the Closed channel.
func (c *Client) Close() {
	clientMu.Lock()
	if !c.closed {
		delete(clients, c.Addr().String())
		c.healthy = false
		c.closed = true
		close(c.Closed)
		c.Client.Close()
	}
	clientMu.Unlock()
}

// startHeartbeat sends periodic heartbeats to client. Closes the
// connection on error. Heartbeats are sent in an infinite loop until
// an error is encountered.
func (c *Client) startHeartbeat() {
	glog.Infof("client %s starting heartbeat", c.Addr())
	// On heartbeat failure, remove this client from cache. A new
	// client to this address will be created on the next call to
	// NewClient().
	for {
		time.Sleep(heartbeatInterval)
		if err := c.heartbeat(); err != nil {
			glog.Infof("client %s heartbeat failed: %v; recycling...", c.Addr(), err)
			c.Close()
			break
		}
	}
}

// heartbeat sends a single heartbeat RPC.
func (c *Client) heartbeat() error {
	call := c.Go("Heartbeat.Ping", &PingRequest{}, &PingResponse{}, nil)
	select {
	case <-call.Done:
		glog.V(1).Infof("client %s heartbeat: %v", c.Addr(), call.Error)
		c.mu.Lock()
		c.healthy = true
		c.mu.Unlock()
		return call.Error
	case <-time.After(heartbeatInterval * 2):
		// Allowed twice gossip interval.
		c.mu.Lock()
		c.healthy = false
		c.mu.Unlock()
		glog.Warningf("client %s unhealthy after %s", c.Addr(), heartbeatInterval)
	}

	<-call.Done
	return call.Error
}
