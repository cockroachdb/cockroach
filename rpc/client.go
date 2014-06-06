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

// init creates a new client RPC cache.
func init() {
	clients = map[string]*Client{}
	heartbeatInterval = defaultHeartbeatInterval
}

// Client is a Cockroach-specific RPC client with an embedded go
// rpc.Client struct.
type Client struct {
	Ready chan struct{} // Closed when client is connected

	mu          sync.RWMutex // Mutex protects the fields below
	*rpc.Client              // Embedded RPC client
	addr        net.Addr     // Remote address of client
	lAddr       net.Addr     // Local address of client
}

// NewClient returns a client RPC stub for the specified address
// (usually a TCP host:port, but for testing may be a unix domain
// socket). The process-wide client RPC cache is consulted first; if
// the requested client is not present, it's created and the cache is
// updated. The returned client is returned immediately and may not be
// healthy.
func NewClient(addr net.Addr) *Client {
	clientMu.Lock()
	if c, ok := clients[addr.String()]; ok {
		clientMu.Unlock()
		return c
	}
	c := &Client{
		addr:  addr,
		Ready: make(chan struct{}),
	}
	clients[c.Addr().String()] = c
	clientMu.Unlock()

	// Attempt to dial connection with exponential backoff starting
	// at 1s and ending at 30s with indefinite retries.
	opts := util.Options{
		Tag:         fmt.Sprintf("client %s connection", addr),
		Backoff:     1 * time.Second,  // first backoff at 1s
		MaxBackoff:  30 * time.Second, // max backoff is 30s
		Constant:    2,                // doubles
		MaxAttempts: 0,                // indefinite retries
	}
	go util.RetryWithBackoff(opts, func() bool {
		// TODO(spencer): use crypto.tls.
		conn, err := net.Dial(addr.Network(), addr.String())
		if err != nil {
			glog.Info(err)
			return false
		}
		c.mu.Lock()
		c.Client = rpc.NewClient(conn)
		c.lAddr = conn.LocalAddr()
		c.mu.Unlock()

		// Launch heartbeat; this ensures at least one heartbeat succeeds
		// before exiting the retry loop.
		if err = c.heartbeat(); err != nil {
			glog.Info(err)
			return false
		}

		glog.Infof("client %s connected", addr)
		close(c.Ready)

		go c.startHeartbeat()

		return true
	})

	return c
}

// Close closes the client connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Client.Close()
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

// startHeartbeat sends periodic heartbeats to client. Closes the
// connection on error. Heartbeats are sent in an infinite loop until
// an error is encountered.
func (c *Client) startHeartbeat() {
	glog.Infof("client %s starting heartbeat", c.Addr())
	for {
		// On heartbeat failure, remove this client from cache. A new
		// client to this address will be created on the next call to
		// NewClient().
		if err := c.heartbeat(); err != nil {
			glog.Infof("heartbeat failed: %v; recycling client %s", err, c.Addr())
			clientMu.Lock()
			delete(clients, c.Addr().String())
			clientMu.Unlock()
			c.Close()
			break
		}
		time.Sleep(heartbeatInterval)
	}
}

// heartbeat sends a single heartbeat RPC.
func (c *Client) heartbeat() error {
	call := c.Go("Heartbeat.Ping", &PingRequest{}, &PingResponse{}, nil)
	select {
	case <-call.Done:
		glog.V(1).Infof("client %s heartbeat: %v", c.Addr(), call.Error)
		return call.Error
	case <-time.After(heartbeatInterval * 2):
		// Allowed twice gossip interval.
		// TODO(spencer): mark client unhealthy.
		glog.Warningf("client %s unhealthy after %s", c.Addr(), heartbeatInterval*2)
	}

	<-call.Done
	return call.Error
}
