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
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/util"
)

var (
	clientMu sync.Mutex         // Protects access to the client cache.
	clients  map[string]*Client // Cache of RPC clients by server address.
)

// init creates a new client RPC cache.
func init() {
	clients = make(map[string]*Client)
}

// Client is a Cockroach-specific RPC client with an embedded go
// rpc.Client struct.
type Client struct {
	*rpc.Client               // Embedded RPC client
	LAddr       net.Addr      // Local address of client
	Ready       chan struct{} // Closed when client is connected
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
		Ready: make(chan struct{}),
	}
	clients[addr.String()] = c
	clientMu.Unlock()

	// Attempt to dial connection with exponential backoff starting
	// at 1s and ending at 30s with indefinite retries.
	opts := util.Options{
		Backoff:     1 * time.Second,  // first backoff at 1s
		MaxBackoff:  30 * time.Second, // max backoff is 30s
		Constant:    2,                // doubles
		MaxAttempts: 0,                // indefinite retries
	}
	go util.RetryWithBackoffOptions(opts, func() bool {
		// TODO(spencer): use crypto.tls.
		conn, err := net.Dial(addr.Network(), addr.String())
		if err != nil {
			log.Print(err)
			return false
		}
		c.Client = rpc.NewClient(conn)
		c.LAddr = conn.LocalAddr()
		close(c.Ready)
		return true
	})

	return c
}
