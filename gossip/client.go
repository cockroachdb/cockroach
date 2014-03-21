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

package gossip

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"

	"github.com/cockroachdb/cockroach/util"
)

const (
	// maxWaitForNewGossip is minimum wait for new gossip before a
	// peer is considered a poor source of good gossip and is GC'd.
	maxWaitForNewGossip = 10 * time.Second
	// gossipDialTimeout is timeout for net.Dial call to connect to
	// a gossip server.
	gossipDialTimeout = 5 * time.Second
)

// init pre-registers net.UnixAddr and net.TCPAddr concrete types with
// gob. If other implementations of net.Addr are passed, they must be
// added here as well.
func init() {
	gob.Register(&net.TCPAddr{})
	gob.Register(&net.UnixAddr{})
}

// client is a client-side RPC connection to a gossip peer node.
type client struct {
	*rpc.Client          // Embedded RPC client
	addr        net.Addr // Peer node network address
	laddr       net.Addr // Local connection address
	forwardAddr net.Addr // Set if disconnected with an alternate addr
	lastFresh   int64    // Last wall time client received fresh info
	err         error    // Set if client experienced an error
	maxAttempts int      // Maximum number of attempts to connect
}

// newClient creates and returns a client struct.
func newClient(addr net.Addr) *client {
	return &client{
		addr: addr,
	}
}

// start dials the remote addr and commences gossip once connected.
// Upon exit, signals client is done by pushing it onto the done
// channel. If the client experienced an error, its err field will
// be set. This method blocks and should be invoked via goroutine.
func (c *client) start(g *Gossip, done chan *client) {
	// Attempt to dial connection with exponential backoff, max 3
	// attempts, starting at 1s, 2s, 4s for a total of 7s.
	opts := util.Options{
		Backoff:     250 * time.Millisecond, // first backoff at 250ms
		MaxBackoff:  2 * time.Second,        // max backoff is 2s
		Constant:    2,                      // doubles
		MaxAttempts: c.maxAttempts,
	}
	util.RetryWithBackoffOptions(opts, func() bool {
		cl, err := c.dial()
		if err != nil {
			log.Print(err)
			return false
		}
		c.Client = cl
		return true
	})
	if c.Client == nil {
		c.err = fmt.Errorf("failed to dial remote server %+v", c.addr)
		done <- c
		return
	}

	// Start gossipping and wait for disconnect or error.
	c.lastFresh = time.Now().UnixNano()
	err := c.gossip(g)
	c.Close() // in all cases, close old client connection
	if err != nil {
		c.err = fmt.Errorf("gossip client: %s", err)
	}
	done <- c
}

// dial with a timeout specified by gossipDialTimeout parameter.
func (c *client) dial() (*rpc.Client, error) {
	conn, err := net.DialTimeout(c.addr.Network(), c.addr.String(), gossipDialTimeout)
	if err != nil {
		return nil, err
	}
	c.laddr = conn.LocalAddr()
	return rpc.NewClient(conn), nil
}

// gossip loops, sending deltas of the infostore and receiving deltas
// in turn. If an alternate is proposed on response, the client addr
// is modified and method returns for forwarding by caller.
func (c *client) gossip(g *Gossip) error {
	localMaxSeq := int64(0)
	remoteMaxSeq := int64(-1)
	for {
		// Do a periodic check to determine whether this outgoing client
		// is duplicating work already being done by an incoming client.
		// To avoid mutual shutdown, we only shutdown our client if our
		// server address is lexicographically less than the other.
		if g.hasIncoming(c.addr) && g.is.NodeAddr.String() < c.addr.String() {
			return fmt.Errorf("stopping outgoing client %s; already have incoming", c.addr)
		}

		// Compute the delta of local node's infostore to send with request.
		g.mu.Lock()
		delta := g.is.delta(c.addr, localMaxSeq)
		if delta != nil {
			localMaxSeq = delta.MaxSeq
		}
		g.mu.Unlock()

		// Send gossip with timeout.
		args := &GossipRequest{
			Addr:   g.is.NodeAddr,
			LAddr:  c.laddr,
			MaxSeq: remoteMaxSeq,
			Delta:  delta,
		}
		reply := new(GossipResponse)
		gossipCall := c.Go("Gossip.Gossip", args, reply, nil)
		gossipTimeout := time.After(*gossipInterval * 2) // allow twice gossip interval
		select {
		case <-gossipCall.Done:
			if gossipCall.Error != nil {
				return gossipCall.Error
			}
		case <-gossipTimeout:
			return fmt.Errorf("timeout after: %v", *gossipInterval*2)
		}

		// Handle remote forwarding.
		if reply.Alternate != nil {
			log.Printf("received forward from %+v to %+v", c.addr, reply.Alternate)
			c.forwardAddr = reply.Alternate
			return nil
		}

		// Combine remote node's infostore delta with ours.
		now := time.Now().UnixNano()
		if reply.Delta != nil {
			g.mu.Lock()
			freshCount := g.is.combine(reply.Delta)
			if freshCount > 0 {
				c.lastFresh = now
			}
			remoteMaxSeq = reply.Delta.MaxSeq
			g.mu.Unlock()
		}
		// Check whether peer node is too boring--disconnect if yes.
		if (now - c.lastFresh) > int64(maxWaitForNewGossip) {
			return fmt.Errorf("peer is too boring")
		}
	}
}
