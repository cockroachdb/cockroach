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

package gossip

import (
	"bytes"
	"encoding/gob"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

const (
	// maxWaitForNewGossip is maximum wait for new gossip before a
	// peer is considered a poor source of good gossip and is GC'd.
	maxWaitForNewGossip = 1 * time.Minute
)

// client is a client-side RPC connection to a gossip peer node.
type client struct {
	peerID      proto.NodeID  // Peer node ID; 0 until first gossip response
	addr        net.Addr      // Peer node network address
	rpcClient   *rpc.Client   // RPC client
	forwardAddr net.Addr      // Set if disconnected with an alternate addr
	lastFresh   int64         // Last wall time client received fresh info
	err         error         // Set if client experienced an error
	closer      chan struct{} // Client shutdown channel
}

// newClient creates and returns a client struct.
func newClient(addr net.Addr) *client {
	return &client{
		addr:   addr,
		closer: make(chan struct{}),
	}
}

// start dials the remote addr and commences gossip once connected.
// Upon exit, signals client is done by pushing it onto the done
// channel. If the client experienced an error, its err field will
// be set. This method starts client processing in a goroutine and
// returns immediately.
func (c *client) start(g *Gossip, done chan *client, context *rpc.Context, stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		c.rpcClient = rpc.NewClient(c.addr, context)
		select {
		case <-c.rpcClient.Healthy():
			// Success!
		case <-c.rpcClient.Closed:
			c.err = util.Errorf("gossip client was closed")
			done <- c
			return
		}

		// Start gossipping and wait for disconnect or error.
		c.lastFresh = time.Now().UnixNano()
		c.err = c.gossip(g, stopper)
		if context.DisableCache {
			c.rpcClient.Close()
		}
		done <- c
	})
}

// close stops the client gossip loop and returns immediately.
func (c *client) close() {
	close(c.closer)
}

// gossip loops, sending deltas of the infostore and receiving deltas
// in turn. If an alternate is proposed on response, the client addr
// is modified and method returns for forwarding by caller.
func (c *client) gossip(g *Gossip, stopper *stop.Stopper) error {
	localMaxSeq := int64(0)
	remoteMaxSeq := int64(-1)
	for {
		// Compute the delta of local node's infostore to send with request.
		g.mu.Lock()
		delta := g.is.delta(c.peerID, localMaxSeq)
		nodeID := g.is.NodeID // needs to be accessed with the lock held
		g.mu.Unlock()
		var deltaBytes []byte
		if delta != nil {
			localMaxSeq = delta.MaxSeq
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(delta); err != nil {
				return util.Errorf("infostore could not be encoded: %s", err)
			}
			deltaBytes = buf.Bytes()
		}

		addr := g.is.NodeAddr
		lAddr := c.rpcClient.LocalAddr()

		// Send gossip with timeout.
		args := &Request{
			NodeID: nodeID,
			Addr:   util.MakeUnresolvedAddr(addr.Network(), addr.String()),
			LAddr:  util.MakeUnresolvedAddr(lAddr.Network(), lAddr.String()),
			MaxSeq: remoteMaxSeq,
			Delta:  deltaBytes,
		}
		reply := &Response{}
		gossipCall := c.rpcClient.Go("Gossip.Gossip", args, reply, nil)
		select {
		case <-gossipCall.Done:
			if gossipCall.Error != nil {
				return gossipCall.Error
			}
		case <-c.rpcClient.Closed:
			return util.Error("client closed")
		case <-c.closer:
			return nil
		case <-stopper.ShouldStop():
			return nil
		case <-time.After(g.interval * 10):
			return util.Errorf("timeout after: %s", g.interval*10)
		}

		// Handle remote forwarding.
		if reply.Alternate != nil {
			var err error
			if c.forwardAddr, err = reply.Alternate.Resolve(); err != nil {
				return util.Errorf("unable to resolve alternate address: %s: %s", reply.Alternate, err)
			}
			return util.Errorf("received forward from %s to %s", c.addr, reply.Alternate)
		}

		// Combine remote node's infostore delta with ours.
		now := time.Now().UnixNano()
		if reply.Delta != nil {
			delta := &infoStore{}
			if err := gob.NewDecoder(bytes.NewBuffer(reply.Delta)).Decode(delta); err != nil {
				return util.Errorf("infostore could not be decoded: %s", err)
			}
			if delta.infoCount() > 0 {
				if log.V(1) {
					log.Infof("gossip: received %s", delta)
				} else {
					log.Infof("gossip: received %d info(s) from %s", delta.infoCount(), c.addr)
				}
			}
			g.mu.Lock()
			c.peerID = delta.NodeID
			g.outgoing.addNode(c.peerID)
			freshCount := g.is.combine(delta)
			if freshCount > 0 {
				c.lastFresh = now
			}
			remoteMaxSeq = delta.MaxSeq

			// If we have the sentinel gossip, we're considered connected.
			g.checkHasConnected()
			g.mu.Unlock()
		}

		// Check whether this outgoing client is duplicating work already
		// being done by an incoming client. To avoid mutual shutdown, we
		// only shutdown our client if our node ID is less than the peer's.
		if g.hasIncoming(c.peerID) && nodeID < c.peerID {
			return util.Errorf("stopping outgoing client %d @ %s; already have incoming", c.peerID, c.addr)
		}
		// Check whether peer node is too boring--disconnect if yes.
		if nodeID != c.peerID && (now-c.lastFresh) > int64(maxWaitForNewGossip) {
			return util.Errorf("peer is too boring")
		}
	}
}
