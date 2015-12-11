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
	"net"
	netrpc "net/rpc"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

// client is a client-side RPC connection to a gossip peer node.
type client struct {
	peerID                roachpb.NodeID  // Peer node ID; 0 until first gossip response
	addr                  net.Addr        // Peer node network address
	rpcClient             *rpc.Client     // RPC client
	forwardAddr           net.Addr        // Set if disconnected with an alternate addr
	sendingGossip         bool            // True if there's an outstanding RPC to send gossip
	remoteHighWaterStamps map[int32]int64 // High water timestamps for remote server; matches rpc Request/Response
	closer                chan struct{}   // Client shutdown channel
}

// newClient creates and returns a client struct.
func newClient(addr net.Addr) *client {
	return &client{
		addr: addr,
		remoteHighWaterStamps: map[int32]int64{},
		closer:                make(chan struct{}),
	}
}

// start dials the remote addr and commences gossip once connected.
// Upon exit, signals client is done by pushing it onto the done
// channel. If the client experienced an error, its err field will
// be set. This method starts client processing in a goroutine and
// returns immediately.
func (c *client) start(g *Gossip, done chan *client, context *rpc.Context, stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		var err error

		c.rpcClient = rpc.NewClient(c.addr, context)
		select {
		case <-c.rpcClient.Healthy():
			// Start gossiping and wait for disconnect or error.
			err = c.gossip(g, stopper)
			if context.DisableCache {
				c.rpcClient.Close()
			}
		case <-c.rpcClient.Closed:
			err = util.Errorf("client closed")
		}

		done <- c

		if err != nil {
			log.Infof("closing client to %s: %s", c.addr, err)
		}
	})
}

// close stops the client gossip loop and returns immediately.
func (c *client) close() {
	close(c.closer)
}

// getGossip requests the latest gossip from the remote server by
// supplying a map of this node's knowledge of other nodes' high water
// timestamps.
func (c *client) getGossip(g *Gossip, addr, lAddr util.UnresolvedAddr, done chan *netrpc.Call) {
	g.mu.Lock()
	defer g.mu.Unlock()

	nodeID := g.is.NodeID
	args := Request{
		NodeID:          nodeID,
		Addr:            addr,
		LAddr:           lAddr,
		HighWaterStamps: g.is.getHighWaterStamps(),
	}
	reply := Response{}
	c.rpcClient.Go("Gossip.Gossip", &args, &reply, done)
}

// sendGossip sends the latest gossip to the remote server, based on
// the remote server's high water timestamps map.
func (c *client) sendGossip(g *Gossip, addr, lAddr util.UnresolvedAddr, done chan *netrpc.Call) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if c.sendingGossip {
		return
	}
	nodeID := g.is.NodeID
	delta := g.is.delta(c.peerID, c.remoteHighWaterStamps)
	if len(delta) == 0 {
		return
	}

	args := Request{
		NodeID:          nodeID,
		Addr:            addr,
		LAddr:           lAddr,
		Delta:           delta,
		HighWaterStamps: g.is.getHighWaterStamps(),
	}
	reply := Response{}
	c.rpcClient.Go("Gossip.Gossip", &args, &reply, done)
	c.sendingGossip = true
}

// handleGossip handles errors, remote forwarding, and combines delta
// gossip infos from the remote server with this node's infostore.
func (c *client) handleGossip(g *Gossip, call *netrpc.Call) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if call.Error != nil {
		return call.Error
	}
	args := call.Args.(*Request)
	reply := call.Reply.(*Response)

	// Combine remote node's infostore delta with ours.
	if reply.Delta != nil {
		freshCount := g.is.combine(reply.Delta, reply.NodeID)
		if infoCount := len(reply.Delta); infoCount > 0 {
			if log.V(1) {
				log.Infof("received %s from %s (%d fresh)", reply.Delta, c.addr, freshCount)
			} else {
				log.Infof("received %d (%d fresh) info(s) from %s", infoCount, freshCount, c.addr)
			}
		}
	} else if len(args.Delta) > 0 {
		log.Infof("sent %d info(s) to %s", len(args.Delta), c.addr)
	}
	c.peerID = reply.NodeID
	g.outgoing.addNode(c.peerID)
	c.remoteHighWaterStamps = reply.HighWaterStamps

	// Handle remote forwarding.
	if reply.Alternate != nil {
		var err error
		if c.forwardAddr, err = reply.Alternate.Resolve(); err != nil {
			return util.Errorf("unable to resolve alternate address: %s: %s", reply.Alternate, err)
		}
		return util.Errorf("received forward from %s to %s", c.addr, reply.Alternate)
	}

	// If we have the sentinel gossip, we're considered connected.
	g.checkHasConnected()

	nodeID := g.is.NodeID
	// Check whether this outgoing client is duplicating work already
	// being done by an incoming client. To avoid mutual shutdown, we
	// only shutdown our client if our node ID is less than the peer's.
	if g.hasIncoming(c.peerID) && nodeID < c.peerID {
		return util.Errorf("stopping outgoing client %d @ %s; already have incoming", c.peerID, c.addr)
	}

	return nil
}

// gossip loops, sending deltas of the infostore and receiving deltas
// in turn. If an alternate is proposed on response, the client addr
// is modified and method returns for forwarding by caller.
func (c *client) gossip(g *Gossip, stopper *stop.Stopper) error {
	// For un-bootstrapped node, g.is.NodeID is 0 when client start gossip,
	// so it's better to get nodeID from g.is every time.
	g.mu.Lock()
	addr := util.MakeUnresolvedAddr(g.is.NodeAddr.Network(), g.is.NodeAddr.String())
	g.mu.Unlock()

	lAddr := util.MakeUnresolvedAddr(c.rpcClient.LocalAddr().Network(), c.rpcClient.LocalAddr().String())
	done := make(chan *netrpc.Call, 10)
	c.getGossip(g, addr, lAddr, done)

	// Register a callback for gossip updates.
	updateCallback := func(_ string, _ roachpb.Value) {
		c.sendGossip(g, addr, lAddr, done)
	}
	// Defer calling "undoer" callback returned from registration.
	defer g.RegisterCallback(".*", updateCallback)()

	// Loop until stopper is signalled, or until either the gossip or
	// RPC clients are closed. getGossip is a hanging get, returning
	// results only when the remote server has new gossip information to
	// share. sendGossip is sent to the remote server when this node has
	// new gossip information to share with the server.
	//
	// Nodes "pull" gossip in order to guarantee that they're connected
	// to the sentinel and not too distant from other nodes in the
	// network. The also "push" their own gossip which guarantees that
	// the sentinel node will contain their info, and therefore every
	// node connected to the sentinel. Just pushing or just pulling
	// wouldn't guarantee a fully connected network.
	for {
		select {
		case call := <-done:
			if err := c.handleGossip(g, call); err != nil {
				return err
			}
			req := call.Args.(*Request)
			// If this was from a gossip pull request, fetch again.
			if req.Delta == nil {
				c.getGossip(g, addr, lAddr, done)
			} else {
				// Otherwise, it's a gossip push request; set sendingGossip
				// flag false and maybe send more gossip if there have been
				// additional updates.
				g.mu.Lock()
				c.sendingGossip = false
				g.mu.Unlock()
				c.sendGossip(g, addr, lAddr, done)
			}
		case <-c.rpcClient.Closed:
			return util.Errorf("client closed")
		case <-c.closer:
			return nil
		case <-stopper.ShouldStop():
			return nil
		}
	}
}
