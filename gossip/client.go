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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"fmt"
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/grpcutil"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

// client is a client-side RPC connection to a gossip peer node.
type client struct {
	peerID      roachpb.NodeID  // Peer node ID; 0 until first gossip response
	addr        net.Addr        // Peer node network address
	rpcClient   GossipClient    // RPC client
	forwardAddr net.Addr        // Set if disconnected with an alternate addr
	remoteNodes map[int32]*Node // Remote server's high water timestamps and min hops
	closer      chan struct{}   // Client shutdown channel
}

// extractKeys returns a string representation of a gossip delta's keys.
func extractKeys(delta map[string]*Info) string {
	keys := []string{}
	for key := range delta {
		keys = append(keys, key)
	}
	return fmt.Sprintf("%s", keys)
}

// newClient creates and returns a client struct.
func newClient(addr net.Addr) *client {
	return &client{
		addr:        addr,
		remoteNodes: map[int32]*Node{},
		closer:      make(chan struct{}),
	}
}

// start dials the remote addr and commences gossip once connected.
// Upon exit, the client is sent on the disconnected channel.
// If the client experienced an error, its err field will
// be set. This method starts client processing in a goroutine and
// returns immediately.
func (c *client) start(g *Gossip, disconnected chan *client, ctx *rpc.Context, stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		defer func() {
			disconnected <- c
		}()

		var dialOpt grpc.DialOption
		if ctx.Insecure {
			dialOpt = grpc.WithInsecure()
		} else {
			tlsConfig, err := ctx.GetClientTLSConfig()
			if err != nil {
				log.Error(err)
				return
			}
			dialOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
		}

		conn, err := grpc.Dial(c.addr.String(), dialOpt)
		if err != nil {
			log.Errorf("failed to dial: %v", err)
			return
		}
		defer func() {
			if err := conn.Close(); err != nil {
				log.Error(err)
			}
		}()
		c.rpcClient = NewGossipClient(conn)

		// Start gossiping.
		if err := c.gossip(g, stopper); err != nil {
			if !grpcutil.IsClosedConnection(err) {
				if c.peerID != 0 {
					log.Infof("closing client to node %d (%s): %s", c.peerID, c.addr, err)
				} else {
					log.Infof("closing client to %s: %s", c.addr, err)
				}
			}
		}
	})
}

// close stops the client gossip loop and returns immediately.
func (c *client) close() {
	close(c.closer)
}

// requestGossip requests the latest gossip from the remote server by
// supplying a map of this node's knowledge of other nodes' high water
// timestamps and min hops.
func (c *client) requestGossip(g *Gossip, addr util.UnresolvedAddr, stream Gossip_GossipClient) error {
	g.mu.Lock()
	args := &Request{
		NodeID: g.is.NodeID,
		Addr:   addr,
		Nodes:  g.is.getNodes(),
	}
	g.mu.Unlock()

	return stream.Send(args)
}

// sendGossip sends the latest gossip to the remote server, based on
// the remote server's notion of other nodes' high water timestamps
// and min hops.
func (c *client) sendGossip(g *Gossip, addr util.UnresolvedAddr, stream Gossip_GossipClient) error {
	g.mu.Lock()
	args := &Request{
		NodeID: g.is.NodeID,
		Addr:   addr,
		Delta:  g.is.delta(c.remoteNodes),
		Nodes:  g.is.getNodes(),
	}
	g.mu.Unlock()

	if len(args.Delta) == 0 {
		return nil
	}

	return stream.Send(args)
}

// handleResponse handles errors, remote forwarding, and combines delta
// gossip infos from the remote server with this node's infostore.
func (c *client) handleResponse(g *Gossip, reply *Response) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Combine remote node's infostore delta with ours.
	if reply.Delta != nil {
		freshCount, err := g.is.combine(reply.Delta, reply.NodeID)
		if err != nil {
			log.Warningf("node %d failed to fully combine delta from node %d: %s", g.is.NodeID, reply.NodeID, err)
		}
		if infoCount := len(reply.Delta); infoCount > 0 {
			if log.V(1) {
				log.Infof("node %d received %s from node %d (%d fresh)", g.is.NodeID, extractKeys(reply.Delta), reply.NodeID, freshCount)
			}
		}
		g.maybeTighten()
	}
	c.peerID = reply.NodeID
	g.outgoing.addNode(c.peerID)
	c.remoteNodes = reply.Nodes

	// Handle remote forwarding.
	if reply.AlternateAddr != nil {
		if g.hasIncoming(reply.AlternateNodeID) || g.hasOutgoing(reply.AlternateNodeID) {
			return util.Errorf("received forward from node %d to %d (%s); already have active connection, skipping",
				reply.NodeID, reply.AlternateNodeID, reply.AlternateAddr)
		}
		forwardAddr, err := reply.AlternateAddr.Resolve()
		if err != nil {
			return util.Errorf("unable to resolve alternate address %s for node %d: %s", reply.AlternateAddr, reply.AlternateNodeID, err)
		}
		c.forwardAddr = forwardAddr
		return util.Errorf("received forward from node %d to %d (%s)", reply.NodeID, reply.AlternateNodeID, reply.AlternateAddr)
	}

	// If we have the sentinel gossip we're considered connected.
	g.checkHasConnected()

	// Check whether this outgoing client is duplicating work already
	// being done by an incoming client, either because an outgoing
	// matches an incoming or the client is connecting to itself.
	if g.is.NodeID == c.peerID {
		return util.Errorf("stopping outgoing client to node %d (%s); loopback connection", c.peerID, c.addr)
	} else if g.hasIncoming(c.peerID) && g.is.NodeID < c.peerID {
		// To avoid mutual shutdown, we only shutdown our client if our
		// node ID is less than the peer's.
		return util.Errorf("stopping outgoing client to node %d (%s); already have incoming", c.peerID, c.addr)
	}

	return nil
}

// TODO(tamird): extract
type contextWithStopper struct {
	context.Context
	stopper *stop.Stopper
}

func (ctx contextWithStopper) Done() <-chan struct{} {
	return ctx.stopper.ShouldDrain()
}

func (ctx contextWithStopper) Err() error {
	select {
	case <-ctx.Done():
		return context.Canceled
	default:
		return nil
	}
}

// gossip loops, sending deltas of the infostore and receiving deltas
// in turn. If an alternate is proposed on response, the client addr
// is modified and method returns for forwarding by caller.
func (c *client) gossip(g *Gossip, stopper *stop.Stopper) error {
	// For un-bootstrapped node, g.is.NodeID is 0 when client start gossip,
	// so it's better to get nodeID from g.is every time.
	g.mu.Lock()
	addr := g.is.NodeAddr
	g.mu.Unlock()

	ctx := contextWithStopper{
		Context: context.Background(),
		stopper: stopper,
	}

	stream, err := c.rpcClient.Gossip(ctx)
	if err != nil {
		return err
	}

	if err := c.requestGossip(g, addr, stream); err != nil {
		return err
	}

	sendGossipChan := make(chan struct{}, 1)

	// Register a callback for gossip updates.
	updateCallback := func(_ string, _ roachpb.Value) {
		select {
		case sendGossipChan <- struct{}{}:
		default:
		}
	}
	// Defer calling "undoer" callback returned from registration.
	defer g.RegisterCallback(".*", updateCallback)()

	// Loop in worker, sending updates from the info store.
	stopper.RunWorker(func() {
		for {
			select {
			case <-sendGossipChan:
				if err := c.sendGossip(g, addr, stream); err != nil {
					if !grpcutil.IsClosedConnection(err) {
						log.Error(err)
					}
					return
				}
			case <-stopper.ShouldStop():
				return
			}
		}
	})

	// Loop until stopper is signalled, or until either the gossip or RPC clients are closed.
	// The stopper's signal is propagated through the context attached to the stream.
	for {
		reply, err := stream.Recv()
		if err != nil {
			return err
		}
		if err := c.handleResponse(g, reply); err != nil {
			return err
		}
	}
}
