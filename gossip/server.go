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
	"math/rand"
	"net"
	"sync"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

// A clientInfo holds information about an incoming client connection
// and is stored in the server's lAddrMap, which is keyed by an
// incoming client's local address.
type clientInfo struct {
	id    roachpb.NodeID
	addr  *util.UnresolvedAddr
	nodes map[int32]*Node
	open  bool
}

// server maintains an array of connected peers to which it gossips
// newly arrived information on a periodic basis.
type server struct {
	mu       sync.Mutex                // Protects the fields below
	is       *infoStore                // The backing infostore
	closed   bool                      // True if server was closed
	incoming nodeSet                   // Incoming client node IDs
	lAddrMap map[string]clientInfo     // Incoming client's local address -> client's node info
	nodeMap  map[roachpb.NodeID]string // Incoming client's node ID -> local address (string)
	tighten  chan roachpb.NodeID       // Channel of too-distant node IDs
	sent     int                       // Count of infos sent from this server to clients
	received int                       // Count of infos received from clients
	ready    *sync.Cond                // Broadcasts wakeup to waiting gossip requests

	simulationCycler *sync.Cond // Used when simulating the network to signal next cycle
}

// newServer creates and returns a server struct.
func newServer() *server {
	s := &server{
		is:       newInfoStore(0, util.UnresolvedAddr{}),
		incoming: makeNodeSet(minPeers),
		lAddrMap: map[string]clientInfo{},
		nodeMap:  map[roachpb.NodeID]string{},
		tighten:  make(chan roachpb.NodeID, 1),
	}
	s.ready = sync.NewCond(&s.mu)
	return s
}

// Gossip receives gossiped information from a peer node.
// The received delta is combined with the infostore, and this
// node's own gossip is returned to requesting client.
func (s *server) Gossip(argsI proto.Message) (proto.Message, error) {
	args := argsI.(*Request)
	reply := &Response{}

	s.mu.Lock()
	defer func() {
		if s.simulationCycler != nil {
			s.simulationCycler.Wait()
		}
		s.mu.Unlock()
	}()

	lAddr, err := args.LAddr.Resolve()
	if err != nil {
		return nil, util.Errorf("local addr %s could not be converted to net.Addr: %s", args.LAddr, err)
	}
	// Verify that the client connection is valid and hasn't been closed.
	if ci, ok := s.lAddrMap[lAddr.String()]; !ok {
		incoming := []string{}
		for key := range s.lAddrMap {
			incoming = append(incoming, key)
		}
		return nil, util.Errorf("node %d: node %d believes its address is %s but that doesn't match any incoming connections: %s",
			s.is.NodeID, args.NodeID, lAddr, incoming)
	} else if !ci.open {
		return nil, util.Errorf("node %d: connection already closed from node %d (%s); ignoring gossip", s.is.NodeID, args.NodeID, lAddr)
	}

	reply.NodeID = s.is.NodeID

	// Decide whether or not we can accept the incoming connection
	// as a permanent peer. We always accept its input and return
	// our delta.
	canAccept := true
	if args.NodeID != 0 {
		if !s.incoming.hasNode(args.NodeID) {
			if !s.incoming.hasSpace() {
				canAccept = false
			} else {
				s.incoming.addNode(args.NodeID)
				// This lookup map restricts incoming connections to a single
				// connection per node ID.
				s.nodeMap[args.NodeID] = lAddr.String()
			}
		} else {
			// Verify that there aren't multiple incoming clients from same
			// node, but with different connections. This can happen when
			// bootstrap connections are initiated through a load balancer.
			if lAddrStr, ok := s.nodeMap[args.NodeID]; ok && lAddrStr != lAddr.String() {
				return nil, util.Errorf("duplicate connection from node %d", args.NodeID)
			}
		}
	}
	// Update the lAddrMap, which allows the incoming client to be
	// removed from the incoming addr set when its connection is
	// closed. See server.serveConn() below.
	if canAccept {
		s.lAddrMap[lAddr.String()] = clientInfo{
			id:    args.NodeID,
			addr:  &args.Addr,
			nodes: args.Nodes,
			open:  true,
		}

		// If incoming infos are specified, combine and exit. This is a
		// "push" from the incoming client.
		if args.Delta != nil {
			s.received += len(args.Delta)
			freshCount, err := s.is.combine(args.Delta, args.NodeID)
			if err != nil {
				log.Warningf("node %d failed to fully combine gossip delta from node %d: %s", s.is.NodeID, args.NodeID, err)
			}
			if log.V(1) {
				log.Infof("node %d received %s from node %d (%d fresh)", s.is.NodeID, extractKeys(args.Delta), args.NodeID, freshCount)
			}
			if s.closed {
				return nil, util.Errorf("gossip server shutdown")
			}
			s.maybeTighten()
			reply.Nodes = s.is.getNodes()
			return reply, nil
		}
	}

	// Otherwise, loop until the server has deltas for the client.
	for {
		// The exit condition for waiting clients.
		if s.closed {
			return nil, util.Errorf("gossip server shutdown")
		}
		if canAccept {
			reply.Delta = s.is.delta(args.NodeID, s.lAddrMap[lAddr.String()].nodes)
			if len(reply.Delta) > 0 {
				if log.V(1) {
					log.Infof("node %d returned %d info(s) to node %d", s.is.NodeID, len(reply.Delta), args.NodeID)
				}
				reply.Nodes = s.is.getNodes()
				s.sent += len(reply.Delta)
				return reply, nil
			}
		} else {
			// If there is no more capacity to accept incoming clients, return
			// a random already-being-serviced incoming client as an alternate.
			var addrs []string
			for addr, cInfo := range s.lAddrMap {
				if cInfo.id != 0 && cInfo.id != s.is.NodeID {
					addrs = append(addrs, addr)
				}
			}
			if len(addrs) > 0 {
				cInfo := s.lAddrMap[addrs[int(rand.Int31n(int32(len(addrs))))]]
				reply.AlternateAddr = cInfo.addr
				reply.AlternateNodeID = cInfo.id
				log.Infof("refusing gossip from node %d (max %d conns); forwarding to %d (%s)",
					args.NodeID, s.incoming.maxSize, cInfo.id, cInfo.addr)
				return reply, nil
			}
		}
		// Wait for server to get new gossip set before computing delta.
		s.ready.Wait()
		// If the client connection was closed, exit.
		if _, ok := s.lAddrMap[lAddr.String()]; !ok {
			return nil, util.Errorf("client connection closed")
		}
	}
}

// InfosSent returns the total count of infos sent to clients.
func (s *server) InfosSent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sent
}

// InfosReceived returns the total count of infos received from clients.
func (s *server) InfosReceived() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.received
}

// maybeTighten examines the infostore for the most distant node and
// if more distant than MaxHops, sends on the tightenNetwork channel
// to start a new client connection.
func (s *server) maybeTighten() {
	distantNodeID, distantHops := s.is.mostDistant()
	if log.V(1) {
		log.Infof("@%d: distantHops: %d from %d", s.is.NodeID, distantHops, distantNodeID)
	}
	if distantHops > MaxHops {
		select {
		case s.tighten <- distantNodeID:
			if log.V(1) {
				log.Infof("if possible, tightening network to node %d (%d > %d)", distantNodeID, distantHops, MaxHops)
			}
		default:
			// Do nothing.
		}
	}
}

// start initializes the infostore with the rpc server address and
// then begins processing connecting clients in an infinite select
// loop via goroutine. Periodically, clients connected and awaiting
// the next round of gossip are awoken via the conditional variable.
func (s *server) start(rpcServer *rpc.Server, addr net.Addr, stopper *stop.Stopper) {
	s.is.NodeAddr = util.MakeUnresolvedAddr(addr.Network(), addr.String())
	if err := rpcServer.Register("Gossip.Gossip", s.Gossip, &Request{}); err != nil {
		log.Fatalf("unable to register gossip service with RPC server: %s", err)
	}
	rpcServer.AddOpenCallback(s.onOpen)
	rpcServer.AddCloseCallback(s.onClose)

	updateCallback := func(_ string, _ roachpb.Value) {
		// Wakeup all pending clients.
		s.ready.Broadcast()
	}
	unregister := s.is.registerCallback(".*", updateCallback)

	stopper.RunWorker(func() {
		select {
		case <-stopper.ShouldStop():
			s.stop(unregister)
			return
		}
	})
}

// stop sets the server's closed bool to true and broadcasts to
// waiting gossip clients to wakeup and finish.
func (s *server) stop(unregister func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	unregister()
	s.closed = true
	s.ready.Broadcast() // wake up clients
}

// onOpen is invoked by the rpcServer each time a client connects.
// Add a placeholder value for the lAddrMap. This prevents races
// where inflight RPCs re-add gossip clients to the incoming map
// even when the incoming connection has been closed out from
// under them.
//
// TODO(spencer): remove use of onOpen callback once gossip is using
//   streaming with gRPC.
func (s *server) onOpen(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	remoteAddr := conn.RemoteAddr().String()
	if _, ok := s.lAddrMap[remoteAddr]; !ok {
		s.lAddrMap[remoteAddr] = clientInfo{open: true}
	}
}

// onClose is invoked by the rpcServer each time a connected client
// is closed. Remove the client from the incoming address set.
func (s *server) onClose(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	remoteAddr := conn.RemoteAddr().String()
	if cInfo, ok := s.lAddrMap[remoteAddr]; ok {
		s.incoming.removeNode(cInfo.id)
		delete(s.nodeMap, cInfo.id)
		s.lAddrMap[remoteAddr] = clientInfo{open: false}
		s.ready.Broadcast() // wake up clients
	}
}
