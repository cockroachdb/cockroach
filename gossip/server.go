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
	"net"
	"sync"

	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

// server maintains an array of connected peers to which it gossips
// newly arrived information on a periodic basis.
type server struct {
	stopper *stop.Stopper

	mu       sync.Mutex                             // Protects the fields below
	is       *infoStore                             // The backing infostore
	incoming nodeSet                                // Incoming client node IDs
	nodeMap  map[util.UnresolvedAddr]roachpb.NodeID // Incoming client's local address -> node ID
	tighten  chan roachpb.NodeID                    // Channel of too-distant node IDs
	sent     int                                    // Count of infos sent from this server to clients
	received int                                    // Count of infos received from clients
	ready    *sync.Cond                             // Broadcasts wakeup to waiting gossip requests

	simulationCycler *sync.Cond // Used when simulating the network to signal next cycle
}

// newServer creates and returns a server struct.
func newServer(stopper *stop.Stopper) *server {
	s := &server{
		stopper:  stopper,
		is:       newInfoStore(0, util.UnresolvedAddr{}, stopper),
		incoming: makeNodeSet(minPeers),
		nodeMap:  make(map[util.UnresolvedAddr]roachpb.NodeID),
		tighten:  make(chan roachpb.NodeID, 1),
	}
	s.ready = sync.NewCond(&s.mu)
	return s
}

func (s *server) gossipSender(argsPtr **Request, senderFn func(*Response) error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reply := new(Response)

	for {
		if !s.stopper.RunTask(func() {
			args := *argsPtr
			delta := s.is.delta(args.Nodes)
			if infoCount := len(delta); infoCount > 0 {
				if log.V(1) {
					log.Infof("node %d returned %d info(s) to node %d", s.is.NodeID, infoCount, args.NodeID)
				}

				*reply = Response{
					NodeID: s.is.NodeID,
					Nodes:  s.is.getNodes(),
					Delta:  delta,
				}

				s.mu.Unlock()
				err := senderFn(reply)
				s.mu.Lock()
				if err != nil {
					log.Error(err)
					return
				}
				s.sent += infoCount
			}
		}) {
			return
		}

		s.ready.Wait()
	}
}

// Gossip receives gossiped information from a peer node.
// The received delta is combined with the infostore, and this
// node's own gossip is returned to requesting client.
func (s *server) Gossip(stream Gossip_GossipServer) error {
	args, err := stream.Recv()
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Verify that there aren't multiple incoming connections from the same
	// node. This can happen when bootstrap connections are initiated through
	// a load balancer.
	if _, ok := s.nodeMap[args.Addr]; ok {
		return util.Errorf("duplicate connection from node at %s", args.Addr)
	}

	s.stopper.RunWorker(func() {
		s.gossipSender(&args, stream.Send)
	})

	reply := new(Response)

	// This loop receives gossip from the client. It does not attempt to send the
	// server's gossip to the client.
	for {
		if args.NodeID != 0 {
			// Decide whether or not we can accept the incoming connection
			// as a permanent peer.
			if s.incoming.hasNode(args.NodeID) {
				// Do nothing.
			} else if s.incoming.hasSpace() {
				s.incoming.addNode(args.NodeID)
				s.nodeMap[args.Addr] = args.NodeID

				defer func(nodeID roachpb.NodeID, addr util.UnresolvedAddr) {
					s.incoming.removeNode(nodeID)
					delete(s.nodeMap, addr)
				}(args.NodeID, args.Addr)
			} else {
				var alternateAddr util.UnresolvedAddr
				var alternateNodeID roachpb.NodeID
				for addr, id := range s.nodeMap {
					alternateAddr = addr
					alternateNodeID = id
					break
				}

				log.Infof("refusing gossip from node %d (max %d conns); forwarding to %d (%s)",
					args.NodeID, s.incoming.maxSize, alternateNodeID, alternateAddr)

				*reply = Response{
					NodeID:          s.is.NodeID,
					AlternateAddr:   &alternateAddr,
					AlternateNodeID: alternateNodeID,
				}

				s.mu.Unlock()
				err := stream.Send(reply)
				s.mu.Lock()
				return err
			}
		}

		s.received += len(args.Delta)
		freshCount, err := s.is.combine(args.Delta, args.NodeID)
		if err != nil {
			log.Warningf("node %d failed to fully combine gossip delta from node %d: %s", s.is.NodeID, args.NodeID, err)
		}
		if log.V(1) {
			log.Infof("node %d received %s from node %d (%d fresh)", s.is.NodeID, extractKeys(args.Delta), args.NodeID, freshCount)
		}
		s.maybeTighten()

		*reply = Response{
			NodeID: s.is.NodeID,
			Nodes:  s.is.getNodes(),
		}

		s.mu.Unlock()
		err = stream.Send(reply)
		s.mu.Lock()
		if err != nil {
			return err
		}

		if cycler := s.simulationCycler; cycler != nil {
			cycler.Wait()
		}

		s.mu.Unlock()
		recvArgs, err := stream.Recv()
		s.mu.Lock()
		if err != nil {
			return err
		}

		// args holds the remote peer state; we need to update it whenever we receive a new non-nil
		// request. We avoid assigning to args directly because the gossip sender worker above has
		// closed over args and may NPE if args were set to nil.
		args = recvArgs
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
func (s *server) start(grpcServer *grpc.Server, addr net.Addr) {
	s.mu.Lock()
	s.is.NodeAddr = util.MakeUnresolvedAddr(addr.Network(), addr.String())
	s.mu.Unlock()
	RegisterGossipServer(grpcServer, s)

	updateCallback := func(_ string, _ roachpb.Value) {
		// Wakeup all pending clients.
		s.ready.Broadcast()
	}
	unregister := s.is.registerCallback(".*", updateCallback)

	s.stopper.RunWorker(func() {
		<-s.stopper.ShouldStop()
		s.mu.Lock()
		defer s.mu.Unlock()
		unregister()
		s.ready.Broadcast() // wake up clients
	})
}
