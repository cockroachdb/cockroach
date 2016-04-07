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

	"golang.org/x/net/context"
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
	ready    chan struct{}                          // Broadcasts wakeup to waiting gossip requests

	simulationCycler *sync.Cond // Used when simulating the network to signal next cycle
}

// newServer creates and returns a server struct.
func newServer(stopper *stop.Stopper) *server {
	return &server{
		stopper:  stopper,
		is:       newInfoStore(0, util.UnresolvedAddr{}, stopper),
		incoming: makeNodeSet(minPeers),
		nodeMap:  make(map[util.UnresolvedAddr]roachpb.NodeID),
		tighten:  make(chan roachpb.NodeID, 1),
		ready:    make(chan struct{}),
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

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	syncChan := make(chan struct{}, 1)
	send := func(reply *Response) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case syncChan <- struct{}{}:
			defer func() { <-syncChan }()
			return stream.Send(reply)
		}
	}

	defer func() { syncChan <- struct{}{} }()

	// Verify that there aren't multiple incoming connections from the same
	// node. This can happen when bootstrap connections are initiated through
	// a load balancer.
	s.mu.Lock()
	_, ok := s.nodeMap[args.Addr]
	s.mu.Unlock()
	if ok {
		return util.Errorf("duplicate connection from node at %s", args.Addr)
	}

	errCh := make(chan error, 1)

	// Starting workers in a task prevents data races during shutdown.
	s.stopper.RunTask(func() {
		s.stopper.RunWorker(func() {
			errCh <- s.gossipReceiver(&args, send, stream.Recv)
		})
	})

	reply := new(Response)

	for {
		s.mu.Lock()

		delta := s.is.delta(args.HighWaterStamps)

		if infoCount := len(delta); infoCount > 0 {
			if log.V(1) {
				log.Infof("node %d returned %d info(s) to node %d", s.is.NodeID, infoCount, args.NodeID)
			}

			*reply = Response{
				NodeID:          s.is.NodeID,
				HighWaterStamps: s.is.getHighWaterStamps(),
				Delta:           delta,
			}

			s.mu.Unlock()
			if err := send(reply); err != nil {
				return err
			}
			s.mu.Lock()
			s.sent += infoCount
		}

		ready := s.ready
		s.mu.Unlock()

		select {
		case <-s.stopper.ShouldDrain():
			return nil
		case err := <-errCh:
			return err
		case <-ready:
		}
	}
}

func (s *server) gossipReceiver(argsPtr **Request, senderFn func(*Response) error, receiverFn func() (*Request, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	reply := new(Response)

	// This loop receives gossip from the client. It does not attempt to send the
	// server's gossip to the client.
	for {
		args := *argsPtr
		if args.NodeID != 0 {
			// Decide whether or not we can accept the incoming connection
			// as a permanent peer.
			if args.NodeID == s.is.NodeID {
				// This is an incoming loopback connection which should be closed by
				// the client.
			} else if s.incoming.hasNode(args.NodeID) {
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
				// Choose a random peer for forwarding.
				altIdx := rand.Intn(len(s.nodeMap))
				for addr, id := range s.nodeMap {
					if altIdx == 0 {
						alternateAddr = addr
						alternateNodeID = id
						break
					}
					altIdx--
				}

				log.Infof("refusing gossip from node %d (max %d conns); forwarding to %d (%s)",
					args.NodeID, s.incoming.maxSize, alternateNodeID, alternateAddr)

				*reply = Response{
					NodeID:          s.is.NodeID,
					AlternateAddr:   &alternateAddr,
					AlternateNodeID: alternateNodeID,
				}

				s.mu.Unlock()
				err := senderFn(reply)
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
			NodeID:          s.is.NodeID,
			HighWaterStamps: s.is.getHighWaterStamps(),
		}

		s.mu.Unlock()
		err = senderFn(reply)
		s.mu.Lock()
		if err != nil {
			return err
		}

		if cycler := s.simulationCycler; cycler != nil {
			cycler.Wait()
		}

		s.mu.Unlock()
		recvArgs, err := receiverFn()
		s.mu.Lock()
		if err != nil {
			return err
		}

		// *argsPtr holds the remote peer state; we need to update it whenever we
		// receive a new non-nil request. We avoid assigning to *argsPtr directly
		// because the gossip sender above has closed over *argsPtr and will NPE if
		// *argsPtr were set to nil.
		*argsPtr = recvArgs
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

	broadcast := func() {
		ready := make(chan struct{})

		s.mu.Lock()
		close(s.ready)
		s.ready = ready
		s.mu.Unlock()
	}
	unregister := s.is.registerCallback(".*", func(_ string, _ roachpb.Value) {
		broadcast()
	})

	s.stopper.RunWorker(func() {
		<-s.stopper.ShouldDrain()

		s.mu.Lock()
		unregister()
		s.mu.Unlock()

		broadcast()
	})
}
