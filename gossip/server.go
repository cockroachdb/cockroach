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
	"math"
	"math/rand"
	"net"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

type clientInfo struct {
	id   roachpb.NodeID
	addr *util.UnresolvedAddr
}

// server maintains an array of connected peers to which it gossips
// newly arrived information on a periodic basis.
type server struct {
	mu       sync.Mutex            // Protects the fields below
	is       infoStore             // The backing infostore
	closed   bool                  // True if server was closed
	incoming nodeSet               // Incoming client node IDs
	lAddrMap map[string]clientInfo // Incoming client's local address -> client's node info
	ready    *sync.Cond            // Broadcasts wakeup to waiting gossip requests

	simulationCycler *sync.Cond // Used when simulating the network to signal next cycle
}

// newServer creates and returns a server struct.
func newServer() *server {
	s := &server{
		is:       newInfoStore(0, util.UnresolvedAddr{}),
		incoming: makeNodeSet(1),
		lAddrMap: map[string]clientInfo{},
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

	addr, err := args.Addr.Resolve()
	if err != nil {
		return nil, util.Errorf("addr %s could not be converted to net.Addr: %s", args.Addr, err)
	}
	lAddr, err := args.LAddr.Resolve()
	if err != nil {
		return nil, util.Errorf("local addr %s could not be converted to net.Addr: %s", args.LAddr, err)
	}

	reply.NodeID = s.is.NodeID

	// Decide whether or not we can accept the incoming connection
	// as a permanent peer. We always accept its input and return
	// our delta.
	canAccept := true
	s.incoming.setMaxSize(s.maxPeers())
	if !s.incoming.hasNode(args.NodeID) {
		if !s.incoming.hasSpace() {
			canAccept = false
		} else {
			s.incoming.addNode(args.NodeID)
			// This lookup map allows the incoming client to be removed from
			// the incoming addr set when its connection is closed. See
			// server.serveConn() below.
			s.lAddrMap[lAddr.String()] = clientInfo{args.NodeID, &args.Addr}
		}
	}

	// Combine incoming infos if specified and exit.
	if args.Delta != nil {
		freshCount := s.is.combine(args.Delta, args.NodeID)
		if log.V(1) {
			log.Infof("received %s from %s (%d fresh)", args.Delta, addr, freshCount)
		} else {
			log.Infof("received %d (%d fresh) info(s) from %s", len(args.Delta), freshCount, addr)
		}
		if s.closed {
			return nil, util.Errorf("gossip server shutdown")
		}
		reply.HighWaterStamps = s.is.getHighWaterStamps()
		return reply, nil
	}

	// Otherwise, loop until the server has deltas for the client.
	for {
		// The exit condition for waiting clients.
		if s.closed {
			return nil, util.Errorf("gossip server shutdown")
		}
		reply.Delta = s.is.delta(args.NodeID, args.HighWaterStamps)
		if len(reply.Delta) > 0 || !canAccept {
			// If there is no more capacity to accept incoming clients, return
			// a random already-being-serviced incoming client as an alternate.
			if !canAccept {
				// Map iteration order is random.
				randIdx := rand.Int31n(int32(len(s.lAddrMap)))
				for _, cInfo := range s.lAddrMap {
					if randIdx == 0 {
						reply.Alternate = cInfo.addr
						break
					}
					randIdx--
				}
			}
			reply.HighWaterStamps = s.is.getHighWaterStamps()
			return reply, nil
		}
		// Wait for server to get new gossip set before computing delta.
		s.ready.Wait()
		// If the client connection was closed, exit.
		if !s.incoming.hasNode(args.NodeID) {
			return nil, util.Errorf("client connection closed")
		}
	}
}

// maxPeers returns the maximum number of peers each gossip node
// may connect to. This is based on maxHops, which is a preset
// maximum for number of hops allowed before the gossip network
// will seek to "tighten" by creating new connections to distant
// nodes.
func (s *server) maxPeers() int {
	var nodeCount int

	if err := s.is.visitInfos(func(key string, i *Info) error {
		if strings.HasPrefix(key, KeyNodeIDPrefix) {
			nodeCount++
		}
		return nil
	}); err != nil {
		panic(err)
	}

	peers := int(math.Ceil(math.Exp(2 * math.Log(float64(nodeCount)) / float64(MaxHops-1))))
	if peers < minPeers {
		return minPeers
	}
	return peers
}

// start initializes the infostore with the rpc server address and
// then begins processing connecting clients in an infinite select
// loop via goroutine. Periodically, clients connected and awaiting
// the next round of gossip are awoken via the conditional variable.
func (s *server) start(rpcServer *rpc.Server, stopper *stop.Stopper) {
	addr := rpcServer.Addr()
	s.is.NodeAddr = util.MakeUnresolvedAddr(addr.Network(), addr.String())
	if err := rpcServer.Register("Gossip.Gossip", s.Gossip, &Request{}); err != nil {
		log.Fatalf("unable to register gossip service with RPC server: %s", err)
	}
	rpcServer.AddCloseCallback(s.onClose)

	updateCallback := func(_ string, _ roachpb.Value) {
		// Wakeup all pending clients.
		s.ready.Broadcast()
	}
	unregister := s.is.registerCallback(".*", updateCallback)

	stopper.RunWorker(func() {
		// Periodically wakeup blocked client gossip requests.
		for {
			select {
			case <-stopper.ShouldStop():
				s.stop(unregister)
				return
			}
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

// onClose is invoked by the rpcServer each time a connected client
// is closed. Remove the client from the incoming address set.
func (s *server) onClose(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cInfo, ok := s.lAddrMap[conn.RemoteAddr().String()]; ok {
		s.incoming.removeNode(cInfo.id)
		s.ready.Broadcast() // wake up clients
	}
}
