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
	ready *sync.Cond // Broadcasts wakeup to waiting gossip requests

	mu       sync.Mutex            // Protects the fields below
	is       infoStore             // The backing infostore
	closed   bool                  // True if server was closed
	incoming nodeSet               // Incoming client node IDs
	lAddrMap map[string]clientInfo // Incoming client's local address -> client's node info
}

// newServer creates and returns a server struct.
func newServer() *server {
	s := &server{
		is:       newInfoStore(0, util.UnresolvedAddr{}),
		incoming: makeNodeSet(MaxPeers),
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
	defer s.mu.Unlock()

	addr, err := args.Addr.Resolve()
	if err != nil {
		return nil, util.Errorf("addr %s could not be converted to net.Addr: %s", args.Addr, err)
	}
	lAddr, err := args.LAddr.Resolve()
	if err != nil {
		return nil, util.Errorf("local addr %s could not be converted to net.Addr: %s", args.LAddr, err)
	}

	reply.NodeID = s.is.NodeID

	// If there is no more capacity to accept incoming clients, return
	// a random already-being-serviced incoming client as an alternate.
	if !s.incoming.hasNode(args.NodeID) {
		if !s.incoming.hasSpace() {
			// Map iteration order is random.
			for _, cInfo := range s.lAddrMap {
				reply.Alternate = cInfo.addr
				return reply, nil
			}
		}
		s.incoming.addNode(args.NodeID)
		// This lookup map allows the incoming client to be removed from
		// the incoming addr set when its connection is closed. See
		// server.serveConn() below.
		s.lAddrMap[lAddr.String()] = clientInfo{args.NodeID, &args.Addr}
	}

	// Update infostore with gossiped infos.
	if infoCount := len(args.Delta); infoCount > 0 {
		if log.V(1) {
			log.Infof("gossip: received %s from %s", args.Delta, addr)
		} else {
			log.Infof("gossip: received %d info(s) from %s", infoCount, addr)
		}
	}

	// Combine incoming infos if specified and exit.
	if args.Delta != nil {
		s.is.combine(args.Delta, args.NodeID)
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
		if len(reply.Delta) > 0 {
			reply.HighWaterStamps = s.is.getHighWaterStamps()
			return reply, nil
		}
		// Wait for server to get new gossip set before computing delta.
		s.ready.Wait()
	}
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

	updateCallback := func(key string, content []byte) {
		// Wakeup all pending clients.
		s.ready.Broadcast()
	}
	unregisterCB := s.is.registerCallback(".*", updateCallback)

	stopper.RunWorker(func() {
		// Periodically wakeup blocked client gossip requests.
		for {
			select {
			case <-stopper.ShouldStop():
				s.stop(unregisterCB)
				return
			}
		}
	})
}

// stop sets the server's closed bool to true and broadcasts to
// waiting gossip clients to wakeup and finish.
func (s *server) stop(unregisterCB interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.ready.Broadcast() // wake up clients
	s.is.unregisterCallback(unregisterCB)
}

// onClose is invoked by the rpcServer each time a connected client
// is closed. Remove the client from the incoming address set.
func (s *server) onClose(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cInfo, ok := s.lAddrMap[conn.RemoteAddr().String()]; ok {
		s.incoming.removeNode(cInfo.id)
	}
}
