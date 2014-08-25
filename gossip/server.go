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
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// server maintains an array of connected peers to which it gossips
// newly arrived information on a periodic basis.
type server struct {
	interval      time.Duration       // Interval at which to gossip fresh info
	mu            sync.Mutex          // Mutex protects is (infostore) & incoming
	ready         *sync.Cond          // Broadcasts wakeup to waiting gossip requests
	is            *infoStore          // The backing infostore
	closed        bool                // True if server was closed
	incoming      *addrSet            // Incoming client addresses
	clientAddrMap map[string]net.Addr // Incoming client's local address -> client's server address
}

// newServer creates and returns a server struct.
func newServer(interval time.Duration) *server {
	s := &server{
		is:            newInfoStore(nil),
		interval:      interval,
		incoming:      newAddrSet(MaxPeers),
		clientAddrMap: map[string]net.Addr{},
	}
	s.ready = sync.NewCond(&s.mu)
	return s
}

// Gossip receives gossipped information from a peer node.
// The received delta is combined with the infostore, and this
// node's own gossip is returned to requesting client.
func (s *server) Gossip(args *Request, reply *Response) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If there is no more capacity to accept incoming clients, return
	// a random already-being-serviced incoming client as an alternate.
	if !s.incoming.hasAddr(args.Addr) {
		if !s.incoming.hasSpace() {
			reply.Alternate = s.incoming.selectRandom()
			return nil
		}
		s.incoming.addAddr(args.Addr)
		// This lookup map allows the incoming client to be removed from
		// the incoming addr set when its connection is closed. See
		// server.serveConn() below.
		s.clientAddrMap[args.LAddr.String()] = args.Addr
	}

	// Update infostore with gossipped infos.
	if args.Delta != nil {
		log.V(1).Infof("received delta infostore from client %s: %s", args.Addr, args.Delta)
		s.is.combine(args.Delta)
	}
	// If requested max sequence is not -1, wait for gossip interval to expire.
	if args.MaxSeq != -1 {
		s.ready.Wait()
	}
	// The exit condition for waiting clients.
	if s.closed {
		return util.Errorf("gossip server shutdown")
	}
	// Return reciprocal delta.
	delta := s.is.delta(args.Addr, args.MaxSeq)
	if delta != nil {
		// If V(1), double check that we can gob-encode the infostore.
		// Problems here seem to very confusingly disappear into the RPC internals.
		if log.V(1) {
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(delta); err != nil {
				log.Fatalf("infostore could not be encoded: %v", err)
			}
		}
		reply.Delta = delta
		log.Infof("gossip: client %s sent %d info(s)", args.Addr, delta.infoCount())
	}
	return nil
}

// jitteredGossipInterval returns a randomly jittered duration from
// interval [0.75 * gossipInterval, 1.25 * gossipInterval).
func (s *server) jitteredGossipInterval() time.Duration {
	return time.Duration(float64(s.interval) * (0.75 + 0.5*rand.Float64()))
}

// start initializes the infostore with the rpc server address and
// then begins processing connecting clients in an infinite select
// loop via goroutine. Periodically, clients connected and awaiting
// the next round of gossip are awoken via the conditional variable.
func (s *server) start(rpcServer *rpc.Server) {
	s.is.NodeAddr = rpcServer.Addr()
	rpcServer.RegisterName("Gossip", s)
	rpcServer.AddCloseCallback(s.onClose)

	go func() {
		// Periodically wakeup blocked client gossip requests.
		gossipTimeout := time.Tick(s.jitteredGossipInterval())
		for {
			select {
			case <-gossipTimeout:
				// Wakeup all blocked gossip requests.
				s.ready.Broadcast()
			}
		}
	}()
}

// stop sets the server's closed bool to true and broadcasts to
// waiting gossip clients to wakeup and finish.
func (s *server) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.ready.Broadcast() // wake up clients
}

// onClose is invoked by the rpcServer each time a connected client
// is closed. Remove the client from the incoming address set.
func (s *server) onClose(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if clientAddr, ok := s.clientAddrMap[conn.RemoteAddr().String()]; ok {
		s.incoming.removeAddr(clientAddr)
	}
}
