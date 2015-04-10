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

	"github.com/cockroachdb/cockroach/proto"
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

// Gossip receives gossiped information from a peer node.
// The received delta is combined with the infostore, and this
// node's own gossip is returned to requesting client.
func (s *server) Gossip(args *proto.GossipRequest, reply *proto.GossipResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	addr, err := args.Addr.NetAddr()
	if err != nil {
		return util.Errorf("addr %s could not be converted to net.Addr: %s", args.Addr, err)
	}
	lAddr, err := args.LAddr.NetAddr()
	if err != nil {
		return util.Errorf("local addr %s could not be converted to net.Addr: %s", args.LAddr, err)
	}

	// If there is no more capacity to accept incoming clients, return
	// a random already-being-serviced incoming client as an alternate.
	if !s.incoming.hasAddr(addr) {
		if !s.incoming.hasSpace() {
			reply.Alternate = proto.FromNetAddr(s.incoming.selectRandom())
			return nil
		}
		s.incoming.addAddr(addr)
		// This lookup map allows the incoming client to be removed from
		// the incoming addr set when its connection is closed. See
		// server.serveConn() below.
		s.clientAddrMap[lAddr.String()] = addr
	}

	// Update infostore with gossiped infos.
	if args.Delta != nil {
		delta := &infoStore{}
		if err := gob.NewDecoder(bytes.NewBuffer(args.Delta)).Decode(delta); err != nil {
			return util.Errorf("infostore could not be decoded: %s", err)
		}
		log.V(1).Infof("received delta infostore from client %s: %s", addr, delta)
		s.is.combine(delta)
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
	delta := s.is.delta(addr, args.MaxSeq)
	if delta != nil {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(delta); err != nil {
			log.Fatalf("infostore could not be encoded: %s", err)
		}
		reply.Delta = buf.Bytes()
		log.Infof("gossip: client %s sent %d info(s)", addr, delta.infoCount())
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
func (s *server) start(rpcServer *rpc.Server, stopper *util.Stopper) {
	stopper.AddWorker()
	defer stopper.SetStopped()

	s.is.NodeAddr = rpcServer.Addr()
	if err := rpcServer.RegisterName("Gossip", s); err != nil {
		log.Fatalf("unable to register gossip service with RPC server: %s", err)
	}
	rpcServer.AddCloseCallback(s.onClose)

	go func() {
		// Periodically wakeup blocked client gossip requests.
		gossipTimeout := time.Tick(s.jitteredGossipInterval())
		for {
			select {
			case <-gossipTimeout:
				// Wakeup all blocked gossip requests.
				s.ready.Broadcast()
			case <-stopper.ShouldStop():
				s.stop()
				return
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
