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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// server maintains an array of connected peers to which it gossips
// newly arrived information on a periodic basis.
type server struct {
	interval      time.Duration       // Interval at which to gossip fresh info
	listener      net.Listener        // Server listener
	rpcServer     *rpc.Server         // RPC server instance
	mu            sync.Mutex          // Mutex protects is (infostore) & incoming
	ready         *sync.Cond          // Broadcasts wakeup to waiting gossip requests
	is            *infoStore          // The backing infostore
	closed        bool                // True if server was closed
	incoming      *addrSet            // Incoming client addresses
	clientAddrMap map[string]net.Addr // Incoming client's local address -> client's server address
}

// newServer creates and returns a server struct.
func newServer(addr net.Addr, interval time.Duration) *server {
	s := &server{
		interval:      interval,
		is:            newInfoStore(addr),
		incoming:      newAddrSet(MaxPeers),
		clientAddrMap: make(map[string]net.Addr),
	}
	s.ready = sync.NewCond(&s.mu)
	return s
}

// Gossip receives gossipped information from a peer node.
// The received delta is combined with the infostore, and this
// node's own gossip is returned to requesting client.
func (s *server) Gossip(args *GossipRequest, reply *GossipResponse) error {
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
		s.is.combine(args.Delta)
	}
	// If requested max sequence is not -1, wait for gossip interval to expire.
	if args.MaxSeq != -1 {
		s.ready.Wait()
	}
	// The exit condition for waiting clients.
	if s.closed {
		return fmt.Errorf("gossip server shutdown")
	}
	// Return reciprocal delta.
	delta := s.is.delta(args.Addr, args.MaxSeq)
	if delta != nil {
		reply.Delta = delta
	}

	return nil
}

// jitteredGossipInterval returns a randomly jittered duration from
// interval [0.75 * gossipInterval, 1.25 * gossipInterval]
func (s *server) jitteredGossipInterval() time.Duration {
	return time.Duration(float64(s.interval) * (0.75 + 0.5*rand.Float64()))
}

// serve starts up listener via goroutine and then processes
// connecting clients in an infinite select loop. Periodically, newly
// available gossip is sent to each peer client in turn. This method
// blocks and should be launched via goroutine.
func (s *server) serve() {
	// Start gossip server.
	go s.listen()

	// Periodically wakeup blocked client gossip requests.
	gossipTimeout := time.Tick(s.jitteredGossipInterval())
	for {
		select {
		case <-gossipTimeout:
			// Wakeup all blocked gossip requests.
			s.ready.Broadcast()
		}
	}
}

// stopServing sets the server's closed bool to true and closes the
// listener.
func (s *server) stopServing() {
	s.mu.Lock()
	s.closed = true
	s.listener.Close()
	s.ready.Broadcast() // wake up clients
	s.mu.Unlock()
}

// listen begins serving requests for the gossip protocol. This method
// should be invoked by goroutine as it will block.
func (s *server) listen() {
	ln, err := net.Listen(s.is.NodeAddr.Network(), s.is.NodeAddr.String())
	if err != nil {
		log.Fatalf("unable to start gossip node: %s", err)
	}
	s.listener = ln

	// Start serving gossip protocol in a loop until listener is closed.
	log.Printf("serving gossip protocol on %+v...", s.is.NodeAddr)
	s.rpcServer = rpc.NewServer()
	s.rpcServer.RegisterName("Gossip", s)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if !s.closed {
				log.Fatalf("gossip server terminated: %s", err)
			}
			break
		}
		// Serve connection to completion in a goroutine.
		go s.serveConn(conn)
	}
}

// serveConn synchronously serves a single connection. When the
// connection is closed, the client address is removed from the
// incoming set.
func (s *server) serveConn(conn net.Conn) {
	s.rpcServer.ServeConn(conn)
	s.mu.Lock()
	if clientAddr, ok := s.clientAddrMap[conn.RemoteAddr().String()]; ok {
		s.incoming.removeAddr(clientAddr)
	}
	s.mu.Unlock()
	conn.Close()
}
