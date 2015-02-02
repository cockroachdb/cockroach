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
// Author: Ben Darnell

package multiraft

import (
	"net"
	"net/rpc"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/util/log"
	"github.com/coreos/etcd/raft/raftpb"
)

// The Transport interface is supplied by the application to manage communication with
// other nodes. It is responsible for mapping from IDs to some communication channel
// (in the simplest case, a host:port pair could be used as an ID, although this would
// make it impossible to move an instance from one host to another except by syncing
// up a new node from scratch).
type Transport interface {
	// Listen informs the Transport of the local node's ID and callback interface.
	// The Transport should associate the given id with the server object so other Transport's
	// Connect methods can find it.
	Listen(id NodeID, server ServerInterface) error

	// Stop undoes a previous Listen.
	Stop(id NodeID)

	// Connect looks up a node by id and returns a stub interface to submit RPCs to it.
	Connect(id NodeID) (ClientInterface, error)
}

// RaftMessageRequest wraps a raft message.
type RaftMessageRequest struct {
	GroupID uint64
	Message raftpb.Message
}

// RaftMessageResponse is empty (raft uses a one-way messaging model; if a response
// is needed it will be sent as a separate message).
type RaftMessageResponse struct {
}

// ServerInterface is the methods we expose for use by net/rpc.
type ServerInterface interface {
	RaftMessage(req *RaftMessageRequest, resp *RaftMessageResponse) error
}

var (
	raftMessageName = "MultiRaft.RaftMessage"
)

// ClientInterface is the interface expected of the client provided by a transport.
// It is satisfied by rpc.Client, but could be implemented in other ways (using
// rpc.Call as a dumb data structure)
type ClientInterface interface {
	Go(serviceMethod string, args interface{}, reply interface{}, done chan *rpc.Call) *rpc.Call
	Close() error
}

// asyncClient bridges MultiRaft's channel-oriented interface with the synchronous RPC interface.
// Outgoing requests are run in a non-blocking fire-and-forget fashion.
type asyncClient struct {
	nodeID NodeID
	conn   ClientInterface
}

func (a *asyncClient) raftMessage(req *RaftMessageRequest) {
	a.conn.Go(raftMessageName, req, &RaftMessageResponse{}, nil)
}

type localRPCTransport struct {
	mu        sync.Mutex
	listeners map[NodeID]net.Listener
}

// NewLocalRPCTransport creates a Transport for local testing use. MultiRaft instances
// sharing the same local Transport can find and communicate with each other by ID (which
// can be an arbitrary string). Each instance binds to a different unused port on
// localhost.
// Because this is just for local testing, it doesn't use TLS.
func NewLocalRPCTransport() Transport {
	return &localRPCTransport{
		listeners: make(map[NodeID]net.Listener),
	}
}

func (lt *localRPCTransport) Listen(id NodeID, server ServerInterface) error {
	rpcServer := rpc.NewServer()
	err := rpcServer.RegisterName("MultiRaft", server)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}

	lt.mu.Lock()
	if _, ok := lt.listeners[id]; ok {
		log.Fatalf("node %d already listening", id)
	}
	lt.listeners[id] = listener
	lt.mu.Unlock()
	go lt.accept(rpcServer, listener)
	return nil
}

func (lt *localRPCTransport) accept(server *rpc.Server, listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if strings.HasSuffix(err.Error(), "use of closed network connection") {
				return
			}
			// TODO(bdarnell): are any transient errors possible here?
			log.Fatalf("localRPCTransport.accept: %s", err.Error())
			continue
		}
		go server.ServeConn(conn)
	}
}

func (lt *localRPCTransport) Stop(id NodeID) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.listeners[id].Close()
	delete(lt.listeners, id)
}

func (lt *localRPCTransport) Connect(id NodeID) (ClientInterface, error) {
	lt.mu.Lock()
	address := lt.listeners[id].Addr().String()
	lt.mu.Unlock()
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return client, nil
}
