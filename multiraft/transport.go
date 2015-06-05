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

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
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
	Listen(id proto.RaftNodeID, server ServerInterface) error

	// Stop undoes a previous Listen.
	Stop(id proto.RaftNodeID)

	// Send a message to the node specified in the request's To field.
	Send(req *RaftMessageRequest) error

	// Close all associated connections.
	Close()
}

// RaftMessageRequest wraps a raft message.
type RaftMessageRequest struct {
	GroupID proto.RaftID
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

type localRPCTransport struct {
	mu        sync.Mutex
	listeners map[proto.RaftNodeID]net.Listener
	clients   map[proto.RaftNodeID]*rpc.Client
	conns     map[net.Conn]struct{}
	closed    chan struct{}
}

// NewLocalRPCTransport creates a Transport for local testing use. MultiRaft instances
// sharing the same local Transport can find and communicate with each other by ID (which
// can be an arbitrary string). Each instance binds to a different unused port on
// localhost.
// Because this is just for local testing, it doesn't use TLS.
func NewLocalRPCTransport() Transport {
	return &localRPCTransport{
		listeners: make(map[proto.RaftNodeID]net.Listener),
		clients:   make(map[proto.RaftNodeID]*rpc.Client),
		conns:     make(map[net.Conn]struct{}),
		closed:    make(chan struct{}),
	}
}

func (lt *localRPCTransport) Listen(id proto.RaftNodeID, server ServerInterface) error {
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
			log.Fatalf("localRPCTransport.accept: %s", err)
			continue
		}
		lt.mu.Lock()
		lt.conns[conn] = struct{}{}
		lt.mu.Unlock()
		go func(conn net.Conn) {
			defer func() {
				lt.mu.Lock()
				defer lt.mu.Unlock()
				delete(lt.conns, conn)
			}()
			server.ServeConn(conn)
		}(conn)
	}
}

func (lt *localRPCTransport) Stop(id proto.RaftNodeID) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.listeners[id].Close()
	delete(lt.listeners, id)
	if client, ok := lt.clients[id]; ok {
		client.Close()
		delete(lt.clients, id)
	}
}

func (lt *localRPCTransport) getClient(id proto.RaftNodeID) (*rpc.Client, error) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	select {
	case <-lt.closed:
		return nil, util.Errorf("transport is closed")
	default:
	}

	client, ok := lt.clients[id]
	if ok {
		return client, nil
	}

	listener, ok := lt.listeners[id]
	if !ok {
		return nil, util.Errorf("unknown peer %v", id)
	}
	address := listener.Addr().String()

	// If this wasn't test code we wouldn't want to call Dial while holding the lock.
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	lt.clients[id] = client
	return client, err
}

func (lt *localRPCTransport) Send(req *RaftMessageRequest) error {
	client, err := lt.getClient(proto.RaftNodeID(req.Message.To))
	if err != nil {
		return err
	}
	call := client.Go(raftMessageName, req, &RaftMessageResponse{}, nil)
	select {
	case <-call.Done:
		// If the call failed synchronously, report an error.
		return call.Error
	default:
		// Otherwise, fire-and-forget.
		go func() {
			select {
			case <-call.Done:
			case <-lt.closed:
				return
			}
			if call.Error != nil {
				log.Errorf("sending rpc failed: %s", call.Error)
			}
		}()
		return nil
	}
}

func (lt *localRPCTransport) Close() {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	close(lt.closed)
	for _, c := range lt.clients {
		err := c.Close()
		if err != nil {
			log.Warningf("error stopping client: %s", err)
		}
	}
	for conn := range lt.conns {
		conn.Close()
	}
}
