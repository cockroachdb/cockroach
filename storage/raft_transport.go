// Copyright 2015 The Cockroach Authors.
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

package storage

import (
	"net"
	"sync"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/grpcutil"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

// RaftMessageHandler is the callback type used by RaftTransport.
type RaftMessageHandler func(*RaftMessageRequest) error

// The RaftTransport interface is supplied by the application to manage communication with
// other nodes. It is responsible for mapping from IDs to some communication channel.
// TODO(bdarnell): this interface needs to be updated and may just go away.
type RaftTransport interface {
	// Listen informs the RaftTransport of a local store's ID and callback interface.
	// The RaftTransport should associate the given id with the server object so other RaftTransport's
	// Connect methods can find it.
	Listen(id roachpb.StoreID, server RaftMessageHandler) error

	// Stop undoes a previous Listen.
	Stop(id roachpb.StoreID)

	// Send a message to the node specified in the request's To field.
	Send(req *RaftMessageRequest) error

	// Close all associated connections.
	Close()
}

type clientWithConn struct {
	conn   *grpc.ClientConn
	client MultiRaftClient
}

type localRPCTransport struct {
	mu      sync.Mutex
	servers map[roachpb.StoreID]net.Addr
	clients map[roachpb.StoreID]clientWithConn
	closed  chan struct{}
	stopper *stop.Stopper
}

// NewLocalRPCTransport creates a RaftTransport for local testing use. Stores
// sharing the same local Transport can find and communicate with each other by ID (which
// can be an arbitrary string). Each instance binds to a different unused port on
// localhost.
// Because this is just for local testing, it doesn't use TLS.
// TODO(bdarnell): can we get rid of LocalRPCTransport?
func NewLocalRPCTransport(stopper *stop.Stopper) RaftTransport {
	return &localRPCTransport{
		servers: make(map[roachpb.StoreID]net.Addr),
		clients: make(map[roachpb.StoreID]clientWithConn),
		closed:  make(chan struct{}),
		stopper: stopper,
	}
}

// RaftMessage implements the generated gRPC server interface.
func (handler RaftMessageHandler) RaftMessage(stream MultiRaft_RaftMessageServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if err := handler(req); err != nil {
			return err
		}
	}
}

func (lt *localRPCTransport) Listen(id roachpb.StoreID, handler RaftMessageHandler) error {
	grpcServer := grpc.NewServer()
	RegisterMultiRaftServer(grpcServer, handler)

	addr := util.CreateTestAddr("tcp")
	ln, err := grpcutil.ListenAndServeGRPC(lt.stopper, grpcServer, addr, nil)
	if err != nil {
		return err
	}

	lt.mu.Lock()
	if _, ok := lt.servers[id]; ok {
		log.Fatalf("node %d already listening", id)
	}
	lt.servers[id] = ln.Addr()
	lt.mu.Unlock()

	return nil
}

func (lt *localRPCTransport) Stop(id roachpb.StoreID) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	delete(lt.servers, id)
	if clientConn, ok := lt.clients[id]; ok {
		if err := clientConn.conn.Close(); err != nil {
			log.Warningf("error stopping client: %s", err)
		}

		delete(lt.clients, id)
	}
}

func (lt *localRPCTransport) getClient(id roachpb.StoreID) (MultiRaftClient, error) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	select {
	case <-lt.closed:
		return nil, util.Errorf("transport is closed")
	default:
	}

	if clientConn, ok := lt.clients[id]; ok {
		return clientConn.client, nil
	}

	addr, ok := lt.servers[id]
	if !ok {
		return nil, util.Errorf("unknown peer %v", id)
	}

	// If this wasn't test code we wouldn't want to call Dial while holding the lock.
	conn, err := grpc.Dial(addr.String(), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := NewMultiRaftClient(conn)
	lt.clients[id] = clientWithConn{conn: conn, client: client}
	return client, nil
}

func (lt *localRPCTransport) Send(req *RaftMessageRequest) error {
	client, err := lt.getClient(req.ToReplica.StoreID)
	if err != nil {
		return err
	}
	stream, err := client.RaftMessage(grpcutil.NewContextWithStopper(context.Background(), lt.stopper))
	if err != nil {
		return err
	}
	return stream.Send(req)
}

func (lt *localRPCTransport) Close() {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	close(lt.closed)
	for _, clientConn := range lt.clients {
		if err := clientConn.conn.Close(); err != nil {
			log.Warningf("error stopping client: %s", err)
		}
	}
}
