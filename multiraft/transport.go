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
	netrpc "net/rpc"
	"sync"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/roachpb"
	crpc "github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/rpc/codec"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

// The Transport interface is supplied by the application to manage communication with
// other nodes. It is responsible for mapping from IDs to some communication channel.
type Transport interface {
	// Listen informs the Transport of a local store's ID and callback interface.
	// The Transport should associate the given id with the server object so other Transport's
	// Connect methods can find it.
	Listen(id roachpb.StoreID, server ServerInterface) error

	// Stop undoes a previous Listen.
	Stop(id roachpb.StoreID)

	// Send a message to the node specified in the request's To field.
	Send(req *RaftMessageRequest) error

	// Close all associated connections.
	Close()
}

// ServerInterface is the methods we expose for use by net/rpc.
type ServerInterface interface {
	RaftMessage(req *RaftMessageRequest) (*RaftMessageResponse, error)
}

var (
	raftMessageName = "MultiRaft.RaftMessage"
)

type localRPCTransport struct {
	mu      sync.Mutex
	servers map[roachpb.StoreID]*crpc.Server
	clients map[roachpb.StoreID]*netrpc.Client
	conns   map[net.Conn]struct{}
	closed  chan struct{}
	stopper *stop.Stopper
}

// NewLocalRPCTransport creates a Transport for local testing use. MultiRaft instances
// sharing the same local Transport can find and communicate with each other by ID (which
// can be an arbitrary string). Each instance binds to a different unused port on
// localhost.
// Because this is just for local testing, it doesn't use TLS.
func NewLocalRPCTransport(stopper *stop.Stopper) Transport {
	return &localRPCTransport{
		servers: make(map[roachpb.StoreID]*crpc.Server),
		clients: make(map[roachpb.StoreID]*netrpc.Client),
		conns:   make(map[net.Conn]struct{}),
		closed:  make(chan struct{}),
		stopper: stopper,
	}
}

func (lt *localRPCTransport) Listen(id roachpb.StoreID, server ServerInterface) error {
	addr := util.CreateTestAddr("tcp")
	rpcServer := crpc.NewServer(addr, &crpc.Context{
		Context: base.Context{
			Insecure: true,
		},
		Stopper:      lt.stopper,
		DisableCache: true,
	})
	err := rpcServer.RegisterAsync(raftMessageName, false, /*not public*/
		func(argsI proto.Message, callback func(proto.Message, error)) {
			args := argsI.(*RaftMessageRequest)
			resp, err := server.RaftMessage(args)
			callback(resp, err)
		}, &RaftMessageRequest{})
	if err != nil {
		return err
	}

	lt.mu.Lock()
	if _, ok := lt.servers[id]; ok {
		log.Fatalf("node %d already listening", id)
	}
	lt.servers[id] = rpcServer
	lt.mu.Unlock()

	return rpcServer.Start()
}

func (lt *localRPCTransport) Stop(id roachpb.StoreID) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.servers[id].Close()
	delete(lt.servers, id)
	if client, ok := lt.clients[id]; ok {
		client.Close()
		delete(lt.clients, id)
	}
}

func (lt *localRPCTransport) getClient(id roachpb.StoreID) (*netrpc.Client, error) {
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

	server, ok := lt.servers[id]
	if !ok {
		return nil, util.Errorf("unknown peer %v", id)
	}
	address := server.Addr().String()

	// If this wasn't test code we wouldn't want to call Dial while holding the lock.
	conn, err := codec.TLSDialHTTP("tcp", address, base.NetworkTimeout, nil)
	if err != nil {
		return nil, err
	}
	client = netrpc.NewClientWithCodec(codec.NewClientCodec(conn))
	lt.clients[id] = client
	return client, err
}

func (lt *localRPCTransport) Send(req *RaftMessageRequest) error {
	client, err := lt.getClient(req.ToReplica.StoreID)
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
	for _, s := range lt.servers {
		s.Close()
	}
}
