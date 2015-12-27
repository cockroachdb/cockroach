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

// TODO(bdarnell): remove/change raftMessageName
const raftMessageName = "MultiRaft.RaftMessage"

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

type serverWithAddr struct {
	server *crpc.Server
	addr   net.Addr
}

type localRPCTransport struct {
	mu      sync.Mutex
	servers map[roachpb.StoreID]serverWithAddr
	clients map[roachpb.StoreID]*netrpc.Client
	conns   map[net.Conn]struct{}
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
		servers: make(map[roachpb.StoreID]serverWithAddr),
		clients: make(map[roachpb.StoreID]*netrpc.Client),
		conns:   make(map[net.Conn]struct{}),
		closed:  make(chan struct{}),
		stopper: stopper,
	}
}

func (lt *localRPCTransport) Listen(id roachpb.StoreID, handler RaftMessageHandler) error {
	ctx := crpc.Context{
		Context: base.Context{
			Insecure: true,
		},
		Stopper:      lt.stopper,
		DisableCache: true,
	}
	rpcServer := crpc.NewServer(&ctx)
	err := rpcServer.RegisterAsync(raftMessageName, false, /*not public*/
		func(argsI proto.Message, callback func(proto.Message, error)) {
			defer func() {
				// TODO(bdarnell): the http/rpc code is swallowing panics somewhere.
				if p := recover(); p != nil {
					log.Fatalf("caught panic: %s", p)
				}
			}()
			args := argsI.(*RaftMessageRequest)
			err := handler(args)
			callback(&RaftMessageResponse{}, err)
		}, &RaftMessageRequest{})
	if err != nil {
		return err
	}

	tlsConfig, err := ctx.GetServerTLSConfig()
	if err != nil {
		return err
	}

	addr := util.CreateTestAddr("tcp")
	ln, err := util.ListenAndServe(ctx.Stopper, rpcServer, addr, tlsConfig)
	if err != nil {
		return err
	}

	lt.mu.Lock()
	if _, ok := lt.servers[id]; ok {
		log.Fatalf("node %d already listening", id)
	}
	lt.servers[id] = serverWithAddr{rpcServer, ln.Addr()}
	lt.mu.Unlock()

	return nil
}

func (lt *localRPCTransport) Stop(id roachpb.StoreID) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
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

	srvWithAddr, ok := lt.servers[id]
	if !ok {
		return nil, util.Errorf("unknown peer %v", id)
	}
	address := srvWithAddr.addr.String()

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
				log.Errorf("sending %s rpc from %s to %s failed: %s", req.Message.Type,
					req.FromReplica, req.ToReplica, call.Error)
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
