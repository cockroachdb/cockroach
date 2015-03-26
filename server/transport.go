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
// Author: Timothy Chen

package server

import (
	"net"
	"net/rpc"
	"sync"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	crpc "github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/rpc/codec"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	raftServiceName = "MultiRaft"
	raftMessageName = raftServiceName + ".RaftMessage"
)

// RPCTransport handles the rpc messages for multiraft.
type RPCTransport struct {
	gossip    *gossip.Gossip
	rpcServer *crpc.Server
	mu        sync.Mutex
	servers   map[multiraft.NodeID]multiraft.ServerInterface
	clients   map[multiraft.NodeID]*rpc.Client
}

// NewRPCTransport creates a new RPCTransport with existing gossip and rpc server.
func NewRPCTransport(gossip *gossip.Gossip, rpcServer *crpc.Server) (multiraft.Transport, error) {
	t := &RPCTransport{
		gossip:    gossip,
		rpcServer: rpcServer,
		servers:   make(map[multiraft.NodeID]multiraft.ServerInterface),
		clients:   make(map[multiraft.NodeID]*rpc.Client),
	}

	err := t.rpcServer.RegisterName("MultiRaft", (*transportRPCServer)(t))
	if err != nil {
		return nil, err
	}

	return t, nil
}

// transportServer is a type alias to separate RPC methods
// (which net/rpc finds via reflection) from others.
type transportRPCServer RPCTransport

// RaftMessage proxies the incoming request to the listening server interface.
func (t *transportRPCServer) RaftMessage(protoReq *proto.RaftMessageRequest,
	resp *proto.RaftMessageResponse) error {
	// Convert from proto to internal formats.
	req := &multiraft.RaftMessageRequest{GroupID: protoReq.GroupID}
	if err := req.Message.Unmarshal(protoReq.Msg); err != nil {
		return err
	}

	t.mu.Lock()
	server, ok := t.servers[multiraft.NodeID(req.Message.To)]
	t.mu.Unlock()

	if ok {
		return server.RaftMessage(req, &multiraft.RaftMessageResponse{})
	}

	return util.Errorf("Unable to proxy message to node: %d", req.Message.To)
}

// Listen registers a ServerInterface to be proxied messages.
func (t *RPCTransport) Listen(id multiraft.NodeID, server multiraft.ServerInterface) error {
	t.mu.Lock()
	t.servers[id] = server
	t.mu.Unlock()
	return nil
}

// Stop unregisters the server id from getting messages.
func (t *RPCTransport) Stop(id multiraft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.servers, id)
}

func (t *RPCTransport) getClient(id multiraft.NodeID) (*rpc.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	client, ok := t.clients[id]
	if ok {
		return client, nil
	}

	nodeIDKey := gossip.MakeNodeIDGossipKey(proto.NodeID(id))
	info, err := t.gossip.GetInfo(nodeIDKey)
	if info == nil || err != nil {
		return nil, util.Errorf("Unable to lookup address for node: %d. Error: %s", id, err)
	}
	address := info.(net.Addr)

	conn, err := net.Dial("tcp", address.String())
	if err != nil {
		return nil, err
	}
	client = rpc.NewClientWithCodec(codec.NewClientCodec(conn))

	t.clients[id] = client

	return client, err
}

// Send a message to the specified Node id.
func (t *RPCTransport) Send(id multiraft.NodeID, req *multiraft.RaftMessageRequest) error {
	// Convert internal to proto formats.
	protoReq := &proto.RaftMessageRequest{GroupID: req.GroupID}
	var err error
	if protoReq.Msg, err = req.Message.Marshal(); err != nil {
		return err
	}

	client, err := t.getClient(id)
	if err != nil {
		return err
	}
	call := client.Go(raftMessageName, protoReq, &proto.RaftMessageResponse{}, nil)
	select {
	case <-call.Done:
		// If the call failed synchronously, report an error.
		return call.Error
	default:
		// Otherwise, fire-and-forget.
		go func() {
			<-call.Done
			if call.Error != nil {
				log.Errorf("raft message failed: %s", call.Error)
			}
		}()
		return nil
	}
}

// Close all outgoing client connections.
func (t *RPCTransport) Close() {
	for _, c := range t.clients {
		err := c.Close()
		if err != nil {
			log.Warningf("error stopping client: %s", err)
		}
	}
}
