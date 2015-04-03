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
// Author: Timothy Chen
// Author: Ben Darnell

package server

import (
	"sync"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	raftServiceName = "MultiRaft"
	raftMessageName = raftServiceName + ".RaftMessage"
)

// rpcTransport handles the rpc messages for multiraft.
type rpcTransport struct {
	gossip     *gossip.Gossip
	rpcServer  *rpc.Server
	rpcContext *rpc.Context
	mu         sync.Mutex
	servers    map[multiraft.NodeID]multiraft.ServerInterface
}

// newRPCTransport creates a new rpcTransport with specified gossip and rpc server.
func newRPCTransport(gossip *gossip.Gossip, rpcServer *rpc.Server, rpcContext *rpc.Context) (
	multiraft.Transport, error) {
	t := &rpcTransport{
		gossip:     gossip,
		rpcServer:  rpcServer,
		rpcContext: rpcContext,
		servers:    make(map[multiraft.NodeID]multiraft.ServerInterface),
	}

	err := t.rpcServer.RegisterName(raftServiceName, (*transportRPCServer)(t))
	if err != nil {
		return nil, err
	}

	return t, nil
}

// transportRPCServer is a type alias to separate RPC methods
// (which net/rpc finds via reflection) from others.
type transportRPCServer rpcTransport

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

// Listen implements the multiraft.Transport interface by registering a ServerInterface
// to receive proxied messages.
func (t *rpcTransport) Listen(id multiraft.NodeID, server multiraft.ServerInterface) error {
	t.mu.Lock()
	t.servers[id] = server
	t.mu.Unlock()
	return nil
}

// Stop implements the multiraft.Transport interface by unregistering the server id.
func (t *rpcTransport) Stop(id multiraft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.servers, id)
}

// Send a message to the specified Node id.
func (t *rpcTransport) Send(id multiraft.NodeID, req *multiraft.RaftMessageRequest) error {
	// Convert internal to proto formats.
	protoReq := &proto.RaftMessageRequest{GroupID: req.GroupID}
	var err error
	if protoReq.Msg, err = req.Message.Marshal(); err != nil {
		return err
	}

	nodeID, _ := storage.DecodeRaftNodeID(id)
	addr, err := storage.NodeIDToAddress(t.gossip, nodeID)
	if err != nil {
		return err
	}

	client := rpc.NewClient(addr, nil, t.rpcContext)
	select {
	case <-client.Ready:
	case <-client.Closed:
		return util.Errorf("raft client failed to connect")
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

// Close shuts down an rpcTransport.
func (t *rpcTransport) Close() {
	// No-op since we share the global cache of client connections.
}
