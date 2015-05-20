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
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	raftServiceName = "MultiRaft"
	raftMessageName = raftServiceName + ".RaftMessage"
	// Outgoing messages are queued on a per-node basis on a channel of
	// this size.
	raftSendBufferSize = 500
	// When no message has been sent to a Node for that duration, the
	// corresponding instance of processQueue will shut down.
	raftIdleTimeout = time.Minute
)

// raftMessageTimeout specifies the acceptable wait time for a single
// message to a node to succeed.
// It is a variable to adapt it for race testing on slow machines.
var raftMessageTimeout = 1 * time.Second

// rpcTransport handles the rpc messages for multiraft.
type rpcTransport struct {
	gossip     *gossip.Gossip
	rpcServer  *rpc.Server
	rpcContext *rpc.Context
	mu         sync.Mutex
	servers    map[proto.RaftNodeID]multiraft.ServerInterface
	queues     map[proto.RaftNodeID]chan *multiraft.RaftMessageRequest
}

// newRPCTransport creates a new rpcTransport with specified gossip and rpc server.
func newRPCTransport(gossip *gossip.Gossip, rpcServer *rpc.Server, rpcContext *rpc.Context) (
	multiraft.Transport, error) {
	t := &rpcTransport{
		gossip:     gossip,
		rpcServer:  rpcServer,
		rpcContext: rpcContext,
		servers:    make(map[proto.RaftNodeID]multiraft.ServerInterface),
		queues:     make(map[proto.RaftNodeID]chan *multiraft.RaftMessageRequest),
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
	server, ok := t.servers[proto.RaftNodeID(req.Message.To)]
	t.mu.Unlock()

	if ok {
		return server.RaftMessage(req, &multiraft.RaftMessageResponse{})
	}

	return util.Errorf("Unable to proxy message to node: %d", req.Message.To)
}

// Listen implements the multiraft.Transport interface by registering a ServerInterface
// to receive proxied messages.
func (t *rpcTransport) Listen(id proto.RaftNodeID, server multiraft.ServerInterface) error {
	t.mu.Lock()
	t.servers[id] = server
	t.mu.Unlock()
	return nil
}

// Stop implements the multiraft.Transport interface by unregistering the server id.
func (t *rpcTransport) Stop(id proto.RaftNodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.servers, id)
}

func (t *rpcTransport) processQueue(nodeID proto.RaftNodeID) {
	t.mu.Lock()
	ch, ok := t.queues[nodeID]
	t.mu.Unlock()
	if !ok {
		return
	}
	var req *multiraft.RaftMessageRequest
	protoReq := &proto.RaftMessageRequest{}
	for {
		select {
		case <-t.rpcContext.Stopper.ShouldStop():
			return
		case req = <-ch:
		}
		if req == nil {
			continue
		}

		// Convert to proto format.
		protoReq.Reset()
		protoReq.GroupID = req.GroupID
		var err error
		if protoReq.Msg, err = req.Message.Marshal(); err != nil {
			log.Errorf("could not marshal message: %s", err)
		}

		nodeID, _ := proto.DecodeRaftNodeID(proto.RaftNodeID(req.Message.To))
		addr, err := t.gossip.GetNodeIDAddress(nodeID)
		if err != nil {
			log.Errorf("could not get address for node %d: %s", nodeID, err)
		}

		deadline := time.After(raftMessageTimeout)
		client := rpc.NewClient(addr, nil, t.rpcContext)
		// TODO(tschottdorf) should let MultiRaft know that the node is gone;
		// need a feedback mechanism for that.
		// Potentially easiest is to arrange for the next call to Send() to
		// fail appropriately.
		select {
		case <-t.rpcContext.Stopper.ShouldStop():
			return
		case <-client.Closed:
			log.Errorf("raft client failed to connect")
			continue
		case <-deadline:
			log.Errorf("call timed out after %s", raftMessageTimeout)
			continue
		case <-client.Ready:
		}

		call := client.Go(raftMessageName, protoReq, &proto.RaftMessageResponse{}, nil)
		select {
		case <-t.rpcContext.Stopper.ShouldStop():
			return
		case <-client.Closed:
			log.Errorf("raft client failed to connect")
			continue
		case <-deadline:
			log.Errorf("call timed out after %s", raftMessageTimeout)
			continue
		case <-call.Done:
		}
		if call.Error != nil {
			log.Errorf("raft message failed: %s", call.Error)
		}
	}
}

// Send a message to the recipient specified in the request.
func (t *rpcTransport) Send(req *multiraft.RaftMessageRequest) error {
	raftNodeID := proto.RaftNodeID(req.Message.To)
	t.mu.Lock()
	ch, ok := t.queues[raftNodeID]
	if !ok {
		ch = make(chan *multiraft.RaftMessageRequest, raftSendBufferSize)
		t.queues[raftNodeID] = ch
		go t.processQueue(raftNodeID)
	}
	t.mu.Unlock()

	select {
	case ch <- req:
	default:
		return util.Errorf("queue for node %d is full", req.Message.To)
	}
	return nil
}

// Close shuts down an rpcTransport.
func (t *rpcTransport) Close() {
	// No-op since we share the global cache of client connections.
}
