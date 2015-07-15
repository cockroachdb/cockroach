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
	gogoproto "github.com/gogo/protobuf/proto"

	gorpc "net/rpc"
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

	if t.rpcServer != nil {
		if err := t.rpcServer.RegisterAsync(raftMessageName, t.RaftMessage,
			&proto.RaftMessageRequest{}); err != nil {
			return nil, err
		}
	}

	return t, nil
}

// RaftMessage proxies the incoming request to the listening server interface.
func (t *rpcTransport) RaftMessage(args gogoproto.Message, callback func(gogoproto.Message, error)) {
	protoReq := args.(*proto.RaftMessageRequest)
	// Convert from proto to internal formats.
	req := &multiraft.RaftMessageRequest{GroupID: protoReq.GroupID}
	if err := req.Message.Unmarshal(protoReq.Msg); err != nil {
		callback(nil, err)
		return
	}

	t.mu.Lock()
	server, ok := t.servers[proto.RaftNodeID(req.Message.To)]
	t.mu.Unlock()

	if !ok {
		callback(nil, util.Errorf("Unable to proxy message to node: %d", req.Message.To))
		return
	}

	// Raft responses are empty so we don't actually need to convert
	// between multiraft's internal struct and the external proto
	// representation.  In fact, we don't even need to wait for the
	// message to be processed to invoke the callback. We are just
	// (ab)using the async handler mechanism to get this (synchronous)
	// handler called in the RPC server's goroutine so we can preserve
	// order of incoming messages.
	err := server.RaftMessage(req, &multiraft.RaftMessageResponse{})
	callback(&proto.RaftMessageResponse{}, err)
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

// processQueue creates a client and sends messages from its designated queue
// via that client, exiting when the client fails or when it idles out. All
// messages remaining in the queue at that point are lost and a new instance of
// processQueue should be started by the next message to be sent.
// TODO(tschottdorf) should let MultiRaft know if the node is down;
// need a feedback mechanism for that. Potentially easiest is to arrange for
// the next call to Send() to fail appropriately.
func (t *rpcTransport) processQueue(raftNodeID proto.RaftNodeID) {
	t.mu.Lock()
	ch, ok := t.queues[raftNodeID]
	t.mu.Unlock()
	if !ok {
		return
	}
	// Clean-up when the loop below shuts down.
	defer func() {
		t.mu.Lock()
		delete(t.queues, raftNodeID)
		t.mu.Unlock()
	}()

	nodeID, _ := proto.DecodeRaftNodeID(raftNodeID)
	addr, err := t.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		log.Errorf("could not get address for node %d: %s", nodeID, err)
		return
	}
	client := rpc.NewClient(addr, nil, t.rpcContext)
	select {
	case <-t.rpcContext.Stopper.ShouldStop():
		return
	case <-client.Closed:
		log.Warningf("raft client for node %d failed to connect", nodeID)
		return
	case <-time.After(raftIdleTimeout):
		// Should never happen.
		log.Errorf("raft client for node %d stuck connecting", nodeID)
		return
	case <-client.Ready:
	}

	done := make(chan *gorpc.Call, cap(ch))
	var req *multiraft.RaftMessageRequest
	protoReq := &proto.RaftMessageRequest{}
	protoResp := &proto.RaftMessageResponse{}
	for {
		select {
		case <-t.rpcContext.Stopper.ShouldStop():
			return
		case <-time.After(raftIdleTimeout):
			if log.V(1) {
				log.Infof("closing Raft transport to %d due to inactivity", nodeID)
			}
			return
		case <-client.Closed:
			log.Warningf("raft client for node %d closed", nodeID)
			return
		case call := <-done:
			if call.Error != nil {
				log.Errorf("raft message to node %d failed: %s", nodeID, call.Error)
			}
			continue
		case req = <-ch:
		}
		if req == nil {
			return
		}

		// Convert to proto format.
		protoReq.Reset()
		protoReq.GroupID = req.GroupID
		var err error
		if protoReq.Msg, err = req.Message.Marshal(); err != nil {
			log.Errorf("could not marshal message: %s", err)
			continue
		}

		if !client.IsHealthy() {
			log.Warningf("raft client for node %d unhealthy", nodeID)
			return
		}
		client.Go(raftMessageName, protoReq, protoResp, done)

		// TODO(tschottdorf): work around #1176 by wasting just a little
		// bit of time before moving to the next request.
		select {
		case <-done:
		case <-time.After(10 * time.Millisecond):
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
