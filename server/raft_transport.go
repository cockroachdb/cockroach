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
// permissions and limitations under the License.
//
// Author: Timothy Chen
// Author: Ben Darnell

package server

import (
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/gogo/protobuf/proto"

	gorpc "net/rpc"
)

const (
	// TODO(bdarnell): consider changing raftServiceName/raftMessageName
	raftServiceName = "MultiRaft"
	raftMessageName = raftServiceName + ".RaftMessage"
	// Outgoing messages are queued on a per-node basis on a channel of
	// this size.
	raftSendBufferSize = 500
	// When no message has been sent to a Node for that duration, the
	// corresponding instance of processQueue will shut down.
	raftIdleTimeout = time.Minute
)

// rpcTransport handles the rpc messages for raft.
type rpcTransport struct {
	gossip     *gossip.Gossip
	rpcServer  *rpc.Server
	rpcContext *rpc.Context
	mu         sync.Mutex
	handlers   map[roachpb.StoreID]storage.RaftMessageHandler
	queues     map[roachpb.StoreID]chan *storage.RaftMessageRequest
}

// newRPCTransport creates a new rpcTransport with specified gossip and rpc server.
func newRPCTransport(gossip *gossip.Gossip, rpcServer *rpc.Server, rpcContext *rpc.Context) (
	storage.RaftTransport, error) {
	t := &rpcTransport{
		gossip:     gossip,
		rpcServer:  rpcServer,
		rpcContext: rpcContext,
		handlers:   make(map[roachpb.StoreID]storage.RaftMessageHandler),
		queues:     make(map[roachpb.StoreID]chan *storage.RaftMessageRequest),
	}

	if t.rpcServer != nil {
		if err := t.rpcServer.RegisterAsync(raftMessageName, false, /*not public*/
			t.RaftMessage, &storage.RaftMessageRequest{}); err != nil {
			return nil, err
		}
	}

	return t, nil
}

// RaftMessage proxies the incoming request to the listening server interface.
func (t *rpcTransport) RaftMessage(args proto.Message, callback func(proto.Message, error)) {
	req := args.(*storage.RaftMessageRequest)

	t.mu.Lock()
	handler, ok := t.handlers[req.ToReplica.StoreID]
	t.mu.Unlock()

	if !ok {
		callback(nil, util.Errorf("Unable to proxy message to node: %d", req.Message.To))
		return
	}

	// Raft responses are empty so we don't actually need to get a
	// response from the handler. In fact, we don't even need to wait
	// for the message to be processed to invoke the callback. We are
	// just (ab)using the async handler mechanism to get this
	// (synchronous) handler called in the RPC server's goroutine so we
	// can preserve order of incoming messages.
	err := handler(req)
	callback(&storage.RaftMessageResponse{}, err)
}

// Listen implements the storage.RaftTransport interface by
// registering a RaftMessageHandler to receive proxied messages.
func (t *rpcTransport) Listen(id roachpb.StoreID, handler storage.RaftMessageHandler) error {
	t.mu.Lock()
	t.handlers[id] = handler
	t.mu.Unlock()
	return nil
}

// Stop implements the storage.RaftTransport interface by unregistering the server id.
func (t *rpcTransport) Stop(id roachpb.StoreID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.handlers, id)
}

// processQueue creates a client and sends messages from its designated queue
// via that client, exiting when the client fails or when it idles out. All
// messages remaining in the queue at that point are lost and a new instance of
// processQueue should be started by the next message to be sent.
// TODO(tschottdorf) should let raft know if the node is down;
// need a feedback mechanism for that. Potentially easiest is to arrange for
// the next call to Send() to fail appropriately.
func (t *rpcTransport) processQueue(nodeID roachpb.NodeID, storeID roachpb.StoreID) {
	t.mu.Lock()
	ch, ok := t.queues[storeID]
	t.mu.Unlock()
	if !ok {
		return
	}
	// Clean-up when the loop below shuts down.
	defer func() {
		t.mu.Lock()
		delete(t.queues, storeID)
		t.mu.Unlock()
	}()

	addr, err := t.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		if log.V(1) {
			log.Errorf("could not get address for node %d: %s", nodeID, err)
		}
		return
	}
	client := rpc.NewClient(addr, t.rpcContext)
	select {
	case <-t.rpcContext.Stopper.ShouldStop():
		return
	case <-client.Closed:
		log.Warningf("raft client for node %d was closed", nodeID)
		return
	case <-time.After(raftIdleTimeout):
		// Should never happen.
		log.Errorf("raft client for node %d stuck connecting", nodeID)
		return
	case <-client.Healthy():
	}

	done := make(chan *gorpc.Call, cap(ch))
	var req *storage.RaftMessageRequest
	protoResp := &storage.RaftMessageResponse{}
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

		client.Go(raftMessageName, req, protoResp, done)
	}
}

// Send a message to the recipient specified in the request.
func (t *rpcTransport) Send(req *storage.RaftMessageRequest) error {
	t.mu.Lock()
	ch, ok := t.queues[req.ToReplica.StoreID]
	if !ok {
		ch = make(chan *storage.RaftMessageRequest, raftSendBufferSize)
		t.queues[req.ToReplica.StoreID] = ch
		go t.processQueue(req.ToReplica.NodeID, req.ToReplica.StoreID)
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
