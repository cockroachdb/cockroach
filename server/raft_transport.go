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
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"

	gorpc "net/rpc"
)

const (
	raftServiceName         = "MultiRaft"
	raftMessageName         = raftServiceName + ".RaftMessage"
	multiPackageMessageName = raftServiceName + ".MultiPackageMessage"

	// Outgoing messages are queued on a per-node basis on a channel of
	// this size.
	raftSendBufferSize = 500
	// When no message has been sent to a Node for that duration, the
	// corresponding instance of processQueue will shut down.
	raftIdleTimeout = time.Minute

	// When send snapshot, the maximum package size can not execeed 1M.
	maximumPackageSize = 1024 * 1024

	// Last package id
	lastPackageID = -1
)

// rpcTransport handles the rpc messages for multiraft.
type rpcTransport struct {
	gossip     *gossip.Gossip
	rpcServer  *rpc.Server
	rpcContext *rpc.Context
	mu         sync.Mutex
	servers    map[roachpb.StoreID]multiraft.ServerInterface
	queues     map[roachpb.StoreID]chan *multiraft.RaftMessageRequest

	multiPackageMessageLock sync.Mutex
	multiPackageMessage     map[roachpb.RangeID]*[]byte
}

// newRPCTransport creates a new rpcTransport with specified gossip and rpc server.
func newRPCTransport(gossip *gossip.Gossip, rpcServer *rpc.Server, rpcContext *rpc.Context) (
	multiraft.Transport, error) {
	t := &rpcTransport{
		gossip:              gossip,
		rpcServer:           rpcServer,
		rpcContext:          rpcContext,
		servers:             make(map[roachpb.StoreID]multiraft.ServerInterface),
		queues:              make(map[roachpb.StoreID]chan *multiraft.RaftMessageRequest),
		multiPackageMessage: make(map[roachpb.RangeID]*[]byte),
	}

	if t.rpcServer != nil {
		if err := t.rpcServer.RegisterAsync(raftMessageName, false, /*not public*/
			t.RaftMessage, &multiraft.RaftMessageRequest{}); err != nil {
			return nil, err
		}

		if err := t.rpcServer.RegisterAsync(multiPackageMessageName, false, /*not public*/
			t.MultiPackageMessage, &multiraft.MultiPackageMessageRequest{}); err != nil {
			return nil, err
		}
	}

	return t, nil
}

// MultiPackageMessage merges incoming multi-package request into the real request,
// and proxies to the listening server interface.
func (t *rpcTransport) MultiPackageMessage(args proto.Message, callback func(proto.Message, error)) {
	req := args.(*multiraft.MultiPackageMessageRequest)

	t.multiPackageMessageLock.Lock()
	_, ok := t.multiPackageMessage[req.GroupID]
	if !ok {
		t.multiPackageMessage[req.GroupID] = &[]byte{}
	}
	data := t.multiPackageMessage[req.GroupID]
	*data = append(*data, req.RawBytes...)

	// Not receive all the packages, return and wait for next package.
	if req.PackageId != lastPackageID {
		t.multiPackageMessageLock.Unlock()
		callback(nil, nil)
		return
	}

	delete(t.multiPackageMessage, req.GroupID)
	t.multiPackageMessageLock.Unlock()

	// Now, we get all the packages. Unmasharl it and get the real message.
	var msg multiraft.RaftMessageRequest
	err := proto.Unmarshal(*data, &msg)
	if err != nil {
		callback(nil, util.Errorf("Unable to unmarshal multi-package message of group: %d", req.GroupID))
		return
	}

	t.RaftMessage(&msg, callback)
}

// RaftMessage proxies the incoming request to the listening server interface.
func (t *rpcTransport) RaftMessage(args proto.Message, callback func(proto.Message, error)) {
	req := args.(*multiraft.RaftMessageRequest)

	t.mu.Lock()
	server, ok := t.servers[req.ToReplica.StoreID]
	t.mu.Unlock()

	if !ok {
		callback(nil, util.Errorf("Unable to proxy message to node: %d", req.Message.To))
		return
	}

	// Raft responses are empty so we don't actually need to convert
	// between multiraft's internal struct and the external proto
	// representation. In fact, we don't even need to wait for the
	// message to be processed to invoke the callback. We are just
	// (ab)using the async handler mechanism to get this (synchronous)
	// handler called in the RPC server's goroutine so we can preserve
	// order of incoming messages.
	resp, err := server.RaftMessage(req)
	callback(resp, err)
}

// Listen implements the multiraft.Transport interface by registering a ServerInterface
// to receive proxied messages.
func (t *rpcTransport) Listen(id roachpb.StoreID, server multiraft.ServerInterface) error {
	t.mu.Lock()
	t.servers[id] = server
	t.mu.Unlock()
	return nil
}

// Stop implements the multiraft.Transport interface by unregistering the server id.
func (t *rpcTransport) Stop(id roachpb.StoreID) {
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
	var req *multiraft.RaftMessageRequest
	protoResp := &multiraft.RaftMessageResponse{}
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
func (t *rpcTransport) Send(req *multiraft.RaftMessageRequest) error {
	if req.Message.Type == raftpb.MsgSnap {
		go t.sendSnapshot(req)
		return nil
	}

	t.mu.Lock()
	ch, ok := t.queues[req.ToReplica.StoreID]
	if !ok {
		ch = make(chan *multiraft.RaftMessageRequest, raftSendBufferSize)
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

// sendSnapshot create a new standalone rpc client without cache to send
// snapshot to avoid block raft heartbeat message. But snapshot may be
// very large or network is slow, lead to rpc transmission time over rpc
// heartbeat timeout. So snapshot data is divided into small packets
// that not exceed "maximumPackageSize". Receiver will buffer all the packages
// and merge them to get the real request message.
func (t *rpcTransport) sendSnapshot(req *multiraft.RaftMessageRequest) {
	nodeID := req.ToReplica.NodeID
	addr, err := t.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		log.Errorf("could not get address for node %d: %s", nodeID, err)
		return
	}

	// start a new connection to send snapshot alone.
	ctx := t.rpcContext.Copy()
	ctx.DisableCache = true
	client := rpc.NewClient(addr, ctx)
	defer client.Close()

	// Encode the whole RaftMessageRequest,
	// calculate the number of packages to send.
	data, err := req.Marshal()
	dataSize := len(data)
	inteval := dataSize / maximumPackageSize
	reminder := dataSize % maximumPackageSize

	for i := 0; i < inteval+1; i++ {
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
		case <-client.Healthy():
		}

		var mreq multiraft.MultiPackageMessageRequest
		mreq.GroupID = req.GroupID
		mreq.PackageId = int32(i)

		startIndex := i * maximumPackageSize
		endIndex := (i + 1) * maximumPackageSize

		// The last package, mark PackageId "-1".
		if i == inteval && reminder != 0 {
			endIndex = startIndex + reminder
			mreq.PackageId = int32(lastPackageID)
		}

		mreq.RawBytes = data[startIndex:endIndex]
		protoResp := &multiraft.RaftMessageResponse{}
		done := make(chan *gorpc.Call, 1)

		client.Go(multiPackageMessageName, &mreq, protoResp, done)
		call := <-done
		if call.Error != nil {
			log.Errorf("snapshot message to node %d failed: %s", nodeID, call.Error)
			return
		}
	}
}
