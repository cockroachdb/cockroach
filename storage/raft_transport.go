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
// Author: Timothy Chen

package storage

import (
	"net"
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

type raftMessageHandler func(*RaftMessageRequest) error

// NodeAddressResolver is the function used by RaftTransport to map node IDs to
// network addresses.
type NodeAddressResolver func(roachpb.NodeID) (net.Addr, error)

// GossipPolymorphismShim is a thin wrapper around gossip's GetNodeIDAddress
// that allows its return value to be used as the net.Addr interface.
func GossipPolymorphismShim(gossip *gossip.Gossip) NodeAddressResolver {
	return func(nodeID roachpb.NodeID) (net.Addr, error) {
		return gossip.GetNodeIDAddress(nodeID)
	}
}

type streamWithCancel struct {
	stream MultiRaft_RaftMessageClient
	cancel func()
}

// RaftTransport handles the rpc messages for raft.
type RaftTransport struct {
	resolver   NodeAddressResolver
	rpcContext *rpc.Context

	mu struct {
		sync.RWMutex
		handlers map[roachpb.StoreID]raftMessageHandler
		streams  map[roachpb.NodeID]streamWithCancel
	}
}

// NewRaftTransport creates a new RaftTransport with specified resolver and grpc server.
func NewRaftTransport(resolver NodeAddressResolver, grpcServer *grpc.Server, rpcContext *rpc.Context) *RaftTransport {
	t := &RaftTransport{
		resolver:   resolver,
		rpcContext: rpcContext,
	}
	t.mu.handlers = make(map[roachpb.StoreID]raftMessageHandler)
	t.mu.streams = make(map[roachpb.NodeID]streamWithCancel)

	if grpcServer != nil {
		RegisterMultiRaftServer(grpcServer, t)
	}

	return t
}

// RaftMessage proxies the incoming request to the listening server interface.
func (t *RaftTransport) RaftMessage(stream MultiRaft_RaftMessageServer) (err error) {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		t.mu.Lock()
		handler, ok := t.mu.handlers[req.ToReplica.StoreID]
		t.mu.Unlock()

		if !ok {
			return util.Errorf("Unable to proxy message to node: %d", req.Message.To)
		}

		if err := handler(req); err != nil {
			return err
		}
	}
}

// Listen registers a raftMessageHandler to receive proxied messages.
func (t *RaftTransport) Listen(storeID roachpb.StoreID, handler raftMessageHandler) {
	t.mu.Lock()
	t.mu.handlers[storeID] = handler
	t.mu.Unlock()
}

// Stop unregisters a raftMessageHandler.
func (t *RaftTransport) Stop(storeID roachpb.StoreID) {
	t.mu.Lock()
	delete(t.mu.handlers, storeID)
	t.mu.Unlock()
}

func (t *RaftTransport) getStream(nodeID roachpb.NodeID) (MultiRaft_RaftMessageClient, error) {
	t.mu.RLock()
	swc, ok := t.mu.streams[nodeID]
	t.mu.RUnlock()
	if ok {
		return swc.stream, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	addr, err := t.resolver(nodeID)
	if err != nil {
		return nil, err
	}

	conn, err := t.rpcContext.GRPCDial(addr.String())
	if err != nil {
		return nil, err
	}

	client := NewMultiRaftClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.RaftMessage(ctx)
	if err != nil {
		return nil, err
	}

	t.mu.streams[nodeID] = streamWithCancel{
		stream: stream,
		cancel: func() {
			cancel()
			if err := conn.Close(); err != nil {
				log.Warning(err)
			}
		},
	}

	go func() {
		var response RaftMessageResponse

		if err := stream.RecvMsg(&response); err != nil {
			log.Errorf("stream closed with: %s", err)
		}

		t.mu.Lock()
		if swc, ok := t.mu.streams[nodeID]; ok {
			swc.cancel()
			delete(t.mu.streams, nodeID)
		}
		t.mu.Unlock()
	}()

	return stream, nil
}

// Send a message to the recipient specified in the request.
func (t *RaftTransport) Send(req *RaftMessageRequest) error {
	stream, err := t.getStream(req.ToReplica.NodeID)
	if err != nil {
		return err
	}
	return stream.Send(req)
}
