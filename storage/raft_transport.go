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
// Author: Tamir Duberstein (tamird@gmail.com)

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

// GossipAddressResolver is a thin wrapper around gossip's GetNodeIDAddress
// that allows its return value to be used as the net.Addr interface.
func GossipAddressResolver(gossip *gossip.Gossip) NodeAddressResolver {
	return func(nodeID roachpb.NodeID) (net.Addr, error) {
		return gossip.GetNodeIDAddress(nodeID)
	}
}

type cachedStream struct {
	stream MultiRaft_RaftMessageClient
	cancel func()

	once sync.Once
	init func()
	err  error
}

// RaftTransport handles the rpc messages for raft.
type RaftTransport struct {
	resolver   NodeAddressResolver
	rpcContext *rpc.Context

	mu struct {
		sync.Mutex
		handlers map[roachpb.StoreID]raftMessageHandler
		// streams' value type must be a pointer to allow for lazy (mutating)
		// initialization.
		streams map[roachpb.NodeID]*cachedStream
	}
}

// NewDummyRaftTransport returns a dummy raft transport for use in tests which
// need a non-nil raft transport that need not function.
func NewDummyRaftTransport() *RaftTransport {
	return NewRaftTransport(nil, nil, nil)
}

// NewRaftTransport creates a new RaftTransport with specified resolver and grpc server.
func NewRaftTransport(resolver NodeAddressResolver, grpcServer *grpc.Server, rpcContext *rpc.Context) *RaftTransport {
	t := &RaftTransport{
		resolver:   resolver,
		rpcContext: rpcContext,
	}
	t.mu.handlers = make(map[roachpb.StoreID]raftMessageHandler)
	t.mu.streams = make(map[roachpb.NodeID]*cachedStream)

	if grpcServer != nil {
		RegisterMultiRaftServer(grpcServer, t)
	}

	return t
}

// RaftMessage proxies the incoming request to the listening server interface.
func (t *RaftTransport) RaftMessage(stream MultiRaft_RaftMessageServer) (err error) {
	errCh := make(chan error, 1)

	t.rpcContext.Stopper.RunTask(func() {
		t.rpcContext.Stopper.RunWorker(func() {
			errCh <- func() error {
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
			}()
		})
	})

	select {
	case err := <-errCh:
		return err
	case <-t.rpcContext.Stopper.ShouldDrain():
		return stream.SendAndClose(new(RaftMessageResponse))
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

func (t *RaftTransport) invalidateStream(nodeID roachpb.NodeID) {
	t.mu.Lock()
	if cs, ok := t.mu.streams[nodeID]; ok {
		cs.cancel()
		delete(t.mu.streams, nodeID)
	}
	t.mu.Unlock()
}

func (t *RaftTransport) newStream(nodeID roachpb.NodeID) *cachedStream {
	var cs cachedStream
	cs.init = func() {
		var addr net.Addr
		if addr, cs.err = t.resolver(nodeID); cs.err != nil {
			return
		}

		var conn *grpc.ClientConn
		if conn, cs.err = t.rpcContext.GRPCDial(addr.String()); cs.err != nil {
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		cs.cancel = func() {
			cancel()
			if err := conn.Close(); err != nil {
				log.Warningf("raft transport failed to close grpc connection: %s", err)
			}
		}

		client := NewMultiRaftClient(conn)
		if cs.stream, cs.err = client.RaftMessage(ctx); cs.err != nil {
			return
		}

		go func() {
			var response RaftMessageResponse

			if err := cs.stream.RecvMsg(&response); err != nil {
				log.Errorf("raft transport received grpc error: %s; invalidating connection to: %s", err, addr)
			}

			t.invalidateStream(nodeID)
		}()
	}

	return &cs
}

func (t *RaftTransport) getStream(nodeID roachpb.NodeID) (MultiRaft_RaftMessageClient, error) {
	t.mu.Lock()
	cs, ok := t.mu.streams[nodeID]
	if !ok {
		cs = t.newStream(nodeID)
		t.mu.streams[nodeID] = cs
	}
	t.mu.Unlock()

	cs.once.Do(cs.init)
	if cs.err != nil {
		t.invalidateStream(nodeID)
	}

	return cs.stream, cs.err
}

func (t *RaftTransport) send(req *RaftMessageRequest) error {
	stream, err := t.getStream(req.ToReplica.NodeID)
	if err != nil {
		return err
	}
	return stream.Send(req)
}

// Send a message to the recipient specified in the request.
func (t *RaftTransport) Send(req *RaftMessageRequest) error {
	var err error
	if !t.rpcContext.Stopper.RunTask(func() { err = t.send(req) }) {
		err = util.Errorf("node stopped")
	}
	return err
}
