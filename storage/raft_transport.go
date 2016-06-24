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
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

const (
	// Outgoing messages are queued on a per-node basis on a channel of
	// this size.
	raftSendBufferSize = 500
	// When no message has been sent to a Node for that duration, the
	// corresponding instance of processQueue will shut down.
	raftIdleTimeout = time.Minute
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

// RaftSnapshotStatus contains a MsgSnap message and its resulting
// error, for asynchronous notification of completion.
type RaftSnapshotStatus struct {
	Req *RaftMessageRequest
	Err error
}

// RaftTransport handles the rpc messages for raft.
type RaftTransport struct {
	resolver           NodeAddressResolver
	rpcContext         *rpc.Context
	SnapshotStatusChan chan RaftSnapshotStatus

	mu struct {
		sync.Mutex
		handlers map[roachpb.StoreID]raftMessageHandler
		queues   map[bool]map[roachpb.NodeID]chan *RaftMessageRequest
	}
}

// NewDummyRaftTransport returns a dummy raft transport for use in tests which
// need a non-nil raft transport that need not function.
func NewDummyRaftTransport() *RaftTransport {
	return NewRaftTransport(nil, nil, nil)
}

// NewRaftTransport creates a new RaftTransport with specified resolver and grpc server.
// Callers are responsible for monitoring RaftTransport.SnapshotStatusChan.
func NewRaftTransport(resolver NodeAddressResolver, grpcServer *grpc.Server, rpcContext *rpc.Context) *RaftTransport {
	t := &RaftTransport{
		resolver:           resolver,
		rpcContext:         rpcContext,
		SnapshotStatusChan: make(chan RaftSnapshotStatus),
	}
	t.mu.handlers = make(map[roachpb.StoreID]raftMessageHandler)
	t.mu.queues = make(map[bool]map[roachpb.NodeID]chan *RaftMessageRequest)

	if grpcServer != nil {
		RegisterMultiRaftServer(grpcServer, t)
	}

	return t
}

// RaftMessage proxies the incoming request to the listening server interface.
func (t *RaftTransport) RaftMessage(stream MultiRaft_RaftMessageServer) (err error) {
	errCh := make(chan error, 1)

	// Node stopping error is caught below in the select.
	_ = t.rpcContext.Stopper.RunTask(func() {
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
						return errors.Errorf("Unable to proxy message to node: %d", req.Message.To)
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

// processQueue creates a client and sends messages from its designated queue
// via that client, exiting when the client fails or when it idles out. All
// messages remaining in the queue at that point are lost and a new instance of
// processQueue should be started by the next message to be sent.
// TODO(tschottdorf) should let raft know if the node is down;
// need a feedback mechanism for that. Potentially easiest is to arrange for
// the next call to Send() to fail appropriately.
func (t *RaftTransport) processQueue(ch chan *RaftMessageRequest, nodeID roachpb.NodeID) error {
	addr, err := t.resolver(nodeID)
	if err != nil {
		return err
	}

	conn, err := t.rpcContext.GRPCDial(addr.String())
	if err != nil {
		return err
	}
	client := NewMultiRaftClient(conn)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	stream, err := client.RaftMessage(ctx)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)

	// Starting workers in a task prevents data races during shutdown.
	if t.rpcContext.Stopper.RunTask(func() {
		t.rpcContext.Stopper.RunWorker(func() {
			errCh <- stream.RecvMsg(new(RaftMessageResponse))
		})
	}) != nil {
		// Early exit on node stopped error.
		return nil
	}

	var raftIdleTimer timeutil.Timer
	defer raftIdleTimer.Stop()
	for {
		raftIdleTimer.Reset(raftIdleTimeout)
		select {
		case <-t.rpcContext.Stopper.ShouldStop():
			return nil
		case <-raftIdleTimer.C:
			raftIdleTimer.Read = true
			return nil
		case err := <-errCh:
			return err
		case req := <-ch:
			err := stream.Send(req)
			if req.Message.Type == raftpb.MsgSnap {
				select {
				case <-t.rpcContext.Stopper.ShouldStop():
					return nil
				case t.SnapshotStatusChan <- RaftSnapshotStatus{req, err}:
				}

			}
			if err != nil {
				return err
			} else if log.V(2) || log.V(1) && req.Message.Type == raftpb.MsgSnap {
				log.Infof("successfully sent %s to %s at %s", req.Message.Type, req.ToReplica, addr)
			}
		}
	}
}

// Send a message to the recipient specified in the request.
func (t *RaftTransport) Send(req *RaftMessageRequest) error {
	isSnap := req.Message.Type == raftpb.MsgSnap
	t.mu.Lock()
	// We use two queues; one will be used for snapshots, the other for all other
	// traffic. This is done to prevent snapshots from blocking other traffic.
	queues, ok := t.mu.queues[isSnap]
	if !ok {
		queues = make(map[roachpb.NodeID]chan *RaftMessageRequest)
		t.mu.queues[isSnap] = queues
	}
	ch, ok := queues[req.ToReplica.NodeID]
	if !ok {
		ch = make(chan *RaftMessageRequest, raftSendBufferSize)
		queues[req.ToReplica.NodeID] = ch
	}
	t.mu.Unlock()

	if !ok {
		// Starting workers in a task prevents data races during shutdown.
		if err := t.rpcContext.Stopper.RunTask(func() {
			t.rpcContext.Stopper.RunWorker(func() {
				if err := t.processQueue(ch, req.ToReplica.NodeID); err != nil {
					log.Error(err)
				}

				t.mu.Lock()
				delete(queues, req.ToReplica.NodeID)
				t.mu.Unlock()
			})
		}); err != nil {
			return err
		}
	}

	select {
	case ch <- req:
		return nil
	default:
		return errors.Errorf("queue for node %d is full", req.ToReplica.NodeID)
	}
}
