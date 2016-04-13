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
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
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
		queues   map[roachpb.NodeID]chan *RaftMessageRequest
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
	t.mu.queues = make(map[roachpb.NodeID]chan *RaftMessageRequest)

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

// processQueue creates a client and sends messages from its designated queue
// via that client, exiting when the client fails or when it idles out. All
// messages remaining in the queue at that point are lost and a new instance of
// processQueue should be started by the next message to be sent.
// TODO(tschottdorf) should let raft know if the node is down;
// need a feedback mechanism for that. Potentially easiest is to arrange for
// the next call to Send() to fail appropriately.
func (t *RaftTransport) processQueue(nodeID roachpb.NodeID) {
	t.mu.Lock()
	ch, ok := t.mu.queues[nodeID]
	t.mu.Unlock()
	if !ok {
		return
	}
	// Clean-up when the loop below shuts down.
	defer func() {
		t.mu.Lock()
		delete(t.mu.queues, nodeID)
		t.mu.Unlock()
	}()

	addr, err := t.resolver(nodeID)
	if err != nil {
		if log.V(1) {
			log.Errorf("failed to get address for node %d: %s", nodeID, err)
		}
		return
	}

	if log.V(1) {
		log.Infof("dialing node %d at %s", nodeID, addr)
	}
	conn, err := t.rpcContext.GRPCDial(addr.String())
	if err != nil {
		if log.V(1) {
			log.Errorf("failed to dial: %s", err)
		}
		return
	}
	client := NewMultiRaftClient(conn)
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	if log.V(1) {
		log.Infof("establishing Raft transport stream to node %d at %s", nodeID, addr)
	}
	// We start two streams; one will be used for snapshots, the other for all
	// other traffic. This is done to prevent snapshots from blocking other
	// traffic.
	streams := make([]MultiRaft_RaftMessageClient, 2)
	for i := range streams {
		stream, err := client.RaftMessage(ctx)
		if err != nil {
			if log.V(1) {
				log.Errorf("failed to establish Raft transport stream to node %d at %s: %s", nodeID, addr, err)
			}
			return
		}
		streams[i] = stream
	}

	errCh := make(chan error, len(streams))

	// Starting workers in a task prevents data races during shutdown.
	t.rpcContext.Stopper.RunTask(func() {
		for i := range streams {
			// Avoid closing over a `range` binding.
			stream := streams[i]

			t.rpcContext.Stopper.RunWorker(func() {
				// NB: only one error will ever be read from this channel. That's fine,
				// given that the channel is buffered to the maximum number of errors
				// that will be written to it.
				errCh <- stream.RecvMsg(new(RaftMessageResponse))
			})
		}
	})

	snapStream := streams[0]
	restStream := streams[1]

	var raftIdleTimer util.Timer
	defer raftIdleTimer.Stop()
	for {
		raftIdleTimer.Reset(raftIdleTimeout)
		select {
		case <-t.rpcContext.Stopper.ShouldStop():
			return
		case <-raftIdleTimer.C:
			raftIdleTimer.Read = true
			if log.V(1) {
				log.Infof("closing Raft transport to %d at %s due to inactivity", nodeID, addr)
			}
			return
		case err := <-errCh:
			if log.V(1) {
				if err != nil {
					log.Infof("remote node %d at %s closed Raft transport with error: %s", nodeID, addr, err)
				} else {
					log.Infof("remote node %d at %s closed Raft transport", nodeID, addr)
				}
			}
			return
		case req := <-ch:
			if req.Message.Type == raftpb.MsgSnap {
				t.rpcContext.Stopper.RunAsyncTask(func() {
					err := snapStream.Send(req)
					if err != nil {
						log.Errorf("failed to send Raft snapshot to node %d at %s: %s", nodeID, addr, err)
					} else if log.V(1) {
						log.Infof("successfully sent a Raft snapshot to node %d at %s", nodeID, addr)
					}
					t.SnapshotStatusChan <- RaftSnapshotStatus{req, err}
				})
			} else {
				if err := restStream.Send(req); err != nil {
					log.Error(err)
					return
				}
			}
		}
	}
}

// Send a message to the recipient specified in the request.
func (t *RaftTransport) Send(req *RaftMessageRequest) error {
	isRunning := true
	t.mu.Lock()
	ch, ok := t.mu.queues[req.ToReplica.NodeID]
	if !ok {
		ch = make(chan *RaftMessageRequest, raftSendBufferSize)
		t.mu.queues[req.ToReplica.NodeID] = ch

		// Starting workers in a task prevents data races during shutdown.
		isRunning = t.rpcContext.Stopper.RunTask(func() {
			t.rpcContext.Stopper.RunWorker(func() {
				t.processQueue(req.ToReplica.NodeID)
			})
		})
	}
	t.mu.Unlock()

	if !isRunning {
		return util.Errorf("node stopped")
	}

	select {
	case ch <- req:
		return nil
	default:
		return util.Errorf("queue for node %d is full", req.ToReplica.NodeID)
	}
}
