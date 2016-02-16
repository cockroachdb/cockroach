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

	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/grpcutil"
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

// rpcTransport handles the rpc messages for raft.
type rpcTransport struct {
	gossip     *gossip.Gossip
	rpcContext *rpc.Context
	mu         sync.Mutex
	handlers   map[roachpb.StoreID]storage.RaftMessageHandler
	queues     map[roachpb.StoreID]chan *storage.RaftMessageRequest
}

// newRPCTransport creates a new rpcTransport with specified gossip and grpc server.
func newRPCTransport(gossip *gossip.Gossip, grpcServer *grpc.Server, rpcContext *rpc.Context) storage.RaftTransport {
	t := &rpcTransport{
		gossip:     gossip,
		rpcContext: rpcContext,
		handlers:   make(map[roachpb.StoreID]storage.RaftMessageHandler),
		queues:     make(map[roachpb.StoreID]chan *storage.RaftMessageRequest),
	}

	if grpcServer != nil {
		storage.RegisterMultiRaftServer(grpcServer, t)
	}

	return t
}

// RaftMessage proxies the incoming request to the listening server interface.
func (t *rpcTransport) RaftMessage(stream storage.MultiRaft_RaftMessageServer) error {
	errCh := make(chan error, 1)

	// Starting workers in a task prevents data races during shutdown.
	t.rpcContext.Stopper.RunTask(func() {
		t.rpcContext.Stopper.RunWorker(func() {
			errCh <- func() error {
				for {
					req, err := stream.Recv()
					if err != nil {
						return err
					}

					t.mu.Lock()
					handler, ok := t.handlers[req.ToReplica.StoreID]
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
	case <-t.rpcContext.Stopper.ShouldDrain():
		return nil
	case err := <-errCh:
		return err
	}
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

	var dialOpt grpc.DialOption
	if t.rpcContext.Insecure {
		dialOpt = grpc.WithInsecure()
	} else {
		tlsConfig, err := t.rpcContext.GetClientTLSConfig()
		if err != nil {
			log.Error(err)
			return
		}
		dialOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	conn, err := grpc.Dial(addr.String(), dialOpt)
	if err != nil {
		log.Errorf("failed to dial: %v", err)
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Error(err)
		}
	}()
	client := storage.NewMultiRaftClient(conn)
	ctx := grpcutil.NewContextWithStopper(context.Background(), t.rpcContext.Stopper)
	stream, err := client.RaftMessage(ctx)
	if err != nil {
		log.Error(err)
		return
	}
	defer func() {
		if err := stream.CloseSend(); err != nil {
			log.Error(err)
		}
	}()

	var raftIdleTimer util.Timer
	defer raftIdleTimer.Stop()
	for {
		raftIdleTimer.Reset(raftIdleTimeout)
		select {
		case <-ctx.Done():
			return
		case <-raftIdleTimer.C:
			raftIdleTimer.Read = true
			if log.V(1) {
				log.Infof("closing Raft transport to %d due to inactivity", nodeID)
			}
			return
		case req := <-ch:
			if err := stream.Send(req); err != nil {
				log.Error(err)
				return
			}
		}
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
