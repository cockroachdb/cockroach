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
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/rubyist/circuitbreaker"
)

const (
	// Outgoing messages are queued per-replica on a channel of this size.
	raftSendBufferSize = 100

	// When no message has been queued for this duration, the corresponding
	// instance of processQueue will shut down.
	//
	// TODO(tamird): make culling of outbound streams more evented, so that we
	// need not rely on this timeout to shut things down.
	raftIdleTimeout = time.Minute
)

// RaftMessageHandler is the interface that must be implemented by
// arguments to RaftTransport.Listen.
type RaftMessageHandler interface {
	// HandleRaftRequest is called for each incoming message. If it
	// returns an error it will be streamed back to the sender of the
	// message as a RaftMessageResponse.
	HandleRaftRequest(context.Context, *RaftMessageRequest) *roachpb.Error

	// HandleRaftResponse is called for each raft response. Note that
	// not all messages receive a response.
	HandleRaftResponse(context.Context, *RaftMessageResponse)
}

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
//
// The raft transport is asynchronous with respect to the caller, and
// internally multiplexes outbound messages. Internally, each message is
// queued on a per-destination queue before being asynchronously delivered.
//
// Callers are required to construct a RaftSender before being able to
// dispatch messages, and must provide an error handler which will be invoked
// asynchronously in the event that the recipient of any message closes its
// inbound RPC stream. This callback is asynchronous with respect to the
// outbound message which caused the remote to hang up; all that is known is
// which remote hung up.
type RaftTransport struct {
	resolver           NodeAddressResolver
	rpcContext         *rpc.Context
	SnapshotStatusChan chan RaftSnapshotStatus

	mu struct {
		syncutil.Mutex
		handlers map[roachpb.StoreID]RaftMessageHandler
		queues   map[bool]map[roachpb.NodeID]chan *RaftMessageRequest
		breakers map[roachpb.NodeID]*circuit.Breaker
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
	t.mu.handlers = make(map[roachpb.StoreID]RaftMessageHandler)
	t.mu.queues = make(map[bool]map[roachpb.NodeID]chan *RaftMessageRequest)
	t.mu.breakers = make(map[roachpb.NodeID]*circuit.Breaker)

	if grpcServer != nil {
		RegisterMultiRaftServer(grpcServer, t)
	}

	return t
}

// handleRaftRequest proxies a request to the listening server interface.
func (t *RaftTransport) handleRaftRequest(
	ctx context.Context, req *RaftMessageRequest,
) *roachpb.Error {
	t.mu.Lock()
	handler, ok := t.mu.handlers[req.ToReplica.StoreID]
	t.mu.Unlock()

	if !ok {
		return roachpb.NewErrorf(
			"unable to accept Raft message from %+v: no store registered for %+v",
			req.ToReplica, req.FromReplica)
	}

	return handler.HandleRaftRequest(ctx, req)
}

// newRaftMessageResponse constructs a RaftMessageResponse from the
// given request and error.
func newRaftMessageResponse(req *RaftMessageRequest, pErr *roachpb.Error) *RaftMessageResponse {
	resp := &RaftMessageResponse{
		RangeID: req.RangeID,
		// From and To are reversed in the response.
		ToReplica:   req.FromReplica,
		FromReplica: req.ToReplica,
	}
	if pErr != nil {
		resp.Union.SetValue(pErr)
	}
	return resp
}

// RaftMessage proxies the incoming requests to the listening server interface.
func (t *RaftTransport) RaftMessage(stream MultiRaft_RaftMessageServer) (err error) {
	errCh := make(chan error, 1)

	// Node stopping error is caught below in the select.
	if err := t.rpcContext.Stopper.RunTask(func() {
		t.rpcContext.Stopper.RunWorker(func() {
			errCh <- func() error {
				for {
					req, err := stream.Recv()
					if err != nil {
						return err
					}

					if pErr := t.handleRaftRequest(stream.Context(), req); pErr != nil {
						if err := stream.Send(newRaftMessageResponse(req, pErr)); err != nil {
							return err
						}
					}
				}
			}()
		})
	}); err != nil {
		return err
	}

	select {
	case err := <-errCh:
		return err
	case <-t.rpcContext.Stopper.ShouldQuiesce():
		return nil
	}
}

// RaftMessageSync proxies the incoming request to the listening server interface.
func (t *RaftTransport) RaftMessageSync(ctx context.Context, req *RaftMessageRequest,
) (*RaftMessageResponse, error) {
	var pErr *roachpb.Error
	if err := t.rpcContext.Stopper.RunTask(func() {
		pErr = t.handleRaftRequest(ctx, req)
	}); err != nil {
		return nil, err
	}
	return newRaftMessageResponse(req, pErr), nil
}

// Listen registers a raftMessageHandler to receive proxied messages.
func (t *RaftTransport) Listen(storeID roachpb.StoreID, handler RaftMessageHandler) {
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

// GetCircuitBreaker returns the circuit breaker controlling
// connection attempts to the specified node.
func (t *RaftTransport) GetCircuitBreaker(nodeID roachpb.NodeID) *circuit.Breaker {
	t.mu.Lock()
	defer t.mu.Unlock()
	breaker, ok := t.mu.breakers[nodeID]
	if !ok {
		breaker = t.rpcContext.NewBreaker()
		t.mu.breakers[nodeID] = breaker
	}
	return breaker
}

// connectAndProcess connects to the node and then processes the
// provided channel containing a queue of raft messages until there is
// an unrecoverable error with the underlying connection. A circuit
// breaker is used to allow fast failures in SendAsync which will drop
// incoming raft messages and report unreachable status to the raft group.
func (t *RaftTransport) connectAndProcess(nodeID roachpb.NodeID, ch chan *RaftMessageRequest) {
	breaker := t.GetCircuitBreaker(nodeID)
	consecFailures := breaker.ConsecFailures()
	if err := breaker.Call(func() error {
		addr, err := t.resolver(nodeID)
		if err != nil {
			return err
		}
		conn, err := t.rpcContext.GRPCDial(addr.String(), grpc.WithBlock())
		if err != nil {
			return err
		}
		return t.processQueue(nodeID, ch, conn)
	}, 0); err != nil {
		if consecFailures == 0 {
			log.Warningf(context.TODO(), "raft transport stream to node %d failed: %s", nodeID, err)
		}
		return
	}
	if consecFailures > 0 {
		log.Infof(context.TODO(), "raft transport stream to node %d established", nodeID)
	}
}

// processQueue opens a Raft client stream and sends messages from the
// designated queue (ch) via that stream, exiting when an error is received or
// when it idles out. All messages remaining in the queue at that point are
// lost and a new instance of processQueue will be started by the next message
// to be sent.
func (t *RaftTransport) processQueue(nodeID roachpb.NodeID, ch chan *RaftMessageRequest, conn *grpc.ClientConn) error {
	client := NewMultiRaftClient(conn)
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	stream, err := client.RaftMessage(ctx)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)

	// Starting workers in a task prevents data races during shutdown.
	if err := t.rpcContext.Stopper.RunTask(func() {
		t.rpcContext.Stopper.RunWorker(func() {
			errCh <- func() error {
				for {
					resp, err := stream.Recv()
					if err != nil {
						return err
					}
					t.mu.Lock()
					handler, ok := t.mu.handlers[resp.ToReplica.StoreID]
					t.mu.Unlock()
					if !ok {
						log.Warningf(context.TODO(), "no handler found for store %s in response %s",
							resp.ToReplica.StoreID, resp)
						continue
					}
					handler.HandleRaftResponse(stream.Context(), resp)
				}
			}()
		})
	}); err != nil {
		return err
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
			}
		}
	}
}

// SendAsync sends a message to the recipient specified in the request. It
// returns false if the outgoing queue is full and calls s.onError when the
// recipient closes the stream.
func (t *RaftTransport) SendAsync(req *RaftMessageRequest) bool {
	isHeartbeat := (req.Message.Type == raftpb.MsgHeartbeat ||
		req.Message.Type == raftpb.MsgHeartbeatResp)
	if req.RangeID == 0 && !isHeartbeat {
		// Coalesced heartbeats are addressed to range 0; everything else
		// needs an explicit range ID.
		panic("only heartbeat messages may be sent to range ID 0")
	}
	isSnap := req.Message.Type == raftpb.MsgSnap
	toNodeID := req.ToReplica.NodeID

	// First, check the circuit breaker for connections to the outgoing
	// node. We fail fast if the breaker is open to drop the raft
	// message and have the caller mark the raft group as unreachable.
	breaker := t.GetCircuitBreaker(toNodeID)
	if !breaker.Ready() {
		return false
	}

	t.mu.Lock()
	// We use two queues; one will be used for snapshots, the other for all other
	// traffic. This is done to prevent snapshots from blocking other traffic.
	queues, ok := t.mu.queues[isSnap]
	if !ok {
		queues = make(map[roachpb.NodeID]chan *RaftMessageRequest)
		t.mu.queues[isSnap] = queues
	}
	ch, ok := queues[toNodeID]
	if !ok {
		ch = make(chan *RaftMessageRequest, raftSendBufferSize)
		queues[toNodeID] = ch
	}
	t.mu.Unlock()

	if !ok {
		// Starting workers in a task prevents data races during shutdown.
		if err := t.rpcContext.Stopper.RunTask(func() {
			t.rpcContext.Stopper.RunWorker(func() {
				t.connectAndProcess(toNodeID, ch)
				t.mu.Lock()
				delete(queues, toNodeID)
				t.mu.Unlock()
			})
		}); err != nil {
			return false
		}
	}

	select {
	case ch <- req:
		return true
	default:
		return false
	}
}

// SendSync sends a raft message and waits for an acknowledgement.
func (t *RaftTransport) SendSync(ctx context.Context, req *RaftMessageRequest) error {
	addr, err := t.resolver(req.ToReplica.NodeID)
	if err != nil {
		return err
	}
	conn, err := t.rpcContext.GRPCDial(addr.String(), grpc.WithBlock())
	if err != nil {
		return err
	}
	client := NewMultiRaftClient(conn)
	resp, err := client.RaftMessageSync(ctx, req)
	if err != nil {
		return err
	}
	switch val := resp.Union.GetValue().(type) {
	case *roachpb.Error:
		return val.GoError()
	case nil:
		return nil
	default:
		return errors.Errorf("unexpected response value %T %s", val, val)
	}
}
