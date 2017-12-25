// Copyright 2014 The Cockroach Authors.
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

package storage_test

import (
	"context"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

const channelServerBrokenRangeMessage = "channelServer broken range"

type channelServer struct {
	ch       chan *storage.RaftMessageRequest
	maxSleep time.Duration

	// If non-zero, all messages to this range will return errors
	brokenRange roachpb.RangeID
}

func newChannelServer(bufSize int, maxSleep time.Duration) channelServer {
	return channelServer{
		ch:       make(chan *storage.RaftMessageRequest, bufSize),
		maxSleep: maxSleep,
	}
}

func (s channelServer) HandleRaftRequest(
	ctx context.Context, req *storage.RaftMessageRequest, _ storage.RaftMessageResponseStream,
) *roachpb.Error {
	if s.maxSleep != 0 {
		// maxSleep simulates goroutine scheduling delays that could
		// result in messages being processed out of order (in previous
		// transport implementations).
		time.Sleep(time.Duration(rand.Int63n(int64(s.maxSleep))))
	}
	if s.brokenRange != 0 && s.brokenRange == req.RangeID {
		return roachpb.NewErrorf(channelServerBrokenRangeMessage)
	}
	s.ch <- req
	return nil
}

func (s channelServer) HandleRaftResponse(
	ctx context.Context, resp *storage.RaftMessageResponse,
) error {
	// Mimic the logic in (*Store).HandleRaftResponse without requiring an
	// entire Store object to be pulled into these tests.
	if val, ok := resp.Union.GetValue().(*roachpb.Error); ok {
		if err, ok := val.GetDetail().(*roachpb.StoreNotFoundError); ok {
			return err
		}
	}
	log.Fatalf(ctx, "unexpected raft response: %s", resp)
	return nil
}

func (s channelServer) HandleSnapshot(
	header *storage.SnapshotRequest_Header, stream storage.SnapshotResponseStream,
) error {
	panic("unexpected HandleSnapshot")
}

// raftTransportTestContext contains objects needed to test RaftTransport.
// Typical usage will add multiple nodes with AddNode, attach channels
// to at least one store with ListenStore, and send messages with Send.
type raftTransportTestContext struct {
	t              testing.TB
	stopper        *stop.Stopper
	transports     map[roachpb.NodeID]*storage.RaftTransport
	nodeRPCContext *rpc.Context
	gossip         *gossip.Gossip
}

func newRaftTransportTestContext(t testing.TB) *raftTransportTestContext {
	rttc := &raftTransportTestContext{
		t:          t,
		stopper:    stop.NewStopper(),
		transports: map[roachpb.NodeID]*storage.RaftTransport{},
	}
	rttc.nodeRPCContext = rpc.NewContext(
		log.AmbientContext{Tracer: tracing.NewTracer()},
		testutils.NewNodeTestBaseContext(),
		hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		rttc.stopper,
		&cluster.MakeTestingClusterSettings().Version,
	)
	server := rpc.NewServer(rttc.nodeRPCContext) // never started
	rttc.gossip = gossip.NewTest(
		1, rttc.nodeRPCContext, server, rttc.stopper, metric.NewRegistry(),
	)
	return rttc
}

func (rttc *raftTransportTestContext) Stop() {
	rttc.stopper.Stop(context.TODO())
}

// AddNode registers a node with the cluster. Nodes must be added
// before they can be used in other methods of
// raftTransportTestContext. The node will be gossiped immediately.
func (rttc *raftTransportTestContext) AddNode(nodeID roachpb.NodeID) *storage.RaftTransport {
	transport, addr := rttc.AddNodeWithoutGossip(nodeID, util.TestAddr, rttc.stopper)
	rttc.GossipNode(nodeID, addr)
	return transport
}

// AddNodeWithoutGossip registers a node with the cluster. Nodes must
// be added before they can be used in other methods of
// raftTransportTestContext. Unless you are testing the effects of
// delaying gossip, use AddNode instead.
func (rttc *raftTransportTestContext) AddNodeWithoutGossip(
	nodeID roachpb.NodeID, addr net.Addr, stopper *stop.Stopper,
) (*storage.RaftTransport, net.Addr) {
	grpcServer := rpc.NewServer(rttc.nodeRPCContext)
	transport := storage.NewRaftTransport(
		log.AmbientContext{Tracer: tracing.NewTracer()},
		cluster.MakeTestingClusterSettings(),
		storage.GossipAddressResolver(rttc.gossip),
		grpcServer,
		rttc.nodeRPCContext,
	)
	rttc.transports[nodeID] = transport
	ln, err := netutil.ListenAndServeGRPC(stopper, grpcServer, addr)
	if err != nil {
		rttc.t.Fatal(err)
	}
	return transport, ln.Addr()
}

// GossipNode gossips the node's address, which is necessary before
// any messages can be sent to it. Normally done automatically by
// AddNode.
func (rttc *raftTransportTestContext) GossipNode(nodeID roachpb.NodeID, addr net.Addr) {
	if err := rttc.gossip.AddInfoProto(gossip.MakeNodeIDKey(nodeID),
		&roachpb.NodeDescriptor{
			NodeID:  nodeID,
			Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
		},
		time.Hour); err != nil {
		rttc.t.Fatal(err)
	}
}

// ListenStore registers a store on a node and returns a channel for
// messages sent to that store.
func (rttc *raftTransportTestContext) ListenStore(
	nodeID roachpb.NodeID, storeID roachpb.StoreID,
) channelServer {
	ch := newChannelServer(100, 10*time.Millisecond)
	rttc.transports[nodeID].Listen(storeID, ch)
	return ch
}

// Send a message. Returns false if the message was dropped.
func (rttc *raftTransportTestContext) Send(
	from, to roachpb.ReplicaDescriptor, rangeID roachpb.RangeID, msg raftpb.Message,
) bool {
	msg.To = uint64(to.ReplicaID)
	msg.From = uint64(from.ReplicaID)
	req := &storage.RaftMessageRequest{
		RangeID:     rangeID,
		Message:     msg,
		ToReplica:   to,
		FromReplica: from,
	}
	return rttc.transports[from.NodeID].SendAsync(req)
}

func TestSendAndReceive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rttc := newRaftTransportTestContext(t)
	defer rttc.Stop()

	// Create several servers, each of which has two stores (A raft
	// node ID addresses a store). Node 1 has stores 1 and 2, node 2 has
	// stores 3 and 4, etc.
	//
	// We suppose that range 1 is replicated across the odd-numbered
	// stores in reverse order to ensure that the various IDs are not
	// equal: replica 1 is store 5, replica 2 is store 3, and replica 3
	// is store 1.
	const numNodes = 3
	const storesPerNode = 2
	nextNodeID := roachpb.NodeID(2)
	nextStoreID := roachpb.StoreID(2)

	// Per-node state.
	transports := map[roachpb.NodeID]*storage.RaftTransport{}

	// Per-store state.
	storeNodes := map[roachpb.StoreID]roachpb.NodeID{}
	channels := map[roachpb.StoreID]channelServer{}
	replicaIDs := map[roachpb.StoreID]roachpb.ReplicaID{
		1: 3,
		3: 2,
		5: 1,
	}

	messageTypes := map[raftpb.MessageType]struct{}{
		raftpb.MsgHeartbeat: {},
	}

	for nodeIndex := 0; nodeIndex < numNodes; nodeIndex++ {
		nodeID := nextNodeID
		nextNodeID++
		transports[nodeID] = rttc.AddNode(nodeID)

		for storeIndex := 0; storeIndex < storesPerNode; storeIndex++ {
			storeID := nextStoreID
			nextStoreID++

			storeNodes[storeID] = nodeID

			channels[storeID] = rttc.ListenStore(nodeID, storeID)
		}
	}

	messageTypeCounts := make(map[roachpb.StoreID]map[raftpb.MessageType]int)

	// Each store sends one snapshot and one heartbeat to each store, including
	// itself.
	for toStoreID, toNodeID := range storeNodes {
		if _, ok := messageTypeCounts[toStoreID]; !ok {
			messageTypeCounts[toStoreID] = make(map[raftpb.MessageType]int)
		}

		for fromStoreID, fromNodeID := range storeNodes {
			baseReq := storage.RaftMessageRequest{
				RangeID: 1,
				Message: raftpb.Message{
					From: uint64(fromStoreID),
					To:   uint64(toStoreID),
				},
				FromReplica: roachpb.ReplicaDescriptor{
					NodeID:  fromNodeID,
					StoreID: fromStoreID,
				},
				ToReplica: roachpb.ReplicaDescriptor{
					NodeID:  toNodeID,
					StoreID: toStoreID,
				},
			}

			for messageType := range messageTypes {
				req := baseReq
				req.Message.Type = messageType

				if !transports[fromNodeID].SendAsync(&req) {
					t.Errorf("unable to send %s from %d to %d", req.Message.Type, fromNodeID, toNodeID)
				}
				messageTypeCounts[toStoreID][req.Message.Type]++
			}
		}
	}

	// Read all the messages from the channels. Note that the transport
	// does not guarantee in-order delivery between independent
	// transports, so we just verify that the right number of messages
	// end up in each channel.
	for toStoreID := range storeNodes {
		for len(messageTypeCounts[toStoreID]) > 0 {
			req := <-channels[toStoreID].ch
			if req.Message.To != uint64(toStoreID) {
				t.Errorf("got unexpected message %v on channel %d", req, toStoreID)
			}

			if typeCounts, ok := messageTypeCounts[toStoreID]; ok {
				if _, ok := typeCounts[req.Message.Type]; ok {
					typeCounts[req.Message.Type]--
					if typeCounts[req.Message.Type] == 0 {
						delete(typeCounts, req.Message.Type)
					}
				} else {
					t.Errorf("expected %v to have key %v, but it did not", typeCounts, req.Message.Type)
				}
			} else {
				t.Errorf("expected %v to have key %v, but it did not", messageTypeCounts, toStoreID)
			}
		}

		delete(messageTypeCounts, toStoreID)

		select {
		case req := <-channels[toStoreID].ch:
			t.Errorf("got unexpected message %v on channel %d", req, toStoreID)
		case <-time.After(100 * time.Millisecond):
		}
	}

	if len(messageTypeCounts) > 0 {
		t.Errorf("remaining messages expected: %v", messageTypeCounts)
	}

	// Real raft messages have different node/store/replica IDs.
	// Send a message from replica 2 (on store 3, node 2) to replica 1 (on store 5, node 3)
	fromStoreID := roachpb.StoreID(3)
	toStoreID := roachpb.StoreID(5)
	expReq := &storage.RaftMessageRequest{
		RangeID: 1,
		Message: raftpb.Message{
			Type: raftpb.MsgApp,
			From: uint64(replicaIDs[fromStoreID]),
			To:   uint64(replicaIDs[toStoreID]),
		},
		FromReplica: roachpb.ReplicaDescriptor{
			NodeID:    storeNodes[fromStoreID],
			StoreID:   fromStoreID,
			ReplicaID: replicaIDs[fromStoreID],
		},
		ToReplica: roachpb.ReplicaDescriptor{
			NodeID:    storeNodes[toStoreID],
			StoreID:   toStoreID,
			ReplicaID: replicaIDs[toStoreID],
		},
	}
	if !transports[storeNodes[fromStoreID]].SendAsync(expReq) {
		t.Errorf("unable to send message from %d to %d", fromStoreID, toStoreID)
	}
	if req := <-channels[toStoreID].ch; !proto.Equal(req, expReq) {
		t.Errorf("got unexpected message %v on channel %d", req, toStoreID)
	}

	select {
	case req := <-channels[toStoreID].ch:
		t.Errorf("got unexpected message %v on channel %d", req, toStoreID)
	default:
	}
}

// TestInOrderDelivery verifies that for a given pair of nodes, raft
// messages are delivered in order.
func TestInOrderDelivery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rttc := newRaftTransportTestContext(t)
	defer rttc.Stop()

	const numMessages = 100
	serverReplica := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rttc.AddNode(serverReplica.NodeID)
	serverChannel := rttc.ListenStore(serverReplica.NodeID, serverReplica.StoreID)

	clientReplica := roachpb.ReplicaDescriptor{
		NodeID:    1,
		StoreID:   1,
		ReplicaID: 1,
	}
	rttc.AddNode(clientReplica.NodeID)

	for i := 0; i < numMessages; i++ {
		if !rttc.Send(clientReplica, serverReplica, 1, raftpb.Message{Commit: uint64(i)}) {
			t.Errorf("failed to send message %d", i)
		}
	}

	for i := 0; i < numMessages; i++ {
		req := <-serverChannel.ch
		if req.Message.Commit != uint64(i) {
			t.Errorf("messages out of order: got %d while expecting %d", req.Message.Commit, i)
		}
	}
}

// TestRaftTransportCircuitBreaker verifies that messages will be
// dropped waiting for raft node connection to be established.
func TestRaftTransportCircuitBreaker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rttc := newRaftTransportTestContext(t)
	defer rttc.Stop()

	serverReplica := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	_, serverAddr := rttc.AddNodeWithoutGossip(serverReplica.NodeID, util.TestAddr, rttc.stopper)
	serverChannel := rttc.ListenStore(serverReplica.NodeID, serverReplica.StoreID)

	clientReplica := roachpb.ReplicaDescriptor{
		NodeID:    1,
		StoreID:   1,
		ReplicaID: 1,
	}
	clientTransport := rttc.AddNode(clientReplica.NodeID)

	// The transport is set up asynchronously, so we expect the first
	// Send to return true here.
	if !rttc.Send(clientReplica, serverReplica, 1, raftpb.Message{Commit: 1}) {
		t.Errorf("unexpectedly failed sending while connection is being asynchronously established")
	}

	// However, sending repeated messages should begin dropping once
	// the circuit breaker does trip.
	testutils.SucceedsSoon(t, func() error {
		if rttc.Send(clientReplica, serverReplica, 1, raftpb.Message{Commit: 1}) {
			return errors.Errorf("expected circuit breaker to trip")
		}
		return nil
	})

	// Now, gossip address of server.
	rttc.GossipNode(serverReplica.NodeID, serverAddr)

	// Keep sending commit=2 until breaker resets and we receive the
	// first instance. It's possible an earlier message for commit=1
	// snuck in.
	testutils.SucceedsSoon(t, func() error {
		if !rttc.Send(clientReplica, serverReplica, 1, raftpb.Message{Commit: 2}) {
			clientTransport.GetCircuitBreaker(serverReplica.NodeID).Reset()
		}
		select {
		case req := <-serverChannel.ch:
			if req.Message.Commit == 2 {
				return nil
			}
		default:
		}
		return errors.Errorf("expected message commit=2")
	})
}

// TestRaftTransportIndependentRanges ensures that errors from one
// range do not interfere with messages to another range on the same
// store.
func TestRaftTransportIndependentRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rttc := newRaftTransportTestContext(t)
	defer rttc.Stop()

	server := roachpb.ReplicaDescriptor{
		NodeID:    1,
		StoreID:   1,
		ReplicaID: 1,
	}
	serverTransport := rttc.AddNode(server.NodeID)
	client := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rttc.AddNode(client.NodeID)

	const numMessages = 50
	channelServer := newChannelServer(numMessages*2, 10*time.Millisecond)
	channelServer.brokenRange = 13
	serverTransport.Listen(server.StoreID, channelServer)

	for i := 0; i < numMessages; i++ {
		for _, rangeID := range []roachpb.RangeID{1, 13} {
			if !rttc.Send(client, server, rangeID, raftpb.Message{Commit: uint64(i)}) {
				t.Errorf("failed to send message %d to range %s", i, rangeID)
			}
		}
	}
	for i := 0; i < numMessages; i++ {
		select {
		case msg := <-channelServer.ch:
			if msg.Message.Commit != uint64(i) {
				t.Errorf("got message %d while expecting %d", msg.Message.Commit, i)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}
}

// TestReopenConnection verifies that if a raft response indicates that the
// expected store isn't present on the node, that the connection gets
// terminated and reopened before retrying, to ensure that the transport
// doesn't get stuck in an endless retry loop against the wrong node.
func TestReopenConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rttc := newRaftTransportTestContext(t)
	defer rttc.Stop()

	// Use a special stopper for the initial server so that we can fully stop it
	// (releasing its bound network address) before the rest of the test pieces.
	serverStopper := stop.NewStopper()
	serverReplica := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	serverTransport, serverAddr :=
		rttc.AddNodeWithoutGossip(serverReplica.NodeID, util.TestAddr, serverStopper)
	rttc.GossipNode(serverReplica.NodeID, serverAddr)
	rttc.ListenStore(serverReplica.NodeID, serverReplica.StoreID)

	clientReplica := roachpb.ReplicaDescriptor{
		NodeID:    1,
		StoreID:   1,
		ReplicaID: 1,
	}
	rttc.AddNode(clientReplica.NodeID)
	rttc.ListenStore(clientReplica.NodeID, clientReplica.StoreID)

	// Take down the old server and start a new one at the same address.
	serverTransport.Stop(serverReplica.StoreID)
	serverStopper.Stop(context.TODO())

	replacementReplica := roachpb.ReplicaDescriptor{
		NodeID:    3,
		StoreID:   3,
		ReplicaID: 3,
	}
	rttc.AddNodeWithoutGossip(replacementReplica.NodeID, serverAddr, rttc.stopper)
	replacementChannel := rttc.ListenStore(replacementReplica.NodeID, replacementReplica.StoreID)

	// Try sending a message to the old server's store (at the address its
	// replacement is now running at) before its replacement has been gossiped.
	// We just want to ensure that doing so doesn't deadlock the client transport.
	// Successive attempts should instead trip the circuit breaker.
	if !rttc.Send(clientReplica, serverReplica, 1, raftpb.Message{Commit: 1}) {
		t.Errorf("unexpectedly failed sending first message to recently downed node")
	}
	testutils.SucceedsSoon(t, func() error {
		if rttc.Send(clientReplica, serverReplica, 1, raftpb.Message{Commit: 1}) {
			return errors.Errorf("expected circuit breaker to trip")
		}
		return nil
	})

	// Then, to ensure the client hasn't been deadlocked, add the replacement node
	// to the gossip network and send it a request.
	rttc.GossipNode(replacementReplica.NodeID, serverAddr)

	// Sending messages to the old store should still be safe.
	testutils.SucceedsSoon(t, func() error {
		if rttc.Send(clientReplica, serverReplica, 1, raftpb.Message{Commit: 1}) {
			return errors.Errorf("expected circuit breaker to trip")
		}
		return nil
	})

	// Keep sending commit=2 until breaker resets and we receive the
	// first instance. It's possible an earlier message for commit=1
	// snuck in.
	testutils.SucceedsSoon(t, func() error {
		if !rttc.Send(clientReplica, replacementReplica, 1, raftpb.Message{Commit: 2}) {
			t.Error("unexpectedly failed sending to replacement replica")
		}
		select {
		case req := <-replacementChannel.ch:
			if req.Message.Commit == 2 {
				return nil
			}
			return errors.Errorf("expected message commit=2, got %+v", req)
		default:
		}
		return errors.Errorf("expected message commit=2, got nothing")
	})
}
