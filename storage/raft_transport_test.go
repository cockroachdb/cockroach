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
//
// Author: Timothy Chen

package storage_test

import (
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/grpcutil"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/netutil"
	"github.com/cockroachdb/cockroach/util/stop"
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

func (s channelServer) RaftMessage(req *storage.RaftMessageRequest) error {
	if s.maxSleep != 0 {
		// maxSleep simulates goroutine scheduling delays that could
		// result in messages being processed out of order (in previous
		// transport implementations).
		time.Sleep(time.Duration(rand.Int63n(int64(s.maxSleep))))
	}
	if s.brokenRange != 0 && s.brokenRange == req.RangeID {
		return errors.Errorf(channelServerBrokenRangeMessage)
	}
	s.ch <- req
	return nil
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
	rttc.nodeRPCContext = rpc.NewContext(testutils.NewNodeTestBaseContext(), nil, rttc.stopper)
	server := rpc.NewServer(rttc.nodeRPCContext) // never started
	rttc.gossip = gossip.New(rttc.nodeRPCContext, server, nil, rttc.stopper, metric.NewRegistry())
	rttc.gossip.SetNodeID(1)
	return rttc
}

func (rttc *raftTransportTestContext) Stop() {
	rttc.stopper.Stop()
}

// AddNode registers a node with the cluster. Nodes must be added
// before they can be used in other methods of
// raftTransportTestContext. The node will be gossiped immediately.
func (rttc *raftTransportTestContext) AddNode(nodeID roachpb.NodeID) *storage.RaftTransport {
	transport, addr := rttc.AddNodeWithoutGossip(nodeID)
	rttc.GossipNode(nodeID, addr)
	return transport
}

// AddNodeWithoutGossip registers a node with the cluster. Nodes must
// be added before they can be used in other methods of
// raftTransportTestContext. Unless you are testing the effects of
// delaying gossip, use AddNode instead.
func (rttc *raftTransportTestContext) AddNodeWithoutGossip(
	nodeID roachpb.NodeID,
) (*storage.RaftTransport, net.Addr) {
	grpcServer := rpc.NewServer(rttc.nodeRPCContext)
	ln, err := netutil.ListenAndServeGRPC(rttc.stopper, grpcServer, util.TestAddr)
	if err != nil {
		rttc.t.Fatal(err)
	}
	transport := storage.NewRaftTransport(storage.GossipAddressResolver(rttc.gossip),
		grpcServer, rttc.nodeRPCContext)
	rttc.transports[nodeID] = transport
	return transport, ln.Addr()
}

// GossipNode gossips the node's address, which is necessary before
// any messages can be sent to it. Normally done automatically by
// AddNode.
func (rttc *raftTransportTestContext) GossipNode(nodeID roachpb.NodeID, addr net.Addr) {
	if err := rttc.gossip.AddInfoProto(gossip.MakeNodeIDKey(nodeID),
		&roachpb.NodeDescriptor{
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
	rttc.transports[nodeID].Listen(storeID, ch.RaftMessage)
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
	sender := rttc.transports[from.NodeID].MakeSender(
		func(err error, _ roachpb.ReplicaDescriptor) {
			if err != nil && !grpcutil.IsClosedConnection(err) &&
				!testutils.IsError(err, channelServerBrokenRangeMessage) {
				rttc.t.Fatal(err)
			}
		})
	return sender.SendAsync(req)
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

	messageTypes := []raftpb.MessageType{
		raftpb.MsgSnap,
		raftpb.MsgHeartbeat,
	}

	for nodeIndex := 0; nodeIndex < numNodes; nodeIndex++ {
		nodeID := nextNodeID
		nextNodeID++
		transports[nodeID] = rttc.AddNode(nodeID)

		// This channel is normally unbuffered, but it is also normally serviced by
		// the raft goroutine. Since we don't have that goroutine in this test, we
		// must buffer the channel to prevent snapshots from blocking while we
		// iterate through the recipients in an order that may differ from the
		// sending order.
		sendersPerNode := storesPerNode
		recipientsPerSender := numNodes * storesPerNode
		outboundSnapshotsPerNode := sendersPerNode * recipientsPerSender
		transports[nodeID].SnapshotStatusChan = make(chan storage.RaftSnapshotStatus, outboundSnapshotsPerNode)

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

			for _, messageType := range messageTypes {
				req := baseReq
				req.Message.Type = messageType

				if !transports[fromNodeID].MakeSender(func(err error, _ roachpb.ReplicaDescriptor) {
					if err != nil && !grpcutil.IsClosedConnection(err) {
						panic(err)
					}
				}).SendAsync(&req) {
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
		func() {
			for len(messageTypeCounts[toStoreID]) > 0 {
				req := <-channels[toStoreID].ch
				if req.Message.To != uint64(toStoreID) {
					t.Errorf("got unexpected message %v on channel %d", req, toStoreID)
				}

				// Each MsgSnap should have a corresponding entry on the
				// sender's SnapshotStatusChan.
				if req.Message.Type == raftpb.MsgSnap {
					st := <-transports[req.FromReplica.NodeID].SnapshotStatusChan
					if st.Err != nil {
						t.Errorf("unexpected error sending snapshot: %s", st.Err)
					}
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
		}()

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
	if !transports[storeNodes[fromStoreID]].MakeSender(func(err error, _ roachpb.ReplicaDescriptor) {
		if err != nil && !grpcutil.IsClosedConnection(err) {
			panic(err)
		}
	}).SendAsync(expReq) {
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
	_, serverAddr := rttc.AddNodeWithoutGossip(serverReplica.NodeID)
	serverChannel := rttc.ListenStore(serverReplica.NodeID, serverReplica.StoreID)

	clientReplica := roachpb.ReplicaDescriptor{
		NodeID:    1,
		StoreID:   1,
		ReplicaID: 1,
	}
	clientTransport := rttc.AddNode(clientReplica.NodeID)

	if rttc.Send(clientReplica, serverReplica, 1, raftpb.Message{Commit: 1}) {
		t.Errorf("succeeded in sending when message should be dropped")
	}

	// None should go through as the receiving node's address has not been gossiped.
	select {
	case req := <-serverChannel.ch:
		t.Fatalf("should not have received any Raft messages from client: %s", req)
	default:
	}

	// Now, gossip address of server.
	rttc.GossipNode(serverReplica.NodeID, serverAddr)

	// Message was dropped, not queued, so still shouldn't just appear.
	select {
	case req := <-serverChannel.ch:
		t.Fatalf("should not have received any Raft messages from client: %s", req)
	default:
	}

	// Reset the circuit breaker & resend and verify message arrives at server.
	clientTransport.GetCircuitBreaker(serverReplica.NodeID).Reset()
	if !rttc.Send(clientReplica, serverReplica, 1, raftpb.Message{Commit: 1}) {
		t.Errorf("sent raft message was unexpectedly dropped")
	}

	req := <-serverChannel.ch
	if req.Message.Commit != 1 {
		t.Errorf("expected message 1; got %s", req)
	}
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
	serverTransport.Listen(server.StoreID, channelServer.RaftMessage)

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
