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
	"testing"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

type channelServer struct {
	ch       chan *storage.RaftMessageRequest
	maxSleep time.Duration
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
	s.ch <- req
	return nil
}

func TestSendAndReceive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	nodeRPCContext := rpc.NewContext(testutils.NewNodeTestBaseContext(), nil, stopper)
	g := gossip.New(nodeRPCContext, nil, stopper)
	g.SetNodeID(roachpb.NodeID(1))

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
		grpcServer := rpc.NewServer(nodeRPCContext)
		ln, err := util.ListenAndServeGRPC(stopper, grpcServer, util.TestAddr)
		if err != nil {
			t.Fatal(err)
		}

		addr := ln.Addr()
		// Have to call g.SetNodeID before call g.AddInfo.
		g.ResetNodeID(roachpb.NodeID(nodeID))
		if err := g.AddInfoProto(gossip.MakeNodeIDKey(nodeID),
			&roachpb.NodeDescriptor{
				Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
			},
			time.Hour); err != nil {
			t.Fatal(err)
		}

		transport := storage.NewRaftTransport(storage.GossipAddressResolver(g), grpcServer, nodeRPCContext)
		transports[nodeID] = transport

		for storeIndex := 0; storeIndex < storesPerNode; storeIndex++ {
			storeID := nextStoreID
			nextStoreID++

			storeNodes[storeID] = nodeID

			channel := newChannelServer(numNodes*storesPerNode*len(messageTypes), 0)
			transport.Listen(storeID, channel.RaftMessage)
			channels[storeID] = channel
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

				if err := transports[fromNodeID].Send(&req); err != nil {
					t.Errorf("unable to send %s from %d to %d: %s", req.Message.Type, fromNodeID, toNodeID, err)
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
		GroupID: 1,
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
	if err := transports[storeNodes[fromStoreID]].Send(expReq); err != nil {
		t.Errorf("unable to send message from %d to %d: %s", fromStoreID, toStoreID, err)
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
	stopper := stop.NewStopper()
	defer stopper.Stop()
	nodeRPCContext := rpc.NewContext(testutils.NewNodeTestBaseContext(), nil, stopper)
	g := gossip.New(nodeRPCContext, nil, stopper)

	grpcServer := rpc.NewServer(nodeRPCContext)
	ln, err := util.ListenAndServeGRPC(stopper, grpcServer, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}

	const numMessages = 100
	nodeID := roachpb.NodeID(roachpb.NodeID(2))
	serverTransport := storage.NewRaftTransport(storage.GossipAddressResolver(g), grpcServer, nodeRPCContext)
	serverChannel := newChannelServer(numMessages, 10*time.Millisecond)
	serverTransport.Listen(roachpb.StoreID(nodeID), serverChannel.RaftMessage)
	addr := ln.Addr()
	// Have to set gossip.NodeID before calling gossip.AddInfoXXX.
	g.SetNodeID(nodeID)
	if err := g.AddInfoProto(gossip.MakeNodeIDKey(nodeID),
		&roachpb.NodeDescriptor{
			Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
		},
		time.Hour); err != nil {
		t.Fatal(err)
	}

	clientNodeID := roachpb.NodeID(2)
	clientTransport := storage.NewRaftTransport(storage.GossipAddressResolver(g), nil, nodeRPCContext)

	for i := 0; i < numMessages; i++ {
		req := &storage.RaftMessageRequest{
			GroupID: 1,
			Message: raftpb.Message{
				To:     uint64(nodeID),
				From:   uint64(clientNodeID),
				Commit: uint64(i),
			},
			ToReplica: roachpb.ReplicaDescriptor{
				NodeID:    nodeID,
				StoreID:   roachpb.StoreID(nodeID),
				ReplicaID: roachpb.ReplicaID(nodeID),
			},
			FromReplica: roachpb.ReplicaDescriptor{
				NodeID:    clientNodeID,
				StoreID:   roachpb.StoreID(clientNodeID),
				ReplicaID: roachpb.ReplicaID(clientNodeID),
			},
		}
		if err := clientTransport.Send(req); err != nil {
			t.Errorf("failed to send message %d: %s", i, err)
		}
	}

	for i := 0; i < numMessages; i++ {
		req := <-serverChannel.ch
		if req.Message.Commit != uint64(i) {
			t.Errorf("messages out of order: got %d while expecting %d", req.Message.Commit, i)
		}
	}
}
