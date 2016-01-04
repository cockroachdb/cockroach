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

package server

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/coreos/etcd/raft/raftpb"
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
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	nodeRPCContext := rpc.NewContext(nodeTestBaseContext, hlc.NewClock(hlc.UnixNano), stopper)
	g := gossip.New(nodeRPCContext, gossip.TestBootstrap)
	g.SetNodeID(roachpb.NodeID(1))

	// Create several servers, each of which has two stores (A raft
	// node ID addresses a store). Node 1 has stores 1 and 2, node 2 has
	// stores 3 and 4, etc.
	//
	// We suppose that range 1 is replicated across the odd-numbered
	// stores in reverse order to ensure that the various IDs are not
	// equal: replica 1 is store 5, replica 2 is store 3, and replica 3
	// is store 1.
	const numServers = 3
	const storesPerServer = 2
	const numStores = numServers * storesPerServer
	nextNodeID := roachpb.NodeID(2)
	nextStoreID := roachpb.StoreID(2)

	// Per-node state.
	transports := map[roachpb.NodeID]storage.RaftTransport{}

	// Per-store state.
	storeNodes := map[roachpb.StoreID]roachpb.NodeID{}
	channels := map[roachpb.StoreID]channelServer{}
	replicaIDs := map[roachpb.StoreID]roachpb.ReplicaID{
		1: 3,
		3: 2,
		5: 1,
	}

	for serverIndex := 0; serverIndex < numServers; serverIndex++ {
		nodeID := nextNodeID
		nextNodeID++
		server := rpc.NewServer(nodeRPCContext)
		tlsConfig, err := nodeRPCContext.GetServerTLSConfig()
		if err != nil {
			t.Fatal(err)
		}
		ln, err := util.ListenAndServe(stopper, server, util.CreateTestAddr("tcp"), tlsConfig)
		if err != nil {
			t.Fatal(err)
		}

		addr := ln.Addr()
		// Have to call g.SetNodeID before call g.AddInfo
		g.SetNodeID(roachpb.NodeID(nodeID))
		if err := g.AddInfoProto(gossip.MakeNodeIDKey(nodeID),
			&roachpb.NodeDescriptor{
				Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
			},
			time.Hour); err != nil {
			t.Fatal(err)
		}

		transport, err := newRPCTransport(g, server, nodeRPCContext)
		if err != nil {
			t.Fatalf("Unexpected error creating transport, Error: %s", err)
		}
		defer transport.Close()
		transports[nodeID] = transport

		for store := 0; store < storesPerServer; store++ {
			storeID := nextStoreID
			nextStoreID++

			storeNodes[storeID] = nodeID

			channel := newChannelServer(10, 0)
			if err := transport.Listen(storeID, channel.RaftMessage); err != nil {
				t.Fatal(err)
			}
			channels[storeID] = channel
		}
	}

	// Heartbeat messages: Each store sends one message to each store.
	for fromStoreID, fromNodeID := range storeNodes {
		for toStoreID, toNodeID := range storeNodes {
			req := &storage.RaftMessageRequest{
				GroupID: 0,
				Message: raftpb.Message{
					Type: raftpb.MsgHeartbeat,
					From: uint64(fromStoreID),
					To:   uint64(toStoreID),
				},
				FromReplica: roachpb.ReplicaDescriptor{
					NodeID:    fromNodeID,
					StoreID:   fromStoreID,
					ReplicaID: 0,
				},
				ToReplica: roachpb.ReplicaDescriptor{
					NodeID:    toNodeID,
					StoreID:   toStoreID,
					ReplicaID: 0,
				},
			}

			if err := transports[fromNodeID].Send(req); err != nil {
				t.Errorf("Unable to send message from %d to %d: %s", fromNodeID, toNodeID, err)
			}
		}
	}

	// Read all the messages from the channels. Note that the transport
	// does not guarantee in-order delivery between independent
	// transports, so we just verify that the right number of messages
	// end up in each channel.
	for toStoreID := range storeNodes {
		for range storeNodes {
			select {
			case req := <-channels[toStoreID].ch:
				if req.Message.To != uint64(toStoreID) {
					t.Errorf("invalid message received on channel %d: %+v",
						toStoreID, req)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for message")
			}
		}

		select {
		case req := <-channels[toStoreID].ch:
			t.Errorf("got unexpected message %+v on channel %d", req, toStoreID)
		default:
		}
	}

	// Real raft messages have different node/store/replica IDs.
	// Send a message from replica 2 (on store 3, node 2) to replica 1 (on store 5, node 3)
	fromStoreID := roachpb.StoreID(3)
	toStoreID := roachpb.StoreID(5)
	req := &storage.RaftMessageRequest{
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
	if err := transports[storeNodes[fromStoreID]].Send(req); err != nil {
		t.Errorf("Unable to send message from %d to %d: %s", fromStoreID, toStoreID, err)
	}
	select {
	case req2 := <-channels[toStoreID].ch:
		if !reflect.DeepEqual(req, req2) {
			t.Errorf("got unexpected message %+v", req2)
		}

	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	select {
	case req := <-channels[toStoreID].ch:
		t.Errorf("got unexpected message %+v on channel %d", req, toStoreID)
	default:
	}
}

// TestInOrderDelivery verifies that for a given pair of nodes, raft
// messages are delivered in order.
func TestInOrderDelivery(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	nodeRPCContext := rpc.NewContext(nodeTestBaseContext, hlc.NewClock(hlc.UnixNano), stopper)
	g := gossip.New(nodeRPCContext, gossip.TestBootstrap)
	g.SetNodeID(roachpb.NodeID(1))

	server := rpc.NewServer(nodeRPCContext)
	tlsConfig, err := nodeRPCContext.GetServerTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := util.ListenAndServe(stopper, server, util.CreateTestAddr("tcp"), tlsConfig)
	if err != nil {
		t.Fatal(err)
	}

	const numMessages = 100
	nodeID := roachpb.NodeID(roachpb.NodeID(2))
	serverTransport, err := newRPCTransport(g, server, nodeRPCContext)
	if err != nil {
		t.Fatal(err)
	}
	defer serverTransport.Close()
	serverChannel := newChannelServer(numMessages, 10*time.Millisecond)
	if err := serverTransport.Listen(roachpb.StoreID(nodeID), serverChannel.RaftMessage); err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr()
	// Have to set gossip.NodeID before call gossip.AddInofXXX
	g.SetNodeID(nodeID)
	if err := g.AddInfoProto(gossip.MakeNodeIDKey(nodeID),
		&roachpb.NodeDescriptor{
			Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
		},
		time.Hour); err != nil {
		t.Fatal(err)
	}

	clientNodeID := roachpb.NodeID(2)
	clientTransport, err := newRPCTransport(g, nil, nodeRPCContext)
	if err != nil {
		t.Fatal(err)
	}
	defer clientTransport.Close()

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
