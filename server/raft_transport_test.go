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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Timothy Chen

package server

import (
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/coreos/etcd/raft/raftpb"
)

type channelServer struct {
	ch       chan *multiraft.RaftMessageRequest
	maxSleep time.Duration
}

func newChannelServer(bufSize int, maxSleep time.Duration) channelServer {
	return channelServer{
		ch:       make(chan *multiraft.RaftMessageRequest, bufSize),
		maxSleep: maxSleep,
	}
}

func (s channelServer) RaftMessage(req *multiraft.RaftMessageRequest,
	resp *multiraft.RaftMessageResponse) error {
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
	g := gossip.New(nodeRPCContext, gossip.TestInterval, gossip.TestBootstrap)

	// Create several servers, each of which has two stores (A multiraft node ID addresses
	// a store).
	const numServers = 3
	const storesPerServer = 2
	const numStores = numServers * storesPerServer
	// servers has length numServers.
	servers := []*rpc.Server{}
	// All the rest have length numStores (note that several stores share a transport).
	nextNodeID := proto.NodeID(1)
	nodeIDs := []proto.RaftNodeID{}
	transports := []multiraft.Transport{}
	channels := []channelServer{}
	for serverIndex := 0; serverIndex < numServers; serverIndex++ {
		server := rpc.NewServer(util.CreateTestAddr("tcp"), nodeRPCContext)
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
		defer server.Close()

		transport, err := newRPCTransport(g, server, nodeRPCContext)
		if err != nil {
			t.Fatalf("Unexpected error creating transport, Error: %s", err)
		}
		defer transport.Close()

		for store := 0; store < storesPerServer; store++ {
			protoNodeID := nextNodeID
			nodeID := proto.MakeRaftNodeID(protoNodeID, 1)
			nextNodeID++

			channel := newChannelServer(10, 0)
			if err := transport.Listen(nodeID, channel); err != nil {
				t.Fatal(err)
			}

			addr := server.Addr()
			if err := g.AddInfo(gossip.MakeNodeIDKey(protoNodeID),
				&proto.NodeDescriptor{
					Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
				},
				time.Hour); err != nil {
				t.Fatal(err)
			}

			nodeIDs = append(nodeIDs, nodeID)
			transports = append(transports, transport)
			channels = append(channels, channel)
		}

		servers = append(servers, server)
	}

	// Each store sends one message to each store.
	for from := 0; from < numStores; from++ {
		for to := 0; to < numStores; to++ {
			req := &multiraft.RaftMessageRequest{
				GroupID: 1,
				Message: raftpb.Message{
					From: uint64(nodeIDs[from]),
					To:   uint64(nodeIDs[to]),
					Type: raftpb.MsgHeartbeat,
				},
			}

			if err := transports[from].Send(req); err != nil {
				t.Errorf("Unable to send message from %d to %d: %s", nodeIDs[from], nodeIDs[to], err)
			}
		}
	}

	// Read all the messages from the channels. Note that the transport
	// does not guarantee in-order delivery between independent
	// transports, so we just verify that the right number of messages
	// end up in each channel.
	for to := 0; to < numStores; to++ {
		for from := 0; from < numStores; from++ {
			select {
			case req := <-channels[to].ch:
				if req.Message.To != uint64(nodeIDs[to]) {
					t.Errorf("invalid message received on channel %d (expected from %d): %+v",
						nodeIDs[to], nodeIDs[from], req)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for message")
			}
		}

		select {
		case req := <-channels[to].ch:
			t.Errorf("got unexpected message %+v on channel %d", req, nodeIDs[to])
		default:
		}
	}
}

// TestInOrderDelivery verifies that for a given pair of nodes, raft
// messages are delivered in order.
func TestInOrderDelivery(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	nodeRPCContext := rpc.NewContext(nodeTestBaseContext, hlc.NewClock(hlc.UnixNano), stopper)
	g := gossip.New(nodeRPCContext, gossip.TestInterval, gossip.TestBootstrap)

	server := rpc.NewServer(util.CreateTestAddr("tcp"), nodeRPCContext)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	const numMessages = 100
	protoNodeID := proto.NodeID(1)
	raftNodeID := proto.MakeRaftNodeID(protoNodeID, 1)
	serverTransport, err := newRPCTransport(g, server, nodeRPCContext)
	if err != nil {
		t.Fatal(err)
	}
	defer serverTransport.Close()
	serverChannel := newChannelServer(numMessages, 10*time.Millisecond)
	if err := serverTransport.Listen(raftNodeID, serverChannel); err != nil {
		t.Fatal(err)
	}
	addr := server.Addr()
	if err := g.AddInfo(gossip.MakeNodeIDKey(protoNodeID),
		&proto.NodeDescriptor{
			Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
		},
		time.Hour); err != nil {
		t.Fatal(err)
	}

	clientNodeID := proto.MakeRaftNodeID(2, 2)
	clientTransport, err := newRPCTransport(g, nil, nodeRPCContext)
	if err != nil {
		t.Fatal(err)
	}
	defer clientTransport.Close()

	for i := 0; i < numMessages; i++ {
		req := &multiraft.RaftMessageRequest{
			GroupID: 1,
			Message: raftpb.Message{
				To:     uint64(raftNodeID),
				From:   uint64(clientNodeID),
				Commit: uint64(i),
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
