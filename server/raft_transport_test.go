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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/coreos/etcd/raft/raftpb"
)

type ChannelServer chan *multiraft.RaftMessageRequest

func (s ChannelServer) RaftMessage(req *multiraft.RaftMessageRequest,
	resp *multiraft.RaftMessageResponse) error {
	s <- req
	return nil
}

func TestSendAndReceive(t *testing.T) {
	rpcContext := rpc.NewContext(hlc.NewClock(hlc.UnixNano), rpc.LoadInsecureTLSConfig())
	g := gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)

	// Create several servers, each of which has two stores (A multiraft node ID addresses
	// a store).
	const numServers = 3
	const storesPerServer = 2
	const numStores = numServers * storesPerServer
	// servers has length numServers.
	servers := []*rpc.Server{}
	// All the rest have length numStores (note that several stores share a transport).
	nextNodeID := proto.NodeID(1)
	nodeIDs := []multiraft.NodeID{}
	transports := []multiraft.Transport{}
	channels := []ChannelServer{}
	for server := 0; server < numServers; server++ {
		server := rpc.NewServer(util.CreateTestAddr("tcp"), rpcContext)
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
		defer server.Close()

		transport, err := newRPCTransport(g, server, rpcContext)
		if err != nil {
			t.Fatalf("Unexpected error creating transport, Error: %s", err)
		}
		defer transport.Close()

		for store := 0; store < storesPerServer; store++ {
			protoNodeID := nextNodeID
			nodeID := storage.MakeRaftNodeID(protoNodeID, 1)
			nextNodeID++

			channel := make(ChannelServer, 10)
			if err := transport.Listen(nodeID, channel); err != nil {
				t.Fatal(err)
			}

			if err := g.AddInfo(gossip.MakeNodeIDKey(protoNodeID),
				&storage.NodeDescriptor{Address: server.Addr()},
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

			if err := transports[from].Send(multiraft.NodeID(nodeIDs[to]), req); err != nil {
				t.Errorf("Unable to send message from %d to %d: %s", nodeIDs[from], nodeIDs[to], err)
			}
		}
	}

	// Read all the messages from the channels.  Note that the transport does not
	// currently guarantee in-order delivery, so we just verify that the right number
	// of messages end up in each channel.
	for to := 0; to < numStores; to++ {
		for from := 0; from < numStores; from++ {
			select {
			case req := <-channels[to]:
				if req.Message.To != uint64(nodeIDs[to]) {
					t.Errorf("invalid message received on channel %d (expected from %d): %+v",
						nodeIDs[to], nodeIDs[from], req)
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for message")
			}
		}

		select {
		case req := <-channels[to]:
			t.Errorf("got unexpected message %+v on channel %d", req, nodeIDs[to])
		default:
		}
	}
}
