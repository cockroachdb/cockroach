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
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/coreos/etcd/raft/raftpb"
)

type TestTransportServer struct {
	messages chan *multiraft.RaftMessageRequest
}

func (s *TestTransportServer) RaftMessage(req *multiraft.RaftMessageRequest,
	resp *multiraft.RaftMessageResponse) error {
	s.messages <- req
	return nil
}

func TestSendAndReceive(t *testing.T) {
	rpcContext := rpc.NewContext(hlc.NewClock(hlc.UnixNano), rpc.LoadInsecureTLSConfig())
	gossip := gossip.New(rpcContext, gossip.TestInterval, "")
	//addr1 := util.CreateTestAddr("tcp")
	addr1, err := net.ResolveTCPAddr("tcp", "127.0.0.1:12345")
	if err != nil {
		t.Fatal(err)
	}
	rpcServer1 := rpc.NewServer(addr1, rpcContext)
	if err := rpcServer1.Start(); err != nil {
		t.Fatal(err)
	}
	transport1, err := NewRPCTransport(gossip, rpcServer1)
	if err != nil {
		t.Errorf("Unexpected error creating transport1, Error: %s", err)
	}

	addr2 := util.CreateTestAddr("tcp")
	rpcServer2 := rpc.NewServer(addr2, rpcContext)
	if err := rpcServer2.Start(); err != nil {
		t.Fatal(err)
	}
	transport2, err := NewRPCTransport(gossip, rpcServer2)
	if err != nil {
		t.Errorf("Unexpected error creating transport2, Error: %s", err)
	}

	if err := gossip.AddInfo("node-1", rpcServer1.Addr(), time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := gossip.AddInfo("node-2", rpcServer2.Addr(), time.Hour); err != nil {
		t.Fatal(err)
	}

	messages := make(chan *multiraft.RaftMessageRequest, 10)
	testServer := &TestTransportServer{messages}
	if err := transport1.Listen(1, testServer); err != nil {
		t.Fatal(err)
	}

	msg := raftpb.Message{
		From: uint64(2),
		To:   uint64(1),
		Type: raftpb.MsgHeartbeat,
	}

	req := &multiraft.RaftMessageRequest{
		GroupID: 1,
		Message: msg,
	}

	err = transport2.Send(multiraft.NodeID(1), req)

	if err != nil {
		t.Fatalf("Unable to send message to node 1, Error: %s", err)
	}

	foundReq := <-messages

	foundMsg := foundReq.Message

	if foundMsg.To != uint64(1) || foundMsg.From != uint64(2) || foundMsg.Type != raftpb.MsgHeartbeat {
		t.Errorf("Invalid message received from transport")
	}

}
