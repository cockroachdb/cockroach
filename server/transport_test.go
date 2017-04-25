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
	gossip := gossip.New(rpcContext)
	addr1 := util.CreateTestAddr("tcp")
	rpcServer1 := rpc.NewServer(addr1, rpcContext)
	rpcServer1.Start()
	transport1, err := NewRPCTransport(gossip, rpcServer1)
	if err != nil {
		t.Errorf("Unexpected error creating transport1, Error: %s", err)
	}

	addr2 := util.CreateTestAddr("tcp")
	rpcServer2 := rpc.NewServer(addr2, rpcContext)
	rpcServer2.Start()
	transport2, err := NewRPCTransport(gossip, rpcServer2)
	if err != nil {
		t.Errorf("Unexpected error creating transport2, Error: %s", err)
	}

	gossip.AddInfo("node-1", rpcServer1.Addr(), time.Hour)
	gossip.AddInfo("node-2", rpcServer2.Addr(), time.Hour)

	messages := make(chan *multiraft.RaftMessageRequest)
	testServer := &TestTransportServer{messages}
	transport1.Listen(1, testServer)

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
		t.Errorf("Unable to send message to node 1, Error: %s", err)
	}

	foundReq := <-messages

	foundMsg := foundReq.Message

	if foundMsg.To != uint64(1) || foundMsg.From != uint64(2) || foundMsg.Type != raftpb.MsgHeartbeat {
		t.Errorf("Invalid message received from transport")
	}

}
