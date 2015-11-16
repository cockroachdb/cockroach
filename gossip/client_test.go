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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

// startGossip creates local and remote gossip instances.
// The remote gossip instance launches its gossip service.
func startGossip(t *testing.T) (local, remote *Gossip, stopper *stop.Stopper) {
	lclock := hlc.NewClock(hlc.UnixNano)
	stopper = stop.NewStopper()
	lRPCContext := rpc.NewContext(&base.Context{Insecure: true}, lclock, stopper)

	laddr := util.CreateTestAddr("tcp")
	lserver := rpc.NewServer(laddr, lRPCContext)
	if err := lserver.Start(); err != nil {
		t.Fatal(err)
	}
	local = New(lRPCContext, TestBootstrap)
	if err := local.SetNodeDescriptor(&roachpb.NodeDescriptor{
		NodeID:  1,
		Address: util.MakeUnresolvedAddr(laddr.Network(), laddr.String()),
	}); err != nil {
		t.Fatal(err)
	}
	rclock := hlc.NewClock(hlc.UnixNano)
	raddr := util.CreateTestAddr("tcp")
	rRPCContext := rpc.NewContext(&base.Context{Insecure: true}, rclock, stopper)
	rserver := rpc.NewServer(raddr, rRPCContext)
	if err := rserver.Start(); err != nil {
		t.Fatal(err)
	}
	remote = New(rRPCContext, TestBootstrap)
	if err := remote.SetNodeDescriptor(&roachpb.NodeDescriptor{
		NodeID:  2,
		Address: util.MakeUnresolvedAddr(raddr.Network(), raddr.String()),
	}); err != nil {
		t.Fatal(err)
	}
	local.start(lserver, stopper)
	remote.start(rserver, stopper)
	time.Sleep(time.Millisecond)
	return
}

type fakeGossipServer struct {
	mu           sync.Mutex
	nodeID       roachpb.NodeID
	nodeAddr     util.UnresolvedAddr
	gotFirstMsg  bool
	firstMsgChan chan struct{}
}

func newFakeGossipServer(rpcServer *rpc.Server, stopper *stop.Stopper) (*fakeGossipServer, error) {
	s := &fakeGossipServer{
		firstMsgChan: make(chan struct{}),
	}
	if err := rpcServer.Register("Gossip.Gossip", s.Gossip, &Request{}); err != nil {
		return nil, util.Errorf("unable to register gossip service with RPC server: %s", err)
	}
	return s, nil
}

func (s *fakeGossipServer) Gossip(argsI proto.Message) (proto.Message, error) {
	args := argsI.(*Request)
	reply := &Response{}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeID = args.NodeID
	if !s.gotFirstMsg {
		s.gotFirstMsg = true
		close(s.firstMsgChan)
	}

	return reply, nil
}

func (s *fakeGossipServer) getNodeID() roachpb.NodeID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nodeID
}

// startFakeServerGossip creates local gossip instances and remote faked gossip instance.
// The remote gossip instance launches its faked gossip service just for
// check the client message.
func startFakeServerGossip(t *testing.T) (local *Gossip, remote *fakeGossipServer, stopper *stop.Stopper) {
	lclock := hlc.NewClock(hlc.UnixNano)
	stopper = stop.NewStopper()
	lRPCContext := rpc.NewContext(&base.Context{Insecure: true}, lclock, stopper)

	laddr := util.CreateTestAddr("tcp")
	lserver := rpc.NewServer(laddr, lRPCContext)
	if err := lserver.Start(); err != nil {
		t.Fatal(err)
	}
	local = New(lRPCContext, TestBootstrap)
	local.start(lserver, stopper)

	rclock := hlc.NewClock(hlc.UnixNano)
	raddr := util.CreateTestAddr("tcp")
	rRPCContext := rpc.NewContext(&base.Context{Insecure: true}, rclock, stopper)
	rserver := rpc.NewServer(raddr, rRPCContext)
	if err := rserver.Start(); err != nil {
		t.Fatal(err)
	}

	remote, err := newFakeGossipServer(rserver, stopper)
	if err != nil {
		t.Fatal(err)
	}
	addr := rserver.Addr()
	remote.nodeAddr = util.MakeUnresolvedAddr(addr.Network(), addr.String())
	time.Sleep(time.Millisecond)
	return
}

// TestClientGossip verifies a client can gossip a delta to the server.
func TestClientGossip(t *testing.T) {
	defer leaktest.AfterTest(t)
	local, remote, stopper := startGossip(t)
	disconnected := make(chan *client, 1)
	client := newClient(remote.is.NodeAddr)

	defer func() {
		stopper.Stop()
		if client != <-disconnected {
			t.Errorf("expected client disconnect after remote close")
		}
	}()

	if err := local.AddInfo("local-key", nil, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := remote.AddInfo("remote-key", nil, time.Second); err != nil {
		t.Fatal(err)
	}

	// Use an insecure context. We're talking to unix socket which are not in the certs.
	lclock := hlc.NewClock(hlc.UnixNano)
	rpcContext := rpc.NewContext(&base.Context{Insecure: true}, lclock, stopper)
	client.start(local, disconnected, rpcContext, stopper)

	util.SucceedsWithin(t, 500*time.Millisecond, func() error {
		if _, err := remote.GetInfo("local-key"); err != nil {
			return err
		}
		if _, err := local.GetInfo("remote-key"); err != nil {
			return err
		}
		return nil
	})
}

// TestClientNodeID verifies a client's gossip request with correct NodeID.
func TestClientNodeID(t *testing.T) {
	defer leaktest.AfterTest(t)

	local, remote, stopper := startFakeServerGossip(t)
	disconnected := make(chan *client, 1)

	// Use an insecure context. We're talking to unix socket which are not in the certs.
	lclock := hlc.NewClock(hlc.UnixNano)
	rpcContext := rpc.NewContext(&base.Context{Insecure: true}, lclock, stopper)

	// Start a gossip client.
	c := newClient(remote.nodeAddr)
	defer func() {
		stopper.Stop()
		if c != <-disconnected {
			t.Errorf("expected client disconnect after remote close")
		}
	}()
	c.start(local, disconnected, rpcContext, stopper)
	// Wait for c.gossip to start.
	<-remote.firstMsgChan
	nodeID := roachpb.NodeID(1)
	// Simulate a nodeID setting after c.gossip started.
	local.SetNodeID(nodeID)
	// Ignore the msg with old nodeID, Wait for the new nodeID to take effect.
	time.Sleep(10 * time.Millisecond)
	// Check to see if client send the correct NodeID.
	if remote.getNodeID() != nodeID {
		t.Fatalf("client should send NodeID with %v, got %v", nodeID, remote.getNodeID())
	}
}
