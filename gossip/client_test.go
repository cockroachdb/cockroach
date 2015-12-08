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
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	netutil "github.com/cockroachdb/cockroach/util/net"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

// startGossip creates local and remote gossip instances.
// The remote gossip instance launches its gossip service.
func startGossip(t *testing.T) (local, remote *Gossip, stopper *stop.Stopper) {
	stopper = stop.NewStopper()
	lclock := hlc.NewClock(hlc.UnixNano)
	lRPCContext := rpc.NewContext(&base.Context{Insecure: true}, lclock, stopper)

	laddr := util.CreateTestAddr("tcp")
	lserver := rpc.NewServer(lRPCContext)
	lTLSConfig, err := lRPCContext.GetServerTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	lln, err := netutil.ListenAndServe(stopper, lserver, laddr, lTLSConfig)
	if err != nil {
		t.Fatal(err)
	}
	local = New(lRPCContext, TestBootstrap)
	local.SetNodeID(1)
	if err := local.SetNodeDescriptor(&roachpb.NodeDescriptor{
		NodeID:  1,
		Address: util.MakeUnresolvedAddr(laddr.Network(), laddr.String()),
	}); err != nil {
		t.Fatal(err)
	}
	rclock := hlc.NewClock(hlc.UnixNano)
	rRPCContext := rpc.NewContext(&base.Context{Insecure: true}, rclock, stopper)

	raddr := util.CreateTestAddr("tcp")
	rserver := rpc.NewServer(rRPCContext)
	rTLSConfig, err := rRPCContext.GetServerTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	rln, err := netutil.ListenAndServe(stopper, rserver, raddr, rTLSConfig)
	if err != nil {
		t.Fatal(err)
	}
	remote = New(rRPCContext, TestBootstrap)
	remote.SetNodeID(2)
	if err := remote.SetNodeDescriptor(&roachpb.NodeDescriptor{
		NodeID:  2,
		Address: util.MakeUnresolvedAddr(raddr.Network(), raddr.String()),
	}); err != nil {
		t.Fatal(err)
	}
	local.start(lserver, lln.Addr(), stopper)
	remote.start(rserver, rln.Addr(), stopper)
	time.Sleep(time.Millisecond)
	return
}

type fakeGossipServer struct {
	nodeAddr   util.UnresolvedAddr
	nodeIDChan chan roachpb.NodeID
}

func newFakeGossipServer(rpcServer *rpc.Server, stopper *stop.Stopper) (*fakeGossipServer, error) {
	s := &fakeGossipServer{
		nodeIDChan: make(chan roachpb.NodeID),
	}
	if err := rpcServer.Register("Gossip.Gossip", s.Gossip, &Request{}); err != nil {
		return nil, util.Errorf("unable to register gossip service with RPC server: %s", err)
	}
	return s, nil
}

func (s *fakeGossipServer) Gossip(argsI proto.Message) (proto.Message, error) {
	args := argsI.(*Request)
	reply := &Response{}
	s.nodeIDChan <- args.NodeID

	return reply, nil
}

// startFakeServerGossip creates local gossip instances and remote faked gossip instance.
// The remote gossip instance launches its faked gossip service just for
// check the client message.
func startFakeServerGossip(t *testing.T) (local *Gossip, remote *fakeGossipServer, stopper *stop.Stopper) {
	stopper = stop.NewStopper()
	lclock := hlc.NewClock(hlc.UnixNano)
	lRPCContext := rpc.NewContext(&base.Context{Insecure: true}, lclock, stopper)

	laddr := util.CreateTestAddr("tcp")
	lserver := rpc.NewServer(lRPCContext)
	lTLSConfig, err := lRPCContext.GetServerTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	lln, err := netutil.ListenAndServe(stopper, lserver, laddr, lTLSConfig)
	if err != nil {
		t.Fatal(err)
	}
	local = New(lRPCContext, TestBootstrap)
	local.start(lserver, lln.Addr(), stopper)

	rclock := hlc.NewClock(hlc.UnixNano)
	rRPCContext := rpc.NewContext(&base.Context{Insecure: true}, rclock, stopper)

	raddr := util.CreateTestAddr("tcp")
	rserver := rpc.NewServer(rRPCContext)
	rTLSConfig, err := rRPCContext.GetServerTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	rln, err := netutil.ListenAndServe(stopper, rserver, raddr, rTLSConfig)
	if err != nil {
		t.Fatal(err)
	}

	if remote, err = newFakeGossipServer(rserver, stopper); err != nil {
		t.Fatal(err)
	}
	addr := rln.Addr()
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

	// Use an insecure context. We're talking to tcp socket which are not in the certs.
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

	// Use an insecure context. We're talking to tcp socket which are not in the certs.
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
	receivedNodeID := <-remote.nodeIDChan

	nodeID := roachpb.NodeID(1)
	// Simulate a nodeID setting after c.gossip started.
	local.SetNodeID(nodeID)
	// Check if client send the correct NodeID after new nodeID take effect.
	util.SucceedsWithin(t, time.Second, func() error {
		select {
		case receivedNodeID = <-remote.nodeIDChan:
			if receivedNodeID == nodeID {
				return nil
			}
		}
		return util.Errorf("client should send NodeID with %v, got %v", nodeID, receivedNodeID)
	})
}

// TestClientDisconnectRedundant verifies that the gossip server
// will drop an outgoing client connection that is already an
// inbound client connection of another node.
func TestClientDisconnectRedundant(t *testing.T) {
	defer leaktest.AfterTest(t)
	t.Skip("TODO(spencer): #3350")
	local, remote, stopper := startGossip(t)
	defer stopper.Stop()
	// startClient doesn't lock the underlying gossip
	// object, so we acquire those locks here.
	local.mu.Lock()
	remote.mu.Lock()
	rAddr := remote.is.NodeAddr
	lAddr := local.is.NodeAddr
	lclock := hlc.NewClock(hlc.UnixNano)
	rpcContext := rpc.NewContext(&base.Context{Insecure: true}, lclock, stopper)
	local.startClient(rAddr, rpcContext, stopper)
	remote.startClient(lAddr, rpcContext, stopper)
	local.mu.Unlock()
	remote.mu.Unlock()
	local.manage(stopper)
	remote.manage(stopper)
	wasConnected1, wasConnected2 := false, false
	util.SucceedsWithin(t, 10*time.Second, func() error {
		// Check which of the clients is connected to the other.
		ok1 := local.findClient(func(c *client) bool { return c.addr.String() == rAddr.String() }) != nil
		ok2 := remote.findClient(func(c *client) bool { return c.addr.String() == lAddr.String() }) != nil
		if ok1 {
			wasConnected1 = true
		}
		if ok2 {
			wasConnected2 = true
		}
		// Check if at some point both nodes were connected to
		// each other, but now aren't any more.
		// Unfortunately it's difficult to get a more direct
		// read on what's happening without really messing with
		// the internals.
		if wasConnected1 && wasConnected2 && (!ok1 || !ok2) {
			return nil
		}
		return errors.New("not connected")
	})
}
