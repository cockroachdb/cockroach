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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

const (
	// With the default gossip interval, some tests
	// may take longer than they need.
	gossipInterval = 20 * time.Millisecond
)

// startGossip creates local and remote gossip instances.
// The remote gossip instance launches its gossip service.
func startGossip(t *testing.T) (local, remote *Gossip, stopper *stop.Stopper) {
	lclock := hlc.NewClock(hlc.UnixNano)
	stopper = stop.NewStopper()
	lRPCContext := rpc.NewContext(nodeTestBaseContext, lclock, stopper)

	laddr := util.CreateTestAddr("tcp")
	lserver := rpc.NewServer(laddr, lRPCContext)
	if err := lserver.Start(); err != nil {
		t.Fatal(err)
	}
	local = New(lRPCContext, gossipInterval, TestBootstrap)
	if err := local.SetNodeDescriptor(&proto.NodeDescriptor{
		NodeID: 1,
		Address: proto.Addr{
			Network: laddr.Network(),
			Address: laddr.String(),
		}}); err != nil {
		t.Fatal(err)
	}
	rclock := hlc.NewClock(hlc.UnixNano)
	raddr := util.CreateTestAddr("tcp")
	rRPCContext := rpc.NewContext(nodeTestBaseContext, rclock, stopper)
	rserver := rpc.NewServer(raddr, rRPCContext)
	if err := rserver.Start(); err != nil {
		t.Fatal(err)
	}
	remote = New(rRPCContext, gossipInterval, TestBootstrap)
	if err := local.SetNodeDescriptor(&proto.NodeDescriptor{
		NodeID: 2,
		Address: proto.Addr{
			Network: raddr.Network(),
			Address: raddr.String(),
		},
	}); err != nil {
		t.Fatal(err)
	}
	local.start(lserver, stopper)
	remote.start(rserver, stopper)
	time.Sleep(time.Millisecond)
	return
}

// TestClientGossip verifies a client can gossip a delta to the server.
func TestClientGossip(t *testing.T) {
	defer leaktest.AfterTest(t)
	local, remote, stopper := startGossip(t)
	if err := local.AddInfo("local-key", "local value", time.Second); err != nil {
		t.Fatal(err)
	}
	if err := remote.AddInfo("remote-key", "remote value", time.Second); err != nil {
		t.Fatal(err)
	}
	disconnected := make(chan *client, 1)

	client := newClient(remote.is.NodeAddr)
	// Use an insecure context. We're talking to unix socket which are not in the certs.
	lclock := hlc.NewClock(hlc.UnixNano)
	rpcContext := rpc.NewContext(nodeTestBaseContext, lclock, stopper)
	client.start(local, disconnected, rpcContext, stopper)

	if err := util.IsTrueWithin(func() bool {
		_, lerr := remote.GetInfo("local-key")
		_, rerr := local.GetInfo("remote-key")
		return lerr == nil && rerr == nil
	}, 500*time.Millisecond); err != nil {
		t.Errorf("gossip exchange failed or taking too long")
	}

	stopper.Stop()
	log.Info("done serving")
	if client != <-disconnected {
		t.Errorf("expected client disconnect after remote close")
	}
}

// TestClientDisconnectRedundant verifies that the gossip server
// will drop an outgoing client connection that is already an
// inbound client connection of another node.
func TestClientDisconnectRedundant(t *testing.T) {
	defer leaktest.AfterTest(t)
	local, remote, stopper := startGossip(t)
	defer stopper.Stop()
	// startClient doesn't lock the underlying gossip
	// object, so we acquire those locks here.
	local.mu.Lock()
	remote.mu.Lock()
	rAddr := remote.is.NodeAddr
	lAddr := local.is.NodeAddr
	lclock := hlc.NewClock(hlc.UnixNano)
	rpcContext := rpc.NewContext(nodeTestBaseContext, lclock, stopper)
	local.startClient(rAddr, rpcContext, stopper)
	remote.startClient(lAddr, rpcContext, stopper)
	local.mu.Unlock()
	remote.mu.Unlock()
	local.manage(stopper)
	remote.manage(stopper)
	wasConnected1, wasConnected2 := false, false
	if err := util.IsTrueWithin(func() bool {
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
			return true
		}
		return false
	}, 10*time.Second); err != nil {
		t.Fatal(err)
	}
}
