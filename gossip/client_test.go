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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/golang/glog"
)

func init() {
	// With the default gossip interval, some tests
	// may take longer than they need.
	*GossipInterval = 20 * time.Millisecond
}

// startGossip creates local and remote gossip instances.
// The remote gossip instance launches its gossip service.
func startGossip(t *testing.T) (local, remote *Gossip, lserver, rserver *rpc.Server) {
	laddr := util.CreateTestAddr("unix")
	lserver = rpc.NewServer(laddr)
	lserver.Start()
	local = New()
	raddr := util.CreateTestAddr("unix")
	rserver = rpc.NewServer(raddr)
	rserver.Start()
	remote = New()
	local.start(lserver)
	remote.start(rserver)
	time.Sleep(time.Millisecond)
	return
}

// TestClientGossip verifies a client can gossip a delta to the server.
func TestClientGossip(t *testing.T) {
	local, remote, lserver, rserver := startGossip(t)
	local.AddInfo("local-key", "local value", time.Second)
	remote.AddInfo("remote-key", "remote value", time.Second)
	disconnected := make(chan *client, 1)

	client := newClient(remote.is.NodeAddr)
	go client.start(local, disconnected)

	if err := util.IsTrueWithin(func() bool {
		_, lerr := remote.GetInfo("local-key")
		_, rerr := local.GetInfo("remote-key")
		return lerr == nil && rerr == nil
	}, 500*time.Millisecond); err != nil {
		t.Errorf("gossip exchange failed or taking too long")
	}

	remote.stop()
	local.stop()
	lserver.Close()
	rserver.Close()
	glog.Info("done serving")
	if client != <-disconnected {
		t.Errorf("expected client disconnect after remote close")
	}
}

// TestClientDisconnectRedundant verifies that the gossip server
// will drop an outgoing client connection that is already an
// inbound client connection of another node.
func TestClientDisconnectRedundant(t *testing.T) {
	local, remote, _, _ := startGossip(t)
	// startClient doesn't lock the underlying gossip
	// object, so we acquire those locks here.
	local.mu.Lock()
	remote.mu.Lock()
	rAddr := remote.is.NodeAddr
	lAddr := local.is.NodeAddr
	local.startClient(rAddr)
	remote.startClient(lAddr)
	local.mu.Unlock()
	remote.mu.Unlock()
	go local.manage()
	go remote.manage()
	wasConnected1, wasConnected2 := false, false
	if err := util.IsTrueWithin(func() bool {
		// Check which of the clients is connected to the other.
		local.clientsMu.Lock()
		defer local.clientsMu.Unlock()
		remote.clientsMu.Lock()
		defer remote.clientsMu.Unlock()
		_, ok1 := local.clients[rAddr.String()]
		_, ok2 := remote.clients[lAddr.String()]
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
	}, 500*time.Millisecond); err != nil {
		t.Fatalf("timeout reached before redundant client connection was closed")
	}
}
