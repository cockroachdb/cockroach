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
	"net"
	"testing"
	"time"
)

func waitFor(cond func() bool, desc string, t *testing.T) {
	const maxTime = 500 * time.Millisecond
	for elapsed := 0 * time.Nanosecond; elapsed < maxTime; {
		if cond() {
			return
		}
		time.Sleep(maxTime / 100)
		elapsed += maxTime / 100
	}
	t.Errorf("exceeded %s waiting for %s", maxTime, desc)
}

// startGossip creates local and remote gossip instances.
// The remote gossip instance launches its gossip service.
func startGossip(t *testing.T) (local, remote *Gossip) {
	laddr := &net.UnixAddr{Net: "unix", Name: tempUnixFile()}
	local = New(laddr)
	raddr := &net.UnixAddr{Net: "unix", Name: tempUnixFile()}
	remote = New(raddr)
	go remote.serve()
	time.Sleep(time.Millisecond)
	return
}

// TestClientGossip verifies a client can gossip a delta to the server.
func TestClientGossip(t *testing.T) {
	local, remote := startGossip(t)
	local.AddInfo("local-key", StringValue("local value"), time.Second)
	remote.AddInfo("remote-key", StringValue("remote value"), time.Second)
	disconnected := make(chan *client, 1)

	client := newClient(remote.is.NodeAddr)
	go client.start(local, disconnected)

	waitFor(func() bool {
		return remote.GetInfo("local-key") != nil && local.GetInfo("remote-key") != nil
	}, "gossip exchange", t)

	remote.stopServing()
	if client != <-disconnected {
		t.Errorf("expected client disconnect after remote close")
	}
}
