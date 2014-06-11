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

package rpc

import (
	"net"
	"net/rpc"
	"testing"
	"time"
)

const testAddr = "localhost:0"

func TestClientHeartbeat(t *testing.T) {
	heartbeatInterval = 10 * time.Millisecond
	addr, err := net.ResolveTCPAddr("tcp", testAddr)
	if err != nil {
		t.Fatalf("invalid test server address %s: %s", testAddr, err)
	}
	s := NewServer(addr)
	s.Start()
	c := NewClient(s.Addr())
	time.Sleep(heartbeatInterval * 2)
	if c != NewClient(s.Addr()) {
		t.Error("expected cached client to be returned while healthy")
	}
	<-c.Ready
	s.Close()
	c.Close()
}

// TestClientHeartbeatBadServer verifies that the client is not marked
// as "ready" until a heartbeat request succeeds.
func TestClientHeartbeatBadServer(t *testing.T) {
	heartbeatInterval = 10 * time.Millisecond
	addr, err := net.ResolveTCPAddr("tcp", testAddr)
	if err != nil {
		t.Fatalf("invalid test server address %s: %s", testAddr, err)
	}
	// Create a server which doesn't support heartbeats.
	s := &Server{
		Server:         rpc.NewServer(),
		addr:           addr,
		closeCallbacks: make([]func(conn net.Conn), 0, 1),
	}
	s.Start()

	// Now, create a client. It should attempt a heartbeat and fail,
	// causing retry loop to activate.
	c := NewClient(s.Addr())
	select {
	case <-c.Ready:
		t.Error("unexpected client heartbeat success")
	case <-time.After(10 * time.Millisecond):
		// TODO(spencer): this isn't so great, as it relies on client
		// being able to connect in 10ms. Would be better to wait on
		// a notification of the client having attempted the heartbeat
		// and failed.
	}
	s.Close()
	c.Close()
}
