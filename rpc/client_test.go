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

	"github.com/cockroachdb/cockroach/util"
)

func init() {
	// Setting the heartbeat interval in individual tests
	// triggers the race detector, so this is better for now.
	heartbeatInterval = 10 * time.Millisecond
}

func TestClientHeartbeat(t *testing.T) {
	addr := util.CreateTestAddr("tcp")
	s := NewServer(addr)
	s.Start()
	c := NewClient(s.Addr(), nil)
	time.Sleep(heartbeatInterval * 2)
	if c != NewClient(s.Addr(), nil) {
		t.Error("expected cached client to be returned while healthy")
	}
	<-c.Ready
	s.Close()
}

// TestClientHeartbeatBadServer verifies that the client is not marked
// as "ready" until a heartbeat request succeeds.
func TestClientHeartbeatBadServer(t *testing.T) {
	addr := util.CreateTestAddr("tcp")
	// Create a server which doesn't support heartbeats.
	s := &Server{
		Server:         rpc.NewServer(),
		addr:           addr,
		closeCallbacks: make([]func(conn net.Conn), 0, 1),
	}
	s.Start()

	// Now, create a client. It should attempt a heartbeat and fail,
	// causing retry loop to activate.
	c := NewClient(s.Addr(), nil)
	select {
	case <-c.Ready:
		t.Error("unexpected client heartbeat success")
	case <-c.Closed:
	}
	s.Close()
}
