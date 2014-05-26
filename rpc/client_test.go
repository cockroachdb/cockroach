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
	go s.ListenAndServe()
	// Delve into rpc server struct to ensure we get correct address since we're
	// picking an unused port. If we don't wait for the server to start listening,
	// the address will be incorrect.
	<-s.listening
	c := NewClient(s.Addr())
	time.Sleep(heartbeatInterval * 2)
	if c != NewClient(s.Addr()) {
		t.Error("expected cached client to be returned while healthy")
	}
	s.Close()
	c.Close()

	// Wait for the heartbeat interval, which should see a failure on
	// the client. Then get a new client and verify it's a new client
	// struct.
	time.Sleep(heartbeatInterval + 1*time.Millisecond)
	if c == NewClient(s.Addr()) {
		t.Error("expected failed client to not be returned in 2nd call to NewClient")
	}
}
