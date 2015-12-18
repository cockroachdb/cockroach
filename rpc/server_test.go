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
// permissions and limitations under the License.
//
// Author: jqmp (jaqueramaphan@gmail.com)

package rpc

import (
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

func TestDuplicateRegistration(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()

	s := Server{
		methods: make(map[string]method),
	}
	heartbeat := &Heartbeat{}
	if err := s.RegisterPublic("Foo.Bar", heartbeat.Ping, &PingRequest{}); err != nil {
		t.Fatalf("unexpected failure on first registration: %s", err)
	}
	if err := s.RegisterPublic("Foo.Bar", heartbeat.Ping, &PingRequest{}); err == nil {
		t.Fatalf("unexpected success on second registration")
	}
}

func TestUnregisteredMethod(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	nodeContext := newNodeTestContext(nil, stopper)

	_, ln := newTestServer(t, nodeContext, false)

	opts := Options{
		N: 1,
	}

	// Sending an invalid method fails cleanly, but leaves the connection
	// in a valid state.
	_, err := sendRPC(opts, []net.Addr{ln.Addr()}, nodeContext, "Foo.Bar",
		&PingRequest{}, &PingResponse{})
	if !testutils.IsError(err, ".*rpc: couldn't find method: Foo.Bar") {
		t.Fatalf("expected 'couldn't find method' but got %s", err)
	}
	if _, err := sendPing(opts, []net.Addr{ln.Addr()}, nodeContext); err != nil {
		t.Fatalf("unexpected failure sending ping after unknown request: %s", err)
	}
}
