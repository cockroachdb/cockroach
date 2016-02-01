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
	netrpc "net/rpc"
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

type Heartbeat struct{}

func (h *Heartbeat) Ping(args proto.Message) (proto.Message, error) {
	return &PingResponse{}, nil
}

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

	// Sending an invalid method fails cleanly, but leaves the connection
	// in a valid state.
	client := NewClient(ln.Addr(), nodeContext)
	<-client.Healthy()

	done := make(chan *netrpc.Call, 1)
	client.Go("Foo.Bar", &PingRequest{}, &PingResponse{}, done)
	call := <-done

	if !testutils.IsError(call.Error, "rpc: unable to find method: Foo.Bar") {
		t.Fatalf("expected 'rpc: unable to find method' but got %s", call.Error)
	}
}
