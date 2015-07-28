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
// Author: jqmp (jaqueramaphan@gmail.com)

package rpc

import (
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

func checkUpdateMatches(t *testing.T, network, oldAddrString, newAddrString, expAddrString string) {
	oldAddr := util.MakeUnresolvedAddr(network, oldAddrString)
	newAddr := util.MakeUnresolvedAddr(network, newAddrString)
	expAddr := util.MakeUnresolvedAddr(network, expAddrString)

	retAddr, err := updatedAddr(oldAddr, newAddr)
	if err != nil {
		t.Fatalf("updatedAddr failed on %v, %v: %v", oldAddr, newAddr, err)
	}

	if retAddr.String() != expAddrString {
		t.Fatalf("updatedAddr(%v, %v) was %s; expected %s", oldAddr, newAddr, retAddr, expAddr)
	}
}

func checkUpdateFails(t *testing.T, network, oldAddrString, newAddrString string) {
	oldAddr := util.MakeUnresolvedAddr(network, oldAddrString)
	newAddr := util.MakeUnresolvedAddr(network, newAddrString)

	retAddr, err := updatedAddr(oldAddr, newAddr)
	if err == nil {
		t.Fatalf("updatedAddr(%v, %v) should have failed; instead returned %v", oldAddr, newAddr, retAddr)
	}
}

func TestUpdatedAddr(t *testing.T) {
	defer leaktest.AfterTest(t)
	for _, network := range []string{"tcp", "tcp4", "tcp6"} {
		checkUpdateMatches(t, network, "localhost:0", "127.0.0.1:1234", "localhost:1234")
		checkUpdateMatches(t, network, "localhost:1234", "127.0.0.1:1234", "localhost:1234")
		// This case emits a warning, but doesn't fail.
		checkUpdateMatches(t, network, "localhost:1234", "127.0.0.1:1235", "localhost:1235")
	}

	checkUpdateMatches(t, "unix", "address", "address", "address")
	checkUpdateFails(t, "unix", "address", "anotheraddress")
}

func TestDuplicateRegistration(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()

	s := NewServer(util.CreateTestAddr("tcp"), NewNodeTestContext(nil, stopper))
	heartbeat := &Heartbeat{}
	if err := s.Register("Foo.Bar", heartbeat.Ping, &proto.PingRequest{}); err != nil {
		t.Fatalf("unexpected failure on first registration: %s", err)
	}
	if err := s.Register("Foo.Bar", heartbeat.Ping, &proto.PingRequest{}); err == nil {
		t.Fatalf("unexpected success on second registration")
	}
}

func TestUnregisteredMethod(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	nodeContext := NewNodeTestContext(nil, stopper)

	s := createAndStartNewServer(t, nodeContext)

	opts := Options{
		N: 1,
	}

	// Sending an invalid method fails cleanly, but leaves the connection
	// in a valid state.
	_, err := sendRPC(opts, []net.Addr{s.Addr()}, nodeContext, "Foo.Bar",
		&proto.PingRequest{}, &proto.PingResponse{})
	if !testutils.IsError(err, ".*rpc: couldn't find method: Foo.Bar") {
		t.Fatalf("expected 'couldn't find method' but got %s", err)
	}
	if _, err := sendPing(opts, []net.Addr{s.Addr()}, nodeContext); err != nil {
		t.Fatalf("unexpected failure sending ping after unknown request: %s", err)
	}
}
