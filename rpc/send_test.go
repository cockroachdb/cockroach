// Copyright 2015 The Cockroach Authors.
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
// Author: joezxy (joe.zxy@foxmail.com)

package rpc

import (
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

func TestInvalidAddrLength(t *testing.T) {

	// The provided addrs is nil, so its length will be always
	// less than the specified response number
	ret, err := Send(Options{N: 1}, "", nil, nil, nil, nil)

	// the expected return is nil and SendError
	if _, ok := err.(SendError); !ok || ret != nil {
		t.Fatalf("Shorter addrs should return nil and SendError.")
	}
}

// TestSendToOneClient verifies that Send correctly sends a request
// to one server using the heartbeat RPC.
func TestSendToOneClient(t *testing.T) {
	rpcContext := createNewTestRPCContext(t)
	s := createAndStartNewServer(rpcContext, t)
	defer s.Close()

	opts := Options{
		N:               1,
		Ordering:        OrderStable,
		SendNextTimeout: 1 * time.Second,
		Timeout:         1 * time.Second,
	}
	replies, err := sendPing(opts, []net.Addr{s.Addr()}, rpcContext)
	if err != nil {
		t.Fatal(err)
	}
	if len(replies) != 1 {
		t.Errorf("Exactly one reply is expected, but got %v", len(replies))
	}
}

// TestSendToMultipleClients verifies that Send correctly sends
// multiple requests to multiple server using the heartbeat RPC.
func TestSendToMultipleClients(t *testing.T) {
	rpcContext := createNewTestRPCContext(t)
	numServers := 4
	var addrs []net.Addr
	for i := 0; i < numServers; i++ {
		s := createAndStartNewServer(rpcContext, t)
		defer s.Close()
		addrs = append(addrs, s.Addr())
	}
	for n := 1; n < numServers; n++ {
		// Send n requests.
		opts := Options{
			N:               n,
			Ordering:        OrderStable,
			SendNextTimeout: 1 * time.Second,
			Timeout:         1 * time.Second,
		}
		replies, err := sendPing(opts, addrs, rpcContext)
		if err != nil {
			t.Fatal(err)
		}
		if len(replies) != n {
			t.Errorf("%v replies are expected, but got %v", n, len(replies))
		}
	}
}

// TestRetryableError verifies that Send returns a retryable error
// when it hits an RPC error.
func TestRetryableError(t *testing.T) {
	rpcContext := createNewTestRPCContext(t)
	s := createAndStartNewServer(rpcContext, t)

	// Wait until the server becomes ready and shut down the server.
	c := NewClient(s.Addr(), nil, rpcContext)
	<-c.Ready
	// Directly call Close() to close the connection without
	// removing the client from the cache.
	c.Client.Close()
	s.Close()

	opts := Options{
		N:               1,
		Ordering:        OrderStable,
		SendNextTimeout: 1 * time.Second,
		Timeout:         1 * time.Second,
	}
	_, err := sendPing(opts, []net.Addr{s.Addr()}, rpcContext)
	if err == nil {
		t.Fatalf("Unexpected success")
	}
	retryErr, ok := err.(util.Retryable)
	if !ok {
		t.Fatalf("Unexpected error type: %v", err)
	}
	if !retryErr.CanRetry() {
		t.Errorf("Expected retryable error: %v", retryErr)
	}
}

// TestUnretryableError verifies that Send returns an unretryable
// error when it hits a critical error.
func TestUnretryableError(t *testing.T) {
	rpcContext := createNewTestRPCContext(t)
	s := createAndStartNewServer(rpcContext, t)

	opts := Options{
		N:               1,
		Ordering:        OrderStable,
		SendNextTimeout: 1 * time.Second,
		Timeout:         1 * time.Second,
	}
	getArgs := func(addr net.Addr) interface{} {
		return &proto.PingRequest{}
	}
	// Make getRetry return a non-proto value so that the proto
	// integrity check fails.
	getReply := func() interface{} {
		return 0
	}
	_, err := Send(opts, "Heartbeat.Ping", []net.Addr{s.Addr()}, getArgs, getReply, rpcContext)
	if err == nil {
		t.Fatalf("Unexpected success")
	}
	retryErr, ok := err.(util.Retryable)
	if !ok {
		t.Fatalf("Unexpected error type: %v", err)
	}
	if retryErr.CanRetry() {
		t.Errorf("Unexpected retryable error: %v", retryErr)
	}
}

// createNewTestRPCContext creates an RPCContext used for test.
func createNewTestRPCContext(t *testing.T) *Context {
	tlsConfig, err := LoadTestTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	return NewContext(hlc.NewClock(hlc.UnixNano), tlsConfig)
}

// createAndStartNewServer creates and starts a new server with a test address.
func createAndStartNewServer(rpcContext *Context, t *testing.T) *Server {
	s := NewServer(util.CreateTestAddr("tcp"), rpcContext)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	return s
}

// sendPing sends Ping requests to specified addresses using Send.
func sendPing(opts Options, addrs []net.Addr, rpcContext *Context) ([]interface{}, error) {
	getArgs := func(addr net.Addr) interface{} {
		return &proto.PingRequest{}
	}
	getReply := func() interface{} {
		return &proto.PingResponse{}
	}
	return Send(opts, "Heartbeat.Ping", addrs, getArgs, getReply, rpcContext)
}
