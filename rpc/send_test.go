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
	"net/rpc"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	gogoproto "github.com/gogo/protobuf/proto"
)

func TestInvalidAddrLength(t *testing.T) {
	defer leaktest.AfterTest(t)

	// The provided addrs is nil, so its length will be always
	// less than the specified response number
	ret, err := Send(Options{N: 1}, "", nil, nil, nil, nil)

	// the expected return is nil and SendError
	if _, ok := err.(*proto.SendError); !ok || ret != nil {
		t.Fatalf("Shorter addrs should return nil and SendError.")
	}
}

// TestSendToOneClient verifies that Send correctly sends a request
// to one server using the heartbeat RPC.
func TestSendToOneClient(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	nodeContext := NewNodeTestContext(nil, stopper)
	s := createAndStartNewServer(t, nodeContext)

	opts := Options{
		N:               1,
		Ordering:        OrderStable,
		SendNextTimeout: 1 * time.Second,
		Timeout:         1 * time.Second,
	}
	replies, err := sendPing(opts, []net.Addr{s.Addr()}, nodeContext)
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
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	nodeContext := NewNodeTestContext(nil, stopper)

	numServers := 4
	var addrs []net.Addr
	for i := 0; i < numServers; i++ {
		s := createAndStartNewServer(t, nodeContext)
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
		replies, err := sendPing(opts, addrs, nodeContext)
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
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	nodeContext := NewNodeTestContext(nil, stopper)
	s := createAndStartNewServer(t, nodeContext)

	c := NewClient(s.Addr(), nodeContext)
	// Wait until the client becomes ready and shut down the server.
	<-c.Healthy()
	s.Close()

	opts := Options{
		N:               1,
		Ordering:        OrderStable,
		SendNextTimeout: 1 * time.Second,
		Timeout:         1 * time.Second,
	}
	if _, err := sendPing(opts, []net.Addr{s.Addr()}, nodeContext); err != nil {
		retryErr, ok := err.(retry.Retryable)
		if !ok {
			t.Fatalf("Unexpected error type: %v", err)
		}
		if !retryErr.CanRetry() {
			t.Errorf("Expected retryable error: %v", retryErr)
		}
	} else {
		t.Fatalf("Unexpected success")
	}
}

type BrokenResponse struct {
	*proto.ResponseHeader
}

func (*BrokenResponse) Verify() error {
	return util.Errorf("boom")
}

// TestUnretryableError verifies that Send returns an unretryable
// error when it hits a critical error.
func TestUnretryableError(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	nodeContext := NewNodeTestContext(nil, stopper)
	s := createAndStartNewServer(t, nodeContext)

	opts := Options{
		N:               1,
		Ordering:        OrderStable,
		SendNextTimeout: 1 * time.Second,
		Timeout:         5 * time.Second,
	}
	getArgs := func(addr net.Addr) gogoproto.Message {
		return &proto.RequestHeader{}
	}
	// Make getRetry return a BrokenResponse so that the proto
	// integrity check fails.
	getReply := func() gogoproto.Message {
		return &BrokenResponse{&proto.ResponseHeader{}}
	}
	_, err := Send(opts, "Heartbeat.Ping", []net.Addr{s.Addr()}, getArgs, getReply, nodeContext)
	if err == nil {
		t.Fatalf("Unexpected success")
	}
	retryErr, ok := err.(retry.Retryable)
	if !ok {
		t.Fatalf("Unexpected error type: %v", err)
	}
	if retryErr.CanRetry() {
		t.Errorf("Unexpected retryable error: %v", retryErr)
	}
}

type Heartbeat struct{}

func (h *Heartbeat) Ping(args gogoproto.Message) (gogoproto.Message, error) {
	time.Sleep(50 * time.Millisecond)
	return &proto.PingResponse{}, nil
}

// TestClientNotReady verifies that Send gets an RPC error when a client
// does not become ready.
func TestClientNotReady(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	nodeContext := NewNodeTestContext(nil, stopper)

	addr := util.CreateTestAddr("tcp")

	// Construct a server that listens but doesn't do anything.
	s := &Server{
		context: nodeContext,
		addr:    addr,
		methods: map[string]method{},
	}
	if err := s.RegisterPublic("Heartbeat.Ping", (&Heartbeat{}).Ping, &proto.PingRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}

	opts := Options{
		N:               1,
		Ordering:        OrderStable,
		SendNextTimeout: 100 * time.Nanosecond,
		Timeout:         100 * time.Nanosecond,
	}

	// Send RPC to an address where no server is running.
	if _, err := sendPing(opts, []net.Addr{s.Addr()}, nodeContext); err != nil {
		retryErr, ok := err.(retry.Retryable)
		if !ok {
			t.Fatalf("Unexpected error type: %v", err)
		}
		if !retryErr.CanRetry() {
			t.Errorf("Expected retryable error: %v", retryErr)
		}
	} else {
		t.Fatalf("Unexpected success")
	}

	// Send the RPC again with no timeout.
	opts.SendNextTimeout = 0
	opts.Timeout = 0
	c := make(chan struct{})
	go func() {
		if _, err := sendPing(opts, []net.Addr{s.Addr()}, nodeContext); err == nil {
			t.Fatalf("expected error when client is closed")
		} else if !strings.Contains(err.Error(), "failed as client connection was closed") {
			t.Fatal(err)
		}
		close(c)
	}()
	select {
	case <-c:
		t.Fatalf("Unexpected end of rpc call")
	case <-time.After(1 * time.Millisecond):
	}

	// Grab the client for our invalid address and close it. This will
	// cause the blocked ping RPC to finish.
	NewClient(s.Addr(), nodeContext).Close()
	select {
	case <-c:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("RPC call failed to return")
	}
}

// TestComplexScenarios verifies various complex success/failure scenarios by
// mocking sendOne.
func TestComplexScenarios(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	nodeContext := NewNodeTestContext(nil, stopper)

	testCases := []struct {
		numServers               int
		numRequests              int
		numErrors                int
		numRetryableErrors       int
		success                  bool
		isRetryableErrorExpected bool
	}{
		// --- Success scenarios ---
		{1, 1, 0, 0, true, false},
		{5, 1, 0, 0, true, false},
		{5, 5, 0, 0, true, false},
		// There are some errors, but enough RPCs succeed.
		{5, 1, 1, 0, true, false},
		{5, 1, 4, 0, true, false},
		{5, 3, 2, 0, true, false},

		// --- Failure scenarios ---
		// Too many requests.
		{1, 5, 0, 0, false, false},
		// All RPCs fail.
		{5, 1, 5, 0, false, false},
		// Some RPCs fail and we do not have enough remaining clients.
		{5, 3, 3, 0, false, false},
		// All RPCs fail, but some of the errors are retryable.
		{5, 1, 5, 1, false, true},
		{5, 3, 5, 3, false, true},
		// Some RPCs fail, but we do have enough remaining clients and recoverable errors.
		{5, 3, 3, 1, false, true},
		{5, 3, 4, 2, false, true},
	}
	for i, test := range testCases {
		// Copy the values to avoid data race. sendOneFn might
		// be called after this test case finishes.
		numErrors := test.numErrors
		numRetryableErrors := test.numRetryableErrors

		var serverAddrs []net.Addr
		for j := 0; j < test.numServers; j++ {
			s := createAndStartNewServer(t, nodeContext)
			serverAddrs = append(serverAddrs, s.Addr())
		}

		opts := Options{
			N:               test.numRequests,
			Ordering:        OrderStable,
			SendNextTimeout: 1 * time.Second,
			Timeout:         1 * time.Second,
		}
		getArgs := func(addr net.Addr) gogoproto.Message {
			return &proto.PingRequest{}
		}
		getReply := func() gogoproto.Message {
			return &proto.PingResponse{}
		}

		// Mock sendOne.
		sendOneFn = func(client *Client, timeout time.Duration, method string, args, reply gogoproto.Message, done chan *rpc.Call) {
			addr := client.addr
			addrID := -1
			for serverAddrID, serverAddr := range serverAddrs {
				if serverAddr.String() == addr.String() {
					addrID = serverAddrID
					break
				}
			}
			if addrID == -1 {
				t.Fatalf("%d: %v is not found in serverAddrs: %v", i, addr, serverAddrs)
			}
			call := rpc.Call{
				Reply: reply,
			}
			if addrID < numErrors {
				call.Error = NewSendError("test", addrID < numRetryableErrors)
			}
			done <- &call
		}
		defer func() { sendOneFn = sendOne }()

		replies, err := Send(opts, "Heartbeat.Ping", serverAddrs, getArgs, getReply, nodeContext)
		if test.success {
			if len(replies) != test.numRequests {
				t.Errorf("%d: %v replies are expected, but got %v", i, test.numRequests, len(replies))
			}
			continue
		}

		retryErr, ok := err.(retry.Retryable)
		if !ok {
			t.Fatalf("%d: Unexpected error type: %v", i, err)
		}
		if retryErr.CanRetry() != test.isRetryableErrorExpected {
			t.Errorf("%d: Unexpected error: %v", i, retryErr)
		}
	}
}

// createAndStartNewServer creates and starts a new server with a test address.
func createAndStartNewServer(t *testing.T, ctx *Context) *Server {
	s := NewServer(util.CreateTestAddr("tcp"), ctx)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	return s
}

// sendPing sends Ping requests to specified addresses using Send.
func sendPing(opts Options, addrs []net.Addr, rpcContext *Context) ([]gogoproto.Message, error) {
	return sendRPC(opts, addrs, rpcContext, "Heartbeat.Ping",
		&proto.PingRequest{}, &proto.PingResponse{})
}

func sendRPC(opts Options, addrs []net.Addr, rpcContext *Context, name string,
	args, reply gogoproto.Message) ([]gogoproto.Message, error) {
	getArgs := func(addr net.Addr) gogoproto.Message {
		return args
	}
	getReply := func() gogoproto.Message {
		return gogoproto.Clone(reply)
	}
	return Send(opts, name, addrs, getArgs, getReply, rpcContext)
}
