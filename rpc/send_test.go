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
// permissions and limitations under the License.
//
// Author: joezxy (joe.zxy@foxmail.com)

package rpc

import (
	"net"
	"net/rpc"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

func TestInvalidAddrLength(t *testing.T) {
	defer leaktest.AfterTest(t)

	// The provided addrs is nil, so its length will be always
	// less than the specified response number
	ret, err := Send(Options{}, "", nil, nil, nil, nil)

	// the expected return is nil and SendError
	if _, ok := err.(*roachpb.SendError); !ok || ret != nil {
		t.Fatalf("Shorter addrs should return nil and SendError.")
	}
}

// TestSendToOneClient verifies that Send correctly sends a request
// to one server using the heartbeat RPC.
func TestSendToOneClient(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	ctx := newNodeTestContext(nil, stopper)
	_, ln := newTestServer(t, ctx, false)

	opts := Options{
		Ordering:        OrderStable,
		SendNextTimeout: 1 * time.Second,
		Timeout:         10 * time.Second,
	}
	reply, err := sendPing(opts, []net.Addr{ln.Addr()}, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reply == nil {
		t.Errorf("expected reply")
	}
}

// TestRetryableError verifies that Send returns a retryable error
// when it hits an RPC error.
func TestRetryableError(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	nodeContext := newNodeTestContext(nil, stopper)
	nodeContext.heartbeatTimeout = 10 * nodeContext.heartbeatInterval
	_, ln := newTestServer(t, nodeContext, false)

	c := NewClient(ln.Addr(), nodeContext)
	// Wait until the client becomes healthy and shut down the server.
	<-c.Healthy()
	ln.Close()
	// Wait until the client becomes unhealthy.
	func() {
		for r := retry.Start(retry.Options{}); r.Next(); {
			select {
			case <-c.Healthy():
			case <-time.After(1 * time.Nanosecond):
				return
			}
		}
	}()

	opts := Options{
		Ordering:        OrderStable,
		SendNextTimeout: 100 * time.Millisecond,
		Timeout:         100 * time.Millisecond,
	}
	if _, err := sendPing(opts, []net.Addr{ln.Addr()}, nodeContext); err != nil {
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
	*roachpb.ResponseHeader
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

	nodeContext := newNodeTestContext(nil, stopper)
	_, ln := newTestServer(t, nodeContext, false)

	opts := Options{
		Ordering:        OrderStable,
		SendNextTimeout: 1 * time.Second,
		Timeout:         10 * time.Second,
	}
	getArgs := func(addr net.Addr) proto.Message {
		return &roachpb.Span{}
	}
	// Make getReply return a BrokenResponse so that the proto
	// integrity check fails.
	getReply := func() proto.Message {
		return &BrokenResponse{&roachpb.ResponseHeader{}}
	}
	_, err := Send(opts, "Heartbeat.Ping", []net.Addr{ln.Addr()}, getArgs, getReply, nodeContext)
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

func (h *Heartbeat) Ping(args proto.Message) (proto.Message, error) {
	time.Sleep(50 * time.Millisecond)
	return &PingResponse{}, nil
}

// TestClientNotReady verifies that Send gets an RPC error when a client
// does not become ready.
func TestClientNotReady(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	nodeContext := newNodeTestContext(nil, stopper)

	// Construct a server that listens but doesn't do anything.
	s, ln := newTestServer(t, nodeContext, true)
	if err := s.RegisterPublic("Heartbeat.Ping", (&Heartbeat{}).Ping, &PingRequest{}); err != nil {
		t.Fatal(err)
	}

	opts := Options{
		Ordering:        OrderStable,
		SendNextTimeout: 100 * time.Nanosecond,
		Timeout:         100 * time.Nanosecond,
	}

	// Send RPC to an address where no server is running.
	if _, err := sendPing(opts, []net.Addr{ln.Addr()}, nodeContext); err != nil {
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
	c := make(chan error)
	go func() {
		if _, err := sendPing(opts, []net.Addr{ln.Addr()}, nodeContext); err == nil {
			c <- util.Errorf("expected error when client is closed")
		} else if !strings.Contains(err.Error(), "failed as client connection was closed") {
			c <- err
		}
		close(c)
	}()

	select {
	case <-c:
		t.Fatalf("Unexpected end of rpc call")
	case <-time.After(1 * time.Millisecond):
	}

	// Grab the client for our invalid address and close it. This will cause the
	// blocked ping RPC to finish.
	NewClient(ln.Addr(), nodeContext).Close()
	if err := <-c; err != nil {
		t.Fatal(err)
	}
}

// TestComplexScenarios verifies various complex success/failure scenarios by
// mocking sendOne.
func TestComplexScenarios(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	defer stopper.Stop()

	nodeContext := newNodeTestContext(nil, stopper)

	testCases := []struct {
		numServers               int
		numErrors                int
		numRetryableErrors       int
		success                  bool
		isRetryableErrorExpected bool
	}{
		// --- Success scenarios ---
		{1, 0, 0, true, false},
		{5, 0, 0, true, false},
		// There are some errors, but enough RPCs succeed.
		{5, 1, 0, true, false},
		{5, 4, 0, true, false},
		{5, 2, 0, true, false},

		// --- Failure scenarios ---
		// All RPCs fail.
		{5, 5, 0, false, false},
		// All RPCs fail, but some of the errors are retryable.
		{5, 5, 1, false, true},
		{5, 5, 3, false, true},
		// Some RPCs fail, but we do have enough remaining clients and recoverable errors.
		{5, 5, 2, false, true},
	}
	for i, test := range testCases {
		// Copy the values to avoid data race. sendOneFn might
		// be called after this test case finishes.
		numErrors := test.numErrors
		numRetryableErrors := test.numRetryableErrors

		var serverAddrs []net.Addr
		for j := 0; j < test.numServers; j++ {
			_, ln := newTestServer(t, nodeContext, false)
			serverAddrs = append(serverAddrs, ln.Addr())
		}

		opts := Options{
			Ordering:        OrderStable,
			SendNextTimeout: 1 * time.Second,
			Timeout:         10 * time.Second,
		}
		getArgs := func(addr net.Addr) proto.Message {
			return &PingRequest{}
		}
		getReply := func() proto.Message {
			return &PingResponse{}
		}

		// Mock sendOne.
		sendOneFn = func(client *Client, timeout time.Duration, method string, args, reply proto.Message, done chan *rpc.Call) {
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

		reply, err := Send(opts, "Heartbeat.Ping", serverAddrs, getArgs, getReply, nodeContext)
		if test.success {
			if reply == nil {
				t.Errorf("%d: expected reply", i)
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

// sendPing sends Ping requests to specified addresses using Send.
func sendPing(opts Options, addrs []net.Addr, rpcContext *Context) (proto.Message, error) {
	return sendRPC(opts, addrs, rpcContext, "Heartbeat.Ping",
		&PingRequest{}, &PingResponse{})
}

func sendRPC(opts Options, addrs []net.Addr, rpcContext *Context, name string,
	args, reply proto.Message) (proto.Message, error) {
	getArgs := func(addr net.Addr) proto.Message {
		return args
	}
	getReply := func() proto.Message {
		return proto.Clone(reply)
	}
	return Send(opts, name, addrs, getArgs, getReply, rpcContext)
}
