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

package kv

import (
	"errors"
	"net"
	"testing"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/tracing"
)

// newNodeTestContext returns a rpc.Context for testing.
// It is meant to be used by nodes.
func newNodeTestContext(clock *hlc.Clock, stopper *stop.Stopper) *rpc.Context {
	ctx := rpc.NewContext(testutils.NewNodeTestBaseContext(), clock, stopper)
	ctx.HeartbeatInterval = 10 * time.Millisecond
	ctx.HeartbeatTimeout = 5 * time.Second
	return ctx
}

func newTestServer(t *testing.T, ctx *rpc.Context) (*grpc.Server, net.Listener) {
	s := rpc.NewServer(ctx)

	addr := util.CreateTestAddr("tcp")
	ln, err := util.ListenAndServeGRPC(ctx.Stopper, s, addr)
	if err != nil {
		t.Fatal(err)
	}

	return s, ln
}

type Node time.Duration

func (n Node) Batch(ctx context.Context, args *roachpb.BatchRequest) (*roachpb.BatchResponse, error) {
	if n > 0 {
		time.Sleep(time.Duration(n))
	}
	return &roachpb.BatchResponse{}, nil
}

func TestInvalidAddrLength(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The provided replicas is nil, so its length will be always less than the
	// specified response number
	sp := tracing.NewTracer().StartSpan("node test")
	defer sp.Finish()
	opts := SendOptions{Trace: sp}
	ret, err := send(opts, nil, roachpb.BatchRequest{}, nil)

	// the expected return is nil and SendError
	if _, ok := err.(*roachpb.SendError); !ok || ret != nil {
		t.Fatalf("Shorter replicas should return nil and SendError.")
	}
}

// TestSendToOneClient verifies that Send correctly sends a request
// to one server using the heartbeat RPC.
func TestSendToOneClient(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()

	ctx := newNodeTestContext(nil, stopper)
	s, ln := newTestServer(t, ctx)
	roachpb.RegisterInternalServer(s, Node(0))

	sp := tracing.NewTracer().StartSpan("node test")
	defer sp.Finish()

	opts := SendOptions{
		Ordering:        orderStable,
		SendNextTimeout: 1 * time.Second,
		Timeout:         10 * time.Second,
		Trace:           sp,
	}
	reply, err := sendBatch(opts, []net.Addr{ln.Addr()}, ctx)
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
	defer leaktest.AfterTest(t)()

	clientStopper := stop.NewStopper()
	defer clientStopper.Stop()
	clientContext := newNodeTestContext(nil, clientStopper)
	clientContext.HeartbeatTimeout = 10 * clientContext.HeartbeatInterval

	serverStopper := stop.NewStopper()
	serverContext := newNodeTestContext(nil, serverStopper)

	s, ln := newTestServer(t, serverContext)
	roachpb.RegisterInternalServer(s, Node(0))

	conn, err := clientContext.GRPCDial(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// Wait until the client becomes healthy and shut down the server.
	for clientState, err := conn.State(); clientState != grpc.Ready; clientState, err = conn.WaitForStateChange(ctx, clientState) {
		if err != nil {
			t.Fatal(err)
		}
		if clientState == grpc.Shutdown {
			t.Fatalf("%v has unexpectedly shut down", conn)
		}
	}
	serverStopper.Stop()
	// Wait until the client becomes unhealthy.
	for clientState, err := conn.State(); clientState != grpc.TransientFailure; clientState, err = conn.WaitForStateChange(ctx, clientState) {
		if err != nil {
			t.Fatal(err)
		}
		if clientState == grpc.Shutdown {
			t.Fatalf("%v has unexpectedly shut down", conn)
		}
	}

	sp := tracing.NewTracer().StartSpan("node test")
	defer sp.Finish()

	opts := SendOptions{
		Ordering:        orderStable,
		SendNextTimeout: 100 * time.Millisecond,
		Timeout:         100 * time.Millisecond,
		Trace:           sp,
	}
	if _, err := sendBatch(opts, []net.Addr{ln.Addr()}, clientContext); err != nil {
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

// TestUnretryableError verifies that Send returns an unretryable
// error when it hits a critical error.
func TestUnretryableError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()

	nodeContext := newNodeTestContext(nil, stopper)
	_, ln := newTestServer(t, nodeContext)

	sp := tracing.NewTracer().StartSpan("node test")
	defer sp.Finish()

	opts := SendOptions{
		Ordering:        orderStable,
		SendNextTimeout: 1 * time.Second,
		Timeout:         10 * time.Second,
		Trace:           sp,
	}

	sendOneFn = func(_ batchClient, _ time.Duration,
		_ *rpc.Context, _ opentracing.Span, done chan batchCall) {
		done <- batchCall{
			reply: &roachpb.BatchResponse{},
			err:   errors.New("unretryable"),
		}
	}
	defer func() { sendOneFn = sendOne }()

	_, err := sendBatch(opts, []net.Addr{ln.Addr()}, nodeContext)
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

// TestClientNotReady verifies that Send gets an RPC error when a client
// does not become ready.
func TestClientNotReady(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()

	nodeContext := newNodeTestContext(nil, stopper)

	// Construct a server that listens but doesn't do anything.
	s, ln := newTestServer(t, nodeContext)
	roachpb.RegisterInternalServer(s, Node(50*time.Millisecond))

	sp := tracing.NewTracer().StartSpan("node test")
	defer sp.Finish()

	opts := SendOptions{
		Ordering:        orderStable,
		SendNextTimeout: 100 * time.Nanosecond,
		Timeout:         100 * time.Nanosecond,
		Trace:           sp,
	}

	// Send RPC to an address where no server is running.
	if _, err := sendBatch(opts, []net.Addr{ln.Addr()}, nodeContext); err != nil {
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
		if _, err := sendBatch(opts, []net.Addr{ln.Addr()}, nodeContext); !testutils.IsError(err, "(failed as client connection was closed|transport is closing)") {
			c <- util.Errorf("unexpected error when client was closed: %v", err)
		}
		close(c)
	}()

	select {
	case err := <-c:
		t.Fatalf("Unexpected end of rpc call: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	// Grab the client for our invalid address and close it. This will cause the
	// blocked ping RPC to finish.
	conn, err := nodeContext.GRPCDial(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-c; err != nil {
		t.Fatal(err)
	}
}

// TestComplexScenarios verifies various complex success/failure scenarios by
// mocking sendOne.
func TestComplexScenarios(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
			_, ln := newTestServer(t, nodeContext)
			serverAddrs = append(serverAddrs, ln.Addr())
		}

		sp := tracing.NewTracer().StartSpan("node test")
		defer sp.Finish()

		opts := SendOptions{
			Ordering:        orderStable,
			SendNextTimeout: 1 * time.Second,
			Timeout:         10 * time.Second,
			Trace:           sp,
		}

		// Mock sendOne.
		sendOneFn = func(client batchClient, _ time.Duration,
			_ *rpc.Context, _ opentracing.Span, done chan batchCall) {
			addrID := -1
			for serverAddrID, serverAddr := range serverAddrs {
				if serverAddr.String() == client.remoteAddr {
					addrID = serverAddrID
					break
				}
			}
			if addrID == -1 {
				t.Fatalf("%d: %s is not found in serverAddrs: %v", i, client.remoteAddr, serverAddrs)
			}
			call := batchCall{
				reply: &roachpb.BatchResponse{},
			}
			if addrID < numErrors {
				call.err = roachpb.NewSendError("test", addrID < numRetryableErrors)
			}
			done <- call
		}
		defer func() { sendOneFn = sendOne }()

		reply, err := sendBatch(opts, serverAddrs, nodeContext)
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

func makeReplicas(addrs ...net.Addr) ReplicaSlice {
	replicas := make(ReplicaSlice, len(addrs))
	for i, addr := range addrs {
		replicas[i].NodeDesc = &roachpb.NodeDescriptor{
			Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
		}
	}
	return replicas
}

// sendBatch sends Batch requests to specified addresses using send.
func sendBatch(opts SendOptions, addrs []net.Addr, rpcContext *rpc.Context) (*roachpb.BatchResponse, error) {
	return send(opts, makeReplicas(addrs...), roachpb.BatchRequest{}, rpcContext)
}
