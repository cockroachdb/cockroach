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

	ln, err := util.ListenAndServeGRPC(ctx.Stopper, s, util.TestAddr)
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
	opts := SendOptions{Context: context.Background()}
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

	opts := SendOptions{
		Ordering:        orderStable,
		SendNextTimeout: 1 * time.Second,
		Timeout:         10 * time.Second,
		Context:         context.Background(),
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

	serverStopper := stop.NewStopper()
	serverContext := newNodeTestContext(nil, serverStopper)

	s, ln := newTestServer(t, serverContext)
	roachpb.RegisterInternalServer(s, Node(0))

	conn, err := clientContext.GRPCDial(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	waitForConnState := func(desiredState grpc.ConnectivityState) {
		clientState, err := conn.State()
		for clientState != desiredState {
			if err != nil {
				t.Fatal(err)
			}
			if clientState == grpc.Shutdown {
				t.Fatalf("%v has unexpectedly shut down", conn)
			}
			clientState, err = conn.WaitForStateChange(ctx, clientState)
		}
	}
	// Wait until the client becomes healthy and shut down the server.
	waitForConnState(grpc.Ready)
	serverStopper.Stop()
	// Wait until the client becomes unhealthy.
	waitForConnState(grpc.TransientFailure)

	opts := SendOptions{
		Ordering:        orderStable,
		SendNextTimeout: 100 * time.Millisecond,
		Timeout:         100 * time.Millisecond,
		Context:         context.Background(),
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

// setupSendNextTest sets up a situation in which SendNextTimeout has
// caused RPCs to be sent to all three replicas simultaneously. The
// caller may then cause those RPCs to finish by writing to one of the
// 'done' channels in the first return value; the second returned
// channel will contain the final result of the send() call.
func setupSendNextTest(t *testing.T) ([]chan batchCall, chan batchCall, *stop.Stopper) {
	stopper := stop.NewStopper()
	nodeContext := newNodeTestContext(nil, stopper)

	// Using a real listener here speeds up the tests, compared to using
	// a fake non-resolving address.
	_, ln := newTestServer(t, nodeContext)
	addrs := []net.Addr{ln.Addr(), ln.Addr(), ln.Addr()}

	opts := SendOptions{
		Ordering:        orderStable,
		SendNextTimeout: 1 * time.Millisecond,
		Timeout:         10 * time.Second,
		Context:         context.Background(),
	}

	doneChanChan := make(chan chan batchCall, len(addrs))
	sendOneFn = func(_ SendOptions, _ *rpc.Context,
		_ batchClient, done chan batchCall) {
		doneChanChan <- done
	}
	stopper.AddCloser(stop.CloserFn(func() { sendOneFn = sendOne }))

	sendChan := make(chan batchCall, 1)
	go func() {
		// Send the batch. This will block until we signal one of the done
		// channels.
		br, err := sendBatch(opts, addrs, nodeContext)
		sendChan <- batchCall{br, err}
	}()

	var doneChans []chan batchCall
	for i := 0; i < len(addrs); i++ {
		doneChans = append(doneChans, <-doneChanChan)
	}
	return doneChans, sendChan, stopper
}

// Test the behavior of SendNextTimeout when all servers are slow to
// respond.
func TestSendNext_AllSlow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	doneChans, sendChan, stopper := setupSendNextTest(t)
	defer stopper.Stop()

	// Now that all replicas have been contacted, let one finish.
	doneChans[1] <- batchCall{
		reply: &roachpb.BatchResponse{
			BatchResponse_Header: roachpb.BatchResponse_Header{
				Now: roachpb.Timestamp{Logical: 42},
			},
		},
		err: nil,
	}

	// The RPC now completes successfully.
	bc := <-sendChan
	if bc.err != nil {
		t.Fatal(bc.err)
	}
	// Make sure the response we sent in is the one we get back.
	if bc.reply.Now.Logical != 42 {
		t.Errorf("got unexpected response: %s", bc.reply)
	}
}

// Test the behavior of SendNextTimeout when some servers return
// RPC errors but one succeeds.
func TestSendNext_RPCErrorThenSuccess(t *testing.T) {
	defer leaktest.AfterTest(t)()

	doneChans, sendChan, stopper := setupSendNextTest(t)
	defer stopper.Stop()

	// Now that all replicas have been contacted, let two finish with
	// retryable errors.
	for i := 1; i <= 2; i++ {
		doneChans[i] <- batchCall{
			reply: nil,
			err:   roachpb.NewSendError("boom", true),
		}
	}

	// The client is still waiting for the third slow RPC to complete.
	select {
	case bc := <-sendChan:
		t.Fatalf("got unexpected response %v", bc)
	default:
	}

	// Now let the final server complete the RPC successfully.
	doneChans[0] <- batchCall{
		reply: nil,
		err:   nil,
	}

	// The client side now completes successfully.
	bc := <-sendChan
	if bc.err != nil {
		t.Fatal(bc.err)
	}
}

// Test the behavior of SendNextTimeout when all servers return
// RPC errors (this is effectively the same whether
// SendNextTimeout is used or not).
func TestSendNext_AllRPCErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	doneChans, sendChan, stopper := setupSendNextTest(t)
	defer stopper.Stop()

	// All replicas finish with RPC errors.
	for i := 0; i <= 2; i++ {
		doneChans[i] <- batchCall{
			reply: nil,
			err:   errors.New("boom"),
		}
	}

	// The client side completes with a retryable send error.
	bc := <-sendChan
	if sErr, ok := bc.err.(*roachpb.SendError); !ok {
		t.Errorf("did not get expected SendError; got %T instead", bc.err)
	} else if !sErr.CanRetry() {
		t.Errorf("expected a retryable error")
	}
}

// TestClientNotReady verifies that Send gets an RPC error when a client
// does not become ready.
func TestClientNotReady(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()

	// Construct a server that listens but doesn't do anything. Notice that we
	// never start accepting connections on the listener.
	ln, err := net.Listen(util.TestAddr.Network(), util.TestAddr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	opts := SendOptions{
		Ordering:        orderStable,
		SendNextTimeout: 100 * time.Nanosecond,
		Timeout:         100 * time.Nanosecond,
		Context:         context.Background(),
	}

	// Send RPC to an address where no server is running.
	nodeContext := newNodeTestContext(nil, stopper)
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

	// Send the RPC again with no timeout. We create a new node context to ensure
	// there is a new connection.
	nodeContext = newNodeTestContext(nil, stopper)
	opts.SendNextTimeout = 0
	opts.Timeout = 0
	c := make(chan error)
	sent := make(chan struct{})

	// Start a goroutine to accept the connection from the client. We'll close
	// the sent channel after receiving the connection, thus ensuring that the
	// RPC was sent before we closed the connection. We intentionally do not
	// close the server connection as doing so triggers other gRPC code paths.
	go func() {
		_, err := ln.Accept()
		if err != nil {
			c <- err
		} else {
			close(sent)
		}
	}()
	go func() {
		_, err := sendBatch(opts, []net.Addr{ln.Addr()}, nodeContext)
		if !testutils.IsError(err, "failed as client connection was closed") {
			c <- util.Errorf("unexpected error: %v", err)
		}
		close(c)
	}()

	select {
	case err := <-c:
		t.Fatalf("Unexpected end of rpc call: %v", err)
	case <-sent:
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

	// TODO(bdarnell): the retryable flag is no longer used for RPC errors.
	// Rework this test to incorporate application-level errors carried in
	// the BatchResponse.
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
		{5, 5, 0, false, true},
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

		opts := SendOptions{
			Ordering:        orderStable,
			SendNextTimeout: 1 * time.Second,
			Timeout:         10 * time.Second,
			Context:         context.Background(),
		}

		// Mock sendOne.
		sendOneFn = func(_ SendOptions, _ *rpc.Context,
			client batchClient, done chan batchCall) {
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
