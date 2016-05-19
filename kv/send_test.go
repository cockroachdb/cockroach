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
	"strconv"
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
func (n Node) PollFrozen(_ context.Context, _ *roachpb.PollFrozenRequest) (*roachpb.PollFrozenResponse, error) {
	panic("unimplemented")
}

func TestInvalidAddrLength(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The provided replicas is nil, so its length will be always less than the
	// specified response number
	opts := SendOptions{Context: context.Background()}
	ret, err := (&DistSender{}).sendToReplicas(opts, 0, nil, roachpb.BatchRequest{}, nil)

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
		SendNextTimeout: 100 * time.Millisecond,
		Timeout:         100 * time.Millisecond,
		Context:         context.Background(),
	}
	if _, err := sendBatch(opts, []net.Addr{ln.Addr()}, clientContext); err == nil {
		t.Fatalf("Unexpected success")
	}
}

// channelSaveTransport captures the 'done' channels of every RPC it
// "sends".
type channelSaveTransport struct {
	ch        chan chan BatchCall
	remaining int
}

func (c *channelSaveTransport) IsExhausted() bool {
	return c.remaining <= 0
}

func (c *channelSaveTransport) SendNext(done chan BatchCall) {
	c.remaining--
	c.ch <- done
}

func (*channelSaveTransport) Close() {
}

// setupSendNextTest sets up a situation in which SendNextTimeout has
// caused RPCs to be sent to all three replicas simultaneously. The
// caller may then cause those RPCs to finish by writing to one of the
// 'done' channels in the first return value; the second returned
// channel will contain the final result of the send() call.
//
// TODO(bdarnell): all the 'done' channels are currently the same.
// Either give each call its own channel, return a list of (replica
// descriptor, channel) pair, or decide we don't care about
// distinguishing them and just send a single channel.
func setupSendNextTest(t *testing.T) ([]chan BatchCall, chan BatchCall, *stop.Stopper) {
	stopper := stop.NewStopper()
	nodeContext := newNodeTestContext(nil, stopper)

	addrs := []net.Addr{
		util.NewUnresolvedAddr("dummy", "1"),
		util.NewUnresolvedAddr("dummy", "2"),
		util.NewUnresolvedAddr("dummy", "3"),
	}

	doneChanChan := make(chan chan BatchCall, len(addrs))

	opts := SendOptions{
		SendNextTimeout: 1 * time.Millisecond,
		Timeout:         10 * time.Second,
		Context:         context.Background(),
		transportFactory: func(_ SendOptions,
			_ *rpc.Context,
			replicas ReplicaSlice,
			_ roachpb.BatchRequest,
		) (Transport, error) {
			return &channelSaveTransport{
				ch:        doneChanChan,
				remaining: len(replicas),
			}, nil
		},
	}

	sendChan := make(chan BatchCall, 1)
	go func() {
		// Send the batch. This will block until we signal one of the done
		// channels.
		br, err := sendBatch(opts, addrs, nodeContext)
		sendChan <- BatchCall{br, err}
	}()

	doneChans := make([]chan BatchCall, len(addrs))
	for i := range doneChans {
		// Note that this blocks until the replica has been contacted.
		doneChans[i] = <-doneChanChan
	}
	return doneChans, sendChan, stopper
}

// Test the behavior of SendNextTimeout when all servers are slow to
// respond (but successful).
func TestSendNext_AllSlow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	doneChans, sendChan, stopper := setupSendNextTest(t)
	defer stopper.Stop()

	// Now that all replicas have been contacted, let one finish.
	doneChans[1] <- BatchCall{
		Reply: &roachpb.BatchResponse{
			BatchResponse_Header: roachpb.BatchResponse_Header{
				Now: roachpb.Timestamp{Logical: 42},
			},
		},
		Err: nil,
	}

	// The RPC now completes successfully.
	bc := <-sendChan
	if bc.Err != nil {
		t.Fatal(bc.Err)
	}
	// Make sure the response we sent in is the one we get back.
	if bc.Reply.Now.Logical != 42 {
		t.Errorf("got unexpected response: %s", bc.Reply)
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
		doneChans[i] <- BatchCall{
			Reply: nil,
			Err:   roachpb.NewSendError("boom"),
		}
	}

	// The client is still waiting for the third slow RPC to complete.
	select {
	case bc := <-sendChan:
		t.Fatalf("got unexpected response %v", bc)
	default:
	}

	// Now let the final server complete the RPC successfully.
	doneChans[0] <- BatchCall{
		Reply: &roachpb.BatchResponse{},
		Err:   nil,
	}

	// The client side now completes successfully.
	bc := <-sendChan
	if bc.Err != nil {
		t.Fatal(bc.Err)
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
		doneChans[i] <- BatchCall{
			Reply: nil,
			Err:   errors.New("boom"),
		}
	}

	// The client side completes with a retryable send error.
	bc := <-sendChan
	if _, ok := bc.Err.(*roachpb.SendError); !ok {
		t.Errorf("did not get expected SendError; got %T instead", bc.Err)
	}
}

func TestSendNext_RetryableApplicationErrorThenSuccess(t *testing.T) {
	defer leaktest.AfterTest(t)()

	doneChans, sendChan, stopper := setupSendNextTest(t)
	defer stopper.Stop()

	// One replica finishes with a retryable error.
	doneChans[1] <- BatchCall{
		Reply: &roachpb.BatchResponse{
			BatchResponse_Header: roachpb.BatchResponse_Header{
				Error: roachpb.NewError(roachpb.NewRangeNotFoundError(1)),
			},
		},
	}

	// A second replica finishes successfully.
	doneChans[2] <- BatchCall{
		Reply: &roachpb.BatchResponse{},
	}

	// The client send finishes with the second response.
	bc := <-sendChan
	if bc.Err != nil {
		t.Fatalf("unexpected RPC error: %s", bc.Err)
	}
	if bc.Reply.Error != nil {
		t.Errorf("expected successful reply, got %s", bc.Reply.Error)
	}
}

func TestSendNext_AllRetryableApplicationErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	doneChans, sendChan, stopper := setupSendNextTest(t)
	defer stopper.Stop()

	// All replicas finish with a retryable error.
	for _, ch := range doneChans {
		ch <- BatchCall{
			Reply: &roachpb.BatchResponse{
				BatchResponse_Header: roachpb.BatchResponse_Header{
					Error: roachpb.NewError(roachpb.NewRangeNotFoundError(1)),
				},
			},
		}
	}

	// The client send finishes with one of the errors, wrapped in a SendError.
	bc := <-sendChan
	if bc.Err == nil {
		t.Fatalf("expected SendError, got err=nil and reply=%s", bc.Reply)
	} else if _, ok := bc.Err.(*roachpb.SendError); !ok {
		t.Fatalf("expected SendError, got err=%s", bc.Err)
	} else if exp := "range 1 was not found"; !testutils.IsError(bc.Err, exp) {
		t.Errorf("expected SendError to contain %q, but got %s", exp, bc.Err)
	}
}

func TestSendNext_NonRetryableApplicationError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	doneChans, sendChan, stopper := setupSendNextTest(t)
	defer stopper.Stop()

	// One replica finishes with a non-retryable error.
	doneChans[1] <- BatchCall{
		Reply: &roachpb.BatchResponse{
			BatchResponse_Header: roachpb.BatchResponse_Header{
				Error: roachpb.NewError(roachpb.NewTransactionReplayError()),
			},
		},
	}

	// The client completes with that error, without waiting for the
	// others to finish.
	bc := <-sendChan
	if bc.Err != nil {
		t.Fatalf("expected error in payload, not rpc error %s", bc.Err)
	}
	if _, ok := bc.Reply.Error.GetDetail().(*roachpb.TransactionReplayError); !ok {
		t.Errorf("expected TransactionReplayError, got %v", bc.Reply.Error)
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
		SendNextTimeout: 100 * time.Nanosecond,
		Timeout:         100 * time.Nanosecond,
		Context:         context.Background(),
	}

	// Send RPC to an address where no server is running.
	nodeContext := newNodeTestContext(nil, stopper)
	if _, err := sendBatch(opts, []net.Addr{ln.Addr()}, nodeContext); err == nil {
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

// firstNErrorTransport is a mock transport that sends an error on
// requests to the first N addresses, then succeeds.
type firstNErrorTransport struct {
	replicas  ReplicaSlice
	args      roachpb.BatchRequest
	numErrors int
	numSent   int
}

func (f *firstNErrorTransport) IsExhausted() bool {
	return f.numSent >= len(f.replicas)
}

func (f *firstNErrorTransport) SendNext(done chan BatchCall) {
	call := BatchCall{
		Reply: &roachpb.BatchResponse{},
	}
	if f.numSent < f.numErrors {
		call.Err = roachpb.NewSendError("test")
	}
	f.numSent++
	done <- call
}

func (*firstNErrorTransport) Close() {
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
		numServers int
		numErrors  int
		success    bool
	}{
		// --- Success scenarios ---
		{1, 0, true},
		{5, 0, true},
		// There are some errors, but enough RPCs succeed.
		{5, 1, true},
		{5, 4, true},
		{5, 2, true},

		// --- Failure scenarios ---
		// All RPCs fail.
		{5, 5, false},
	}
	for i, test := range testCases {
		var serverAddrs []net.Addr
		for j := 0; j < test.numServers; j++ {
			serverAddrs = append(serverAddrs, util.NewUnresolvedAddr("dummy",
				strconv.Itoa(j)))
		}

		opts := SendOptions{
			SendNextTimeout: 1 * time.Second,
			Timeout:         10 * time.Second,
			Context:         context.Background(),
			transportFactory: func(_ SendOptions,
				_ *rpc.Context,
				replicas ReplicaSlice,
				args roachpb.BatchRequest,
			) (Transport, error) {
				return &firstNErrorTransport{
					replicas:  replicas,
					args:      args,
					numErrors: test.numErrors,
				}, nil
			},
		}

		reply, err := sendBatch(opts, serverAddrs, nodeContext)
		if test.success {
			if err != nil {
				t.Errorf("%d: unexpected error: %s", i, err)
			}
			if reply == nil {
				t.Errorf("%d: expected reply", i)
			}
		} else {
			if err == nil {
				t.Errorf("%d: unexpected success", i)
			}
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
	return (&DistSender{}).sendToReplicas(opts, 0, makeReplicas(addrs...), roachpb.BatchRequest{}, rpcContext)
}
